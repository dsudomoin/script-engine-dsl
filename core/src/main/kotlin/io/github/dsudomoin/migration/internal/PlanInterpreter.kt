package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.EffectRef
import io.github.dsudomoin.migration.HandlerScope
import io.github.dsudomoin.migration.Input
import io.github.dsudomoin.migration.InputScope
import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.Progress
import io.github.dsudomoin.migration.MigrationPlan
import io.github.dsudomoin.migration.RunScope
import io.github.dsudomoin.migration.SourceScope
import io.github.dsudomoin.migration.Stage
import io.github.dsudomoin.migration.WriteOutcome
import io.github.dsudomoin.migration.WriteResult
import io.github.dsudomoin.migration.csv.openCsv
import java.time.Duration
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Исполняет [MigrationPlan] в свежем контексте прогона.
 *
 * Стадии идут строго последовательно; провал стадии не запускает следующие. Кэш input'ов живёт
 * здесь, а не в плане: план неизменяем и не должен переживать состояние прогона.
 *
 * Публичен ради runner'а из модуля `migration-dsl-kora`; в коде миграций не используется.
 */
class PlanInterpreter(
    private val base: RunContext,
    private val progressSink: (String) -> Unit = { base.log.info(it) },
) {

    private val inputs = ConcurrentHashMap<Input<*>, Any?>()

    fun execute(plan: MigrationPlan) {
        val outputs = plan.outputs.map { handle ->
            val csv = base.openCsv(handle.filename, *handle.headers.toTypedArray())
            handle.bind(csv)
            handle to csv
        }

        try {
            plan.validation?.let { check ->
                StageScope(base, CompletionTracker(), inputs).check()
            }

            for (stage in plan.stages) {
                when (stage) {
                    is Stage.Flat<*> -> runFlat(stage)
                    is Stage.Scoped<*, *> -> runScoped(stage)
                }
            }
        } finally {
            // Закрываем сами: реестр прогона сделает это повторно и идемпотентно, зато строки гарантированно
            // оказываются на диске к моменту, когда прогон считается завершённым.
            outputs.forEach { (handle, csv) ->
                try {
                    csv.close()
                } catch (e: Throwable) {
                    val message = "output '${handle.filename}' close failed: ${e.javaClass.simpleName}"
                    base.log.warn(message, e)
                    base.report.addWarning(message)
                } finally {
                    handle.unbind()
                }
            }
        }
    }

    private fun <T> runFlat(stage: Stage.Flat<T>) {
        val ticker = ProgressTicker(stage.progress, total = null, defaultEvery = base.defaultProgressEvery, sink = progressSink)
        val skipped = AtomicLong()
        runScopeUnit(stage.completionTimeout) { scope ->
            runItems(stage.items(scope), stage, scope, stage.onItemError, stage.handle, ticker, skipped)
        }
    }

    private fun <P, T> runScoped(stage: Stage.Scoped<P, T>) {
        // Родители читаются в собственном scope'е: его ресурсы (например курсор по списку стратегий)
        // живут дольше отдельного родителя и закрываются в конце стадии.
        val parentScope = StageScope(base, CompletionTracker(), inputs)
        try {
            val ticker = ProgressTicker(stage.progress, total = null, defaultEvery = base.defaultProgressEvery, sink = progressSink)
            for (parent in stage.parents(parentScope)) {
                // Счётчик порога — на родителя: граница scope'а и есть единица работы.
                val skipped = AtomicLong()
                runScopeUnit(stage.completionTimeout) { scope ->
                    runItems(stage.items(scope, parent), stage, scope, stage.onItemError, stage.handle, ticker, skipped)
                }
            }
        } finally {
            closeScopedResources(parentScope)
        }
    }

    /**
     * Один scope: тело, потом барьер, потом закрытие ресурсов.
     *
     * Порядок важен: барьер стоит ДО закрытия, иначе ресурс, через который шла отправка, закроется
     * раньше, чем придут подтверждения. Барьер отрабатывает и на уже провалившемся scope'е: уже
     * отправленное не отзывается, и его исход обязан попасть в отчёт.
     */
    private fun runScopeUnit(timeout: Duration?, body: (StageScope) -> Unit) {
        val tracker = CompletionTracker(onFailure = { failure ->
            safeAudit(failure.error, failure.effect.item)
        })
        val scope = StageScope(base, tracker, inputs)

        var failure: Throwable? = null
        try {
            body(scope)
        } catch (e: Throwable) {
            failure = e
        }

        try {
            tracker.sealAndAwait(timeout)
        } catch (barrierError: Throwable) {
            failure = failure?.also { it.addSuppressed(barrierError) } ?: barrierError
        }

        // Итоги снимаются ПОСЛЕ закрытия ресурсов: закрывающийся ресурс может попытаться дослать
        // «хвост», и такая поздняя регистрация обязана попасть в отчёт именно этого scope'а.
        try {
            closeScopedResources(scope)
        } finally {
            base.report.addBarrierOutcome(
                acked = tracker.acked,
                failed = tracker.failed,
                abandoned = tracker.abandoned,
                late = tracker.lateRegistered,
            )
            tracker.callbackFailures.forEach {
                base.report.addWarning("effect audit failed: ${it.javaClass.simpleName}: ${it.message ?: ""}")
            }
        }

        failure?.let { throw it }
    }


    /**
     * Обход элементов стадии.
     *
     * При `parallel > 1` окно in-flight ограничено семафором, а permit берётся ДО `next()` — поэтому
     * ленивый источник читается ровно настолько, насколько его успевают потреблять.
     *
     * После ошибки новые задачи не сабмитятся, но уже запущенные доводятся до конца, а не
     * прерываются: захват всех permit'ов возможен только когда каждая задача отпустила свой. Без этого
     * воркер-«зомби» писал бы в уже закрытый выход и регистрировал эффекты после барьера.
     */
    private fun <P, T> runItems(
        items: Sequence<T>,
        stage: Stage<P, T>,
        scope: StageScope,
        policy: ItemError<T>,
        handle: HandlerScope.(T) -> Unit,
        ticker: ProgressTicker,
        skipped: AtomicLong,
    ) {
        val threshold = stageThreshold(stage)
        val parallel = stageParallel(stage)

        if (parallel <= 1) {
            for (item in items) {
                processItem(policy, scope, handle, item, skipped, threshold)
                ticker.tick()
            }
            return
        }

        val permits = Semaphore(parallel)
        val failures = ConcurrentLinkedQueue<Throwable>()
        val iterator = items.iterator()

        try {
            while (failures.isEmpty()) {
                permits.acquire()
                if (failures.isNotEmpty()) {
                    permits.release()
                    break
                }
                val hasNext = try {
                    iterator.hasNext()
                } catch (e: Throwable) {
                    permits.release()
                    failures += e
                    break
                }
                if (!hasNext) {
                    permits.release()
                    break
                }
                val item = try {
                    iterator.next()
                } catch (e: Throwable) {
                    permits.release()
                    failures += e
                    break
                }
                // Одно взятое разрешение — ровно одно возвращённое, кем бы оно ни было возвращено.
                // Если executor отверг задачу, воркера не будет, и без явного release внешний
                // acquire(parallel) ждал бы разрешения, которое уже некому отпустить.
                val released = AtomicBoolean(false)
                val releasePermit = { if (released.compareAndSet(false, true)) permits.release() }
                try {
                    base.executor.execute {
                        try {
                            processItem(policy, scope, handle, item, skipped, threshold)
                            ticker.tick()
                        } catch (e: Throwable) {
                            failures += e
                        } finally {
                            releasePermit()
                        }
                    }
                } catch (e: Throwable) {
                    releasePermit()
                    failures += e
                    break
                }
            }
        } finally {
            permits.acquire(parallel)
            permits.release(parallel)
        }

        val all = failures.toList()
        if (all.isNotEmpty()) {
            val primary = all.first()
            all.drop(1).forEach { if (it !== primary) primary.addSuppressed(it) }
            throw primary
        }
    }

    private fun <P, T> stageParallel(stage: Stage<P, T>): Int = when (stage) {
        is Stage.Flat<*> -> stage.parallel
        is Stage.Scoped<*, *> -> stage.parallel
    } ?: base.defaultParallel

    private fun <P, T> stageThreshold(stage: Stage<P, T>): Long = when (stage) {
        is Stage.Flat<*> -> stage.errorThreshold
        is Stage.Scoped<*, *> -> stage.errorThreshold
    } ?: base.errorThreshold

    private fun <T> processItem(
        policy: ItemError<T>,
        scope: StageScope,
        handle: HandlerScope.(T) -> Unit,
        item: T,
        skipped: AtomicLong,
        threshold: Long,
    ) {
        base.report.incProcessed()
        scope.currentItem.set(item)
        try {
            handle(scope, item)
            base.report.incSuccessful()
        } catch (e: Throwable) {
            handleItemFailure(policy, e, item, skipped, threshold)
        } finally {
            scope.currentItem.remove()
        }
    }

    private fun <T> handleItemFailure(
        policy: ItemError<T>,
        e: Throwable,
        item: T,
        skipped: AtomicLong,
        threshold: Long,
    ) {
        val decision = when (policy) {
            is ItemError.Fail -> ItemError.Decision.Fail
            is ItemError.Skip -> ItemError.Decision.Skip
            is ItemError.Handle -> policy.decide(e, item)
        }
        safeAudit(e, item)
        when (decision) {
            ItemError.Decision.Skip -> {
                base.report.incSkipped()
                // Инкремент и сравнение — одна атомарная операция: ровно один воркер увидит
                // превышение, и число состоявшихся skip'ов не зависит от планировщика.
                val n = skipped.incrementAndGet()
                if (threshold > 0 && n > threshold) throw ErrorThresholdExceeded(threshold, n)
            }
            ItemError.Decision.Fail -> {
                base.report.incFailed()
                throw e
            }
        }
    }

    // Отказ закрытия не подменяет исходную ошибку стадии и не прерывает закрытие соседей —
    // та же семантика, что у реестра ресурсов прогона.
    private fun closeScopedResources(scope: StageScope) {
        scope.drainScopedResources().forEach { close ->
            try {
                close()
            } catch (e: Throwable) {
                val message = "scoped resource close failed: ${e.javaClass.simpleName}: ${e.message ?: ""}"
                base.log.warn(message, e)
                base.report.addWarning(message)
            }
        }
    }

    // Аудитор может упасть сам (кончился диск) — терять из-за этого исходную ошибку нельзя.
    private fun safeAudit(e: Throwable, item: Any?) {
        try {
            base.auditError(e, item)
        } catch (auditError: Throwable) {
            base.log.warn("auditError failed for item=$item; original error preserved", auditError)
            base.report.addWarning(
                "auditError failed: ${auditError.javaClass.simpleName}: ${auditError.message ?: ""}",
            )
        }
    }
}

/**
 * Контекст одного scope'а. Реализует все три пользовательских scope'а сразу: разделяются они только
 * типами на границе DSL, а во время исполнения это одно и то же состояние.
 */
internal class StageScope(
    private val base: RunContext,
    private val tracker: CompletionTracker,
    private val inputs: ConcurrentHashMap<Input<*>, Any?>,
) : SourceScope, HandlerScope, InputScope, RunScope by base {

    // Обрабатываемый элемент нужен для контекста отказа, а воркеров у стадии может быть несколько.
    val currentItem = ThreadLocal<Any?>()

    private val scopedResources = ArrayDeque<() -> Unit>()

    @Suppress("UNCHECKED_CAST")
    override fun <I> resolve(input: Input<I>): I =
        inputs.computeIfAbsent(input) { input.load(this) } as I

    override fun <C : Any, T> pages(
        name: String?,
        first: () -> List<T>,
        next: (C) -> List<T>,
        nextCursor: (List<T>) -> C,
        continueWhen: (List<T>) -> Boolean,
    ): Sequence<T> = pagedSequence(name, first, next, nextCursor, continueWhen) { rows ->
        base.report.addRawPage(rows)
    }

    override fun scopedResource(close: () -> Unit) {
        synchronized(scopedResources) { scopedResources.addFirst(close) }
    }

    /** Забрать зарегистрированные закрытия в порядке, обратном регистрации. */
    fun drainScopedResources(): List<() -> Unit> = synchronized(scopedResources) {
        scopedResources.toList().also { scopedResources.clear() }
    }

    override fun write(name: String, args: Map<String, Any?>, action: () -> WriteOutcome): WriteResult {
        if (base.dryRun) {
            base.log.info("[DRY-RUN] $name ${renderArgs(args)}")
            base.report.incDryRunSkipped(name)
            return WriteResult.DryRunSkipped
        }
        return when (val outcome = action()) {
            is WriteOutcome.Applied -> {
                base.report.incAppliedWrite(name)
                WriteResult.Applied
            }

            is WriteOutcome.Rejected -> {
                base.report.incRejectedWrite(name)
                base.log.warn("$name rejected ${renderArgs(args)}: ${outcome.reason}")
                WriteResult.Rejected(outcome.reason)
            }
        }
    }

    override fun writeRows(name: String, args: Map<String, Any?>, action: () -> Int): WriteResult =
        write(name, args) {
            val rows = action()
            if (rows > 0) WriteOutcome.Applied else WriteOutcome.Rejected("no rows matched")
        }

    override fun publish(name: String, args: Map<String, Any?>, send: () -> CompletionStage<*>) {
        if (base.dryRun) {
            base.log.info("[DRY-RUN] $name ${renderArgs(args)}")
            base.report.incDryRunSkipped(name)
            return
        }
        tracker.register(EffectRef(name, args, currentItem.get()), send)
    }

    private fun renderArgs(args: Map<String, Any?>): String =
        if (args.isEmpty()) "" else args.entries.joinToString(", ", "(", ")") { "${it.key}=${it.value}" }
}
