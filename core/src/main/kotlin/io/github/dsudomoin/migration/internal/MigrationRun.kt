package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.EachResult
import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationScope
import io.github.dsudomoin.migration.Progress
import io.github.dsudomoin.migration.csv.CsvOutput
import io.github.dsudomoin.migration.error.CsvFileErrorReporter
import io.github.dsudomoin.migration.error.ErrorReporter
import io.github.dsudomoin.migration.report.ReportBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * Состояние одного прогона и единственная реализация [MigrationScope].
 *
 * Пул потоков создаётся только тогда, когда его попросили через `each(parallel > 1)`:
 * однопоточная миграция — подавляющий случай, и она не должна платить ни за создание
 * пула, ни за его остановку, ни за лишние кадры в стектрейсе.
 *
 * Публичен ради модуля `migration-dsl-kora`; конструктор закрыт.
 */
class MigrationRun internal constructor(
    override val dryRun: Boolean,
    override val log: Logger,
    override val outputFolder: Path,
    override val errors: ErrorReporter,
    val report: ReportBuilder,
    private val name: String,
    private val defaultProgressEvery: Int,
    private val defaultErrorThreshold: Long,
) : MigrationScope {

    // Флаг и очередь меняются под одним монитором: раздельные примитивы позволяли бы
    // закрытию целиком уложиться между чтением флага и добавлением в очередь, и такой
    // ресурс не закрывался бы никогда.
    private val stateLock = Any()
    private val resources = ArrayDeque<AutoCloseable>()
    private var closed = false
    private val outputs = ConcurrentHashMap<String, CsvOutput>()

    @Volatile
    private var pool: ExecutorService? = null

    /**
     * Запустить тело миграции. Исключение из него пробрасывается вызывающему: решение,
     * что с ним делать, принимает тот, кто владеет кодом возврата.
     */
    fun executeBody(migration: Migration) {
        with(migration) { run() }
    }

    override fun <T> each(
        items: Sequence<T>,
        parallel: Int,
        onItemError: ItemError<T>,
        progress: Progress,
        errorThreshold: Long?,
        handle: (T) -> Unit,
    ): EachResult {
        require(parallel > 0) { "parallel must be > 0, got $parallel" }
        val threshold = errorThreshold ?: defaultErrorThreshold
        val ticker = ProgressTicker(
            mode = progress,
            total = null,
            defaultEvery = defaultProgressEvery,
            sink = { log.info(it) },
        )
        val counters = EachCounters()

        if (parallel == 1) {
            for (item in items) {
                processItem(onItemError, handle, item, counters, threshold)
                ticker.tick()
            }
        } else {
            runParallel(items, parallel, onItemError, handle, counters, threshold, ticker)
        }
        return counters.snapshot()
    }

    /**
     * Окно in-flight ограничено семафором, а permit берётся ДО `next()` — поэтому ленивый
     * источник читается ровно настолько, насколько его успевают потреблять.
     *
     * После ошибки новые задачи не сабмитятся, но уже запущенные доводятся до конца:
     * захватить все permit'ы можно только когда каждая задача отпустила свой. Без этого
     * воркер-«зомби» писал бы в уже закрытые ресурсы прогона.
     */
    private fun <T> runParallel(
        items: Sequence<T>,
        parallel: Int,
        onItemError: ItemError<T>,
        handle: (T) -> Unit,
        counters: EachCounters,
        threshold: Long,
        ticker: ProgressTicker,
    ) {
        val permits = Semaphore(parallel)
        val failures = ConcurrentLinkedQueue<Throwable>()
        val iterator = items.iterator()
        val executor = pool()

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
                // Если executor отверг задачу, воркера не будет, и без явного release финальный
                // acquire(parallel) ждал бы разрешения, которое уже некому отпустить.
                val released = AtomicBoolean(false)
                val releasePermit = { if (released.compareAndSet(false, true)) permits.release() }
                try {
                    executor.execute {
                        try {
                            processItem(onItemError, handle, item, counters, threshold)
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

    private fun <T> processItem(
        policy: ItemError<T>,
        handle: (T) -> Unit,
        item: T,
        counters: EachCounters,
        threshold: Long,
    ) {
        report.incProcessed()
        counters.processed.incrementAndGet()
        try {
            handle(item)
            report.incSuccessful()
            counters.successful.incrementAndGet()
        } catch (e: Throwable) {
            handleItemFailure(policy, e, item, counters, threshold)
        }
    }

    private fun <T> handleItemFailure(
        policy: ItemError<T>,
        e: Throwable,
        item: T,
        counters: EachCounters,
        threshold: Long,
    ) {
        val decision = try {
            when (policy) {
                is ItemError.Fail -> ItemError.Decision.Fail
                is ItemError.Skip -> ItemError.Decision.Skip
                is ItemError.Handle -> policy.decide(e, item)
            }
        } catch (classifierError: Throwable) {
            // Сбой классификатора не имеет права проглотить отказ элемента: исходная ошибка
            // уходит в suppressed, и оба конца видны в errors.csv.
            classifierError.addSuppressed(e)
            audit(classifierError, item)
            report.incFailed()
            counters.failed.incrementAndGet()
            throw classifierError
        }
        audit(e, item)
        when (decision) {
            ItemError.Decision.Skip -> {
                report.incSkipped()
                // Инкремент и сравнение — одна атомарная операция: ровно один воркер увидит
                // превышение, и число состоявшихся skip'ов не зависит от планировщика.
                val n = counters.skipped.incrementAndGet()
                if (threshold > 0 && n > threshold) throw ErrorThresholdExceeded(threshold, n)
            }

            ItemError.Decision.Fail -> {
                report.incFailed()
                counters.failed.incrementAndGet()
                throw e
            }
        }
    }

    /**
     * Выход по имени: один файл — один handle на весь прогон.
     *
     * В императивном теле вызов `csv(...)` легко оказывается внутри цикла, и переоткрытие
     * файла на каждой итерации затирало бы написанное раньше.
     */
    fun csvOutput(key: String, factory: () -> CsvOutput): CsvOutput =
        outputs.computeIfAbsent(key) { factory().also { register(it) } }

    /**
     * Записать ошибку в `errors.csv`, не потеряв её, если сам аудитор упал (кончился диск).
     */
    fun audit(e: Throwable, item: Any?) {
        try {
            errors.report(e, item)
        } catch (auditError: Throwable) {
            log.warn("auditError failed for item=$item; original error preserved", auditError)
            report.addWarning("auditError failed: ${auditError.javaClass.simpleName}: ${auditError.message ?: ""}")
        }
    }

    private fun pool(): ExecutorService {
        pool?.let { return it }
        return synchronized(stateLock) {
            pool ?: newPool().also {
                pool = it
                register(AutoCloseable { shutdownPool(it) })
            }
        }
    }

    // Cached, а не fixed: реальный параллелизм задаёт семафор цикла, и пул обязан выдать
    // столько потоков, сколько попросили — иначе `parallel` был бы декорацией.
    private fun newPool(): ExecutorService {
        val threadNo = AtomicInteger()
        return Executors.newCachedThreadPool { r ->
            Thread(r, "migration-$name-${threadNo.incrementAndGet()}").apply { isDaemon = true }
        }
    }

    /**
     * Останов собственного пула. `shutdown()` + ожидание вместо голого `shutdownNow()`:
     * к этому моменту тело миграции уже отработало, но асинхронные хвосты ещё могут
     * доигрывать. Не вставший пул — это потеря задач, и она обязана быть видной в отчёте.
     */
    private fun shutdownPool(pool: ExecutorService) {
        pool.shutdown()
        val terminated = try {
            pool.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            false
        }
        if (!terminated) {
            val dropped = pool.shutdownNow().size
            val msg = "migration thread pool did not terminate in ${SHUTDOWN_WAIT_SECONDS}s; " +
                "$dropped queued task(s) dropped"
            log.warn(msg)
            report.addWarning(msg)
        }
    }

    /**
     * Зарегистрировать ресурс прогона. После закрытия реестра закрывает сразу: иначе
     * поздний ресурс остался бы открытым навсегда.
     */
    fun register(c: AutoCloseable) {
        val closeNow = synchronized(stateLock) {
            if (closed) {
                true
            } else {
                resources.addFirst(c)
                false
            }
        }
        // Закрываем вне монитора: close() может сам обратиться к реестру.
        if (closeNow) closeOne(c, "late-registered ")
    }

    /**
     * Закрыть все зарегистрированные ресурсы в обратном порядке. Идемпотентно; ошибки
     * закрытия не пробрасываются, а уезжают в предупреждения отчёта.
     */
    fun closeRegistered() {
        val snapshot = synchronized(stateLock) {
            if (closed) return
            closed = true
            resources.toList().also { resources.clear() }
        }
        snapshot.forEach { closeOne(it, "") }
    }

    private fun closeOne(c: AutoCloseable, prefix: String) {
        try {
            c.close()
        } catch (e: Throwable) {
            log.warn("${prefix}resource close failed: $c", e)
            report.addWarning(
                "${prefix}resource close failed: $c (${e.javaClass.simpleName}: ${e.message ?: "no message"})",
            )
        }
    }

    /** Локальные счётчики одного `each` — рядом с общими счётчиками прогона. */
    private class EachCounters {
        val processed = AtomicLong()
        val successful = AtomicLong()
        val skipped = AtomicLong()
        val failed = AtomicLong()

        fun snapshot() = EachResult(processed.get(), successful.get(), skipped.get(), failed.get())
    }

    companion object {
        private const val SHUTDOWN_WAIT_SECONDS = 30L

        /** Фабрика для юнит-тестов самой библиотеки. */
        fun forTest(
            outputFolder: Path,
            dryRun: Boolean = false,
            name: String = "test",
            progressEvery: Int = 1000,
            errorThreshold: Long = 0,
        ): MigrationRun {
            Files.createDirectories(outputFolder)
            return MigrationRun(
                dryRun = dryRun,
                log = LoggerFactory.getLogger("test.$name"),
                outputFolder = outputFolder,
                errors = CsvFileErrorReporter(
                    name,
                    outputFolder.resolve("errors.csv"),
                    outputFolder.resolve("errors.log"),
                ),
                report = ReportBuilder(name, dryRun),
                name = name,
                defaultProgressEvery = progressEvery,
                defaultErrorThreshold = errorThreshold,
            )
        }
    }
}
