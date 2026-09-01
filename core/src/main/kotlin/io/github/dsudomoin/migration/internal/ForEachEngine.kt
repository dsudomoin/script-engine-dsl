package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.Progress
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.Semaphore

internal class ForEachEngine(
    private val ctx: MigrationContext,
) {
    private val warnedCallbacks = ConcurrentHashMap.newKeySet<String>()


    fun <T> runSingle(
        items: Iterable<T>,
        parallel: Int,
        onError: OnError,
        onErrorLog: ((Throwable, T) -> Unit)?,
        logEach: ((T) -> String)?,
        progress: Progress,
        block: MigrationContext.(T) -> Unit,
    ) {
        val total = (items as? Collection<T>)?.size?.toLong()
        val ticker = ProgressTicker(progress, total, defaultEvery = ctx.defaultProgressEvery) {
            ctx.log.info("[forEach] $it")
        }

        runAcross(items, parallel) { item ->
            processOne(item, onError, onErrorLog, logEach) { block(ctx, item) }
            ticker.tick()
        }
    }

    fun <T> runChunked(
        items: Iterable<T>,
        chunk: Int,
        parallel: Int,
        onError: OnError,
        onErrorLog: ((Throwable, List<T>) -> Unit)?,
        logEach: ((List<T>) -> String)?,
        progress: Progress,
        block: MigrationContext.(List<T>) -> Unit,
    ) {
        val batches = items.chunked(chunk)
        val total = batches.size.toLong()
        val ticker = ProgressTicker(progress, total, defaultEvery = ctx.defaultProgressEvery) {
            ctx.log.info("[forEach] $it")
        }

        runAcross(batches, parallel) { batch ->
            processOne(batch, onError, onErrorLog, logEach) { block(ctx, batch) }
            ticker.tick()
        }
    }

    private fun <U> runAcross(items: Iterable<U>, parallel: Int, work: (U) -> Unit) {
        require(parallel > 0) { "forEach(parallel = $parallel): parallel must be > 0" }
        if (parallel == 1) {
            items.forEach(work)
            return
        }
        // ctx.executor — то, что Kora wired через @Tag(MigrationExecutor::class) или дефолтный
        // FixedThreadPool runner'а. Bounded submission через Semaphore — Sequence-источники не
        // материализуются целиком в очередь executor'а.
        val executor = ctx.executor
        val semaphore = Semaphore(parallel)
        val exceptions = ConcurrentLinkedQueue<Throwable>()
        // Здесь копится только то, что ещё имеет смысл отменять. Раньше список рос по одному
        // Future на КАЖДЫЙ item за весь прогон и не чистился никогда: семафор ограничивает число
        // задач в работе, но не число удерживаемых объектов — на миллионных выгрузках это живой
        // мусор в heap'е. Компактим, когда список перерастает окно семафора.
        val futures = ArrayList<Future<*>>()
        val compactThreshold = maxOf(parallel * 4, 64)

        // Если executor — ExecutorService, используем submit(): возвращает Future, у которого
        // cancel(true) реально шлёт Thread.interrupt() в running task. Cooperative-блок (user
        // code, использующий Thread.sleep, interruptible I/O) — завершится
        // с InterruptedException и task закроется быстро. Plain Executor (без shutdown/submit)
        // получает fallback на CompletableFuture, где cancel(true) игнорирует interrupt-флаг —
        // outstanding tasks дойдут до конца естественно. Решение Kora-runner владеет дефолтным
        // ExecutorService (FixedThreadPool), так что в боевом сценарии fast-cancel работает.
        val canInterrupt = executor is ExecutorService

        val iter = items.iterator()
        while (iter.hasNext()) {
            if (exceptions.isNotEmpty()) break
            semaphore.acquire()
            if (exceptions.isNotEmpty()) {
                semaphore.release()
                break
            }
            // `iter.next()` сам может бросить (например, `Sequence.map { rs.next() }` на закрытом
            // ResultSet, или ошибка чтения CSV в `readCsv`). Перехватываем — иначе уже-submitted
            // futures остаются in-flight без cancel/join: исключение пробивает наружу runAcross,
            // outstanding tasks продолжают выполняться до конца, finally runner'а позже выключит
            // пул через `shutdownNow()`, но это далеко не сразу. Лучше — обработать как обычную
            // ошибку: освобождаем permit, добавляем в exceptions, break — драйн-цикл ниже сам
            // вызовет `cancel(true)` на всех submitted и пробросит primary.
            val item = try {
                iter.next()
            } catch (t: Throwable) {
                semaphore.release()
                if (t is InterruptedException) {
                    Thread.currentThread().interrupt(); throw t
                }
                exceptions.add(t)
                break
            }
            val task = Runnable {
                try {
                    work(item)
                } catch (t: Throwable) {
                    // Interruption = кооперативная отмена после первого fail'а в другом worker'е
                    // (мы сами вызвали cancel(true) ниже). Не считаем «бизнес-ошибкой».
                    if (t !is InterruptedException) exceptions.add(t)
                } finally {
                    semaphore.release()
                }
            }
            if (futures.size >= compactThreshold) futures.removeIf { it.isDone }
            futures += if (canInterrupt) {
                (executor as ExecutorService).submit(task)
            } else {
                CompletableFuture.runAsync(task, executor)
            }
        }

        // Ожидание завершения всех submitted. Если в любой момент в exceptions появилась запись
        // (worker fail'нул) — однократно cancel'им ВСЕ остальные. cancel(true) шлёт interrupt
        // в running threads (для FutureTask из FixedThreadPool); cooperative user-code в
        // Thread.sleep / interruptible I/O — выходит с InterruptedException.
        // ForkJoinTask/CompletableFuture игнорируют mayInterruptIfRunning — для них cancel —
        // best-effort (pending-task'и снимутся из очереди, running дойдут до конца).
        var selfInterrupted = false
        var cancelledAll = false
        for (f in futures) {
            if (!cancelledAll && exceptions.isNotEmpty()) {
                cancelledAll = true
                for (g in futures) {
                    try {
                        g.cancel(true)
                    } catch (_: Throwable) {
                    }
                }
            }
            try {
                f.get()
            } catch (_: ExecutionException) {
                // business exception уже накоплено в exceptions queue (через catch в task'е)
            } catch (_: CancellationException) {
                // мы сами cancel'нули — норм
            } catch (_: InterruptedException) {
                // Прервали runAcross-поток (runner / parent forEach). Сохраняем флаг, продолжаем
                // дренировать остальные futures чтобы не leak'нуть resources, затем re-throw.
                selfInterrupted = true
                Thread.currentThread().interrupt()
            }
        }

        if (selfInterrupted) throw InterruptedException("forEach interrupted while awaiting workers")

        if (exceptions.isNotEmpty()) {
            val list = exceptions.toList()
            val primary = list[0]
            for (i in 1 until list.size) primary.addSuppressed(list[i])
            throw primary
        }
    }

    private fun <U> processOne(
        item: U,
        onError: OnError,
        onErrorLog: ((Throwable, U) -> Unit)?,
        logEach: ((U) -> String)?,
        run: () -> Unit,
    ) {
        ctx.report.incProcessed()

        when (onError) {
            is OnError.Fail -> try {
                run()
                successAccounting(item, logEach)
            } catch (e: Throwable) {
                if (isCancellation(e)) {
                    Thread.currentThread().interrupt(); throw e
                }
                safeAudit(e, item)
                ctx.report.incFailed()
                onErrorLog?.let { cb -> safeCallback("onErrorLog") { cb(e, item) } }
                throw e
            }

            is OnError.Skip -> try {
                run()
                successAccounting(item, logEach)
            } catch (e: Throwable) {
                skipAccounting(e, item, onErrorLog)
            }

            is OnError.Handle -> try {
                run()
                successAccounting(item, logEach)
            } catch (e: Throwable) {
                if (isCancellation(e)) {
                    Thread.currentThread().interrupt(); throw e
                }
                val decision = try {
                    onError.decide(e, item)
                } catch (classifierError: Throwable) {
                    // Пользовательский decide() сам бросил — это его баг. Сохраняем оригинальное
                    // бизнес-исключение через addSuppressed (чтобы human-debugger видел оба),
                    // аудитим original (это item, на котором миграция реально сломалась),
                    // прокидываем classifier-throw наверх — там сработает unhandled-policy.
                    classifierError.addSuppressed(e)
                    safeAudit(e, item)
                    ctx.report.incFailed()
                    onErrorLog?.let { cb -> safeCallback("onErrorLog") { cb(e, item) } }
                    throw classifierError
                }
                when (decision) {
                    OnError.Decision.Skip -> skipAccounting(e, item, onErrorLog)
                    OnError.Decision.Fail -> {
                        safeAudit(e, item)
                        ctx.report.incFailed()
                        onErrorLog?.let { cb -> safeCallback("onErrorLog") { cb(e, item) } }
                        throw e
                    }
                }
            }
        }
    }

    private fun <U> successAccounting(item: U, logEach: ((U) -> String)?) {
        ctx.report.incSuccessful()
        if (logEach == null) return
        safeCallback("logEach") { logEach(item)?.let { ctx.log.info(it) } }
    }

    /**
     * Диагностические колбэки (`logEach`, `onErrorLog`) не должны уметь провалить item. Раньше их
     * исключение летело из учётного блока и трактовалось как отказ обработки: item попадал и в
     * `successful`, и в `failed`, а под дефолтным `OnError.Fail` миграция обрывалась уже ПОСЛЕ
     * того, как запись была выполнена. Про первый такой сбой сообщаем в отчёт, дальше молчим —
     * иначе на миллионе item'ов список warnings сам станет утечкой.
     */
    private inline fun safeCallback(what: String, body: () -> Unit) {
        try {
            body()
        } catch (t: Throwable) {
            if (t is InterruptedException) {
                Thread.currentThread().interrupt(); throw t
            }
            if (warnedCallbacks.add(what)) {
                ctx.log.warn("$what callback failed; item outcome is unchanged", t)
                ctx.report.addWarning(
                    "$what callback failed: ${t.javaClass.simpleName}: ${t.message ?: ""} " +
                        "(item outcome unchanged; further failures of this callback are not reported)",
                )
            }
        }
    }

    private fun <U> skipAccounting(e: Throwable, item: U, onErrorLog: ((Throwable, U) -> Unit)?) {
        // Cancellation-сигналы — не наша «бизнес-ошибка», не аудитим и не считаем как skipped.
        if (isCancellation(e)) {
            Thread.currentThread().interrupt(); throw e
        }
        safeAudit(e, item)
        ctx.report.incSkipped()
        onErrorLog?.let { cb -> safeCallback("onErrorLog") { cb(e, item) } }
        // Realtime threshold check: после каждого SKIP проверяем, не превысили ли допуск.
        val thr = ctx.errorThreshold
        if (thr > 0) {
            val skipped = ctx.report.skippedCount()
            if (skipped > thr) throw ErrorThresholdExceeded(thr, skipped)
        }
    }

    private fun isCancellation(e: Throwable): Boolean =
        e is InterruptedException || e is CancellationException

    /**
     * Безопасный вызов `auditError`. Если сам аудитор упал (например, диск кончился при записи
     * errors.csv) — записываем warning в отчёт, оригинальное исключение НЕ заменяется, поскольку
     * этот метод не пробрасывает. Вызывающий код продолжает работать с исходным `e`.
     */
    private fun safeAudit(e: Throwable, item: Any?) {
        try {
            ctx.auditError(e, item)
        } catch (auditErr: Throwable) {
            ctx.log.warn("auditError failed for item=$item; original error preserved", auditErr)
            ctx.report.addWarning(
                "auditError failed: ${auditErr.javaClass.simpleName}: ${auditErr.message ?: ""}"
            )
        }
    }
}
