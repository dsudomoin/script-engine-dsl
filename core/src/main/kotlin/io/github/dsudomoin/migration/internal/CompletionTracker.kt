package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.EffectFailure
import io.github.dsudomoin.migration.EffectRef
import io.github.dsudomoin.migration.ScopeCompletionTimeout
import io.github.dsudomoin.migration.ScopeEffectsFailed
import java.time.Duration
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Учёт асинхронных эффектов одного scope'а и барьер на его границе.
 *
 * Хранит счётчики и ограниченный набор отказов, а не сами завершённые stage'и: на миллионе
 * сообщений список futures сам стал бы утечкой. Каждый отказ отдаётся [onFailure] поштучно
 * (туда вешается аудит в `errors.csv`), а [maxRetainedFailures] ограничивает только то, что попадёт
 * в отчёт и в `suppressed`.
 */
internal class CompletionTracker(
    private val maxRetainedFailures: Int = 32,
    private val onFailure: (EffectFailure) -> Unit = {},
) {

    private val lock = ReentrantLock()
    private val allSettled = lock.newCondition()

    private var pending = 0L
    private var sealed = false
    private var ackedCount = 0L
    private var failedCount = 0L
    private var abandonedCount = 0L
    private var lateCount = 0L
    private val retained = ArrayDeque<EffectFailure>()

    val acked: Long get() = lock.withLock { ackedCount }
    val failed: Long get() = lock.withLock { failedCount }
    val abandoned: Long get() = lock.withLock { abandonedCount }
    val lateRegistered: Long get() = lock.withLock { lateCount }
    val retainedFailures: List<EffectFailure> get() = lock.withLock { retained.toList() }

    /**
     * Регистрирует эффект и запускает [send].
     *
     * Счётчик растёт ДО вызова [send], а не после: иначе между отправкой и регистрацией
     * есть окно, в котором запись уже в буфере продюсера, а барьер видит ноль и проходит.
     *
     * Подписка выполняется вне лока, чтобы колбэки драйвера не исполнялись под ним.
     */
    fun register(effect: EffectRef, send: () -> CompletionStage<*>) {
        val late = lock.withLock {
            if (sealed) {
                lateCount++
                true
            } else {
                pending++
                false
            }
        }

        val stage = try {
            send()
        } catch (e: Throwable) {
            // Синхронный бросок (ошибка сериализации, переполнение буфера): без декремента
            // барьер висел бы до таймаута из-за эффекта, который так и не был отправлен.
            if (!late) releaseOne()
            throw e
        }

        stage.whenComplete { _, error -> settle(effect, error, late) }
    }

    /**
     * Закрывает регистрацию и ждёт подтверждения всех эффектов scope'а.
     *
     * @throws ScopeCompletionTimeout если за [timeout] подтвердились не все
     * @throws ScopeEffectsFailed если хотя бы один завершился ошибкой
     */
    fun sealAndAwait(timeout: Duration?) {
        var timedOut = false
        lock.withLock {
            sealed = true
            var nanos = timeout?.toNanos() ?: Long.MAX_VALUE
            while (pending > 0L) {
                if (nanos <= 0L) {
                    abandonedCount = pending
                    timedOut = true
                    break
                }
                nanos = allSettled.awaitNanos(nanos)
            }
        }

        if (timedOut) {
            throw ScopeCompletionTimeout(
                "$abandoned effect(s) were not acknowledged within ${timeout?.toMillis()}ms",
            )
        }

        val failures = retainedFailures
        if (failures.isNotEmpty()) {
            val first = failures.first()
            val error = ScopeEffectsFailed(
                "$failed async effect(s) failed; first was '${first.effect.name}' ${first.effect.args}",
                first.error,
            )
            failures.drop(1).forEach { error.addSuppressed(it.error) }
            throw error
        }
    }

    private fun settle(effect: EffectRef, error: Throwable?, late: Boolean) {
        val failure = error?.let { EffectFailure(effect, unwrap(it)) }

        if (!late) {
            lock.withLock {
                if (failure != null) {
                    failedCount++
                    if (retained.size < maxRetainedFailures) retained += failure
                } else {
                    ackedCount++
                }
                pending--
                if (pending == 0L) allSettled.signalAll()
            }
        }

        // Аудит идёт вне лока и для поздних эффектов тоже: сообщение было отправлено по-настоящему,
        // и его отказ обязан быть виден, даже если барьер его уже не ждёт.
        if (failure != null) onFailure(failure)
    }

    private fun releaseOne() = lock.withLock {
        pending--
        if (pending == 0L) allSettled.signalAll()
    }

    // CompletableFuture оборачивает причину в CompletionException; в отчёте нужна исходная ошибка.
    private fun unwrap(e: Throwable): Throwable =
        if (e is CompletionException && e.cause != null) e.cause!! else e
}
