package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.EffectFailure
import io.github.dsudomoin.migration.EffectRef
import io.github.dsudomoin.migration.LateEffectRegistration
import io.github.dsudomoin.migration.ScopeCompletionTimeout
import io.github.dsudomoin.migration.ScopeEffectsFailed
import java.time.Duration
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicBoolean
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
    private val callbackErrors = ArrayDeque<Throwable>()

    val acked: Long get() = lock.withLock { ackedCount }
    val failed: Long get() = lock.withLock { failedCount }
    val abandoned: Long get() = lock.withLock { abandonedCount }
    val lateRegistered: Long get() = lock.withLock { lateCount }
    val retainedFailures: List<EffectFailure> get() = lock.withLock { retained.toList() }

    /** Ошибки, брошенные самим [onFailure]; интерпретатор поднимает их в `report.warnings`. */
    val callbackFailures: List<Throwable> get() = lock.withLock { callbackErrors.toList() }

    /**
     * Регистрирует эффект и запускает [send].
     *
     * Счётчик растёт ДО вызова [send], а не после: иначе между отправкой и регистрацией
     * есть окно, в котором запись уже в буфере продюсера, а барьер видит ноль и проходит.
     *
     * Подписка выполняется вне лока, чтобы колбэки драйвера не исполнялись под ним, но внутри того
     * же `try`, что и [send]: сбой самой подписки иначе оставил бы резервирование без колбэка, и
     * барьер ждал бы эффект, о судьбе которого узнать уже нельзя.
     *
     * @throws LateEffectRegistration если барьер уже закрыт — [send] при этом не вызывается
     */
    fun register(effect: EffectRef, send: () -> CompletionStage<*>) {
        lock.withLock {
            if (sealed) {
                lateCount++
                throw LateEffectRegistration(
                    "effect '${effect.name}' ${effect.args} was registered after the scope barrier " +
                        "had been sealed; it was NOT sent",
                )
            }
            pending++
        }

        // Одно резервирование — ровно одно освобождение, кем бы оно ни было сделано: колбэком
        // синхронно завершившегося stage'а или catch'ем ниже.
        val settled = AtomicBoolean(false)
        try {
            val stage = send()
            @Suppress("SENSELESS_COMPARISON")
            if (stage == null) {
                error("effect '${effect.name}' send() returned null instead of a CompletionStage")
            }
            stage.whenComplete { _, error -> settle(effect, error, settled) }
        } catch (e: Throwable) {
            // Синхронный бросок (ошибка сериализации, переполнение буфера, сбой подписки): без
            // освобождения барьер висел бы из-за эффекта, который так и не был отправлен.
            if (settled.compareAndSet(false, true)) releaseOne()
            throw e
        }
    }

    /**
     * Закрывает регистрацию и ждёт подтверждения всех эффектов scope'а.
     *
     * @throws ScopeCompletionTimeout если за [timeout] подтвердились не все
     * @throws ScopeEffectsFailed если хотя бы один завершился ошибкой
     */
    fun sealAndAwait(timeout: Duration?) {
        var timedOut = false
        try {
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
        } catch (e: InterruptedException) {
            // Прерванный барьер — это брошенные эффекты, а не успех: отражаем их в отчёте, возвращаем
            // флаг вызывающему (его съел бы catch выше по стеку) и отдаём ошибку наверх.
            lock.withLock { abandonedCount = pending }
            Thread.currentThread().interrupt()
            throw e
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

    /**
     * Аудит выполняется ДО декремента `pending` и вне лока.
     *
     * Этот порядок и есть смысл барьера: если освободить резервирование раньше аудита,
     * `sealAndAwait` вернёт управление, runner закроет `CsvFileErrorReporter`, и запись об отказе
     * исчезнет молча — `report()` после `close()` ничего не делает. Вне лока — чтобы медленный
     * аудитор не держал регистрацию соседних эффектов.
     */
    private fun settle(effect: EffectRef, error: Throwable?, settled: AtomicBoolean) {
        if (!settled.compareAndSet(false, true)) return
        val failure = error?.let { EffectFailure(effect, unwrap(it)) }

        try {
            if (failure != null) onFailure(failure)
        } catch (callbackError: Throwable) {
            // Отказ аудита не подменяет отказ доставки: он копится отдельно и станет warning'ом.
            lock.withLock { callbackErrors += callbackError }
        } finally {
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
    }

    private fun releaseOne() = lock.withLock {
        pending--
        if (pending == 0L) allSettled.signalAll()
    }

    // CompletableFuture оборачивает причину в CompletionException; в отчёте нужна исходная ошибка.
    private fun unwrap(e: Throwable): Throwable =
        if (e is CompletionException && e.cause != null) e.cause!! else e
}
