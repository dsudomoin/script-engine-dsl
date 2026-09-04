package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.EffectRef
import io.github.dsudomoin.migration.ScopeCompletionTimeout
import io.github.dsudomoin.migration.ScopeEffectsFailed
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class CompletionTrackerTest {

    private fun ref(name: String = "e", item: Any? = null) = EffectRef(name, emptyMap(), item)

    @Test
    fun `успешная доставка считается acked только после завершения`() {
        val tracker = CompletionTracker()
        val future = CompletableFuture<String>()

        tracker.register(ref()) { future }
        assertThat(tracker.acked).isZero()

        future.complete("ok")
        tracker.sealAndAwait(Duration.ofSeconds(5))

        assertThat(tracker.acked).isEqualTo(1)
        assertThat(tracker.failed).isZero()
    }

    @Test
    fun `барьер ждёт, пока stage не завершён`() {
        val tracker = CompletionTracker()
        val future = CompletableFuture<String>()
        tracker.register(ref()) { future }

        val barrierPassed = CountDownLatch(1)
        val waiter = Thread {
            tracker.sealAndAwait(Duration.ofSeconds(10))
            barrierPassed.countDown()
        }.apply { isDaemon = true; start() }

        assertThat(barrierPassed.await(300, TimeUnit.MILLISECONDS)).isFalse()

        future.complete("ok")

        assertThat(barrierPassed.await(5, TimeUnit.SECONDS)).isTrue()
        waiter.join(1000)
    }

    @Test
    fun `stage, завершённый синхронно в момент регистрации, учтён`() {
        val tracker = CompletionTracker()

        tracker.register(ref()) { CompletableFuture.completedFuture("already done") }
        tracker.sealAndAwait(Duration.ofSeconds(5))

        assertThat(tracker.acked).isEqualTo(1)
    }

    @Test
    fun `отказ доставки валит барьер и сохраняет контекст эффекта`() {
        val tracker = CompletionTracker()
        val boom = RuntimeException("broker rejected")

        tracker.register(EffectRef("kafka.publish", mapOf("id" to 7), item = "item-7")) {
            CompletableFuture.failedFuture<String>(boom)
        }

        assertThatThrownBy { tracker.sealAndAwait(Duration.ofSeconds(5)) }
            .isInstanceOf(ScopeEffectsFailed::class.java)
            .hasRootCauseMessage("broker rejected")

        assertThat(tracker.failed).isEqualTo(1)
        assertThat(tracker.acked).isZero()
        assertThat(tracker.retainedFailures).singleElement().satisfies({ failure ->
            assertThat(failure.effect.name).isEqualTo("kafka.publish")
            assertThat(failure.effect.args).containsEntry("id", 7)
            assertThat(failure.effect.item).isEqualTo("item-7")
        })
    }

    @Test
    fun `синхронный бросок из send не оставляет висячий pending`() {
        val tracker = CompletionTracker()

        assertThatThrownBy {
            tracker.register(ref()) { throw IllegalStateException("producer buffer full") }
        }.isInstanceOf(IllegalStateException::class.java)

        // если бы декремента не было, барьер провисел бы до таймаута
        tracker.sealAndAwait(Duration.ofMillis(500))
        assertThat(tracker.acked).isZero()
        assertThat(tracker.failed).isZero()
    }

    @Test
    fun `таймаут бросает и учитывает незавершённые как abandoned`() {
        val tracker = CompletionTracker()
        tracker.register(ref()) { CompletableFuture<String>() }

        assertThatThrownBy { tracker.sealAndAwait(Duration.ofMillis(300)) }
            .isInstanceOf(ScopeCompletionTimeout::class.java)

        assertThat(tracker.abandoned).isEqualTo(1)
        assertThat(tracker.acked).isZero()
    }

    @Test
    fun `поздняя регистрация после барьера учитывается отдельно и не ломает счётчики`() {
        val tracker = CompletionTracker()
        tracker.register(ref()) { CompletableFuture.completedFuture("ok") }
        tracker.sealAndAwait(Duration.ofSeconds(5))

        tracker.register(ref("late")) { CompletableFuture.completedFuture("zombie") }

        assertThat(tracker.lateRegistered).isEqualTo(1)
        assertThat(tracker.acked).isEqualTo(1)
    }

    @Test
    fun `конкурентная регистрация из многих потоков учтена полностью`() {
        val tracker = CompletionTracker()
        val threads = 16
        val perThread = 100
        val start = CountDownLatch(1)

        val workers = (1..threads).map {
            Thread {
                start.await()
                repeat(perThread) {
                    tracker.register(ref()) { CompletableFuture.completedFuture("ok") }
                }
            }.apply { isDaemon = true; start() }
        }
        start.countDown()
        workers.forEach { it.join(10_000) }

        tracker.sealAndAwait(Duration.ofSeconds(10))

        assertThat(tracker.acked).isEqualTo((threads * perThread).toLong())
    }

    @Test
    fun `число сохранённых отказов ограничено, счётчик — нет`() {
        val tracker = CompletionTracker(maxRetainedFailures = 3)

        repeat(10) { i ->
            tracker.register(ref("e$i")) { CompletableFuture.failedFuture<String>(RuntimeException("fail-$i")) }
        }

        assertThatThrownBy { tracker.sealAndAwait(Duration.ofSeconds(5)) }
            .isInstanceOf(ScopeEffectsFailed::class.java)

        assertThat(tracker.failed).isEqualTo(10)
        assertThat(tracker.retainedFailures).hasSize(3)
    }

    @Test
    fun `каждый отказ отдаётся наблюдателю, а не только сохранённые`() {
        val audited = mutableListOf<String>()
        val tracker = CompletionTracker(
            maxRetainedFailures = 2,
            onFailure = { failure -> synchronized(audited) { audited += failure.effect.name } },
        )

        repeat(5) { i ->
            tracker.register(ref("e$i")) { CompletableFuture.failedFuture<String>(RuntimeException("x")) }
        }
        assertThatThrownBy { tracker.sealAndAwait(Duration.ofSeconds(5)) }
            .isInstanceOf(ScopeEffectsFailed::class.java)

        assertThat(audited).containsExactlyInAnyOrder("e0", "e1", "e2", "e3", "e4")
    }
}
