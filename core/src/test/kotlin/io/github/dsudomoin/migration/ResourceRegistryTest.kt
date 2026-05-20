package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.internal.DefaultMigrationContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class ResourceRegistryTest {

    @Test
    fun `close - в обратном порядке регистрации`() {
        val ctx = DefaultMigrationContext.test()
        val closedOrder = mutableListOf<String>()
        ctx.register(AutoCloseable { closedOrder += "A" })
        ctx.register(AutoCloseable { closedOrder += "B" })
        ctx.register(AutoCloseable { closedOrder += "C" })

        ctx.closeRegistered()

        assertThat(closedOrder).containsExactly("C", "B", "A")
    }

    @Test
    fun `close - изоляция падающих ресурсов, остальные закрываются`() {
        val ctx = DefaultMigrationContext.test()
        val closed = mutableListOf<String>()
        ctx.register(AutoCloseable { closed += "A" })
        ctx.register(AutoCloseable { throw RuntimeException("B fails") })
        ctx.register(AutoCloseable { closed += "C" })

        ctx.closeRegistered()

        assertThat(closed).containsExactly("C", "A")
        val warnings = ctx.report.build().warnings
        assertThat(warnings).hasSize(1)
        assertThat(warnings.first())
            .contains("resource close failed")
            .contains("RuntimeException")
            .contains("B fails")
    }

    @Test
    fun `close - идемпотентность, повторный вызов - no-op`() {
        val ctx = DefaultMigrationContext.test()
        val count = AtomicInteger()
        ctx.register(AutoCloseable { count.incrementAndGet() })

        ctx.closeRegistered()
        ctx.closeRegistered()
        ctx.closeRegistered()

        assertThat(count.get()).isEqualTo(1)
    }

    @Test
    fun `register - thread-safe под параллельной регистрацией`() {
        val ctx = DefaultMigrationContext.test()
        val workers = 16
        val perWorker = 100
        val expectedTotal = workers * perWorker
        val closed = AtomicInteger()

        val pool = Executors.newFixedThreadPool(workers)
        val start = CountDownLatch(1)
        val done = CountDownLatch(workers)

        repeat(workers) {
            pool.submit {
                start.await()
                repeat(perWorker) {
                    ctx.register(AutoCloseable { closed.incrementAndGet() })
                }
                done.countDown()
            }
        }
        start.countDown()
        check(done.await(5, TimeUnit.SECONDS)) { "registration timed out" }
        pool.shutdown()
        check(pool.awaitTermination(2, TimeUnit.SECONDS))

        ctx.closeRegistered()

        assertThat(closed.get()).isEqualTo(expectedTotal)
    }

    @Test
    fun `register - после close ресурс закрывается немедленно`() {
        val ctx = DefaultMigrationContext.test()
        ctx.closeRegistered()

        var closedLate = false
        ctx.register(AutoCloseable { closedLate = true })

        assertThat(closedLate).isTrue()
    }

    @Test
    fun `register - после close падающий ресурс идёт в warnings и не бросает`() {
        val ctx = DefaultMigrationContext.test()
        ctx.closeRegistered()

        ctx.register(AutoCloseable { throw RuntimeException("late fail") })

        val warnings = ctx.report.build().warnings
        assertThat(warnings).hasSize(1)
        assertThat(warnings.first())
            .contains("late-registered")
            .contains("late fail")
    }
}
