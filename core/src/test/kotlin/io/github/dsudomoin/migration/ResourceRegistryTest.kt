package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.internal.RunContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class ResourceRegistryTest {

    @Test
    fun `close - в обратном порядке регистрации`() {
        val ctx = RunContext.test()
        val closedOrder = mutableListOf<String>()
        ctx.register(AutoCloseable { closedOrder += "A" })
        ctx.register(AutoCloseable { closedOrder += "B" })
        ctx.register(AutoCloseable { closedOrder += "C" })

        ctx.closeRegistered()

        assertThat(closedOrder).containsExactly("C", "B", "A")
    }

    @Test
    fun `close - изоляция падающих ресурсов, остальные закрываются`() {
        val ctx = RunContext.test()
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
        val ctx = RunContext.test()
        val count = AtomicInteger()
        ctx.register(AutoCloseable { count.incrementAndGet() })

        ctx.closeRegistered()
        ctx.closeRegistered()
        ctx.closeRegistered()

        assertThat(count.get()).isEqualTo(1)
    }

    @Test
    fun `register - thread-safe под параллельной регистрацией`() {
        val ctx = RunContext.test()
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
        val ctx = RunContext.test()
        ctx.closeRegistered()

        var closedLate = false
        ctx.register(AutoCloseable { closedLate = true })

        assertThat(closedLate).isTrue()
    }

    @Test
    fun `register - после close падающий ресурс идёт в warnings и не бросает`() {
        val ctx = RunContext.test()
        ctx.closeRegistered()

        ctx.register(AutoCloseable { throw RuntimeException("late fail") })

        val warnings = ctx.report.build().warnings
        assertThat(warnings).hasSize(1)
        assertThat(warnings.first())
            .contains("late-registered")
            .contains("late fail")
    }

    @Test
    fun `ресурс, зарегистрированный во время закрытия, всё равно закрывается`() {
        val ctx = RunContext.test()
        val lateClosed = AtomicBoolean(false)

        // Закрытие соседа регистрирует новый ресурс — это тот же интерливинг, что и гонка
        // register/closeRegistered, только детерминированный.
        ctx.register(AutoCloseable { ctx.register(AutoCloseable { lateClosed.set(true) }) })
        ctx.closeRegistered()

        assertThat(lateClosed).describedAs("поздняя регистрация обязана закрыться немедленно").isTrue()
    }

    @Test
    fun `shared не отдаёт из кэша закрытый ресурс`() {
        val ctx = RunContext.test()
        val created = AtomicInteger()
        val factory = { created.incrementAndGet(); AutoCloseable { } }

        ctx.register(AutoCloseable { ctx.shared("key", factory) })
        ctx.closeRegistered()

        ctx.shared("key", factory)
        assertThat(created.get())
            .describedAs("закрытый объект не должен остаться в кэше shared")
            .isEqualTo(2)
    }

    @Test
    fun `ни один ресурс не теряется при гонке регистрации и закрытия`() {
        // Окно дефекта — между чтением флага closed и добавлением в очередь: закрытие,
        // уложившееся целиком в этот промежуток, оставляло ресурс незакрытым навсегда.
        // Детерминированного шва для него нет, поэтому бьём по окну многократно.
        repeat(300) {
            val ctx = RunContext.test()
            val registered = AtomicInteger()
            val closed = AtomicInteger()
            val start = CountDownLatch(1)

            val writers = (1..4).map {
                Thread {
                    start.await()
                    repeat(25) {
                        registered.incrementAndGet()
                        ctx.register(AutoCloseable { closed.incrementAndGet() })
                    }
                }.apply { isDaemon = true; start() }
            }
            val closer = Thread {
                start.await()
                ctx.closeRegistered()
            }.apply { isDaemon = true; start() }

            start.countDown()
            writers.forEach { it.join(10_000) }
            closer.join(10_000)
            // Ресурсы, зарегистрированные уже после закрытия, закрываются немедленно; поэтому
            // после схождения потоков закрытыми обязаны быть все до единого.
            ctx.closeRegistered()

            assertThat(closed.get())
                .describedAs("итерация %s: зарегистрировано %s, закрыто %s", it, registered.get(), closed.get())
                .isEqualTo(registered.get())
        }
    }
}
