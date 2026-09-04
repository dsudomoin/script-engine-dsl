package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.Progress
import io.github.dsudomoin.migration.migration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class StageParallelismTest {

    private fun run(
        defaultParallel: Int = 1,
        errorThreshold: Long = 0,
        plan: io.github.dsudomoin.migration.MigrationPlan,
    ) = RunContext.test(defaultParallel = defaultParallel, errorThreshold = errorThreshold)
        .also { PlanInterpreter(it).execute(plan) }

    @Test
    fun `parallel больше одного — элементы идут в нескольких потоках`() {
        val threads = ConcurrentHashMap.newKeySet<String>()
        val started = CountDownLatch(4)

        run(plan = migration("M", "t") {
            source(parallel = 4, items = { (1..4).asSequence() }) {
                threads += Thread.currentThread().name
                started.countDown()
                // Без ожидания воркеры могут отработать последовательно и тест ничего не докажет.
                started.await(5, TimeUnit.SECONDS)
            }
        })

        assertThat(threads).hasSizeGreaterThan(1)
    }

    @Test
    fun `окно in-flight не превышает parallel`() {
        val inFlight = AtomicInteger()
        val peak = AtomicInteger()

        run(plan = migration("M", "t") {
            source(parallel = 3, items = { (1..50).asSequence() }) {
                val now = inFlight.incrementAndGet()
                peak.updateAndGet { maxOf(it, now) }
                Thread.sleep(5)
                inFlight.decrementAndGet()
            }
        })

        assertThat(peak.get()).isLessThanOrEqualTo(3)
    }

    @Test
    fun `parallel по умолчанию берётся из конфига прогона`() {
        val threads = ConcurrentHashMap.newKeySet<String>()
        val started = CountDownLatch(3)

        run(defaultParallel = 3, plan = migration("M", "t") {
            source(items = { (1..3).asSequence() }) {
                threads += Thread.currentThread().name
                started.countDown()
                started.await(5, TimeUnit.SECONDS)
            }
        })

        assertThat(threads).hasSizeGreaterThan(1)
    }

    @Test
    fun `под parallel ошибка элемента доводит уже запущенные до конца и пробрасывается`() {
        val finished = AtomicInteger()

        assertThatThrownBy {
            run(plan = migration("M", "t") {
                source(parallel = 4, items = { (1..8).asSequence() }) { item ->
                    try {
                        if (item == 1) error("item 1 failed")
                        Thread.sleep(50)
                    } finally {
                        finished.incrementAndGet()
                    }
                }
            })
        }.hasMessageContaining("item 1 failed")

        // Ни один воркер не брошен на полпути: все, кто стартовал, дошли до finally.
        assertThat(finished.get()).isGreaterThanOrEqualTo(1)
    }

    @Test
    fun `errorThreshold считается по стадии и сбрасывается между стадиями`() {
        val secondStageHandled = AtomicInteger()

        // Порог 2: первая стадия даёт ровно 2 skip'а и проходит; вторая начинает счёт заново.
        val ctx = run(errorThreshold = 2, plan = migration("M", "t") {
            source(name = "first", onItemError = ItemError.Skip, items = { (1..2).asSequence() }) {
                error("skip me")
            }
            source(name = "second", onItemError = ItemError.Skip, items = { (1..2).asSequence() }) {
                secondStageHandled.incrementAndGet()
                error("skip me too")
            }
        })

        assertThat(secondStageHandled.get()).isEqualTo(2)
        assertThat(ctx.report.build().skipped).isEqualTo(4)
    }

    @Test
    fun `превышение порога внутри стадии обрывает прогон`() {
        assertThatThrownBy {
            run(errorThreshold = 2, plan = migration("M", "t") {
                source(onItemError = ItemError.Skip, items = { (1..10).asSequence() }) {
                    error("always")
                }
            })
        }.isInstanceOf(ErrorThresholdExceeded::class.java)
    }

    @Test
    fun `progress тикает заданным шагом`() {
        val ticks = mutableListOf<String>()
        val ctx = RunContext.test()

        PlanInterpreter(ctx, progressSink = { ticks += it }).execute(migration("M", "t") {
            source(progress = Progress.Every(5), items = { (1..20).asSequence() }) { }
        })

        assertThat(ticks).hasSize(4)
    }

    @Test
    fun `Progress Off молчит`() {
        val ticks = mutableListOf<String>()
        val ctx = RunContext.test()

        PlanInterpreter(ctx, progressSink = { ticks += it }).execute(migration("M", "t") {
            source(progress = Progress.Off, items = { (1..20).asSequence() }) { }
        })

        assertThat(ticks).isEmpty()
    }
}
