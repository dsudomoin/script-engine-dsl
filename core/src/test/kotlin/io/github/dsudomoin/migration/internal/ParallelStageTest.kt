package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.error.CsvFileErrorReporter
import io.github.dsudomoin.migration.migration
import io.github.dsudomoin.migration.report.ReportBuilder
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger

class ParallelStageTest {

    @TempDir
    lateinit var folder: Path

    private fun ctx(executor: Executor): RunContext = RunContext.internalCreate(
        dryRun = false,
        name = "parallel-test",
        report = ReportBuilder("parallel-test", "t", false),
        executor = executor,
        outputFolder = folder,
        errors = CsvFileErrorReporter(
            "parallel-test", "t",
            folder.resolve("errors.csv"), folder.resolve("errors.log"),
        ),
    )

    @Test
    fun `отказ executor не подвешивает стадию и становится ошибкой стадии`() {
        val rejecting = Executor { throw RejectedExecutionException("pool is full") }
        val plan = migration("REJECT", "t") {
            source(parallel = 2, items = { sequenceOf(1, 2, 3) }) { }
        }

        assertTimeoutPreemptively<Unit>(Duration.ofSeconds(10)) {
            assertThatThrownBy { PlanInterpreter(ctx(rejecting)).execute(plan) }
                .isInstanceOf(RejectedExecutionException::class.java)
                .hasMessage("pool is full")
        }
    }

    @Test
    fun `уже запущенные воркеры доводятся до конца после отказа executor`() {
        val pool = Executors.newFixedThreadPool(2)
        val finished = AtomicInteger()
        val submissions = AtomicInteger()
        // Первая задача уходит в пул, остальные отвергаются: воркер первой обязан доиграть,
        // а не быть брошенным вместе со стадией.
        val flaky = Executor { r ->
            if (submissions.incrementAndGet() == 1) pool.execute(r) else throw RejectedExecutionException("full")
        }
        val plan = migration("PARTIAL", "t") {
            source(parallel = 2, items = { sequenceOf(1, 2, 3) }) {
                Thread.sleep(300)
                finished.incrementAndGet()
            }
        }

        try {
            assertTimeoutPreemptively<Unit>(Duration.ofSeconds(10)) {
                assertThatThrownBy { PlanInterpreter(ctx(flaky)).execute(plan) }
                    .isInstanceOf(RejectedExecutionException::class.java)
            }
            assertThat(finished.get()).describedAs("запущенный воркер обязан доиграть").isEqualTo(1)
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `исправный параллельный прогон обрабатывает все элементы`() {
        val pool = Executors.newFixedThreadPool(4)
        val seen = AtomicInteger()
        val plan = migration("OK", "t") {
            source(parallel = 4, items = { (1..50).asSequence() }) { seen.incrementAndGet() }
        }

        try {
            val ctx = ctx(pool)
            PlanInterpreter(ctx).execute(plan)
            assertThat(seen.get()).isEqualTo(50)
            assertThat(ctx.report.build().successful).isEqualTo(50)
        } finally {
            pool.shutdownNow()
        }
    }
}
