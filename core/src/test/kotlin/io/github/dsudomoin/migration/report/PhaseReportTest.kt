package io.github.dsudomoin.migration.report

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.WriteOutcome
import io.github.dsudomoin.migration.internal.PlanInterpreter
import io.github.dsudomoin.migration.internal.RunContext
import io.github.dsudomoin.migration.migration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class PhaseReportTest {

    @TempDir
    lateinit var folder: Path

    private fun ctx() = RunContext.test(outputFolder = folder)

    @Test
    fun `две фазы дают раздельные счётчики и корректную сумму`() {
        val ctx = ctx()
        val plan = migration("TWO", "t") {
            source(name = "copy portfolios", items = { sequenceOf(1, 2, 3) }) {
                write("db.update") { WriteOutcome.Applied }
            }
            source(name = "copy dividends", onItemError = ItemError.Skip, items = { sequenceOf(1, 2) }) {
                error("nope")
            }
        }
        PlanInterpreter(ctx).execute(plan)
        val report = ctx.report.build()

        assertThat(report.phases.map { it.name }).containsExactly("copy portfolios", "copy dividends")

        val first = report.phases[0]
        assertThat(first.processed).isEqualTo(3)
        assertThat(first.successful).isEqualTo(3)
        assertThat(first.appliedWrites["db.update"]).isEqualTo(3)

        val second = report.phases[1]
        assertThat(second.processed).isEqualTo(2)
        assertThat(second.skipped).isEqualTo(2)
        assertThat(second.successful).isZero()
        assertThat(second.appliedWrites).isEmpty()

        assertThat(report.processed).isEqualTo(5)
        assertThat(report.processed).isEqualTo(report.successful + report.skipped + report.failed)
        assertThat(report.phases.sumOf { it.processed }).isEqualTo(report.processed)
    }

    @Test
    fun `одна неявная фаза отдельной секцией не показывается`() {
        val ctx = ctx()
        PlanInterpreter(ctx).execute(
            migration("ONE", "t") { source(items = { sequenceOf(1) }) { } },
        )
        assertThat(ctx.report.build().phases).isEmpty()
    }

    @Test
    fun `scoped-фаза учитывает всех родителей как одну фазу`() {
        val ctx = ctx()
        val plan = migration("SC", "t") {
            source(name = "warmup", items = { sequenceOf(1) }) { }
            scoped(
                name = "per strategy",
                parents = { sequenceOf("a", "b") },
                items = { _ -> sequenceOf(1, 2) },
            ) { }
        }
        PlanInterpreter(ctx).execute(plan)
        val phases = ctx.report.build().phases

        assertThat(phases.map { it.name }).containsExactly("warmup", "per strategy")
        assertThat(phases[1].processed).describedAs("2 родителя × 2 элемента").isEqualTo(4)
    }

    @Test
    fun `run-failure не попадает в failed элементов`() {
        val builder = ReportBuilder("R", "t", false)
        builder.incProcessed()
        builder.incSuccessful()
        builder.recordRunFailure()
        val report = builder.build()

        assertThat(report.failed).isZero()
        assertThat(report.unhandledFailures).isEqualTo(1)
        assertThat(report.processed).isEqualTo(report.successful + report.skipped + report.failed)
    }

    @Test
    fun `effectFailureCount включает отказы доставки, а не только неподтверждённые`() {
        val builder = ReportBuilder("R", "t", false)
        builder.addBarrierOutcome(acked = 1, failed = 2, abandoned = 3, late = 4)
        assertThat(builder.effectFailureCount()).isEqualTo(9)
    }

    @Test
    fun `счётчики вне фазы не приписываются соседней фазе`() {
        val builder = ReportBuilder("R", "t", false)
        builder.incDryRunSkipped("before.phase")
        builder.beginPhase("one")
        builder.incProcessed()
        builder.endPhase()
        builder.beginPhase("two")
        builder.incProcessed()
        builder.endPhase()
        builder.incDryRunSkipped("after.phase")

        val report = builder.build()
        assertThat(report.phases.map { it.name }).containsExactly("one", "two")
        assertThat(report.phases.flatMap { it.dryRunSkipped.keys }).isEmpty()
        assertThat(report.dryRunSkipped.keys).containsExactlyInAnyOrder("before.phase", "after.phase")
    }
}
