package io.github.dsudomoin.migration.report

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.csv.readCsv
import io.github.dsudomoin.migration.internal.PlanInterpreter
import io.github.dsudomoin.migration.internal.RunContext
import io.github.dsudomoin.migration.migration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/**
 * Тождество `processed = successful + skipped + failed` — единственное, что позволяет читать
 * отчёт как баланс. Строки, отброшенные при чтении источника, до стадии не доходят и потому
 * не являются item-skip'ами.
 */
class CountersIdentityTest {

    @TempDir
    lateinit var dir: Path

    private fun csvWithTwoBrokenRows(): Path {
        val f = dir.resolve("customers.csv")
        Files.writeString(f, "id,spend\n1,100\n2,NOT_A_NUMBER\n3,300\n4,\n")
        return f
    }

    @Test
    fun `отброшенные строки источника не попадают в item-skip и не ломают баланс`() {
        val ctx = RunContext.test(outputFolder = dir.resolve("out"))
        val f = csvWithTwoBrokenRows()

        PlanInterpreter(ctx).execute(migration("M", "t") {
            source(items = { readCsv(f, onRowError = ItemError.Skip) { it.getValue("spend").toLong() } }) { }
        })

        val report = ctx.report.build()
        assertThat(report.processed).isEqualTo(2)
        assertThat(report.successful).isEqualTo(2)
        assertThat(report.skipped).isZero()
        assertThat(report.sourceSkipped)
            .describedAs("битые строки видны, но отдельно от item-skip'ов")
            .isEqualTo(2)
        assertThat(report.processed)
            .isEqualTo(report.successful + report.skipped + report.failed)
    }

    @Test
    fun `item-skip и отброшенные строки считаются раздельно`() {
        val ctx = RunContext.test(outputFolder = dir.resolve("out"))
        val f = csvWithTwoBrokenRows()

        PlanInterpreter(ctx).execute(migration("M", "t") {
            source(
                onItemError = ItemError.Skip,
                items = { readCsv(f, onRowError = ItemError.Skip) { it.getValue("spend").toLong() } },
            ) { spend ->
                if (spend == 100L) error("item rejected")
            }
        })

        val report = ctx.report.build()
        assertThat(report.processed).isEqualTo(2)
        assertThat(report.successful).isEqualTo(1)
        assertThat(report.skipped).isEqualTo(1)
        assertThat(report.sourceSkipped).isEqualTo(2)
        assertThat(report.processed)
            .isEqualTo(report.successful + report.skipped + report.failed)
    }

    @Test
    fun `отброшенные строки видны в отчёте отдельной строкой`() {
        val ctx = RunContext.test(outputFolder = dir.resolve("out"))
        val f = csvWithTwoBrokenRows()

        PlanInterpreter(ctx).execute(migration("M", "t") {
            source(items = { readCsv(f, onRowError = ItemError.Skip) { it.getValue("spend").toLong() } }) { }
        })

        val text = ReportFormatter().format(ctx.report.build())
        assertThat(text).contains("Source rows dropped")
    }
}
