package io.github.dsudomoin.migration.report

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationScope
import io.github.dsudomoin.migration.MigrationTest
import io.github.dsudomoin.migration.csv.readCsv
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/**
 * Тождество `processed = successful + skipped + failed` — единственное арифметическое
 * обещание отчёта. Строки, отброшенные при чтении источника, и отказы уровня прогона
 * в него не входят намеренно: ни то, ни другое не является исходом обработанного элемента.
 */
class CountersIdentityTest {

    @TempDir
    lateinit var tmp: Path

    @Test
    fun `processed равно successful плюс skipped плюс failed`() {
        val migration = object : Migration("IDENTITY") {
            override fun MigrationScope.run() {
                each((1..10).toList(), onItemError = ItemError.Skip) {
                    if (it % 3 == 0) error("boom")
                }
            }
        }

        val report = MigrationTest.run(migration, outputFolder = tmp).report

        assertThat(report.processed).isEqualTo(report.successful + report.skipped + report.failed)
        assertThat(report.processed).isEqualTo(10)
        assertThat(report.skipped).isEqualTo(3)
        assertThat(report.successful).isEqualTo(7)
    }

    @Test
    fun `терминальный отказ элемента считается ровно один раз`() {
        val migration = object : Migration("SINGLE-FAIL") {
            override fun MigrationScope.run() {
                each((1..5).toList()) { if (it == 3) error("boom") }
            }
        }

        val report = MigrationTest.run(migration, outputFolder = tmp).report

        // Элемент учтён в failed, а падение всего прогона — отдельно в unhandledFailures.
        assertThat(report.failed).isEqualTo(1)
        assertThat(report.unhandledFailures).isEqualTo(1)
        assertThat(report.processed).isEqualTo(report.successful + report.skipped + report.failed)
    }

    @Test
    fun `отброшенные строки источника не входят в тождество`() {
        val file = tmp.resolve("in.csv")
        Files.writeString(file, "id,spend\n1,10\n2,не-число\n3,30\n")
        val migration = object : Migration("SOURCE-SKIP") {
            override fun MigrationScope.run() {
                val rows = readCsv(file.toString(), onRowError = ItemError.Skip) {
                    it.getValue("spend").toLong()
                }
                each(rows) { }
            }
        }

        val report = MigrationTest.run(migration, outputFolder = tmp.resolve("out")).report

        assertThat(report.sourceSkipped).isEqualTo(1)
        assertThat(report.processed).isEqualTo(2)
        assertThat(report.skipped).describedAs("строка источника — не item-skip").isZero()
        assertThat(report.processed).isEqualTo(report.successful + report.skipped + report.failed)
    }
}
