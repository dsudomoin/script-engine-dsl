package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.error.includeItem
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

class MigrationExecutionTest {

    @TempDir
    lateinit var tmp: Path

    @Test
    fun `успешный прогон возвращает отчёт без ошибки`() {
        val migration = object : Migration("OK-001") {
            override fun MigrationScope.run() {
                each(listOf(1, 2, 3)) { }
            }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp)

        assertThat(outcome.failure).isNull()
        assertThat(outcome.report.name).isEqualTo("OK-001")
        assertThat(outcome.report.processed).isEqualTo(3)
        assertThat(outcome.report.successful).isEqualTo(3)
        assertThat(outcome.report.duration).isNotNull()
    }

    @Test
    fun `ошибка тела попадает в outcome, а не летит наружу`() {
        val migration = object : Migration("FAIL-001") {
            override fun MigrationScope.run() {
                error("всё пропало")
            }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp)

        assertThat(outcome.failure).isInstanceOf(IllegalStateException::class.java)
        assertThat(outcome.failure).hasMessage("всё пропало")
        assertThat(outcome.report.unhandledFailures).isEqualTo(1)
    }

    @Test
    fun `упавший элемент уезжает в errors_csv`() {
        val migration = object : Migration("AUDIT-001") {
            override fun MigrationScope.run() {
                errors.includeItem<Int> { "item=$it" }
                each(listOf(1, 2, 3), onItemError = ItemError.Skip) {
                    if (it == 2) error("boom")
                }
            }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp)

        assertThat(outcome.report.errorsFile).isNotNull()
        val audit = Files.readString(tmp.resolve("errors.csv"))
        assertThat(audit).contains("item=2")
    }

    @Test
    fun `dryRun виден в теле и в отчёте`() {
        var seen: Boolean? = null
        val migration = object : Migration("DRY-001") {
            override fun MigrationScope.run() {
                seen = dryRun
            }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp, dryRun = true)

        assertThat(seen).isTrue()
        assertThat(outcome.report.dryRun).isTrue()
    }

    @Test
    fun `артефакты прошлого прогона удаляются на старте`() {
        val stale = tmp.resolve("errors.csv")
        Files.writeString(stale, "stale\n")
        val migration = object : Migration("CLEAN-001") {
            override fun MigrationScope.run() { }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp)

        assertThat(Files.exists(stale)).isFalse()
        assertThat(outcome.report.errorsFile).isNull()
    }

    @Test
    fun `пул закрывается после параллельного прогона`() {
        val migration = object : Migration("POOL-001") {
            override fun MigrationScope.run() {
                each((1..20).toList(), parallel = 3) { }
            }
        }

        MigrationTest.run(migration, outputFolder = tmp)

        // Потоки пула — демоны с именем migration-POOL-001-N; после закрытия прогона
        // живых оставаться не должно.
        val alive = Thread.getAllStackTraces().keys.filter { it.name.startsWith("migration-POOL-001-") && it.isAlive }
        assertThat(alive).isEmpty()
    }
}
