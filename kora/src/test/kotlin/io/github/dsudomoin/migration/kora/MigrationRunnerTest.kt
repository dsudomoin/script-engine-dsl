package io.github.dsudomoin.migration.kora

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationScope
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

class MigrationRunnerTest {

    @TempDir
    lateinit var tmp: Path

    private class Ok : Migration("OK-001") {
        override fun MigrationScope.run() {
            each(listOf(1, 2, 3)) { }
        }
    }

    private class Boom : Migration("BOOM-001") {
        override fun MigrationScope.run() {
            error("boom")
        }
    }

    private class OverThreshold : Migration("THRESHOLD-001") {
        override fun MigrationScope.run() {
            each((1..10).toList(), onItemError = ItemError.Skip, errorThreshold = 2) {
                error("boom")
            }
        }
    }

    private fun config(run: String?) = MigrationConfigValues(
        run = run,
        dryRun = false,
        outputFolder = tmp.toString(),
    )

    private fun runWith(
        run: String?,
        migrations: List<Migration>,
        onReport: (io.github.dsudomoin.migration.report.MigrationReport) -> Unit = {},
    ): List<Int> {
        val codes = mutableListOf<Int>()
        MigrationRunner(config(run), migrations, onReport) { codes += it }.init()
        return codes
    }

    @Test
    fun `без migration_run runner простаивает и не зовёт exit`() {
        assertThat(runWith(null, listOf(Ok()))).isEmpty()
    }

    @Test
    fun `успешный прогон даёт 0`() {
        assertThat(runWith("OK-001", listOf(Ok()))).containsExactly(0)
    }

    @Test
    fun `упавшая миграция даёт 1`() {
        assertThat(runWith("BOOM-001", listOf(Boom()))).containsExactly(1)
    }

    @Test
    fun `превышенный порог ошибок даёт 1`() {
        assertThat(runWith("THRESHOLD-001", listOf(OverThreshold()))).containsExactly(1)
    }

    @Test
    fun `неизвестное имя даёт 2`() {
        assertThat(runWith("NOPE", listOf(Ok()))).containsExactly(2)
    }

    @Test
    fun `дублирующиеся имена дают 2`() {
        assertThat(runWith("OK-001", listOf(Ok(), Ok()))).containsExactly(2)
    }

    @Test
    fun `отчёт попадает в onReport`() {
        var processed = -1L
        runWith("OK-001", listOf(Ok())) { processed = it.processed }
        assertThat(processed).isEqualTo(3)
    }

    @Test
    fun `артефакты прогона ложатся в outputFolder`() {
        runWith("OK-001", listOf(Ok()))
        assertThat(Files.exists(tmp.resolve("migration.log"))).isTrue()
    }
}
