package io.github.dsudomoin.migration.report

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

class ReportFormatterTest {
    private fun sample(dryRun: Boolean = false, warnings: List<String> = emptyList()) = MigrationReport(
        name = "SAMPLE-001", author = "team",
        startedAt = Instant.parse("2026-04-23T10:00:00Z"),
        finishedAt = Instant.parse("2026-04-23T10:07:15Z"),
        duration = Duration.ofSeconds(435),
        dryRun = dryRun,
        processed = 12543, successful = 12301,
        skipped = 42, failed = 0,
        dryRunSkipped = if (dryRun) mapOf("jdbc.execute" to 98L, "kafka.publish" to 12543L) else emptyMap(),
        errorsFile = null, tracesFile = null,
        warnings = warnings,
    )

    @Test
    fun `unicode формат содержит все секции`() {
        val text = ReportFormatter(asciiOnly = false).format(sample())
        assertThat(text)
            .contains("SAMPLE-001", "author: team", "Duration:  7m 15s")
            .contains("Processed:", "12 543")
            .contains("Successful", "12 301")
            .contains("Skipped", "42")
    }

    @Test
    fun `dryRun секция показывает разбивку по labels`() {
        val text = ReportFormatter(asciiOnly = false).format(sample(dryRun = true))
        assertThat(text).contains("Mode:      DRY-RUN", "jdbc.execute: 98", "kafka.publish: 12543")
    }

    @Test
    fun `asciiOnly - заменяет unicode на ASCII эквиваленты`() {
        val text = ReportFormatter(asciiOnly = true).format(sample())
        assertThat(text).doesNotContain("═", "✓", "↻", "⊘", "✗", "⌀")
        assertThat(text).contains("[OK]", "[SK]", "=")
    }

    @Test
    fun `warnings block - unicode с маркером и буллет-листом`() {
        val text = ReportFormatter(asciiOnly = false).format(
            sample(warnings = listOf("close failed: A (IOException: disk full)", "close failed: B (RuntimeException: oops)"))
        )
        assertThat(text)
            .contains("⚠ Warnings:")
            .contains("    - close failed: A (IOException: disk full)")
            .contains("    - close failed: B (RuntimeException: oops)")
    }

    @Test
    fun `warnings block - asciiOnly показывает - 1 - маркер`() {
        val text = ReportFormatter(asciiOnly = true).format(
            sample(warnings = listOf("close failed: A"))
        )
        assertThat(text)
            .contains("[!] Warnings:")
            .contains("    - close failed: A")
            .doesNotContain("⚠")
    }

    @Test
    fun `warnings block - отсутствует когда warnings пуст`() {
        val text = ReportFormatter(asciiOnly = false).format(sample())
        assertThat(text).doesNotContain("Warnings:", "⚠", "[!]")
    }

    @Test
    fun `многофазный отчёт печатает секцию по каждой фазе`() {
        val builder = ReportBuilder("M", "t", false)
        builder.beginPhase("copy portfolios")
        repeat(3) { builder.incProcessed(); builder.incSuccessful(); builder.incAppliedWrite("db.update") }
        builder.endPhase()
        builder.beginPhase("copy dividends")
        builder.incProcessed()
        builder.incSkipped()
        builder.addBarrierOutcome(acked = 5, failed = 1, abandoned = 0, late = 0)
        builder.endPhase()

        val text = ReportFormatter(asciiOnly = true).format(builder.build())

        assertThat(text).contains("Phases:")
        assertThat(text).containsPattern("copy portfolios[\\s\\S]*?processed 3, ok 3")
        assertThat(text).containsPattern("copy portfolios[\\s\\S]*?applied db.update: 3")
        assertThat(text).containsPattern("copy dividends[\\s\\S]*?skipped 1")
        assertThat(text).containsPattern("copy dividends[\\s\\S]*?acked 5")
        assertThat(text).containsPattern("copy dividends[\\s\\S]*?failed effects 1")
    }

    @Test
    fun `однофазный отчёт секции фаз не печатает`() {
        val builder = ReportBuilder("M", "t", false)
        builder.beginPhase("phase-1")
        builder.incProcessed()
        builder.incSuccessful()
        builder.endPhase()

        assertThat(ReportFormatter(asciiOnly = true).format(builder.build())).doesNotContain("Phases:")
    }

    @Test
    fun `unhandled failures печатаются отдельной строкой от failed`() {
        val builder = ReportBuilder("M", "t", false)
        builder.recordRunFailure()
        val text = ReportFormatter(asciiOnly = true).format(builder.build())

        assertThat(text).contains("Unhandled failures:")
        assertThat(text).containsPattern("Failed:\\s+0")
    }
}
