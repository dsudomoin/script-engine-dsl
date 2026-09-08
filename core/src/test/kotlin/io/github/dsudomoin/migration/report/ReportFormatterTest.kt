package io.github.dsudomoin.migration.report

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.time.Duration
import java.time.Instant

class ReportFormatterTest {

    private fun report(
        dryRun: Boolean = false,
        sourceSkipped: Long = 0,
        unhandledFailures: Long = 0,
        warnings: List<String> = emptyList(),
        errorsFile: Path? = null,
        tracesFile: Path? = null,
    ): MigrationReport {
        val started = Instant.parse("2026-09-08T10:00:00Z")
        return MigrationReport(
            name = "SAMPLE-001",
            startedAt = started,
            finishedAt = started.plusSeconds(74),
            duration = Duration.ofSeconds(74),
            dryRun = dryRun,
            processed = 10_000,
            successful = 9_998,
            skipped = 2,
            failed = 0,
            sourceSkipped = sourceSkipped,
            unhandledFailures = unhandledFailures,
            errorsFile = errorsFile,
            tracesFile = tracesFile,
            warnings = warnings,
        )
    }

    @Test
    fun `шапка и счётчики попадают в отчёт`() {
        val text = ReportFormatter().format(report())

        assertThat(text).contains("Migration: SAMPLE-001")
        assertThat(text).contains("Mode:      REAL")
        assertThat(text).contains("Processed:                 10 000")
        assertThat(text).contains("Successful:            9 998")
        assertThat(text).contains("Skipped (errors):      2")
    }

    @Test
    fun `dry-run отмечен в шапке`() {
        assertThat(ReportFormatter().format(report(dryRun = true))).contains("Mode:      DRY-RUN")
    }

    @Test
    fun `нулевые необязательные строки не печатаются`() {
        val text = ReportFormatter().format(report())

        assertThat(text).doesNotContain("Source rows dropped")
        assertThat(text).doesNotContain("Unhandled failures")
        assertThat(text).doesNotContain("Warnings")
        assertThat(text).doesNotContain("Error details")
    }

    @Test
    fun `ненулевые строки появляются`() {
        val text = ReportFormatter().format(
            report(
                sourceSkipped = 3,
                unhandledFailures = 1,
                warnings = listOf("pool did not terminate"),
                errorsFile = Path.of("logs/errors.csv"),
                tracesFile = Path.of("logs/errors.log"),
            ),
        )

        assertThat(text).contains("Source rows dropped:     3")
        assertThat(text).contains("Unhandled failures:      1")
        assertThat(text).contains("- pool did not terminate")
        assertThat(text).contains("Error details:  logs/errors.csv")
        assertThat(text).contains("Error traces:   logs/errors.log")
    }

    @Test
    fun `asciiOnly убирает unicode-глифы`() {
        val text = ReportFormatter(asciiOnly = true).format(report(warnings = listOf("w")))

        assertThat(text).contains("[OK]").contains("[SK]").contains("[FL]").contains("[!]")
        assertThat(text).doesNotContain("✓").doesNotContain("⊘").doesNotContain("✗").doesNotContain("═")
    }
}
