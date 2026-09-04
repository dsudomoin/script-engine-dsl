package io.github.dsudomoin.migration.report

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

/**
 * Отчёт обязан показывать исходы эффектов: ради них движок их и считает. Без этих строк
 * прогон сообщает меньше, чем реально сделал.
 */
class EffectReportTest {

    private fun report(
        applied: Map<String, Long> = emptyMap(),
        rejected: Map<String, Long> = emptyMap(),
        acked: Long = 0,
        failedEffects: Long = 0,
        abandoned: Long = 0,
        late: Long = 0,
        rawPages: Long = 0,
        rawRows: Long = 0,
    ) = MigrationReport(
        name = "M",
        author = "t",
        startedAt = Instant.parse("2026-01-01T00:00:00Z"),
        finishedAt = Instant.parse("2026-01-01T00:00:05Z"),
        duration = Duration.ofSeconds(5),
        dryRun = false,
        processed = 10,
        successful = 10,
        skipped = 0,
        failed = 0,
        dryRunSkipped = emptyMap(),
        appliedWrites = applied,
        rejectedWrites = rejected,
        acknowledgedPublishes = acked,
        failedEffects = failedEffects,
        abandonedPublishes = abandoned,
        lateRegistered = late,
        rawPages = rawPages,
        rawRows = rawRows,
        errorsFile = null,
        tracesFile = null,
    )

    @Test
    fun `применённые и отклонённые записи видны раздельно с разбивкой по меткам`() {
        val text = ReportFormatter().format(
            report(
                applied = mapOf("acc.update" to 7, "acc.close" to 1),
                rejected = mapOf("acc.update" to 2),
            ),
        )

        assertThat(text).contains("Applied writes")
        assertThat(text).contains("acc.close: 1", "acc.update: 7")
        assertThat(text).contains("Rejected writes")
        assertThat(text).contains("acc.update: 2")
    }

    @Test
    fun `подтверждённые публикации видны, когда они есть`() {
        val text = ReportFormatter().format(report(acked = 1_234))

        assertThat(text).contains("Acknowledged publishes")
        assertThat(text).contains("1 234")
    }

    @Test
    fun `отказы и неподтверждённые эффекты показываются отдельными строками`() {
        val text = ReportFormatter().format(report(failedEffects = 3, abandoned = 2, late = 1))

        assertThat(text).contains("Failed effects")
        assertThat(text).contains("Unconfirmed effects")
    }

    @Test
    fun `сырые страницы показывают разницу между прочитанным и обработанным`() {
        val text = ReportFormatter().format(report(rawPages = 12, rawRows = 5_000))

        assertThat(text).contains("Source pages read")
        assertThat(text).contains("5 000")
    }

    @Test
    fun `чистый прогон не печатает нулевых строк про эффекты`() {
        val text = ReportFormatter().format(report())

        assertThat(text).doesNotContain("Applied writes")
        assertThat(text).doesNotContain("Acknowledged publishes")
        assertThat(text).doesNotContain("Failed effects")
        assertThat(text).doesNotContain("Unconfirmed effects")
        assertThat(text).doesNotContain("Source pages read")
    }

    @Test
    fun `asciiOnly не оставляет unicode в новых строках`() {
        val text = ReportFormatter(asciiOnly = true).format(
            report(applied = mapOf("a" to 1), acked = 1, failedEffects = 1, abandoned = 1, rawPages = 1, rawRows = 1),
        )

        assertThat(text).matches("\\A\\p{ASCII}*\\z")
    }
}
