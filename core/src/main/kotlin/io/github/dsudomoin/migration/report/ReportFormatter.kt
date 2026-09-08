package io.github.dsudomoin.migration.report

import io.github.dsudomoin.migration.internal.humanizeDuration
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Locale

/**
 * Рендерит [MigrationReport] в человекочитаемый текстовый блок, который runner пишет в лог
 * в конце прогона.
 *
 * Строки сверх обязательных счётчиков печатаются только когда им есть что показать:
 * постоянные нулевые строки приучили бы глаз их пропускать.
 *
 * @param asciiOnly заменить unicode-символы (`═`, `✓`, `⊘`, `⚠`) на ASCII-эквиваленты
 *                  (`=`, `[OK]`, `[SK]`, `[!]`). Полезно для CI без UTF-8 терминала.
 */
class ReportFormatter(private val asciiOnly: Boolean = false) {

    private val dt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss XXX").withZone(ZoneId.systemDefault())

    /** Отрендерить отчёт. Результат уже содержит `\n` — можно прямо в `log.info`. */
    fun format(r: MigrationReport): String {
        val head = if (asciiOnly) "=".repeat(59) else "═".repeat(59)
        val mid = if (asciiOnly) "-".repeat(59) else "─".repeat(59)
        val ok = if (asciiOnly) "[OK]" else "✓"
        val sk = if (asciiOnly) "[SK]" else "⊘"
        val fl = if (asciiOnly) "[FL]" else "✗"
        val warn = if (asciiOnly) "[!]" else "⚠"

        val unhandledLine = if (r.unhandledFailures > 0) {
            "  $fl Unhandled failures:      ${fmt(r.unhandledFailures)}\n"
        } else ""

        val sourceSkippedLine = if (r.sourceSkipped > 0) {
            "  $sk Source rows dropped:     ${fmt(r.sourceSkipped)}\n"
        } else ""

        val warningsBlock = if (r.warnings.isNotEmpty()) {
            val list = r.warnings.joinToString("\n") { "    - $it" }
            "  $warn Warnings:\n$list\n"
        } else ""

        val mode = if (r.dryRun) "DRY-RUN" else "REAL"

        return buildString {
            append("$head\n")
            append("Migration: ${r.name}\n")
            append("Started:   ${dt.format(r.startedAt)}\n")
            append("Finished:  ${dt.format(r.finishedAt)}\n")
            append("Duration:  ${humanizeDuration(r.duration)}\n")
            append("Mode:      $mode\n")
            append("$mid\n")
            append("Processed:                 ${fmt(r.processed)}\n")
            append("  $ok Successful:            ${fmt(r.successful)}\n")
            append("  $sk Skipped (errors):      ${fmt(r.skipped)}\n")
            append("  $fl Failed:                ${fmt(r.failed)}\n")
            append(unhandledLine)
            append(sourceSkippedLine)
            append(warningsBlock)
            append("$mid\n")
            if (r.errorsFile != null) append("Error details:  ${r.errorsFile}\n")
            if (r.tracesFile != null) append("Error traces:   ${r.tracesFile}\n")
            append("$head\n")
        }
    }

    private fun fmt(n: Long): String =
        if (n >= 1000) String.format(Locale.US, "%,d", n).replace(',', ' ') else n.toString()
}
