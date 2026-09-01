package io.github.dsudomoin.migration.report

import io.github.dsudomoin.migration.internal.humanizeDuration
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.*

/**
 * Рендерит [MigrationReport] в человекочитаемый текстовый блок, который runner пишет в лог
 * в конце прогона.
 *
 * Структура отчёта:
 * - Header: имя/автор, времена, длительность, mode (REAL/DRY-RUN).
 * - Счётчики: Processed / Successful / Skipped / Failed.
 * - Warnings-блок (если есть нефатальные ошибки close, etc).
 * - Dry-run breakdown (если в режиме dry-run).
 * - Пути к `errors.csv` / `errors.log` (если миграция шла с авто-аудитором).
 *
 * @param asciiOnly заменить unicode-символы (`═`, `✓`, `⊘`, `⚠`, ...) на ASCII-эквиваленты
 *                  (`=`, `[OK]`, `[SK]`, `[!]`, ...). Полезно для CI/среды без UTF-8 терминала.
 */
class ReportFormatter(private val asciiOnly: Boolean = false) {

    private val dt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss XXX").withZone(ZoneId.systemDefault())

    /** Отрендерить отчёт. Результат уже содержит `\n`-разделители — можно прямо передавать в `log.info`. */
    fun format(r: MigrationReport): String {
        val head = if (asciiOnly) "=".repeat(59) else "═".repeat(59)
        val mid = if (asciiOnly) "-".repeat(59) else "─".repeat(59)
        val ok = if (asciiOnly) "[OK]" else "✓"
        val sk = if (asciiOnly) "[SK]" else "⊘"
        val fl = if (asciiOnly) "[FL]" else "✗"
        val dr = if (asciiOnly) "[--]" else "⌀"
        val warn = if (asciiOnly) "[!]" else "⚠"

        // Печатаем строку только когда отказы есть: в норме её быть не должно, и постоянная
        // нулевая строка приучила бы глаз её пропускать.
        val asyncLine = if (r.asyncFailed > 0) {
            "  $fl Async delivery failed:  ${fmt(r.asyncFailed)}\n"
        } else ""

        val warningsBlock = if (r.warnings.isNotEmpty()) {
            val list = r.warnings.joinToString("\n") { "    - $it" }
            "  $warn Warnings:\n$list\n"
        } else ""

        val dryLine = if (r.dryRunSkipped.isNotEmpty()) {
            // Сортируем по label, чтобы порядок в отчёте был детерминированным run-to-run:
            // ReportBuilder копит в ConcurrentHashMap, iteration order у которого нестабилен.
            val breakdown = r.dryRunSkipped.entries.sortedBy { it.key }
                .joinToString(", ") { "${it.key}: ${it.value}" }
            "  $dr Dry-run skipped writes:    ($breakdown)\n"
        } else ""

        val mode = if (r.dryRun) "DRY-RUN" else "REAL"

        return buildString {
            append("$head\n")
            append("Migration: ${r.name}  (author: ${r.author})\n")
            append("Started:   ${dt.format(r.startedAt)}\n")
            append("Finished:  ${dt.format(r.finishedAt)}\n")
            append("Duration:  ${humanizeDuration(r.duration)}\n")
            append("Mode:      $mode\n")
            append("$mid\n")
            append("Processed:                 ${fmt(r.processed)}\n")
            append("  $ok Successful:            ${fmt(r.successful)}\n")
            append("  $sk Skipped (errors):      ${fmt(r.skipped)}\n")
            append("  $fl Failed:                ${fmt(r.failed)}\n")
            append(asyncLine)
            append(warningsBlock)
            append(dryLine)
            append("$mid\n")
            if (r.errorsFile != null) append("Error details:  ${r.errorsFile}\n")
            if (r.tracesFile != null) append("Error traces:   ${r.tracesFile}\n")
            append("$head\n")
        }
    }

    private fun fmt(n: Long): String {
        return if (n >= 1000) String.format(Locale.US, "%,d", n).replace(',', ' ') else n.toString()
    }
}
