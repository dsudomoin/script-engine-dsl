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

        // Каждая строка ниже печатается только когда ей есть что показать: постоянные нулевые
        // строки приучили бы глаз их пропускать.
        fun breakdown(m: Map<String, Long>): String =
            // Сортировка по метке даёт детерминированный порядок run-to-run: копится всё в
            // ConcurrentHashMap, iteration order у которого нестабилен.
            m.entries.sortedBy { it.key }.joinToString(", ") { "${it.key}: ${it.value}" }

        val appliedLine = if (r.appliedWrites.isNotEmpty()) {
            "  $ok Applied writes:          ${fmt(r.appliedWrites.values.sum())}  (${breakdown(r.appliedWrites)})\n"
        } else ""

        val rejectedLine = if (r.rejectedWrites.isNotEmpty()) {
            "  $sk Rejected writes:         ${fmt(r.rejectedWrites.values.sum())}  (${breakdown(r.rejectedWrites)})\n"
        } else ""

        val ackedLine = if (r.acknowledgedPublishes > 0) {
            "  $ok Acknowledged publishes:  ${fmt(r.acknowledgedPublishes)}\n"
        } else ""

        val failedEffectsLine = if (r.failedEffects > 0) {
            "  $fl Failed effects:          ${fmt(r.failedEffects)}\n"
        } else ""

        // Отправлено, но не подтверждено: по таймауту барьера либо зарегистрировано после него.
        // Это не «успех» и не «отказ» — исход неизвестен, и молча исчезать он не имеет права.
        val unconfirmed = r.abandonedPublishes + r.lateRegistered
        val unconfirmedLine = if (unconfirmed > 0) {
            "  $warn Unconfirmed effects:     ${fmt(unconfirmed)}" +
                "  (abandoned: ${r.abandonedPublishes}, late: ${r.lateRegistered})\n"
        } else ""

        val sourceSkippedLine = if (r.sourceSkipped > 0) {
            "  $sk Source rows dropped:     ${fmt(r.sourceSkipped)}\n"
        } else ""

        val rawLine = if (r.rawPages > 0) {
            "  $dr Source pages read:       ${fmt(r.rawPages)}  (rows: ${fmt(r.rawRows)})\n"
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
            append(appliedLine)
            append(rejectedLine)
            append(ackedLine)
            append(failedEffectsLine)
            append(unconfirmedLine)
            append(sourceSkippedLine)
            append(rawLine)
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
