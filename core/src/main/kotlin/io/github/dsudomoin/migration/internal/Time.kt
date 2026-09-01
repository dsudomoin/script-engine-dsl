package io.github.dsudomoin.migration.internal

import java.time.Duration

/**
 * Человекочитаемый рендер длительности: `5s`, `7m 15s`, `1h 23m 45s`. Hours-bucket важен — без
 * него многочасовая миграция показывается как `420m15s`, что неудобно читать.
 *
 * Общий для `ReportFormatter` (финальный отчёт) и `ProgressTicker` (in-progress лог): формат
 * длительности обязан совпадать в обоих, иначе один и тот же прогон читается по-разному.
 */
internal fun humanizeDuration(d: Duration): String {
    val s = d.seconds
    return when {
        s >= 3600 -> "${s / 3600}h ${(s % 3600) / 60}m ${s % 60}s"
        s >= 60 -> "${s / 60}m ${s % 60}s"
        else -> "${s}s"
    }
}
