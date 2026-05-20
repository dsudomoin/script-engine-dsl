package io.github.dsudomoin.migration.internal

import java.time.Duration

/**
 * Человекочитаемый рендер длительности: `5s`, `7m 15s`, `1h 23m 45s`. Hours-bucket важен — без
 * него многочасовая миграция показывается как `420m15s`, что неудобно читать.
 *
 * Используется и в `ReportFormatter` (финальный отчёт), и в `ProgressTicker` (in-progress лог).
 * До v0.2.x было два почти-идентичных хелпера, причём в `ProgressTicker` не было hours-bucket
 * (`humanize` пропускал часы) — это сознательно вынесено в общий helper.
 */
internal fun humanizeDuration(d: Duration): String {
    val s = d.seconds
    return when {
        s >= 3600 -> "${s / 3600}h ${(s % 3600) / 60}m ${s % 60}s"
        s >= 60 -> "${s / 60}m ${s % 60}s"
        else -> "${s}s"
    }
}
