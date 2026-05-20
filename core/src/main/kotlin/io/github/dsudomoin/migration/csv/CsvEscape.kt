package io.github.dsudomoin.migration.csv

/**
 * Квотинг CSV-cell'а по RFC 4180. Если строка содержит запятую, double-quote, `\n` или `\r` —
 * оборачиваем в `"..."` и удваиваем внутренние `"` → `""`. Иначе — возвращаем как есть.
 *
 * Используется и в `CsvOutput.row(...)` (пользовательские CSV через `openCsv`), и в
 * `CsvFileErrorReporter` (авто-аудитор ошибок). Логика была дублирована в обоих местах —
 * вынесена в общий internal helper.
 */
internal fun csvEscape(s: String): String {
    val needs = s.any { it == ',' || it == '"' || it == '\n' || it == '\r' }
    return if (needs) "\"" + s.replace("\"", "\"\"") + "\"" else s
}
