package io.github.dsudomoin.migration.csv

import io.github.dsudomoin.migration.MigrationPrecondition

/**
 * Одна строка входного файла. Доступ к ячейкам — по имени колонки, без учёта регистра
 * и способа записи: `spend_amount`, `SPEND_AMOUNT` и `spendAmount` — одна и та же колонка.
 *
 * В `errors.csv` строка попадает через [toString]; если в колонках есть чувствительные данные,
 * сократи представление: `errors.includeItem<CsvRow> { "id=" + it["id"] }`.
 */
class CsvRow internal constructor(
    /**
     * Номер записи, считая заголовок первой строкой: первая запись — 2, вторая — 3.
     *
     * Совпадает с номером строки в текстовом редакторе, пока ни одно значение не содержит
     * перевода строки внутри кавычек — такое значение остаётся одной записью.
     */
    val lineNumber: Long,
    private val header: CsvHeader,
    private val values: List<String>,
) {

    /** Имена колонок файла в исходном виде (без окружающих пробелов). */
    val columns: List<String> get() = header.names

    /**
     * Значение ячейки. Если такой колонки в файле нет — [CsvStructureException] со списком
     * того, что в файле есть: без этого опечатка в имени отлаживается вслепую.
     *
     * Отсутствующая ячейка у короткой строки — это пустая строка, а не ошибка: выгрузки
     * регулярно обрывают хвостовые разделители.
     */
    operator fun get(column: String): String =
        getOrNull(column) ?: throw CsvStructureException(
            "no column '$column' in CSV at line $lineNumber; columns: ${header.names.joinToString(", ")}",
        )

    /** Значение ячейки или `null`, если такой колонки нет в файле. */
    fun getOrNull(column: String): String? {
        val idx = header.indexOf(column) ?: return null
        return values.getOrElse(idx) { "" }
    }

    /** Как колонка названа в самом файле — чтобы в тексте ошибки указать на неё, а не на поле DTO. */
    internal fun columnName(column: String): String? = header.indexOf(column)?.let { header.names[it] }

    /** Строка всех значений с номером строки — то, что увидит человек в `errors.csv`. */
    override fun toString(): String =
        header.names.indices.joinToString(", ", prefix = "line $lineNumber: ") { i ->
            "${header.names[i]}=${values.getOrElse(i) { "" }}"
        }
}

/**
 * Файл не той формы, какую ждёт миграция: нет колонки, имена колонок неразличимы.
 *
 * Отдельный тип, а не голый [IllegalArgumentException], потому что такую ошибку `readCsv`
 * пробрасывает мимо `onRowError`: она одинаково сломает каждую следующую строку, и
 * `ItemError.Skip` превратил бы прогон в тихий пустой. `require(...)` в теле маппера
 * бросает обычный [IllegalArgumentException] и политике по-прежнему подчиняется.
 */
class CsvStructureException internal constructor(message: String) :
    IllegalArgumentException(message), MigrationPrecondition

/**
 * Заголовок файла: исходные имена плюс индекс для поиска, нечувствительного к регистру
 * и к способу записи составных имён.
 */
internal class CsvHeader(rawNames: List<String>) {

    val names: List<String> = rawNames.map { it.trim() }

    private val index: Map<String, Int> = buildIndex()

    fun indexOf(column: String): Int? = index[lookupKey(column)]

    private fun buildIndex(): Map<String, Int> {
        val result = HashMap<String, Int>(names.size)
        val collisions = mutableListOf<Pair<String, String>>()
        names.forEachIndexed { i, name ->
            val key = lookupKey(name)
            val previous = result.put(key, i)
            if (previous != null) collisions += names[previous] to name
        }
        // Две колонки, схлопнувшиеся в одно имя, — это неразрешимая двусмысленность: молча
        // выбрав одну, мы бы тихо читали не те данные.
        if (collisions.isNotEmpty()) {
            val pairs = collisions.joinToString("; ") { (a, b) -> "'$a' и '$b'" }
            throw CsvStructureException(
                "CSV колонки $pairs неразличимы после нормализации имён — переименуйте одну из них",
            )
        }
        return result
    }

    private fun lookupKey(column: String): String = normalizeHeader(column).lowercase()
}

/**
 * Приводит имя колонки к camelCase: `spend_amount`, `SPEND_AMOUNT`, `Spend Amount` → `spendAmount`.
 *
 * Имя без разделителей возвращается как есть: иначе уже camelCase-заголовок `createdAt`
 * превратился бы в `createdat` и разошёлся с одноимённым свойством DTO.
 */
internal fun normalizeHeader(raw: String): String {
    val trimmed = raw.trim()
    if (trimmed.none { it == '_' || it == ' ' || it == '-' }) return trimmed
    val parts = trimmed.split('_', ' ', '-').filter { it.isNotEmpty() }
    if (parts.isEmpty()) return trimmed
    return parts.first().lowercase() +
        parts.drop(1).joinToString("") { part -> part.lowercase().replaceFirstChar { it.uppercaseChar() } }
}
