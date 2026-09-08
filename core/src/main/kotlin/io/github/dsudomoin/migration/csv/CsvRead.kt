package io.github.dsudomoin.migration.csv

import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvParser
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.MigrationScope
import io.github.dsudomoin.migration.internal.MigrationRun
import java.io.InputStream
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path

/**
 * Ленивое чтение CSV с заголовком. Строка приезжает в [map] как [CsvRow]; результат годится
 * для миллионных файлов — файл не поднимается в память целиком.
 *
 * [onRowError] применяется к каждой строке отдельно — и к разбору CSV, и к работе [map].
 * Отброшенная строка считается в `sourceSkipped`, а не в `skipped`: до цикла она не дошла
 * и никогда не была `processed`. Исключение — [CsvStructureException]: файл не той формы
 * валит прогон при любой политике.
 *
 * @param path путь к файлу или, при `classpath = true`, имя ресурса.
 * @param classpath читать из ресурсов, а не с файловой системы.
 * @param delimiter разделитель колонок: выгрузки из Excel в русской локали идут через `;`.
 * @param quote символ кавычки вокруг значений, содержащих разделитель или перевод строки.
 * @param charset кодировка файла (например `Charset.forName("windows-1251")`). BOM в начале
 *                срезается в любом случае и, если он от UTF-16, задаёт кодировку сам.
 */
fun <T> MigrationScope.readCsv(
    path: String,
    classpath: Boolean = false,
    delimiter: Char = ',',
    quote: Char = '"',
    charset: Charset = Charsets.UTF_8,
    onRowError: ItemError<CsvRow> = ItemError.Fail,
    map: (CsvRow) -> T,
): Sequence<T> {
    val run = this as MigrationRun
    return csvSequence(run, delimiter, quote, charset, onRowError, map) {
        openCsvStream(run, path, classpath)
    }
}

internal fun openCsvStream(run: MigrationRun, path: String, classpath: Boolean): InputStream =
    if (classpath) {
        // contextClassLoader может быть null (system threads, кастомные пулы) — fallback на
        // загрузчик нашего класса, он есть всегда.
        val cl = Thread.currentThread().contextClassLoader ?: run.javaClass.classLoader
        cl.getResourceAsStream(path) ?: error("Resource not found on classpath: $path")
    } else {
        Files.newInputStream(Path.of(path))
    }

/**
 * Поток открывается только на первом `next()` и сразу регистрируется в реестре прогона:
 * `sequence { }` — это корутина, и при неполном потреблении (`take(n)`, ранний выход)
 * она просто бросается — `use`-блок внутри неё не доигрывает и дескриптор утёк бы.
 *
 * Заголовок разбирается своими руками, а не через `CsvSchema.withHeader()`: так его видно
 * до первой записи — есть где проверить коллизии имён и сверить колонки с ожиданиями.
 */
private fun <T> csvSequence(
    run: MigrationRun,
    delimiter: Char,
    quote: Char,
    charset: Charset,
    onRowError: ItemError<CsvRow>,
    map: (CsvRow) -> T,
    openStream: () -> InputStream,
): Sequence<T> = sequence {
    openStream().use { stream ->
        run.register(stream)
        val reader = bomAwareReader(stream, charset)
        rowReader(delimiter, quote).readValues<Array<String>>(reader).use { iter ->
            if (!iter.hasNext()) return@use
            val header = CsvHeader(iter.next().asList())

            var lineNumber = 1L
            while (iter.hasNext()) {
                lineNumber++
                var value: T? = null
                var ok = false
                // Разобранная строка нужна аудиту: без неё по errors.csv невозможно понять,
                // какая именно строка сломалась.
                var row: CsvRow? = null
                try {
                    val cells = iter.next()
                    if (isBlankRow(cells)) continue
                    row = CsvRow(lineNumber, header, cells.asList())
                    value = map(row)
                    ok = true
                } catch (e: Throwable) {
                    handleRowError(run, e, onRowError, row)
                }
                if (ok) {
                    @Suppress("UNCHECKED_CAST")
                    yield(value as T)
                }
            }
        }
    }
}

// ObjectMapper дорог в создании и потокобезопасен после настройки; ObjectReader иммутабелен.
//
// WRAP_AS_ARRAY обязателен: без него схема без объявленных колонок отвергает вторую ячейку
// строки как «лишнюю» (Too many entries: expected at most 0) — а колонок мы заранее и не знаем.
private val CSV_MAPPER = CsvMapper().apply { enable(CsvParser.Feature.WRAP_AS_ARRAY) }

internal fun rowReader(delimiter: Char, quote: Char) =
    CSV_MAPPER.readerFor(Array<String>::class.java).with(
        CsvSchema.emptySchema()
            .withColumnSeparator(delimiter)
            .withQuoteChar(quote),
    )

// Хвостовая пустая строка — норма для выгрузок; считать её битой записью значило бы
// сыпать в errors.csv на ровном месте.
internal fun isBlankRow(cells: Array<String>): Boolean =
    cells.isEmpty() || (cells.size == 1 && cells[0].isBlank())

private fun handleRowError(
    run: MigrationRun,
    e: Throwable,
    onRowError: ItemError<CsvRow>,
    row: CsvRow?,
) {
    if (e is InterruptedException) {
        Thread.currentThread().interrupt()
        throw e
    }
    if (e is CsvStructureException) throw e

    val decision = try {
        when (onRowError) {
            is ItemError.Fail -> ItemError.Decision.Fail
            is ItemError.Skip -> ItemError.Decision.Skip
            // Сломался сам разбор CSV — строки нет, и классификатору не с чем работать.
            is ItemError.Handle -> if (row == null) ItemError.Decision.Fail else onRowError.decide(e, row)
        }
    } catch (classifierError: Throwable) {
        // Та же семантика, что у классификатора элемента: сбой классификатора становится
        // основной ошибкой, а исходная уезжает в suppressed.
        classifierError.addSuppressed(e)
        run.audit(classifierError, row)
        throw classifierError
    }
    if (decision == ItemError.Decision.Fail) throw e

    run.audit(e, row)
    run.report.incSourceSkipped()
}
