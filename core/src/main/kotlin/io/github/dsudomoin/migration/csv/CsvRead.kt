package io.github.dsudomoin.migration.csv

import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import io.github.dsudomoin.migration.MigrationScope
import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.internal.MigrationRun
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path

/**
 * Ленивое чтение CSV с заголовком. Строка приезжает в [mapper] как `Map<String, String>`
 * (ключ — имя колонки), результат годится для миллионных файлов.
 *
 * [onRowError] применяется к каждой строке отдельно — и к разбору CSV, и к работе [mapper].
 * Отброшенная строка считается в `sourceSkipped`, а не в `skipped`: до цикла она не дошла
 * и никогда не была `processed`.
 */
fun <T> MigrationScope.readCsv(
    path: String,
    classpath: Boolean = false,
    onRowError: ItemError<Map<String, String>> = ItemError.Fail,
    mapper: (Map<String, String>) -> T,
): Sequence<T> {
    val run = this as MigrationRun
    return csvSequence(run, onRowError, mapper) {
        if (classpath) {
            // contextClassLoader может быть null (system threads, кастомные пулы) — fallback на
            // загрузчик нашего класса, он есть всегда.
            val cl = Thread.currentThread().contextClassLoader ?: run.javaClass.classLoader
            cl.getResourceAsStream(path) ?: error("Resource not found on classpath: $path")
        } else {
            Files.newInputStream(Path.of(path))
        }
    }
}

/**
 * Поток открывается только на первом `next()` и сразу регистрируется в реестре прогона:
 * `sequence { }` — это корутина, и при неполном потреблении (`take(n)`, ранний выход)
 * она просто бросается — `use`-блок внутри неё не доигрывает и дескриптор утёк бы.
 */
private fun <T> csvSequence(
    run: MigrationRun,
    onRowError: ItemError<Map<String, String>>,
    mapper: (Map<String, String>) -> T,
    openStream: () -> InputStream,
): Sequence<T> = sequence {
    val csvMapper = CsvMapper()
    val schema = CsvSchema.emptySchema().withHeader()
    openStream().use { stream ->
        run.register(stream)
        csvMapper.readerFor(Map::class.java).with(schema)
            .readValues<Map<String, String>>(stream)
            .use { iter ->
                while (iter.hasNext()) {
                    var value: T? = null
                    var ok = false
                    // Разобранная строка нужна аудиту: без неё по errors.csv невозможно понять,
                    // какая именно строка сломалась.
                    var raw: Map<String, String>? = null
                    try {
                        raw = iter.next()
                        value = mapper(raw)
                        ok = true
                    } catch (e: Throwable) {
                        handleRowError(run, e, onRowError, raw)
                    }
                    if (ok) {
                        @Suppress("UNCHECKED_CAST")
                        yield(value as T)
                    }
                }
            }
    }
}

private fun handleRowError(
    run: MigrationRun,
    e: Throwable,
    onRowError: ItemError<Map<String, String>>,
    row: Map<String, String>?,
) {
    if (e is InterruptedException) {
        Thread.currentThread().interrupt()
        throw e
    }
    val decision = try {
        when (onRowError) {
            is ItemError.Fail -> ItemError.Decision.Fail
            is ItemError.Skip -> ItemError.Decision.Skip
            is ItemError.Handle -> onRowError.decide(e, row ?: emptyMap())
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
