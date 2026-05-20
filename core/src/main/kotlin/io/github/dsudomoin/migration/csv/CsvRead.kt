package io.github.dsudomoin.migration.csv

import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import io.github.dsudomoin.migration.MigrationContext
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path

/**
 * Ленивое чтение CSV с header'ом. Возвращает `Sequence<T>` — стрим, материализуется по мере
 * итерации, подходит для миллионных файлов.
 *
 * Каждая строка передаётся в [mapper] как `Map<String, String>` (ключ = имя колонки из header'а).
 *
 * @param path относительный/абсолютный путь к файлу. Под [classpath]=true — путь внутри ресурсов.
 * @param classpath если `true`, [path] резолвится через `Thread.contextClassLoader.getResourceAsStream`.
 *                  Уместно для test-fixture CSV. Default `false`.
 */
fun <T> MigrationContext.readCsv(
    path: String,
    classpath: Boolean = false,
    mapper: (Map<String, String>) -> T,
): Sequence<T> = lazyCsvSequence(mapper) {
    if (classpath) {
        // contextClassLoader может быть null (system threads, кастомные пулы) — fallback на
        // загрузчик нашего класса, он всегда есть.
        val cl = Thread.currentThread().contextClassLoader ?: this::class.java.classLoader
        cl.getResourceAsStream(path)
            ?: error("Resource not found on classpath: $path")
    } else {
        Files.newInputStream(Path.of(path))
    }
}

/** Перегрузка с явным [Path] — для случаев, когда path уже посчитан (например, относительно [MigrationContext.outputFolder]). */
fun <T> MigrationContext.readCsv(
    path: Path,
    mapper: (Map<String, String>) -> T,
): Sequence<T> = lazyCsvSequence(mapper) { Files.newInputStream(path) }

/**
 * Строит ленивую `Sequence<T>` поверх CSV. **Stream открывается только на первом `next()`** —
 * если пользователь забыл консьюмить (early return, `.take(0)`, exception до итерации), файловый
 * хендл не открывается и не утекает.
 *
 * Stream обёрнут в `use`-блок: даже если `readValues(stream)` упадёт (например, malformed CSV)
 * до того, как `it.use {}` возьмёт ownership через MappingIterator, stream всё равно закроется.
 * После последнего элемента — `it.use {}` закрывает `MappingIterator`, который тоже закрывает
 * underlying stream (idempotent close, нет double-close).
 */
private inline fun <T> lazyCsvSequence(
    crossinline mapper: (Map<String, String>) -> T,
    crossinline openStream: () -> InputStream,
): Sequence<T> = sequence {
    val csvMapper = CsvMapper()
    val schema = CsvSchema.emptySchema().withHeader()
    openStream().use { stream ->
        csvMapper.readerFor(Map::class.java).with(schema)
            .readValues<Map<String, String>>(stream)
            .use { iter ->
                while (iter.hasNext()) {
                    yield(mapper(iter.next()))
                }
            }
    }
}
