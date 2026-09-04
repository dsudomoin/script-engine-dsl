package io.github.dsudomoin.migration.csv

import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import io.github.dsudomoin.migration.RunScope
import io.github.dsudomoin.migration.ItemError
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
 * @param onRowError что делать со строкой, которую не удалось разобрать или отобразить в `T`.
 *                   См. [readCsv] KDoc про политики.
 */
fun <T> RunScope.readCsv(
    path: String,
    classpath: Boolean = false,
    onRowError: ItemError<Map<String, String>> = ItemError.Fail,
    mapper: (Map<String, String>) -> T,
): Sequence<T> = lazyCsvSequence(onRowError, mapper) {
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

/** Перегрузка с явным [Path] — для случаев, когда path уже посчитан (например, относительно [RunScope.outputFolder]). */
fun <T> RunScope.readCsv(
    path: Path,
    onRowError: ItemError<Map<String, String>> = ItemError.Fail,
    mapper: (Map<String, String>) -> T,
): Sequence<T> = lazyCsvSequence(onRowError, mapper) { Files.newInputStream(path) }

/**
 * Строит ленивую `Sequence<T>` поверх CSV. **Stream открывается только на первом `next()`** —
 * если пользователь забыл консьюмить (early return, `.take(0)`, exception до итерации), файловый
 * хендл не открывается.
 *
 * Открытый stream сразу регистрируется в [RunScope]: `sequence { }` — это корутина, и
 * при неполном потреблении (`take(n)`, ранний выход, ошибка выше по стеку) она просто
 * бросается — `use`-блок внутри неё не доигрывает и дескриптор утёк бы. Реестр закроет его в
 * любом случае; повторный `close()` на уже закрытом потоке — no-op.
 *
 * [onRowError] применяется к КАЖДОЙ строке отдельно: и к разбору CSV, и к работе [mapper].
 * `Fail` (дефолт) — первая же плохая строка валит прогон. `Skip` / `Handle→Skip` — строка
 * аудитится в `errors.csv`, инкрементит `report.sourceSkipped` и не доходит до обработчика стадии.
 *
 * В аудит уезжает сама разобранная строка (`Map<String, String>`), иначе по `errors.csv`
 * невозможно понять, какая именно строка сломалась. Если в колонках есть чувствительные данные,
 * сократи представление: `errors.includeItem<Map<String, String>> { "id=" + it["id"] }`.
 */
private fun <T> RunScope.lazyCsvSequence(
    onRowError: ItemError<Map<String, String>>,
    mapper: (Map<String, String>) -> T,
    openStream: () -> InputStream,
): Sequence<T> = sequence {
    val ctx = this@lazyCsvSequence
    val csvMapper = CsvMapper()
    val schema = CsvSchema.emptySchema().withHeader()
    openStream().use { stream ->
        ctx.register(stream)
        csvMapper.readerFor(Map::class.java).with(schema)
            .readValues<Map<String, String>>(stream)
            .use { iter ->
                while (iter.hasNext()) {
                    var value: T? = null
                    var ok = false
                    // Разобранная строка нужна аудиту: без неё в errors.csv уезжает пустой
                    // itemRepr, и по файлу невозможно понять, на какой именно строке встало.
                    var raw: Map<String, String>? = null
                    try {
                        // next() тоже под политикой: битая строка (рваные кавычки, лишние
                        // колонки) — это ровно тот случай, ради которого пишут ItemError.Skip.
                        raw = iter.next()
                        value = mapper(raw)
                        ok = true
                    } catch (e: Throwable) {
                        ctx.handleRowError(e, onRowError, raw)
                    }
                    if (ok) {
                        @Suppress("UNCHECKED_CAST")
                        yield(value as T)
                    }
                }
            }
    }
}

/**
 * Применяет [onRowError] к сбойной строке. `Fail` пробрасывает исходное исключение;
 * `Skip` аудитит строку и считает её в `report.sourceSkipped` — отдельно от item-skip'ов
 * стадии: такая строка никогда не была `processed`, и смешивать её со `skipped` значило бы
 * сломать тождество `processed = successful + skipped + failed`.
 */
private fun RunScope.handleRowError(e: Throwable, onRowError: ItemError<Map<String, String>>, row: Map<String, String>?) {
    if (e is InterruptedException) {
        Thread.currentThread().interrupt()
        throw e
    }
    val decision = when (onRowError) {
        is ItemError.Fail -> ItemError.Decision.Fail
        is ItemError.Skip -> ItemError.Decision.Skip
        // Классификатор получает саму разобранную строку: без неё нельзя отличить битую запись
        // от записи с недопустимым значением.
        is ItemError.Handle -> onRowError.decide(e, row ?: emptyMap())
    }
    if (decision == ItemError.Decision.Fail) throw e

    auditError(e, row)
    report.incSourceSkipped()
}
