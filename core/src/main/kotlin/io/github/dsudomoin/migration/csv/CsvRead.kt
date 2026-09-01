package io.github.dsudomoin.migration.csv

import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.internal.ErrorThresholdExceeded
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
fun <T> MigrationContext.readCsv(
    path: String,
    classpath: Boolean = false,
    onRowError: OnError = OnError.Fail,
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

/** Перегрузка с явным [Path] — для случаев, когда path уже посчитан (например, относительно [MigrationContext.outputFolder]). */
fun <T> MigrationContext.readCsv(
    path: Path,
    onRowError: OnError = OnError.Fail,
    mapper: (Map<String, String>) -> T,
): Sequence<T> = lazyCsvSequence(onRowError, mapper) { Files.newInputStream(path) }

/**
 * Строит ленивую `Sequence<T>` поверх CSV. **Stream открывается только на первом `next()`** —
 * если пользователь забыл консьюмить (early return, `.take(0)`, exception до итерации), файловый
 * хендл не открывается.
 *
 * Открытый stream сразу регистрируется в [MigrationContext]: `sequence { }` — это корутина, и
 * при неполном потреблении (`take(n)`, ранний выход, ошибка выше по стеку) она просто
 * бросается — `use`-блок внутри неё не доигрывает и дескриптор утёк бы. Реестр закроет его в
 * любом случае; повторный `close()` на уже закрытом потоке — no-op.
 *
 * [onRowError] применяется к КАЖДОЙ строке отдельно: и к разбору CSV, и к работе [mapper].
 * `Fail` (дефолт) — первая же плохая строка валит прогон. `Skip` / `Handle→Skip` — строка
 * аудитится в `errors.csv`, инкрементит `report.skipped` (то есть считается в `errorThreshold`)
 * и не доходит до `forEach`.
 *
 * В аудит уезжает сама разобранная строка (`Map<String, String>`), иначе по `errors.csv`
 * невозможно понять, какая именно строка сломалась. Если в колонках есть чувствительные данные,
 * сократи представление: `errors.includeItem<Map<String, String>> { "id=" + it["id"] }`.
 */
private fun <T> MigrationContext.lazyCsvSequence(
    onRowError: OnError,
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
                        // колонки) — это ровно тот случай, ради которого пишут OnError.Skip.
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
 * `Skip` аудитит строку, считает её в `report.skipped` и проверяет `errorThreshold` — ровно та
 * же механика, что и у `forEach`, чтобы порог считался по всем ошибкам прогона, а не только по
 * ошибкам тела цикла.
 */
private fun MigrationContext.handleRowError(e: Throwable, onRowError: OnError, row: Map<String, String>?) {
    if (e is InterruptedException) {
        Thread.currentThread().interrupt()
        throw e
    }
    val decision = when (onRowError) {
        is OnError.Fail -> OnError.Decision.Fail
        is OnError.Skip -> OnError.Decision.Skip
        is OnError.Handle -> onRowError.decide(e, null)
    }
    if (decision == OnError.Decision.Fail) throw e

    auditError(e, row)
    report.incSkipped()
    if (errorThreshold > 0) {
        val skipped = report.skippedCount()
        if (skipped > errorThreshold) throw ErrorThresholdExceeded(errorThreshold, skipped)
    }
}
