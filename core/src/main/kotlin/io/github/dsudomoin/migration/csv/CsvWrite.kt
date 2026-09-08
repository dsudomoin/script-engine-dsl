package io.github.dsudomoin.migration.csv

import io.github.dsudomoin.migration.MigrationScope
import io.github.dsudomoin.migration.internal.MigrationRun
import java.io.BufferedWriter
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.InvalidPathException
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * Handle для записи CSV-выхода. Создаётся через [csv] и закрывается движком в конце прогона,
 * в обратном порядке открытия.
 *
 * Методы:
 * - [row] thread-safe (можно вызывать из параллельных воркеров стадии).
 * - [close] идемпотентный (повторный вызов — no-op).
 */
interface CsvOutput : AutoCloseable {
    /**
     * Записать одну строку. Ячейки сериализуются через `toString()` (null → пустая строка),
     * запятые/кавычки/переводы строк автоматически квотируются по RFC 4180.
     *
     * **Не делает flush** — буферизованные строки попадут на диск при [flush] или [close].
     * Это сознательный выбор: на 1M-row выгрузках per-row flush был бы узким местом
     * (миллион syscall'ов). Runner закрывает все [CsvOutput] в `finally` после исполнения плана —
     * к этому моменту все строки гарантированно на диске. Если миграция жёстко падает
     * (kill -9, OOM до close), последние буферизованные строки теряются — для типичных
     * аналитических CSV это приемлемо.
     *
     * **Thread-safety, но не масштабируется бесконечно:** метод сериализует записи через общий
     * `synchronized` lock. На `parallel = 4..16` цена несущественна; на `parallel > 32` запись в
     * один и тот же CSV становится точкой сериализации воркеров. Если упёрся — пиши в несколько
     * CSV (`openCsv("shard-$worker.csv", ...)`) или агрегируй в памяти и flush'и батчем.
     *
     * Бросает [IllegalStateException], если файл уже закрыт.
     */
    fun row(vararg cells: Any?)

    /**
     * Явно flush'нуть буфер на диск. Полезно после крупного batch'а, чтобы данные были
     * видны другим процессам / устойчивы к жёсткому падению JVM. На обычной работе вызывать
     * не нужно — close() в `finally` runner'а сам flushит.
     */
    fun flush()
}

private class CsvOutputImpl(
    private val path: Path,
    private val writer: BufferedWriter,
    private val delimiter: Char,
) : CsvOutput {
    private val lock = Any()

    @Volatile
    private var closed = false

    override fun row(vararg cells: Any?) {
        synchronized(lock) {
            if (closed) throw IllegalStateException("CsvOutput is closed: $path")
            writer.write(cells.joinToString(delimiter.toString()) { csvEscape(it?.toString() ?: "", delimiter) })
            writer.newLine()
            // NO flush per row — flush в close()/flush().
        }
    }

    override fun flush() {
        synchronized(lock) {
            if (!closed) writer.flush()
        }
    }

    override fun close() {
        synchronized(lock) {
            if (closed) return
            closed = true
            writer.close()    // BufferedWriter.close() flushит внутри
        }
    }

    override fun toString(): String = "CsvOutput($path)"
}

/**
 * Открыть файл и написать заголовок. Без регистрации в реестре прогона: кто открыл,
 * тот и решает, кто закроет.
 *
 * Заголовки квотируются по RFC 4180 — имя колонки вроде `"Order ID, total"` иначе
 * превратилось бы в две колонки вместо одной.
 */
internal fun openCsvFile(
    path: Path,
    headers: List<String>,
    delimiter: Char = ',',
    charset: Charset = Charsets.UTF_8,
    bom: Boolean = false,
): CsvOutput {
    Files.createDirectories(path.parent ?: Path.of("."))
    val w = Files.newBufferedWriter(path, charset, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    if (bom) w.write(BOM)
    w.write(headers.joinToString(delimiter.toString()) { csvEscape(it, delimiter) })
    w.newLine()
    w.flush()
    return CsvOutputImpl(path, w, delimiter)
}

/** U+FEFF: в UTF-8 кодируется теми самыми `EF BB BF`, по которым Excel узнаёт кодировку. */
private const val BOM = "\uFEFF"

/**
 * Имена, которые движок открывает в `outputFolder` сам.
 *
 * Пользовательский выход с таким именем писал бы в тот же файл параллельно с аудитором
 * или файловым логгером, а оба открывают его с `TRUNCATE_EXISTING` — то есть аудит прогона
 * молча уничтожался бы ровно там, где он нужнее всего.
 */
private val RESERVED_ARTIFACTS = setOf("errors.csv", "errors.log", "migration.log")

/**
 * CSV-выход прогона: отчёты, экспорты, диагностика. Файл открывается в `outputFolder`
 * один раз на имя и закрывается движком в конце прогона.
 *
 * Повторный вызов с тем же [filename] возвращает тот же handle — см. [MigrationRun.csvOutput];
 * формат при этом задаётся первым вызовом, последующие его не меняют.
 *
 * Под dry-run файл всё равно пишется: выход — диагностический артефакт, а не изменение
 * целевой системы.
 *
 * @param delimiter разделитель колонок. `;` — если файл поедет в Excel в русской локали.
 * @param charset кодировка файла.
 * @param bom писать ли BOM. Excel без него читает UTF-8 как ANSI и показывает кириллицу
 *            кракозябрами; для программного потребителя BOM, наоборот, лишний.
 */
fun MigrationScope.csv(
    filename: String,
    vararg headers: String,
    delimiter: Char = ',',
    charset: Charset = Charsets.UTF_8,
    bom: Boolean = false,
): CsvOutput {
    val run = this as MigrationRun
    // Без проверки BOM уехал бы в файл заменяющим символом (в windows-1251 это '?') и сломал
    // бы имя первой колонки — ровно ту беду, от которой он и должен спасать.
    require(!bom || charset.newEncoder().canEncode(BOM)) {
        "csv '$filename': кодировка ${charset.name()} не умеет BOM — уберите bom = true"
    }
    val normalized = try {
        Path.of(filename).normalize()
    } catch (e: InvalidPathException) {
        throw IllegalArgumentException("csv '$filename' is not a valid path", e)
    }
    require(!normalized.isAbsolute && !normalized.startsWith("..")) {
        "csv '$filename' must stay inside the migration outputFolder"
    }
    require(normalized.toString() !in RESERVED_ARTIFACTS) {
        "csv '$filename' uses a reserved engine artifact name: $RESERVED_ARTIFACTS"
    }
    return run.csvOutput(normalized.toString()) {
        openCsvFile(outputFolder.resolve(normalized), headers.toList(), delimiter, charset, bom)
    }
}
