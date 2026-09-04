package io.github.dsudomoin.migration.csv

import io.github.dsudomoin.migration.RunScope
import java.io.BufferedWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * Handle для записи CSV-output'а. Создаётся через [openCsv]. Регистрируется в [RunScope]
 * как [AutoCloseable] — закрывается runner'ом после исполнения плана, в обратном порядке регистрации.
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
) : CsvOutput {
    private val lock = Any()

    @Volatile
    private var closed = false

    override fun row(vararg cells: Any?) {
        synchronized(lock) {
            if (closed) throw IllegalStateException("CsvOutput is closed: $path")
            writer.write(cells.joinToString(",") { csvEscape(it?.toString() ?: "") })
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
 * Открывает CSV-файл по абсолютному [path], пишет header сразу, возвращает [CsvOutput] handle.
 * Регистрирует его в [RunScope] — runner закроет в `finally`.
 *
 * Поведение:
 * - `mkdir -p` на parent-папку.
 * - `CREATE + TRUNCATE_EXISTING` — файл перезаписывается при ререн-е миграции.
 * - Под dry-run файл **всё равно создаётся** и пишется. Это решение спеки (см. v0.1.0 §4.1):
 *   `--dry-run` остаётся диагностическим артефактом.
 */
fun RunScope.openCsv(path: Path, vararg headers: String): CsvOutput {
    Files.createDirectories(path.parent ?: Path.of("."))
    val w = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    // Заголовки тоже квотируем по RFC 4180 — на случай, если кто-то передаст "Order ID, total"
    // (с запятой). Без escape header превратился бы в три колонки вместо двух, парсинг ломается.
    w.write(headers.joinToString(",") { csvEscape(it) })
    w.newLine()
    w.flush()
    val out = CsvOutputImpl(path, w)
    register(out)
    return out
}

/**
 * Открывает CSV-файл с именем [filename] относительно [RunScope.outputFolder].
 * Самый частый случай — `openCsv("processed.csv", "id", "status")`.
 *
 * Поддерживает вложенные пути (`openCsv("nested/sub/out.csv", ...)`) — промежуточные каталоги
 * создадутся через `mkdir -p`.
 */
fun RunScope.openCsv(filename: String, vararg headers: String): CsvOutput =
    openCsv(outputFolder.resolve(filename), *headers)
