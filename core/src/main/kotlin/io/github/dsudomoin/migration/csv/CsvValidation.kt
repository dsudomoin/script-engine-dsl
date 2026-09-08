package io.github.dsudomoin.migration.csv

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.MigrationPrecondition
import java.io.InputStream
import java.nio.charset.Charset

/**
 * Входной файл забракован целиком, до первого обработанного элемента.
 *
 * @property source путь или имя ресурса, который проверяли.
 * @property problems по одной строке на каждую забракованную запись файла, с её номером.
 * @property truncated проверка остановлена на [MAX_PROBLEMS] ошибках — в файле их может быть больше.
 */
class CsvValidationException internal constructor(
    val source: String,
    val problems: List<String>,
    val truncated: Boolean,
) : RuntimeException(buildMessage(source, problems, truncated)), MigrationPrecondition {

    private companion object {
        fun buildMessage(source: String, problems: List<String>, truncated: Boolean): String {
            val n = problems.size
            val count = if (truncated) "$n и более строк не прошли" else "$n ${rowsFailed(n)}"
            val head = "$source: $count проверку — миграция не запускалась, целевая система не тронута"
            val tail = if (truncated) "\n  ... проверка остановлена после $MAX_PROBLEMS ошибок" else ""
            return problems.joinToString("\n  ", prefix = "$head\n  ") + tail
        }

        // Текст читает человек, разбирающийся с отказом; «1 строк не прошли» его отвлекает.
        fun rowsFailed(n: Int): String = when {
            n % 100 in 11..14 -> "строк не прошли"
            n % 10 == 1 -> "строка не прошла"
            n % 10 in 2..4 -> "строки не прошли"
            else -> "строк не прошли"
        }
    }
}

/**
 * Сколько ошибок собирать, прежде чем остановиться. Сотня битых строк — это уже приговор файлу,
 * и дочитывать ради ещё одной тысячи таких же незачем.
 */
internal const val MAX_PROBLEMS = 100

/**
 * Прогнать файл целиком, ничего не делая с результатом, и собрать строки, на которых [bind]
 * упал бы фатально.
 *
 * Строки, которые политика и так пропустила бы, не собираются и **не аудитируются**: их
 * запишет рабочий проход, и двойная запись в `errors.csv` только запутала бы разбор.
 */
internal fun validateCsv(
    source: String,
    openStream: () -> InputStream,
    delimiter: Char,
    quote: Char,
    charset: Charset,
    onRowError: ItemError<CsvRow>,
    bind: (CsvRow) -> Unit,
) {
    val problems = ArrayList<String>()
    openStream().use { stream ->
        val reader = bomAwareReader(stream, charset)
        rowReader(delimiter, quote).readValues<Array<String>>(reader).use { iter ->
            if (!iter.hasNext()) return
            val header = CsvHeader(iter.next().asList())

            var lineNumber = 1L
            while (iter.hasNext() && problems.size < MAX_PROBLEMS) {
                lineNumber++
                val cells = iter.next()
                if (isBlankRow(cells)) continue
                val row = CsvRow(lineNumber, header, cells.asList())
                try {
                    bind(row)
                } catch (e: Throwable) {
                    if (e is InterruptedException) {
                        Thread.currentThread().interrupt()
                        throw e
                    }
                    // Файл не той формы — это приговор всему файлу, а не одной строке.
                    if (e is CsvStructureException) throw e
                    if (wouldFail(onRowError, e, row)) problems += describe(lineNumber, e)
                }
            }
        }
    }
    if (problems.isNotEmpty()) {
        throw CsvValidationException(source, problems, truncated = problems.size >= MAX_PROBLEMS)
    }
}

/**
 * Упал бы прогон на этой строке. Сбой самого классификатора считается фатальным: он не смог
 * разрешить пропуск, а молча пропустить за него — значит подменить его решение.
 */
private fun wouldFail(policy: ItemError<CsvRow>, e: Throwable, row: CsvRow): Boolean = when (policy) {
    is ItemError.Fail -> true
    is ItemError.Skip -> false
    is ItemError.Handle -> try {
        policy.decide(e, row) == ItemError.Decision.Fail
    } catch (_: Throwable) {
        true
    }
}

/**
 * Одна строка списка. Номер приписывается только тем ошибкам, которые его не называют сами:
 * [CsvCellException] уже собрана с номером внутри.
 *
 * Сообщения Jackson многострочные («at [Source: ...] (through reference chain: ...)») — в списке
 * на сотню строк это нечитаемо, а суть всегда в первой строке.
 */
private fun describe(lineNumber: Long, e: Throwable): String {
    val text = e.message?.lineSequence()?.firstOrNull()?.trim()?.takeIf { it.isNotEmpty() }
        ?: e.javaClass.simpleName
    return if (e is CsvCellException) text else "строка $lineNumber: $text"
}
