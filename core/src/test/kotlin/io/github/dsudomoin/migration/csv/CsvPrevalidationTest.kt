package io.github.dsudomoin.migration.csv

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationPrecondition
import io.github.dsudomoin.migration.MigrationScope
import io.github.dsudomoin.migration.MigrationTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/**
 * Предпроверка входного файла.
 *
 * Смысл: если файл битый, миграция не должна тронуть целевую систему вообще. Падение на
 * середине оставляет систему в состоянии, которое потом никто не разберёт, — а починить
 * файл по одной строке за прогон и вовсе издевательство.
 */
class CsvPrevalidationTest {

    @TempDir
    lateinit var tmp: Path

    data class Row(val id: Long, val spend: Long)

    private val broken = "id,spend\n1,10\n2,НЕ-ЧИСЛО\n3,30\n4,ТОЖЕ-НЕ\n5,50\n"

    private fun run(
        content: String,
        onRowError: ItemError<CsvRow> = ItemError.Fail,
        streaming: Boolean = false,
        seen: MutableList<Long> = mutableListOf(),
    ): Triple<List<Long>, Throwable?, Long> {
        val file = tmp.resolve("in.csv")
        Files.writeString(file, content)
        val migration = object : Migration("PREVALIDATE") {
            override fun MigrationScope.run() {
                each(readCsvAs<Row>(file.toString(), onRowError = onRowError, streaming = streaming)) {
                    seen += it.id
                }
            }
        }
        val outcome = MigrationTest.run(migration, outputFolder = tmp.resolve("out"))
        return Triple(seen, outcome.failure, outcome.report.processed)
    }

    @Test
    fun `битый файл валит прогон до первой обработанной строки`() {
        val (seen, failure, processed) = run(broken)

        assertThat(seen).describedAs("ни одна строка не дошла до обработчика").isEmpty()
        assertThat(processed).isZero()
        assertThat(failure).isInstanceOf(CsvValidationException::class.java)
        assertThat(failure).isInstanceOf(MigrationPrecondition::class.java)
    }

    @Test
    fun `сообщение перечисляет все битые строки, а не только первую`() {
        val (_, failure, _) = run(broken)

        assertThat(failure).hasMessageContaining("строка 3").hasMessageContaining("строка 5")
        assertThat((failure as CsvValidationException).problems).hasSize(2)
    }

    @Test
    fun `предпроверка не дублирует аудит — errors csv не пухнет по строке на строку`() {
        run(broken)

        val errors = Files.readString(tmp.resolve("out").resolve("errors.csv"))
        // Считаем записи, а не строки файла: сообщение отказа многострочное и в CSV лежит
        // одним закавыченным полем. Запись должна быть ровно одна — уровня прогона.
        // Построчно предпроверка не аудитирует: ни одна из строк не была элементом обработки.
        val records = errors.lineSequence().count { it.matches(Regex("^\\d{4}-\\d{2}-\\d{2}T.*")) }
        assertThat(records).isEqualTo(1)
    }

    @Test
    fun `Skip отключает предпроверку — битые строки для того и разрешены`() {
        val (seen, failure, processed) = run(broken, onRowError = ItemError.Skip)

        assertThat(failure).isNull()
        assertThat(seen).containsExactly(1L, 3L, 5L)
        assertThat(processed).isEqualTo(3)
    }

    @Test
    fun `streaming = true возвращает потоковое поведение — падение уже в процессе`() {
        val (seen, failure, _) = run(broken, streaming = true)

        assertThat(failure).isNotNull()
        assertThat(failure).isNotInstanceOf(MigrationPrecondition::class.java)
        assertThat(seen).describedAs("первая строка успела обработаться").containsExactly(1L)
    }

    @Test
    fun `Handle валит предпроверку только на тех строках, которые он не пропускает`() {
        val onlyFirstIsFatal = ItemError.Handle<CsvRow> { _, row ->
            if (row["id"] == "2") ItemError.Decision.Fail else ItemError.Decision.Skip
        }

        val (seen, failure, _) = run(broken, onRowError = onlyFirstIsFatal)

        assertThat(failure).isInstanceOf(CsvValidationException::class.java)
        assertThat(failure!!.message)
            .contains("строка 3")
            .describedAs("строка 5 классификатором пропускается")
            .doesNotContain("строка 5")
        assertThat(seen).isEmpty()
    }

    @Test
    fun `Handle пропускает всё — предпроверка молчит и прогон идёт`() {
        val alwaysSkip = ItemError.Handle<CsvRow> { _, _ -> ItemError.Decision.Skip }

        val (seen, failure, _) = run(broken, onRowError = alwaysSkip)

        assertThat(failure).isNull()
        assertThat(seen).containsExactly(1L, 3L, 5L)
    }

    @Test
    fun `целый файл проходит предпроверку и читается ровно один раз содержательно`() {
        val (seen, failure, processed) = run("id,spend\n1,10\n2,20\n")

        assertThat(failure).isNull()
        assertThat(seen).containsExactly(1L, 2L)
        assertThat(processed).isEqualTo(2)
    }

    @Test
    fun `нет колонки под обязательное поле — тоже отказ до старта`() {
        val (seen, failure, _) = run("id\n1\n2\n")

        assertThat(seen).isEmpty()
        assertThat(failure).isInstanceOf(CsvStructureException::class.java)
        assertThat(failure).isInstanceOf(MigrationPrecondition::class.java)
        assertThat(failure).hasMessageContaining("spend")
    }
}
