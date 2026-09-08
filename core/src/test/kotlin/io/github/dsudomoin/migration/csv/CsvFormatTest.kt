package io.github.dsudomoin.migration.csv

import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationScope
import io.github.dsudomoin.migration.MigrationTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path

/**
 * Формат входного файла: разделитель, кавычки, кодировка, BOM и имена колонок.
 *
 * Всё это реальные свойства чужих выгрузок: Excel кладёт BOM и `;`, 1С — windows-1251,
 * а имена колонок приезжают в любом регистре и через подчёркивание.
 */
class CsvFormatTest {

    @TempDir
    lateinit var tmp: Path

    private fun read(
        content: ByteArray,
        delimiter: Char = ',',
        quote: Char = '"',
        charset: Charset = Charsets.UTF_8,
        pick: (CsvRow) -> String,
    ): List<String> {
        val file = tmp.resolve("in.csv")
        Files.write(file, content)
        val seen = mutableListOf<String>()
        val migration = object : Migration("FORMAT") {
            override fun MigrationScope.run() {
                readCsv(file.toString(), delimiter = delimiter, quote = quote, charset = charset) { pick(it) }
                    .forEach { seen += it }
            }
        }
        val outcome = MigrationTest.run(migration, outputFolder = tmp.resolve("out"))
        assertThat(outcome.failure).isNull()
        return seen
    }

    @Test
    fun `точка с запятой как разделитель`() {
        val rows = read("id;name\n1;Иван\n2;Пётр\n".toByteArray(), delimiter = ';') { it["name"] }

        assertThat(rows).containsExactly("Иван", "Пётр")
    }

    @Test
    fun `табуляция как разделитель`() {
        val rows = read("id\tname\n1\tA\n".toByteArray(), delimiter = '\t') { it["name"] }

        assertThat(rows).containsExactly("A")
    }

    @Test
    fun `значение с разделителем внутри кавычек не рвёт строку`() {
        val rows = read("id;name\n1;\"Иванов; ООО\"\n".toByteArray(), delimiter = ';') { it["name"] }

        assertThat(rows).containsExactly("Иванов; ООО")
    }

    @Test
    fun `BOM от Excel срезается и не ломает имя первой колонки`() {
        val bom = byteArrayOf(0xEF.toByte(), 0xBB.toByte(), 0xBF.toByte())
        val rows = read(bom + "id,name\n1,A\n".toByteArray()) { it["id"] }

        assertThat(rows).containsExactly("1")
    }

    @Test
    fun `windows-1251 читается при явной кодировке`() {
        val cp1251 = Charset.forName("windows-1251")
        val rows = read("id,name\n1,Иван\n".toByteArray(cp1251), charset = cp1251) { it["name"] }

        assertThat(rows).containsExactly("Иван")
    }

    @Test
    fun `имена колонок триммятся и сопоставляются без учёта регистра и подчёркиваний`() {
        val content = " ID , Spend_Amount ,createdAt\n7,100,2026-01-01\n".toByteArray()

        assertThat(read(content) { it["id"] }).containsExactly("7")
        assertThat(read(content) { it["spendAmount"] }).containsExactly("100")
        assertThat(read(content) { it["spend_amount"] }).containsExactly("100")
        assertThat(read(content) { it["SPEND_AMOUNT"] }).containsExactly("100")
        assertThat(read(content) { it["createdAt"] }).containsExactly("2026-01-01")
    }

    @Test
    fun `CsvRow знает номер строки и список колонок`() {
        val rows = read("id,name\n1,A\n2,B\n".toByteArray()) { "${it.lineNumber}:${it.columns.joinToString("|")}" }

        // Нумерация — как в текстовом редакторе: заголовок — строка 1, первая запись — 2.
        assertThat(rows).containsExactly("2:id|name", "3:id|name")
    }

    @Test
    fun `строка переживает итерацию и доезжает до цикла целой`() {
        // Самый вероятный способ применения: readCsv отдаёт сами строки, а разбор идёт в each.
        // Если бы Jackson переиспользовал массив ячеек между строками, все элементы стали бы
        // копией последней — и это не заметил бы ни один тест, читающий значение сразу.
        val file = tmp.resolve("in.csv")
        Files.writeString(file, "id,name\n1,A\n2,B\n3,C\n")
        val seen = mutableListOf<String>()
        val migration = object : Migration("ROW-RETAINED") {
            override fun MigrationScope.run() {
                each(readCsv(file.toString()) { it }.toList()) { seen += "${it.lineNumber}:${it["name"]}" }
            }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp.resolve("out"))

        assertThat(outcome.failure).isNull()
        assertThat(seen).containsExactly("2:A", "3:B", "4:C")
    }

    @Test
    fun `обращение к несуществующей колонке говорит, что есть в файле`() {
        val file = tmp.resolve("in.csv")
        Files.writeString(file, "id,name\n1,A\n")
        val migration = object : Migration("MISSING-COLUMN") {
            override fun MigrationScope.run() {
                readCsv(file.toString()) { it["spend"] }.toList()
            }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp.resolve("out"))

        assertThat(outcome.failure).isInstanceOf(IllegalArgumentException::class.java)
        assertThat(outcome.failure).hasMessageContaining("spend").hasMessageContaining("id, name")
    }

    @Test
    fun `коллизия имён после нормализации отвергает файл`() {
        val file = tmp.resolve("in.csv")
        Files.writeString(file, "spend_amount,spendAmount\n1,2\n")
        val migration = object : Migration("COLLISION") {
            override fun MigrationScope.run() {
                readCsv(file.toString()) { it["spendAmount"] }.toList()
            }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp.resolve("out"))

        assertThat(outcome.failure).isInstanceOf(IllegalArgumentException::class.java)
        assertThat(outcome.failure)
            .hasMessageContaining("spend_amount")
            .hasMessageContaining("spendAmount")
    }

    @Test
    fun `строка в аудите показывает номер и значения`() {
        val file = tmp.resolve("in.csv")
        Files.writeString(file, "id,name\n1,A\n")
        var rendered = ""
        val migration = object : Migration("ROW-TOSTRING") {
            override fun MigrationScope.run() {
                readCsv(file.toString()) { rendered = it.toString(); it["id"] }.toList()
            }
        }

        MigrationTest.run(migration, outputFolder = tmp.resolve("out"))

        assertThat(rendered).contains("line 2").contains("id=1").contains("name=A")
    }
}
