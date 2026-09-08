package io.github.dsudomoin.migration.csv

import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationScope
import io.github.dsudomoin.migration.MigrationTest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path

class CsvWriteTest {

    @TempDir
    lateinit var tmp: Path

    /** Запускает тело миграции и отдаёт handle наружу — чтобы проверять его после прогона. */
    private fun runWriting(dryRun: Boolean = false, body: MigrationScope.() -> CsvOutput): CsvOutput {
        var handle: CsvOutput? = null
        val migration = object : Migration("CSV-WRITE") {
            override fun MigrationScope.run() {
                handle = body()
            }
        }
        val outcome = MigrationTest.run(migration, outputFolder = tmp, dryRun = dryRun)
        assertThat(outcome.failure).isNull()
        return handle!!
    }

    @Test
    fun `пишет header сразу, row дописывает`() {
        runWriting {
            csv("out.csv", "id", "status").also {
                it.row(1, "ok")
                it.row(2, "ok")
            }
        }

        assertThat(Files.readAllLines(tmp.resolve("out.csv")))
            .containsExactly("id,status", "1,ok", "2,ok")
    }

    @Test
    fun `разделитель применяется и к заголовку, и к строкам`() {
        runWriting {
            csv("out.csv", "id", "status", delimiter = ';').also { it.row(1, "ok") }
        }

        assertThat(Files.readAllLines(tmp.resolve("out.csv"))).containsExactly("id;status", "1;ok")
    }

    @Test
    fun `квотирование идёт по своему разделителю, а не по запятой`() {
        runWriting {
            csv("out.csv", "id", "name", delimiter = ';').also {
                it.row(1, "Иванов; ООО")
                it.row(2, "Петров, ИП")
            }
        }

        // Точка с запятой рвёт строку и потому квотируется; запятая при этом разделителем
        // не является и остаётся обычным символом — иначе файл распухал бы кавычками впустую.
        assertThat(Files.readAllLines(tmp.resolve("out.csv")))
            .containsExactly("id;name", """1;"Иванов; ООО"""", "2;Петров, ИП")
    }

    @Test
    fun `BOM пишется по запросу — Excel открывает кириллицу без бубна`() {
        runWriting {
            csv("out.csv", "имя", bom = true).also { it.row("Иван") }
        }

        val bytes = Files.readAllBytes(tmp.resolve("out.csv"))
        assertThat(bytes.take(3)).containsExactly(0xEF.toByte(), 0xBB.toByte(), 0xBF.toByte())
    }

    @Test
    fun `charset применяется к содержимому`() {
        val cp1251 = Charset.forName("windows-1251")
        runWriting {
            csv("out.csv", "имя", charset = cp1251).also { it.row("Иван") }
        }

        assertThat(Files.readAllLines(tmp.resolve("out.csv"), cp1251)).containsExactly("имя", "Иван")
    }

    @Test
    fun `BOM в кодировке, которая его не умеет, отвергается до записи`() {
        val migration = object : Migration("CSV-BOM-BAD") {
            override fun MigrationScope.run() {
                csv("out.csv", "имя", charset = Charset.forName("windows-1251"), bom = true)
            }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp)

        // Иначе BOM молча уехал бы в файл вопросительным знаком и сломал первую колонку.
        assertThat(outcome.failure).isInstanceOf(IllegalArgumentException::class.java)
        assertThat(outcome.failure).hasMessageContaining("windows-1251")
        assertThat(Files.exists(tmp.resolve("out.csv"))).isFalse()
    }

    @Test
    fun `выход в чужом формате читается обратно своим же readCsv`() {
        val cp1251 = Charset.forName("windows-1251")
        runWriting {
            csv("out.csv", "id", "name", delimiter = ';', charset = cp1251).also {
                it.row(1, "Иванов; ООО")
                it.row(2, "Пётр")
            }
        }

        val seen = mutableListOf<String>()
        val reader = object : Migration("CSV-ROUNDTRIP") {
            override fun MigrationScope.run() {
                readCsv(tmp.resolve("out.csv").toString(), delimiter = ';', charset = cp1251) { it["name"] }
                    .forEach { seen += it }
            }
        }
        assertThat(MigrationTest.run(reader, outputFolder = tmp.resolve("rt")).failure).isNull()
        assertThat(seen).containsExactly("Иванов; ООО", "Пётр")
    }

    @Test
    fun `header виден до первого row`() {
        val migration = object : Migration("CSV-HEADER") {
            override fun MigrationScope.run() {
                csv("out.csv", "a", "b")
                // Файл читается прямо внутри прогона: header флашится при открытии, а не на close.
                assertThat(Files.readAllLines(outputFolder.resolve("out.csv"))).containsExactly("a,b")
            }
        }

        assertThat(MigrationTest.run(migration, outputFolder = tmp).failure).isNull()
    }

    @Test
    fun `под dry-run файл всё равно пишется`() {
        runWriting(dryRun = true) {
            csv("out.csv", "x").also { it.row(1) }
        }

        assertThat(Files.readAllLines(tmp.resolve("out.csv"))).containsExactly("x", "1")
    }

    @Test
    fun `после закрытия прогона row бросает`() {
        val handle = runWriting { csv("out.csv", "x").also { it.row(1) } }

        assertThatThrownBy { handle.row(2) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("closed")
    }

    @Test
    fun `close идемпотентен`() {
        val handle = runWriting { csv("out.csv", "x").also { it.row(1) } }

        handle.close()
        handle.close()

        assertThat(Files.readAllLines(tmp.resolve("out.csv"))).containsExactly("x", "1")
    }

    @Test
    fun `row потокобезопасен под параллельной записью`() {
        val perWorker = 250
        val workers = 8
        val migration = object : Migration("CSV-PARALLEL") {
            override fun MigrationScope.run() {
                val out = csv("out.csv", "worker", "n")
                each((1..workers).toList(), parallel = workers) { id ->
                    repeat(perWorker) { i -> out.row(id, i) }
                }
            }
        }

        assertThat(MigrationTest.run(migration, outputFolder = tmp).failure).isNull()

        val lines = Files.readAllLines(tmp.resolve("out.csv"))
        assertThat(lines).hasSize(workers * perWorker + 1)
        assertThat(lines.first()).isEqualTo("worker,n")
        // Каждая строка цела: два числовых поля, без склеек и обрывов.
        lines.drop(1).forEach { line ->
            val parts = line.split(",")
            assertThat(parts).hasSize(2)
            assertThat(parts[0].toIntOrNull()).isNotNull()
            assertThat(parts[1].toIntOrNull()).isNotNull()
        }
    }

    @Test
    fun `вложенный путь создаёт промежуточные каталоги`() {
        runWriting {
            csv("nested/sub/out.csv", "x", "y").also {
                it.row(1, 2)
                it.row(3, 4)
            }
        }

        assertThat(Files.readAllLines(tmp.resolve("nested/sub/out.csv")))
            .containsExactly("x,y", "1,2", "3,4")
    }

    @Test
    fun `запятые, кавычки и переводы строк квотируются`() {
        runWriting {
            csv("out.csv", "a", "b").also {
                it.row("plain", "with,comma")
                it.row("with\"quote", "with\nnewline")
            }
        }

        val text = Files.readString(tmp.resolve("out.csv"))
        assertThat(text).contains("\"with,comma\"")
        assertThat(text).contains("\"with\"\"quote\"")
        assertThat(text).contains("\"with\nnewline\"")
    }
}
