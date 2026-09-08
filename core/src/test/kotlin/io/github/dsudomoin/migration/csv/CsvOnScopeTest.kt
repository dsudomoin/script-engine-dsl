package io.github.dsudomoin.migration.csv

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationScope
import io.github.dsudomoin.migration.MigrationTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

class CsvOnScopeTest {

    @TempDir
    lateinit var tmp: Path

    @Test
    fun `csv пишет заголовок и строки и закрывается движком`() {
        val migration = object : Migration("CSV-001") {
            override fun MigrationScope.run() {
                val out = csv("result.csv", "id", "tier")
                each(listOf(1L, 2L)) { out.row(it, "GOLD") }
            }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp)

        assertThat(outcome.failure).isNull()
        assertThat(Files.readAllLines(tmp.resolve("result.csv")))
            .containsExactly("id,tier", "1,GOLD", "2,GOLD")
    }

    @Test
    fun `повторный csv с тем же именем возвращает тот же handle`() {
        val migration = object : Migration("CSV-002") {
            override fun MigrationScope.run() {
                each(listOf(1L, 2L)) { id ->
                    val out = csv("loop.csv", "id")
                    out.row(id)
                }
            }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp)

        assertThat(outcome.failure).isNull()
        assertThat(Files.readAllLines(tmp.resolve("loop.csv"))).containsExactly("id", "1", "2")
    }

    @Test
    fun `зарезервированное имя отвергается`() {
        val migration = object : Migration("CSV-003") {
            override fun MigrationScope.run() {
                csv("errors.csv", "id")
            }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp)

        assertThat(outcome.failure).isInstanceOf(IllegalArgumentException::class.java)
        assertThat(outcome.failure).hasMessageContaining("reserved")
    }

    @Test
    fun `путь за пределы папки артефактов отвергается`() {
        val migration = object : Migration("CSV-004") {
            override fun MigrationScope.run() {
                csv("../escape.csv", "id")
            }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp)

        assertThat(outcome.failure).isInstanceOf(IllegalArgumentException::class.java)
        assertThat(outcome.failure).hasMessageContaining("outputFolder")
    }

    @Test
    fun `readCsv читает файл с classpath и маппит строки`() {
        val seen = mutableListOf<Pair<String, Int>>()
        val migration = object : Migration("CSV-005") {
            override fun MigrationScope.run() {
                val rows = readCsv("input/contracts.csv", classpath = true) { row ->
                    row["contract"] to row["value"].toInt()
                }
                each(rows) { seen += it }
            }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp)

        assertThat(outcome.failure).isNull()
        assertThat(seen).containsExactly("A-1" to 10, "A-2" to 20, "A-3" to 30)
        assertThat(outcome.report.processed).isEqualTo(3)
    }

    @Test
    fun `битая строка под Skip уезжает в sourceSkipped`() {
        val file = tmp.resolve("broken.csv")
        Files.writeString(file, "contract,value\nA-1,10\nA-2,не-число\nA-3,30\n")
        val seen = mutableListOf<Int>()
        val migration = object : Migration("CSV-006") {
            override fun MigrationScope.run() {
                val rows = readCsv(file.toString(), onRowError = ItemError.Skip) { row ->
                    row["value"].toInt()
                }
                each(rows) { seen += it }
            }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp)

        assertThat(outcome.failure).isNull()
        assertThat(seen).containsExactly(10, 30)
        assertThat(outcome.report.sourceSkipped).isEqualTo(1)
        assertThat(outcome.report.processed).isEqualTo(2)
    }
}
