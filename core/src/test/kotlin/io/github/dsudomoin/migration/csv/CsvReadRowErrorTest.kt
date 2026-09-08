package io.github.dsudomoin.migration.csv

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationScope
import io.github.dsudomoin.migration.MigrationTest
import io.github.dsudomoin.migration.report.MigrationReport
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/**
 * Политика на битую строку. До появления `onRowError` ошибка маппера летела из генератора
 * `Sequence` мимо политики цикла: одна плохая строка в CSV валила весь прогон.
 */
class CsvReadRowErrorTest {

    @TempDir
    lateinit var tmp: Path

    private lateinit var input: Path

    @BeforeEach
    fun setUp() {
        input = tmp.resolve("in.csv")
        Files.writeString(input, "id,spend\n1,100\n2,NOT_A_NUMBER\n3,300\n")
    }

    private fun readInto(
        sink: MutableList<Long>,
        onRowError: ItemError<Map<String, String>>,
        file: Path = input,
    ): Pair<MigrationReport, Throwable?> {
        val migration = object : Migration("CSV-ROWS") {
            override fun MigrationScope.run() {
                readCsv(file.toString(), onRowError = onRowError) { it.getValue("spend").toLong() }
                    .forEach { sink += it }
            }
        }
        val outcome = MigrationTest.run(migration, outputFolder = tmp.resolve("out"))
        return outcome.report to outcome.failure
    }

    @Test
    fun `Skip пропускает битую строку, аудитит её и отдаёт остальные`() {
        val rows = mutableListOf<Long>()

        val (report, failure) = readInto(rows, ItemError.Skip)

        assertThat(failure).isNull()
        assertThat(rows).containsExactly(100L, 300L)
        assertThat(report.sourceSkipped).isEqualTo(1)
        assertThat(Files.readString(tmp.resolve("out").resolve("errors.csv")))
            .contains("NumberFormatException")
    }

    @Test
    fun `Fail по умолчанию — битая строка валит чтение`() {
        val rows = mutableListOf<Long>()

        val (_, failure) = readInto(rows, ItemError.Fail)

        assertThat(failure).isInstanceOf(NumberFormatException::class.java)
        assertThat(rows).containsExactly(100L)
    }

    @Test
    fun `отброшенные строки считаются отдельно от item-skip`() {
        // Отброшенная строка не является item-skip'ом: до цикла она не дошла, поэтому
        // тождество processed = successful + skipped + failed остаётся целым.
        val allBroken = tmp.resolve("many.csv")
        Files.writeString(allBroken, "id,spend\n1,X\n2,Y\n")
        val rows = mutableListOf<Long>()

        val (report, failure) = readInto(rows, ItemError.Skip, allBroken)

        assertThat(failure).isNull()
        assertThat(rows).isEmpty()
        assertThat(report.sourceSkipped).isEqualTo(2)
        assertThat(report.skipped).describedAs("item-skip здесь ни при чём").isZero()
    }

    @Test
    fun `недопотреблённая последовательность не оставляет открытый дескриптор`() {
        val first = mutableListOf<String>()
        val migration = object : Migration("CSV-PARTIAL") {
            override fun MigrationScope.run() {
                // take(1) бросает корутину генератора на полпути: use-блок внутри неё не
                // доигрывает, и без регистрации в реестре поток остался бы открытым до конца процесса.
                first += readCsv(input.toString()) { it.getValue("id") }.take(1).toList()
            }
        }

        val outcome = MigrationTest.run(migration, outputFolder = tmp.resolve("out"))

        assertThat(first).containsExactly("1")
        assertThat(outcome.report.warnings)
            .describedAs("закрытие зарегистрированного потока проходит без предупреждений")
            .isEmpty()
    }
}
