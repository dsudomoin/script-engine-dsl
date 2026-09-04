package io.github.dsudomoin.migration.csv

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.internal.RunContext
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/**
 * Политика на битую строку. До появления `onRowError` ошибка маппера летела из генератора
 * `Sequence` мимо `OnError` цикла: одна плохая строка в CSV валила весь прогон, хотя
 * export-архетип в документации обещал ровно обратное.
 */
class CsvReadRowErrorTest {

    private fun csv(dir: Path): Path {
        val f = dir.resolve("in.csv")
        Files.writeString(f, "id,spend\n1,100\n2,NOT_A_NUMBER\n3,300\n")
        return f
    }

    @Test
    fun `Skip пропускает битую строку, аудитит её и отдаёт остальные`(@TempDir dir: Path) {
        val ctx = RunContext.test(outputFolder = dir.resolve("out"))

        val rows = with(ctx) {
            readCsv(csv(dir), onRowError = ItemError.Skip) { it.getValue("spend").toLong() }.toList()
        }

        assertThat(rows).containsExactly(100L, 300L)
        assertThat(ctx.report.build().sourceSkipped).isEqualTo(1)
        ctx.errors.close()
        assertThat(Files.readString(dir.resolve("out").resolve("errors.csv"))).contains("NumberFormatException")
    }

    @Test
    fun `Fail по умолчанию — битая строка валит чтение`(@TempDir dir: Path) {
        val ctx = RunContext.test(outputFolder = dir.resolve("out"))

        val thrown = catchThrowable {
            with(ctx) { readCsv(csv(dir)) { it.getValue("spend").toLong() }.toList() }
        }

        assertThat(thrown).isInstanceOf(NumberFormatException::class.java)
    }

    @Test
    fun `пропущенные строки считаются отдельно от item-skip`(@TempDir dir: Path) {
        // Порог ошибок больше не живёт в readCsv: он считается по стадии. И отброшенная строка
        // не является item-skip'ом — до стадии она не дошла, поэтому счётчик отдельный.
        val ctx = RunContext.test(outputFolder = dir.resolve("out"))
        val f = dir.resolve("many.csv")
        Files.writeString(f, "id,spend\n1,X\n2,Y\n")

        val rows = with(ctx) { readCsv(f, onRowError = ItemError.Skip) { it.getValue("spend").toLong() }.toList() }

        assertThat(rows).isEmpty()
        assertThat(ctx.report.build().sourceSkipped).isEqualTo(2)
        assertThat(ctx.report.build().skipped).describedAs("item-skip здесь не при чём").isZero()
    }

    @Test
    fun `недопотреблённая последовательность не оставляет открытый дескриптор`(@TempDir dir: Path) {
        val ctx = RunContext.test(outputFolder = dir.resolve("out"))

        // take(1) бросает корутину генератора на полпути: use-блок внутри неё не доигрывает,
        // и без регистрации в реестре поток остался бы открытым до конца процесса.
        val first = with(ctx) { readCsv(csv(dir)) { it.getValue("id") }.take(1).toList() }

        assertThat(first).containsExactly("1")
        ctx.closeRegistered()
        assertThat(ctx.report.build().warnings)
            .describedAs("закрытие зарегистрированного потока проходит без предупреждений")
            .isEmpty()
    }
}
