package io.github.dsudomoin.migration.error

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

class CsvFileErrorReporterTest {

    @Test
    fun `файл не создаётся до первой ошибки`(@TempDir tmp: Path) {
        val csv = tmp.resolve("errors.csv")
        val log = tmp.resolve("errors.log")
        val reporter = CsvFileErrorReporter("SAMPLE-001", "team", csv, log)

        assertThat(Files.exists(csv)).isFalse()
        assertThat(Files.exists(log)).isFalse()

        reporter.close()
    }

    @Test
    fun `report пишет CSV строку и полный stacktrace в log`(@TempDir tmp: Path) {
        val csv = tmp.resolve("errors.csv")
        val log = tmp.resolve("errors.log")
        val reporter = CsvFileErrorReporter("SAMPLE-001", "team", csv, log)
        val error = IllegalStateException("broken")

        reporter.report(error, item = "item-123")
        reporter.close()

        val csvLines = Files.readAllLines(csv)
        assertThat(csvLines[0]).isEqualTo("timestamp,migration,author,itemRepr,errorClass,errorMessage")
        assertThat(csvLines[1]).contains("SAMPLE-001", "team", "item-123", "IllegalStateException", "broken")

        val logContent = Files.readString(log)
        assertThat(logContent).contains("SAMPLE-001", "item=item-123", "java.lang.IllegalStateException: broken")
    }

    @Test
    fun `includeItem - per-type сериализатор`(@TempDir tmp: Path) {
        val reporter = CsvFileErrorReporter("T", "a", tmp.resolve("e.csv"), tmp.resolve("e.log"))
        reporter.includeItem<Map<String, Int>> { "size=${it.size}" }

        reporter.report(RuntimeException("x"), item = mapOf("a" to 1, "b" to 2))
        reporter.close()

        val csv = Files.readString(tmp.resolve("e.csv"))
        assertThat(csv).contains("size=2")
    }

    @Test
    fun `re-run перезаписывает старый errors_csv, не дописывает header посередине`(@TempDir tmp: Path) {
        val csv = tmp.resolve("errors.csv")
        val log = tmp.resolve("errors.log")

        // Первый прогон — пишем 2 ошибки.
        CsvFileErrorReporter("T", "a", csv, log).use { reporter ->
            reporter.report(RuntimeException("first-1"), item = "i-1")
            reporter.report(RuntimeException("first-2"), item = "i-2")
        }
        val firstLines = Files.readAllLines(csv).size
        assertThat(firstLines).isEqualTo(3)         // header + 2 ошибки

        // Второй прогон — новый Reporter на тот же путь, пишем 1 ошибку.
        CsvFileErrorReporter("T", "a", csv, log).use { reporter ->
            reporter.report(RuntimeException("second-only"), item = "i-99")
        }

        val lines = Files.readAllLines(csv)
        // ровно 1 header + 1 строка. Старые first-1, first-2 затёрты, header посередине не появился.
        assertThat(lines).hasSize(2)
        assertThat(lines[0]).isEqualTo("timestamp,migration,author,itemRepr,errorClass,errorMessage")
        assertThat(lines[1]).contains("second-only")
        // Header встречается ровно один раз во всём файле.
        assertThat(lines.count { it.startsWith("timestamp,") }).isEqualTo(1)
    }

    @Test
    fun `truncate больше 500 символов по умолчанию`(@TempDir tmp: Path) {
        val reporter = CsvFileErrorReporter("T", "a", tmp.resolve("e.csv"), tmp.resolve("e.log"))
        val long = "x".repeat(1000)

        reporter.report(RuntimeException("x"), item = long)
        reporter.close()

        val csv = Files.readString(tmp.resolve("e.csv"))
        val itemField = csv.lines()[1].split(",")[3].trim('"')
        assertThat(itemField.length).isLessThanOrEqualTo(503)
        assertThat(itemField).endsWith("...")
    }
}
