package io.github.dsudomoin.migration.csv

import io.github.dsudomoin.migration.internal.RunContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

class CsvReadTest {
    @Test
    fun `из classpath - читает строки как Map`() {
        val ctx = RunContext.test()
        val contracts = with(ctx) {
            readCsv("input/contracts.csv", classpath = true) { it["contract"]!! }.toList()
        }
        assertThat(contracts).containsExactly("A-1", "A-2", "A-3")
    }

    @Test
    fun `из файла`(@org.junit.jupiter.api.io.TempDir tmp: Path) {
        val f = tmp.resolve("data.csv")
        Files.writeString(f, "id,name\n1,foo\n2,bar\n")

        val ctx = RunContext.test()
        val rows = with(ctx) { readCsv(f) { r -> r["id"] to r["name"] }.toList() }

        assertThat(rows).containsExactly("1" to "foo", "2" to "bar")
    }
}
