package io.github.dsudomoin.migration.csv

import io.github.dsudomoin.migration.internal.RunContext
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class CsvWriteTest {

    @Test
    fun `openCsv - пишет header сразу и row дописывает`(@TempDir tmp: Path) {
        val ctx = RunContext.test()
        val out = tmp.resolve("out.csv")

        with(ctx) {
            val csv = openCsv(out, "id", "status")
            csv.row(1, "ok")
            csv.row(2, "ok")
        }
        ctx.closeRegistered()

        val lines = Files.readAllLines(out)
        assertThat(lines).containsExactly("id,status", "1,ok", "2,ok")
    }

    @Test
    fun `openCsv - header виден до первого row`(@TempDir tmp: Path) {
        val ctx = RunContext.test()
        val out = tmp.resolve("out.csv")

        with(ctx) { openCsv(out, "a", "b") }
        // ничего не пишем, файл уже должен иметь header
        val lines = Files.readAllLines(out)
        assertThat(lines).containsExactly("a,b")
    }

    @Test
    fun `dry-run - файл всё равно пишется`(@TempDir tmp: Path) {
        val ctx = RunContext.test(dryRun = true)
        val out = tmp.resolve("out.csv")

        with(ctx) {
            val csv = openCsv(out, "x")
            csv.row(1)
        }
        ctx.closeRegistered()

        assertThat(Files.exists(out)).isTrue()
        assertThat(Files.readAllLines(out)).containsExactly("x", "1")
    }

    @Test
    fun `openCsv - регистрируется и закрывается через closeRegistered`(@TempDir tmp: Path) {
        val ctx = RunContext.test()
        val out = tmp.resolve("out.csv")

        val csv = with(ctx) { openCsv(out, "x") }
        csv.row(1)
        // ctx auto-close
        ctx.closeRegistered()

        // после close - row бросает
        assertThatThrownBy { csv.row(2) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("closed")
    }

    @Test
    fun `close идемпотентен - повторный вызов no-op`(@TempDir tmp: Path) {
        val ctx = RunContext.test()
        val out = tmp.resolve("out.csv")
        val csv = with(ctx) { openCsv(out, "x") }
        csv.row(1)

        csv.close()
        csv.close()
        csv.close()

        assertThat(Files.readAllLines(out)).containsExactly("x", "1")
    }

    @Test
    fun `row thread-safe под параллельной записью`(@TempDir tmp: Path) {
        val ctx = RunContext.test()
        val out = tmp.resolve("out.csv")
        val csv = with(ctx) { openCsv(out, "thread", "n") }

        val workers = 8
        val perWorker = 250
        val expected = workers * perWorker
        val written = AtomicInteger()

        val pool = Executors.newFixedThreadPool(workers)
        val start = CountDownLatch(1)
        val done = CountDownLatch(workers)

        repeat(workers) { id ->
            pool.submit {
                start.await()
                repeat(perWorker) { i ->
                    csv.row(id, i)
                    written.incrementAndGet()
                }
                done.countDown()
            }
        }
        start.countDown()
        check(done.await(10, TimeUnit.SECONDS)) { "parallel row timed out" }
        pool.shutdown()
        check(pool.awaitTermination(5, TimeUnit.SECONDS))
        ctx.closeRegistered()

        val lines = Files.readAllLines(out)
        assertThat(written.get()).isEqualTo(expected)
        assertThat(lines.size).isEqualTo(expected + 1) // +header
        assertThat(lines.first()).isEqualTo("thread,n")
        // каждая строка валидна — два числовых поля, без "склеек" или ломаных строк
        lines.drop(1).forEach { line ->
            val parts = line.split(",")
            assertThat(parts).hasSize(2)
            assertThat(parts[0].toIntOrNull()).isNotNull()
            assertThat(parts[1].toIntOrNull()).isNotNull()
        }
    }

    @Test
    fun `openCsv(filename) - резолвится в ctx outputFolder`(@TempDir tmp: Path) {
        val ctx = RunContext.test(outputFolder = tmp)

        with(ctx) {
            val csv = openCsv("nested/sub/out.csv", "x", "y")
            csv.row(1, 2)
            csv.row(3, 4)
        }
        ctx.closeRegistered()

        val target = tmp.resolve("nested/sub/out.csv")
        assertThat(Files.exists(target)).isTrue()
        assertThat(Files.readAllLines(target)).containsExactly("x,y", "1,2", "3,4")
    }

    @Test
    fun `escape - запятые, кавычки и переводы строк квотируются`(@TempDir tmp: Path) {
        val ctx = RunContext.test()
        val out = tmp.resolve("out.csv")

        with(ctx) {
            val csv = openCsv(out, "a", "b")
            csv.row("plain", "with,comma")
            csv.row("with\"quote", "with\nnewline")
        }
        ctx.closeRegistered()

        val text = Files.readString(out)
        assertThat(text).contains("\"with,comma\"")
        assertThat(text).contains("\"with\"\"quote\"")
        assertThat(text).contains("\"with\nnewline\"")
    }
}
