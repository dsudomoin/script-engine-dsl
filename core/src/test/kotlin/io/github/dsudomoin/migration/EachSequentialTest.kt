package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.internal.ErrorThresholdExceeded
import io.github.dsudomoin.migration.internal.MigrationRun
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class EachSequentialTest {

    @TempDir
    lateinit var tmp: Path

    private class Counting(private val sink: MutableList<Int>) : Migration("COUNTING") {
        override fun MigrationScope.run() {
            each(listOf(1, 2, 3)) { sink += it }
        }
    }

    @Test
    fun `проходит все элементы в порядке источника`() {
        val seen = mutableListOf<Int>()
        val run = MigrationRun.forTest(outputFolder = tmp)

        run.executeBody(Counting(seen))

        assertThat(seen).containsExactly(1, 2, 3)
        assertThat(run.report.processedCount()).isEqualTo(3)
    }

    @Test
    fun `Skip аудитирует упавший элемент и продолжает`() {
        val seen = mutableListOf<Int>()
        val migration = object : Migration("SKIPPING") {
            override fun MigrationScope.run() {
                each(listOf(1, 2, 3), onItemError = ItemError.Skip) {
                    if (it == 2) error("boom")
                    seen += it
                }
            }
        }
        val run = MigrationRun.forTest(outputFolder = tmp)

        run.executeBody(migration)

        assertThat(seen).containsExactly(1, 3)
        val report = run.report.build()
        assertThat(report.processed).isEqualTo(3)
        assertThat(report.successful).isEqualTo(2)
        assertThat(report.skipped).isEqualTo(1)
    }

    @Test
    fun `Fail пробрасывает ошибку элемента`() {
        val migration = object : Migration("FAILING") {
            override fun MigrationScope.run() {
                each(listOf(1, 2, 3)) { if (it == 2) error("boom") }
            }
        }
        val run = MigrationRun.forTest(outputFolder = tmp)

        assertThatThrownBy { run.executeBody(migration) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessage("boom")
    }

    @Test
    fun `Handle решает судьбу элемента по типу исключения`() {
        val seen = mutableListOf<Int>()
        val migration = object : Migration("HANDLING") {
            override fun MigrationScope.run() {
                val policy = ItemError.Handle<Int> { e, _ ->
                    if (e is IllegalArgumentException) ItemError.Decision.Skip else ItemError.Decision.Fail
                }
                each(listOf(1, 2, 3), onItemError = policy) {
                    if (it == 2) throw IllegalArgumentException("пропускаем")
                    seen += it
                }
            }
        }
        val run = MigrationRun.forTest(outputFolder = tmp)

        run.executeBody(migration)

        assertThat(seen).containsExactly(1, 3)
        assertThat(run.report.build().skipped).isEqualTo(1)
    }

    @Test
    fun `порог ошибок валит цикл`() {
        val migration = object : Migration("THRESHOLD") {
            override fun MigrationScope.run() {
                each((1..10).toList(), onItemError = ItemError.Skip, errorThreshold = 2) {
                    error("boom")
                }
            }
        }
        val run = MigrationRun.forTest(outputFolder = tmp)

        assertThatThrownBy { run.executeBody(migration) }
            .isInstanceOf(ErrorThresholdExceeded::class.java)
    }

    @Test
    fun `each возвращает свои счётчики`() {
        var result: EachResult? = null
        val migration = object : Migration("RESULT") {
            override fun MigrationScope.run() {
                result = each(listOf(1, 2, 3), onItemError = ItemError.Skip) {
                    if (it == 3) error("boom")
                }
            }
        }

        MigrationRun.forTest(outputFolder = tmp).executeBody(migration)

        assertThat(result!!.processed).isEqualTo(3)
        assertThat(result!!.successful).isEqualTo(2)
        assertThat(result!!.skipped).isEqualTo(1)
        assertThat(result!!.failed).isEqualTo(0)
    }

    @Test
    fun `dryRun виден в теле миграции и внутри лямбды each`() {
        val insideBody = mutableListOf<Boolean>()
        val insideEach = mutableListOf<Boolean>()
        val migration = object : Migration("DRY") {
            override fun MigrationScope.run() {
                insideBody += dryRun
                each(listOf(1)) { insideEach += dryRun }
            }
        }

        MigrationRun.forTest(outputFolder = tmp, dryRun = true).executeBody(migration)

        assertThat(insideBody).containsExactly(true)
        assertThat(insideEach).containsExactly(true)
    }
}
