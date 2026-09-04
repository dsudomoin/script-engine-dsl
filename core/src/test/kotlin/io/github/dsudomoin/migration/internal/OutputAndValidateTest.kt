package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.migration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.io.path.readLines

class OutputAndValidateTest {

    @TempDir
    lateinit var folder: Path

    private fun run(dryRun: Boolean = false, plan: io.github.dsudomoin.migration.MigrationPlan) =
        RunContext.test(dryRun = dryRun, outputFolder = folder)
            .also { PlanInterpreter(it).execute(plan) }

    @Test
    fun `output пишет заголовки и строки в outputFolder`() {
        run(plan = migration("M", "t") {
            val csv = output("result.csv", "id", "tier")
            source(items = { sequenceOf(1, 2) }) { item -> csv.row(item, "GOLD-$item") }
        })

        assertThat(folder.resolve("result.csv").readLines())
            .containsExactly("id,tier", "1,GOLD-1", "2,GOLD-2")
    }

    @Test
    fun `output пишется и под dry-run — он не меняет целевую систему`() {
        run(dryRun = true, plan = migration("M", "t") {
            val csv = output("dry.csv", "id")
            source(items = { sequenceOf(7) }) { item -> csv.row(item) }
        })

        assertThat(folder.resolve("dry.csv").readLines()).containsExactly("id", "7")
    }

    @Test
    fun `два output в одной миграции не мешают друг другу`() {
        run(plan = migration("M", "t") {
            val ok = output("ok.csv", "id")
            val bad = output("bad.csv", "id", "reason")
            source(items = { sequenceOf(1, 2) }) { item ->
                if (item == 1) ok.row(item) else bad.row(item, "rejected")
            }
        })

        assertThat(folder.resolve("ok.csv").readLines()).containsExactly("id", "1")
        assertThat(folder.resolve("bad.csv").readLines()).containsExactly("id,reason", "2,rejected")
    }

    @Test
    fun `output нельзя использовать вне прогона`() {
        val plan = migration("M", "t") {
            val csv = output("orphan.csv", "id")
            source(items = { emptySequence<Int>() }) { }
            // попытка писать прямо при построении плана
            assertThatThrownBy { csv.row(1) }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("orphan.csv")
        }

        assertThat(plan.stages).hasSize(1)
    }

    @Test
    fun `после прогона output отвязывается от файла`() {
        lateinit var captured: io.github.dsudomoin.migration.OutputHandle

        run(plan = migration("M", "t") {
            val csv = output("closed.csv", "id")
            captured = csv
            source(items = { sequenceOf(1) }) { item -> csv.row(item) }
        })

        assertThatThrownBy { captured.row(2) }
            .isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `validate выполняется до первой стадии`() {
        val order = mutableListOf<String>()

        run(plan = migration("M", "t") {
            validate { order += "validate" }
            source(items = { order += "source"; sequenceOf(1) }) { order += "handle" }
        })

        assertThat(order).containsExactly("validate", "source", "handle")
    }

    @Test
    fun `ошибка validate завершает прогон и не запускает стадии`() {
        var sourceOpened = false

        assertThatThrownBy {
            run(plan = migration("M", "t") {
                validate { require(false) { "pageSize must be > 0" } }
                source(items = { sourceOpened = true; sequenceOf(1) }) { }
            })
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("pageSize")

        assertThat(sourceOpened).isFalse()
    }

    @Test
    fun `validate видит тот же контекст прогона`() {
        var seenDryRun: Boolean? = null

        run(dryRun = true, plan = migration("M", "t") {
            validate { seenDryRun = dryRun }
            source(items = { emptySequence<Int>() }) { }
        })

        assertThat(seenDryRun).isTrue()
    }
}
