package io.github.dsudomoin.migration.plan

import io.github.dsudomoin.migration.migration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class MigrationPlanTest {

    @Test
    fun `построение плана не исполняет ни input, ни source, ни handler`() {
        var touched = false

        val plan = migration(name = "M-001", author = "tester") {
            input("payload") { touched = true; 42 }
            source(items = { touched = true; sequenceOf(1) }) { touched = true }
        }

        assertThat(touched).isFalse()
        assertThat(plan.name).isEqualTo("M-001")
        assertThat(plan.author).isEqualTo("tester")
        assertThat(plan.stages).hasSize(1)
    }

    @Test
    fun `стадии сохраняют порядок объявления`() {
        val plan = migration(name = "M-002", author = "tester") {
            source(name = "first", items = { emptySequence<Int>() }) { }
            source(name = "second", items = { emptySequence<Int>() }) { }
            source(name = "third", items = { emptySequence<Int>() }) { }
        }

        assertThat(plan.stages.map { it.name }).containsExactly("first", "second", "third")
    }

    @Test
    fun `единственная стадия может быть безымянной`() {
        val plan = migration(name = "M-003", author = "tester") {
            source(items = { emptySequence<Int>() }) { }
        }

        assertThat(plan.stages.single().name).isNull()
    }

    @Test
    fun `при двух стадиях безымянная запрещена`() {
        assertThatThrownBy {
            migration(name = "M-004", author = "tester") {
                source(name = "named", items = { emptySequence<Int>() }) { }
                source(items = { emptySequence<Int>() }) { }
            }
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("name")
    }

    @Test
    fun `дублирующиеся имена стадий запрещены`() {
        assertThatThrownBy {
            migration(name = "M-005", author = "tester") {
                source(name = "same", items = { emptySequence<Int>() }) { }
                source(name = "same", items = { emptySequence<Int>() }) { }
            }
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("same")
    }

    @Test
    fun `дублирующиеся имена inputs запрещены`() {
        assertThatThrownBy {
            migration(name = "M-006", author = "tester") {
                input("dup") { 1 }
                input("dup") { 2 }
                source(items = { emptySequence<Int>() }) { }
            }
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("dup")
    }

    @Test
    fun `миграция без стадий запрещена`() {
        assertThatThrownBy {
            migration(name = "M-007", author = "tester") { }
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("stage")
    }

    @Test
    fun `зарезервированные имена артефактов запрещены`() {
        listOf("errors.csv", "errors.log", "migration.log", "./errors.csv", "sub/../errors.log")
            .forEach { name ->
                assertThatThrownBy {
                    migration("R", "t") { output(name); source(items = { sequenceOf(1) }) { } }
                }
                    .describedAs(name)
                    .isInstanceOf(IllegalArgumentException::class.java)
                    .hasMessageContaining("reserved")
            }
    }

    @Test
    fun `output не может выйти за папку артефактов`() {
        listOf("../escape.csv", "/tmp/absolute.csv").forEach { name ->
            assertThatThrownBy {
                migration("R", "t") { output(name); source(items = { sequenceOf(1) }) { } }
            }
                .describedAs(name)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("outputFolder")
        }
    }

    @Test
    fun `обычные и вложенные имена по-прежнему разрешены`() {
        val plan = migration("R", "t") {
            output("report.csv", "a")
            output("nested/report.csv", "a")
            source(items = { sequenceOf(1) }) { }
        }
        assertThat(plan.outputs.map { it.filename }).containsExactly("report.csv", "nested/report.csv")
    }
}
