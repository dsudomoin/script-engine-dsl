package io.github.dsudomoin.migration.plan

import io.github.dsudomoin.migration.Input
import io.github.dsudomoin.migration.internal.PlanInterpreter
import io.github.dsudomoin.migration.internal.RunContext
import io.github.dsudomoin.migration.migration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

class InputCacheTest {

    @TempDir
    lateinit var folder: Path

    private fun ctx() = RunContext.test(outputFolder = folder)

    @Test
    fun `nullable input со значением null загружается один раз`() {
        val loads = AtomicInteger()
        lateinit var maybe: Input<String?>
        val plan = migration("NULL", "t") {
            maybe = input<String?>("maybe") { loads.incrementAndGet(); null }
            source(items = {
                assertThat(resolve(maybe)).isNull()
                assertThat(resolve(maybe)).isNull()
                sequenceOf(1)
            }) { }
        }

        PlanInterpreter(ctx()).execute(plan)

        assertThat(loads.get()).describedAs("null — такое же значение, как любое другое").isEqualTo(1)
    }

    @Test
    fun `непустой input по-прежнему загружается один раз`() {
        val loads = AtomicInteger()
        lateinit var value: Input<String>
        val plan = migration("VAL", "t") {
            value = input("value") { loads.incrementAndGet(); "v" }
            source(items = { resolve(value); resolve(value); sequenceOf(1) }) { }
        }

        PlanInterpreter(ctx()).execute(plan)

        assertThat(loads.get()).isEqualTo(1)
    }

    @Test
    fun `упавший загрузчик не кэшируется как успешный null`() {
        val loads = AtomicInteger()
        lateinit var bad: Input<String>
        val plan = migration("BAD", "t") {
            bad = input("bad") { loads.incrementAndGet(); error("cannot load") }
            source(items = {
                runCatching { resolve(bad) }
                runCatching { resolve(bad) }
                sequenceOf(1)
            }) { }
        }

        PlanInterpreter(ctx()).execute(plan)

        assertThat(loads.get()).describedAs("упавший загрузчик обязан быть вызван повторно").isEqualTo(2)
    }

    @Test
    fun `два прогона одного плана не делят кэш`() {
        val loads = AtomicInteger()
        lateinit var once: Input<String>
        val plan = migration("REUSE", "t") {
            once = input("once") { loads.incrementAndGet(); "v" }
            source(items = { resolve(once); sequenceOf(1) }) { }
        }

        PlanInterpreter(ctx()).execute(plan)
        PlanInterpreter(ctx()).execute(plan)

        assertThat(loads.get()).isEqualTo(2)
    }
}
