package io.github.dsudomoin.migration.plan

import io.github.dsudomoin.migration.Input
import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.internal.PlanInterpreter
import io.github.dsudomoin.migration.internal.RunContext
import io.github.dsudomoin.migration.migration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

/**
 * Проверка формы из ТЗ: зависимость объявлена в сигнатуре узла, значение приезжает параметром.
 *
 * Каждый сценарий списан с реального места в архетипах — иначе оценка «раздувает / не раздувает»
 * ничего не стоит. Ни одной аннотации типа на стороне вызова здесь быть не должно.
 */
class InputAsParameterTest {

    @TempDir
    lateinit var folder: Path

    private fun ctx() = RunContext.test(outputFolder = folder)

    // correction-archetype.md:256 — плоский source, один input
    @Test
    fun `плоский source с одним input`() {
        val seen = mutableListOf<Long>()
        val plan = migration("FLAT", "t") {
            val ids = input("ids") { listOf(1L, 2L, 3L) }
            source(
                ids,
                name = "update",
                onItemError = ItemError.Skip,
                items = { loaded -> loaded.asSequence() },
            ) { id ->
                seen += id
            }
        }
        PlanInterpreter(ctx()).execute(plan)
        assertThat(seen).containsExactly(1L, 2L, 3L)
    }

    // full-showcase.md:381+400 — один input нужен и родителям, и элементам
    @Test
    fun `scoped, где input нужен и parents, и items`() {
        val handled = mutableListOf<String>()
        val plan = migration("SCOPED", "t") {
            val segments = input("segments") {
                mapOf("gold" to listOf("a", "b"), "silver" to listOf("c"))
            }
            scoped(
                segments,
                parents = { loaded -> loaded.keys.asSequence() },
                name = "per segment",
                // редкий случай: значение нужно и элементам — тот же кэш, без второго запроса
                items = { segment -> resolve(segments).getValue(segment).asSequence() },
            ) { customer ->
                handled += customer
            }
        }
        PlanInterpreter(ctx()).execute(plan)
        assertThat(handled).containsExactlyInAnyOrder("a", "b", "c")
    }

    @Test
    fun `две зависимости в одной стадии`() {
        val out = mutableListOf<String>()
        val plan = migration("TWO", "t") {
            val ids = input("ids") { listOf(1, 2) }
            val names = input("names") { mapOf(1 to "one", 2 to "two") }
            source(
                ids,
                names,
                items = { loadedIds, loadedNames ->
                    loadedIds.asSequence().map { loadedNames.getValue(it) }
                },
            ) { name ->
                out += name
            }
        }
        PlanInterpreter(ctx()).execute(plan)
        assertThat(out).containsExactly("one", "two")
    }

    @Test
    fun `input загружается один раз, даже когда нужен и parents, и items`() {
        val loads = AtomicInteger()
        lateinit var segments: Input<Map<String, List<String>>>
        val plan = migration("ONCE", "t") {
            segments = input("segments") {
                loads.incrementAndGet()
                mapOf("gold" to listOf("a", "b"))
            }
            scoped(
                segments,
                parents = { it.keys.asSequence() },
                // редкий случай: значение нужно и элементам — тот же кэш, без второго запроса
                items = { segment -> resolve(segments).getValue(segment).asSequence() },
            ) { }
        }
        PlanInterpreter(ctx()).execute(plan)
        assertThat(loads.get()).isEqualTo(1)
    }
}
