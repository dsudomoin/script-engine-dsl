package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.WriteOutcome
import io.github.dsudomoin.migration.WriteResult
import io.github.dsudomoin.migration.migration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture

class PlanInterpreterTest {

    private fun run(dryRun: Boolean = false, plan: io.github.dsudomoin.migration.MigrationPlan) =
        RunContext.test(dryRun = dryRun).also { ctx ->
            PlanInterpreter(ctx).execute(plan)
        }

    @Test
    fun `handler вызывается на каждый элемент источника`() {
        val seen = mutableListOf<Int>()

        val ctx = run(plan = migration("M", "t") {
            source(items = { sequenceOf(1, 2, 3) }) { item -> seen += item }
        })

        assertThat(seen).containsExactly(1, 2, 3)
        assertThat(ctx.report.build().successful).isEqualTo(3)
        assertThat(ctx.report.build().processed).isEqualTo(3)
    }

    @Test
    fun `стадии исполняются строго последовательно`() {
        val order = mutableListOf<String>()

        run(plan = migration("M", "t") {
            source(name = "first", items = { order += "src-1"; sequenceOf(1) }) { order += "handle-1" }
            source(name = "second", items = { order += "src-2"; sequenceOf(2) }) { order += "handle-2" }
        })

        assertThat(order).containsExactly("src-1", "handle-1", "src-2", "handle-2")
    }

    @Test
    fun `провал стадии не запускает следующую`() {
        var secondStarted = false

        assertThatThrownBy {
            run(plan = migration("M", "t") {
                source(name = "boom", items = { sequenceOf(1) }) { error("stage failed") }
                source(name = "never", items = { secondStarted = true; sequenceOf(2) }) { }
            })
        }.hasMessageContaining("stage failed")

        assertThat(secondStarted).isFalse()
    }

    @Test
    fun `Skip пропускает сломавшийся элемент и доводит стадию до конца`() {
        val handled = mutableListOf<Int>()

        val ctx = run(plan = migration("M", "t") {
            source(onItemError = ItemError.Skip, items = { sequenceOf(1, 2, 3) }) { item ->
                if (item == 2) error("bad item")
                handled += item
            }
        })

        assertThat(handled).containsExactly(1, 3)
        val report = ctx.report.build()
        assertThat(report.successful).isEqualTo(2)
        assertThat(report.skipped).isEqualTo(1)
    }

    @Test
    fun `Handle видит типизированный элемент`() {
        val classified = mutableListOf<String>()

        val ctx = run(plan = migration("M", "t") {
            source(
                onItemError = ItemError.Handle { _, item: String ->
                    classified += item
                    if (item.startsWith("skip")) ItemError.Decision.Skip else ItemError.Decision.Fail
                },
                items = { sequenceOf("skip-a", "skip-b") },
            ) { error("always") }
        })

        assertThat(classified).containsExactly("skip-a", "skip-b")
        assertThat(ctx.report.build().skipped).isEqualTo(2)
    }

    @Test
    fun `input разрешается один раз за прогон`() {
        var loads = 0

        run(plan = migration("M", "t") {
            val shared = input("shared") { loads++; 42 }
            source(name = "a", items = { sequenceOf(resolve(shared)) }) { }
            source(name = "b", items = { sequenceOf(resolve(shared), resolve(shared)) }) { }
        })

        assertThat(loads).isEqualTo(1)
    }

    @Test
    fun `write возвращает Applied и Rejected и учитывает их раздельно`() {
        val results = mutableListOf<WriteResult>()

        val ctx = run(plan = migration("M", "t") {
            source(items = { sequenceOf(1, 2, 3) }) { item ->
                results += write("acc.update", mapOf("id" to item)) {
                    if (item == 2) WriteOutcome.Rejected("no rows matched") else WriteOutcome.Applied
                }
            }
        })

        assertThat(results).containsExactly(
            WriteResult.Applied,
            WriteResult.Rejected("no rows matched"),
            WriteResult.Applied,
        )
        val report = ctx.report.build()
        assertThat(report.appliedWrites["acc.update"]).isEqualTo(2)
        assertThat(report.rejectedWrites["acc.update"]).isEqualTo(1)
    }

    @Test
    fun `под dry-run write не вызывается и даёт DryRunSkipped`() {
        var executed = false

        val ctx = run(dryRun = true, plan = migration("M", "t") {
            source(items = { sequenceOf(1) }) {
                val result = write("acc.update") { executed = true; WriteOutcome.Applied }
                assertThat(result).isEqualTo(WriteResult.DryRunSkipped)
            }
        })

        assertThat(executed).isFalse()
        assertThat(ctx.report.build().dryRunSkipped["acc.update"]).isEqualTo(1)
    }

    @Test
    fun `под dry-run publish вообще не вызывает send`() {
        var sent = false

        val ctx = run(dryRun = true, plan = migration("M", "t") {
            source(items = { sequenceOf(1) }) {
                publish("kafka.send") { sent = true; CompletableFuture.completedFuture("x") }
            }
        })

        assertThat(sent).isFalse()
        assertThat(ctx.report.build().dryRunSkipped["kafka.send"]).isEqualTo(1)
    }

    @Test
    fun `стадия не завершается, пока не подтверждены все publish`() {
        val pending = CompletableFuture<String>()
        var stageFinished = false

        val worker = Thread {
            run(plan = migration("M", "t") {
                source(items = { sequenceOf(1) }) {
                    publish("kafka.send") { pending }
                }
            })
            stageFinished = true
        }.apply { isDaemon = true; start() }

        Thread.sleep(300)
        assertThat(stageFinished).isFalse()

        pending.complete("ok")
        worker.join(5_000)
        assertThat(stageFinished).isTrue()
    }

    @Test
    fun `отказ publish валит стадию на барьере, а не через item error policy`() {
        var skippedByPolicy = false

        assertThatThrownBy {
            run(plan = migration("M", "t") {
                source(
                    onItemError = ItemError.Handle { _, _: Int ->
                        skippedByPolicy = true
                        ItemError.Decision.Skip
                    },
                    items = { sequenceOf(1) },
                ) {
                    publish("kafka.send") { CompletableFuture.failedFuture<String>(RuntimeException("broker down")) }
                }
            })
        }.hasRootCauseMessage("broker down")

        assertThat(skippedByPolicy).isFalse()
    }
}
