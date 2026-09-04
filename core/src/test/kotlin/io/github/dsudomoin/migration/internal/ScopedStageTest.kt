package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.ScopeCompletionTimeout
import io.github.dsudomoin.migration.migration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class ScopedStageTest {

    private fun run(dryRun: Boolean = false, plan: io.github.dsudomoin.migration.MigrationPlan) =
        RunContext.test(dryRun = dryRun).also { PlanInterpreter(it).execute(plan) }

    @Test
    fun `родители и их элементы идут в заданном порядке`() {
        val order = mutableListOf<String>()

        run(plan = migration("M", "t") {
            scoped(
                parents = { sequenceOf("A", "B") },
                items = { parent -> order += "open-$parent"; sequenceOf("$parent-1", "$parent-2") },
            ) { item -> order += "handle-$item" }
        })

        assertThat(order).containsExactly(
            "open-A", "handle-A-1", "handle-A-2",
            "open-B", "handle-B-1", "handle-B-2",
        )
    }

    @Test
    fun `барьер закрывается на каждом родителе до перехода к следующему`() {
        val futures = ConcurrentHashMap<String, CompletableFuture<String>>()
        val secondParentOpened = CountDownLatch(1)

        val worker = Thread {
            run(plan = migration("M", "t") {
                scoped(
                    parents = { sequenceOf("A", "B") },
                    items = { parent ->
                        if (parent == "B") secondParentOpened.countDown()
                        sequenceOf(parent)
                    },
                ) { parent ->
                    publish("send") { futures.computeIfAbsent(parent) { CompletableFuture() } }
                }
            })
        }.apply { isDaemon = true; start() }

        // Пока подтверждение по A не пришло, второй родитель не должен даже открыться.
        assertThat(secondParentOpened.await(400, TimeUnit.MILLISECONDS)).isFalse()

        awaitRegistered(futures, "A").complete("ok")
        assertThat(secondParentOpened.await(5, TimeUnit.SECONDS)).isTrue()

        // Латч срабатывает в items, а publish идёт позже — ждём именно регистрацию.
        awaitRegistered(futures, "B").complete("ok")
        worker.join(5_000)
        assertThat(worker.isAlive).isFalse()
    }

    private fun awaitRegistered(
        futures: ConcurrentHashMap<String, CompletableFuture<String>>,
        key: String,
    ): CompletableFuture<String> {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (System.nanoTime() < deadline) {
            futures[key]?.let { return it }
            Thread.sleep(10)
        }
        error("publish for '$key' was never registered")
    }

    @Test
    fun `таймаут барьера считается по отдельному родителю`() {
        assertThatThrownBy {
            run(plan = migration("M", "t") {
                scoped(
                    parents = { sequenceOf("A") },
                    completionTimeout = Duration.ofMillis(300),
                    items = { parent -> sequenceOf(parent) },
                ) {
                    publish("send") { CompletableFuture<String>() }
                }
            })
        }.isInstanceOf(ScopeCompletionTimeout::class.java)
    }

    @Test
    fun `ресурсы родителя закрываются на его границе, а не в конце прогона`() {
        val closedAt = mutableListOf<String>()
        val handled = mutableListOf<String>()

        run(plan = migration("M", "t") {
            scoped(
                parents = { sequenceOf("A", "B") },
                items = { parent ->
                    scopedResource { closedAt += "closed-$parent" }
                    sequenceOf(parent)
                },
            ) { parent -> handled += "handled-$parent" }
        })

        // Ресурс A обязан закрыться ДО того, как начался родитель B.
        assertThat(closedAt).containsExactly("closed-A", "closed-B")
        assertThat(handled).containsExactly("handled-A", "handled-B")
    }

    @Test
    fun `ошибка внутри родителя останавливает стадию и не трогает следующих`() {
        var openedB = false

        assertThatThrownBy {
            run(plan = migration("M", "t") {
                scoped(
                    parents = { sequenceOf("A", "B") },
                    items = { parent ->
                        if (parent == "B") openedB = true
                        sequenceOf(parent)
                    },
                ) { error("handler failed on it") }
            })
        }.hasMessageContaining("handler failed")

        assertThat(openedB).isFalse()
    }

    @Test
    fun `Skip внутри родителя не прерывает обход остальных родителей`() {
        val handled = mutableListOf<String>()

        val ctx = run(plan = migration("M", "t") {
            scoped(
                parents = { sequenceOf("A", "B") },
                onItemError = ItemError.Skip,
                items = { parent -> sequenceOf(parent) },
            ) { parent ->
                if (parent == "A") error("bad parent")
                handled += parent
            }
        })

        assertThat(handled).containsExactly("B")
        assertThat(ctx.report.build().skipped).isEqualTo(1)
    }
}
