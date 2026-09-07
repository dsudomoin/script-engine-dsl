package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.ScopeEffectsFailed
import io.github.dsudomoin.migration.internal.RunContext
import io.github.dsudomoin.migration.internal.PlanInterpreter
import io.github.dsudomoin.migration.migration
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/**
 * Учёт async-публикации. Обработчик засчитывает item в момент отправки, а брокер отвечает позже,
 * поэтому исход доставки известен только на барьере стадии. Сам [KafkaOps] ничего не считает:
 * его future отдаётся в `publish { }`, и учётом занимается движок.
 */
class KafkaAsyncAccountingTest {

    private fun producer() = MockProducer(false, StringSerializer(), StringSerializer())

    /** Ждёт, пока запись реально попадёт в продюсер: до этого завершать нечего. */
    private fun awaitSent(p: MockProducer<String, String>) {
        val deadline = System.nanoTime() + 5_000_000_000L
        while (p.history().isEmpty() && System.nanoTime() < deadline) Thread.sleep(5)
    }

    @Test
    fun `отказ доставки валит стадию на барьере и попадает в errors csv`(@TempDir dir: Path) {
        val ctx = RunContext.test(outputFolder = dir)
        val p = producer()

        val plan = migration("KAFKA-FAIL", "test") {
            source(completionTimeout = java.time.Duration.ofSeconds(10), items = { sequenceOf("cust-42") }) { key ->
                publish("customers.resync", args = mapOf("key" to key)) {
                    kafka(p).publishAsync("customers.resync", key, """{"id":42}""")
                }
            }
        }

        val worker = Thread {
            awaitSent(p)
            p.errorNext(RuntimeException("broker down"))
        }.apply { isDaemon = true; start() }

        assertThatThrownBy { PlanInterpreter(ctx).execute(plan) }
            .isInstanceOf(ScopeEffectsFailed::class.java)
            .hasRootCauseMessage("broker down")
        worker.join(5_000)

        ctx.errors.close()
        val report = ctx.report.build()
        assertThat(report.failedEffects)
            .describedAs("потерянное сообщение обязано быть видно в отчёте")
            .isEqualTo(1)
        assertThat(report.acknowledgedPublishes).isZero()
        assertThat(Files.readString(dir.resolve("errors.csv")))
            .contains("broker down")
            .contains("cust-42")
    }

    @Test
    fun `подтверждённая доставка считается acked`(@TempDir dir: Path) {
        val ctx = RunContext.test(outputFolder = dir)
        val p = producer()

        val plan = migration("KAFKA-OK", "test") {
            source(completionTimeout = java.time.Duration.ofSeconds(10), items = { sequenceOf("cust-1") }) { key ->
                publish("customers.resync", args = mapOf("key" to key)) {
                    kafka(p).publishAsync("customers.resync", key, "{}")
                }
            }
        }

        val worker = Thread {
            awaitSent(p)
            p.completeNext()
        }.apply { isDaemon = true; start() }

        PlanInterpreter(ctx).execute(plan)
        worker.join(5_000)

        val report = ctx.report.build()
        assertThat(report.acknowledgedPublishes).isEqualTo(1)
        assertThat(report.failedEffects).isZero()
        assertThat(p.history()).hasSize(1)
    }

    @Test
    fun `успешная доставка ничего не портит`(@TempDir dir: Path) {
        val ctx = RunContext.test(outputFolder = dir)
        val p = producer()

        val ops = with(handler(ctx)) { kafka(p) }
        val future = ops.publishAsync("customers.resync", "cust-1", "{}")
        p.completeNext()

        assertThat(future.get()?.topic).isEqualTo("customers.resync")
        ops.close()
        assertThat(p.history()).hasSize(1)
    }

    @Test
    fun `под dry-run сообщение не уходит в брокер`(@TempDir dir: Path) {
        val ctx = RunContext.test(dryRun = true, outputFolder = dir)
        val p = producer()

        val result = with(handler(ctx)) { kafka(p).publishAsync("customers.resync", "cust-1", "{}") }.get()

        assertThat(result).isNull()
        assertThat(p.history()).isEmpty()
        assertThat(ctx.report.build().dryRunSkipped).containsEntry("kafka.publishAsync", 1L)
    }

    @Test
    fun `kafka() и topic() мемоизируются на прогон, а не плодят объекты в цикле`(@TempDir dir: Path) {
        val ctx = RunContext.test(outputFolder = dir)
        val p = producer()

        // Вызов внутри forEach — штатный сценарий; без мемоизации реестр ресурсов рос бы
        // по объекту на item и сам стал бы утечкой на миллионных прогонах.
        val opsList = (1..100).map { with(handler(ctx)) { kafka(p) } }
        val topics = (1..100).map { with(handler(ctx)) { topic(p, "customers.resync") } }

        assertThat(opsList.distinct()).hasSize(1)
        assertThat(topics.distinct()).hasSize(1)
    }
}
