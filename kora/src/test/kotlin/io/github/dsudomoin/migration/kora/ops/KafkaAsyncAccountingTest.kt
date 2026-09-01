package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.internal.DefaultMigrationContext
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/**
 * Учёт async-публикации. `forEach` засчитывает item успешным в момент отправки, а брокер отвечает
 * позже — и возвращаемый future в реальных скриптах почти никто не читает. Раньше отказ доставки
 * не попадал никуда: ни в `errors.csv`, ни в счётчики, ни в код возврата.
 */
class KafkaAsyncAccountingTest {

    private fun producer() = MockProducer(false, StringSerializer(), StringSerializer())

    @Test
    fun `отказ доставки попадает в отчёт и в errors csv`(@TempDir dir: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = dir)
        val p = producer()

        val ops = with(ctx) { kafka(p) }
        val future = ops.publishAsync("customers.resync", "cust-42", """{"id":42}""")
        p.errorNext(RuntimeException("broker down"))

        assertThat(future.isCompletedExceptionally).isTrue()

        ops.close()
        ctx.errors.close()

        assertThat(ctx.report.build().asyncFailed)
            .describedAs("потерянное сообщение обязано быть видно в отчёте")
            .isEqualTo(1)
        assertThat(Files.readString(dir.resolve("errors.csv")))
            .contains("broker down")
            .contains("cust-42")
        assertThat(ctx.report.build().warnings.single()).contains("rejected by the broker")
    }

    @Test
    fun `успешная доставка ничего не портит`(@TempDir dir: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = dir)
        val p = producer()

        val ops = with(ctx) { kafka(p) }
        val future = ops.publishAsync("customers.resync", "cust-1", "{}")
        p.completeNext()

        assertThat(future.get()?.topic).isEqualTo("customers.resync")
        ops.close()
        assertThat(ctx.report.build().asyncFailed).isEqualTo(0)
        assertThat(p.history()).hasSize(1)
    }

    @Test
    fun `под dry-run сообщение не уходит в брокер`(@TempDir dir: Path) {
        val ctx = DefaultMigrationContext.test(dryRun = true, outputFolder = dir)
        val p = producer()

        val result = with(ctx) { kafka(p).publishAsync("customers.resync", "cust-1", "{}") }.get()

        assertThat(result).isNull()
        assertThat(p.history()).isEmpty()
        assertThat(ctx.report.build().dryRunSkipped).containsEntry("kafka.publishAsync", 1L)
    }

    @Test
    fun `kafka() и topic() мемоизируются на прогон, а не плодят объекты в цикле`(@TempDir dir: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = dir)
        val p = producer()

        // Вызов внутри forEach — штатный сценарий; без мемоизации реестр ресурсов рос бы
        // по объекту на item и сам стал бы утечкой на миллионных прогонах.
        val opsList = (1..100).map { with(ctx) { kafka(p) } }
        val topics = (1..100).map { with(ctx) { topic(p, "customers.resync") } }

        assertThat(opsList.distinct()).hasSize(1)
        assertThat(topics.distinct()).hasSize(1)
    }
}
