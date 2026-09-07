package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.internal.RunContext
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * `topic()` собирает свой хендл из `kafka()`, а оба мемоизируются через один и тот же
 * `shared`-кэш. Пока `kafka()` вызывался внутри фабрики `shared`, это был вложенный
 * `computeIfAbsent` на одной `ConcurrentHashMap` — `IllegalStateException("Recursive update")`,
 * срабатывающий по совпадению бина и потому плавающий от запуска к запуску.
 */
class KafkaTopicSharedTest {

    private fun producer() = MockProducer(true, StringSerializer(), StringSerializer())

    @Test
    fun `topic на холодном кэше не срывается в рекурсивный computeIfAbsent`() {
        val p = producer()
        val ctx = RunContext.test()

        val handle = with(handler(ctx)) { topic(p, "orders.resync") }

        assertThat(handle).isNotNull()
    }

    @Test
    fun `повторный topic отдаёт тот же хендл, и kafka мемоизирован отдельно`() {
        val p = producer()
        val ctx = RunContext.test()

        with(handler(ctx)) {
            val first = topic(p, "t")
            val second = topic(p, "t")
            assertThat(second).isSameAs(first)
            assertThat(kafka(p)).isSameAs(kafka(p))
        }
    }
}
