package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.internal.DefaultMigrationContext
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.kafka.ConfluentKafkaContainer
import java.time.Duration
import java.util.Properties
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaOpsIntegrationTest {

    private val kafka = ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.1").apply { start() }
    private lateinit var producer: Producer<String, String>

    @BeforeAll
    fun setup() {
        val props = Properties().apply {
            setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        }
        producer = KafkaProducer(props)
    }

    @AfterAll
    fun teardown() {
        producer.close()
        kafka.stop()
    }

    @Test
    fun `publish - сообщение доходит до топика`() {
        val topic = "t-" + UUID.randomUUID().toString().take(8)
        val ctx = DefaultMigrationContext.test()

        with(ctx) { kafka(producer).publish(topic, "k1", "v1") }

        val received = consumeOne(topic)
        assertThat(received).isEqualTo("k1" to "v1")
    }

    @Test
    fun `publish под dry-run - ничего не отправляется`() {
        val topic = "t-" + UUID.randomUUID().toString().take(8)
        val ctx = DefaultMigrationContext.test(dryRun = true)

        with(ctx) { kafka(producer).publish(topic, "k-dry", "v-dry") }

        val received = consumeOneOrNull(topic, Duration.ofSeconds(3))
        assertThat(received).isNull()
    }

    @Test
    fun `topic - отправляет в указанный топик`() {
        val topicName = "t-" + UUID.randomUUID().toString().take(8)
        val ctx = DefaultMigrationContext.test()

        with(ctx) {
            val t = topic(producer, topicName)
            t.send("k1", "v1")
        }
        ctx.closeRegistered()

        val received = consumeOne(topicName)
        assertThat(received).isEqualTo("k1" to "v1")
    }

    @Test
    fun `topic - все три send долетают, close дёргает flush`() {
        val topicName = "t-" + UUID.randomUUID().toString().take(8)
        val ctx = DefaultMigrationContext.test()

        with(ctx) {
            val t = topic(producer, topicName)
            t.send("k1", "v1")
            t.send("k2", "v2")
            t.send("k3", "v3")
        }
        ctx.closeRegistered()

        val all = consumeAll(topicName, expected = 3, timeout = Duration.ofSeconds(10))
        assertThat(all).containsExactlyInAnyOrderElementsOf(listOf("k1" to "v1", "k2" to "v2", "k3" to "v3"))
    }

    @Test
    fun `topic под dry-run - send не доходит до брокера`() {
        val topicName = "t-" + UUID.randomUUID().toString().take(8)
        val ctx = DefaultMigrationContext.test(dryRun = true)

        with(ctx) {
            val t = topic(producer, topicName)
            t.send("k-dry", "v-dry")
        }
        ctx.closeRegistered()

        val received = consumeOneOrNull(topicName, Duration.ofSeconds(3))
        assertThat(received).isNull()
    }

    @Test
    fun `topic - close идемпотентен`() {
        val topicName = "t-" + UUID.randomUUID().toString().take(8)
        val ctx = DefaultMigrationContext.test()
        val t = with(ctx) { topic(producer, topicName) }
        t.send("k1", "v1")

        t.close()
        t.close()
        t.close()

        // никаких exception, сообщение долетело
        val received = consumeOne(topicName)
        assertThat(received).isEqualTo("k1" to "v1")
    }

    private fun newConsumer(): KafkaConsumer<String, String> {
        val props = Properties().apply {
            setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID())
            setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        }
        return KafkaConsumer(props)
    }

    private fun consumeOne(topic: String): Pair<String, String> {
        newConsumer().use { c ->
            c.subscribe(listOf(topic))
            val deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos()
            while (System.nanoTime() < deadline) {
                val records = c.poll(Duration.ofMillis(500))
                val r = records.firstOrNull()
                if (r != null) return r.key() to r.value()
            }
            error("No message received in topic $topic")
        }
    }

    private fun consumeOneOrNull(topic: String, timeout: Duration): Pair<String, String>? {
        newConsumer().use { c ->
            c.subscribe(listOf(topic))
            val deadline = System.nanoTime() + timeout.toNanos()
            while (System.nanoTime() < deadline) {
                val records = c.poll(Duration.ofMillis(500))
                val r = records.firstOrNull()
                if (r != null) return r.key() to r.value()
            }
            return null
        }
    }

    private fun consumeAll(topic: String, expected: Int, timeout: Duration): List<Pair<String, String>> {
        newConsumer().use { c ->
            c.subscribe(listOf(topic))
            val deadline = System.nanoTime() + timeout.toNanos()
            val out = mutableListOf<Pair<String, String>>()
            while (out.size < expected && System.nanoTime() < deadline) {
                val records = c.poll(Duration.ofMillis(500))
                records.forEach { out += it.key() to it.value() }
            }
            return out
        }
    }
}
