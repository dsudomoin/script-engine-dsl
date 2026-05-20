package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.MigrationContext
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Ad-hoc Kafka-publish операции. Используется когда нужно publish'ить в разные топики из одного
 * скрипта без заведения [KafkaTopic]-handle на каждый.
 *
 * Для типичного «один топик на миграцию» — предпочитай [topic] (`val t = topic(producer, name)`,
 * потом `t.send(key, value)`), который автоматически делает `flush()` на закрытии.
 */
class KafkaOps<K, V> internal constructor(
    private val ctx: MigrationContext,
    private val producer: Producer<K, V>,
) {
    /** Метаданные опубликованного сообщения (`null` под dry-run). */
    data class PublishResult(val topic: String, val partition: Int, val offset: Long)

    /**
     * Sync-publish. Блокируется до ack от брокера. Под dry-run — `null`, в лог
     * `INFO [DRY-RUN] kafka.publish (topic=..., key=...)`.
     */
    fun publish(topic: String, key: K, value: V): PublishResult? =
        ctx.guardWrite(
            label = "kafka.publish",
            args = mapOf("topic" to topic, "key" to key),
            dryRunDefault = null,
        ) {
            val md: RecordMetadata = producer.send(ProducerRecord(topic, key, value)).get()
            PublishResult(md.topic(), md.partition(), md.offset())
        }

    /**
     * Async-publish. Возвращает `CompletableFuture` — не блокируется на ack. Под dry-run —
     * completed future со значением `null`, реальный `producer.send` не вызывается. Уместно
     * когда `forEach` параллелит само и не нужно per-item ожидание ack'а.
     */
    fun publishAsync(topic: String, key: K, value: V): CompletableFuture<PublishResult?> =
        ctx.guardWrite(
            label = "kafka.publishAsync",
            args = mapOf("topic" to topic, "key" to key),
            dryRunDefault = CompletableFuture.completedFuture<PublishResult?>(null),
        ) {
            val cf = CompletableFuture<PublishResult?>()
            producer.send(ProducerRecord(topic, key, value)) { md, err ->
                if (err != null) cf.completeExceptionally(err)
                else cf.complete(PublishResult(md.topic(), md.partition(), md.offset()))
            }
            cf
        }

    internal fun flushProducer() {
        producer.flush()
    }
}

/** Фабрика [KafkaOps] — для ad-hoc публикаций без `topic`-handle. */
fun <K, V> MigrationContext.kafka(producer: Producer<K, V>): KafkaOps<K, V> = KafkaOps(this, producer)

/**
 * Handle для публикации в один топик. Создаётся через [topic]. Регистрируется как
 * [AutoCloseable] — runner вызовет `close()` в `finally`, что приведёт к `producer.flush()`
 * (под dry-run — no-op).
 *
 * Типичная схема:
 * ```
 * val resync = topic(producer, "customers.resync")
 * forEach(items) { item -> resync.send(item.id, item.toEvent()) }
 * // flush на закрытии runner'ом
 * ```
 */
class KafkaTopic<K, V> internal constructor(
    private val ctx: MigrationContext,
    private val ops: KafkaOps<K, V>,
    private val name: String,
) : AutoCloseable {

    private val closed = AtomicBoolean(false)

    /** Sync-publish в забинженный топик. Под dry-run возвращает `null`, в отчёт идёт `kafka.publish`. */
    fun send(key: K, value: V): KafkaOps.PublishResult? = ops.publish(name, key, value)

    /**
     * Закрытие. Идемпотентно (CAS-защита от двойного вызова из параллельных воркеров).
     * Под `dryRun = false` дёргает `producer.flush()` **ровно один раз**; под dry-run — no-op.
     * `Producer` сам по себе **не** закрывается — он принадлежит Kora-графу.
     */
    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        if (!ctx.dryRun) ops.flushProducer()
    }

    override fun toString(): String = "KafkaTopic($name)"
}

/**
 * Фабрика [KafkaTopic]. Регистрирует handle в [MigrationContext] — runner закроет (с flush'ем)
 * после `migrate()`.
 */
fun <K, V> MigrationContext.topic(producer: Producer<K, V>, name: String): KafkaTopic<K, V> {
    val t = KafkaTopic(this, KafkaOps(this, producer), name)
    register(t)
    return t
}
