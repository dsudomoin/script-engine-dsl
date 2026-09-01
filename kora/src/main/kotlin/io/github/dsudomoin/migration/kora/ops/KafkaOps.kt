package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.MigrationContext
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Ad-hoc Kafka-publish операции. Используется когда нужно publish'ить в разные топики из одного
 * скрипта без заведения [KafkaTopic]-handle на каждый.
 *
 * Инстанс — один на `Producer` в пределах прогона (см. [MigrationContext.shared]), поэтому
 * `kafka(producer)` можно спокойно вызывать прямо в теле `forEach`. Регистрируется в контексте:
 * runner закроет его после `migrate()`, и закрытие делает `flush()` — без этого callback'и
 * async-отправок могли бы не успеть отработать до печати отчёта.
 *
 * Для типичного «один топик на миграцию» — предпочитай [topic] (`val t = topic(producer, name)`,
 * потом `t.send(key, value)`).
 */
class KafkaOps<K, V> internal constructor(
    private val ctx: MigrationContext,
    private val producer: Producer<K, V>,
) : AutoCloseable {

    private val closed = AtomicBoolean(false)
    private val asyncFailures = AtomicLong()

    /** Метаданные опубликованного сообщения (`null` под dry-run). */
    data class PublishResult(val topic: String, val partition: Int, val offset: Long)

    /**
     * Sync-publish. Блокируется до ack от брокера, ошибка доставки летит наружу и штатно доходит
     * до `OnError` и `errors.csv`. Под dry-run — `null`, в лог
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
     * completed future со значением `null`, реальный `producer.send` не вызывается.
     *
     * **Про учёт.** `forEach` засчитывает item успешным в момент отправки, а брокер отвечает
     * позже — и возвращаемый future в реальных скриптах почти никто не читает. Поэтому отказ
     * доставки обрабатывается здесь же, в callback'е: строка уходит в `errors.csv`, счётчик
     * `report.asyncFailed` растёт, а runner по нему поднимает exit-код до 1. Иначе отчёт
     * показывал бы «successful: 1 000 000» при молча потерянных сообщениях.
     */
    fun publishAsync(topic: String, key: K, value: V): CompletableFuture<PublishResult?> =
        ctx.guardWrite(
            label = "kafka.publishAsync",
            args = mapOf("topic" to topic, "key" to key),
            dryRunDefault = CompletableFuture.completedFuture<PublishResult?>(null),
        ) {
            val cf = CompletableFuture<PublishResult?>()
            producer.send(ProducerRecord(topic, key, value)) { md, err ->
                if (err != null) {
                    asyncFailures.incrementAndGet()
                    ctx.report.incAsyncFailed()
                    // Аудит идёт из IO-потока продюсера: если сам аудитор упал (диск кончился),
                    // терять исходную ошибку доставки нельзя — она уже учтена счётчиком выше.
                    try {
                        ctx.auditError(err, key)
                    } catch (auditErr: Throwable) {
                        ctx.log.warn("auditError failed for failed kafka delivery key=$key", auditErr)
                    }
                    cf.completeExceptionally(err)
                } else {
                    cf.complete(PublishResult(md.topic(), md.partition(), md.offset()))
                }
            }
            cf
        }

    /**
     * Закрытие. Идемпотентно. Под `dryRun = false` дёргает `producer.flush()` — он дожидается
     * ack'ов по всем отправленным сообщениям, то есть к возврату из `close()` все callback'и
     * [publishAsync] уже отработали и счётчики отчёта верны. `Producer` при этом **не**
     * закрывается — он принадлежит графу Kora.
     */
    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        if (ctx.dryRun) return
        producer.flush()
        val failed = asyncFailures.get()
        if (failed > 0) {
            ctx.report.addWarning(
                "kafka.publishAsync: $failed message(s) were rejected by the broker; see errors.csv",
            )
        }
    }

    override fun toString(): String = "KafkaOps($producer)"
}

/**
 * Фабрика [KafkaOps] — для ad-hoc публикаций без `topic`-handle. Инстанс мемоизируется на
 * `Producer` в пределах прогона, поэтому вызов внутри `forEach` не плодит объекты.
 */
fun <K, V> MigrationContext.kafka(producer: Producer<K, V>): KafkaOps<K, V> =
    shared(producer) { KafkaOps(this, producer) }

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
    private val ops: KafkaOps<K, V>,
    private val name: String,
) : AutoCloseable {

    /** Sync-publish в забинженный топик. Под dry-run возвращает `null`, в отчёт идёт `kafka.publish`. */
    fun send(key: K, value: V): KafkaOps.PublishResult? = ops.publish(name, key, value)

    /**
     * Async-publish в забинженный топик. Отказ доставки учитывается так же, как в
     * [KafkaOps.publishAsync] — попадает в `errors.csv` и в `report.asyncFailed`.
     */
    fun sendAsync(key: K, value: V): CompletableFuture<KafkaOps.PublishResult?> = ops.publishAsync(name, key, value)

    /** Закрытие. Делегирует в [KafkaOps.close] — тот идемпотентен и делает `flush()`. */
    override fun close() {
        ops.close()
    }

    override fun toString(): String = "KafkaTopic($name)"
}

/**
 * Фабрика [KafkaTopic]. Хендл мемоизируется на пару (`producer`, `name`) и регистрируется в
 * контексте — runner закроет (с flush'ем) после `migrate()`. Повторный вызов с теми же
 * аргументами (в том числе прямо в теле `forEach`) отдаёт тот же объект.
 */
fun <K, V> MigrationContext.topic(producer: Producer<K, V>, name: String): KafkaTopic<K, V> =
    shared(TopicKey(producer, name)) { KafkaTopic(kafka(producer), name) }

/** Ключ мемоизации [topic]: идентичность продюсера плюс имя топика. */
private data class TopicKey(val producer: Producer<*, *>, val name: String)
