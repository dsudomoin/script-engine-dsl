package io.github.dsudomoin.migration.report

import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Thread-safe аккумулятор статистики прогона. Инкрементируется автоматически из `forEach`-движка
 * и `guardWrite`. Пользователю напрямую дёргать счётчики не нужно — доступ к нему через
 * [io.github.dsudomoin.migration.MigrationContext.report] полезен в основном для прогресс-форматтеров (`Progress.Custom`).
 *
 * @param startedAt момент создания (≈ старт прогона). Дефолт `Instant.now()`.
 */
class ReportBuilder(
    val name: String,
    val author: String,
    val dryRun: Boolean,
    val startedAt: Instant = Instant.now(),
) {
    private val processed = AtomicLong()
    private val successful = AtomicLong()
    private val skipped = AtomicLong()
    private val failed = AtomicLong()
    private val asyncFailed = AtomicLong()
    private val dryRunSkipped = ConcurrentHashMap<String, AtomicLong>()
    private val warnings = CopyOnWriteArrayList<String>()

    fun incProcessed()                { processed.incrementAndGet() }
    fun incSuccessful()               { successful.incrementAndGet() }
    fun incSkipped()                  { skipped.incrementAndGet() }
    fun incFailed()                   { failed.incrementAndGet() }

    /**
     * Отказ ДОСТАВКИ, обнаруженный после того, как item уже засчитан успешным (async-publish в
     * Kafka: `forEach` видит успех в момент отправки, брокер отвечает ошибкой позже). Отдельный
     * счётчик, а не `failed`, чтобы не ломать тождество `processed = successful + skipped + failed`
     * и при этом не прятать потерю сообщений.
     */
    fun incAsyncFailed()              { asyncFailed.incrementAndGet() }

    /** Инкрементить счётчик dry-run-пропущенных write'ов по метке (используется внутри `guardWrite`). */
    fun incDryRunSkipped(label: String) {
        dryRunSkipped.computeIfAbsent(label) { AtomicLong() }.incrementAndGet()
    }

    /** Добавить нефатальное предупреждение (например, исключение при закрытии ресурса). */
    fun addWarning(message: String)   { warnings.add(message) }

    /** Текущее значение `skipped` — для проверки `errorThreshold` по ходу прогона. */
    fun skippedCount(): Long = skipped.get()

    /** Текущее число отказов async-доставки — runner использует для финального exit-кода. */
    fun asyncFailedCount(): Long = asyncFailed.get()

    /** Текущее значение `processed` — runner использует для post-mortem проверок. */
    fun processedCount(): Long = processed.get()

    /**
     * `true`, если через `guardWrite` не прошло ни одной записи. Под dry-run это единственный
     * наблюдаемый признак того, что скрипт пишет мимо гейта (забытый `mutation { }`).
     */
    fun noWritesGated(): Boolean = dryRunSkipped.isEmpty()

    /** Immutable snapshot текущего состояния. Вызывается runner'ом один раз в конце. */
    fun build(errorsFile: Path? = null, tracesFile: Path? = null): MigrationReport {
        val finished = Instant.now()
        return MigrationReport(
            name = name,
            author = author,
            startedAt = startedAt,
            finishedAt = finished,
            duration = Duration.between(startedAt, finished),
            dryRun = dryRun,
            processed = processed.get(),
            successful = successful.get(),
            skipped = skipped.get(),
            failed = failed.get(),
            asyncFailed = asyncFailed.get(),
            dryRunSkipped = dryRunSkipped.mapValues { it.value.get() },
            errorsFile = errorsFile,
            tracesFile = tracesFile,
            warnings = warnings.toList(),
        )
    }
}
