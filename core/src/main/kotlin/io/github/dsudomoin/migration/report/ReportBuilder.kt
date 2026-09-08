package io.github.dsudomoin.migration.report

import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong

/**
 * Thread-safe аккумулятор статистики прогона. Инкрементируется движком, а не кодом миграции.
 *
 * @param startedAt момент создания (≈ старт прогона).
 */
class ReportBuilder(
    val name: String,
    val dryRun: Boolean,
    val startedAt: Instant = Instant.now(),
) {
    private val processed = AtomicLong()
    private val successful = AtomicLong()
    private val skipped = AtomicLong()
    private val failed = AtomicLong()
    private val sourceSkipped = AtomicLong()
    private val unhandledFailures = AtomicLong()
    private val warnings = CopyOnWriteArrayList<String>()

    /**
     * Отказ уровня прогона — не исход элемента.
     *
     * Отдельно от [incFailed] намеренно: у такого отказа могло не быть обрабатываемого
     * элемента, и общий счётчик ломал бы тождество
     * `processed = successful + skipped + failed`.
     */
    fun recordRunFailure() { unhandledFailures.incrementAndGet() }

    fun incProcessed() { processed.incrementAndGet() }

    fun incSuccessful() { successful.incrementAndGet() }

    fun incSkipped() { skipped.incrementAndGet() }

    fun incFailed() { failed.incrementAndGet() }

    /**
     * Строка источника отброшена при чтении (битый CSV) и до цикла не дошла.
     *
     * Отдельный счётчик, а не `skipped`: иначе ломается тождество счётчиков — такая
     * строка никогда не была `processed`.
     */
    fun incSourceSkipped() { sourceSkipped.incrementAndGet() }

    /** Добавить нефатальное предупреждение (например, исключение при закрытии ресурса). */
    fun addWarning(message: String) { warnings.add(message) }

    /** Текущее значение `skipped`. */
    fun skippedCount(): Long = skipped.get()

    /** Текущее значение `processed`. */
    fun processedCount(): Long = processed.get()

    /** Immutable snapshot текущего состояния. Вызывается один раз в конце прогона. */
    fun build(errorsFile: Path? = null, tracesFile: Path? = null): MigrationReport {
        val finished = Instant.now()
        return MigrationReport(
            name = name,
            startedAt = startedAt,
            finishedAt = finished,
            duration = Duration.between(startedAt, finished),
            dryRun = dryRun,
            processed = processed.get(),
            successful = successful.get(),
            skipped = skipped.get(),
            failed = failed.get(),
            sourceSkipped = sourceSkipped.get(),
            unhandledFailures = unhandledFailures.get(),
            errorsFile = errorsFile,
            tracesFile = tracesFile,
            warnings = warnings.toList(),
        )
    }
}
