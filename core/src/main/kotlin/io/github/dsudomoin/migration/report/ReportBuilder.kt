package io.github.dsudomoin.migration.report

import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Thread-safe аккумулятор статистики прогона. Инкрементируется автоматически интерпретатором плана
 * и `guardWrite`. Пользователю напрямую дёргать счётчики не нужно — доступ к нему через
 * [io.github.dsudomoin.migration.RunScope.report] полезен в основном для прогресс-форматтеров (`Progress.Custom`).
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
    private val dryRunSkipped = ConcurrentHashMap<String, AtomicLong>()
    private val appliedWrites = ConcurrentHashMap<String, AtomicLong>()
    private val rejectedWrites = ConcurrentHashMap<String, AtomicLong>()
    private val acknowledgedPublishes = AtomicLong()
    private val failedEffects = AtomicLong()
    private val abandonedPublishes = AtomicLong()
    private val lateRegistered = AtomicLong()
    private val sourceSkipped = AtomicLong()
    private val rawPages = AtomicLong()
    private val rawRows = AtomicLong()
    private val warnings = CopyOnWriteArrayList<String>()

    fun incProcessed()                { processed.incrementAndGet() }
    fun incSuccessful()               { successful.incrementAndGet() }
    fun incSkipped()                  { skipped.incrementAndGet() }
    fun incFailed()                   { failed.incrementAndGet() }

    /**
     * Строка источника отброшена при чтении (битый CSV) и до стадии не дошла.
     *
     * Отдельный счётчик, а не `skipped`: иначе ломается тождество
     * `processed = successful + skipped + failed` — такая строка никогда не была `processed`.
     */
    fun incSourceSkipped()            { sourceSkipped.incrementAndGet() }

    /**
     * Инкрементить счётчик dry-run-пропущенных write'ов по метке (используется внутри `guardWrite`).
     */
    fun incDryRunSkipped(label: String) {
        dryRunSkipped.computeIfAbsent(label) { AtomicLong() }.incrementAndGet()
    }

    /** Запись применилась. */
    fun incAppliedWrite(label: String) {
        appliedWrites.computeIfAbsent(label) { AtomicLong() }.incrementAndGet()
    }

    /** Запись отклонена — ожидаемый отрицательный исход, а не сбой. */
    fun incRejectedWrite(label: String) {
        rejectedWrites.computeIfAbsent(label) { AtomicLong() }.incrementAndGet()
    }

    /** Итоги барьера scope'а: подтверждено, отказано, брошено по таймауту, зарегистрировано поздно. */
    fun addBarrierOutcome(acked: Long, failed: Long, abandoned: Long, late: Long) {
        acknowledgedPublishes.addAndGet(acked)
        failedEffects.addAndGet(failed)
        abandonedPublishes.addAndGet(abandoned)
        lateRegistered.addAndGet(late)
    }

    /** Сырая страница пагинатора: единственное место, где видно «прочитано» до фильтров. */
    fun addRawPage(rows: Int) {
        rawPages.incrementAndGet()
        rawRows.addAndGet(rows.toLong())
    }

    /** Число брошенных и поздно зарегистрированных эффектов — runner поднимает по ним exit-код. */
    fun unconfirmedEffectsCount(): Long = abandonedPublishes.get() + lateRegistered.get()

    /** Добавить нефатальное предупреждение (например, исключение при закрытии ресурса). */
    fun addWarning(message: String)   { warnings.add(message) }

    /** Текущее значение `skipped` — для проверки `errorThreshold` по ходу прогона. */
    fun skippedCount(): Long = skipped.get()

    /** Текущее значение `processed` — runner использует для post-mortem проверок. */
    fun processedCount(): Long = processed.get()

    /**
     * `true`, если через `guardWrite` не прошло ни одной записи. Под dry-run это единственный
     * наблюдаемый признак того, что миграция пишет мимо гейта (забытый `write { }`).
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
            dryRunSkipped = dryRunSkipped.mapValues { it.value.get() },
            appliedWrites = appliedWrites.mapValues { it.value.get() },
            rejectedWrites = rejectedWrites.mapValues { it.value.get() },
            acknowledgedPublishes = acknowledgedPublishes.get(),
            failedEffects = failedEffects.get(),
            abandonedPublishes = abandonedPublishes.get(),
            lateRegistered = lateRegistered.get(),
            sourceSkipped = sourceSkipped.get(),
            rawPages = rawPages.get(),
            rawRows = rawRows.get(),
            errorsFile = errorsFile,
            tracesFile = tracesFile,
            warnings = warnings.toList(),
        )
    }
}
