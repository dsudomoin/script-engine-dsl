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
    private val unhandledFailures = AtomicLong()
    private val warnings = CopyOnWriteArrayList<String>()

    /** Счётчики одной фазы. Дублируют итоговые, а не заменяют их: итоги прогона считаются как и раньше. */
    private class PhaseCounters(val name: String) {
        val processed = AtomicLong()
        val successful = AtomicLong()
        val skipped = AtomicLong()
        val failed = AtomicLong()
        val sourceSkipped = AtomicLong()
        val acknowledgedPublishes = AtomicLong()
        val failedEffects = AtomicLong()
        val abandonedPublishes = AtomicLong()
        val lateRegistered = AtomicLong()
        val rawPages = AtomicLong()
        val rawRows = AtomicLong()
        val dryRunSkipped = ConcurrentHashMap<String, AtomicLong>()
        val appliedWrites = ConcurrentHashMap<String, AtomicLong>()
        val rejectedWrites = ConcurrentHashMap<String, AtomicLong>()
    }

    private val phaseList = CopyOnWriteArrayList<PhaseCounters>()

    // Фазы строго последовательны, а интерпретатор дожидается всех воркеров и барьера прежде чем
    // закрыть фазу — поэтому «текущая фаза» это одно volatile-поле, а не стек и не ThreadLocal.
    @Volatile
    private var currentPhase: PhaseCounters? = null

    /** Открыть фазу. Вызывает только интерпретатор плана. */
    fun beginPhase(name: String) {
        val counters = PhaseCounters(name)
        phaseList += counters
        currentPhase = counters
    }

    /** Закрыть текущую фазу. Вызывает только интерпретатор плана. */
    fun endPhase() {
        currentPhase = null
    }

    /**
     * Отказ уровня прогона или стадии — не исход элемента.
     *
     * Отдельно от [incFailed] намеренно: у такого отказа могло не быть обрабатываемого элемента
     * (ошибка источника, валидации, барьера), и общий счётчик ломал бы тождество
     * `processed = successful + skipped + failed`.
     */
    fun recordRunFailure()            { unhandledFailures.incrementAndGet() }

    fun incProcessed()                { processed.incrementAndGet(); currentPhase?.processed?.incrementAndGet() }
    fun incSuccessful()               { successful.incrementAndGet(); currentPhase?.successful?.incrementAndGet() }
    fun incSkipped()                  { skipped.incrementAndGet(); currentPhase?.skipped?.incrementAndGet() }
    fun incFailed()                   { failed.incrementAndGet(); currentPhase?.failed?.incrementAndGet() }

    /**
     * Строка источника отброшена при чтении (битый CSV) и до стадии не дошла.
     *
     * Отдельный счётчик, а не `skipped`: иначе ломается тождество
     * `processed = successful + skipped + failed` — такая строка никогда не была `processed`.
     */
    fun incSourceSkipped()            { sourceSkipped.incrementAndGet(); currentPhase?.sourceSkipped?.incrementAndGet() }

    /**
     * Инкрементить счётчик dry-run-пропущенных write'ов по метке (используется внутри `guardWrite`).
     */
    fun incDryRunSkipped(label: String) {
        dryRunSkipped.computeIfAbsent(label) { AtomicLong() }.incrementAndGet()
        currentPhase?.dryRunSkipped?.computeIfAbsent(label) { AtomicLong() }?.incrementAndGet()
    }

    /** Запись применилась. */
    fun incAppliedWrite(label: String) {
        appliedWrites.computeIfAbsent(label) { AtomicLong() }.incrementAndGet()
        currentPhase?.appliedWrites?.computeIfAbsent(label) { AtomicLong() }?.incrementAndGet()
    }

    /** Запись отклонена — ожидаемый отрицательный исход, а не сбой. */
    fun incRejectedWrite(label: String) {
        rejectedWrites.computeIfAbsent(label) { AtomicLong() }.incrementAndGet()
        currentPhase?.rejectedWrites?.computeIfAbsent(label) { AtomicLong() }?.incrementAndGet()
    }

    /** Итоги барьера scope'а: подтверждено, отказано, брошено по таймауту, зарегистрировано поздно. */
    fun addBarrierOutcome(acked: Long, failed: Long, abandoned: Long, late: Long) {
        acknowledgedPublishes.addAndGet(acked)
        failedEffects.addAndGet(failed)
        abandonedPublishes.addAndGet(abandoned)
        lateRegistered.addAndGet(late)
        currentPhase?.let {
            it.acknowledgedPublishes.addAndGet(acked)
            it.failedEffects.addAndGet(failed)
            it.abandonedPublishes.addAndGet(abandoned)
            it.lateRegistered.addAndGet(late)
        }
    }

    /** Сырая страница пагинатора: единственное место, где видно «прочитано» до фильтров. */
    fun addRawPage(rows: Int) {
        rawPages.incrementAndGet()
        rawRows.addAndGet(rows.toLong())
        currentPhase?.let { it.rawPages.incrementAndGet(); it.rawRows.addAndGet(rows.toLong()) }
    }

    /**
     * Отказы эффектов, из-за которых прогон не имеет права закончиться нулём: провалившиеся,
     * не дождавшиеся подтверждения и зарегистрированные после барьера.
     *
     * `failedEffects` входит сюда наравне с остальными: известный отказ доставки — это потерянное
     * сообщение, а не «залогированная ошибка», и LOG_AND_COMPLETE не должен превращать его в ноль.
     */
    fun effectFailureCount(): Long =
        failedEffects.get() + abandonedPublishes.get() + lateRegistered.get()

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
            unhandledFailures = unhandledFailures.get(),
            phases = if (phaseList.size > 1) phaseList.map { it.snapshot() } else emptyList(),
            errorsFile = errorsFile,
            tracesFile = tracesFile,
            warnings = warnings.toList(),
        )
    }

    private fun PhaseCounters.snapshot() = PhaseReport(
        name = name,
        processed = processed.get(),
        successful = successful.get(),
        skipped = skipped.get(),
        failed = failed.get(),
        sourceSkipped = sourceSkipped.get(),
        appliedWrites = appliedWrites.mapValues { it.value.get() },
        rejectedWrites = rejectedWrites.mapValues { it.value.get() },
        dryRunSkipped = dryRunSkipped.mapValues { it.value.get() },
        acknowledgedPublishes = acknowledgedPublishes.get(),
        failedEffects = failedEffects.get(),
        abandonedPublishes = abandonedPublishes.get(),
        lateRegistered = lateRegistered.get(),
        rawPages = rawPages.get(),
        rawRows = rawRows.get(),
    )
}
