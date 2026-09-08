package io.github.dsudomoin.migration.report

import java.nio.file.Path
import java.time.Duration
import java.time.Instant

/**
 * Immutable снимок статистики прогона. Создаётся [ReportBuilder.build] в конце прогона,
 * рендерится [ReportFormatter].
 *
 * @property processed всего обработано элементов циклами `each`.
 * @property successful сколько прошли успешно.
 * @property skipped сколько ушли в Skip-ветку (элементы в `errors.csv`).
 * @property failed элементы, отказ которых политика признала терминальным. Держится тождество
 *                  `processed = successful + skipped + failed`.
 * @property sourceSkipped строки, отброшенные при чтении источника (битый CSV). В тождество
 *                  не входят: до цикла они не дошли.
 * @property unhandledFailures отказы уровня прогона, у которых обрабатываемого элемента могло не
 *                  быть (ошибка источника, падение тела миграции). В тождество тоже не входят.
 * @property errorsFile путь к `errors.csv`, если файл был создан.
 * @property tracesFile путь к `errors.log`, если файл был создан.
 * @property warnings нефатальные предупреждения (например, ошибка закрытия ресурса).
 *                  На код возврата не влияют.
 */
data class MigrationReport(
    val name: String,
    val startedAt: Instant,
    val finishedAt: Instant,
    val duration: Duration,
    val dryRun: Boolean,
    val processed: Long,
    val successful: Long,
    val skipped: Long,
    val failed: Long,
    val sourceSkipped: Long = 0,
    val unhandledFailures: Long = 0,
    val errorsFile: Path?,
    val tracesFile: Path?,
    val warnings: List<String> = emptyList(),
)
