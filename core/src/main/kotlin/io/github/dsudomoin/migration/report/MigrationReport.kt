package io.github.dsudomoin.migration.report

import java.nio.file.Path
import java.time.Duration
import java.time.Instant

/**
 * Immutable снимок статистики прогона миграции. Создаётся [ReportBuilder.build] в конце прогона,
 * рендерится [ReportFormatter] в текстовый отчёт.
 *
 * @property processed всего обработано элементов стадиями.
 * @property successful сколько прошли успешно.
 * @property skipped сколько ушли в Skip-ветку (item'ы в `errors.csv`).
 * @property failed unhandled-исключения, пойманные runner'ом (зависит от [io.github.dsudomoin.migration.ScriptPolicy]).
 * @property failedEffects отказы асинхронных эффектов, обнаруженные на барьере стадии. Не входят
 *                         в [failed]: item к тому моменту уже посчитан.
 * @property abandonedPublishes отправленные эффекты, не подтвердившиеся за таймаут барьера.
 * @property lateRegistered эффекты, зарегистрированные уже после закрытия барьера.
 * @property sourceSkipped строки, отброшенные при чтении источника (битый CSV). Не входят в
 *                         [processed] и [skipped]: до стадии они не дошли.
 * @property rawPages сырые страницы, прочитанные `pages(...)` — до пользовательских фильтров.
 * @property dryRunSkipped разбивка `label → count` для skipped writes под dry-run. Ключи —
 *                         human-readable метки операций (`"jdbc.execute"`, `"kafka.publish"`,
 *                         `"customer.tier"`).
 * @property errorsFile путь к `errors.csv` (для печати в отчёте; не nullable если миграция запускалась).
 * @property tracesFile путь к `errors.log`.
 * @property warnings нефатальные предупреждения runner'а (например, исключения при закрытии
 *                    registered-ресурсов). Не влияют на exit-code.
 */
data class MigrationReport(
    val name: String,
    val author: String,
    val startedAt: Instant,
    val finishedAt: Instant,
    val duration: Duration,
    val dryRun: Boolean,
    val processed: Long,
    val successful: Long,
    val skipped: Long,
    val failed: Long,
    val dryRunSkipped: Map<String, Long>,
    val appliedWrites: Map<String, Long> = emptyMap(),
    val rejectedWrites: Map<String, Long> = emptyMap(),
    val acknowledgedPublishes: Long = 0,
    val failedEffects: Long = 0,
    val abandonedPublishes: Long = 0,
    val lateRegistered: Long = 0,
    val sourceSkipped: Long = 0,
    val rawPages: Long = 0,
    val rawRows: Long = 0,
    val errorsFile: Path?,
    val tracesFile: Path?,
    val warnings: List<String> = emptyList(),
)
