package io.github.dsudomoin.migration.report

import java.nio.file.Path
import java.time.Duration
import java.time.Instant

/**
 * Immutable снимок статистики прогона миграции. Создаётся [ReportBuilder.build] в конце прогона,
 * рендерится [ReportFormatter] в текстовый отчёт.
 *
 * @property processed всего обработано item'ов / батчей (зависит от чанк-режима `forEach`).
 * @property successful сколько прошли успешно.
 * @property skipped сколько ушли в Skip-ветку (item'ы в `errors.csv`).
 * @property failed unhandled-исключения, поймаонные runner'ом (зависит от [io.github.dsudomoin.migration.ScriptPolicy]).
 * @property dryRunSkipped разбивка `label → count` для skipped writes под dry-run. Ключи —
 *                         human-readable метки операций (`"jdbc.execute"`, `"kafka.publish"`,
 *                         `"mutation:legacy webhook ..."`).
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
    val errorsFile: Path?,
    val tracesFile: Path?,
    val warnings: List<String> = emptyList(),
)
