package io.github.dsudomoin.migration.kora

import io.github.dsudomoin.migration.ScriptPolicy
import ru.tinkoff.kora.config.common.annotation.ConfigSource
import java.nio.file.Path

/**
 * HOCON-конфиг runner'а. Секция `migration { ... }` в `application.conf`.
 *
 * Минимум для запуска — `migration.run = ${?MIGRATION_RUN}`. Остальное опционально.
 *
 * @property run имя миграции к запуску (совпадает с [io.github.dsudomoin.migration.Migration.name]). `null` = idle.
 * @property dryRun `true` — все write'ы через `guardWrite` пропускаются, в отчёт идёт breakdown.
 * @property outputFolder папка для артефактов прогона (`migration.log`, `errors.csv`/`log`, user CSVs).
 *                        `null` → дефолт `logs/${run}`.
 * @property defaults дефолты для `forEach`, threshold'а ошибок и unhandled-policy.
 * @property errorReporting тонкая настройка авто-аудитора. Пути файлов выводятся из [outputFolder].
 * @property report настройки рендера итогового отчёта (ASCII / unicode).
 */
@ConfigSource("migration")
data class MigrationConfig(
    var run: String? = null,
    var dryRun: Boolean = false,
    var outputFolder: Path? = null,
    var defaults: Defaults = Defaults(),
    var errorReporting: ErrorReporting = ErrorReporting(),
    var report: Report = Report(),
) {
    /**
     * Дефолтные параметры исполнения.
     *
     * @property onUnhandled политика для `migrate()`-level исключений (см. [ScriptPolicy]).
     * @property errorThreshold если `skipped > threshold` — прервать прогон с exit 1. `0` = отключено.
     * @property progressEvery период дефолтного `Progress.Default` логгера.
     * @property parallel дефолтный размер `FixedThreadPool` runner'а. Можно override'ить через
     *                    `@Tag(MigrationExecutor::class)`.
     */
    data class Defaults(
        var onUnhandled: ScriptPolicy = ScriptPolicy.FAIL_FAST,
        var errorThreshold: Long = 0,
        var progressEvery: Int = 1000,
        var parallel: Int = 1,
    )

    /**
     * Настройка авто-аудитора ошибок.
     *
     * @property includeStackTrace писать ли в `errors.log` стектрейсы. `false` = только CSV.
     * @property maxItemReprLength обрезать `itemRepr` в CSV до этой длины (с `...`).
     */
    data class ErrorReporting(
        var includeStackTrace: Boolean = true,
        var maxItemReprLength: Int = 500,
    )

    /** @property asciiOnly заменить unicode-glyphs в отчёте на ASCII (`✓` → `[OK]` и т.д.). */
    data class Report(
        var asciiOnly: Boolean = false,
    )
}
