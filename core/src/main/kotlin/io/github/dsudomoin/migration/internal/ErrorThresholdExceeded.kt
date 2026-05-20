package io.github.dsudomoin.migration.internal

/**
 * Sentinel-исключение, бросаемое `ForEachEngine`, когда `report.skipped > ctx.errorThreshold`.
 * Используется runner'ом для специальной обработки (exit 1 с осмысленным сообщением вместо
 * audit-как-обычной-ошибки).
 *
 * Не предназначено для перехвата пользовательским кодом.
 */
class ErrorThresholdExceeded internal constructor(
    threshold: Long,
    actual: Long,
) : RuntimeException("Error threshold exceeded: skipped=$actual > $threshold")
