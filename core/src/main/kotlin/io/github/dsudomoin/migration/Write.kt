package io.github.dsudomoin.migration

/**
 * Исход записи, который сообщает сама операция.
 *
 * [Rejected] — ожидаемый отрицательный результат условного апдейта (ни одна строка не подошла,
 * версия разошлась), а не сбой. Сбой — это исключение из тела записи.
 */
sealed interface WriteOutcome {
    data object Applied : WriteOutcome
    data class Rejected(val reason: String) : WriteOutcome
}

/** Что движок сделал с записью. Под dry-run тело записи не вызывается вовсе. */
sealed interface WriteResult {
    data object Applied : WriteResult
    data class Rejected(val reason: String) : WriteResult
    data object DryRunSkipped : WriteResult
}
