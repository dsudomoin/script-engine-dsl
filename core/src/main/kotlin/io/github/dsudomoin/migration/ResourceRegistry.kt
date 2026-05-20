package io.github.dsudomoin.migration

/**
 * Реестр долгоживущих ресурсов миграции, закрываемых runner'ом после `migrate()`.
 *
 * Реализуется [MigrationContext]. Пользовательские расширения вроде `openCsv` или `topic`
 * автоматически вызывают [register] на созданном handle — пользователю самому регистрировать
 * ничего не нужно. Прямой вызов [register] полезен только для самописных AutoCloseable-обёрток.
 *
 * Контракт реализации:
 * - `register` thread-safe (можно вызывать из `forEach`-воркеров).
 * - Закрытие — в обратном порядке регистрации, в `finally` после возврата из `migrate()`.
 * - Исключение из `close()` не пробрасывается: логируется WARN'ом и попадает в
 *   `report.warnings`, exit-code не меняется.
 */
interface ResourceRegistry {
    /** Добавить [c] в реестр. Закроется runner'ом в `finally` после `migrate()`. */
    fun register(c: AutoCloseable)
}
