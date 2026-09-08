package io.github.dsudomoin.migration.error

/**
 * Контракт авто-аудитора item-уровневых ошибок. Дефолтная реализация — [CsvFileErrorReporter]
 * (пишет в `errors.csv` + `errors.log` под [io.github.dsudomoin.migration.MigrationScope.outputFolder]).
 *
 * Пользователь может подменить дефолт своей реализацией для интеграции с Sentry, Kibana,
 * JSON Lines, и т.д. — для этого пишет свой `ErrorReporter` и подключает через кастомный
 * `internalCreate` (см. `customization.md`).
 *
 * Реализации должны быть thread-safe: [report] и [registerSerializer] могут вызываться из
 * параллельных воркеров стадии.
 */
interface ErrorReporter : AutoCloseable {
    /**
     * Записать ошибку. Вызывается движком автоматически на каждой ошибке элемента.
     * Пользовательский прямой вызов нужен только для ad-hoc отчётности.
     */
    fun report(e: Throwable, item: Any?)

    /**
     * Зарегистрировать per-type сериализатор для item'а. Используется внутри [report] для
     * рендера `item` (если зарегистрирован сериализатор на его класс / супертип / интерфейс).
     *
     * Обычно вызывается через inline-reified extension [includeItem], а не напрямую.
     */
    fun registerSerializer(cls: Class<*>, f: (Any) -> String)
}

/**
 * Регистрация сериализатора по типу [T]. Эквивалент `registerSerializer(T::class.java) { f(it as T) }`.
 * Регистрировать удобнее всего в первых строках `run()` — до первого цикла `each`.
 * ```
 * errors.includeItem<Customer> { "id=${it.id}, status=${it.status}" }
 * ```
 */
@Suppress("UNCHECKED_CAST")
inline fun <reified T : Any> ErrorReporter.includeItem(noinline f: (T) -> String) {
    registerSerializer(T::class.java) { f(it as T) }
}
