package io.github.dsudomoin.migration.error

/**
 * Контракт авто-аудитора item-уровневых ошибок. Дефолтная реализация — [CsvFileErrorReporter]
 * (пишет в `errors.csv` + `errors.log` под [io.github.dsudomoin.migration.MigrationScope.outputFolder]).
 *
 * Подменить дефолт (Sentry, Kibana, JSON Lines) можно, собрав прогон самому через
 * `MigrationExecution.execute` со своей реализацией — см. `docs/examples/customization.md`.
 *
 * Реализации обязаны быть потокобезопасными: [report] и [registerSerializer] зовутся из
 * параллельных воркеров цикла.
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
