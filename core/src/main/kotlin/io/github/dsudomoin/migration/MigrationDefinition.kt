package io.github.dsudomoin.migration

/**
 * Определение одной миграции. Регистрируется в Kora-графе как `@Component`; runner собирает все
 * определения через `All<MigrationDefinition>`.
 *
 * [name] — константа: по ней runner выбирает миграцию и проверяет дубли, **не строя ни одного
 * плана**. [plan] вызывается ровно один раз и только у выбранной миграции — именно поэтому это
 * метод, а не свойство: инициализатор свойства выполнился бы у всех компонентов при сборке графа.
 *
 * ```
 * @Component
 * class BackfillCustomerTier(private val repo: CustomerRepository) : MigrationDefinition {
 *     override val name = "CUSTOMER-TIER-001"
 *     override fun plan() = migration(name, author = "example") { ... }
 * }
 * ```
 */
interface MigrationDefinition {
    val name: String

    fun plan(): MigrationPlan
}

/**
 * Политика runner'а при необработанной ошибке, вышедшей за пределы стадий.
 *
 * Не влияет на ошибки элементов — там действует [ItemError]. И не позволяет продолжить
 * следующие стадии: провал стадии всегда останавливает прогон, а [LOG_AND_COMPLETE] меняет
 * только exit-код.
 */
enum class ScriptPolicy {
    /** Залогировать, учесть в отчёте, вернуть exit-code 1. Дефолт. */
    FAIL_FAST,

    /** Залогировать, учесть, напечатать отчёт и вернуть 0. Для compliance-сценариев. */
    LOG_AND_COMPLETE,
}
