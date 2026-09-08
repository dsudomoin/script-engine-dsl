package io.github.dsudomoin.migration

/**
 * Одна миграция. Регистрируется в Kora-графе как `@Component`; runner выбирает её по [name],
 * сравнивая с `migration.run` из конфига.
 *
 * [run] объявлен extension-членом: внутри одновременно видны зависимости класса и хелперы
 * движка, без префиксов. Длинное тело разбивается приватными extension-функциями на
 * [MigrationScope] — обычный приватный метод хелперов не увидит.
 *
 * ```
 * @Component
 * class FixOrders(private val repo: OrderRepository) : Migration("FIX-ORDERS-001") {
 *     override fun MigrationScope.run() {
 *         each(repo.stuckOrders()) { order ->
 *             if (!dryRun) repo.fix(order.id)
 *         }
 *     }
 * }
 * ```
 */
abstract class Migration(val name: String) {
    abstract fun MigrationScope.run()
}
