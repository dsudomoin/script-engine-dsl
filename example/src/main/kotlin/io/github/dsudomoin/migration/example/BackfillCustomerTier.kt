package io.github.dsudomoin.migration.example

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.MigrationDefinition
import io.github.dsudomoin.migration.Progress
import io.github.dsudomoin.migration.WriteOutcome
import io.github.dsudomoin.migration.csv.readCsv
import io.github.dsudomoin.migration.error.includeItem
import io.github.dsudomoin.migration.migration
import ru.tinkoff.kora.common.Component

/** Строка входного файла, уже разобранная в типы. */
data class Customer(val id: Long, val email: String, val spend: Long)

/**
 * Проставить клиентам тариф по сумме покупок: читаем выгрузку, считаем tier, пишем в репозиторий,
 * рядом кладём CSV с тем, что получилось.
 *
 * Показывает четыре вещи, ради которых библиотека и существует:
 * - `readCsv(onRowError = ItemError.Skip)` — битая строка не валит прогон, а уезжает в `errors.csv`;
 * - `source(parallel = 4)` — реальные четыре воркера, счётчики и прогресс без ручного кода;
 * - `write { }` — единственный способ провести вызов чужого компонента через dry-run-гейт;
 * - `output` — выходной файл, который движок закроет сам.
 */
@Component
class BackfillCustomerTier(
    private val repository: CustomerTierRepository,
) : MigrationDefinition {

    override val name = "CUSTOMER-TIER-001"

    override fun plan() = migration(name = name, author = "example") {
        val report = output("customer-tier.csv", "id", "spend", "tier")

        source(
            parallel = 4,
            onItemError = ItemError.Skip,
            progress = Progress.Every(5),
            items = {
                // Без этого в errors.csv поедет toString() со всеми полями, включая почту. Две
                // регистрации, потому что до обработчика строка может сломаться ещё на разборе —
                // тогда аудит получает сырую строку CSV, а не Customer.
                errors.includeItem<Customer> { "id=${it.id}" }
                errors.includeItem<Map<String, String>> { "id=${it["id"]}" }

                readCsv("customers.csv", classpath = true, onRowError = ItemError.Skip) { row ->
                    Customer(
                        id = row.getValue("id").toLong(),
                        email = row.getValue("email"),
                        spend = row.getValue("spend").toLong(),
                    )
                }
            },
        ) { customer ->
            val tier = tierFor(customer.spend)

            write("customer.tier", args = mapOf("id" to customer.id, "tier" to tier)) {
                repository.updateTier(customer.id, tier)
                WriteOutcome.Applied
            }

            report.row(customer.id, customer.spend, tier)
        }
    }

    private fun tierFor(spend: Long): String = when {
        spend >= 10_000 -> "PLATINUM"
        spend >= 1_000 -> "GOLD"
        else -> "SILVER"
    }
}
