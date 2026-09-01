package io.github.dsudomoin.migration.example

import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.Progress
import io.github.dsudomoin.migration.csv.openCsv
import io.github.dsudomoin.migration.csv.readCsv
import io.github.dsudomoin.migration.error.includeItem
import io.github.dsudomoin.migration.forEach
import io.github.dsudomoin.migration.mutation
import ru.tinkoff.kora.common.Component

/** Строка входного файла, уже разобранная в типы. */
data class Customer(val id: Long, val email: String, val spend: Long)

/**
 * Проставить клиентам тариф по сумме покупок: читаем выгрузку, считаем tier, пишем в репозиторий,
 * рядом кладём CSV с тем, что получилось.
 *
 * Показывает четыре вещи, ради которых библиотека и существует:
 * - `readCsv(onRowError = OnError.Skip)` — битая строка не валит прогон, а уезжает в `errors.csv`;
 * - `forEach(parallel = 4)` — реальные четыре воркера, счётчики и прогресс без ручного кода;
 * - `mutation { }` — единственный способ провести вызов чужого компонента через dry-run-гейт;
 * - `openCsv` — выходной файл, который runner закроет сам.
 */
@Component
class BackfillCustomerTier(
    private val repository: CustomerTierRepository,
) : Migration(name = "CUSTOMER-TIER-001", author = "example") {

    override fun MigrationContext.migrate() {
        // Без этого в errors.csv поедет toString() со всеми полями, включая почту. Две
        // регистрации, потому что до `forEach` строка может сломаться ещё на разборе — тогда
        // аудит получает сырую строку CSV, а не Customer.
        errors.includeItem<Customer> { "id=${it.id}" }
        errors.includeItem<Map<String, String>> { "id=${it["id"]}" }

        val report = openCsv("customer-tier.csv", "id", "spend", "tier")

        val customers = readCsv("customers.csv", classpath = true, onRowError = OnError.Skip) { row ->
            Customer(
                id = row.getValue("id").toLong(),
                email = row.getValue("email"),
                spend = row.getValue("spend").toLong(),
            )
        }

        forEach(
            customers,
            parallel = 4,
            onError = OnError.Skip,
            progress = Progress.Every(5),
        ) { customer ->
            val tier = tierFor(customer.spend)

            mutation("customer.tier", args = mapOf("id" to customer.id, "tier" to tier)) {
                repository.updateTier(customer.id, tier)
            }

            report.row(customer.id, customer.spend, tier)
        }

        log.info("готово, файл с результатом: {}", repository.path().toAbsolutePath())
    }

    private fun tierFor(spend: Long): String = when {
        spend >= 10_000 -> "PLATINUM"
        spend >= 1_000 -> "GOLD"
        else -> "SILVER"
    }
}
