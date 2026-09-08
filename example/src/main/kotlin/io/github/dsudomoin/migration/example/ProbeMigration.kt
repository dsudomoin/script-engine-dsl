package io.github.dsudomoin.migration.example

import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationScope
import ru.tinkoff.kora.common.Component

/**
 * Временный зонд: проверяет, что KSP Kora переваривает новую форму миграции —
 * абстрактный базовый класс с extension-членом. Удаляется вместе с [ProbeCollector],
 * когда пример переедет на новый API по-настоящему.
 */
@Component
class ProbeMigration(
    private val repository: CustomerTierRepository,
) : Migration("PROBE-001") {

    override fun MigrationScope.run() {
        log.info("probe sees repository at {}", repository.path())
        each(listOf(1, 2, 3)) { log.info("probe item {}", it) }
    }
}
