package io.github.dsudomoin.migration.example

import io.github.dsudomoin.migration.Migration
import ru.tinkoff.kora.application.graph.All
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.common.annotation.Root

/**
 * Временный зонд: если этот компонент собрался, значит KSP умеет собирать
 * `All<Migration>` по абстрактному базовому классу — то, на чём держится runner.
 *
 * `@Root` обязателен: Kora резолвит граф только от корневого набора, и компонент,
 * от которого никто не зависит, в сгенерированный граф не попадает вообще.
 */
@Root
@Component
class ProbeCollector(migrations: All<Migration>) {
    val names: List<String> = migrations.map { it.name }
}
