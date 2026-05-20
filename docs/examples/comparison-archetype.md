# Пример: Comparison-архетип

**Задача.** По тикету SAMPLE-001 после миграции с одного Cassandra-кластера
на другой DBA хочет получить полный список контрактов, у которых значение
`value` различается между `primary` и `replica`. Никаких записей —
только diff-репорт.

Это «read-only» архетип: чисто аналитический скрипт, ноль side-effects.

## Setup

Для HOCON `migration`, Cassandra-кластеров (с `@Tag`), `@KoraApp` — см.
[resend-archetype.md §2–3, §8](resend-archetype.md) и [pilot-тест](../../kora/src/test/kotlin/io/migration/kora/pilot/ComparisonPilotTest.kt).
В сборке нужен `database-cassandra` модуль Kora.

## Конфиг скрипта + tag-классы

```hocon
cassandra {
  primary { contactPoints = ["primary.ca.prod:9042"]; localDatacenter = "dc1"; keyspace = "t" }
  replica { contactPoints = ["replica.ca.prod:9042"]; localDatacenter = "dc2"; keyspace = "t" }
}

sample {
  inputFile  = "input/contracts.csv"
  batchSize  = 200
  parallel   = 4
  initialContractId = null
  initialContractId = ${?TASK42002_INITIAL}
}
```

```kotlin
class PrimaryCluster
class ReplicaCluster

@ConfigSource("sample")
data class SampleConfig(
    var inputFile: String,
    var batchSize: Int,
    var parallel: Int,
    var initialContractId: String?,
)
```

## Скрипт

```kotlin
package com.example.migrations

import com.datastax.oss.driver.api.core.CqlSession
import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.csv.openCsv
import io.github.dsudomoin.migration.csv.readCsv
import io.github.dsudomoin.migration.forEach
import io.github.dsudomoin.migration.kora.ops.cassandra
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.common.Tag

@Component
class SampleMigration(
    @Tag(PrimaryCluster::class) private val primary: CqlSession,
    @Tag(ReplicaCluster::class) private val replica: CqlSession,
    private val config: SampleConfig,
) : Migration(name = "SAMPLE-001", author = "dba") {

    override fun MigrationContext.migrate() {
        val contracts = readCsv(config.inputFile) { it["contract"]!! }
            .filter { config.initialContractId == null || it > config.initialContractId!! }
            .toList()

        val mismatches = openCsv("mismatches.csv", "contract", "primary", "replica")
        val missingOnReplica = openCsv("missing-on-replica.csv", "contract", "primary")

        forEach(contracts, chunk = config.batchSize, parallel = config.parallel, onError = OnError.Skip) { batch ->
            val p = cassandra(primary)
                .query("select id, value from t.items where id in :ids", "ids" to batch) { it.getString("id") to it.getString("value") }
                .toMap()
            val r = cassandra(replica)
                .query("select id, value from t.items where id in :ids", "ids" to batch) { it.getString("id") to it.getString("value") }
                .toMap()

            batch.forEach { id ->
                val pv = p[id]
                val rv = r[id]
                when {
                    pv != null && rv == null  -> missingOnReplica.row(id, pv)
                    pv != rv                  -> mismatches.row(id, pv ?: "<null>", rv ?: "<null>")
                    // pv == rv — совпадение, ничего не пишем
                }
            }
        }
    }
}
```

## Запуск

```bash
MIGRATION_RUN=SAMPLE-001 ./gradlew run
```

Артефакты — в `logs/SAMPLE-001/`:
```
logs/SAMPLE-001/
├── migration.log
├── errors.csv             # пусто, если SKIP не сработал
├── errors.log
├── mismatches.csv         # contract, primary value, replica value
└── missing-on-replica.csv # contract, primary value
```

## Что архетип демонстрирует

| Фича | Где |
|---|---|
| Два source-кластера с `@Tag` | Constructor inject через `@Tag(PrimaryCluster::class)` и `@Tag(ReplicaCluster::class)` |
| Two-tier CSV output | `openCsv` × 2, оба под `outputFolder` — никаких `csvSink` |
| Чисто read-only | Ни `execute`, ни `mutation`, ни `topic.send` — нет вообще side-effects. `dryRun = true` или `false` ведут себя одинаково |
| Initial-id фильтр для возобновления | `.filter { it > config.initialContractId }` — если миграция упала на середине, рестартуешь с того же ID |
| `OnError.Skip` | Если запрос к одному из кластеров упал — пропускаем батч (запишется в `errors.csv` авто-репортером), миграция продолжается |

## Вариация: для миллиона контрактов — стрим из CSV

Если входной CSV длинный (1M+ строк), материализовать в `List<String>` дорого:

```kotlin
val contracts = readCsv(config.inputFile) { it["contract"]!! }   // Sequence<String>

forEach(contracts, chunk = 200, parallel = 4, onError = OnError.Skip) { batch ->
    // тот же код
}
```

`forEach` умеет работать с `Sequence` напрямую (есть перегрузка). Память не
расходуется на полный список — обрабатываются батчи по мере поступления.
