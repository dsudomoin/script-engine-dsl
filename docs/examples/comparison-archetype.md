# Пример: Comparison-архетип

**Задача.** По тикету SAMPLE-001 после миграции с одного Cassandra-кластера
на другой DBA хочет получить полный список контрактов, у которых значение
`value` различается между `primary` и `replica`. Никаких записей —
только diff-репорт.

Это «read-only» архетип: чисто аналитический скрипт, ноль side-effects.

## Setup

Сборка (KSP) и `@KoraApp` — см.
[resend-archetype.md §2–3, §8](resend-archetype.md); прогон архетипа целиком против
двух реальных кластеров — в
[pilot-тесте](../../kora/src/test/kotlin/io/github/dsudomoin/migration/kora/pilot/ComparisonPilotTest.kt).
В сборке нужен `database-cassandra` модуль Kora. Два `CqlSession` под `@Tag`
регистрируются своим `@Module` — тем же приёмом, что и вторая JDBC-база в
[correction-archetype.md](correction-archetype.md); из библиотеки в `@KoraApp`
подмешивается только `MigrationModule`.

## Конфиг скрипта + tag-классы

```hocon
migration {
  run    = ${?MIGRATION_RUN}
  dryRun = ${?MIGRATION_DRY_RUN}
}

# Две секции в форме kora-шного CassandraConfig — по одной на кластер. Дефолтная
# секция `cassandra {}` из CassandraModule не используется: оба сеанса поднимаются
# своим @Module с @Tag.
cassandra {
  primary {
    basic {
      contactPoints   = ["primary.ca.prod:9042"]
      dc              = "dc1"
      sessionKeyspace = "t"
      request {
        consistency = "LOCAL_QUORUM"   # уровень для всех запросов сессии — см. «Consistency» ниже
        timeout     = "10s"
      }
    }
  }
  replica {
    basic {
      contactPoints   = ["replica.ca.prod:9042"]
      dc              = "dc2"
      sessionKeyspace = "t"
      request {
        consistency = "LOCAL_QUORUM"
        timeout     = "10s"
      }
    }
  }
}

sample {
  inputFile  = "input/contracts.csv"
  batchSize  = 200
  parallel   = 4
  initialContractId = null
  initialContractId = ${?SAMPLE_INITIAL_CONTRACT}
}
```

Строка `dryRun` нужна даже здесь, где писать нечего: библиотека не читает окружение
сама, `MIGRATION_DRY_RUN=true` без `${?MIGRATION_DRY_RUN}` в конфиге не включает
ничего. Пусть секция `migration` во всех скриптах выглядит одинаково — тогда
скопированный в пишущий архетип конфиг не устроит сюрприз.

```kotlin
import ru.tinkoff.kora.config.common.annotation.ConfigSource

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

`parallel = 4` — это четыре реально работающих воркера: пул runner'а cached и выдаёт
столько потоков, сколько запросил конкретный `forEach`. `migration.defaults.parallel`
задаёт лишь значение аргумента по умолчанию, если его не написали явно. Четыре батча
= до восьми одновременных запросов (по два на батч), их разруливает пул соединений
самого драйвера — `advanced.connection.pool.localSize` в конфиге кластера.

`readCsv` по умолчанию `onRowError = OnError.Fail`: битая строка входного файла валит
прогон. Если список контрактов приезжает из чужой выгрузки, лучше
`readCsv(config.inputFile, onRowError = OnError.Skip) { it["contract"]!! }` — строка
уедет в `errors.csv` и посчитается в `errorThreshold`, а сравнение продолжится.

## Consistency: через DSL её не задать

Сравнение двух кластеров — ровно тот сценарий, где уровень согласованности определяет
результат: чтение с `ONE`/`LOCAL_ONE` на кластере с отстающей репликой выдаёт
расхождения, которых нет.

`CassandraOps` уровень согласованности не принимает. В нём нет ни параметра
`ConsistencyLevel`, ни выбора execution profile: `query`/`stream`/`execute` строят
`BoundStatement` через `boundStatementBuilder()` и биндят только именованные
параметры. Профили, объявленные в `cassandra.*.profiles`, выбрать тоже нечем.

Варианты (по возрастанию цены):

1. **Задать на уровне сессии** — `basic.request.consistency` в конфиге каждого
   кластера (см. HOCON выше). Уровень применится ко всем запросам этой сессии; для
   одноразового скрипта сравнения этого обычно достаточно, и это рекомендуемый путь.
2. **Свой op для конкретного запроса** — если нужен разный уровень для разных
   запросов в одном скрипте. `CqlSession` в конструкторе уже есть, обходить DSL
   нечем — просто соберите statement руками:

```kotlin
import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.Row

private fun <T> CqlSession.selectQuorum(cql: String, ids: List<String>, mapper: (Row) -> T): List<T> {
    val bound = prepare(cql).boundStatementBuilder()
        .setList("ids", ids, String::class.java)
        .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
        .build()
    return execute(bound).map(mapper).toList()
}
```

Чтение мимо DSL ничего не ломает: `query`/`stream` и так не проходят через dry-run
гейт (репетиция обязана читать), а учёт item'ов ведёт `forEach`, а не op. Для
**записи** так делать нельзя — она обязана идти через `execute`/`mutation`, иначе
пройдёт мимо гейта и выполнится под dry-run по-настоящему.

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
| Чисто read-only | Ни `execute`, ни `mutation`, ни `topic.send` — side-effect'ов нет вовсе. `dryRun = true` и `false` ведут себя одинаково. Под репетицией runner допишет в лог и отчёт предупреждение «processed N item(s) but intercepted 0 writes» — оно ищет забытый `mutation { }`, и для этого архетипа ожидаемо |
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

`forEach` умеет работать с `Sequence` напрямую (есть отдельная перегрузка с `chunk`,
режущая лениво). Память не расходуется на полный список — материализуется только
текущий батч.

Ранний выход из такого цикла безопасен: `readCsv` регистрирует открытый поток в
контексте, и runner закроет дескриптор в `finally` даже если `Sequence` не
дочитана до конца.

## Вариация: значение — UDT (`frozen<money>`)

«Самая большая боль» в Cassandra-сравнении — когда `value` не скаляр, а
user-defined type. Читать UDT-колонку DSL умеет напрямую: `query { row -> ... }`
отдаёт сырой DataStax `Row`, а на нём есть `getUdtValue(...)`.

Схема:

```cql
CREATE TYPE t.money (amount bigint, currency text);
CREATE TABLE t.items (contract text PRIMARY KEY, value frozen<money>);
```

Маппим UDT в доменный `data class` прямо в mapper'е — **не** сравниваем `UdtValue`
напрямую (почему — ниже):

```kotlin
import com.datastax.oss.driver.api.core.cql.Row

data class Money(val amount: Long, val currency: String)

// getUdtValue и getString у DataStax объявлены @Nullable — в Kotlin это `UdtValue?` и
// `String?`, поэтому оба случая приходится закрывать явно, иначе mapper не скомпилируется.
private fun Row.money(col: String): Money? =
    getUdtValue(col)?.let { Money(it.getLong("amount"), it.getString("currency") ?: "") }
```

```kotlin
forEach(contracts, chunk = config.batchSize, parallel = config.parallel, onError = OnError.Skip) { batch ->
    val p = cassandra(primary)
        .query("select contract, value from t.items where contract in :ids", "ids" to batch) {
            it.getString("contract") to it.money("value")
        }.toMap()
    val r = cassandra(replica)
        .query("select contract, value from t.items where contract in :ids", "ids" to batch) {
            it.getString("contract") to it.money("value")
        }.toMap()

    batch.forEach { id ->
        val pv = p[id]
        val rv = r[id]
        when {
            pv != null && rv == null -> missingOnReplica.row(id, "${pv.amount} ${pv.currency}")
            pv != rv                 -> mismatches.row(
                id,
                pv?.let { "${it.amount} ${it.currency}" } ?: "<null>",
                rv?.let { "${it.amount} ${it.currency}" } ?: "<null>",
            )
        }
    }
}
```

### Два подводных камня — именно из-за UDT

1. **Почему `data class`, а не сравнение `UdtValue` «в лоб».** `UdtValue.equals`
   сверяет не только значения полей, но и `DataType` — а это keyspace-qualified имя
   типа. Между двумя кластерами оно может не совпасть (либо формально совпасть, но
   дать нечитаемый diff в CSV). Маппинг в `Money` даёт честный value-equals и
   нормальный вывод в репорт.

2. **UDT нельзя забиндить как параметр `:name`.** Фабрика `cassandra(...)` биндит
   только скаляры, `List` и `Set` (см. `CassandraOps.setExplicit`). `Map`/UDT/tuple
   в `WHERE`/`IN` — мимо: бросит на этапе биндинга. В comparison это не мешает (UDT
   только в `SELECT`), но если UDT понадобится **в условии** — собирай
   `BoundStatementBuilder` сам в custom op и передавай `GenericType`. См.
   [customization.md](customization.md).
