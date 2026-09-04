# Пример: Comparison-архетип

> Пошаговое описание узлов — в [гайде](../USER_GUIDE.md).

**Задача.** По тикету SAMPLE-001 после миграции с одного Cassandra-кластера
на другой DBA хочет получить полный список контрактов, у которых значение
`value` различается между `primary` и `replica`. Никаких записей —
только diff-репорт.

Это «read-only» архетип: чисто аналитический план, ноль side-effects.

## Setup

Сборка (KSP) и `@KoraApp` — см.
[resend-archetype.md §2–3, §8](resend-archetype.md); прогон архетипа целиком против
двух реальных кластеров — в
[pilot-тесте](../../kora/src/test/kotlin/io/github/dsudomoin/migration/kora/pilot/ComparisonPilotTest.kt).
Это буквально этот архетип на новом API: `output` × 2, одна стадия, `.chunked(...)`
в источнике — можно смотреть как на исполняемую версию текста ниже.
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
interface SampleConfig {
    fun inputFile(): String
    fun batchSize(): Int = 200
    fun parallel(): Int = 4
    fun initialContractId(): String?     // отсутствует в конфиге → null
}
```

## Скрипт

```kotlin
package com.example.migrations

import com.datastax.oss.driver.api.core.CqlSession
import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.MigrationDefinition
import io.github.dsudomoin.migration.csv.readCsv
import io.github.dsudomoin.migration.error.includeItem
import io.github.dsudomoin.migration.kora.ops.cassandra
import io.github.dsudomoin.migration.migration
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.common.Tag

@Component
class SampleMigration(
    @Tag(PrimaryCluster::class) private val primary: CqlSession,
    @Tag(ReplicaCluster::class) private val replica: CqlSession,
    private val config: SampleConfig,
) : MigrationDefinition {

    override val name = "SAMPLE-001"

    override fun plan() = migration(name = name, author = "dba") {
        val mismatches = output("mismatches.csv", "contract", "primary", "replica")
        val missingOnReplica = output("missing-on-replica.csv", "contract", "primary")

        // Ошибка конфига должна быть видна до первого запроса в кластеры, а не на середине.
        validate {
            require(config.batchSize() > 0) { "sample.batchSize должен быть > 0" }
            require(config.parallel() > 0) { "sample.parallel должен быть > 0" }
        }

        source(
            parallel = config.parallel(),
            onItemError = ItemError.Skip,
            items = {
                errors.includeItem<List<String>> { "batch of ${it.size}, first=${it.firstOrNull()}" }

                // Локальная переменная, а не config.initialContractId() внутри filter:
                // у метода интерфейса нет smart cast, и сравнение с null пришлось бы писать дважды.
                val from = config.initialContractId()

                readCsv(config.inputFile()) { it.getValue("contract") }
                    .filter { from == null || it > from }
                    .chunked(config.batchSize())
            },
        ) { batch ->
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

Три вещи, которые изменились по сравнению со старым API и на которые стоит посмотреть:

- **Оба выхода объявлены в билдере**, а не в теле. Файл открывается один раз на прогон,
  заголовки задаются один раз, закрывает его движок — писать `close()` не нужно и негде.
- **Батчинг — обычный `Sequence.chunked(N)`**, отдельного параметра `chunk` у стадии больше
  нет. Ленивость сохраняется: миллион строк из CSV не материализуется, режется на лету.
- **`items = { }` выполняется один раз, до первого обработчика** — поэтому регистрация
  сериализаторов (`errors.includeItem`) живёт именно там.

`parallel = 4` — это четыре реально работающих воркера: пул runner'а cached и выдаёт
столько потоков, сколько запросила конкретная стадия. `migration.defaults.parallel`
задаёт лишь значение аргумента по умолчанию, если его не написали явно. Четыре батча
= до восьми одновременных запросов (по два на батч), их разруливает пул соединений
самого драйвера — `advanced.connection.pool.localSize` в конфиге кластера.

`readCsv` по умолчанию `onRowError = ItemError.Fail`: битая строка входного файла валит
прогон. Если список контрактов приезжает из чужой выгрузки, лучше
`readCsv(config.inputFile(), onRowError = ItemError.Skip) { it.getValue("contract") }` —
строка уедет в `errors.csv` и в счётчик `report.sourceSkipped` (`Source rows dropped`
в отчёте), а сравнение продолжится. В `errorThreshold` такие строки не считаются — порог
сторожит ошибки обработки, а не грязь на входе.

## Гранулярность и возобновление

Item стадии — **батч**, а не контракт. Значит:

- `ItemError.Skip` при отказе одного из кластеров теряет весь батч (200 контрактов),
  и в `errors.csv` уедет одна строка — поэтому сериализатор зарегистрирован на
  `List<String>`, а не на `String`;
- `report.processed` считает батчи: 200 000 контрактов при `batchSize = 200` дадут
  `Processed: 1 000`;
- `errorThreshold` тоже считает батчи.

Возобновление после падения — `initialContractId`: фильтр по возрастающему id
отбрасывает уже сравнённое. Ровно этот сценарий проверяет второй тест пилота.
Если сравнение достаточно длинное, чтобы курсор хотелось двигать автоматически,
источником становится `pages(...)` вместо `readCsv` — см.
[export-archetype.md](export-archetype.md#почему-pages-а-не-jdbcstream).

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
гейт (репетиция обязана читать), а учёт item'ов ведёт движок стадии, а не op. Для
**записи** так делать нельзя — она обязана идти через `execute` или через
`write("label") { ... }`, иначе пройдёт мимо гейта и выполнится под dry-run
по-настоящему.

## Запуск

```bash
MIGRATION_RUN=SAMPLE-001 ./gradlew run

# Возобновить с места падения
MIGRATION_RUN=SAMPLE-001 SAMPLE_INITIAL_CONTRACT=A-77123 ./gradlew run
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
| `MigrationDefinition` + `plan()` | `name` — константа; план строится один раз и только у выбранной миграции |
| Два source-кластера с `@Tag` | Constructor inject через `@Tag(PrimaryCluster::class)` и `@Tag(ReplicaCluster::class)` |
| Two-tier CSV output | `output(...)` × 2 в билдере, оба под `outputFolder`, авто-закрытие |
| `validate { }` | Проверка конфига один раз, до первой стадии и до любого запроса |
| `.chunked(batchSize)` | Батчинг обычным `Sequence.chunked` — ради `where id in :ids`, а не ради движка |
| Чисто read-only | Ни `execute`, ни `write`, ни `publish` — side-effect'ов нет вовсе. `dryRun = true` и `false` ведут себя одинаково. Под репетицией runner допишет в лог и отчёт предупреждение «processed N item(s) but intercepted 0 writes» — оно ищет забытый dry-run-гейт, и для этого архетипа ожидаемо |
| Initial-id фильтр для возобновления | `.filter { from == null || it > from }` — если прогон упал на середине, рестартуешь с того же ID |
| `ItemError.Skip` | Если запрос к одному из кластеров упал — пропускаем батч (запишется в `errors.csv` авто-репортером), сравнение продолжается |

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
source(
    parallel = config.parallel(),
    onItemError = ItemError.Skip,
    items = { readCsv(config.inputFile()) { it.getValue("contract") }.chunked(config.batchSize()) },
) { batch ->
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

## Вариация: сравнение в две стадии

Если после diff'а хочется отдельным проходом починить найденное, это вторая стадия —
а не второй скрипт. Стадии идут строго последовательно, и починка не начнётся, если
сравнение провалилось:

```kotlin
override fun plan() = migration(name = name, author = "dba") {
    val mismatches = output("mismatches.csv", "contract", "primary", "replica")
    val repaired = output("repaired.csv", "contract")

    // Найденные расхождения живут в поле класса: между стадиями состояние передаётся
    // обычным способом, движок для этого ничего не предлагает и не должен.
    val found = java.util.concurrent.ConcurrentLinkedQueue<String>()

    source(name = "compare", parallel = 4, onItemError = ItemError.Skip, items = { /* ... */ }) { batch ->
        // ... сравнение; при расхождении:
        // mismatches.row(id, pv, rv); found += id
    }

    source(name = "repair", parallel = 2, onItemError = ItemError.Skip, items = { found.asSequence() }) { id ->
        write("replica.repair", args = mapOf("contract" to id)) {
            cassandra(replica).execute("update t.items set value = :v where id = :id", "v" to "...", "id" to id)
            WriteOutcome.Applied
        }
        repaired.row(id)
    }
}
```

`write(...)` здесь нужен не ради гейта — `cassandra.execute` проходит его сам, — а ради
учёта: `appliedWrites["replica.repair"]` в отчёте покажет, сколько контрактов реально
починено. Если бы отрицательный исход был штатным (строка не подошла), тело вернуло бы
`WriteOutcome.Rejected("reason")` и он попал бы в `rejectedWrites` отдельно от сбоев.
