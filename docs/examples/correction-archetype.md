# Пример: Correction-архетип

> Пошаговое описание узлов — в [гайде](../USER_GUIDE.md).

**Задача.** По тикету SAMPLE-001 compliance прислал CSV со списком ID клиентов,
которым нужно проставить `status = 'UNDER_REVIEW'` и оставить аудит-запись.
Никаких внешних сервисов, никакой Kafka — только Postgres.

Это самый «дешёвый» архетип. План умещается в один `output` и одну стадию.

## Setup
Сборка (KSP), `db.*`, `@KoraApp` — см.
[resend-archetype.md §2–3, §8](resend-archetype.md). Здесь только то, что
специфично для этого скрипта. Секция runner'а — минимум из двух строк:

```hocon
migration {
  run    = ${?MIGRATION_RUN}
  dryRun = ${?MIGRATION_DRY_RUN}
}
```

Строка `dryRun` обязательна: библиотека не читает окружение сама, `${?VAR}` —
единственный канал. Забудешь её — `MIGRATION_DRY_RUN=true` из раздела «Запуск»
выполнит все `update`'ы по-настоящему.

**Запускаемый код.** Отдельного clone&run-проекта в репозитории нет, но
JDBC-операции этого архетипа покрыты интеграционным тестом на Testcontainers
(реальный Postgres 16):
[`SqlOpsIntegrationTest`](../../kora/src/test/kotlin/io/github/dsudomoin/migration/kora/ops/SqlOpsIntegrationTest.kt)
— `./gradlew :kora:test`. Полный end-to-end прогон плана-архетипа против двух
реальных БД — в
[`ComparisonPilotTest`](../../kora/src/test/kotlin/io/github/dsudomoin/migration/kora/pilot/ComparisonPilotTest.kt).

## Конфиг скрипта

```hocon
sample {
  inputFile = "input/incident-customers.csv"
  batchSize = 500
  parallel  = 4
}
```

```kotlin
import ru.tinkoff.kora.config.common.annotation.ConfigSource

@ConfigSource("sample")
interface SampleConfig {
    fun inputFile(): String
    fun batchSize(): Int = 500
    fun parallel(): Int = 4
}
```

`parallel = 4` здесь означает 4 одновременно работающих батча, каждый со своей
транзакцией и своим соединением — значит `db.maxPoolSize` должен быть не меньше
этого числа, иначе воркеры будут ждать соединение в Hikari.

## Скрипт

```kotlin
package com.example.migrations

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.MigrationDefinition
import io.github.dsudomoin.migration.csv.readCsv
import io.github.dsudomoin.migration.error.includeItem
import io.github.dsudomoin.migration.kora.ops.jdbc
import io.github.dsudomoin.migration.kora.ops.transactional
import io.github.dsudomoin.migration.migration
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory

@Component
class SampleMigration(
    private val db: JdbcConnectionFactory,
    private val config: SampleConfig,
) : MigrationDefinition {

    override val name = "SAMPLE-001"

    override fun plan() = migration(name = name, author = "team") {
        val processed = output("processed.csv", "id")

        source(
            parallel = config.parallel(),
            onItemError = ItemError.Skip,
            items = {
                // Сериализатор регистрируется здесь: items выполняется до первого обработчика.
                // Тип — List<String>, потому что item стадии после .chunked(...) это батч, а не id.
                errors.includeItem<List<String>> { "batch of ${it.size}, first=${it.firstOrNull()}" }

                // Батчинг — обычный Sequence.chunked: отдельного параметра chunk у стадии нет.
                readCsv(config.inputFile()) { it.getValue("customer_id") }
                    .chunked(config.batchSize())
            },
        ) { batch ->
            transactional(jdbc(db)) {
                batch.forEach { id ->
                    execute("update customers set status = 'UNDER_REVIEW', updated_at = now() where id = :id", "id" to id)
                    execute("insert into customer_audit(customer_id, event, ts) values (:id, 'incident_review', now())", "id" to id)
                    processed.row(id)
                }
            }
        }
    }
}
```

`includeItem` — extension на `ErrorReporter`, живёт в
`io.github.dsudomoin.migration.error`; без этого импорта скрипт не соберётся.

`readCsv` по умолчанию идёт с `onRowError = ItemError.Fail`: битая строка входного
файла валит прогон до первого `update`'а. Это разумный дефолт для compliance-списка
— лучше починить файл, чем молча обработать половину. Если входной CSV заведомо
грязный, политика задаётся явно:
`readCsv(config.inputFile(), onRowError = ItemError.Skip) { it.getValue("customer_id") }` —
тогда строка уедет в `errors.csv` и посчитается в `report.sourceSkipped` (строка
`Source rows dropped` в отчёте), а до обработчика не доедет. Это отдельный счётчик от
`report.skipped`: `errorThreshold` его не считает — порог сторожит ошибки обработки,
а не грязь на входе.

Если из `update`/`insert` нужно забрать строки (`... returning id`), это
`executeReturning(sql, ...) { rs -> ... }`, а не `query`: `query`/`stream` принимают
только читающие запросы и на пишущем бросают `IllegalArgumentException`. Причина —
они не проходят dry-run гейт, и `query("insert ... returning id")` выполнялся бы
по-настоящему во время репетиции. `executeReturning` проходит гейт как обычная
запись и под dry-run возвращает пустой список.

## Нужен ли здесь `write { }`

Не обязателен. `jdbc.execute` / `batch` / `executeReturning` **сами** проходят через
dry-run-гейт, поэтому под репетицией ничего не запишется и без обёртки.

`write { }` / `writeRows { }` обязательны там, где вызов идёт мимо ops библиотеки:
типизированный Kora `@HttpClient`, `@KafkaPublisher`, чужой репозиторий, SDK — их DSL
перехватить не может.

Поверх ops это осознанный обмен, а не улучшение по умолчанию:

- **что получаешь** — бизнес-исход в отчёте: `appliedWrites["customers.under-review"]`
  и отдельно `rejectedWrites` для «ни одна строка не подошла», вместо безымянного
  `jdbc.execute`;
- **что теряешь** — под dry-run тело `write` не выполняется вовсе, поэтому вложенный
  `jdbc.execute` до своего гейта не доходит: в breakdown окажется одна твоя метка, а не
  честное число SQL-запросов.

Пример, где обмен оправдан — там, где ноль изменённых строк это штатный исход, а не сбой:

```kotlin
source(
    parallel = 8,
    onItemError = ItemError.Skip,
    items = { readCsv(config.inputFile()) { it.getValue("customer_id") } },
) { id ->
    // 0 затронутых строк — это не сбой, а Rejected("no rows matched"):
    // клиента уже нет или он уже в нужном статусе. В отчёте это отдельный счётчик.
    writeRows("customers.under-review", args = mapOf("id" to id)) {
        jdbc(db).execute("update customers set status = 'UNDER_REVIEW' where id = :id and status <> 'UNDER_REVIEW'", "id" to id)
    }
    processed.row(id)
}
```

`writeRows` уместен ровно там, где операция реально возвращает число изменённых строк
(`UPDATE ... WHERE`). Для Cassandra-INSERT или delete+insert числа строк нет — там
`write("label") { ...; WriteOutcome.Applied }`.

## Гранулярность: батч — это один item

При `.chunked(500)` единицей учёта, единицей `Skip` и единицей потери является **батч**:

- исключение на 137-м клиенте из 500 обрывает весь батч; транзакция откатится целиком
  (в этом и смысл `transactional`), а оставшиеся 363 не обработаются никогда;
- `report.skipped++` — это +1, а не +364, и `errorThreshold` считает батчи;
- в `errors.csv` уедет одна строка, и item'ом там будет `List<String>` из 500 id —
  поэтому сериализатор регистрируется на `List<String>`, а не на `String`.

Батчинг берут ради round-trip'ов (одна транзакция на 500 записей вместо 500 транзакций).
Если тело — цикл независимых вызовов, честнее item-by-item: убери `.chunked(...)`, и
`Skip` будет терять ровно одного клиента.

## Запуск

```bash
# Dry-run: посчитать сколько update'ов ушло бы в БД, без реального выполнения
MIGRATION_RUN=SAMPLE-001 MIGRATION_DRY_RUN=true \
  DB_USER=app DB_PASSWORD=... ./gradlew run

# Боевой
MIGRATION_RUN=SAMPLE-001 \
  DB_USER=app DB_PASSWORD=... ./gradlew run
```

Артефакты — в `logs/SAMPLE-001/`:
```
logs/SAMPLE-001/
├── migration.log
├── errors.csv      # батчи, попавшие в Skip-ветку
├── errors.log
└── processed.csv   # ID, которые успешно обновились
```

Что именно делает dry-run с этим скриптом: `transactional` **не** пропускает блок
целиком — тело выполняется, пропускаются отдельные записи внутри него. То есть
соединение не открывается, оба `execute` не доходят до БД (в breakdown отчёта —
`jdbc.transactional` по числу батчей и `jdbc.execute` по числу запросов), а
`processed.row(id)` отрабатывает как обычно — `processed.csv` после репетиции будет
заполнен. Это тот же принцип, что и у `output`: CSV-выход остаётся диагностическим
артефактом прогона.

## Что архетип демонстрирует

| Фича | Где |
|---|---|
| Read CSV → Postgres update | Каноничный correction-сценарий |
| `output(...)` в билдере | Файл открывается один раз на прогон, закрывает движок |
| `.chunked(N)` в `items` | Батчинг — обычный `Sequence.chunked`, отдельного параметра у стадии нет |
| `transactional { ... }` | update + insert в одной транзакции (rollback если что-то упало) |
| `ItemError.Skip` | Если батч упал — он пишется в `errors.csv` авто-репортером, стадия продолжается. Retry для transient deadlock'ов Postgres — на уровне Kora `@Retry` на repository-методе, не на уровне DSL |
| `errors.includeItem<List<String>> { ... }` | Per-type сериализатор: item — батч, в CSV пишется его размер и первый id |
| `writeRows("label") { ... }` | Отделяет «применилось» от «ни одна строка не подошла» — там, где это отдельный бизнес-исход |
| Параллелизм по батчам | `parallel = 4` батча по 500 строк бегут одновременно — но **внутри** одного батча update'ы идут sequential (один tx, один Connection). В каждый момент времени активно 4 БД-соединения, не 2000 |
| `transactional` **внутри** обработчика | Единственный корректный порядок. Наоборот — параллельная стадия внутри `transactional` — tx-bound `SqlOps` бросит `IllegalStateException`: `java.sql.Connection` не потокобезопасен, и DSL не даёт молча испортить данные |

## Вариация: без транзакции (когда update идемпотентен)

```kotlin
source(
    parallel = 8,
    onItemError = ItemError.Skip,
    items = { readCsv(config.inputFile()) { it.getValue("customer_id") } },
) { id ->
    jdbc(db).execute("update customers set status = 'UNDER_REVIEW' where id = :id", "id" to id)
    processed.row(id)
}
```

Каждый `execute` — своя micro-транзакция. Восемь воркеров здесь настоящие, значит
восемь одновременных `db.inTx` — `db.maxPoolSize` поднимай до восьми, иначе половина
потоков будет стоять в очереди за соединением.

На 1M записей это быстрее, чем транзакция по 500, потому что нет блокировок на
батч. Уместно когда:
- update идемпотентен (повторный вызов даёт тот же результат);
- audit-запись не нужна или пишется отдельной стадией.

Отдельной стадией — это буквально так:

```kotlin
override fun plan() = migration(name = name, author = "team") {
    val processed = output("processed.csv", "id")
    val ids = input("ids") { readCsv(config.inputFile()) { it.getValue("customer_id") }.toList() }

    source(name = "update", parallel = 8, onItemError = ItemError.Skip, items = { resolve(ids).asSequence() }) { id ->
        jdbc(db).execute("update customers set status = 'UNDER_REVIEW' where id = :id", "id" to id)
        processed.row(id)
    }

    source(name = "audit", parallel = 4, items = { resolve(ids).asSequence().chunked(500) }) { batch ->
        jdbc(db).batch("insert into customer_audit(customer_id, event, ts) values (?, 'incident_review', now())", batch) { ps, id ->
            ps.setString(1, id)
        }
    }
}
```

Стадии идут строго последовательно: аудит не начнётся, пока апдейт не закончился, и
не начнётся вовсе, если апдейт провалился. Имена обязательны — как только стадий больше
одной, план без имён не построится. `input("ids")` читает файл один раз на прогон:
обе стадии спрашивают одно и то же значение, а не открывают файл дважды.

## Вариация: две Postgres-базы (main + audit)

Когда апдейт идёт в одну БД, а аудит — в другую (отдельный инстанс под другим
владельцем). Источников столько, сколько `@Tag`-инъекций в конструктор — ровно как
два Cassandra-кластера в [comparison-archetype.md](comparison-archetype.md).

**Поднять второй datasource** — стандартный Kora-приём «несколько БД через `@Tag`»:
дефолтная секция `db {}` приходит из `JdbcDatabaseModule`, вторую (`audit {}`)
регистрируешь своим `@Module` с `@Tag`-компонентами вплоть до `JdbcDatabase` (он же
`JdbcConnectionFactory`). Тег — пустой класс-маркер `class AuditDb` (как
`PrimaryCluster` в comparison). Точные сигнатуры — в Kora-доке *database-jdbc →
Multiple Databases*; для скрипта важно лишь, что в конструктор прилетают **два**
`JdbcConnectionFactory`.

### Конфиг

```hocon
migration {
  run    = ${?MIGRATION_RUN}
  dryRun = ${?MIGRATION_DRY_RUN}
}

db {
  jdbcUrl     = ${MAIN_JDBC_URL}
  username    = ${MAIN_USER}
  password    = ${MAIN_PASS}
  poolName    = "sample-main"
  maxPoolSize = 4
}

audit {
  jdbcUrl     = ${AUDIT_JDBC_URL}
  username    = ${AUDIT_USER}
  password    = ${AUDIT_PASS}
  poolName    = "sample-audit"
  maxPoolSize = 4
}
```

`poolName` в Kora — обязательный ключ `JdbcDatabaseConfig` наравне с
`jdbcUrl`/`username`/`password`: без него граф не соберётся. У двух датасорсов имена
пулов должны различаться, иначе метрики и логи Hikari сольются в один. `maxPoolSize`
держи не ниже `sample.parallel` — каждый батч занимает по соединению в каждой из баз.

### Скрипт

```kotlin
import ru.tinkoff.kora.common.Tag

/** Пустой класс-маркер — по нему Kora различает второй датасорс. */
class AuditDb

@Component
class SampleMigration(
    private val main: JdbcConnectionFactory,                        // дефолтный db {}
    @Tag(AuditDb::class) private val audit: JdbcConnectionFactory,  // секция audit {}
    private val config: SampleConfig,
) : MigrationDefinition {

    override val name = "SAMPLE-001"

    override fun plan() = migration(name = name, author = "team") {
        source(
            parallel = config.parallel(),
            onItemError = ItemError.Skip,
            items = {
                errors.includeItem<List<String>> { "batch of ${it.size}, first=${it.firstOrNull()}" }
                readCsv(config.inputFile()) { it.getValue("customer_id") }.chunked(config.batchSize())
            },
        ) { batch ->
            transactional(jdbc(main)) {
                batch.forEach { id ->
                    execute("update customers set status = 'UNDER_REVIEW', updated_at = now() where id = :id", "id" to id)
                }
            }
            transactional(jdbc(audit)) {
                batch.forEach { id ->
                    execute("insert into customer_audit(customer_id, event, ts) values (:id, 'incident_review', now())", "id" to id)
                }
            }
        }
    }
}
```

### Важно: это ДВЕ независимые транзакции — атомарности между БД нет

`transactional` открывает tx на **одном** `JdbcConnectionFactory` (`db.inTx`).
Двухфазного коммита (XA) поверх двух баз DSL не даёт — и не пытается. Значит:

- если `main` закоммитился, а `audit` упал → батч уедет в `errors.csv`
  (`ItemError.Skip`), но `customers` **уже обновлены**. Rollback'а через границу БД не
  будет.

Как с этим жить (по убыванию надёжности):

1. **Outbox.** Писать аудит в ту же `main`-БД (одна tx, атомарно), а в audit-базу
   перекладывать отдельным разгребателем. Единственный вариант с настоящей
   атомарностью.
2. **Идемпотентный re-run.** Порядок «сначала `audit`, потом `main`»; повторный
   прогон по `errors.csv` до-вставляет недостающий аудит (`insert ... on conflict do
   nothing`).
3. **Если вторая база read-only** (читаем справочник из `replica`, апдейтим `main`)
   — проблема снимается сама: один writer, одна tx. Безопасный дефолт, выбирай его,
   если задача позволяет.
