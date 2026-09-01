# Пример: Correction-архетип

**Задача.** По тикету SAMPLE-001 compliance прислал CSV со списком ID клиентов,
которым нужно проставить `status = 'UNDER_REVIEW'` и оставить аудит-запись.
Никаких внешних сервисов, никакой Kafka — только Postgres.

Это самый «дешёвый» архетип. Скрипт умещается в ~15 строк.

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
— `./gradlew :kora:test`. Полный end-to-end прогон миграции-архетипа против двух
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
data class SampleConfig(
    var inputFile: String,
    var batchSize: Int,
    var parallel: Int,
)
```

`parallel = 4` здесь означает 4 одновременно работающих батча, каждый со своей
транзакцией и своим соединением — значит `db.maxPoolSize` должен быть не меньше
этого числа, иначе воркеры будут ждать соединение в Hikari.

## Скрипт

```kotlin
package com.example.migrations

import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.csv.openCsv
import io.github.dsudomoin.migration.csv.readCsv
import io.github.dsudomoin.migration.error.includeItem
import io.github.dsudomoin.migration.forEach
import io.github.dsudomoin.migration.kora.ops.jdbc
import io.github.dsudomoin.migration.kora.ops.transactional
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory

@Component
class SampleMigration(
    private val db: JdbcConnectionFactory,
    private val config: SampleConfig,
) : Migration(name = "SAMPLE-001", author = "team") {

    override fun MigrationContext.migrate() {
        errors.includeItem<String> { "customer_id=$it" }

        val ids = readCsv(config.inputFile) { it["customer_id"]!! }.toList()
        val processed = openCsv("processed.csv", "id")

        forEach(
            ids,
            chunk = config.batchSize,
            parallel = config.parallel,
            onError = OnError.Skip,
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

Если из `update`/`insert` нужно забрать строки (`... returning id`), это
`executeReturning(sql, ...) { rs -> ... }`, а не `query`: `query`/`stream` принимают
только читающие запросы и на пишущем бросают `IllegalArgumentException`. Причина —
они не проходят dry-run гейт, и `query("insert ... returning id")` выполнялся бы
по-настоящему во время репетиции. `executeReturning` проходит гейт как обычная
запись и под dry-run возвращает пустой список.

`readCsv` по умолчанию идёт с `onRowError = OnError.Fail`: битая строка входного
файла валит прогон до первого `update`'а. Это разумный дефолт для compliance-списка
— лучше починить файл, чем молча обработать половину. Если входной CSV заведомо
грязный, политика задаётся явно:
`readCsv(config.inputFile, onRowError = OnError.Skip) { it["customer_id"]!! }` —
тогда строка уедет в `errors.csv`, посчитается в `report.skipped` и в
`errorThreshold`, а до `forEach` не доедет.

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
├── errors.csv      # ID, попавшие в Skip-ветку
├── errors.log
└── processed.csv   # ID, которые успешно обновились
```

Что именно делает dry-run с этим скриптом: `transactional` **не** пропускает блок
целиком — тело выполняется, пропускаются отдельные записи внутри него. То есть
соединение не открывается, оба `execute` не доходят до БД (в breakdown отчёта —
`jdbc.transactional` по числу батчей и `jdbc.execute` по числу запросов), а
`processed.row(id)` отрабатывает как обычно — `processed.csv`
после репетиции будет заполнен. Это тот же принцип, что и у `openCsv`: CSV-выход
остаётся диагностическим артефактом прогона.

## Что архетип демонстрирует

| Фича | Где |
|---|---|
| Read CSV → Postgres update | Каноничный correction-сценарий |
| `transactional { ... }` | update + insert в одной транзакции (rollback если что-то упало) |
| `OnError.Skip` | Если update + audit упали — батч пишется в `errors.csv` авто-репортером, миграция продолжается. Retry для transient deadlock'ов Postgres — на уровне Kora `@Retry` на repository-методе, не на уровне DSL |
| `errors.includeItem<String> { ... }` | Per-type сериализатор: item — `customer_id`, в CSV пишется красиво |
| Один `openCsv` | Только список обработанных. Прочее (`errors.csv`, `migration.log`) появится автоматически от runner'а |
| Параллелизм по батчам | `parallel = 4` батча по `chunk = 500` строк бегут одновременно — но **внутри** одного батча update'ы идут sequential (один tx, один Connection). В каждый момент времени активно 4 БД-соединения, не 2000. Воркеры настоящие: пул runner'а cached и выдаёт столько потоков, сколько запросил `forEach`; `migration.defaults.parallel` — лишь дефолт для вызовов без явного аргумента |
| `transactional` **внутри** `forEach` | Единственный корректный порядок. Наоборот — `forEach(parallel > 1)` внутри `transactional` — tx-bound `SqlOps` бросит `IllegalStateException`: `java.sql.Connection` не потокобезопасен, и DSL не даёт молча испортить данные |

## Вариация: без транзакции (когда update идемпотентен)

```kotlin
forEach(ids, parallel = 8, onError = OnError.Skip) { id ->
    jdbc(db).execute("update customers set status = 'UNDER_REVIEW' where id = :id", "id" to id)
    processed.row(id)
}
```

Каждый `execute` — своя micro-транзакция. Восемь воркеров здесь настоящие, значит
восемь одновременных `db.inTx` — `db.maxPoolSize` поднимай до восьми, иначе половина
потоков будет стоять в очереди за соединением.

На 1M записей это быстрее, чем `transactional` по 500, потому что нет блокировок на
батч. Уместно когда:
- update идемпотентен (повторный вызов даёт тот же результат);
- audit-запись не нужна или пишется отдельным шагом.

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
) : Migration(name = "SAMPLE-001", author = "team") {

    override fun MigrationContext.migrate() {
        errors.includeItem<String> { "customer_id=$it" }
        val ids = readCsv(config.inputFile) { it["customer_id"]!! }.toList()

        forEach(ids, chunk = config.batchSize, parallel = config.parallel, onError = OnError.Skip) { batch ->
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
  (`OnError.Skip`), но `customers` **уже обновлены**. Rollback'а через границу БД не
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
