# Пример: Correction-архетип

**Задача.** По тикету SAMPLE-001 compliance прислал CSV со списком ID клиентов,
которым нужно проставить `status = 'UNDER_REVIEW'` и оставить аудит-запись.
Никаких внешних сервисов, никакой Kafka — только Postgres.

Это самый «дешёвый» архетип. Скрипт умещается в ~15 строк.

## Setup
Для HOCON-блока `migration`, `db.*`, `@KoraApp` и Gradle — см.
[resend-archetype.md §2–3, §8](resend-archetype.md). Здесь только то, что
специфично для этого скрипта.

## Конфиг скрипта

```hocon
sample {
  inputFile = "input/incident-customers.csv"
  batchSize = 500
  parallel  = 4
}
```

```kotlin
@ConfigSource("sample")
data class SampleConfig(
    var inputFile: String,
    var batchSize: Int,
    var parallel: Int,
)
```

## Скрипт

```kotlin
package com.example.migrations

import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.csv.openCsv
import io.github.dsudomoin.migration.csv.readCsv
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

## Что архетип демонстрирует

| Фича | Где |
|---|---|
| Read CSV → Postgres update | Каноничный correction-сценарий |
| `transactional { ... }` | update + insert в одной транзакции (rollback если что-то упало) |
| `OnError.Skip` | Если update + audit упали — батч пишется в `errors.csv` авто-репортером, миграция продолжается. Retry для transient deadlock'ов Postgres — на уровне Kora `@Retry` на repository-методе, не на уровне DSL |
| `errors.includeItem<String> { ... }` | Per-type сериализатор: item — `customer_id`, в CSV пишется красиво |
| Один `openCsv` | Только список обработанных. Прочее (`errors.csv`, `migration.log`) появится автоматически от runner'а |
| Параллелизм по батчам | `parallel = 4` батча по `chunk = 500` строк бегут одновременно — но **внутри** одного батча update'ы идут sequential (один tx, один Connection). В каждый момент времени активно 4 БД-соединения, не 2000 |

## Вариация: без транзакции (когда update идемпотентен)

```kotlin
forEach(ids, parallel = 8, onError = OnError.Skip) { id ->
    jdbc(db).execute("update customers set status = 'UNDER_REVIEW' where id = :id", "id" to id)
    processed.row(id)
}
```

Каждый `execute` — своя micro-транзакция. На 1M записей быстрее, чем
`transactional` по 500, потому что нет блокировок на батч. Уместно когда:
- update идемпотентен (повторный вызов даёт тот же результат);
- audit-запись не нужна или пишется отдельным шагом.
