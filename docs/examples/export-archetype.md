# Пример: Export-архетип (DB → CSV snapshot)

**Задача.** По тикету SAMPLE-001 финдиру нужен CSV-снимок всех клиентов
в статусе `VIP` с балансом и датой присоединения — чтобы скормить в Excel
для квартальной отчётности. Источник — Postgres, выход — один CSV.

Это «read-only export» архетип: никаких внешних сервисов, никаких
update'ов. Самый короткий из доступных скриптов.

## Setup

Для HOCON `migration` и `db.*`, `@KoraApp` — см.
[resend-archetype.md §2–3, §8](resend-archetype.md).

## Конфиг скрипта (минимальный)

```hocon
sample {
  minBalance = 1000000   # фильтр — только клиенты с балансом >= порога
  parallel   = 4
}
```

```kotlin
@ConfigSource("sample")
data class SampleConfig(
    var minBalance: Long,
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
import io.github.dsudomoin.migration.forEach
import io.github.dsudomoin.migration.kora.ops.jdbc
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory

@Component
class SampleMigration(
    private val db: JdbcConnectionFactory,
    private val config: SampleConfig,
) : Migration(name = "SAMPLE-001", author = "finance") {

    override fun MigrationContext.migrate() {
        val out = openCsv("vip-snapshot.csv", "id", "email", "balance", "joined_at")

        jdbc(db).stream(
            "select id, email, balance, joined_at from customers where status = 'VIP' and balance >= :min order by id",
            "min" to config.minBalance,
            fetchSize = 5000,
            mapper = { rs ->
                Vip(
                    id = rs.getLong("id"),
                    email = rs.getString("email"),
                    balance = rs.getLong("balance"),
                    joinedAt = rs.getTimestamp("joined_at").toInstant(),
                )
            },
        ) { vips ->
            forEach(vips, parallel = config.parallel, onError = OnError.Skip) { v ->
                out.row(v.id, v.email, v.balance, v.joinedAt)
            }
        }
    }

    private data class Vip(val id: Long, val email: String, val balance: Long, val joinedAt: java.time.Instant)
}
```

## Запуск

```bash
MIGRATION_RUN=SAMPLE-001 \
  DB_USER=app DB_PASSWORD=... ./gradlew run
```

Артефакты — в `logs/SAMPLE-001/`:
```
logs/SAMPLE-001/
├── migration.log
├── errors.csv          # пусто, если БД ровно отвечала
├── errors.log
└── vip-snapshot.csv    # отдаёшь финдиру
```

## Что архетип демонстрирует

| Фича | Где |
|---|---|
| `jdbc(db).stream(...) { rows -> ... }` | True JDBC cursor (`setFetchSize`), callback-scoped `Sequence<T>` — для больших выгрузок (>>RAM) must-have |
| `forEach(rows, ...)` внутри `consume` | `rows: Sequence<T>` валиден только в scope `consume`; `forEach` пишется прямо там |
| Один `openCsv` | Output — целевой артефакт миграции |
| `OnError.Skip` | Если строка нечитаемая (`null` в not-null поле и т.д.) — пропускаем, не валим всю выгрузку |
| Минимум зависимостей | Только `JdbcConnectionFactory` — никаких Kafka/HTTP/Cassandra |

## Вариация: разбивка на несколько CSV по сегменту

Если финдиру удобнее разные категории VIP'ов в отдельных файлах:

```kotlin
override fun MigrationContext.migrate() {
    val gold = openCsv("vip-gold.csv",     "id", "email", "balance")
    val platinum = openCsv("vip-platinum.csv", "id", "email", "balance")
    val diamond = openCsv("vip-diamond.csv",  "id", "email", "balance")

    jdbc(db).stream(
        "select ... where status = 'VIP'",
        fetchSize = 5000,
        mapper = { Vip.fromRs(it) },
    ) { vips ->
        forEach(vips, parallel = 4, onError = OnError.Skip) { v ->
            when {
                v.balance >= 100_000_000L -> diamond.row(v.id, v.email, v.balance)
                v.balance >=  10_000_000L -> platinum.row(v.id, v.email, v.balance)
                else                      -> gold.row(v.id, v.email, v.balance)
            }
        }
    }
}
```

Три CSV — три строки сверху, все живут до конца migrate(), `row` thread-safe.
Все три `openCsv` зарегистрированы в ctx — runner закроет их в обратном порядке.
