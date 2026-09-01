# Пример: Export-архетип (DB → CSV snapshot)

**Задача.** По тикету SAMPLE-001 финдиру нужен CSV-снимок всех клиентов
в статусе `VIP` с балансом и датой присоединения — чтобы скормить в Excel
для квартальной отчётности. Источник — Postgres, выход — один CSV.

Это «read-only export» архетип: никаких внешних сервисов, никаких
update'ов. Самый короткий из доступных скриптов.

## Setup

Сборка (KSP), `db.*` и `@KoraApp` — см.
[resend-archetype.md §2–3, §8](resend-archetype.md). Секция самого runner'а:

```hocon
migration {
  run    = ${?MIGRATION_RUN}
  dryRun = ${?MIGRATION_DRY_RUN}
}
```

Обе строки обязательны: библиотека не читает окружение сама, переменные приезжают
только через `${?VAR}`. Без строки `dryRun` команда с `MIGRATION_DRY_RUN=true`
отработает как боевая.

Для этого архетипа dry-run всё равно ничего не меняет — писать нечего, а `openCsv`
пишет и под репетицией (это диагностический артефакт). Заметно будет одно: runner
допишет в отчёт предупреждение «processed N item(s) but intercepted 0 writes» —
для read-only выгрузки это ожидаемо.

## Конфиг скрипта (минимальный)

```hocon
sample {
  minBalance = 1000000   # фильтр — только клиенты с балансом >= порога
  parallel   = 4
}
```

```kotlin
import ru.tinkoff.kora.config.common.annotation.ConfigSource

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
import java.time.Instant

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
            // mapper обязан быть тотальным: он работает при итерации курсора, а не в теле
            // цикла, и его исключение НЕ попадает под onError (см. «Нечитаемая строка» ниже).
            mapper = { rs ->
                Vip(
                    id = rs.getLong("id"),
                    email = rs.getString("email") ?: "",
                    balance = rs.getLong("balance"),
                    joinedAt = rs.getTimestamp("joined_at")?.toInstant(),
                )
            },
        ) { vips ->
            forEach(vips, parallel = config.parallel, onError = OnError.Skip) { v ->
                out.row(v.id, v.email, v.balance, v.joinedAt)
            }
        }
    }

    private data class Vip(val id: Long, val email: String, val balance: Long, val joinedAt: Instant?)
}
```

`row(...)` принимает `Any?` и пишет `null` пустой ячейкой — nullable-поля можно
отдавать как есть, отдельная подстановка не нужна.

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
| `OnError.Skip` | Ошибка **в теле цикла** (запись строки в CSV) — item уезжает в `errors.csv`, выгрузка продолжается. Ошибка mapper'а сюда не попадает — см. ниже |
| `parallel = config.parallel` | Пул runner'а cached: сколько воркеров попросил `forEach`, столько реально и работает. `migration.defaults.parallel` — только дефолт для вызовов без явного аргумента |
| Минимум зависимостей | Только `JdbcConnectionFactory` — никаких Kafka/HTTP/Cassandra |

Про `parallel` в этом конкретном архетипе: тело цикла — одна строка в CSV под общим
`synchronized`-локом, а курсор БД читается последовательно. Выигрыш от `parallel > 1`
здесь околонулевой; поднимать его стоит, когда в теле появляется round-trip
(HTTP-обогащение, вторая БД). Ставить `parallel` больше `db.maxPoolSize` смысла нет
в любом случае.

## Нечитаемая строка: где `OnError` работает, а где нет

`onError` у `forEach` накрывает **только тело цикла**. `mapper` у `jdbc.stream`
вызывается при итерации курсора — то есть до того, как item попал в цикл, — и его
исключение обрабатывается как ошибка источника: `forEach` останавливается и
пробрасывает его наверх, независимо от `OnError.Skip`. Поэтому mapper выше написан
тотальным (`?.` на nullable-колонках), а не «пусть упадёт, Skip спасёт».

У CSV-источника ровно для этого случая есть отдельная политика — параметр
`onRowError` у `readCsv`, и его нужно **передать явно**: по умолчанию там
`OnError.Fail`, то есть первая же битая строка валит прогон.

```kotlin
import io.github.dsudomoin.migration.csv.readCsv

// Битая строка (рваные кавычки, лишняя колонка, не-число в id) уедет в errors.csv,
// инкрементит report.skipped и до forEach не доедет.
val ids = readCsv("input/customers.csv", onRowError = OnError.Skip) { it["id"]!!.toLong() }

forEach(ids, parallel = 4, onError = OnError.Skip) { id ->
    // тело цикла — здесь уже действует onError
}
```

`report.skipped` — общий счётчик и для строк, отброшенных `readCsv`, и для item'ов,
отброшенных циклом; оба считаются в `migration.defaults.errorThreshold`.

## Вариация: разбивка на несколько CSV по сегменту

Если финдиру удобнее разные категории VIP'ов в отдельных файлах:

```kotlin
override fun MigrationContext.migrate() {
    val gold = openCsv("vip-gold.csv",     "id", "email", "balance")
    val platinum = openCsv("vip-platinum.csv", "id", "email", "balance")
    val diamond = openCsv("vip-diamond.csv",  "id", "email", "balance")

    jdbc(db).stream(
        "select id, email, balance, joined_at from customers where status = 'VIP' order by id",
        fetchSize = 5000,
        mapper = { rs ->
            Vip(
                id = rs.getLong("id"),
                email = rs.getString("email") ?: "",
                balance = rs.getLong("balance"),
                joinedAt = rs.getTimestamp("joined_at")?.toInstant(),
            )
        },
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
