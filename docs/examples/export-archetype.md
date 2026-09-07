# Пример: Export-архетип (DB → CSV snapshot)

> Пошаговое описание узлов — в [гайде](../USER_GUIDE.md).

**Задача.** По тикету SAMPLE-001 финдиру нужен CSV-снимок всех клиентов
в статусе `VIP` с балансом и датой присоединения — чтобы скормить в Excel
для квартальной отчётности. Источник — Postgres, выход — один CSV.

Это «read-only export» архетип: никаких внешних сервисов, никаких
update'ов. Самый короткий из доступных планов — один `output`, одна стадия.

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

Для этого архетипа dry-run всё равно ничего не меняет — писать нечего, а `output`
пишется и под репетицией (это диагностический артефакт). Заметно будет одно: runner
допишет в отчёт предупреждение «processed N item(s) but intercepted 0 writes» —
для read-only выгрузки это ожидаемо.

## Конфиг скрипта (минимальный)

```hocon
sample {
  minBalance = 1000000   # фильтр — только клиенты с балансом >= порога
  pageSize   = 5000      # размер страницы курсора
  parallel   = 4
}
```

```kotlin
import ru.tinkoff.kora.config.common.annotation.ConfigSource

@ConfigSource("sample")
interface SampleConfig {
    fun minBalance(): Long
    fun pageSize(): Int = 5000
    fun parallel(): Int = 4
}
```

## Скрипт

```kotlin
package com.example.migrations

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.MigrationDefinition
import io.github.dsudomoin.migration.error.includeItem
import io.github.dsudomoin.migration.kora.ops.jdbc
import io.github.dsudomoin.migration.migration
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import java.sql.ResultSet
import java.time.Instant

@Component
class SampleMigration(
    private val db: JdbcConnectionFactory,
    private val config: SampleConfig,
) : MigrationDefinition {

    override val name = "SAMPLE-001"

    override fun plan() = migration(name = name, author = "finance") {
        // Выход объявляется в билдере, а не внутри стадии: файл открывается один раз на прогон,
        // заголовки задаются один раз, закрывает его движок.
        val out = output("vip-snapshot.csv", "id", "email", "balance", "joined_at")

        source(
            parallel = config.parallel(),
            onItemError = ItemError.Skip,
            items = {
                errors.includeItem<Vip> { "id=${it.id}" }

                // Курсорная пагинация по возрастающему id. Ленивая: следующая страница читается
                // только когда предыдущая дочитана обработчиком.
                pages(
                    first = {
                        jdbc(db).query(
                            """
                            select id, email, balance, joined_at from customers
                            where status = 'VIP' and balance >= :min
                            order by id limit :n
                            """.trimIndent(),
                            "min" to config.minBalance(), "n" to config.pageSize(),
                        ) { rs -> vip(rs) }
                    },
                    next = { afterId ->
                        jdbc(db).query(
                            """
                            select id, email, balance, joined_at from customers
                            where status = 'VIP' and balance >= :min and id > :after
                            order by id limit :n
                            """.trimIndent(),
                            "min" to config.minBalance(), "after" to afterId, "n" to config.pageSize(),
                        ) { rs -> vip(rs) }
                    },
                    nextCursor = { page -> page.last().id },
                    continueWhen = { page -> page.size >= config.pageSize() },
                )
            },
        ) { v ->
            out.row(v.id, v.email, v.balance, v.joinedAt)
        }
    }

    // mapper обязан быть тотальным: он работает при чтении страницы, а не в теле обработчика,
    // и его исключение НЕ попадает под onItemError (см. «Где ItemError работает, а где нет»).
    private fun vip(rs: ResultSet) = Vip(
        id = rs.getLong("id"),
        email = rs.getString("email") ?: "",
        balance = rs.getLong("balance"),
        joinedAt = rs.getTimestamp("joined_at")?.toInstant(),
    )

    private data class Vip(val id: Long, val email: String, val balance: Long, val joinedAt: Instant?)
}
```

`row(...)` принимает `Any?` и пишет `null` пустой ячейкой — nullable-поля можно
отдавать как есть, отдельная подстановка не нужна.

## Почему `pages`, а не `jdbc.stream`

`jdbc(db).stream(sql, mapper = ..., consume = ...)` никуда не делся, но источником стадии
он быть не может: `Sequence`, которую он отдаёт, валидна **только внутри** `consume` —
на выходе из блока `ResultSet`/`Connection` уже закрыты. А `items = { ... }` обязан вернуть
`Sequence`, которую движок будет читать **после** возврата из лямбды. Отдать оттуда
стрим — значит получить SQL-исключение про закрытый result set на первом же элементе.

Поэтому потоковый источник стадии — это `pages(...)`:

| | `pages(...)` | `jdbc.stream(...) { }` |
|---|---|---|
| Где живёт | `items = { }` (источник стадии) | тело обработчика — там, где вся работа умещается в один вызов |
| Механика | keyset-пагинация: N запросов по `limit` с курсором | один курсор БД (`setFetchSize`), одно соединение на всё чтение |
| Соединение | берётся и отдаётся на каждую страницу | держится открытым всё время чтения |
| Возобновление | тривиально: курсор — обычное значение (`id`) | нет |
| Учёт в отчёте | `rawPages` / `rawRows` — «прочитано» до пользовательских `filter` | нет |

`first` и `next` разделены не косметически: первая страница читается без предиката по
курсору, последующие — строго `id > :after`. Побочно это разрывает цикл вывода типов,
поэтому аннотация типа на параметре `next` не нужна.

`continueWhen` и `nextCursor` смотрят на **сырую** страницу — до любых `.filter`/`.map`,
которые ты навесишь на результат `pages`. Поэтому страница, целиком отсеянная фильтром,
источник не завершает; завершает только пустая сырая страница или `continueWhen == false`.
Если курсор после непустой страницы не сдвинулся, движок бросит `CursorNotAdvancing`,
а не уйдёт в бесконечный цикл — сделай курсор составным (`data class Cut(val cut: Instant, val id: Long)`),
если по одному полю он законно повторяется.

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
| `MigrationDefinition` + `plan()` | `name` — константа, план строится один раз и только у выбранной миграции |
| `output(...)` в билдере | Выход прогона: один файл, одни заголовки, авто-закрытие движком |
| `pages(first, next, nextCursor, continueWhen)` | Ленивый курсорный источник — для выгрузок, не помещающихся в RAM |
| `source(parallel = ...)` | Реальные воркеры: пул runner'а cached и выдаёт столько потоков, сколько попросила стадия |
| `ItemError.Skip` | Ошибка **в теле обработчика** — item уезжает в `errors.csv`, выгрузка продолжается |
| `errors.includeItem<Vip>` | В аудит уедет `id=42`, а не `toString()` со всей почтой |
| Минимум зависимостей | Только `JdbcConnectionFactory` — никаких Kafka/HTTP/Cassandra |

Про `parallel` в этом конкретном архетипе: тело обработчика — одна строка в CSV под общим
`synchronized`-локом, а страницы читаются последовательно, в потоке стадии. Выигрыш от
`parallel > 1` здесь околонулевой; поднимать его стоит, когда в теле появляется round-trip
(HTTP-обогащение, вторая БД). Ставить `parallel` больше `db.maxPoolSize` смысла нет
в любом случае: страницу читает то же соединение, за которым стоят воркеры.

Чтение страниц не убегает вперёд: при `parallel > 1` движок берёт permit семафора **до**
`next()`, поэтому источник вычитывается ровно настолько, насколько его успевают потреблять.

## Где `ItemError` работает, а где нет

`onItemError` накрывает **только тело обработчика**. Всё, что бросается при построении и
чтении источника — `first`/`next` в `pages`, mapper `ResultSet → Vip`, любой `.map { }` на
последовательности — это ошибка источника: стадия останавливается и исключение уходит наверх,
независимо от `ItemError.Skip`. Поэтому mapper выше написан тотальным (`?.` на nullable-колонках),
а не «пусть упадёт, Skip спасёт».

У CSV-источника ровно для этого случая есть отдельная политика — параметр
`onRowError` у `readCsv`, и его нужно **передать явно**: по умолчанию там
`ItemError.Fail`, то есть первая же битая строка валит прогон.

```kotlin
import io.github.dsudomoin.migration.csv.readCsv

source(
    parallel = 4,
    onItemError = ItemError.Skip,
    items = {
        // Битая строка (рваные кавычки, лишняя колонка, не-число в id) уедет в errors.csv,
        // инкрементит report.sourceSkipped и до обработчика не доедет.
        readCsv("input/customers.csv", onRowError = ItemError.Skip) { it.getValue("id").toLong() }
    },
) { id ->
    // тело обработчика — здесь уже действует onItemError
}
```

Счётчики у этих двух случаев **разные**, и это важно:

| | Что инкрементит | Строка в отчёте | Считается в `errorThreshold` |
|---|---|---|---|
| Битая строка `readCsv(onRowError = Skip)` | `report.sourceSkipped` | `Source rows dropped` | нет — до стадии она не дошла |
| Ошибка обработчика под `ItemError.Skip` | `report.skipped` | `Skipped (errors)` | да |

В `errors.csv` уезжают оба, но в тождество `processed = successful + skipped + failed`
входит только второй.

## Вариация: разбивка на несколько CSV по сегменту

Если финдиру удобнее разные категории VIP'ов в отдельных файлах:

```kotlin
override fun plan() = migration(name = name, author = "finance") {
    val gold     = output("vip-gold.csv",     "id", "email", "balance")
    val platinum = output("vip-platinum.csv", "id", "email", "balance")
    val diamond  = output("vip-diamond.csv",  "id", "email", "balance")

    source(
        parallel = 4,
        onItemError = ItemError.Skip,
        items = {
            // FIRST_PAGE / NEXT_PAGE — те же два запроса, что и выше, вынесенные в константы класса.
            pages(
                first  = { jdbc(db).query(FIRST_PAGE, "n" to config.pageSize()) { vip(it) } },
                next   = { afterId -> jdbc(db).query(NEXT_PAGE, "after" to afterId, "n" to config.pageSize()) { vip(it) } },
                nextCursor   = { page -> page.last().id },
                continueWhen = { page -> page.size >= config.pageSize() },
            )
        },
    ) { v ->
        when {
            v.balance >= 100_000_000L -> diamond.row(v.id, v.email, v.balance)
            v.balance >=  10_000_000L -> platinum.row(v.id, v.email, v.balance)
            else                      -> gold.row(v.id, v.email, v.balance)
        }
    }
}
```

Три CSV — три строки в билдере. Все три живут весь прогон, `row` thread-safe, файлы
закрывает движок. Именно поэтому выходы объявляются в билдере: открывай их внутри стадии —
и в `scoped`-стадии каждый новый родитель затирал бы строки предыдущего.

## Вариация: вторая стадия для сверки

Стадий может быть несколько, они идут строго последовательно, и провал одной не запускает
следующие. Удобно, когда после выгрузки хочется отдельным проходом посчитать контрольную
сумму — если стадий больше одной, **имя обязательно у каждой**:

```kotlin
override fun plan() = migration(name = name, author = "finance") {
    val out   = output("vip-snapshot.csv", "id", "email", "balance", "joined_at")
    val totals = output("totals.csv", "metric", "value")

    val expected = input("expected-count") {
        val n = jdbc(db).query("select count(*) c from customers where status = 'VIP'") { it.getLong("c") }.first()
        check(n > 0) { "в БД нет ни одного VIP — проверь окружение, а не выгружай пустоту" }
        n
    }

    validate {
        require(config.pageSize() > 0) { "sample.pageSize должен быть > 0" }
        require(config.minBalance() >= 0) { "sample.minBalance не может быть отрицательным" }
    }

    source(name = "export", items = { /* pages(...) как выше */ }) { v -> out.row(/* ... */) }

    source(expected, name = "verify", items = { sequenceOf(it) }) { expectedCount ->
        totals.row("expected", expectedCount)
    }
}
```

`input(name) { }` — ленивое значение прогона: считается при первом `resolve` и кэшируется
на весь прогон, сколько бы стадий его ни спросили. `resolve` живёт на `SourceScope`, то есть
внутри `items = { }`; в `validate { }` его нет — там `InputScope`, и это осознанно: проверка
конфига не должна ходить в базу и выполнять эффекты.

`validate { }` выполняется один раз, до первой стадии и до любого эффекта, и объявляется
не более одного раза. Это единственное место, где ошибка конфига видна раньше, чем начнутся
записи: сортировку input'ов задаёт первый `resolve`, а не порядок объявления, поэтому
«валидация в первом input» на эту роль не годится.
