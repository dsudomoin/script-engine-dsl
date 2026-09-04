# Пример: HTTP-backfill архетип

> Пошаговое описание узлов — в [гайде](../USER_GUIDE.md).

**Задача.** По тикету SAMPLE-001 после фикса бага в auth-сервисе нужно
прогнать всех клиентов, у которых `last_login < now() - 7 days`, и дёрнуть
для них `POST /v1/users/{id}/refresh-token`. Каждый успешный refresh
отмечаем в CSV; провалы — в `errors.csv` через авто-аудит.

Внешний сервис нестабилен (5xx бывает), поэтому **retry навешивается на
сам HTTP-клиент через Kora `@Retry`** — это правильное место (один remote
call ретраится с backoff, не весь обработчик).

**Код ответа — это ошибка, а не значение.** И типизированный Kora-клиент, и
функциональный `http(call)`-wrapper поднимают исключение на любом ответе вне
`2xx`: Kora бросает `HttpClientResponseException`, DSL — `HttpStatusException`.
Без этого мёртвый бэкенд, отвечающий 500 на каждый запрос, дал бы отчёт
«100 000 successful» при нулевом эффекте бэкфилла.

## Setup

Сборка (KSP, координаты, `@KoraApp`) — см. [resend-archetype.md §2, §8](resend-archetype.md).
Здесь нужны: `ru.tinkoff.kora:http-client-jdk` + `JdkHttpClientModule`,
`ru.tinkoff.kora:resilient-kora` + `ResilientModule` (из-за `@Retry`),
`ru.tinkoff.kora:database-jdbc` + `JdbcDatabaseModule`, и `MigrationModule`.

```hocon
migration {
  run    = ${?MIGRATION_RUN}
  dryRun = ${?MIGRATION_DRY_RUN}      # без этой строки MIGRATION_DRY_RUN=true не включит репетицию

  defaults {
    progressEvery  = 1000
    parallel       = 1                # только дефолт аргумента source(parallel = ...)
    errorThreshold = 200              # >200 неотрефрешенных — прервать стадию с exit 1 (0 = выключено)
  }
}

db {
  jdbcUrl  = ${DB_URL}
  username = ${DB_USER}
  password = ${DB_PASSWORD}
  poolName = "migration-users"        # обязательный ключ Kora JdbcDatabaseConfig
}

httpClient.auth {
  url = ${AUTH_URL}
  requestTimeout = 3s
}

resilient.retry.authRefresh {
  delay     = "250ms"
  attempts  = 4                # 4 retry после оригинала → 5 попыток всего
  delayStep = "250ms"          # linear backoff: 250ms, 500ms, 750ms, 1000ms
}

sample {
  parallel = 8
  pageSize = 5000
}
```

Имя из `@Retry("...")` — это ключ в map'е `resilient.retry`, поэтому оно односегментное:
`resilient.retry.authRefresh` даёт ровно ключ `authRefresh`. Точки внутри имени HOCON
развернёт во вложенные объекты, и конфиг по имени уже не найдётся.

## Типизированный HTTP-клиент с `@Retry`

```kotlin
package com.example.migrations

import ru.tinkoff.kora.config.common.annotation.ConfigSource
import ru.tinkoff.kora.http.client.common.annotation.HttpClient
import ru.tinkoff.kora.http.common.annotation.HttpRoute
import ru.tinkoff.kora.http.common.annotation.Path
import ru.tinkoff.kora.resilient.retry.annotation.Retry

@HttpClient(configPath = "httpClient.auth")
interface AuthClient {

    @HttpRoute(method = "POST", path = "/v1/users/{id}/refresh-token")
    @Retry("authRefresh")                    // имя конфига; параметры в HOCON
    fun refreshToken(@Path("id") id: Long)
}

@ConfigSource("sample")
interface SampleConfig {
    fun parallel(): Int = 8
    fun pageSize(): Int = 5000
}
```

`@Retry` — Kora-аннотация из модуля `resilient-kora`
(`ru.tinkoff.kora.resilient.retry.annotation.Retry`). Она:
- классифицирует исключения (по дефолту — все Throwable, можно сузить своей имплементацией
  `RetryPredicate` и `failurePredicateName = "..."` в HOCON);
- применяет linear backoff (`delay + (n-1)*delayStep`; настоящего exponential в Kora resilient
  встроенно нет);
- ретраит **только** `refreshToken(...)` вызов, не весь обработчик item'а.

Это критично: item-level retry (которого в либе нет) повторил бы всё тело обработчика,
включая последующий `refreshed.row(userId)` и всё, что успело записаться до сбоя.

Ответ `4xx`/`5xx`, доживший до конца retry-серии, вылетает из метода как
`ru.tinkoff.kora.http.client.common.HttpClientResponseException` (в нём `code`, тело и
заголовки) — то есть доходит до `onItemError` как обычное исключение.

## Скрипт

```kotlin
package com.example.migrations

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.MigrationDefinition
import io.github.dsudomoin.migration.WriteOutcome
import io.github.dsudomoin.migration.error.includeItem
import io.github.dsudomoin.migration.kora.ops.jdbc
import io.github.dsudomoin.migration.migration
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory

@Component
class SampleMigration(
    private val db: JdbcConnectionFactory,
    private val auth: AuthClient,
    private val config: SampleConfig,
) : MigrationDefinition {

    override val name = "SAMPLE-001"

    override fun plan() = migration(name = name, author = "sec") {
        val refreshed = output("refreshed.csv", "user_id")

        source(
            parallel = config.parallel(),
            onItemError = ItemError.Skip,
            items = {
                errors.includeItem<Long> { "user_id=$it" }

                pages(
                    first = {
                        jdbc(db).query(
                            "select id from users where last_login < now() - interval '7 days' order by id limit :n",
                            "n" to config.pageSize(),
                        ) { it.getLong("id") }
                    },
                    next = { afterId ->
                        jdbc(db).query(
                            """
                            select id from users
                            where last_login < now() - interval '7 days' and id > :after
                            order by id limit :n
                            """.trimIndent(),
                            "after" to afterId, "n" to config.pageSize(),
                        ) { it.getLong("id") }
                    },
                    nextCursor = { page -> page.last() },
                    continueWhen = { page -> page.size >= config.pageSize() },
                )
            },
        ) { userId ->
            // Kora-клиент про dry-run ничего не знает — гейт даёт write { }.
            write("auth.refresh", args = mapOf("userId" to userId)) {
                auth.refreshToken(userId)      // ретраи — внутри Kora-клиента
                WriteOutcome.Applied
            }
            refreshed.row(userId)
        }
    }
}
```

`parallel = 8` — это восемь одновременных HTTP-вызовов на самом деле: пул runner'а cached
и выдаёт столько воркеров, сколько попросила стадия. `migration.defaults.parallel`
на это не влияет — он лишь подставляется, когда аргумент не указан явно.

Под dry-run тело `write { }` не выполняется вовсе (это не «выполнилось и не отправилось» —
лямбда просто не вызывается), а строка в `refreshed.csv` всё равно пишется: CSV-выходы под
репетицией остаются диагностическим артефактом — сколько строк, столько вызовов ушло бы.
Если хочется отличать репетицию от боя в самом файле, добавь колонку и пиши в неё `dryRun`.

Метка `write` **константная** (`"auth.refresh"`) — это критично: в отчёте получится чистый
агрегат `auth.refresh: 8 421`. Если бы метка содержала `$userId`, в breakdown было бы 8421
уникальных строк, бесполезных. Контекст item'а идёт в `args` — они уезжают в лог
(`[DRY-RUN] auth.refresh (userId=42)`), но не в ключ агрегата.

## Почему `pages`, а не `jdbc.stream`

`jdbc(db).stream(...) { rows -> ... }` отдаёт `Sequence`, валидную **только внутри**
`consume`-блока: на выходе `ResultSet` и `Connection` уже закрыты. А источник стадии
(`items = { }`) обязан вернуть последовательность, которую движок будет читать **после**
возврата из лямбды. Поэтому потоковый источник стадии — это `pages(...)` с курсором по `id`.
Побочная выгода — в отчёте появляются `rawPages`/`rawRows`, то есть «сколько прочитано»
отдельно от «сколько дошло до обработчика».

`stream` остаётся полезным внутри обработчика, где всё чтение и вся работа умещаются в один
вызов, — например, посчитать агрегат по подмножеству.

## Что делать с `WriteResult`

`write` возвращает исход, и его можно разобрать — например, чтобы отделить «сервис сказал
нет» от «сервис не ответил»:

```kotlin
// в билдере, рядом с refreshed:
// val gone = output("gone.csv", "user_id", "reason")

val result = write("auth.refresh", args = mapOf("userId" to userId)) {
    try {
        auth.refreshToken(userId)
        WriteOutcome.Applied
    } catch (e: HttpClientResponseException) {
        // 404 — пользователя больше нет. Это ожидаемый отрицательный исход, а не сбой:
        // в отчёте он попадёт в rejectedWrites, а не в errors.csv.
        if (e.code == 404) WriteOutcome.Rejected("user gone") else throw e
    }
}
when (result) {
    is WriteResult.Applied      -> refreshed.row(userId)
    is WriteResult.Rejected     -> gone.row(userId, result.reason)
    is WriteResult.DryRunSkipped -> refreshed.row(userId)   // репетиция: считаем, что ушло бы
}
```

Граница простая: **`Rejected` — это ожидаемый отрицательный ответ**, `throw` — сбой.
Первое считается в `rejectedWrites` и `errors.csv` не трогает; второе идёт через
`ItemError` и аудитируется.

## Запуск

```bash
# Dry-run: посчитать сколько HTTP-вызовов улетело бы. Реальные refresh — нет.
MIGRATION_RUN=SAMPLE-001 MIGRATION_DRY_RUN=true \
  AUTH_URL=https://auth.internal \
  DB_URL=jdbc:postgresql://pg.prod:5432/users \
  DB_USER=app DB_PASSWORD=... ./gradlew run
```

В отчёте при dry-run будет:
```
  ⌀ Source pages read:       2  (rows: 8 421)
  ⌀ Dry-run skipped writes:    (auth.refresh: 8421)
```

Что показывает: 8421 пользователь с устаревшим логином, для них вызвался бы
refresh. Можно прикинуть масштаб и нагрузку на auth-сервис до боевого запуска.
Пустой breakdown при непустом `Processed` означал бы, что ни одна запись не прошла
через гейт — то есть где-то забыт `write { }` и вызов ушёл в бой по-настоящему;
runner про это отдельно предупреждает и в логе, и в отчёте.

Боевой:
```bash
MIGRATION_RUN=SAMPLE-001 \
  AUTH_URL=https://auth.internal \
  DB_URL=jdbc:postgresql://pg.prod:5432/users \
  DB_USER=app DB_PASSWORD=... ./gradlew run
```

Артефакты — `logs/SAMPLE-001/`:
```
logs/SAMPLE-001/
├── migration.log     # включая логи HTTP-клиента Kora — видно retry'и, timeouts
├── errors.csv        # user_id'ы, которые не отрефрешились даже после retry внутри @Retry
├── errors.log        # стектрейсы (тут IO/timeout/HttpClientResponseException)
└── refreshed.csv     # успешно отрефрешенные
```

## Что архетип демонстрирует

| Фича | Где |
|---|---|
| Типизированный Kora `@HttpClient` | `AuthClient` — генерируется KSP, без ручной реализации |
| Kora `@Retry` на методе клиента | Backoff и classify внутри одного remote-call, не на уровне DSL |
| `write("label", args) { ...; WriteOutcome.Applied }` | Dry-run gate для типизированного клиента: единственный способ провести чужой компонент через репетицию. Метка — константа, контекст item'а — в `args` |
| `WriteOutcome.Rejected` / `WriteResult` | «Сервис ответил нет» отделено от «сервис упал» — разные счётчики в отчёте |
| `ItemError.Skip` | После исчерпания retry внутри `@Retry` исключение всё-таки вылетает — DSL аудитит item в `errors.csv` и продолжает |
| `pages(...)` | Курсорный источник вместо `jdbc.stream` (тот валиден только внутри `consume`) |
| `parallel = 8` | 8 одновременных HTTP-вызовов на самом деле (внешний сервис должен выдержать; иначе уменьшить) |

## Вариация: `http()` функциональный wrapper

Если типизированного клиента нет (например, legacy-сервис без OpenAPI),
можно использовать функциональный wrapper. `HttpCall` — это лямбда
`(method, path, body, headers) -> Int`, возвращающая статус-код:

```kotlin
package com.example.migrations

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.MigrationDefinition
import io.github.dsudomoin.migration.kora.ops.HttpCall
import io.github.dsudomoin.migration.kora.ops.http
import io.github.dsudomoin.migration.kora.ops.jdbc
import io.github.dsudomoin.migration.migration
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import java.net.URI
import java.net.http.HttpRequest
import java.net.http.HttpResponse

@Component
class SampleMigrationRaw(
    private val db: JdbcConnectionFactory,
    private val config: RawAuthConfig,          // @ConfigSource с baseUrl
) : MigrationDefinition {

    override val name = "SAMPLE-002"

    private val jdk = java.net.http.HttpClient.newHttpClient()

    // Адаптер: (method, path, body, headers) -> статус-код. Собирается в самом скрипте —
    // тащить function-тип через граф Kora не нужно.
    private val call: HttpCall = { method, path, body, headers ->
        val publisher = if (body == null) {
            HttpRequest.BodyPublishers.noBody()
        } else {
            HttpRequest.BodyPublishers.ofByteArray(body)
        }
        val request = HttpRequest.newBuilder(URI.create(config.baseUrl() + path))
            .method(method, publisher)
            .apply { headers.forEach { (k, v) -> header(k, v) } }
            .build()
        jdk.send(request, HttpResponse.BodyHandlers.discarding()).statusCode()
    }

    override fun plan() = migration(name = name, author = "sec") {
        val refreshed = output("refreshed.csv", "user_id")

        source(
            parallel = 8,
            onItemError = ItemError.Skip,
            items = {
                // FIRST_PAGE / NEXT_PAGE — те же два запроса, что и в основном скрипте.
                pages(
                    first = { jdbc(db).query(FIRST_PAGE, "n" to 5000) { it.getLong("id") } },
                    next = { afterId -> jdbc(db).query(NEXT_PAGE, "after" to afterId, "n" to 5000) { it.getLong("id") } },
                    nextCursor = { page -> page.last() },
                    continueWhen = { page -> page.size >= 5000 },
                )
            },
        ) { userId ->
            // write { } не нужен: http(...).post сам проходит guardWrite.
            http(call).post("/v1/users/$userId/refresh-token")
            refreshed.row(userId)
        }
    }
}
```

```kotlin
@ConfigSource("rawAuth")
interface RawAuthConfig {
    fun baseUrl(): String
}
```
```hocon
rawAuth.baseUrl = ${AUTH_URL}
```

`http(call).post/patch/put/delete` — все идут через `guardWrite`, dry-run gate
работает «из коробки», `write { }` не нужен. Дальше — про статусы:

- любой ответ вне `200..299` поднимает `HttpStatusException(method, path, status)`,
  поэтому строка `refreshed.row(userId)` до неудачного пользователя не доходит,
  а item уезжает в `errors.csv`;
- под dry-run write-методы возвращают `200`, а не `0`: репетиция обязана идти той же
  веткой кода, что и бой, а ноль отправил бы её в обработку ошибки;
- `get(...)` тоже проверяет код, но dry-run-гейт не проходит — чтение под репетицией
  выполняется по-настоящему.

Минус подхода: нет type-safe ответов и нет встроенного retry — backoff надо городить
руками внутри самого `HttpCall`-адаптера.

## Вариация: классификация HTTP-ошибок

`@Retry` на клиенте ретраит «всё, что бросилось» (или подмножество по `predicate`).
Дальше — на уровне стадии — можно решить, что делать с **финальным** провалом
(после исчерпания retry внутри клиента). Классифицировать нужно по реальным типам:
`HttpClientResponseException` от Kora-клиента и `HttpStatusException` от `http(call)`.

```kotlin
import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.kora.ops.HttpStatusException
import ru.tinkoff.kora.http.client.common.HttpClientResponseException

onItemError = ItemError.Handle { e, userId ->
    val status = when (e) {
        is HttpClientResponseException -> e.code      // типизированный Kora-клиент
        is HttpStatusException         -> e.status    // http(call).post/put/...
        else                           -> null       // IO, timeout, что угодно ещё
    }
    when (status) {
        404  -> ItemError.Decision.Skip   // user'а уже нет — пропускаем тихо
        400  -> ItemError.Decision.Fail   // баг скрипта, останавливаем стадию
        else -> ItemError.Decision.Skip   // 5xx, timeout, connection reset — аудит в errors.csv
    }
}
```

Отличие от старого `OnError.handle`: классификатор получает **типизированный item**
(здесь `userId: Long`, а не `Any?`), поэтому решение может зависеть не только от типа
исключения, но и от самих данных — без каста.

`Handle` принимает решение **один раз** (без backoff). Backoff и повторы — уровень `@Retry`
внутри клиента, не `Handle`. Item, по которому принято `Skip`, автоматически уезжает в
`errors.csv` и считается в `errorThreshold` (у нас 200) — так «тихо пропустили 90%
пользователей» превращается в exit-code 1, а не в зелёный прогон. Порог считается по
стадии и сбрасывается на её границе.

Если тот же 404 логичнее считать не ошибкой вовсе, а штатным отрицательным исходом —
верни `WriteOutcome.Rejected("user gone")` из тела `write` (см. «Что делать с `WriteResult`»)
и до `ItemError` дело просто не дойдёт.
