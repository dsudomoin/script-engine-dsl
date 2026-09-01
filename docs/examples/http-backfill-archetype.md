# Пример: HTTP-backfill архетип

**Задача.** По тикету SAMPLE-001 после фикса бага в auth-сервисе нужно
прогнать всех клиентов, у которых `last_login < now() - 7 days`, и дёрнуть
для них `POST /v1/users/{id}/refresh-token`. Каждый успешный refresh
отмечаем в CSV; провалы — в `errors.csv` через авто-аудит.

Внешний сервис нестабилен (5xx бывает), поэтому **retry навешивается на
сам HTTP-клиент через Kora `@Retry`** — это правильное место (один remote
call ретраится с backoff, не вся миграция).

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
    parallel       = 1                # только дефолт аргумента forEach(parallel = ...)
    errorThreshold = 200              # >200 неотрефрешенных — прервать прогон с exit 1 (0 = выключено)
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
}
```

`@Retry` — Kora-аннотация из модуля `resilient-kora`
(`ru.tinkoff.kora.resilient.retry.annotation.Retry`). Она:
- классифицирует исключения (по дефолту — все Throwable, можно сузить своей имплементацией
  `RetryPredicate` и `failurePredicateName = "..."` в HOCON);
- применяет linear backoff (`delay + (n-1)*delayStep`; настоящего exponential в Kora resilient
  встроенно нет);
- ретраит **только** `refreshToken(...)` вызов, не всё тело `forEach`.

Это критично: DSL-уровневый item-retry (которого в либе нет — см. §6 USER_GUIDE)
повторил бы всё тело `forEach`-блока, включая последующий `refreshed.row(userId)` и т.д.

Ответ `4xx`/`5xx`, доживший до конца retry-серии, вылетает из метода как
`ru.tinkoff.kora.http.client.common.HttpClientResponseException` (в нём `code`, тело и
заголовки) — то есть доходит до `onError` в `forEach` как обычное исключение.

## Скрипт

```kotlin
package com.example.migrations

import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.csv.openCsv
import io.github.dsudomoin.migration.error.includeItem
import io.github.dsudomoin.migration.forEach
import io.github.dsudomoin.migration.kora.ops.jdbc
import io.github.dsudomoin.migration.mutation
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory

@Component
class SampleMigration(
    private val db: JdbcConnectionFactory,
    private val auth: AuthClient,
    private val config: SampleConfig,
) : Migration(name = "SAMPLE-001", author = "sec") {

    override fun MigrationContext.migrate() {
        errors.includeItem<Long> { "user_id=$it" }

        val refreshed = openCsv("refreshed.csv", "user_id")

        jdbc(db).stream(
            "select id from users where last_login < now() - interval '7 days' order by id",
            fetchSize = 5000,
            mapper = { it.getLong("id") },
        ) { stale ->
            forEach(stale, parallel = config.parallel(), onError = OnError.Skip) { userId ->
                mutation("auth.refresh", args = mapOf("userId" to userId)) {
                    auth.refreshToken(userId)   // ретраи — внутри Kora-клиента
                }
                refreshed.row(userId)
            }
        }
    }
}
```

`parallel = 8` — это восемь одновременных HTTP-вызовов на самом деле: пул runner'а cached
и выдаёт столько воркеров, сколько попросил конкретный `forEach`. `migration.defaults.parallel`
на это не влияет — он лишь подставляется, когда аргумент не указан явно.

Под dry-run `mutation { }` не выполняется, а строка в `refreshed.csv` всё равно пишется:
CSV-выходы под репетицией остаются диагностическим артефактом (сколько строк — столько
вызовов ушло бы). Если хочется отличать репетицию от боя в самом файле, добавьте колонку
и пишите в неё `dryRun`.

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
  ⌀ Dry-run skipped writes:    (mutation:auth.refresh: 8421)
```

Что показывает: 8421 пользователь с устаревшим логином, для них вызвался бы
refresh. Можно прикинуть масштаб и нагрузку на auth-сервис до боевого запуска.
Пустой breakdown при непустом `Processed` означал бы забытый `mutation { }` — runner
про это отдельно предупреждает и в логе, и в отчёте.

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
| `mutation("label", args = ...) { ... }` | Dry-run gate для типизированного клиента: вызов оборачивается в `mutation`, чтобы DSL мог его перехватить. `label` — константа, контекст item'а — в `args` |
| `OnError.Skip` | После исчерпания retry внутри `@Retry` исключение всё-таки вылетает в `forEach` — DSL аудитит item в `errors.csv` и продолжает |
| `jdbc(db).stream(...) { stale -> ... }` | True JDBC cursor, callback-scoped `Sequence` — стримим миллион user-id'ов из БД. `stream`/`query` принимают только читающие запросы |
| `parallel = 8` | 8 одновременных HTTP-вызовов на самом деле (внешний сервис должен выдержать; иначе уменьшить) |

## Вариация: `http()` функциональный wrapper

Если типизированного клиента нет (например, legacy-сервис без OpenAPI),
можно использовать функциональный wrapper. `HttpCall` — это лямбда
`(method, path, body, headers) -> Int`, возвращающая статус-код:

```kotlin
package com.example.migrations

import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.csv.openCsv
import io.github.dsudomoin.migration.forEach
import io.github.dsudomoin.migration.kora.ops.HttpCall
import io.github.dsudomoin.migration.kora.ops.http
import io.github.dsudomoin.migration.kora.ops.jdbc
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import java.net.URI
import java.net.http.HttpRequest
import java.net.http.HttpResponse

@Component
class SampleMigrationRaw(
    private val db: JdbcConnectionFactory,
    private val config: RawAuthConfig,          // @ConfigSource с baseUrl
) : Migration(name = "SAMPLE-002", author = "sec") {

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

    override fun MigrationContext.migrate() {
        val refreshed = openCsv("refreshed.csv", "user_id")

        jdbc(db).stream(
            "select id from users where last_login < now() - interval '7 days' order by id",
            mapper = { it.getLong("id") },
        ) { stale ->
            forEach(stale, parallel = 8, onError = OnError.Skip) { userId ->
                http(call).post("/v1/users/$userId/refresh-token")
                refreshed.row(userId)
            }
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
работает «из коробки», `mutation { }` не нужен. Дальше — про статусы:

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
Дальше — на уровне `forEach` — можно решить, что делать с **финальным** провалом
(после исчерпания retry внутри клиента). Классифицировать нужно по реальным типам:
`HttpClientResponseException` от Kora-клиента и `HttpStatusException` от `http(call)`.

```kotlin
import io.github.dsudomoin.migration.kora.ops.HttpStatusException
import ru.tinkoff.kora.http.client.common.HttpClientResponseException

onError = OnError.handle { e, _ ->
    val status = when (e) {
        is HttpClientResponseException -> e.code      // типизированный Kora-клиент
        is HttpStatusException         -> e.status    // http(call).post/put/...
        else                           -> null        // IO, timeout, что угодно ещё
    }
    when (status) {
        404  -> OnError.Decision.Skip   // user'а уже нет — пропускаем тихо
        400  -> OnError.Decision.Fail   // баг скрипта, останавливаем миграцию
        else -> OnError.Decision.Skip   // 5xx, timeout, connection reset — аудит в errors.csv
    }
}
```

`Handle` принимает решение **один раз** (без backoff). Backoff и повторы — уровень `@Retry`
внутри клиента, не `Handle`. Item, по которому принято `Skip`, автоматически уезжает в
`errors.csv` и считается в `migration.defaults.errorThreshold` (у нас 200) — так «тихо
пропустили 90% пользователей» превращается в exit-code 1, а не в зелёный прогон.
