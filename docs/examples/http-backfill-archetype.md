# Пример: HTTP-backfill архетип

**Задача.** По тикету SAMPLE-001 после фикса бага в auth-сервисе нужно
прогнать всех клиентов, у которых `last_login < now() - 7 days`, и дёрнуть
для них `POST /v1/users/{id}/refresh-token`. Каждый успешный refresh
отмечаем в CSV; провалы — в `errors.csv` через авто-аудит.

Внешний сервис нестабилен (5xx бывает), поэтому **retry навешивается на
сам HTTP-клиент через Kora `@Retry`** — это правильное место (один remote
call ретраится с backoff, не вся миграция).

## Setup

Для HOCON `migration` и `db.*`, `@KoraApp` — см.
[resend-archetype.md §2–3, §8](resend-archetype.md). Нужен `http-client-jdk` модуль Kora.

## Конфиг + типизированный HTTP-клиент с `@Retry`

```hocon
httpClient.auth {
  url = ${AUTH_URL}
  requestTimeout = 3s
}

sample {
  parallel = 8
}
```

```kotlin
@HttpClient(configPath = "httpClient.auth")
interface AuthClient {

    @HttpRoute(method = "POST", path = "/v1/users/{id}/refresh-token")
    @Retry("httpClient.auth.refresh")        // имя конфига; параметры в HOCON
    fun refreshToken(@Path("id") id: Long)
}

@ConfigSource("sample")
data class SampleConfig(
    var parallel: Int,
)
```

```hocon
resilient.retry.httpClient.auth.refresh {
  delay     = "250ms"
  attempts  = 4                # 4 retry после оригинала → 5 попыток всего
  delayStep = "250ms"          # linear backoff: 250ms, 500ms, 750ms, 1000ms
}
```

`@Retry` — Kora-аннотация. Она:
- классифицирует исключения (по дефолту — все Throwable, можно сузить через имплементацию `RetryPredicate` и `failurePredicateName = "..."` в HOCON);
- применяет linear backoff (`delay + (n-1)*delayStep`; настоящего exponential в Kora resilient встроенно нет);
- ретраит **только** `refreshToken(...)` вызов, не всё тело `forEach`.

Это критично: DSL-уровневый item-retry (которого в либе нет — см. §6 USER_GUIDE)
повторил бы всё тело `forEach`-блока, включая последующий `refreshed.row(userId)` и т.д.

## Скрипт

```kotlin
package com.example.migrations

import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.csv.openCsv
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
            forEach(stale, parallel = config.parallel, onError = OnError.Skip) { userId ->
                mutation("auth.refresh", args = mapOf("userId" to userId)) {
                    auth.refreshToken(userId)   // ретраи — внутри Kora-клиента
                }
                refreshed.row(userId)
            }
        }
    }
}
```

## Запуск

```bash
# Dry-run: посчитать сколько HTTP-вызовов улетело бы. Реальные refresh — нет.
MIGRATION_RUN=SAMPLE-001 MIGRATION_DRY_RUN=true \
  AUTH_URL=https://auth.internal \
  DB_USER=app DB_PASSWORD=... ./gradlew run
```

В отчёте при dry-run будет:
```
  ⌀ Dry-run skipped writes: (mutation:auth.refresh: 8421)
```

Что показывает: 8421 пользователь с устаревшим логином, для них вызвался бы
refresh. Можно прикинуть масштаб и нагрузку на auth-сервис до боевого запуска.

Боевой:
```bash
MIGRATION_RUN=SAMPLE-001 \
  AUTH_URL=https://auth.internal \
  DB_USER=app DB_PASSWORD=... ./gradlew run
```

Артефакты — `logs/SAMPLE-001/`:
```
logs/SAMPLE-001/
├── migration.log     # включая логи HTTP-клиента Kora — видно retry'и, timeouts
├── errors.csv        # user_id'ы, которые не отрефрешились даже после 5 retry внутри @Retry
├── errors.log        # стектрейсы (тут IO/timeout/5xx)
└── refreshed.csv     # успешно отрефрешенные
```

## Что архетип демонстрирует

| Фича | Где |
|---|---|
| Типизированный Kora `@HttpClient` | `AuthClient` — генерируется KSP, без ручной реализации |
| Kora `@Retry` на методе клиента | Backoff и classify внутри одного remote-call, не на уровне DSL |
| `mutation("label", args = ...) { ... }` | Dry-run gate для типизированного клиента: вызов оборачивается в `mutation`, чтобы DSL мог его перехватить. `label` — константа, контекст item'а — в `args` |
| `OnError.Skip` | После исчерпания retry внутри `@Retry` исключение всё-таки вылетает в `forEach` — DSL аудитит item в `errors.csv` и продолжает |
| `jdbc(db).stream(...) { stale -> ... }` | True JDBC cursor, callback-scoped `Sequence` — стримим миллион user-id'ов из БД |
| `parallel = 8` | 8 одновременных HTTP-вызовов (внешний сервис должен выдержать; иначе уменьшить) |

## Вариация: `http()` функциональный wrapper

Если типизированного клиента нет (например, legacy-сервис без OpenAPI),
можно использовать функциональный wrapper:

```kotlin
class Task42004B(
    private val httpCall: io.github.dsudomoin.migration.kora.ops.HttpCall,
    /* ... */
) : Migration(...) {
    override fun MigrationContext.migrate() {
        // ...
        jdbc(db).stream("select id from users where ...", mapper = { it.getLong("id") }) { stale ->
            forEach(stale, parallel = 8, onError = OnError.Skip) { userId ->
                http(httpCall).post("/v1/users/$userId/refresh-token")
                refreshed.row(userId)
            }
        }
    }
}
```

`http(call).post/patch/put/delete` — все идут через `guardWrite`, dry-run gate
работает «из коробки», `mutation { }` не нужен. Минус: нет type-safe ответов
и нет встроенного retry — backoff надо городить руками или ставить на отдельный
http-client-level wrapper.

## Вариация: классификация HTTP-ошибок

`@Retry` на клиенте ретраит «всё, что бросилось» (или подмножество по `predicate`).
Дальше — на уровне `forEach` — можно решить, что делать с **финальным** провалом
(после исчерпания retry внутри клиента):

```kotlin
onError = OnError.handle { e, _ ->
    when {
        e is AuthClient.NotFound    -> OnError.Decision.Skip   // 404 — user'а уже нет, пропускаем тихо
        e is AuthClient.BadRequest  -> OnError.Decision.Fail   // 400 — баг скрипта, останавливаем миграцию
        else                         -> OnError.Decision.Skip   // 5xx и timeout — аудит в errors.csv
    }
}
```

`Handle` принимает решение **один раз** (без backoff). Backoff и повторы — уровень `@Retry`
внутри клиента, не `Handle`.
