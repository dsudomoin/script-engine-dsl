# Кастомизация Migration DSL

DSL — это набор extension-функций на `MigrationContext` плюс несколько hook'ов
(`register`, `shared`, `guardWrite`, `@Tag(MigrationExecutor)`, `MigrationExit`).
Если из коробки чего-то не хватает — добавляй своё, не дожидаясь патча либы.

Ниже — четыре типичных сценария кастомизации:

1. [Свой ops-wrapper с dry-run gate](#1-свой-ops-wrapper-с-dry-run-gate)
2. [Свой AutoCloseable-ресурс в lifecycle ctx](#2-свой-autocloseable-ресурс-в-lifecycle-ctx)
3. [Кастомный Executor для `forEach`](#3-кастомный-executor-для-foreach)
4. [Кастомный `Progress` + per-type сериализатор ошибок](#4-кастомный-progress--per-type-сериализатор-ошибок)

---

## 1. Свой ops-wrapper с dry-run gate

S3 в либе **нет и не планируется** — ниже он взят как образец стороннего клиента, вокруг
которого пользователь пишет свой ops-хендл. Паттерн одинаков для любого SDK: Redis, S3,
Elasticsearch, внутренний RPC.

Допустим, в проекте используется S3-клиент, и хочется чтобы `s3(client).put(...)` под dry-run
не пытался писать. Напиши обёртку точно так же, как сделаны `jdbc`, `cassandra`, `kafka`:

```kotlin
// com/example/migrations/ops/S3Ops.kt
package com.example.migrations.ops

import io.github.dsudomoin.migration.MigrationContext
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.core.sync.RequestBody

class S3Ops internal constructor(
    private val ctx: MigrationContext,
    private val client: S3Client,
    private val bucket: String,
) {

    fun put(key: String, body: ByteArray, contentType: String = "application/octet-stream"): String? =
        ctx.guardWrite(
            label = "s3.put",
            args = mapOf("bucket" to bucket, "key" to key, "size" to body.size),
            dryRunDefault = null,
        ) {
            client.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).contentType(contentType).build(),
                RequestBody.fromBytes(body),
            )
            "s3://$bucket/$key"
        }

    fun delete(key: String) {
        ctx.guardWrite("s3.delete", mapOf("bucket" to bucket, "key" to key)) {
            client.deleteObject { it.bucket(bucket).key(key) }
        }
    }

    // Read-операции — без guardWrite (под dry-run выполняются нормально)
    fun listKeys(prefix: String): List<String> =
        client.listObjectsV2 { it.bucket(bucket).prefix(prefix) }
            .contents().map { it.key() }
}

fun MigrationContext.s3(client: S3Client, bucket: String): S3Ops = S3Ops(this, client, bucket)
```

Используется обычно:

```kotlin
@Component
class SampleMigration(
    private val s3client: S3Client,
    private val db: JdbcConnectionFactory,
) : Migration("SAMPLE-001", "team") {

    override fun MigrationContext.migrate() {
        val orders = jdbc(db).query("select id, payload from orders where status = 'EXPORT'") {
            it.getLong("id") to it.getBytes("payload")
        }

        forEach(orders, parallel = 4, onError = OnError.Skip) { (id, body) ->
            s3(s3client, "orders-archive").put("orders/$id.json", body)
        }
    }
}
```

Под `MIGRATION_DRY_RUN=true` ни одного реального `PutObject` не уйдёт; в отчёте появится
`s3.put: 8421` и в логе `INFO [DRY-RUN] s3.put (bucket=orders-archive, key=orders/42.json, size=2048)`.

**Что важно:** `guardWrite` — единственный канонический способ интегрироваться с dry-run.
Сигнатур две: `guardWrite(label, args, dryRunDefault) { ... }` возвращает значение (под
репетицией — `dryRunDefault`), `guardWrite(label, args) { ... }` — для void-записи. Значение
`dryRunDefault` выбирай так, чтобы вызывающий код под репетицией шёл той же веткой, что и в
бою: для HTTP-статуса это `200`, а не `0`; для «числа обновлённых строк» — `0`; для хендла —
`null` только если вызывающий это переживает.

Read-операции (`listKeys`, `getObject`) НЕ оборачивай в `guardWrite` — они должны работать
под dry-run.

### Хендл, который зовут внутри `forEach`

`s3(client, bucket)` в примере выше создаёт новый `S3Ops` на каждый вызов. Пока он живёт
в переменной над циклом — это ничего не стоит. Но если хендл дёргается прямо в теле
`forEach` на миллионе item'ов и при этом что-то держит (соединение, буфер, счётчики),
объекты нужно мемоизировать — для этого есть `MigrationContext.shared(key, factory)`:
ресурс создаётся один раз на прогон для данного ключа и сразу регистрируется в реестре
(runner закроет его в `finally`). Ровно так внутри устроены `kafka(producer)` и
`topic(producer, name)`.

```kotlin
class S3Ops internal constructor(
    private val ctx: MigrationContext,
    private val client: S3Client,
    private val bucket: String,
) : AutoCloseable {                       // shared требует AutoCloseable

    // ... put/delete/listKeys как выше

    override fun close() {
        // здесь финализируют multipart-upload'ы, флашат буферы и т.д.
    }
}

/** Ключ мемоизации: идентичность клиента плюс имя бакета. */
private data class S3Key(val client: S3Client, val bucket: String)

fun MigrationContext.s3(client: S3Client, bucket: String): S3Ops =
    shared(S3Key(client, bucket)) { S3Ops(this, client, bucket) }
```

Ограничение одно: `factory` не должна сама звать `shared` — вложенный вызов на той же
мапе заблокируется.

**Retry для S3 transient-ошибок** — навешивай на уровень `S3Client` (AWS SDK сам поддерживает
`RetryPolicy` через `ClientOverrideConfiguration`). Item-level retry в DSL отсутствует
по той же причине, что и для HTTP — повтор тела `forEach`-блока ломает non-idempotent шаги
(см. USER_GUIDE §6 «Retry — на другом уровне»).

---

## 2. Свой AutoCloseable-ресурс в lifecycle ctx

Допустим, нужен rate limiter, который должен жить всю миграцию и закрыться (например, сбросить
оставшиеся в очереди задачи) в конце. `MigrationContext.register(closeable)` — твоя точка входа.

```kotlin
// com/example/migrations/RateLimiterResource.kt
package com.example.migrations

import com.google.common.util.concurrent.RateLimiter
import io.github.dsudomoin.migration.MigrationContext

class ThrottledClient(
    private val rps: Double,
) : AutoCloseable {
    private val limiter = RateLimiter.create(rps)

    fun acquire() { limiter.acquire() }

    override fun close() {
        // tx, оставшиеся задачи, и т.д. — кастомная логика
    }
}

fun MigrationContext.throttler(rps: Double): ThrottledClient {
    val t = ThrottledClient(rps)
    register(t)        // runner закроет в `finally` после migrate()
    return t
}
```

В скрипте:

```kotlin
override fun MigrationContext.migrate() {
    val throttle = throttler(rps = 50.0)            // 50 запросов в секунду
    val client = ExternalApiClient(...)

    forEach(items, parallel = 8) { item ->
        throttle.acquire()
        client.process(item)
    }
}
```

`runner` гарантирует, что `ThrottledClient.close()` будет вызван даже если `migrate()`
бросил исключение — это часть [lifecycle-контракта](../USER_GUIDE.md#16-outputfolder-и-артефакты-прогона).

Применения помимо rate limiter:
- Streaming-handle (отправляешь данные в S3 multi-part upload — `close()` финализирует upload).
- Metric reporter, который накопил counters во время прогона и хочет flush'нуть на закрытии.
- Custom file format writer (JSON Lines, Parquet, etc.) — открываешь `BufferedWriter` сверху,
  закрываешь через ctx.

---

## 3. Кастомный Executor для `forEach`

По умолчанию runner создаёт **cached**-пул (`Executors.newCachedThreadPool`) с daemon-потоками
`migration-<name>-<n>`. Cached, а не fixed, — потому что реальное число воркеров задаёт
аргумент `forEach(parallel = N)` через свой семафор, и пул обязан уметь выдать столько потоков,
сколько попросили: иначе `parallel` был бы декорацией, а вложенный `forEach` вставал бы намертво
на исчерпании фиксированного пула. `migration.defaults.parallel` на размер пула не влияет — это
лишь значение аргумента `parallel` по умолчанию.

Хочешь свой пул (с MDC, метриками, виртуальными потоками) — опубликуй в графе
`Executor` с тегом `@Tag(MigrationExecutor::class)`. Фабрика должна жить в **`@Module`**:
`@Component` вешается на класс-компонент, а метод-фабрику Kora ищет только в модулях.

```kotlin
// com/example/migrations/MigrationExecutorModule.kt
package com.example.migrations

import io.github.dsudomoin.migration.kora.MigrationExecutor
import ru.tinkoff.kora.common.Module
import ru.tinkoff.kora.common.Tag
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

@Module
interface MigrationExecutorModule {

    @Tag(MigrationExecutor::class)
    fun migrationExecutor(): Executor {
        val counter = AtomicInteger()
        return Executors.newCachedThreadPool { r ->
            Thread(r).apply {
                isDaemon = true
                name = "mig-worker-${counter.incrementAndGet()}"
                uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { _, e ->
                    // свой обработчик — например, дёрнуть alert
                    System.err.println("Uncaught: $e")
                }
            }
        }
    }
}
```

Модуль нужно добавить в список родителей `@KoraApp`, иначе граф о нём не узнает:

```kotlin
@KoraApp
interface App : HoconConfigModule, MigrationModule, MigrationExecutorModule
```

Runner подхватит executor через `Optional<Executor>`-инжект с `@Tag(MigrationExecutor::class)`.
Если бина нет — создаст свой дефолтный.

Две вещи, о которых надо помнить, подменяя пул:

- **Пул должен выдавать столько потоков, сколько просит самый жадный `forEach`.** Fixed-пул на
  32 потока при `forEach(parallel = 64)` тихо ограничит параллелизм 32-мя, а вложенный
  `forEach(parallel > 1)` на исчерпанном fixed-пуле может встать в deadlock. Cached или
  virtual-threads пул этой проблемы не имеют.
- **Свой пул runner не гасит.** Дефолтный он регистрирует как `AutoCloseable` и останавливает
  сам (`shutdown()` + ожидание 30 с, потом `shutdownNow()` с предупреждением в отчёт). Кастомный
  считается собственностью пользователя — закрывай его сам (например, `register(AutoCloseable { pool.shutdown() })`
  в начале `migrate()`).

Применения:
- MDC propagation — твой `ThreadFactory` копирует `MDC.getCopyOfContextMap()` из main-thread.
- Metrics-aware пул (`ThreadPoolExecutor` + gauge на active-tasks в Prometheus).
- `Executors.newVirtualThreadPerTaskExecutor()` для JDK 21+ скриптов с большим числом IO-ожиданий.

---

## 4. Кастомный `Progress` + per-type сериализатор ошибок

`Progress.Custom(n) { done, total -> ... }` принимает любую лямбду — можно тянуть state из
скрипта для составного лога. Полезно когда стандартного `"X/Y done"` мало.

```kotlin
import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.Progress
import io.github.dsudomoin.migration.error.includeItem
import io.github.dsudomoin.migration.forEach
import ru.tinkoff.kora.http.client.common.HttpClientResponseException
import java.util.concurrent.atomic.AtomicLong

class SampleMigration(...) : Migration("SAMPLE-001", "team") {

    private val ok = AtomicLong()
    private val skipped = AtomicLong()
    private val processed4xx = AtomicLong()
    private val startMs = System.currentTimeMillis()

    override fun MigrationContext.migrate() {
        errors.includeItem<Order> { "id=${it.id}, customer=${it.customerId}, amount=${it.amount}" }

        forEach(
            items,
            parallel = 8,
            onError = OnError.handle { e, _ ->
                when {
                    // Не-2xx от типизированного Kora-клиента прилетает как
                    // HttpClientResponseException (у http(call) — как HttpStatusException).
                    e is HttpClientResponseException && e.code in 400..499 -> {
                        processed4xx.incrementAndGet()
                        OnError.Decision.Skip
                    }
                    // 5xx ретраится Kora @Retry внутри HTTP-клиента; сюда попадает только
                    // финальный fail после исчерпания retries — аудитим и идём дальше.
                    e is HttpClientResponseException -> OnError.Decision.Skip
                    e is ValidationException         -> OnError.Decision.Skip   // свой доменный тип
                    else                             -> OnError.Decision.Fail
                }
            },
            progress = Progress.Custom(500) { done, total ->
                val elapsedS = ((System.currentTimeMillis() - startMs) / 1000).coerceAtLeast(1)
                val rate = done / elapsedS
                val pct = if (total != null) " (${done * 100 / total}%)" else ""
                "progress: $done/${total ?: "?"}$pct  ok=${ok.get()}  skip=${skipped.get()}  4xx=${processed4xx.get()}  rate=${rate}/s"
            },
        ) { item ->
            try {
                process(item)
                ok.incrementAndGet()
            } catch (e: Exception) {
                val is4xx = e is HttpClientResponseException && e.code in 400..499
                if (!is4xx) skipped.incrementAndGet()
                throw e
            }
        }
    }

    private fun process(item: Order) { /* ... */ }
}
```

Что здесь нового:
- **`errors.includeItem<Order> { ... }`** — кастомный сериализатор. Когда `Order` попадёт в
  `errors.csv` через автоматический аудитор, рендер пойдёт через эту лямбду, а не через `toString()`.
- **`OnError.handle { ... }`** — domain-классификатор по типу исключения. Для каждого типа —
  своё решение (`Decision.Skip` / `Decision.Fail`). Retry в DSL нет: backoff навешивай на
  Kora `@Retry` внутри клиента (см. http-backfill-archetype).
- **`Progress.Custom(500) { ... }`** — кастомный форматтер, тянет `AtomicLong`-счётчики и
  считает скорость на лету.

Применения:
- Прогресс с throughput-метрикой и распределением по типам ошибок.
- Прогресс с разбивкой по сегментам (например, по тенанту: `tenant_a=120, tenant_b=450`).
- Прогресс, который вместо лога пишет в внешнюю метрику (тогда `format` дёргает свой
  `meterRegistry.counter(...)` и возвращает короткую строку для лога).

---

## Что ещё можно кастомизировать

Эти точки расширения не такие частые, но есть:

| Что | Как |
|---|---|
| Альтернативный формат CSV (JSON Lines, Parquet) | Своя имплементация `AutoCloseable`-writer'а + extension `MigrationContext.openJsonl(...)`, регистрируемая через `register()` |
| Кастомный HTTP-клиент (OkHttp / HttpURLConnection) | Передай `HttpCall` лямбду в `http(call)`. См. [http-backfill-archetype.md](http-backfill-archetype.md) §«Вариация: http()» |
| Свой error-reporter (не CSV, а Kibana / Sentry) | `MigrationContext.errors` типизирован как конкретный `CsvFileErrorReporter` (а не интерфейс) — стандартный runner всегда даёт CSV. Чтобы заменить: либо `open` `CsvFileErrorReporter.report(...)` через subclass и подсунуть его в `DefaultMigrationContext.internalCreate(...)` в своём `Lifecycle`-компоненте; либо форкнуть `MigrationRunner` и интанцировать свой `ErrorReporter`-impl. Pluggable из коробки нет — это сознательное решение (см. AGENTS.md §15.2) |
| Не-Kora приложение | Подключи только `migration-dsl-core` (без `migration-dsl-kora`), создавай `DefaultMigrationContext.internalCreate(...)` сам — фабрика на companion-объекте публичная |
| Перехват exit-кода вместо `exitProcess` | Объяви в графе компонент `MigrationExit` (`fun interface MigrationExit { fun exit(code: Int) }`) — runner отдаст код в него и не убьёт JVM. Нужно тестам на настоящем графе и встраиванию runner'а в приложение, которое живёт дальше |
| Программный конфиг без HOCON | `MigrationConfigValues` / `DefaultsValues` / `ErrorReportingValues` / `ReportValues` — data-классы с дефолтами, реализующие `MigrationConfig`. Удобны в тестах и при встраивании runner'а |

## Что НЕ стоит кастомизировать

- **`ForEachEngine`** — internal-класс, сигнатуры могут меняться между minor-версиями
  без deprecation. Если кажется, что нужно — открой issue.
- **`MigrationContext` сам по себе** (как интерфейс) — кастомные реализации сломаются на
  следующей версии при добавлении новых членов в интерфейс. Используй [`DefaultMigrationContext`](../../core/src/main/kotlin/io/github/dsudomoin/migration/internal/DefaultMigrationContext.kt)
  через его фабрики.
- **`Migration.migrate()` вне `MigrationContext`-receiver** — runner полагается на extension-receiver
  для прокидывания контекста. Не пытайся подменить.
