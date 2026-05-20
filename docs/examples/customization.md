# Кастомизация Migration DSL

DSL — это набор extension-функций на `MigrationContext` плюс несколько hook'ов
(`register`, `guardWrite`, `@Tag(MigrationExecutor)`). Если из коробки чего-то не
хватает — добавляй своё, не дожидаясь патча либы.

Ниже — четыре типичных сценария кастомизации:

1. [Свой ops-wrapper с dry-run gate](#1-свой-ops-wrapper-с-dry-run-gate)
2. [Свой AutoCloseable-ресурс в lifecycle ctx](#2-свой-autocloseable-ресурс-в-lifecycle-ctx)
3. [Кастомный Executor для `forEach`](#3-кастомный-executor-для-foreach)
4. [Кастомный `Progress` + per-type сериализатор ошибок](#4-кастомный-progress--per-type-сериализатор-ошибок)

---

## 1. Свой ops-wrapper с dry-run gate

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
Read-операции (`listKeys`, `getObject`) НЕ оборачивай в `guardWrite` — они должны работать
под dry-run.

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

По умолчанию runner создаёт `Executors.newFixedThreadPool(migration.defaults.parallel)` с
`ThreadFactory`, выставляющим `migration-<name>` имена. Хочешь свой пул (с MDC,
metric'ами, или вообще `ForkJoinPool`) — публикуй `@Tag(MigrationExecutor::class) Executor`
в графе:

```kotlin
// com/example/migrations/CustomExecutor.kt
package com.example.migrations

import io.github.dsudomoin.migration.kora.MigrationExecutor
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.common.Tag
import java.util.concurrent.Executor
import java.util.concurrent.Executors

@Component
class MigrationExecutorFactory {

    @Tag(MigrationExecutor::class)
    fun executor(): Executor {
        return Executors.newFixedThreadPool(32) { r ->
            Thread(r).apply {
                isDaemon = true
                name = "mig-worker-${threadCounter.incrementAndGet()}"
                uncaughtExceptionHandler = Thread.UncaughtExceptionHandler { _, e ->
                    // свой обработчик — например, дёрнуть alert
                    System.err.println("Uncaught: $e")
                }
            }
        }
    }

    companion object {
        private val threadCounter = java.util.concurrent.atomic.AtomicInteger()
    }
}
```

Runner подхватит этот executor через `Optional<Executor>` инжект с `@Tag(MigrationExecutor::class)`.
Если бин не зарегистрирован — runner создаст свой дефолтный.

Применения:
- MDC propagation — твой `ThreadFactory` копирует `MDC.getCopyOfContextMap()` из main-thread.
- Metrics-aware пул (`ThreadPoolExecutor` с `ScheduledThreadPoolExecutor` для прокидывания
  active-tasks gauge в Prometheus).
- `VirtualThreadPerTaskExecutor` для JDK 21+ скриптов с большим числом IO-ожиданий.

---

## 4. Кастомный `Progress` + per-type сериализатор ошибок

`Progress.Custom(n) { done, total -> ... }` принимает любую лямбду — можно тянуть state из
скрипта для составного лога. Полезно когда стандартного `"X/Y done"` мало.

```kotlin
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
            onError = OnError.handle { e, item ->
                when (e) {
                    is HttpClient4xxException -> {
                        processed4xx.incrementAndGet()
                        OnError.Decision.Skip
                    }
                    // 5xx ретраится Kora @Retry внутри HTTP-клиента; сюда попадает только
                    // финальный fail после исчерпания retries — аудитим и идём дальше.
                    is HttpClient5xxException -> OnError.Decision.Skip
                    is ValidationException    -> OnError.Decision.Skip
                    else                      -> OnError.Decision.Fail
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
                if (e !is HttpClient4xxException) skipped.incrementAndGet()
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

## Что НЕ стоит кастомизировать

- **`ForEachEngine`** — internal-класс, сигнатуры могут меняться между minor-версиями
  без deprecation. Если кажется, что нужно — открой issue.
- **`MigrationContext` сам по себе** (как интерфейс) — кастомные реализации сломаются на
  следующей версии при добавлении новых членов в интерфейс. Используй [`DefaultMigrationContext`](../../core/src/main/kotlin/io/migration/internal/DefaultMigrationContext.kt)
  через его фабрики.
- **`Migration.migrate()` вне `MigrationContext`-receiver** — runner полагается на extension-receiver
  для прокидывания контекста. Не пытайся подменить.
