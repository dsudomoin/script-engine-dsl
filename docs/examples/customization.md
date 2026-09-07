# Кастомизация Migration DSL

> Пошаговое описание узлов — в [гайде](../USER_GUIDE.md).

DSL — это набор extension-функций на `RunScope` плюс несколько hook'ов
(`register`, `scopedResource`, `shared`, `guardWrite`, `errors.includeItem`,
`@Tag(MigrationExecutor)`, `MigrationExit`). Если из коробки чего-то не хватает —
добавляй своё, не дожидаясь патча либы.

Читающие операции библиотеки (`jdbc(db).query`, `cassandra(s).query`, `http(call).get`,
`readCsv`, `openCsv`, `pages`) — extension'ы на `RunScope`, поэтому работают во всех трёх
контекстах: при загрузке `input`, при построении источника (`items = { }`) и в обработчике.

Пишущие (`jdbc(db).execute`, `cassandra(s).execute`, `http(call).post`, `kafka`, `topic`,
`transactional`) объявлены на `HandlerScope` и доступны **только из обработчика**: внешнее
изменение обязано иметь границу элемента и попадать в учёт эффектов. Пиши свою обёртку так же —
читающую на `RunScope`, изменяющую на `HandlerScope`.

Ниже — пять типичных сценариев кастомизации:

1. [Свой ops-wrapper с dry-run gate](#1-свой-ops-wrapper-с-dry-run-gate)
2. [Свой асинхронный клиент под барьер `publish`](#2-свой-асинхронный-клиент-под-барьер-publish)
3. [Свой ресурс в lifecycle: `register` и `scopedResource`](#3-свой-ресурс-в-lifecycle-register-и-scopedresource)
4. [Кастомный Executor для стадий](#4-кастомный-executor-для-стадий)
5. [Кастомный `Progress` + классификатор ошибок](#5-кастомный-progress--классификатор-ошибок)

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

import io.github.dsudomoin.migration.RunScope
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.core.sync.RequestBody

class S3Ops internal constructor(
    private val ctx: RunScope,
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

// HandlerScope, а не RunScope: у S3Ops есть put/delete, значит это пишущая обёртка
fun HandlerScope.s3(client: S3Client, bucket: String): S3Ops = S3Ops(this, client, bucket)
```

Используется обычно:

```kotlin
@Component
class SampleMigration(
    private val s3client: S3Client,
    private val db: JdbcConnectionFactory,
) : MigrationDefinition {

    override val name = "SAMPLE-001"

    override fun plan() = migration(name = name, author = "team") {
        source(
            parallel = 4,
            onItemError = ItemError.Skip,
            items = {
                jdbc(db).query("select id, payload from orders where status = 'EXPORT'") {
                    it.getLong("id") to it.getBytes("payload")
                }.asSequence()
            },
        ) { (id, body) ->
            s3(s3client, "orders-archive").put("orders/$id.json", body)
        }
    }
}
```

Под `MIGRATION_DRY_RUN=true` ни одного реального `PutObject` не уйдёт; в отчёте появится
`s3.put: 8421` и в логе `INFO [DRY-RUN] s3.put (bucket=orders-archive, key=orders/42.json, size=2048)`.

**Что важно:** `guardWrite` — канонический способ интегрироваться с dry-run **изнутри
операции**. Сигнатур две: `guardWrite(label, args, dryRunDefault) { ... }` возвращает значение
(под репетицией — `dryRunDefault`), `guardWrite(label, args) { ... }` — для void-записи.
Значение `dryRunDefault` выбирай так, чтобы вызывающий код под репетицией шёл той же веткой,
что и в бою: для HTTP-статуса это `200`, а не `0`; для «числа обновлённых строк» — `0`;
для хендла — `null` только если вызывающий это переживает.

Read-операции (`listKeys`, `getObject`) НЕ оборачивай в `guardWrite` — они должны работать
под dry-run.

### `guardWrite` или `write`?

Оба ведут в один и тот же гейт, но живут на разных уровнях и не заменяют друг друга:

| | `guardWrite(label, args, dryRunDefault) { }` | `write(name, args) { ...: WriteOutcome }` |
|---|---|---|
| Кто пишет | автор операции — внутри неё | автор миграции — в обработчике |
| Где доступен | `RunScope` (везде) | `HandlerScope` (только в теле стадии) |
| Что видно в отчёте | `dryRunSkipped[label]` | `dryRunSkipped[name]`, `appliedWrites[name]`, `rejectedWrites[name]` |
| Понятие «ожидаемо отклонено» | нет | есть — `WriteOutcome.Rejected(reason)` |

Правило: **операцию** гейтишь `guardWrite`'ом один раз при её написании; **чужой компонент**
(типизированный Kora-клиент, репозиторий, SDK без обёртки) — `write { }` на месте вызова.
Оборачивать свою уже гейтованную операцию ещё и в `write` не нужно: под dry-run тело `write`
не выполняется, и внутренний `guardWrite` до своего счётчика просто не доедет.

### Хендл, который зовут внутри обработчика

`s3(client, bucket)` в примере выше создаёт новый `S3Ops` на каждый вызов. Пока он живёт
в переменной над циклом — это ничего не стоит. Но если хендл дёргается прямо в теле
обработчика на миллионе item'ов и при этом что-то держит (соединение, буфер, счётчики),
объекты нужно мемоизировать — для этого есть `RunScope.shared(key, factory)`:
ресурс создаётся один раз на прогон для данного ключа и сразу регистрируется в реестре
(движок закроет его в `finally`). Ровно так внутри устроены `kafka(producer)` и
`topic(producer, name)`.

```kotlin
class S3Ops internal constructor(
    private val ctx: RunScope,
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

fun HandlerScope.s3(client: S3Client, bucket: String): S3Ops =
    shared(S3Key(client, bucket)) { S3Ops(this, client, bucket) }
```

Ограничение одно: `factory` не должна сама звать `shared` — вложенный вызов идёт на той же
`ConcurrentHashMap.computeIfAbsent`, а его поведение при рекурсивной модификации не определено.

**Retry для S3 transient-ошибок** — навешивай на уровень `S3Client` (AWS SDK сам поддерживает
`RetryPolicy` через `ClientOverrideConfiguration`). Item-level retry в DSL отсутствует
по той же причине, что и для HTTP: повтор тела обработчика ломает non-idempotent шаги,
уже выполненные до сбоя.

---

## 2. Свой асинхронный клиент под барьер `publish`

`write { }` подходит для синхронного вызова: он завершился — значит записалось. Для
клиента, который отдаёт `CompletionStage` и подтверждает доставку позже, есть `publish`:

```kotlin
publish("crm.notify", args = mapOf("customerId" to c.id)) {
    crmClient.notifyAsync(c.id)      // возвращает CompletionStage<*>
}
```

Что делает движок и чего не нужно писать руками:

- регистрирует stage в барьере **своего scope'а** — стадия не завершится, пока всё
  отправленное не подтвердится; в `scoped`-стадии барьер стоит на каждом родителе;
- считает исходы: `acknowledgedPublishes`, `failedEffects`, `abandonedPublishes`
  (не дождались за `completionTimeout`), `lateRegistered` (зарегистрировано после барьера);
- аудитит каждый отказ в `errors.csv` вместе с item'ом, на котором он был отправлен;
- под dry-run **не вызывает** лямбду вообще и не создаёт фальшивый future — поэтому
  `publish` ничего и не возвращает: возвращать было бы нечего.

Единственное требование к твоему клиенту — отдать `java.util.concurrent.CompletionStage`.
Если у него callback-API, заверни сам:

```kotlin
fun HandlerScope.crm(client: CrmClient): CrmOps = shared(client) { CrmOps(client) }

class CrmOps internal constructor(private val client: CrmClient) : AutoCloseable {

    /** Мост callback → CompletionStage. Гейт здесь НЕ нужен: его ставит publish. */
    fun notifyAsync(id: String): CompletionStage<Unit> {
        val cf = CompletableFuture<Unit>()
        client.notify(id) { err -> if (err == null) cf.complete(Unit) else cf.completeExceptionally(err) }
        return cf
    }

    override fun close() { /* дренаж очереди клиента, если он у него есть */ }
}
```

**Отказ асинхронного эффекта не проходит через `ItemError`** и не может: к моменту, когда
известен исход доставки, item давно посчитан успешным, и `Skip` для него физически
неприменим. Любой такой отказ валит стадию на барьере (`ScopeEffectsFailed`), а
не-дождавшиеся — `ScopeCompletionTimeout`. Отправленное при этом не отзывается: оно уходит
в отчёт отдельной строкой, потому что молча исчезать не имеет права.

---

## 3. Свой ресурс в lifecycle: `register` и `scopedResource`

Две разные точки, и путать их дорого.

### `register(closeable)` — на весь прогон

Ресурс живёт от создания до конца прогона, закрывается движком в `finally` в порядке,
обратном регистрации. Это то, что делают за тебя `openCsv`, `topic(...)`, `shared(...)`.

```kotlin
// com/example/migrations/RateLimiterResource.kt
package com.example.migrations

import com.google.common.util.concurrent.RateLimiter
import io.github.dsudomoin.migration.RunScope

class ThrottledClient(rps: Double) : AutoCloseable {
    private val limiter = RateLimiter.create(rps)

    fun acquire() { limiter.acquire() }

    override fun close() {
        // сброс очереди, финализация, метрики — кастомная логика
    }
}

/**
 * Один лимитер на прогон: `shared` создаёт его ровно один раз для данного ключа и сам
 * зовёт `register`, поэтому звать фабрику из параллельных воркеров безопасно.
 *
 * Голый `register(ThrottledClient(rps))` тоже валиден, но тогда единственность — на тебе:
 * вызов в теле обработчика создаст по лимитеру на каждый item, и «50 rps» превратятся
 * в 50 rps на элемент.
 */
fun RunScope.throttler(rps: Double): ThrottledClient =
    shared(ThrottleKey(rps)) { ThrottledClient(rps) }

private data class ThrottleKey(val rps: Double)
```

В плане:

```kotlin
override fun plan() = migration(name = name, author = "team") {
    source(parallel = 8, items = { sequenceOf(/* ... */) }) { item ->
        val throttle = throttler(rps = 50.0)
        throttle.acquire()
        client.process(item)
    }
}
```

Закрытие гарантировано, даже если стадия бросила исключение. Исключение из самого `close()`
наверх не пробрасывается: оно уезжает WARN'ом в лог и строкой в `report.warnings`,
exit-код от этого не меняется.

### `scopedResource { }` — на границу scope'а

Живёт на `SourceScope`, то есть внутри `items = { }`, и закрывает ресурс на границе
**текущего scope'а**. В плоской стадии это конец стадии; в `scoped`-стадии — граница
родителя: ресурс закроется перед тем, как откроется следующий родитель, а не в конце прогона.

```kotlin
scoped(
    parents = { resolve(segments).asSequence() },
    items = { segment ->
        val cursor = legacy.openCursor(segment)      // держит соединение
        scopedResource { cursor.close() }            // закроется на границе этого родителя
        cursor.asSequence()
    },
) { row -> /* ... */ }
```

Порядок на границе строгий: сначала барьер подтверждений, потом закрытие ресурсов. Иначе
соединение, через которое шла отправка, закрылось бы раньше, чем придут ack'и.

Применения помимо rate limiter:
- Streaming-handle (S3 multi-part upload — `close()` финализирует upload).
- Metric reporter, накопивший counters во время прогона и флашащий их на закрытии.
- Custom file format writer (JSON Lines, Parquet) — открываешь `BufferedWriter` сверху,
  закрываешь через реестр.

---

## 4. Кастомный Executor для стадий

По умолчанию runner создаёт **cached**-пул (`Executors.newCachedThreadPool`) с daemon-потоками
`migration-<name>-<n>`. Cached, а не fixed, — потому что реальное число воркеров задаёт
аргумент `source(parallel = N)` / `scoped(parallel = N)` через свой семафор, и пул обязан уметь
выдать столько потоков, сколько попросили: иначе `parallel` был бы декорацией.
`migration.defaults.parallel` на размер пула не влияет — это лишь значение аргумента
`parallel` по умолчанию.

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

- **Пул должен выдавать столько потоков, сколько просит самая жадная стадия.** Fixed-пул на
  32 потока при `parallel = 64` тихо ограничит параллелизм 32-мя. Cached или
  virtual-threads пул этой проблемы не имеют.
- **Свой пул runner не гасит.** Дефолтный он регистрирует как `AutoCloseable` и останавливает
  сам (`shutdown()` + ожидание 30 с, потом `shutdownNow()` с предупреждением в отчёт). Кастомный
  считается собственностью пользователя — закрывай его сам (например,
  `register(AutoCloseable { pool.shutdown() })` в первом же `items = { }`).

Применения:
- MDC propagation — твой `ThreadFactory` копирует `MDC.getCopyOfContextMap()` из main-thread.
- Metrics-aware пул (`ThreadPoolExecutor` + gauge на active-tasks в Prometheus).
- `Executors.newVirtualThreadPerTaskExecutor()` для JDK 21+ скриптов с большим числом IO-ожиданий.

---

## 5. Кастомный `Progress` + классификатор ошибок

`Progress.Custom(n) { done, total -> ... }` принимает любую лямбду — можно тянуть state из
скрипта для составного лога. Полезно когда стандартного `"X/Y done"` мало.

```kotlin
import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.MigrationDefinition
import io.github.dsudomoin.migration.Progress
import io.github.dsudomoin.migration.WriteOutcome
import io.github.dsudomoin.migration.error.includeItem
import io.github.dsudomoin.migration.migration
import ru.tinkoff.kora.http.client.common.HttpClientResponseException
import java.util.concurrent.atomic.AtomicLong

class SampleMigration(/* ... */) : MigrationDefinition {

    override val name = "SAMPLE-001"

    private val ok = AtomicLong()
    private val skipped = AtomicLong()
    private val rejected4xx = AtomicLong()
    private val startMs = System.currentTimeMillis()

    override fun plan() = migration(name = name, author = "team") {
        source(
            parallel = 8,
            onItemError = ItemError.Handle { e, order ->
                when {
                    // Не-2xx от типизированного Kora-клиента прилетает как
                    // HttpClientResponseException (у http(call) — как HttpStatusException).
                    e is HttpClientResponseException && e.code in 400..499 -> {
                        rejected4xx.incrementAndGet()
                        ItemError.Decision.Skip
                    }
                    // 5xx ретраится Kora @Retry внутри HTTP-клиента; сюда попадает только
                    // финальный fail после исчерпания retries — аудитим и идём дальше.
                    e is HttpClientResponseException -> ItemError.Decision.Skip
                    e is ValidationException         -> ItemError.Decision.Skip   // свой доменный тип
                    // Классификатор видит типизированный item, а не Any? — решение может
                    // зависеть и от самих данных.
                    order.amount > 1_000_000         -> ItemError.Decision.Fail
                    else                             -> ItemError.Decision.Skip
                }
            },
            progress = Progress.Custom(500) { done, total ->
                val elapsedS = ((System.currentTimeMillis() - startMs) / 1000).coerceAtLeast(1)
                val rate = done / elapsedS
                val pct = if (total != null) " (${done * 100 / total}%)" else ""
                "progress: $done/${total ?: "?"}$pct  ok=${ok.get()}  skip=${skipped.get()}  4xx=${rejected4xx.get()}  rate=${rate}/s"
            },
            items = {
                errors.includeItem<Order> { "id=${it.id}, customer=${it.customerId}, amount=${it.amount}" }
                loadOrders()
            },
        ) { order ->
            write("order.sync", args = mapOf("id" to order.id)) {
                process(order)
                WriteOutcome.Applied
            }
            ok.incrementAndGet()
        }
    }
}
```

Что здесь используется:

- **`errors.includeItem<Order> { ... }`** — кастомный сериализатор. Когда `Order` попадёт в
  `errors.csv` через автоматический аудитор, рендер пойдёт через эту лямбду, а не через
  `toString()`. Регистрируется в `items = { }`: этот блок выполняется до первого обработчика.
  Lookup идёт по классу, потом по супертипам и интерфейсам — `includeItem<Map<*, *>>`
  сработает и для `LinkedHashMap`.
- **`ItemError.Handle { e, item -> ... }`** — классификатор, получающий **типизированный**
  item. Retry в DSL нет: backoff навешивай на Kora `@Retry` внутри клиента (см.
  [http-backfill-archetype.md](http-backfill-archetype.md)).
- **`Progress.Custom(500) { ... }`** — форматтер, тянущий `AtomicLong`-счётчики и считающий
  скорость на лету. Есть ещё `Progress.Every(n)` (дефолтный формат с другим периодом),
  `Progress.Default` и `Progress.Off`.

`total` в форматтере — `null` для последовательных источников: длина `Sequence` заранее
неизвестна, и врать про проценты движок не станет.

Применения:
- Прогресс с throughput-метрикой и распределением по типам ошибок.
- Прогресс с разбивкой по сегментам (`tenant_a=120, tenant_b=450`).
- Прогресс, который вместо лога пишет во внешнюю метрику (тогда `format` дёргает свой
  `meterRegistry.counter(...)` и возвращает короткую строку для лога).

---

## Что ещё можно кастомизировать

Эти точки расширения не такие частые, но есть:

| Что | Как |
|---|---|
| Перехват exit-кода вместо `exitProcess` | Объяви в графе компонент `MigrationExit` (`fun interface MigrationExit { fun exit(code: Int) }`) — runner отдаст код в него и не убьёт JVM. Нужно тестам на настоящем графе (см. [`KoraWireUpTest`](../../example/src/test/kotlin/io/github/dsudomoin/migration/example/KoraWireUpTest.kt)) и встраиванию runner'а в приложение, которое живёт дальше |
| Альтернативный формат выхода (JSON Lines, Parquet) | Своя имплементация `AutoCloseable`-writer'а + extension `RunScope.openJsonl(...)`, регистрируемая через `register()`. Обрати внимание: встроенный `output(...)` объявляется в билдере именно потому, что должен открываться один раз на прогон — свой writer открывай так же, а не внутри `items` у `scoped`-стадии |
| Кастомный HTTP-клиент (OkHttp / HttpURLConnection) | Передай `HttpCall` лямбду в `http(call)`. См. [http-backfill-archetype.md](http-backfill-archetype.md) §«Вариация: http()» |
| Свой error-reporter (не CSV, а Kibana / Sentry) | `RunScope.errors` типизирован как конкретный `CsvFileErrorReporter` (а не интерфейс) — стандартный runner всегда даёт CSV. Чтобы заменить: либо переопредели `open fun report(...)` в subclass'е `CsvFileErrorReporter` и передай его в `RunContext.internalCreate(...)` из своего `Lifecycle`-компонента; либо форкни `MigrationRunner`. Pluggable из коробки нет — это сознательное решение |
| Не-Kora приложение | Подключи только `migration-dsl-core` (без `migration-dsl-kora`), собери `RunContext.internalCreate(...)` сам и отдай план в `PlanInterpreter(ctx).execute(plan)` — обе фабрики публичные. Так устроены тесты библиотеки, включая [pilot-тест архетипа сравнения](../../kora/src/test/kotlin/io/github/dsudomoin/migration/kora/pilot/ComparisonPilotTest.kt) |
| Программный конфиг без HOCON | `MigrationConfigValues` / `DefaultsValues` / `ErrorReportingValues` / `ReportValues` — data-классы с дефолтами, реализующие `MigrationConfig`. Удобны в тестах и при встраивании runner'а |
| Своя политика на unhandled-ошибку | `migration(name, author, onUnhandled = ScriptPolicy.LOG_AND_COMPLETE) { }` — параметр билдера перекрывает `migration.defaults.onUnhandled` для конкретной миграции |

## Что НЕ стоит кастомизировать

- **`PlanInterpreter` и `RunContext`** — публичны ради Kora-модуля и тестов, но их
  сигнатуры могут меняться между minor-версиями без deprecation. Используй фабрики
  (`RunContext.test`, `RunContext.internalCreate`), а не конструкторы.
- **Собственные реализации `RunScope` / `SourceScope` / `HandlerScope`** — сломаются на
  следующей версии при добавлении членов в интерфейс. Внутри всё равно один и тот же объект:
  scope'ы разделены только типами на границе DSL.
- **Регистрация стадий во время исполнения.** `@DslMarker` (`@MigrationDsl`) запрещает вызвать
  `source`/`scoped`/`input`/`output` изнутри `items` и `handle` — это ошибка компиляции, и
  обходить её кастами не надо: план неизменяем, и мутировать его на ходу означало бы иметь
  два разных плана у одного прогона.
