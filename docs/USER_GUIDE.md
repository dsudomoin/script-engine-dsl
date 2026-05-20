# Migration DSL — гайд по использованию

Большой документ для тех, кто пишет миграционный скрипт первый раз. Внутри —
по шагам от Gradle-зависимостей до запуска на проде и разбора отчёта.

## Содержание

1. [Установка и подключение](#1-установка-и-подключение)
2. [Первый скрипт](#2-первый-скрипт)
3. [Запуск](#3-запуск)
4. [Что доступно в `migrate()` — `MigrationContext`](#4-что-доступно-в-migrate--migrationcontext)
5. [`forEach` — цикл с параллелизмом](#5-foreach--цикл-с-параллелизмом)
6. [`OnError` — политика обработки ошибок](#6-onerror--политика-обработки-ошибок)
7. [`Progress` — прогресс-логирование](#7-progress--прогресс-логирование)
8. [Чтение CSV — `readCsv`](#8-чтение-csv--readcsv)
9. [Запись CSV — `openCsv`](#9-запись-csv--opencsv)
10. [Postgres / JDBC — `jdbc(db)` и `transactional`](#10-postgres--jdbc--jdbcdb-и-transactional)
11. [Cassandra — `cassandra(session)`](#11-cassandra--cassandrasession)
12. [Kafka — `topic(producer, name)` и `kafka(producer).publish`](#12-kafka--topicproducer-name-и-kafkaproducerpublish)
13. [HTTP — типизированный клиент + `mutation` + `http(call)`](#13-http--типизированный-клиент--mutation--httpcall)
14. [Конфигурация HOCON](#14-конфигурация-hocon)
15. [Dry-run](#15-dry-run)
16. [`outputFolder` и артефакты прогона](#16-outputfolder-и-артефакты-прогона)
17. [`errors.csv` и `errors.includeItem<T>`](#17-errorscsv-и-errorsincludeitemt)
18. [Итоговый отчёт](#18-итоговый-отчёт)
19. [`ScriptPolicy` — что делать при неожиданном падении](#19-scriptpolicy--что-делать-при-неожиданном-падении)
20. [Тестирование скриптов](#20-тестирование-скриптов)
21. [Exit-коды](#21-exit-коды)
22. [Шпаргалка-FAQ](#22-шпаргалка-faq)

---

## 1. Установка и подключение

### Gradle

```kotlin
// build.gradle.kts
plugins {
    kotlin("jvm") version "2.1.20"
    kotlin("kapt") version "2.1.20"
    application
}

kotlin { jvmToolchain(21) }

application { mainClass = "com.example.AppKt" }

dependencies {
    implementation("io.github.dsudomoin.migration:migration-dsl-core:0.1.0")
    implementation("io.github.dsudomoin.migration:migration-dsl-kora:0.1.0")

    // Kora — подключай только те модули, которые реально используешь в скриптах
    implementation("ru.tinkoff.kora:config-hocon:1.1.25")
    implementation("ru.tinkoff.kora:application-graph:1.1.25")
    implementation("ru.tinkoff.kora:database-jdbc:1.1.25")     // если нужен Postgres
    implementation("ru.tinkoff.kora:kafka:1.1.25")              // если нужна Kafka
    implementation("ru.tinkoff.kora:http-client-jdk:1.1.25")    // если нужен HTTP

    runtimeOnly("org.postgresql:postgresql:42.7.7")             // драйвер БД
    runtimeOnly("ch.qos.logback:logback-classic:1.5.16")        // logging backend

    kapt("ru.tinkoff.kora:annotation-processors:1.1.25")
}
```

### `@KoraApp`

```kotlin
// App.kt
@KoraApp
interface App :
    HoconConfigModule,
    JdbcDatabaseModule,
    KafkaProducerModule,
    JdkHttpClientModule,
    MigrationModule

fun main() {
    KoraApplication.run { AppGraph.graph() }
}
```

`MigrationModule` нужен обязательно — через него runner попадает в граф. Остальные модули
включай по потребности.

### `application.conf`

Минимум:
```hocon
migration {
  run = ${?MIGRATION_RUN}        # имя миграции, env-override
}

db {
  jdbcUrl  = ${?DB_URL}
  username = ${?DB_USER}
  password = ${?DB_PASSWORD}
}
```

Полную форму конфига см. в [§14](#14-конфигурация-hocon).

---

## 2. Первый скрипт

```kotlin
package com.example.migrations

import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.csv.openCsv
import io.github.dsudomoin.migration.forEach
import io.github.dsudomoin.migration.kora.ops.jdbc
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory

@Component
class HelloMig(private val db: JdbcConnectionFactory) : Migration(name = "HELLO", author = "you") {

    override fun MigrationContext.migrate() {
        val out = openCsv("ids.csv", "id")

        val ids = jdbc(db).query("select id from customers") { it.getLong("id") }

        forEach(ids) { id ->
            out.row(id)
        }
    }
}
```

Что здесь происходит:

- **`@Component`** — Kora зарегистрирует класс в графе автоматически. Runner найдёт его
  через `All<Migration>`.
- **`Migration(name, author)`** — имя `"HELLO"` совпадает с `migration.run` в HOCON / env'е.
- **`override fun MigrationContext.migrate()`** — тело миграции. `MigrationContext` —
  receiver, поэтому все DSL-расширения (`openCsv`, `jdbc`, `forEach`, и т.д.) доступны без префикса.
- **`openCsv("ids.csv", "id")`** — открывает файл в [`outputFolder`](#16-outputfolder-и-артефакты-прогона)
  (по умолчанию `logs/HELLO/`), пишет header, регистрируется в ctx. Runner закроет в `finally`.
- **`jdbc(db).query(...)`** — простой SELECT, возвращает `List<Long>`.
- **`forEach(ids) { ... }`** — sequential обработка. Параллелизма по умолчанию нет.

---

## 3. Запуск

```bash
MIGRATION_RUN=HELLO ./gradlew run
```

Или из IDE — IntelliJ Run Configuration с env `MIGRATION_RUN=HELLO`.

В консоль уйдёт лог + итоговый отчёт. В файловой системе появится `logs/HELLO/`:

```
logs/HELLO/
├── migration.log     # всё, что писалось через slf4j во время прогона
├── errors.csv        # пусто, если ошибок не было
├── errors.log
└── ids.csv           # наш output
```

---

## 4. Что доступно в `migrate()` — `MigrationContext`

`MigrationContext` — receiver `migrate()`. Через него доступны:

| Свойство | Тип | Что |
|---|---|---|
| `dryRun` | `Boolean` | `true` если запущено с `MIGRATION_DRY_RUN=true` |
| `log` | `slf4j.Logger` | Логгер `io.github.dsudomoin.migration.<имя_миграции>` — пиши сюда свои INFO/WARN/DEBUG |
| `report` | `ReportBuilder` | Счётчики прогона. Обычно дёргают только в `Progress.Custom { ... }` для кастомных метрик |
| `executor` | `Executor` | Пул потоков для `forEach`. Обычно сами не используешь |
| `outputFolder` | `Path` | Путь к папке артефактов (см. [§16](#16-outputfolder-и-артефакты-прогона)) |
| `errors` | `CsvFileErrorReporter` | Авто-аудитор. Используется в основном для `errors.includeItem<T> { ... }` (см. [§17](#17-errorscsv-и-errorsincludeitemt)) |

Доступны методы:
- `register(closeable)` — зарегистрировать свой `AutoCloseable` для авто-close после `migrate()`. Обычно сама либа делает это за тебя (через `openCsv`/`topic`).
- `guardWrite(label, args, action)` — низкоуровневый dry-run gate. Снова, чаще всего сделано внутри DSL.
- `auditError(e, item)` — ручной вызов аудитора. В обычной жизни не нужен.

---

## 5. `forEach` — цикл с параллелизмом

Три перегрузки:

### 5.1. Single-item

```kotlin
forEach(items, parallel = 4, onError = OnError.Skip) { item ->
    // обработка одного item'а в одном из 4 потоков
}
```

### 5.2. Chunked (батчи)

```kotlin
forEach(items, chunk = 500, parallel = 4, onError = OnError.Skip) { batch: List<T> ->
    // batch — кусок из 500 (или меньше для последнего)
}
```

Уместно когда:
- нужны `WHERE id IN :ids` запросы;
- bulk-insert/update эффективнее;
- хочешь меньше round-trip'ов в HTTP API.

### 5.3. `Sequence`-источник

```kotlin
val items: Sequence<Long> = ...

forEach(items, parallel = 4, onError = OnError.Skip) { id ->
    // обработка по мере поступления, без материализации в List
}
```

`Sequence` обычно приходит из `jdbc(db).stream(...) { rows -> ... }` —
в этом случае `forEach` пишется **внутри** `consume`-блока stream'а, потому
что `rows: Sequence<T>` валиден только в его scope (см. §10).

### Все параметры `forEach`

```kotlin
forEach(
    items,
    chunk = 500,                              // для chunked-перегрузки
    parallel = 8,                             // потоков обработки. Дефолт 1
    onError = OnError.Skip,                   // что делать при ошибке. Дефолт OnError.Fail
    onErrorLog = { e, item ->                 // доп. лог при ошибке (помимо авто-аудита). Дефолт null
        log.warn("Failed item=$item: ${e.message}")
    },
    logEach = { item ->                       // лог после успешной обработки. Дефолт null
        "Processed item=$item"
    },
    progress = Progress.Custom(1000) { done, total ->  // прогресс-лог
        "$done/$total processed"
    },
) { item -> ... }
```

> **Про retry.** Item-уровневого retry в DSL **нет** — это сознательный выбор
> (item-retry повторяет всё тело `forEach`-блока, что ломает non-idempotent
> шаги: повторный publish в Kafka, повторный INSERT с auto-PK). Для transient
> ошибок сети — Kora `@Retry` на типизированном `@HttpClient` /
> `@KafkaPublisher` / repository-методе (см. §6 и §13).

---

## 6. `OnError` — политика обработки ошибок

`OnError` — `sealed interface` с тремя вариантами:

### `OnError.Fail` (дефолт)

Прервать миграцию на первой же ошибке. Exit-code 1. Уместно для критичных миграций,
где «частично сделать» хуже, чем «не сделать ничего».

### `OnError.Skip`

Аудитнуть item в `errors.csv`, инкрементировать `report.skipped`, продолжить.

```kotlin
forEach(items, onError = OnError.Skip) { ... }
```

### `OnError.handle { e, item -> Decision.Skip | Decision.Fail }`

Кастомная классификация по типу исключения:

```kotlin
onError = OnError.handle { e, item ->
    when (e) {
        is ValidationException     -> OnError.Decision.Skip
        is DataIntegrityException  -> OnError.Decision.Fail
        else                       -> OnError.Decision.Skip
    }
}
```

`Handle` принимает решение **один раз** по типу ошибки — без backoff'а и без retry. Решение
`Decision.Skip` ведёт к тому же контракту, что `OnError.Skip` (аудит + продолжение);
`Decision.Fail` — к тому же, что `OnError.Fail` (прерывание).

### Retry — на другом уровне

В DSL **нет** item-level retry-механизма (`OnError.RETRY(...)` не существует — было удалено
сознательно). Item-retry повторяет всё тело `forEach`-блока, что ломает корректность
non-idempotent шагов: повторный publish в Kafka, повторный INSERT с auto-PK, повторный
HTTP-вызов после успешного side-effect'а.

Правильные места для retry:

1. **Kora `@Retry`** на типизированном `@HttpClient` / `@KafkaPublisher` / repository-методе —
   ретраит **только** failing remote-вызов, не всю миграцию. Умеет classify + backoff
   из коробки. Это идиоматичный путь.

   ```kotlin
   @HttpClient(configPath = "clients.enrichment")
   interface EnrichmentClient {
       @HttpRoute(method = "GET", path = "/customer/{id}")
       @Retry("clients.enrichment.fetch")          // имя конфига; параметры — в HOCON
       fun fetch(@Path id: Long): CustomerInfo
   }
   ```

   ```hocon
   resilient.retry.clients.enrichment.fetch {
     delay     = "500ms"        # пауза перед первым retry
     attempts  = 2              # доп. попытки после оригинального вызова → 3 попытки всего
     delayStep = "500ms"        # прибавка к delay на каждую следующую попытку
   }                            # → waits: 500ms, 1000ms (linear backoff)
   ```

   Внимание: в Kora resilient backoff **линейный** (`delay + (n-1)*delayStep`), параметра
   `mode = EXPONENTIAL` или подобного у `@Retry` нет. Если нужен настоящий exponential —
   используй императивный `RetryManager.get("name").retry(supplier)` с собственным backoff
   или мирись с linear (`delayStep` побольше — практически достаточно).

   Аннотация принимает **только имя конфига**: `@Retry("config.path.name")`. Никаких
   `attempts = N, delay = M` параметров у аннотации нет — всё в `resilient.retry.<name>`.

   Чтобы аннотация заработала — в `@KoraApp` добавь `ResilientModule` и в `build.gradle`
   зависимость `ru.tinkoff.kora:resilient-kora:<version>`.

2. **Локальный `try/catch`** вокруг конкретного failing-примитива внутри `forEach`-блока,
   когда нужна узкая retry-логика и `@Retry` не подходит.

### Что попадает в `errors.csv`

- `OnError.Skip` → каждый skipped item → запись.
- `OnError.handle { ... → Decision.Skip }` → запись.

В CSV колонки: `timestamp, migration, author, itemRepr, errorClass, errorMessage`.

---

## 7. `Progress` — прогресс-логирование

```kotlin
forEach(items, progress = Progress.Default) { ... }    // дефолт: каждые ~1000
forEach(items, progress = Progress.Off) { ... }         // тишина
forEach(items, progress = Progress.Every(100)) { ... }  // дефолтный формат, каждые 100

forEach(items, progress = Progress.Custom(500) { done, total ->
    "processed=$done/${total ?: "?"} rate=${done * 1000 / max(elapsed, 1)}/s"
}) { ... }
```

`total` — `null` для `Sequence`-источников (driver не знает заранее, сколько).

`Progress` пишет в slf4j на уровне INFO — то же, что попадает в `migration.log`.

---

## 8. Чтение CSV — `readCsv`

```kotlin
// Из файловой системы (относительно CWD JVM)
val ids = readCsv("input/customer-ids.csv") { row -> row["customer_id"]!! }.toList()

// Из classpath (для test-fixture)
val ids = readCsv("fixtures/seed.csv", classpath = true) { row -> row["id"]!! }.toList()

// По абсолютному Path
val ids = readCsv(Path.of("/data/input.csv")) { it["id"]!! }.toList()
```

Возвращает `Sequence<T>`. Mapper получает `Map<String, String>` (ключ — имя колонки из header'а).

Если файла нет — `IOException` на первой итерации.

Если хочешь стримово, без `.toList()`, — передавай `Sequence` напрямую в `forEach`:
```kotlin
forEach(readCsv(path) { ... }, ...) { row -> ... }
```

---

## 9. Запись CSV — `openCsv`

Две перегрузки:

```kotlin
// По имени — резолвится в outputFolder
val processed = openCsv("processed.csv", "id", "status")
val nested    = openCsv("by-date/2026.csv", "id", "amount")  // подкаталоги создадутся

// По абсолютному Path — для случаев когда нужен не-в-outputFolder файл
val external  = openCsv(Path.of("/shared/export.csv"), "id", "name")
```

`openCsv` возвращает [`CsvOutput`](../core/src/main/kotlin/io/migration/csv/CsvWrite.kt) — handle с одним методом:

```kotlin
fun row(vararg cells: Any?)
```

Что важно:
- **Header пишется сразу** при открытии (даже если `row(...)` ни разу не вызвали).
- **`row(...)` thread-safe** — можно вызывать из параллельных `forEach`-воркеров.
- **`close()` идемпотентен** и вызывается runner'ом в `finally` — сам не закрывай.
- **TRUNCATE на ререн** — при повторном запуске той же миграции файл перезаписывается.
- **Под dry-run файлы всё равно пишутся** — это диагностический артефакт.

Запятые, кавычки и переводы строк в значениях автоматически квотируются по RFC 4180.

---

## 10. Postgres / JDBC — `jdbc(db)` и `transactional`

### Чтение

```kotlin
// query → List<T> (материализует всё в память)
val rows = jdbc(db).query(
    "select id, status from orders where created_at > :since",
    "since" to Instant.parse("2026-01-01T00:00:00Z"),
) { rs ->
    Order(rs.getLong("id"), rs.getString("status"))
}

// stream → callback-scoped Sequence<T> поверх true JDBC cursor (PreparedStatement#setFetchSize)
jdbc(db).stream(
    "select id from orders where status = :s",
    "s" to "STUCK",
    fetchSize = 5000,
    mapper = { it.getLong("id") },
) { rows ->
    forEach(rows, parallel = 8, onError = OnError.Skip) { id -> ... }
}
```

> **Важно про `stream`.** API намеренно callback-style. `rows: Sequence<T>` валиден **только
> внутри** `consume`-блока. После возврата из `consume` `ResultSet` / `PreparedStatement` /
> `Connection` уже закрыты — попытка iter'ить наружу даст `SQLException("ResultSet is closed")`.
> Это сознательный дизайн: физически невозможно leak'нуть JDBC-handle за пределы `db.inTx`-scope.
> Если нужно вернуть значение наружу — фолди `rows` в нужный тип прямо внутри `consume`:
>
> ```kotlin
> val total: Long = jdbc(db).stream(
>     "select id from orders", mapper = { it.getLong("id") },
> ) { rows -> rows.count().toLong() }
> ```
>
> `sequence { while(rs.next()) yield(...) }` single-pass — повторная iteration после первого
> прохода даст 0 элементов. Compose (`.map` / `.filter`) — внутри `consume`.

Параметры — named placeholders `:name`. Биндинг через `setObject` — Kora/Postgres сам приводит
типы для большинства случаев. Для специфических — кастуй явно: `"since"::timestamp`.

### Запись

```kotlin
// Одиночный UPDATE/INSERT/DELETE
jdbc(db).execute(
    "update orders set status = :s where id = :id",
    "s" to "DONE", "id" to 42L,
)

// Batch insert
jdbc(db).batch("insert into log(id, msg) values (?, ?)", entries) { ps, entry ->
    ps.setLong(1, entry.id); ps.setString(2, entry.msg)
}
```

`execute` и `batch` идут через `guardWrite` — под dry-run пропускаются, в отчёте видны.

### Транзакция

```kotlin
transactional(jdbc(db)) {
    execute("update orders set status = 'PROCESSING' where id = :id", "id" to orderId)
    execute("insert into order_audit(order_id, event) values (:id, 'process_start')", "id" to orderId)
    // если любой execute упал — rollback всей пары
}
```

Внутри блока `this: SqlOps` — `execute/query/batch` без префикса. Все они используют один
`Connection`, открытый через `db.inTx` Kora. Commit на успехе, rollback на исключении.

**Вложенный `transactional` бросает `IllegalStateException`** — нет smart-merge с outer-tx,
автор должен явно решить, что делать.

---

## 11. Cassandra — `cassandra(session)`

```kotlin
val rows = cassandra(session).query(
    "select id, value from t.items where id in :ids",
    "ids" to listOf("A-1", "A-2", "A-3"),
) { row -> row.getString("id") to row.getString("value") }

// Streaming (driver делает paging автоматом)
val all = cassandra(session).stream("select id from t.items") { it.getString("id") }
forEach(all, parallel = 4) { id -> ... }

// Write
cassandra(session).execute(
    "update t.items set value = :v where id = :id",
    "v" to "new", "id" to "A-1",
)
```

Multi-кластерная миграция — два разных `CqlSession` в графе через `@Tag`, потом
`cassandra(primary)` и `cassandra(replica)` (см. [comparison-archetype.md](examples/comparison-archetype.md)).

---

## 12. Kafka — `topic(producer, name)` и `kafka(producer).publish`

В Kora канонический способ публиковать — **типизированный `@KafkaPublisher`**-интерфейс.
Raw `Producer<K, V>` доступен через `@Tag(SomePublisher::class)` инжект, но используется
реже. Ниже — три способа интегрировать миграцию с Kafka в зависимости от ситуации.

### Способ 1: `@KafkaPublisher.Topic` + `mutation { }` (рекомендуемый)

```kotlin
@KafkaPublisher("kafka.orders.publisher")
interface OrdersPublisher {

    @KafkaPublisher.Topic("kafka.orders.publisher.resyncTopic")
    fun publishResync(key: String, @Json value: OrderEvent)
}

@Component
class Task(private val publisher: OrdersPublisher) : Migration(...) {
    override fun MigrationContext.migrate() {
        forEach(orders) { order ->
            mutation("orders.resync", args = mapOf("orderId" to order.id)) {
                publisher.publishResync(order.id.toString(), OrderEvent.from(order))
            }
        }
    }
}
```

`@KafkaPublisher.Topic("...")` указывает на HOCON-путь, откуда берётся имя топика:

```hocon
kafka.orders.publisher {
  driverProperties { "bootstrap.servers" = ${KAFKA_BROKERS} }
  resyncTopic { topic = "orders.resync" }
}
```

`mutation("label", args = ...) { ... }` оборачивает вызов в dry-run gate. **`label` —
константа** (`"orders.resync"`), чтобы в `report.dryRunSkipped` получился чистый агрегат
`mutation:orders.resync: 8421`. **`args` — диагностика** для лога (`(orderId=42)` в каждой
INFO-строке).

Плюсы: типизированный API, `@Json` сериализация автоматом, имя топика в HOCON
(env-override возможен). Минусы: для каждого `mutation { publisher.method() }` нет
batch-flush-coordination (Kora-генерированный метод — sync per call).

### Способ 2: raw `Producer<K, V>` + `topic(...)` handle

Когда хочется явный handle с авто-flush на закрытии (одна короткая папка на топик).
Раз `@KafkaPublisher` регистрирует `Producer<K, V>` тэгом своего интерфейса, его можно достать:

```kotlin
@KafkaPublisher("kafka.orders.publisher")
interface OrdersPublisher {
    fun send(record: ProducerRecord<String, OrderEvent>)  // нужно хотя бы одну сигнатуру для типов K,V
}

@Component
class Task(
    @Tag(OrdersPublisher::class) private val producer: Producer<String, OrderEvent>,
) : Migration(...) {
    override fun MigrationContext.migrate() {
        val resync = topic(producer, "orders.resync")    // регистрируется в ctx, auto-flush на close
        forEach(orders) { order ->
            resync.send(order.id.toString(), OrderEvent.from(order))
        }
    }
}
```

`KafkaTopic.send(...)` — sync publish (`producer.send(...).get()`). Под dry-run возвращает
`null`, в отчёт идёт `kafka.publish` с разбивкой по topic+key.

Плюсы: одна точка подачи топика, `flush()` один раз в `finally`. Минусы: нужно знать K/V типы
заранее, нет `@Json`-сахара (сериализатор передаёшь через ProducerConfig напрямую).

### Способ 3: `kafka(producer).publish/publishAsync` ad-hoc

Когда из одного скрипта пишешь в **разные** топики и handle на каждый — оверкилл:

```kotlin
kafka(producer).publish("orders.resync", key, value)
kafka(producer).publish("orders.audit",  key, auditEvent)

// Или async:
val future = kafka(producer).publishAsync("orders.resync", key, value)
```

Sync-вариант блокируется на ack, async — возвращает `CompletableFuture`. Оба под dry-run
ничего не отправляют, инкрементят `report.dryRunSkipped["kafka.publish"]`.

### Что выбирать

| Сценарий | Способ |
|---|---|
| Один скрипт, один топик, типизированный value (`@Json`) | **1** — `@KafkaPublisher.Topic` + `mutation` |
| Высокая нагрузка, один топик, нужен явный flush-handle | **2** — raw `Producer` + `topic(...)` |
| Несколько топиков ad-hoc, без оверкилла | **3** — `kafka(producer).publish(...)` |

---

## 13. HTTP — типизированный клиент + `mutation` + `http(call)`

Есть три способа сделать HTTP-вызов из миграции.

### Способ 1: типизированный Kora `@HttpClient` (рекомендуемый)

```kotlin
@HttpClient(configPath = "httpClient.enrichment")
interface EnrichmentService {
    @HttpRoute(method = "GET", path = "/customers/{id}/status")
    fun status(@Path("id") id: String): StatusResponse
}

// В migrate:
val s = enrichment.status(customerId)
```

Это **read** — никаких обёрток. Под dry-run всё равно выполнится.

Для **write** оборачивай в `mutation`:

```kotlin
mutation("auth.refresh", args = mapOf("userId" to userId)) {
    auth.refreshToken(userId)
}
```

Под dry-run [мутация](../core/src/main/kotlin/io/migration/Mutation.kt) **не выполнится**, в лог пойдёт
`INFO [DRY-RUN] mutation:auth.refresh (userId=42)`, в отчёт — `report.dryRunSkipped["mutation:auth.refresh"]` (агрегат
по константному label'у; если бы label включал `$userId`, в отчёте было бы по записи на каждого юзера).

### Способ 2: функциональный `http(call)`

Если типизированного клиента нет (legacy / нет OpenAPI):

```kotlin
val client = HttpClient.newHttpClient()
val call: HttpCall = { method, path, body, headers ->
    val req = HttpRequest.newBuilder(URI.create("$baseUrl$path")).method(method, HttpRequest.BodyPublishers.ofByteArray(body ?: ByteArray(0))).build()
    client.send(req, HttpResponse.BodyHandlers.discarding()).statusCode()
}

// В migrate:
http(call).post("/v1/users/42/refresh", body = ...)
http(call).get("/v1/users/42")
```

Write-методы (`post/patch/put/delete`) идут через dry-run gate автоматом. Read (`get`) — нет.

---

## 14. Конфигурация HOCON

Все поля опциональные, кроме `migration.run`.

```hocon
migration {
  # Имя миграции к запуску. null = runner idle (полезно когда сервис ещё и API хостит).
  run = ${?MIGRATION_RUN}

  # Dry-run: все write-операции пропускаются, в отчёт идёт разбивка skipped writes.
  dryRun = false
  dryRun = ${?MIGRATION_DRY_RUN}

  # Папка для артефактов. null → logs/${migration.run}.
  outputFolder = ${?MIGRATION_OUTPUT_FOLDER}

  defaults {
    onUnhandled    = FAIL_FAST           # FAIL_FAST | LOG_AND_COMPLETE
    errorThreshold = 0                    # >0 — exit 1 если skipped > threshold
    progressEvery  = 1000                 # период дефолтного Progress.Default
    parallel       = 1                    # дефолтный размер ThreadPool runner'а; подними под параллельный forEach
  }

  errorReporting {
    includeStackTrace = true              # writeать ли стектрейсы в errors.log
    maxItemReprLength = 500               # обрезать itemRepr в errors.csv до этой длины
  }

  report {
    asciiOnly = false                     # для CI без UTF-8 терминала
  }
}
```

Env-overrides через `${?VAR}` — стандартный HOCON-синтаксис.

### Кастомный конфиг скрипта

Свои настройки — отдельная секция, через `@ConfigSource`:

```hocon
sample {
  batchSize = 200
  parallel  = 8
}
```

```kotlin
@ConfigSource("sample")
data class SampleConfig(
    var batchSize: Int,
    var parallel: Int,
)
```

`var` — Kora требует setter'ов. Поля без default'ов — обязательные, отсутствие в HOCON
= ошибка на старте.

---

## 15. Dry-run

```bash
MIGRATION_RUN=SAMPLE-001 MIGRATION_DRY_RUN=true ./gradlew run
```

Что происходит:
- **Read-операции** (`jdbc.query/stream`, `cassandra.query/stream`, `readCsv`, типизированный
  `@HttpClient` GET) — выполняются обычно.
- **Write через DSL** (`jdbc.execute`, `cassandra.execute`, `kafka.publish`, `topic.send`,
  `mutation { }`, `http(call).post`) — **не** выполняются. В лог `INFO [DRY-RUN] <label> ...`,
  в отчёт `report.dryRunSkipped[label] += 1`.
- **`openCsv` файлы** — пишутся (диагностический артефакт).

В отчёте под dry-run появится дополнительная строка:
```
  ⌀ Dry-run skipped writes: (jdbc.execute: 1947000, kafka.publish: 973500, mutation:auth.refresh: 147)
```

Из этой строки видно: сколько именно UPDATE/INSERT улетело бы в БД, сколько в Kafka, и т.д.
Готовая оценка масштаба перед боевым прогоном.

**Что НЕ под dry-run-gate:**
- Кастомный код, который ты пишешь сам без обёрток (например, вручную создаёшь `KafkaProducer`
  и шлёшь). Не делай так. Оборачивай в `mutation("...") { ... }` или используй DSL-функции.

---

## 16. `outputFolder` и артефакты прогона

```
logs/SAMPLE-001/
├── migration.log          # slf4j root logger — всё, что писалось в JVM за время прогона
├── errors.csv             # авто-аудит item-уровневых ошибок
├── errors.log             # стектрейсы по тем же ошибкам
├── processed.csv          # твои openCsv(...) — каждый свой файл
├── failed.csv
└── ...
```

Папка определяется так (в порядке приоритета):

1. `migration.outputFolder` в HOCON.
2. Env-override `MIGRATION_OUTPUT_FOLDER`.
3. Дефолт `logs/${migration.name}` относительно JVM CWD.

Создаётся (`mkdir -p`) на старте runner'ом. Все файлы внутри `TRUNCATE`-аются при ререн —
один прогон = один набор артефактов. Если нужна история — заархивируй папку после прогона
(или подставь timestamped путь через env).

В `migrate()` доступна как `outputFolder: Path` — удобно для своих файлов:

```kotlin
val customPath = outputFolder.resolve("custom-stuff.json")
Files.writeString(customPath, json)
```

### `migration.log` — capture-all slf4j

Runner программно подключает logback `FileAppender` к **root**-логгеру. То есть в файле окажется
не только наш `log.info/...`, но и весь output Kora, Hikari, Kafka producer, Cassandra driver
и т.д. Это сознательное решение в пользу compliance: хочется иметь полный аудит того, что
происходило в JVM.

Если logback **не на classpath** (нестандартная конфигурация slf4j) — runner залогирует warn
и продолжит без файла. На прод стоит держать `logback-classic` в `runtimeOnly` зависимостях.

---

## 17. `errors.csv` и `errors.includeItem<T>`

Когда `forEach` получает исключение, оно через [`OnError`](#6-onerror--политика-обработки-ошибок)
может уйти в SKIP. Тогда item пишется в `errors.csv`:

```csv
timestamp,migration,author,itemRepr,errorClass,errorMessage
2026-05-15T13:42:11Z,SAMPLE-001,team,"Order(id=42, customerId=cust-1)",NetworkException,timeout
```

`itemRepr` по умолчанию — `item.toString()`. Для data class это нормально, для строк-id
тоже. Для `Map`-объектов и больших структур — не очень.

**Кастомный сериализатор** — `errors.includeItem<T> { it -> "репрезентация" }`:

```kotlin
override fun MigrationContext.migrate() {
    errors.includeItem<Order> { "order=${it.id}, customer=${it.customerId}" }
    errors.includeItem<String> { "customer_id=$it" }
    // ...
}
```

Регистрация — на любом этапе `migrate()`, до того как пойдут ошибки. Lookup при записи
идёт сначала точно по классу, потом по супертипам и интерфейсам — `includeItem<Map>`
сработает и для `LinkedHashMap`.

`errors.log` параллельно содержит стектрейсы (включается `errorReporting.includeStackTrace = true`,
дефолт).

---

## 18. Итоговый отчёт

В конце прогона runner печатает в лог:

```
═══════════════════════════════════════════════════════════
Migration: SAMPLE-001  (author: team)
Started:   2026-05-15 13:40:00
Finished:  2026-05-15 13:47:15
Duration:  7m 15s
Mode:      REAL
───────────────────────────────────────────────────────────
Processed:                 8 393
  ✓ Successful:            8 380
  ⊘ Skipped (errors):          13
  ✗ Failed:                     0
───────────────────────────────────────────────────────────
Error details:  logs/SAMPLE-001/errors.csv  (13 rows)
Error traces:   logs/SAMPLE-001/errors.log
═══════════════════════════════════════════════════════════
```

Под dry-run:
```
  ⌀ Dry-run skipped writes: (jdbc.execute: 16 842, kafka.publish: 8 421)
```

Если при закрытии ресурсов что-то падало — появится блок:
```
  ⚠ Warnings:
    - resource close failed: KafkaTopic(orders.resync) (TimeoutException: flush timed out)
```

Warnings — нефатальные. На exit-code не влияют. Появляются только если у тебя в скрипте есть
ресурсы с проблемным `close()`.

В CI без UTF-8 терминала включай `migration.report.asciiOnly = true`.

---

## 19. `ScriptPolicy` — что делать при неожиданном падении

Если внутри `migrate()` (за пределами любого `forEach`) вылетит исключение, поведение зависит
от параметра `onUnhandled` в `Migration(...)`.

```kotlin
class Task : Migration(
    name = "TASK-X",
    author = "you",
    onUnhandled = ScriptPolicy.LOG_AND_COMPLETE,  // дефолт FAIL_FAST
)
```

- **`FAIL_FAST`** (дефолт) — залогировать, инкрементить `report.failed`, exit-code 1. Уместно
  когда «не пишем дальше».
- **`LOG_AND_COMPLETE`** — залогировать, инкрементить `report.failed`, **дойти до конца**
  (закрыть ресурсы, напечатать отчёт), вернуть exit-code 0. Уместно для compliance-сценариев:
  хоть какой-то отчёт нужнее, чем абортированный прогон.

**Не путать с `OnError`**: `ScriptPolicy` — для исключений за пределами `forEach`. Item-уровневые
ошибки внутри `forEach` обрабатывает `OnError`.

---

## 20. Тестирование скриптов

DSL даёт фабрику для unit-тестов без поднятия Kora-графа:

```kotlin
class Task42000Test {

    @Test
    fun `обрабатывает все stuck-заказы`(@TempDir tmp: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = tmp)

        val task = SampleMigration(mockDb, mockProducer, mockConfig)
        with(ctx) { task.run { migrate() } }
        ctx.closeRegistered()  // явный close, runner-а нет в тестах

        // assertions: проверяем что попало в файлы, в Kafka mock, в report
        val report = ctx.report.build()
        assertThat(report.successful).isEqualTo(10)
        assertThat(Files.readAllLines(tmp.resolve("processed.csv"))).hasSize(11) // header + 10 rows
    }
}
```

`DefaultMigrationContext.test(...)`:
- Создаёт `CsvFileErrorReporter` и `ReportBuilder`.
- `executor` — `ForkJoinPool.commonPool()` (shared, daemon threads, без shutdown). Для
  тестов с `parallel = 1` не задействуется — `forEach` обходит executor. Для тестов с
  `parallel > 1` обработка идёт в `commonPool` параллельно (не детерминированно). Если
  нужен кастомный executor (например, `Executors.newSingleThreadExecutor()` для строго
  последовательного прогона) — собирай контекст напрямую через
  `DefaultMigrationContext.internalCreate(...)`.
- `outputFolder` — если не передал, создаст в системной tmp.

Для **integration-тестов** реальных операций (JDBC/Cassandra/Kafka) — используй Testcontainers,
как сделано в `kora/src/test/.../SqlOpsIntegrationTest.kt` и пр.

---

## 21. Exit-коды

| Код | Что значит |
|---|---|
| `0` | Success. Включая `LOG_AND_COMPLETE`-сценарии (где было unhandled-исключение, но runner довёл до конца). |
| `1` | `FAIL_FAST` от unhandled-исключения **или** `errorThreshold` превышен. |
| `2` | Misconfiguration: unknown migration name, дубликат имён в графе. |

Прод-инфраструктура (CI/CD, K8s Job) должна читать exit-code и решать ретраить/алертить.

---

## 22. Шпаргалка-FAQ

**Q: Как пропустить миграцию через env, не правя HOCON?**
A: `MIGRATION_RUN= ./gradlew run` (пустое значение = `null` через `${?MIGRATION_RUN}`).

**Q: Почему `transactional` не работает по сценарию X?**
A: До v0.2 это был маркер без реального scope. С v0.2 — real tx через `db.inTx`. Если ставишь
свежую либу — должно работать.

**Q: Как кастомизировать ThreadFactory у `forEach`?**
A: Опубликуй свой `Executor`-бин с `@Tag(MigrationExecutor::class)`. Runner подхватит.

**Q: Как делать миграцию idempotent на повторе?**
A: На уровне SQL — `INSERT ... ON CONFLICT DO NOTHING`, `WHERE NOT EXISTS`. На уровне DSL —
запиши `processed_ids.csv` сверху, читай при старте, фильтруй input по уже-обработанным.

**Q: Можно ли запускать миграцию в Docker WORKDIR=/app — куда упадут логи?**
A: Дефолт `logs/TASK-X` → `/app/logs/TASK-X`. Можно поменять через `MIGRATION_OUTPUT_FOLDER=/var/log/migrations/$RUN_ID`.

**Q: На большой выгрузке OOM на `query` — что делать?**
A: Переходи на `jdbc(db).stream("...", mapper = ...) { rows -> forEach(rows, ...) { ... } }`.
`stream` — true JDBC cursor через `setFetchSize` (дефолт 1000), строки тянутся страницами,
не материализуются целиком. `rows: Sequence<T>` валиден только **внутри** callback'а
`consume` — см. §10.

**Q: Как два кластера Cassandra держать в одном скрипте?**
A: Два разных `CqlSession` в графе с `@Tag`, `@Tag(Primary::class) primary: CqlSession` в
конструкторе скрипта. Дальше — `cassandra(primary).query(...)`, `cassandra(replica).query(...)`.

**Q: Что если в одном скрипте нужно несколько Kafka producer (разные кластеры)?**
A: Два разных `Producer<...>` бина с `@Tag`, конструктор скрипта — два параметра, два `topic(p1, "...")`
и `topic(p2, "...")` сверху `migrate()`.

**Q: `mutation { }` и retry совместимы?**
A: В DSL item-level retry нет (см. §6 «Retry — на другом уровне»). Если хочешь ретраить
сам publish / HTTP-call внутри `mutation` — навешивай `@Retry` на типизированный
Kora-метод, который вызывается из `mutation`-блока. Backoff и classify-логика — из коробки;
`mutation` остаётся dry-run-gate'ом без вмешательства в retry.

**Q: А что если внутри `transactional { }` сделать `mutation { }`?**
A: Можно, **но осторожно**. `mutation` — это просто dry-run gate, никакой связи с tx
state machine. Side-effect внутри `mutation` (Kafka publish, HTTP-вызов, и т.д.) **выполняется
inline**, до коммита tx. Если потом tx откатится — JDBC-изменения rollback'нутся, **а Kafka
сообщение уже улетело**. Это правильное поведение только если ты явно учитываешь это
(например, outbox pattern: пишешь в outbox-таблицу внутри tx, потом отдельный воркер
читает outbox и публикует в Kafka). Для прямой связки tx+publish стандартный совет —
**не делать publish внутри tx**, выноси за пределы.

**Q: Тесты подтягивают `logback-classic`. А в проде нужно?**
A: Да. Иначе runner залогирует warn `Logback не на classpath — migration.log file output отключён`
и `migration.log` не появится. Лучше держать `runtimeOnly("ch.qos.logback:logback-classic:...")`.

**Q: Хочу свой S3-клиент / Redis / etc — как добавить в DSL?**
A: Пишешь свой extension `MigrationContext.myOps(client)` с обёрткой над `guardWrite` для
write-операций. Подробно с примерами — [examples/customization.md](examples/customization.md).

**Q: Хочу свой Executor с MDC / метриками — как?**
A: `@Component class { @Tag(MigrationExecutor::class) fun executor(): Executor = ... }`.
Runner подхватит. См. [examples/customization.md §3](examples/customization.md#3-кастомный-executor-для-foreach).

**Q: Хочу свой формат отчёта об ошибках (JSON / Kibana) вместо CSV — как?**
A: Сложнее. `CsvFileErrorReporter` сейчас захардкожен в `MigrationRunner.init()`. Если нужно
— можно сделать decorator над `DefaultMigrationContext` через свой `internalCreate`-аналог
(`internalCreate` публичный). Открой issue или подумаем как сделать pluggable.
