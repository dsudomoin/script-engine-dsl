# Showcase: все возможности Migration DSL в одном скрипте

**Задача.** По тикету SAMPLE-001 команда compliance потребовала ресверку клиентской
базы: часть клиентов «провисли» в неопределённом статусе после январского
инцидента с legacy-хранилищем. Нужно пройтись по ним и исправить: подтянуть
актуальный профиль из старой Cassandra, обогатить через AML-сервис, сохранить
исправления в Postgres (транзакционно), опубликовать событие в Kafka, а для
клиентов, получивших VIP-статус — дёрнуть webhook в legacy-systems.

Ошибки бывают двух типов на уровне DSL: business-validation (skip + аудит
в CSV), critical (fail). Transient-ошибки сети (5xx, timeout, connection reset)
ретраятся на уровне Kora `@Retry` внутри типизированного HTTP-клиента — до
`forEach`-классификатора попадает уже финальный fail после исчерпания retries.
Глобально — в `LOG_AND_COMPLETE`-режиме, чтобы compliance получил любой отчёт
даже если скрипт поймает что-то неожиданное в середине.

Это сценарий, в котором осмысленно задействованы **все** примитивы DSL.

---

## Что этот скрипт демонстрирует (галочкой прокатимся в конце)

| Примитив | Где в коде |
|---|---|
| Postgres JDBC + `transactional { }` | update профиля + insert в audit (один tx, реальный rollback) |
| Cassandra stream (для больших данных) | profile lookup из legacy |
| Kafka `@KafkaPublisher.Topic` + `mutation { publisher.send(...) }` | событие реcверки (идиоматичный путь Kora) |
| Kora `@HttpClient` для read | AML-сервис |
| `mutation("label") { ... }` для ad-hoc write | webhook в legacy-systems |
| CSV read input | список клиентов к пересверке |
| CSV write × 3 (`openCsv` × 3) | processed / failed / vip-upgrades (handle сверху, auto-close) |
| `OnError.handle { e, item -> Skip/Fail }` | кастомная классификация |
| Kora `@Retry` на HTTP-клиенте AML | linear backoff на transient 5xx/timeout — внутри клиента, не на уровне DSL |
| `forEach(chunk, parallel, onError, logEach, progress)` | все параметры форъача |
| `Progress.Custom(n) { ... }` | кастомный форматтер прогресса |
| `errors.includeItem<Customer> { ... }` | per-type сериализация для CSV аудита |
| `ScriptPolicy.LOG_AND_COMPLETE` | завершиться штатно даже при unhandled throw |
| Dry-run прозрачно | ни одного `if (dryRun)` во всём файле |

---

## 1. HOCON-конфигурация

```hocon
migration {
  run = ${?MIGRATION_RUN}
  dryRun = ${?MIGRATION_DRY_RUN}
  # outputFolder опционально — по умолчанию logs/${migration.run}
  outputFolder = ${?MIGRATION_OUTPUT_FOLDER}
  defaults {
    onUnhandled   = LOG_AND_COMPLETE   # compliance всё равно хочет отчёт
    errorThreshold = 500               # если >500 провалов — прерваться
    progressEvery  = 1000
  }
  errorReporting {
    maxItemReprLength = 250
  }
}

db {
  jdbcUrl  = "jdbc:postgresql://pg.prod:5432/customers"
  username = ${DB_USER}
  password = ${DB_PASSWORD}
}

cassandra.legacy {
  contactPoints    = ["legacy-ca.prod:9042"]
  localDatacenter  = "dc1"
  keyspace         = "customer_legacy"
}

kafka.customers.publisher {
  driverProperties { "bootstrap.servers" = ${KAFKA_BOOTSTRAP} }
  resyncTopic { topic = "customers.resync" }
}

httpClient.aml {
  url = ${AML_URL}
  requestTimeout = 3s
}

httpClient.legacySystems {
  url = ${LEGACY_SYSTEMS_URL}
  requestTimeout = 5s
}

sample {
  inputFile       = "input/customers-to-resync.csv"
  batchSize       = 500
  parallel        = 8
  vipThreshold    = 1000000   # amount в копейках
  initialDate     = null      # фильтр "startedAt >= initialDate"; null = все
  initialDate     = ${?TASK50000_INITIAL_DATE}
}
```

## 2. Конфиг скрипта + DTO

```kotlin
// SampleConfig.kt
package com.example.migrations

import ru.tinkoff.kora.config.common.annotation.ConfigSource
import java.nio.file.Path
import java.time.Instant
import kotlin.time.Duration

@ConfigSource("sample")
data class SampleConfig(
    var inputFile: String,
    var batchSize: Int,
    var parallel: Int,
    var vipThreshold: Long,
    var initialDate: Instant?,
)
```

Поле `outputFolder` тут не нужно: ему теперь владеет сам runner (см. блок `migration.outputFolder` сверху). В `migrate()` он доступен как `outputFolder` (член `MigrationContext`), а `openCsv("file.csv", ...)` резолвит относительно него.

```kotlin
// Customer.kt
package com.example.migrations

import java.time.Instant

data class Customer(
    val id: String,
    val email: String,
    val status: CustomerStatus,
    val balance: Long,      // копейки
    val startedAt: Instant,
)

enum class CustomerStatus { ACTIVE, INACTIVE, SUSPENDED, VIP }

data class CustomerResyncEvent(val id: String, val oldStatus: CustomerStatus, val newStatus: CustomerStatus)
```

## 3. Kora HTTP-клиент AML

```kotlin
// AmlService.kt
package com.example.migrations

import ru.tinkoff.kora.http.client.common.annotation.HttpClient
import ru.tinkoff.kora.http.common.annotation.HttpRoute
import ru.tinkoff.kora.http.common.annotation.Path

@HttpClient(configPath = "httpClient.aml")
interface AmlService {

    @HttpRoute(method = "GET", path = "/verify/{customerId}")
    @Retry("httpClient.aml.verify")             // имя конфига; параметры в HOCON ниже
    fun verify(@Path("customerId") customerId: String): VerificationResult

    data class VerificationResult(
        val approved: Boolean,
        val risk: String,       // LOW / MEDIUM / HIGH
        val reason: String?,
    )
}
```

Соответствующий HOCON-блок (Kora resilient = linear backoff, exponential встроенно нет):

```hocon
resilient.retry.httpClient.aml.verify {
  delay     = "250ms"
  attempts  = 3                # 3 retry после оригинала → 4 попытки всего
  delayStep = "250ms"          # → waits: 250ms, 500ms, 750ms
}
```

`LegacySystemsClient` — такой же: `@HttpClient` интерфейс с методом `POST /webhooks/vip-upgrade`. Показан в использовании через `mutation { }` (код ниже).

### Kora @KafkaPublisher для resync-топика

```kotlin
// CustomerResyncPublisher.kt
package com.example.migrations

import org.apache.kafka.clients.producer.RecordMetadata
import ru.tinkoff.kora.kafka.common.annotation.KafkaPublisher
import ru.tinkoff.kora.json.common.annotation.Json

@KafkaPublisher("kafka.customers.publisher")
interface CustomerResyncPublisher {

    @KafkaPublisher.Topic("kafka.customers.publisher.resyncTopic")
    fun publishResync(key: String, @Json value: CustomerResyncEvent): RecordMetadata
}
```

Имя топика приходит из HOCON `kafka.customers.publisher.resyncTopic.topic` — там оно
переопределяемо без правок кода. Kora генерирует реализацию интерфейса при сборке.

## 4. Исключения, которые скрипт классифицирует

```kotlin
// Exceptions.kt
package com.example.migrations

class AmlValidationException(message: String) : RuntimeException(message)
class DataIntegrityException(message: String) : RuntimeException(message)
```

Это доменные типы, которые ты бросаешь в своём коде (или прокидываешь
из драйверов/клиентов). Ниже DSL будет на них смотреть и решать,
что делать.

## 5. Сам скрипт

```kotlin
// SampleMigration.kt
package com.example.migrations

import com.datastax.oss.driver.api.core.CqlSession
import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.Progress
import io.github.dsudomoin.migration.ScriptPolicy
import io.github.dsudomoin.migration.csv.openCsv
import io.github.dsudomoin.migration.csv.readCsv
import io.github.dsudomoin.migration.forEach
import io.github.dsudomoin.migration.kora.ops.cassandra
import io.github.dsudomoin.migration.kora.ops.jdbc
import io.github.dsudomoin.migration.kora.ops.transactional
import io.github.dsudomoin.migration.mutation
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.common.Tag
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import java.util.concurrent.atomic.AtomicInteger

@Component
class SampleMigration(
    private val pg: JdbcConnectionFactory,
    @Tag(LegacyCluster::class) private val legacy: CqlSession,
    private val aml: AmlService,
    private val legacySystems: LegacySystemsClient,
    private val publisher: CustomerResyncPublisher,
    private val config: SampleConfig,
) : Migration(
    name = "SAMPLE-001",
    author = "team",
    onUnhandled = ScriptPolicy.LOG_AND_COMPLETE,
) {

    private val vipUpgrades = AtomicInteger()

    override fun MigrationContext.migrate() {
        errors.includeItem<Customer> { "id=${it.id}, email=${it.email}, status=${it.status}, balance=${it.balance}" }

        val processed = openCsv("processed.csv",    "id", "oldStatus", "newStatus")
        val failed    = openCsv("failed.csv",       "id", "reason")
        val vips      = openCsv("vip-upgrades.csv", "id", "balance")

        val inputIds = readCsv(config.inputFile) { it["customer_id"]!! }.toList()

        forEach(
            inputIds,
            chunk = config.batchSize,
            parallel = config.parallel,
            onError = OnError.handle { e, _ ->
                when (e) {
                    is AmlValidationException  -> OnError.Decision.Skip
                    is DataIntegrityException  -> OnError.Decision.Fail
                    else                       -> OnError.Decision.Skip
                }
            },
            onErrorLog = { e, batch -> log.warn("batch of ${batch.size} hit ${e.javaClass.simpleName}") },
            logEach = { batch -> "processed batch size=${batch.size}, first=${batch.first()}" },
            progress = Progress.Custom(10) { done, total ->
                "resync: batches=$done/${total ?: "?"}  vipUpgrades=${vipUpgrades.get()}"
            },
        ) { batch ->

            val customers = cassandra(legacy).stream(
                "select id, email, status, balance, started_at from customers where id in :ids",
                "ids" to batch,
            ) { row ->
                Customer(
                    id = row.getString("id")!!,
                    email = row.getString("email")!!,
                    status = CustomerStatus.valueOf(row.getString("status")!!),
                    balance = row.getLong("balance"),
                    startedAt = row.getInstant("started_at")!!,
                )
            }.toList()

            val enriched = customers.map { c ->
                if (c.status == CustomerStatus.INACTIVE) {
                    c to null
                } else {
                    val check = aml.verify(c.id)
                    if (!check.approved) throw AmlValidationException("AML rejected ${c.id}: ${check.reason}")
                    c to check.risk
                }
            }

            transactional(jdbc(pg)) {
                enriched.forEach { (c, risk) ->
                    val newStatus = computeNewStatus(c, risk)
                    execute(
                        "update customers set status = :s, last_resync = now() where id = :id",
                        "s" to newStatus.name, "id" to c.id,
                    )
                    execute(
                        "insert into customers_audit(customer_id, from_status, to_status, ts) values (:id, :f, :t, now())",
                        "id" to c.id, "f" to c.status.name, "t" to newStatus.name,
                    )
                    processed.row(c.id, c.status.name, newStatus.name)
                    mutation("customers.resync", args = mapOf("customerId" to c.id)) {
                        publisher.publishResync(c.id, CustomerResyncEvent(c.id, c.status, newStatus))
                    }
                    if (newStatus == CustomerStatus.VIP && c.balance >= config.vipThreshold) {
                        vips.row(c.id, c.balance)
                        vipUpgrades.incrementAndGet()
                        mutation("legacy.vip-upgrade", args = mapOf("customerId" to c.id)) {
                            legacySystems.onVipUpgrade(c.id)
                        }
                    }
                }
            }
        }
    }

    private fun computeNewStatus(c: Customer, risk: String?): CustomerStatus = when {
        risk == "HIGH"                        -> CustomerStatus.SUSPENDED
        c.balance >= config.vipThreshold      -> CustomerStatus.VIP
        c.status == CustomerStatus.INACTIVE   -> CustomerStatus.INACTIVE
        else                                  -> CustomerStatus.ACTIVE
    }
}

annotation class LegacyCluster
```

Глубина indent'а в горячей петле — 4 (`forEach { transactional { enriched.forEach { if { mutation {} } } } }`). Никакой лесенки `csv → csvSink → csvSink → batchPublish` вокруг `forEach`.

## 6. Разбор — что где происходит

### 6.1. Script-level policy: `LOG_AND_COMPLETE`

```kotlin
onUnhandled = ScriptPolicy.LOG_AND_COMPLETE
```

Если в середине скрипта вылетит что-то совсем неожиданное (например, `OutOfMemoryError` или баг
в `computeNewStatus`), runner не упадёт с exit-code 1 — залогирует ошибку, зафиксирует в `report.failed`,
закроет файлы, напечатает отчёт, вернёт exit-code 0. Для compliance-скрипта это критично:
хоть какой-то отчёт лучше, чем ничего.

Если бы мы хотели fail-fast — убрали бы параметр (`FAIL_FAST` — дефолт).

### 6.2. Кастомный per-type сериализатор ошибок

```kotlin
errors.includeItem<Customer> { "id=${it.id}, email=${it.email}, status=${it.status}, balance=${it.balance}" }
```

Первой строкой `migrate()` регистрируем: если в `errors.csv` попадёт item типа `Customer` —
записывать его **так**, а не через `toString()`. Работает и для `LinkedHashMap<String, Any>` через супер-lookup, если сериализатор зарегистрирован на `Map`.

### 6.3. Декларативно открытые CSV-outputs

```kotlin
val processed = openCsv("processed.csv",    "id", "oldStatus", "newStatus")
val failed    = openCsv("failed.csv",       "id", "reason")
val vips      = openCsv("vip-upgrades.csv", "id", "balance")
```

`openCsv(filename, vararg headers)` резолвит имя файла относительно
`ctx.outputFolder` (`logs/SAMPLE-001/processed.csv` и т.д.). Если нужен абсолютный
путь — есть перегрузка `openCsv(path: Path, vararg headers)`.

Три `CsvOutput`-handle, объявлены сверху `migrate()`, регистрируются в
`MigrationContext` как `AutoCloseable`. Header пишется сразу при открытии.
Дальше `processed.row(...)`, `failed.row(...)`, `vips.row(...)` — где угодно
в теле миграции (включая `forEach`-воркеры — `row(...)` thread-safe). Закрывает
их runner в `finally` после возврата из `migrate()`, в обратном порядке
регистрации.

Под dry-run файлы **всё равно пишутся** (решение спеки v0.1.0 §4.1): скрипт под
`--dry-run` остаётся диагностическим артефактом. Никакой лесенки `csv { csvSink { ... } }`.

### 6.4. Kafka через `@KafkaPublisher.Topic` + `mutation`

```kotlin
mutation("customers.resync", args = mapOf("customerId" to c.id)) {
    publisher.publishResync(c.id, CustomerResyncEvent(c.id, c.status, newStatus))
}
```

`CustomerResyncPublisher` — типизированный Kora `@KafkaPublisher`-интерфейс
(см. §3). Реализация генерируется Kora, имя топика берётся из HOCON-блока
`kafka.customers.publisher.resyncTopic.topic`. `@Json` сериализация — автоматом.

Сам `publishResync(...)` Kora не знает про dry-run — поэтому оборачиваем в
`mutation(...) { ... }`. Под dry-run блок **не выполняется**, в лог идёт
`INFO [DRY-RUN] mutation:customers.resync (customerId=CUST-4277)`, в отчёт —
инкремент `report.dryRunSkipped["mutation:customers.resync"]`. Label —
константа, args — диагностика; в финальном отчёте получаем чистый агрегат
`mutation:customers.resync: 973500`.

Альтернативный путь (raw `Producer<K,V>` через `@Tag(Publisher::class)` +
`topic(producer, name)` handle) описан в [USER_GUIDE §12](../USER_GUIDE.md#12-kafka--topicproducer-name-и-kafkaproducerpublish) — для high-throughput сценариев с явным flush-handle.

### 6.5. forEach на всех параметрах

```kotlin
forEach(
    inputIds,
    chunk = 500,                 // батчи по 500 id
    parallel = 8,                // 8 рабочих потоков
    onError = OnError.handle { e, _ -> ... },
    onErrorLog = { e, batch -> log.warn("...") },
    logEach = { batch -> "processed batch size=${batch.size}, first=${batch.first()}" },
    progress = Progress.Custom(10) { done, total -> "resync: batches=$done/${total ?: "?"}  vipUpgrades=${vipUpgrades.get()}" },
) { batch -> ... }
```

- `chunk = 500` — block получает `List<String>` (batch из 500 id) вместо одиночного id.
- `parallel = 8` — 8 потоков обрабатывают разные батчи одновременно. На 1M клиентов и батче 500 — это ~2000 батчей на 8 потоках, вытянет за разумное время.
- `OnError.handle { ... }` — свой классификатор. Принимает решение **один раз** (`Decision.Skip` / `Decision.Fail`) по типу исключения. Retry в DSL нет — для transient-ошибок навешивай `@Retry` на сам Kora-клиент (см. AML-секцию выше).
- `onErrorLog` — свой дополнительный лог в дополнение к авто-аудиту в CSV.
- `logEach` — лог после каждого успешно обработанного батча.
- `progress = Progress.Custom(10)` — каждые 10 батчей кастомный форматтер тянет текущее значение `vipUpgrades` и печатает.

### 6.6. Cassandra stream вместо query

```kotlin
val customers = cassandra(legacy).stream("select ... where id in :ids", "ids" to batch) { ... }.toList()
```

`.stream(...)` возвращает `Sequence<T>` — не загружает всё в память разом.
На большинстве легаси Cassandra-кластеров это единственный способ вытянуть
крупный `WHERE ... IN` без таймаута. `.toList()` материализует в конце,
потому что батч мы обрабатываем транзакционно — нужно полное знание.

Для миллиона клиентов можно было бы не `.toList()`, а работать со
стримом напрямую — но тогда транзакция пришлось бы открывать per-item.

### 6.7. JDBC transactional scope

```kotlin
transactional(jdbc(pg)) {
    enriched.forEach { (c, risk) ->
        execute("update customers ...")
        execute("insert into customers_audit ...")
        ...
    }
}
```

Для каждого клиента внутри батча — пара «update профиля + insert в аудит-журнал» в одной
транзакции. `transactional` открывает один `Connection` через `JdbcConnectionFactory.inTx`,
создаёт tx-bound `SqlOps`, все `execute/query` внутри блока используют этот единственный
connection. На успехе — `commit`; на исключении — `rollback`. Вложенный `transactional`
бросает `IllegalStateException` (см. спеку v0.2.0 §5.4). До v0.2.0 `transactional` был
маркером без реального scope — теперь это настоящий транзакционный блок.

### 6.8. `mutation { }` — side-effect за пределами DSL-источников

```kotlin
mutation("legacy.vip-upgrade", args = mapOf("customerId" to c.id)) {
    legacySystems.onVipUpgrade(c.id)
}
```

`legacySystems` — типизированный Kora `@HttpClient`. Наш DSL не может встроить
dry-run-gate в сгенерированный Kora'ой код клиента, поэтому пользователь
оборачивает write-вызов в `mutation("label", args = ...) { ... }`. При dry-run содержимое
**не выполняется**, в лог идёт `INFO [DRY-RUN] mutation:legacy.vip-upgrade (customerId=CUST-4277)`.

Label **константный** (`"legacy.vip-upgrade"`) — это критично: в `report.dryRunSkipped` получим
чистый агрегат `mutation:legacy.vip-upgrade: 147`. Если бы label содержал `${c.id}` —
в отчёте было бы 147 уникальных записей, бесполезно. Контекст item'а идёт в `args`.

### 6.9. Классификация ошибок через `OnError.handle`

```kotlin
onError = OnError.handle { e, _ ->
    when (e) {
        is AmlValidationException  -> OnError.Decision.Skip
        is DataIntegrityException  -> OnError.Decision.Fail
        else                       -> OnError.Decision.Skip
    }
}
```

- **AML-валидация** (клиент не прошёл compliance-проверку) → `Skip`. Запись в `failed.csv` (через авто-аудит) + `report.skipped++`. Скрипт продолжает.
- **DataIntegrity** (БД вернула что-то невозможное) → `Fail`. Прерываем весь скрипт — дальше смотреть бесполезно, это баг.
- Всё остальное — `Skip`. Не валим миграцию на том, чего не ожидали; разберёмся по отчёту.

`Handle` принимает решение **один раз** по типу ошибки. Retry в DSL нет: если хочется backoff на transient-ошибки сети — это уровень Kora `@Retry` внутри типизированного клиента (см. AML в §3 выше: `@Retry("httpClient.aml.verify")` + соседний HOCON-блок). До `Handle` тогда долетит только финальный fail после исчерпания retries.

### 6.10. Итоговый отчёт

```
═══════════════════════════════════════════════════════════
Migration: SAMPLE-001  (author: team)
Started:   2026-04-24 14:15:32
Finished:  2026-04-24 14:47:11
Duration:  31m 39s
Mode:      REAL
───────────────────────────────────────────────────────────
Processed:                 1 947
  ✓ Successful:            1 933
  ⊘ Skipped (errors):          14
  ✗ Failed:                     0
───────────────────────────────────────────────────────────
Error details:  logs/SAMPLE-001/errors.csv  (14 rows)
Error traces:   logs/SAMPLE-001/errors.log
═══════════════════════════════════════════════════════════
```

Процессов 1 947 — это **батчи**, не клиенты (размер батча 500, значит клиентов ~975k).
Если хочется видеть счётчик клиентов отдельно — в `logEach` или своём прогресс-форматтере пишем
что нужно (мы вывели `vipUpgrades=${vipUpgrades.get()}` в progress).

Под dry-run в отчёте появится дополнительный блок:
```
  ⌀ Dry-run skipped writes:    (jdbc.execute: 1947000, mutation:customers.resync: 973500, mutation:legacy.vip-upgrade: 147)
```

И по этой строчке видно: сколько именно UPDATE/INSERT ушло бы в Postgres, сколько сообщений в Kafka,
и сколько webhook'ов. 147 VIP-апгрейдов — красивая оценка масштаба без реального сайд-эффекта.

### 6.11. `outputFolder` — всё в одном месте

```
logs/SAMPLE-001/
├── migration.log          # slf4j root logger — всё что писалось в JVM во время миграции
├── errors.csv             # авто-аудит item-ошибок (CsvFileErrorReporter)
├── errors.log             # стектрейсы по тем же ошибкам
├── processed.csv          # наши openCsv() — у каждого свой файл
├── failed.csv
└── vip-upgrades.csv
```

Папка определяется конфигом `migration.outputFolder` (HOCON), env-override
`MIGRATION_OUTPUT_FOLDER`, или дефолтом `logs/${migration.name}` относительно
CWD JVM. Все файлы внутри `TRUNCATE`-аются при ререн-е (один прогон = один
набор артефактов). Если нужна история — заархивируй папку после прогона
(или подставь timestamped путь через env).

`migration.log` собирается через programmatic logback `FileAppender` на root-
логгере. То есть в файле окажется не только наш `log.info/...`, но и весь
output Kora, Hikari, Kafka producer, Cassandra driver и т.д. Это сознательное
решение в пользу compliance-сценариев: хочется иметь полный аудит всего, что
происходило в JVM во время миграции. Если logback не на classpath — runner
сообщит warn и пойдёт без file-логирования.

---

## 7. Вариации (мини-паттерны)

Ниже — короткие куски кода под разные конкретные случаи. Они не
складываются в один скрипт, но показывают, как DSL ложится на типичные
подзадачи.

### Чтение миллиона строк из Postgres стримом, без chunk

```kotlin
jdbc(orders).stream(
    "select id from orders where status = 'STUCK' order by id",
    fetchSize = 5000,
    mapper = { it.getLong("id") },
) { stuck ->
    forEach(stuck, parallel = 8, onError = OnError.Skip) { id ->
        // обработка по одному, без материализации списка
    }
}
```

`jdbc.stream` — true JDBC cursor через `setFetchSize`. `stuck: Sequence<Long>`
валиден только внутри `consume`-блока; `forEach` пишется прямо там.

### Linear backoff для нестабильной внешней API

Backoff — на уровне типизированного Kora-клиента, не на уровне `forEach`:

```kotlin
@HttpClient(configPath = "httpClient.sync")
interface SyncClient {
    @HttpRoute(method = "POST", path = "/sync")
    @Retry("httpClient.sync")               // имя конфига; параметры в HOCON
    fun sync(@Body item: ByteArray)
}

// В migrate:
forEach(items, parallel = 4, onError = OnError.Skip) { item ->
    mutation("sync", args = mapOf("id" to item.id)) { syncClient.sync(item.toJsonBytes()) }
}
```

```hocon
resilient.retry.httpClient.sync {
  delay     = "250ms"
  attempts  = 4                # 4 retry после оригинала → 5 попыток всего
  delayStep = "250ms"          # → waits: 250ms, 500ms, 750ms, 1000ms
}
```

В Kora resilient backoff линейный (`delay + (n-1)*delayStep`). Настоящего exponential
встроенно нет — либо мирись с linear (`delayStep` побольше — практически достаточно),
либо иди в императивный `RetryManager.get("name").retry(supplier)` со своим backoff.
После 5 неудач исключение вылетает в `forEach`, который аудитит item в `errors.csv`
через `OnError.Skip`.

### Кастомный прогресс с несколькими метриками

```kotlin
val ok = AtomicLong(); val skip = AtomicLong()

forEach(items, onError = OnError.Skip,
        progress = Progress.Custom(1000) { done, total ->
            "resync: done=$done/${total ?: "?"}  ok=${ok.get()}  skip=${skip.get()}  rate=${done / Math.max(1, (System.currentTimeMillis() - startMs) / 1000)}/s"
        }) { item ->
    try { doWork(item); ok.incrementAndGet() }
    catch (e: Exception) { skip.incrementAndGet(); throw e }
}
```

### Comparison архетип компактно

```kotlin
forEach(ids, chunk = 200, parallel = 4, onError = OnError.Skip) { batch ->
    val a = cassandra(primary).query("select v from t where id in :ids", "ids" to batch) { it.getString("v") }
    val b = cassandra(replica).query("select v from t where id in :ids", "ids" to batch) { it.getString("v") }
    batch.forEachIndexed { i, id -> if (a[i] != b[i]) row(id, "primary=${a[i]}, replica=${b[i]}") }
}
```

### Запуск только read-части (dry-run) для smoke-теста миграции на проде

```bash
MIGRATION_RUN=SAMPLE-001 MIGRATION_DRY_RUN=true ./gradlew run
```

Всё, что не-read — заблокировано. Всё, что read — выполняется. На проде
читать можно, писать — нельзя. Отчёт покажет, что именно произошло бы.

### Threshold-based abort

```hocon
migration.defaults.errorThreshold = 500
```

Если SKIP-ов накопится больше 500 — runner сам прервёт миграцию с exit-code `1`. Защита от
«тихо пропустили 90% данных, но вроде прошли».

---

## 8. Запуск

```bash
# Dry-run на prod для оценки масштаба и выявления интеграционных сюрпризов
MIGRATION_RUN=SAMPLE-001 \
MIGRATION_DRY_RUN=true \
DB_USER=app DB_PASSWORD=$(vault kv get -field=pw secret/pg) \
KAFKA_BOOTSTRAP=kafka.prod:9092 \
AML_URL=https://aml.internal \
LEGACY_SYSTEMS_URL=https://legacy.internal \
./gradlew run

# Реальный прогон
MIGRATION_RUN=SAMPLE-001 \
DB_USER=app DB_PASSWORD=... \
KAFKA_BOOTSTRAP=kafka.prod:9092 \
AML_URL=https://aml.internal \
LEGACY_SYSTEMS_URL=https://legacy.internal \
./gradlew run
```

Exit-коды:
- `0` — всё штатно, включая `LOG_AND_COMPLETE`-case с ошибками в `errors.csv`.
- `1` — `Fail` (в нашем скрипте — только `DataIntegrityException` или превышение threshold).
- `2` — misconfiguration (имя скрипта не найдено, дубликат имени, битый HOCON).

---

## 9. Галочка: что покрыто

- [x] `Migration` + `@Component` — регистрация в графе.
- [x] `ScriptPolicy.LOG_AND_COMPLETE` — alternative policy.
- [x] `MigrationContext` как receiver `migrate()`.
- [x] `errors.includeItem<T>` — per-type сериализатор.
- [x] `readCsv(path)` — из файла (а не classpath).
- [x] `openCsv(path, vararg headers)` × 3 — декларативно сверху migrate(), auto-close.
- [x] `jdbc(db).query/execute` + `transactional { }` (реальный rollback в v0.2.0).
- [x] `cassandra(session).stream(...)` — streaming read.
- [x] `@KafkaPublisher.Topic` + `mutation(label, args)` — идиоматичный Kora-путь для publish с dry-run gate.
- [x] Kora типизированный `@HttpClient` — прямой вызов (read).
- [x] `mutation("label") { }` — для ad-hoc write через типизированный клиент.
- [x] `forEach(chunk, parallel, onError, onErrorLog, logEach, progress)` — вся сигнатура.
- [x] `OnError.handle { e, _ -> Skip/Fail }` — кастомная классификация.
- [x] Kora `@Retry` на `AmlService.verify` — linear backoff на transient HTTP-ошибки внутри клиента, не на уровне DSL.
- [x] `Progress.Custom(n) { done, total -> ... }` — кастомный форматтер.
- [x] `report.dryRunSkipped` — разбивка по labels в отчёте.
- [x] Dry-run через env, без кода `if (dryRun)`.
- [x] `MIGRATION_RUN` env-override для HOCON `migration.run`.
- [x] Error threshold из HOCON.

**Что не покрыто в этом скрипте, но есть в либе:**
- `http(call).post/patch/put/delete` — функциональный wrapper для ad-hoc HTTP (мы используем `mutation { }` + типизированный клиент — идиоматичнее). Кейс: в legacy-проекте нет типизированного клиента, только сырой `HttpClient` — тогда `http(adapt)`.
- `topic(rawProducer, name).send(...)` — Kafka handle с auto-flush. Альтернатива `@KafkaPublisher.Topic + mutation` для high-throughput сценариев. См. [USER_GUIDE §12](../USER_GUIDE.md#12-kafka--topicproducer-name-и-kafkaproducerpublish).
- `kafka(producer).publish(topic, key, value)` — ad-hoc publish без topic-handle (если шлёшь в один-два разных топика и не хочешь заводить handle на каждый).
- `Progress.Off` — глушилка для шумных логов.
- `ScriptPolicy.FAIL_FAST` — дефолт, мы его переопределяем.
- `mutation<R>(label, args, dryRunDefault)` — generic overload, возвращающий значение из лямбды. Подходит когда нужен HTTP-статус, repository result или `RETURNING`-id (см. AGENTS.md §7.7).
