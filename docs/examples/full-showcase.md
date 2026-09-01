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
| CSV read input + `onRowError` | список клиентов к пересверке, битая строка не валит прогон |
| CSV write × 3 (`openCsv` × 3) | processed / failed / vip-upgrades (handle сверху, auto-close) |
| `OnError.handle { e, item -> Skip/Fail }` | кастомная классификация |
| Kora `@Retry` на HTTP-клиенте AML | linear backoff на transient 5xx/timeout — внутри клиента, не на уровне DSL |
| `forEach(chunk, parallel, onError, logEach, progress)` | все параметры форъача, `parallel` = реальные воркеры |
| `Progress.Custom(n) { ... }` | кастомный форматтер прогресса |
| `errors.includeItem<Customer> { ... }` | per-type сериализация для CSV аудита |
| `ScriptPolicy.LOG_AND_COMPLETE` | завершиться штатно даже при unhandled throw |
| Dry-run прозрачно | ни одного `if (dryRun)` во всём файле |

---

## 1. HOCON-конфигурация

Переменные окружения приезжают **только** через HOCON-подстановку `${?VAR}`: в либе нет ни
одного `System.getenv`. Нет строки `dryRun = ${?MIGRATION_DRY_RUN}` — значит `MIGRATION_DRY_RUN=true`
ничего не включит и прогон пойдёт в бой.

```hocon
migration {
  run    = ${?MIGRATION_RUN}
  dryRun = ${?MIGRATION_DRY_RUN}
  # outputFolder опционально — по умолчанию logs/<имя миграции>
  outputFolder = ${?MIGRATION_OUTPUT_FOLDER}
  defaults {
    onUnhandled    = LOG_AND_COMPLETE   # compliance всё равно хочет отчёт
    errorThreshold = 500                # если >500 skip'ов — прерваться
    progressEvery  = 1000
    # Значение аргумента forEach(parallel = ...) по умолчанию. Размер пула им НЕ задаётся:
    # пул runner'а cached и выдаёт столько воркеров, сколько попросил конкретный forEach.
    parallel       = 1
  }
  errorReporting {
    includeStackTrace = true
    maxItemReprLength = 250
  }
  report.asciiOnly = false
}

db {
  jdbcUrl  = "jdbc:postgresql://pg.prod:5432/customers"
  username = ${DB_USER}
  password = ${DB_PASSWORD}
  poolName = "migration-customers"      # обязательный ключ Kora JdbcDatabaseConfig
}

# Kora CassandraDatabaseModule читает секцию `cassandra`; это и есть legacy-кластер,
# другого Cassandra-соединения приложению не нужно.
cassandra.basic {
  contactPoints   = ["legacy-ca.prod:9042"]
  dc              = "dc1"
  sessionKeyspace = "customer_legacy"
}

# Секция продюсера и секция топика — соседи, топик НЕ вкладывается в продюсера.
kafka.customers {
  publisher {
    driverProperties { "bootstrap.servers" = ${KAFKA_BOOTSTRAP} }
  }
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

# Имя из @Retry("amlVerify") — ключ в map'е resilient.retry, поэтому оно односегментное.
resilient.retry.amlVerify {
  delay     = "250ms"
  attempts  = 3                # 3 retry после оригинала → 4 попытки всего
  delayStep = "250ms"          # linear backoff → waits: 250ms, 500ms, 750ms
}

sample {
  inputFile    = "input/customers-to-resync.csv"
  batchSize    = 500
  parallel     = 8
  vipThreshold = 1000000   # amount в копейках
}
```

## 2. Конфиг скрипта + DTO

`@ConfigSource` описывается интерфейсом с методами: дефолт — тело default-метода, необязательное
значение — nullable-тип. Дефолтных значений параметров конструктора Kotlin-класса KSP не видит,
и такой ключ становится обязательным.

```kotlin
// SampleConfig.kt
package com.example.migrations

import ru.tinkoff.kora.config.common.annotation.ConfigSource

@ConfigSource("sample")
interface SampleConfig {
    fun inputFile(): String
    fun batchSize(): Int = 500
    fun parallel(): Int = 8
    fun vipThreshold(): Long
}
```

Поле `outputFolder` тут не нужно: им владеет сам runner (см. блок `migration.outputFolder` сверху).
В `migrate()` он доступен как `outputFolder` (член `MigrationContext`), а `openCsv("file.csv", ...)`
резолвит относительно него.

```kotlin
// Customer.kt
package com.example.migrations

import ru.tinkoff.kora.json.common.annotation.Json
import java.time.Instant

data class Customer(
    val id: String,
    val email: String,
    val status: CustomerStatus,
    val balance: Long,      // копейки
    val startedAt: Instant,
)

enum class CustomerStatus { ACTIVE, INACTIVE, SUSPENDED, VIP }

// @Json на самом типе обязателен: без него KSP не сгенерирует JsonWriter, и @Json-параметр
// публикующего метода не с чем будет связать.
@Json
data class CustomerResyncEvent(val id: String, val oldStatus: CustomerStatus, val newStatus: CustomerStatus)
```

## 3. Kora HTTP-клиент AML

```kotlin
// AmlService.kt
package com.example.migrations

import ru.tinkoff.kora.http.client.common.annotation.HttpClient
import ru.tinkoff.kora.http.common.annotation.HttpRoute
import ru.tinkoff.kora.http.common.annotation.Path
import ru.tinkoff.kora.json.common.annotation.Json
import ru.tinkoff.kora.resilient.retry.annotation.Retry

@HttpClient(configPath = "httpClient.aml")
interface AmlService {

    @HttpRoute(method = "GET", path = "/verify/{customerId}")
    @Json                                       // тело ответа читается JSON-ридером
    @Retry("amlVerify")                         // имя конфига; параметры в HOCON выше
    fun verify(@Path("customerId") customerId: String): VerificationResult

    @Json
    data class VerificationResult(
        val approved: Boolean,
        val risk: String,       // LOW / MEDIUM / HIGH
        val reason: String?,
    )
}
```

`@Retry` живёт в модуле `resilient-kora` (`ru.tinkoff.kora.resilient.retry.annotation.Retry`),
и графу нужен `ResilientModule` — см. §4.5. Backoff в Kora resilient линейный
(`delay + (n-1)*delayStep`), настоящего exponential встроенно нет.

Ответ вне `2xx`, доживший до конца retry-серии, вылетает как
`ru.tinkoff.kora.http.client.common.HttpClientResponseException` (в нём `code`, тело, заголовки)
и доходит до `onError`-классификатора как обычное исключение.

`LegacySystemsClient` — такой же `@HttpClient`-интерфейс, только пишущий. Показан в
использовании через `mutation { }` (код ниже) — сам Kora про dry-run ничего не знает.

```kotlin
// LegacySystemsClient.kt
package com.example.migrations

import ru.tinkoff.kora.http.client.common.annotation.HttpClient
import ru.tinkoff.kora.http.common.annotation.HttpRoute
import ru.tinkoff.kora.http.common.annotation.Path

@HttpClient(configPath = "httpClient.legacySystems")
interface LegacySystemsClient {

    @HttpRoute(method = "POST", path = "/webhooks/vip-upgrade/{customerId}")
    fun onVipUpgrade(@Path("customerId") customerId: String)
}
```

### Kora @KafkaPublisher для resync-топика

```kotlin
// CustomerResyncPublisher.kt
package com.example.migrations

import org.apache.kafka.clients.producer.RecordMetadata
import ru.tinkoff.kora.json.common.annotation.Json
import ru.tinkoff.kora.kafka.common.annotation.KafkaPublisher

@KafkaPublisher("kafka.customers.publisher")
interface CustomerResyncPublisher {

    @KafkaPublisher.Topic("kafka.customers.resyncTopic")
    fun publishResync(key: String, @Json value: CustomerResyncEvent): RecordMetadata
}
```

Имя топика приходит из HOCON `kafka.customers.resyncTopic.topic` — там оно переопределяемо
без правок кода. Реализацию интерфейса Kora генерирует при сборке (KSP), для `@Json`-значения
нужны артефакт `json-module` и `JsonModule` в графе.

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

## 4.5. Граф приложения

```kotlin
// App.kt
package com.example.migrations

import io.github.dsudomoin.migration.kora.MigrationModule
import ru.tinkoff.kora.application.graph.KoraApplication
import ru.tinkoff.kora.common.KoraApp
import ru.tinkoff.kora.config.hocon.HoconConfigModule
import ru.tinkoff.kora.database.cassandra.CassandraDatabaseModule
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule
import ru.tinkoff.kora.http.client.jdk.JdkHttpClientModule
import ru.tinkoff.kora.json.module.JsonModule
import ru.tinkoff.kora.kafka.common.KafkaModule
import ru.tinkoff.kora.resilient.ResilientModule

@KoraApp
interface App :
    HoconConfigModule,
    JdbcDatabaseModule,
    CassandraDatabaseModule,
    KafkaModule,
    JsonModule,
    JdkHttpClientModule,
    ResilientModule,
    MigrationModule

fun main() {
    KoraApplication.run { AppGraph.graph() }
}
```

От нашей либы нужен ровно один родитель — `MigrationModule`: он и конфиг-секцию `migration`
читает сам, и runner в граф добавляет (тот помечен `@Root`, поэтому создаётся, хотя от него
никто не зависит). Сборка — KSP, координаты и список артефактов см.
[resend-archetype.md §2](resend-archetype.md); здесь дополнительно нужны `database-cassandra`,
`json-module` и `resilient-kora`.

## 5. Сам скрипт

```kotlin
// SampleMigration.kt
package com.example.migrations

import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.Progress
import io.github.dsudomoin.migration.ScriptPolicy
import io.github.dsudomoin.migration.csv.openCsv
import io.github.dsudomoin.migration.csv.readCsv
import io.github.dsudomoin.migration.error.includeItem
import io.github.dsudomoin.migration.forEach
import io.github.dsudomoin.migration.kora.ops.cassandra
import io.github.dsudomoin.migration.kora.ops.jdbc
import io.github.dsudomoin.migration.kora.ops.transactional
import io.github.dsudomoin.migration.mutation
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.cassandra.CassandraDatabase
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import java.util.concurrent.atomic.AtomicInteger

@Component
class SampleMigration(
    private val pg: JdbcConnectionFactory,
    private val legacy: CassandraDatabase,
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

        // onRowError обязателен, если битая строка входного файла не должна валить прогон:
        // по умолчанию там OnError.Fail. Со Skip строка уезжает в errors.csv (item — сама
        // разобранная Map<String, String>), считается в report.skipped (а значит и
        // в errorThreshold) и до forEach не доходит.
        val inputIds = readCsv(config.inputFile(), onRowError = OnError.Skip) { it.getValue("customer_id") }.toList()

        // CqlSession живёт в графе внутри CassandraDatabase; сессию отдаёт currentSession().
        val legacySession = legacy.currentSession()

        forEach(
            inputIds,
            chunk = config.batchSize(),
            parallel = config.parallel(),
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

            val customers = cassandra(legacySession).stream(
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
                    if (!check.approved) {
                        failed.row(c.id, check.reason ?: "AML rejected")
                        throw AmlValidationException("AML rejected ${c.id}: ${check.reason}")
                    }
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
                    if (newStatus == CustomerStatus.VIP && c.balance >= config.vipThreshold()) {
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
        c.balance >= config.vipThreshold()    -> CustomerStatus.VIP
        c.status == CustomerStatus.INACTIVE   -> CustomerStatus.INACTIVE
        else                                  -> CustomerStatus.ACTIVE
    }
}
```

Глубина indent'а в горячей петле — 4 (`forEach { transactional { enriched.forEach { if { mutation {} } } } }`). Никакой лесенки `csv → csvSink → csvSink → batchPublish` вокруг `forEach`.

## 6. Разбор — что где происходит

### 6.1. Script-level policy: `LOG_AND_COMPLETE`

```kotlin
onUnhandled = ScriptPolicy.LOG_AND_COMPLETE
```

Если в середине скрипта вылетит что-то совсем неожиданное (например, баг
в `computeNewStatus`), runner не упадёт с exit-code 1 — залогирует ошибку, зафиксирует в `report.failed`,
закроет файлы, напечатает отчёт, вернёт exit-code 0. Для compliance-скрипта это критично:
хоть какой-то отчёт лучше, чем ничего.

Если бы мы хотели fail-fast — убрали бы параметр (`FAIL_FAST` — дефолт).

### 6.2. Кастомный per-type сериализатор ошибок

```kotlin
errors.includeItem<Customer> { "id=${it.id}, email=${it.email}, status=${it.status}, balance=${it.balance}" }
```

Первой строкой `migrate()` регистрируем: если в `errors.csv` попадёт item типа `Customer` —
записывать его **так**, а не через `toString()`. Импорт — `io.github.dsudomoin.migration.error.includeItem`
(inline-reified extension на `ErrorReporter`).

Здесь есть тонкость, связанная с `chunk`: в `errors.csv` уезжает тот объект, на котором
споткнулся `forEach`, а это **батч** — `List<String>` из 500 id, а не `Customer`. Сериализатор
для `Customer` сработает там, где item'ом является сам `Customer`: при `forEach` без `chunk`
и при ручном `auditError(e, customer)` (см. §6.5.1).

Второй источник записей в `errors.csv` — `readCsv(onRowError = OnError.Skip)`: там item'ом
будет сырая строка файла, `Map<String, String>`. Если в колонках есть чувствительные данные,
регистрируй сериализатор и на неё:
`errors.includeItem<Map<String, String>> { "customer_id=${it["customer_id"]}" }`.

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

Под dry-run файлы **всё равно пишутся** (решение спеки §4.1): скрипт под
`--dry-run` остаётся диагностическим артефактом. Никакой лесенки `csv { csvSink { ... } }`.

### 6.4. Kafka через `@KafkaPublisher.Topic` + `mutation`

```kotlin
mutation("customers.resync", args = mapOf("customerId" to c.id)) {
    publisher.publishResync(c.id, CustomerResyncEvent(c.id, c.status, newStatus))
}
```

`CustomerResyncPublisher` — типизированный Kora `@KafkaPublisher`-интерфейс
(см. §3). Реализация генерируется Kora, имя топика берётся из HOCON-блока
`kafka.customers.resyncTopic.topic`. `@Json` сериализация — автоматом.

Сам `publishResync(...)` Kora не знает про dry-run — поэтому оборачиваем в
`mutation(...) { ... }`. Под dry-run блок **не выполняется**, в лог идёт
`INFO [DRY-RUN] mutation:customers.resync (customerId=CUST-4277)`, в отчёт —
инкремент `report.dryRunSkipped["mutation:customers.resync"]`. Label —
константа, args — диагностика; в финальном отчёте получаем чистый агрегат
`mutation:customers.resync: 973500`.

Альтернативный путь — сырой `Producer<K, V>` + `topic(producer, name)`-handle (для
high-throughput сценариев с явным flush'ем). Продюсер в граф нужно принести самому
(свой `@Module` с фабрикой `KafkaProducer(props)`) либо взять у сгенерированного
publisher'а: он имплементирует `ru.tinkoff.kora.kafka.common.producer.GeneratedPublisher`,
у которого есть `producer(): Producer<ByteArray, ByteArray>`. **`KafkaProducerModule`
в Kora не существует** — если он попадался в старых примерах, это опечатка. Подробнее —
[USER_GUIDE §12](../USER_GUIDE.md#12-kafka--topicproducer-name-и-kafkaproducerpublish).

Хендлы `kafka(producer)` и `topic(producer, name)` мемоизируются в контексте
(`MigrationContext.shared`), поэтому вызывать их прямо в теле `forEach` не накладно;
на закрытии они делают `flush()`, и отказы `sendAsync` успевают доехать до счётчика
`report.asyncFailed` до печати отчёта.

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
- `parallel = 8` — восемь батчей обрабатываются одновременно, и это восемь настоящих воркеров:
  пул runner'а cached, `migration.defaults.parallel` задаёт лишь значение по умолчанию для
  этого аргумента. Вложенный `forEach(parallel > 1)` тоже работает — на исчерпании пула он
  больше не встаёт.
- `OnError.handle { ... }` — свой классификатор. Принимает решение **один раз** (`Decision.Skip` / `Decision.Fail`) по типу исключения. Retry в DSL нет — для transient-ошибок навешивай `@Retry` на сам Kora-клиент (см. AML-секцию выше).
- `onErrorLog` — свой дополнительный лог в дополнение к авто-аудиту в CSV.
- `logEach` — лог после каждого успешно обработанного батча.
- `progress = Progress.Custom(10)` — каждые 10 батчей кастомный форматтер тянет текущее значение `vipUpgrades` и печатает.

Исключение из `logEach` / `onErrorLog` судьбу item'а не меняет: про первый такой сбой
runner пишет предупреждение в отчёт, дальше молчит. Диагностический колбэк не имеет права
провалить уже выполненную запись.

### 6.5.1. Гранулярность: батч — это один item

Это самое неочевидное место архетипа. При `chunk = 500` единицей учёта, единицей `Skip`
и единицей потери является **батч целиком**:

- исключение на 137-м клиенте из 500 обрывает весь батч: первые 136 уже записаны (и
  закоммичены, если транзакция успела закрыться), оставшиеся 363 не обработаются никогда;
- `report.skipped++` — это +1, а не +364, и `errorThreshold = 500` считает батчи;
- в `errors.csv` уедет одна строка, и в колонке item будет `List<String>` из 500 id,
  а не конкретный `Customer`.

В нашем скрипте `AmlValidationException` — это ошибка **одного** клиента, поэтому мы
дополнительно пишем его в `failed.csv` прямо в месте броска. Если хочется полноценного
пер-элементного учёта, ошибку надо ловить внутри батча:

```kotlin
// поле класса рядом с vipUpgrades: private val skippedCustomers = AtomicLong()

val enriched = customers.mapNotNull { c ->
    try {
        if (c.status == CustomerStatus.INACTIVE) c to null else {
            val check = aml.verify(c.id)
            if (!check.approved) throw AmlValidationException("AML rejected ${c.id}: ${check.reason}")
            c to check.risk
        }
    } catch (e: AmlValidationException) {
        auditError(e, c)            // строка в errors.csv по конкретному Customer,
        failed.row(c.id, e.message) // includeItem<Customer> здесь как раз сработает
        skippedCustomers.incrementAndGet()
        null                         // клиент выпадает из батча, остальные 499 доедут
    }
}
```

`auditError(e, item)` — публичный член `MigrationContext`, ровно то же, что делает `forEach`
под `OnError.Skip`. Счётчики отчёта он не трогает: `report.processed/skipped` остаются
счётчиками батчей, поэтому свой `AtomicLong` тут не роскошь. Альтернатива — отказаться от
`chunk` (тогда `Skip` теряет ровно одного клиента, но `where id in :ids` придётся выбросить).

### 6.6. Cassandra stream вместо query

```kotlin
val customers = cassandra(legacySession).stream("select ... where id in :ids", "ids" to batch) { ... }.toList()
```

`.stream(...)` возвращает `Sequence<T>` — не загружает всё в память разом.
На большинстве легаси Cassandra-кластеров это единственный способ вытянуть
крупный `WHERE ... IN` без таймаута. `.toList()` материализует в конце,
потому что батч мы обрабатываем транзакционно — нужно полное знание.

`CqlSession` берётся из `CassandraDatabase.currentSession()` — это компонент графа,
который поднял `CassandraDatabaseModule` по секции `cassandra`. Второй кластер (если бы
он был нужен) пришлось бы собирать своим `@Module` с `@Tag`-ом, Kora даёт одно
Cassandra-соединение на приложение.

Список в `:ids` биндится с выведением типа элемента по первому не-null значению;
пустая коллекция бросит `IllegalArgumentException` (тип элемента вывести не из чего).
Батчи `forEach` пустыми не бывают, так что здесь это не мешает.

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
connection. На успехе — `commit`; на исключении — `rollback`.

Три правила, которые лучше узнать из документации, чем из инцидента:

- **Вложенный `transactional` запрещён** — `IllegalStateException`, причём проверка работает
  и под dry-run тоже: репетиция не имеет права быть зелёной там, где бой падает.
- **Внутри `transactional` нельзя запускать параллельный `forEach`.** `java.sql.Connection`
  не потокобезопасен, поэтому tx-bound `SqlOps` запоминает поток-владелец и бросает
  `IllegalStateException` с внятным текстом, если его дёрнули с другого потока. Правильный
  порядок — `forEach { transactional { ... } }`, как в скрипте, а не наоборот.
- **Под dry-run тело блока выполняется.** Пропускаются отдельные записи внутри него
  (`execute`/`batch`/`mutation`), а сам блок не пропускается — иначе в репетиции не
  выполнились бы чтения и не посчитались бы записи. В breakdown появляется отдельная
  строка `jdbc.transactional` (реальный `Connection` при этом не открывается).

`query(...)` и `stream(...)` принимают только читающие запросы (`select/with/show/explain/
values/table/describe`) — они не проходят dry-run gate, и `query("insert ... returning id")`
выполнился бы В БОЮ во время репетиции. Если нужны строки из пишущего запроса — есть
`executeReturning(sql, ...) { rs -> ... }`: он проходит гейт как обычная запись и под dry-run
возвращает пустой список.

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

Если от вызова нужен результат (HTTP-статус, id из `RETURNING`, доменный объект) — есть
generic-перегрузка `mutation(label, args, dryRunDefault) { ... }`. `dryRunDefault` выбирай так,
чтобы репетиция шла той же веткой кода, что и бой.

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

- **AML-валидация** (клиент не прошёл compliance-проверку) → `Skip`. Батч уходит в `errors.csv`
  (через авто-аудит) + `report.skipped++`. Скрипт продолжает.
- **DataIntegrity** (БД вернула что-то невозможное) → `Fail`. Прерываем весь скрипт — дальше смотреть бесполезно, это баг.
- Всё остальное — `Skip`. Не валим миграцию на том, чего не ожидали; разберёмся по отчёту.

`Handle` принимает решение **один раз** по типу ошибки. Retry в DSL нет: если хочется backoff на transient-ошибки сети — это уровень Kora `@Retry` внутри типизированного клиента (см. AML в §3 выше: `@Retry("amlVerify")` + соседний HOCON-блок). До `Handle` тогда долетит только финальный fail после исчерпания retries — для HTTP это `HttpClientResponseException`, его удобно разбирать по `e.code`.

Если сам классификатор бросит — его исключение уйдёт наверх (с исходной ошибкой в
`suppressed`), item будет аудитнут и посчитан в `failed`. Не пишите в `decide` ничего,
что может упасть.

### 6.10. Итоговый отчёт

```
═══════════════════════════════════════════════════════════
Migration: SAMPLE-001  (author: team)
Started:   2026-04-24 14:15:32 +03:00
Finished:  2026-04-24 14:47:11 +03:00
Duration:  31m 39s
Mode:      REAL
───────────────────────────────────────────────────────────
Processed:                 1 947
  ✓ Successful:            1 933
  ⊘ Skipped (errors):      14
  ✗ Failed:                0
───────────────────────────────────────────────────────────
Error details:  logs/SAMPLE-001/errors.csv
Error traces:   logs/SAMPLE-001/errors.log
═══════════════════════════════════════════════════════════
```

Что здесь надо уметь читать:

- **Processed 1 947 — это батчи**, не клиенты (размер батча 500, значит клиентов ~973 500).
  Если нужен счётчик клиентов — веди свой `AtomicLong` и выводи его в `logEach` или в
  прогресс-форматтере (мы так вывели `vipUpgrades`).
- Числа с разделителем — неразрывающая группировка по тысячам (`1 947`), это формат
  `ReportFormatter`, а не опечатка.
- **Метки времени печатаются со смещением зоны** (`+03:00`). `errors.csv` пишет UTC — по
  смещению одно с другим сопоставляется без гадания, в каком часовом поясе жил под.
- Строки `Error details:` / `Error traces:` появляются только если файлы созданы: авто-аудитор
  создаёт их лениво, на первой ошибке.
- Между счётчиками и dry-run-блоком может появиться строка `✗ Async delivery failed:  N` —
  она печатается **только когда N > 0** и означает, что брокер отверг N сообщений,
  отправленных через `publishAsync` / `sendAsync`. Такие сообщения уже лежат в `errors.csv`,
  а exit-код прогона поднимается до 1, даже если всё остальное прошло гладко.
- Блок `⚠ Warnings:` собирает нефатальное: сбой `close()` у ресурса, упавший диагностический
  колбэк, не остановившийся за 30 секунд пул, отсутствие logback на classpath.

Под dry-run в отчёте появится дополнительный блок (labels отсортированы, порядок
детерминированный):
```
  ⌀ Dry-run skipped writes:    (jdbc.execute: 1947000, jdbc.transactional: 1947, mutation:customers.resync: 973500, mutation:legacy.vip-upgrade: 147)
```

И по этой строчке видно: сколько именно UPDATE/INSERT ушло бы в Postgres, сколько транзакций
открылось бы, сколько сообщений в Kafka и сколько webhook'ов. 147 VIP-апгрейдов — красивая
оценка масштаба без реального сайд-эффекта.

Обратная ситуация — breakdown **пустой** при непустом `Processed` — означает, что ни одна
запись не была перехвачена: значит где-то забыт `mutation { }` и вызов ушёл в бой
по-настоящему. Runner про это отдельно пишет WARN в лог и предупреждение в отчёт: под
dry-run это единственный наблюдаемый признак такой ошибки.

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

Папка определяется конфигом `migration.outputFolder` (HOCON, в том числе через
`${?MIGRATION_OUTPUT_FOLDER}`), или дефолтом `logs/<имя миграции>` относительно
CWD JVM. Все файлы внутри `TRUNCATE`-аются при ререн-е (один прогон = один
набор артефактов). Если нужна история — заархивируй папку после прогона
(или подставь timestamped путь через env). Если папку создать не удалось (нет прав,
битый путь) — это мисконфиг, exit-code 2.

`migration.log` собирается через programmatic logback `FileAppender` на root-
логгере. То есть в файле окажется не только наш `log.info/...`, но и весь
output Kora, Hikari, Kafka producer, Cassandra driver и т.д. Это сознательное
решение в пользу compliance-сценариев: хочется иметь полный аудит всего, что
происходило в JVM во время миграции. Если logback не на classpath — runner
сообщит warn (и в лог, и в отчёт) и пойдёт без file-логирования.

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
валиден только внутри `consume`-блока; `forEach` пишется прямо там. Единица `Skip`
здесь — один заказ, а не батч.

### Linear backoff для нестабильной внешней API

Backoff — на уровне типизированного Kora-клиента, не на уровне `forEach`:

```kotlin
@HttpClient(configPath = "httpClient.sync")
interface SyncClient {
    @HttpRoute(method = "POST", path = "/sync")
    @Retry("syncApi")                        // имя конфига; параметры в HOCON
    fun sync(@Json item: SyncItem)
}

// В migrate:
forEach(items, parallel = 4, onError = OnError.Skip) { item ->
    mutation("sync", args = mapOf("id" to item.id)) { syncClient.sync(item.toSyncItem()) }
}
```

```hocon
resilient.retry.syncApi {
  delay     = "250ms"
  attempts  = 4                # 4 retry после оригинала → 5 попыток всего
  delayStep = "250ms"          # → waits: 250ms, 500ms, 750ms, 1000ms
}
```

Тело запроса — `@Json`-параметр (`@Body`-аннотации в Kora нет). В Kora resilient backoff
линейный (`delay + (n-1)*delayStep`). Настоящего exponential встроенно нет — либо мирись
с linear (`delayStep` побольше — практически достаточно), либо иди в императивный
`RetryManager.get("syncApi").retry(supplier)` со своим backoff.
После исчерпания попыток исключение вылетает в `forEach`, который аудитит item в `errors.csv`
через `OnError.Skip`.

### Кастомный прогресс с несколькими метриками

```kotlin
val ok = AtomicLong(); val skip = AtomicLong()
val startMs = System.currentTimeMillis()

forEach(items, onError = OnError.Skip,
        progress = Progress.Custom(1000) { done, total ->
            val elapsedS = ((System.currentTimeMillis() - startMs) / 1000).coerceAtLeast(1)
            "resync: done=$done/${total ?: "?"}  ok=${ok.get()}  skip=${skip.get()}  rate=${done / elapsedS}/s"
        }) { item ->
    try { doWork(item); ok.incrementAndGet() }
    catch (e: Exception) { skip.incrementAndGet(); throw e }
}
```

### Comparison архетип компактно

```kotlin
val diffs = openCsv("diffs.csv", "id", "reason")

forEach(ids, chunk = 200, parallel = 4, onError = OnError.Skip) { batch ->
    val a = cassandra(primary).query("select id, v from t where id in :ids", "ids" to batch) {
        it.getString("id")!! to it.getString("v")
    }.toMap()
    val b = cassandra(replica).query("select id, v from t where id in :ids", "ids" to batch) {
        it.getString("id")!! to it.getString("v")
    }.toMap()
    batch.forEach { id ->
        if (a[id] != b[id]) diffs.row(id, "primary=${a[id]}, replica=${b[id]}")
    }
}
```

Сравнивать по индексу (`a[i] != b[i]`) нельзя: порядок строк в ответе Cassandra не обязан
совпадать с порядком id в `IN`, да и отсутствующая строка сдвинет всё остальное. Ключ —
только по id. `row(...)` — метод `CsvOutput`, поэтому handle нужно открыть заранее.

### Запуск только read-части (dry-run) для smoke-теста миграции на проде

```bash
MIGRATION_RUN=SAMPLE-001 MIGRATION_DRY_RUN=true ./gradlew run
```

Всё, что идёт через `guardWrite` (`jdbc.execute/batch/executeReturning`, `cassandra.execute/batch`,
`kafka.publish`, `http.post/put/patch/delete`, `mutation { }`) — заблокировано. Всё, что read —
выполняется. Тела `transactional`-блоков тоже выполняются, пропускаются лишь записи внутри них.
На проде читать можно, писать — нельзя. Отчёт покажет, что именно произошло бы.

### Threshold-based abort

```hocon
migration.defaults.errorThreshold = 500
```

Если SKIP-ов накопится больше 500 — runner сам прервёт миграцию с exit-code `1`. Защита от
«тихо пропустили 90% данных, но вроде прошли». Считаются и skip'ы `forEach`, и битые строки
`readCsv(onRowError = OnError.Skip)`; при `chunk` единица счёта — батч.

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
- `1` — `Fail` (в нашем скрипте — `DataIntegrityException`), превышение `errorThreshold`,
  либо отказы async-доставки в Kafka.
- `2` — misconfiguration: имя скрипта не найдено, дубликат имён, битые значения
  `migration.defaults.*`, невозможно создать `outputFolder`.

Код возврата отдаётся через `exitProcess`. Если runner поднимается внутри теста или внутри
приложения, которое живёт дальше, объяви в графе компонент `MigrationExit` — код уедет в него,
а JVM останется жива.

---

## 9. Галочка: что покрыто

- [x] `Migration` + `@Component` — регистрация в графе.
- [x] `ScriptPolicy.LOG_AND_COMPLETE` — alternative policy.
- [x] `MigrationContext` как receiver `migrate()`.
- [x] `errors.includeItem<T>` — per-type сериализатор.
- [x] `readCsv(path, onRowError = OnError.Skip)` — из файла (а не classpath), битая строка не валит прогон.
- [x] `openCsv(filename, vararg headers)` × 3 — декларативно сверху migrate(), auto-close.
- [x] `jdbc(db).query/execute` + `transactional { }` — реальный tx-scope с rollback.
- [x] `cassandra(session).stream(...)` — streaming read через `CassandraDatabase.currentSession()`.
- [x] `@KafkaPublisher.Topic` + `mutation(label, args)` — идиоматичный Kora-путь для publish с dry-run gate.
- [x] Kora типизированный `@HttpClient` — прямой вызов (read).
- [x] `mutation("label") { }` — для ad-hoc write через типизированный клиент.
- [x] `forEach(chunk, parallel, onError, onErrorLog, logEach, progress)` — вся сигнатура.
- [x] `OnError.handle { e, _ -> Skip/Fail }` — кастомная классификация.
- [x] `auditError(e, item)` — пер-элементный аудит внутри батча (§6.5.1).
- [x] Kora `@Retry` на `AmlService.verify` — linear backoff на transient HTTP-ошибки внутри клиента, не на уровне DSL.
- [x] `Progress.Custom(n) { done, total -> ... }` — кастомный форматтер.
- [x] `report.dryRunSkipped` — разбивка по labels в отчёте.
- [x] Dry-run через env + строку `dryRun = ${?MIGRATION_DRY_RUN}`, без кода `if (dryRun)`.
- [x] `MIGRATION_RUN` env-override для HOCON `migration.run`.
- [x] Error threshold из HOCON.

**Что не покрыто в этом скрипте, но есть в либе:**
- `http(call).post/patch/put/delete` — функциональный wrapper для ad-hoc HTTP (мы используем `mutation { }` + типизированный клиент — идиоматичнее). Кейс: в legacy-проекте нет типизированного клиента, только сырой HTTP. Не-2xx там поднимает `HttpStatusException`.
- `jdbc(db).executeReturning(sql, ...) { rs -> ... }` — пишущий запрос, возвращающий строки (`INSERT ... RETURNING`). Проходит dry-run gate, под репетицией отдаёт пустой список.
- `topic(rawProducer, name).send(...)` / `.sendAsync(...)` — Kafka handle с flush на закрытии. Отказ async-доставки уходит в `errors.csv`, в `report.asyncFailed` и поднимает exit-код до 1.
- `kafka(producer).publish(topic, key, value)` — ad-hoc publish без topic-handle (если шлёшь в один-два разных топика и не хочешь заводить handle на каждый).
- `MigrationContext.shared(key) { ... }` — мемоизация собственных op-хендлов на прогон (см. [customization.md](customization.md) §1).
- `Progress.Off` — глушилка для шумных логов.
- `ScriptPolicy.FAIL_FAST` — дефолт, мы его переопределяем.
- `mutation<R>(label, args, dryRunDefault)` — generic overload, возвращающий значение из лямбды. Подходит когда нужен HTTP-статус, repository result или `RETURNING`-id.
- `MigrationExit` — перехват exit-кода вместо `exitProcess`.
