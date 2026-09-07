# Showcase: все возможности Migration DSL в одном плане

> Пошаговое описание узлов — в [гайде](../USER_GUIDE.md).

**Задача.** По тикету SAMPLE-001 команда compliance потребовала ресверку клиентской
базы: часть клиентов «провисли» в неопределённом статусе после январского
инцидента с legacy-хранилищем. Нужно пройтись по ним и исправить: подтянуть
актуальный профиль из старой Cassandra, обогатить через AML-сервис, сохранить
исправления в Postgres (транзакционно), опубликовать событие в Kafka, а для
клиентов, получивших VIP-статус — дёрнуть webhook в legacy-systems.

Клиенты приезжают списком, разбитым по сегментам, и требование эксплуатации такое:
**сегмент считается закрытым только когда все его события подтверждены брокером**.
Отсюда форма плана — `scoped`-стадия с барьером на каждый сегмент, а следом отдельная
стадия для webhook'ов, которая запускается только если ресверка прошла.

Ошибки бывают двух типов на уровне DSL: business-validation (skip + аудит
в CSV), critical (fail). Transient-ошибки сети (5xx, timeout, connection reset)
ретраятся на уровне Kora `@Retry` внутри типизированного HTTP-клиента — до
классификатора стадии попадает уже финальный fail после исчерпания retries.
Глобально — в `LOG_AND_COMPLETE`-режиме, чтобы compliance получил любой отчёт
даже если скрипт поймает что-то неожиданное в середине.

Это сценарий, в котором осмысленно задействованы **все** узлы DSL.

---

## Что этот план демонстрирует (галочкой прокатимся в конце)

| Примитив | Где в коде |
|---|---|
| `MigrationDefinition` + `plan()` | Имя — константа, план строится один раз и только у выбранной миграции |
| `input(name) { }` + `resolve` | Список сегментов читается один раз на прогон |
| `validate { }` | Проверка конфига до первой стадии и до любого эффекта |
| `output(...)` × 3 | processed / failed / vip-upgrades — объявлены в билдере, закрывает движок |
| `scoped(parents, completionTimeout)` | Барьер подтверждений на каждый сегмент |
| Две стадии | Webhook'и не начнутся, пока ресверка не закончилась успешно |
| `pages(...)` | Курсорный источник второй стадии |
| `.chunked(N)` | Батчинг обычным `Sequence.chunked` — ради `where id in :ids` |
| Postgres JDBC + `transactional { }` | update профиля + insert в audit (один tx, реальный rollback) |
| Cassandra `query` | profile lookup из legacy |
| `publish(name, args) { CompletionStage }` | Событие ресверки под барьером стадии |
| `write(name, args) { WriteOutcome }` | Webhook в legacy-systems через типизированный Kora-клиент |
| Kora `@HttpClient` для read | AML-сервис, вызывается напрямую |
| `ItemError.Handle { e, item -> }` | Классификатор с типизированным item'ом |
| `errorThreshold` на стадии | Порог считается по сегменту и сбрасывается на его границе |
| Kora `@Retry` на HTTP-клиенте AML | linear backoff на transient 5xx/timeout — внутри клиента, не в DSL |
| `Progress.Custom(n) { ... }` | Кастомный форматтер прогресса |
| `errors.includeItem<T> { ... }` | Per-type сериализация для CSV аудита |
| `auditError(e, item)` | Пер-элементный аудит внутри батча |
| `ScriptPolicy.LOG_AND_COMPLETE` | Завершиться штатно даже при unhandled throw |
| Dry-run прозрачно | Ни одного `if (dryRun)` во всём файле |

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
    # Значение аргумента source/scoped(parallel = ...) по умолчанию. Размер пула им НЕ
    # задаётся: пул runner'а cached и выдаёт столько воркеров, сколько попросила стадия.
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
  inputFile         = "input/customers-to-resync.csv"
  batchSize         = 500
  parallel          = 8
  vipThreshold      = 1000000   # amount в копейках
  completionTimeout = 10m
  pageSize          = 2000
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
import java.time.Duration

@ConfigSource("sample")
interface SampleConfig {
    fun inputFile(): String
    fun batchSize(): Int = 500
    fun parallel(): Int = 8
    fun vipThreshold(): Long
    fun completionTimeout(): Duration = Duration.ofMinutes(10)
    fun pageSize(): Int = 2000
}
```

Поле `outputFolder` тут не нужно: им владеет сам runner (см. блок `migration.outputFolder` сверху).
В любом scope он доступен как `outputFolder`, а `output("file.csv", ...)` резолвит имя
относительно него.

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

## 3. Kora-клиенты

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
и доходит до классификатора `ItemError.Handle` как обычное исключение.

`LegacySystemsClient` — такой же `@HttpClient`-интерфейс, только пишущий. Показан в
использовании через `write { }` (код ниже) — сам Kora про dry-run ничего не знает.

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
import java.util.concurrent.CompletionStage

@KafkaPublisher("kafka.customers.publisher")
interface CustomerResyncPublisher {

    /** CompletionStage, а не RecordMetadata: это то, что барьер стадии умеет ждать. */
    @KafkaPublisher.Topic("kafka.customers.resyncTopic")
    fun publishResync(key: String, @Json value: CustomerResyncEvent): CompletionStage<RecordMetadata>
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
никто не зависит). Скрипт runner находит через `All<MigrationDefinition>` — достаточно
`@Component` на классе. Сборка — KSP, координаты и список артефактов см.
[resend-archetype.md §2](resend-archetype.md); здесь дополнительно нужны `database-cassandra`,
`json-module` и `resilient-kora`.

## 5. Сам план

```kotlin
// SampleMigration.kt
package com.example.migrations

import io.github.dsudomoin.migration.HandlerScope
import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.MigrationDefinition
import io.github.dsudomoin.migration.Progress
import io.github.dsudomoin.migration.ScriptPolicy
import io.github.dsudomoin.migration.WriteOutcome
import io.github.dsudomoin.migration.csv.readCsv
import io.github.dsudomoin.migration.error.includeItem
import io.github.dsudomoin.migration.kora.ops.cassandra
import io.github.dsudomoin.migration.kora.ops.jdbc
import io.github.dsudomoin.migration.kora.ops.transactional
import io.github.dsudomoin.migration.migration
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.cassandra.CassandraDatabase
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import java.util.concurrent.atomic.AtomicLong

@Component
class SampleMigration(
    private val pg: JdbcConnectionFactory,
    private val legacy: CassandraDatabase,
    private val aml: AmlService,
    private val legacySystems: LegacySystemsClient,
    private val publisher: CustomerResyncPublisher,
    private val config: SampleConfig,
) : MigrationDefinition {

    override val name = "SAMPLE-001"

    private val vipUpgrades = AtomicLong()
    private val skippedCustomers = AtomicLong()

    override fun plan() = migration(
        name = name,
        author = "team",
        onUnhandled = ScriptPolicy.LOG_AND_COMPLETE,
    ) {
        val processed = output("processed.csv",    "id", "oldStatus", "newStatus")
        val failed    = output("failed.csv",       "id", "reason")
        val vips      = output("vip-upgrades.csv", "id", "balance")

        // Ленивое значение прогона: файл читается один раз, обе стадии видят один результат.
        // onRowError нужен явно — по умолчанию там ItemError.Fail, и битая строка валит прогон.
        val segments = input("segments") {
            errors.includeItem<Map<String, String>> { "customer_id=${it["customer_id"]}" }
            readCsv(config.inputFile(), onRowError = ItemError.Skip) {
                it.getValue("segment") to it.getValue("customer_id")
            }.groupBy({ it.first }, { it.second })
        }

        validate {
            require(config.batchSize() > 0) { "sample.batchSize должен быть > 0" }
            require(config.vipThreshold() > 0) { "sample.vipThreshold должен быть > 0" }
        }

        scoped(
            segments,
            name = "resync",
            parents = { loaded -> loaded.keys.asSequence() },
            completionTimeout = config.completionTimeout(),
            parallel = config.parallel(),
            errorThreshold = 100,               // на сегмент; сбрасывается на его границе
            onItemError = ItemError.Handle { e, batch ->
                when (e) {
                    is AmlValidationException -> ItemError.Decision.Skip
                    is DataIntegrityException -> ItemError.Decision.Fail
                    // Классификатор видит типизированный item — здесь это батч id'шников.
                    else -> if (batch.size == 1) ItemError.Decision.Fail else ItemError.Decision.Skip
                }
            },
            progress = Progress.Custom(10) { done, total ->
                "resync: batches=$done/${total ?: "?"}  vipUpgrades=${vipUpgrades.get()}  skipped=${skippedCustomers.get()}"
            },
            items = { segment ->
                errors.includeItem<Customer> { "id=${it.id}, status=${it.status}, balance=${it.balance}" }
                errors.includeItem<List<String>> { "batch of ${it.size}, first=${it.firstOrNull()}" }

                // тот же input нужен и элементам, а параметром он приходит только в parents —
                // resolve вернёт тот же закэшированный объект, второго запроса не будет
                resolve(segments).getValue(segment).asSequence().chunked(config.batchSize())
            },
        ) { batch ->
            val customers = loadFromLegacy(batch)

            // Пер-элементный аудит: AML-отказ — это ошибка ОДНОГО клиента, а не батча,
            // поэтому ловим её здесь, а не отдаём наверх классификатору.
            val enriched = customers.mapNotNull { c ->
                try {
                    val risk = if (c.status == CustomerStatus.INACTIVE) null else {
                        val check = aml.verify(c.id)
                        if (!check.approved) throw AmlValidationException("AML rejected ${c.id}: ${check.reason}")
                        check.risk
                    }
                    c to computeNewStatus(c, risk)
                } catch (e: AmlValidationException) {
                    auditError(e, c)                          // строка в errors.csv по конкретному Customer
                    failed.row(c.id, e.message ?: "AML rejected")
                    skippedCustomers.incrementAndGet()
                    null                                      // клиент выпадает, остальные доедут
                }
            }

            // Сначала коммит, потом публикация: событие о том, чего в базе нет, отозвать нельзя.
            transactional(jdbc(pg)) {
                enriched.forEach { (c, newStatus) ->
                    execute(
                        "update customers set status = :s, last_resync = now() where id = :id",
                        "s" to newStatus.name, "id" to c.id,
                    )
                    execute(
                        "insert into customers_audit(customer_id, from_status, to_status, ts) values (:id, :f, :t, now())",
                        "id" to c.id, "f" to c.status.name, "t" to newStatus.name,
                    )
                }
            }

            enriched.forEach { (c, newStatus) ->
                processed.row(c.id, c.status.name, newStatus.name)
                publish("customers.resync", args = mapOf("customerId" to c.id)) {
                    publisher.publishResync(c.id, CustomerResyncEvent(c.id, c.status, newStatus))
                }
            }
        }

        // Стадия запустится только если ресверка прошла: провал стадии не запускает следующие.
        source(
            name = "vip-webhooks",
            parallel = 4,
            onItemError = ItemError.Skip,
            items = {
                pages(
                    first = {
                        jdbc(pg).query(
                            """
                            select id, balance from customers
                            where status = 'VIP' and last_resync >= current_date
                            order by id limit :n
                            """.trimIndent(),
                            "n" to config.pageSize(),
                        ) { it.getString("id") to it.getLong("balance") }
                    },
                    next = { afterId ->
                        jdbc(pg).query(
                            """
                            select id, balance from customers
                            where status = 'VIP' and last_resync >= current_date and id > :after
                            order by id limit :n
                            """.trimIndent(),
                            "after" to afterId, "n" to config.pageSize(),
                        ) { it.getString("id") to it.getLong("balance") }
                    },
                    nextCursor = { page -> page.last().first },
                    continueWhen = { page -> page.size >= config.pageSize() },
                ).filter { (_, balance) -> balance >= config.vipThreshold() }
            },
        ) { (id, balance) ->
            write("legacy.vip-upgrade", args = mapOf("customerId" to id)) {
                legacySystems.onVipUpgrade(id)
                WriteOutcome.Applied
            }
            vips.row(id, balance)
            vipUpgrades.incrementAndGet()
        }
    }

    /** CqlSession живёт в графе внутри CassandraDatabase; сессию отдаёт currentSession(). */
    private fun HandlerScope.loadFromLegacy(ids: List<String>): List<Customer> =
        cassandra(legacy.currentSession()).query(
            "select id, email, status, balance, started_at from customers where id in :ids",
            "ids" to ids,
        ) { row ->
            Customer(
                id = row.getString("id")!!,
                email = row.getString("email")!!,
                status = CustomerStatus.valueOf(row.getString("status")!!),
                balance = row.getLong("balance"),
                startedAt = row.getInstant("started_at")!!,
            )
        }

    private fun computeNewStatus(c: Customer, risk: String?): CustomerStatus = when {
        risk == "HIGH"                      -> CustomerStatus.SUSPENDED
        c.balance >= config.vipThreshold()  -> CustomerStatus.VIP
        c.status == CustomerStatus.INACTIVE -> CustomerStatus.INACTIVE
        else                                -> CustomerStatus.ACTIVE
    }
}
```

Глубина indent'а в горячей петле — 3 (`scoped { transactional { forEach { } } }`), и ни одной
строки инфраструктуры: ни `ExecutorService`, ни `CountDownLatch`, ни `producer.flush()`,
ни `writer.close()`, ни `if (dryRun)`.

## 6. Разбор — что где происходит

### 6.1. Определение миграции: `name` — константа, `plan()` — метод

```kotlin
override val name = "SAMPLE-001"
override fun plan() = migration(name = name, author = "team", onUnhandled = ...) { ... }
```

`name` — константа: по ней runner выбирает миграцию и проверяет дубли, **не строя ни одного
плана**. `plan()` вызывается ровно один раз и только у выбранной миграции — поэтому это метод,
а не свойство: инициализатор свойства выполнился бы у всех компонентов ещё при сборке графа.

Тело `migration { ... }` выполняется немедленно, но только **регистрирует узлы**: ни одна
пользовательская лямбда (`input`, `validate`, `items`, `handle`) при построении не вызывается.
Ошибка в описании плана (нет ни одной стадии, дубликат имени стадии, `parallel = 0`,
два `validate`) вылезет здесь, до первого эффекта, и даст exit-код 2.

`@DslMarker` дополнительно запрещает вызвать `source`/`scoped`/`input`/`output` изнутри `items`
или `handle` — это ошибка компиляции, а не runtime-сюрприз: план неизменяем.

### 6.2. Script-level policy: `LOG_AND_COMPLETE`

```kotlin
migration(name = name, author = "team", onUnhandled = ScriptPolicy.LOG_AND_COMPLETE) { ... }
```

Если вылетит что-то совсем неожиданное (например, баг в `computeNewStatus`), runner не
упадёт с exit-code 1 — залогирует ошибку, зафиксирует в `report.failed`, закроет файлы,
напечатает отчёт, вернёт exit-code 0. Для compliance-скрипта это критично: хоть какой-то
отчёт лучше, чем ничего.

Чего `LOG_AND_COMPLETE` **не** делает: он не продолжает следующие стадии (провал стадии
всегда останавливает прогон) и не превращает неподтверждённые эффекты в ноль — потерянные
сообщения поднимают exit-код независимо от политики.

Параметр билдера перекрывает `migration.defaults.onUnhandled` из HOCON. Хотим fail-fast —
убираем параметр (`FAIL_FAST` — дефолт).

### 6.3. `input` + `validate` — то, что выполняется до стадий

```kotlin
val segments = input("segments") { readCsv(...).groupBy(...) }

validate {
    require(config.batchSize() > 0) { "sample.batchSize должен быть > 0" }
    require(config.vipThreshold() > 0) { "sample.vipThreshold должен быть > 0" }
}
```

`input(name) { }` — ленивое значение прогона: вычисляется при первом `resolve` и кэшируется
до конца прогона. Идентичность — сам объект `Input`, а значение живёт в контексте прогона,
поэтому один и тот же план можно исполнить повторно и не получить кэш прошлого запуска.

`resolve` живёт на `SourceScope`, то есть внутри `items = { }` и `parents = { }`. В `validate { }`
его нет — там `InputScope`, и это осознанно: проверка конфига не должна ходить в БД.

`validate { }` выполняется один раз, **до первой стадии и до любого эффекта**, и объявляется
не более одного раза. Это отдельный узел, а не «первый input», потому что порядок разрешения
input'ов задаёт первый `resolve`, а не порядок объявления, — стадия успела бы выполнить
эффекты с непроверенным конфигом.

### 6.4. Три `output` в билдере

```kotlin
val processed = output("processed.csv",    "id", "oldStatus", "newStatus")
val failed    = output("failed.csv",       "id", "reason")
val vips      = output("vip-upgrades.csv", "id", "balance")
```

Имя резолвится относительно `outputFolder` (`logs/SAMPLE-001/processed.csv` и т.д.), заголовки
пишутся сразу при открытии, файл открывается **один раз на прогон** и закрывается движком.

Почему именно в билдере, а не внутри стадии: в `scoped`-стадии открытие «на каждого родителя»
затирало бы строки предыдущего сегмента. Один выход — один файл — один прогон.

Дальше `processed.row(...)` зовётся откуда угодно, включая параллельные воркеры: `row(...)`
thread-safe. Вне прогона хендл бросает `IllegalStateException`, а не пишет в чужой файл.

Под dry-run файлы **всё равно пишутся**: выход не меняет целевую систему, и скрипт под
репетицией остаётся диагностическим артефактом.

### 6.5. `scoped` — барьер на каждый сегмент

```kotlin
scoped(
    segments,
    name = "resync",
    parents = { loaded -> loaded.keys.asSequence() },
    completionTimeout = config.completionTimeout(),
    parallel = config.parallel(),
    errorThreshold = 100,
    items = { segment -> ... },
) { batch -> ... }
```

Порядок исполнения одного родителя, буквально:

1. открывается сегмент, строится его источник (`items(segment)`);
2. элементы обрабатываются, при `parallel = 8` — окном в 8 воркеров;
3. источник исчерпан → ждём завершения всех воркеров;
4. **барьер**: ждём подтверждения по всем `publish` этого сегмента, не дольше
   `completionTimeout`;
5. закрываются ресурсы, зарегистрированные внутри сегмента через `scopedResource`;
6. только теперь открывается следующий сегмент.

Родители идут **строго последовательно** — параллелизм живёт внутри родителя. Ошибка внутри
сегмента останавливает стадию и до следующего сегмента дело не доходит.

`completionTimeout` ограничивает **только шаг 4** — ожидание подтверждений после исчерпания
источника, а не время чтения и отправки. Десять минут здесь означают «сколько ждём ack'ов
у брокера», а не «сколько работает сегмент».

`errorThreshold = 100` считается **по родителю** и сбрасывается на его границе: сто skip'ов
в одном сегменте прерывают прогон, а сто skip'ов, размазанных по десяти сегментам, — нет.
Значение стадии перекрывает `migration.defaults.errorThreshold`.

### 6.6. Батч — это один item

При `.chunked(500)` единицей учёта, единицей `Skip` и единицей отката является батч:

- `report.processed` считает батчи, а не клиентов;
- в `errors.csv` уедет `List<String>` из 500 id — поэтому сериализатор зарегистрирован
  и на `Customer`, и на `List<String>`;
- `errorThreshold` тоже считает батчи.

Именно поэтому AML-отказ ловится **внутри** батча:

```kotlin
} catch (e: AmlValidationException) {
    auditError(e, c)                          // ровно то, что делает движок под ItemError.Skip
    failed.row(c.id, e.message ?: "AML rejected")
    skippedCustomers.incrementAndGet()
    null
}
```

`auditError(e, item)` — публичный член `RunScope`: пишет строку в `errors.csv` (через
зарегистрированный `includeItem<Customer>`) и стектрейс в `errors.log`. Счётчики отчёта он
не трогает — `processed`/`skipped` остаются счётчиками батчей, поэтому свой `AtomicLong`
тут не роскошь, а необходимость.

Альтернатива — отказаться от `.chunked(...)`: тогда `Skip` теряет ровно одного клиента,
но `where id in :ids` придётся выбросить и Cassandra получит запрос на каждого.

### 6.7. Сначала транзакция, потом `publish`

```kotlin
transactional(jdbc(pg)) { enriched.forEach { ... execute ... } }

enriched.forEach { (c, newStatus) ->
    processed.row(...)
    publish("customers.resync", args = mapOf("customerId" to c.id)) { publisher.publishResync(...) }
}
```

Публикация вынесена **за** транзакцию сознательно: событие, отправленное внутри блока,
который потом откатится, отозвать невозможно — подписчики узнают о статусе, которого в базе
нет. Обратный порядок (сначала commit, потом publish) оставляет худший случай «закоммитили,
но не отправили», а он лечится повторным прогоном.

Про сам `transactional`, три правила, которые лучше узнать из документации, чем из инцидента:

- **Вложенный `transactional` запрещён** — `IllegalStateException`, причём проверка работает
  и под dry-run: репетиция не имеет права быть зелёной там, где бой падает.
- **Внутри `transactional` нельзя гонять параллельные воркеры.** `java.sql.Connection` не
  потокобезопасен, поэтому tx-bound `SqlOps` запоминает поток-владелец и бросает
  `IllegalStateException`, если его дёрнули с другого. Правильный порядок — `transactional`
  внутри обработчика, как здесь, а не стадия внутри транзакции.
- **Под dry-run тело блока выполняется.** Пропускаются отдельные записи внутри него
  (`execute`/`batch`), а сам блок — нет: иначе в репетиции не выполнились бы чтения и не
  посчитались бы записи. В breakdown появляется отдельная строка `jdbc.transactional`
  (реальный `Connection` при этом не открывается).

`query(...)` и `stream(...)` принимают только читающие запросы (`select/with/show/explain/
values/table/describe`) — они не проходят dry-run gate, и `query("insert ... returning id")`
выполнился бы В БОЮ во время репетиции. Нужны строки из пишущего запроса — есть
`executeReturning(sql, ...) { rs -> ... }`: он проходит гейт как обычная запись и под dry-run
возвращает пустой список.

### 6.8. `publish` против `write`

```kotlin
// Асинхронный эффект: исход известен позже, ждём его на барьере.
publish("customers.resync", args = mapOf("customerId" to c.id)) {
    publisher.publishResync(c.id, CustomerResyncEvent(c.id, c.status, newStatus))
}

// Синхронный вызов чужого компонента: исход известен сразу.
write("legacy.vip-upgrade", args = mapOf("customerId" to id)) {
    legacySystems.onVipUpgrade(id)
    WriteOutcome.Applied
}
```

Оба — способ провести вызов, о котором DSL ничего не знает, через dry-run-гейт: ни Kora
`@KafkaPublisher`, ни Kora `@HttpClient` про репетицию не в курсе. Разница в том, когда
известен исход:

| | `write` | `publish` |
|---|---|---|
| Тело возвращает | `WriteOutcome` | `CompletionStage<*>` |
| Исход | сразу | на барьере scope'а |
| Возвращает вызывающему | `WriteResult` | ничего |
| Отказ идёт через | `ItemError` (как обычная ошибка item'а) | барьер: `ScopeEffectsFailed`, мимо `ItemError` |
| Счётчики | `appliedWrites` / `rejectedWrites` | `acknowledgedPublishes` / `failedEffects` / `abandonedPublishes` |
| Под dry-run | тело не вызывается, `DryRunSkipped` | тело не вызывается, future не создаётся |

Метка **константная** (`"customers.resync"`, `"legacy.vip-upgrade"`) — это критично: в отчёте
получается чистый агрегат `customers.resync: 973500`. Если бы метка содержала `${c.id}`,
в breakdown оказалось бы 973500 уникальных строк. Контекст item'а идёт в `args`: они уезжают
в лог (`[DRY-RUN] customers.resync (customerId=CUST-4277)`), но не в ключ агрегата.

Отказ асинхронного эффекта не проходит через `ItemError` и не может: к моменту ответа брокера
item давно посчитан успешным, и `Skip` для него физически неприменим. Каждый отказ поштучно
уезжает в `errors.csv` вместе с тем item'ом, на котором был отправлен, а стадия падает на
барьере.

### 6.9. Классификация ошибок через `ItemError.Handle`

```kotlin
onItemError = ItemError.Handle { e, batch ->
    when (e) {
        is AmlValidationException -> ItemError.Decision.Skip
        is DataIntegrityException -> ItemError.Decision.Fail
        else -> if (batch.size == 1) ItemError.Decision.Fail else ItemError.Decision.Skip
    }
}
```

- **AML-валидация** → `Skip` (в этом скрипте до классификатора она обычно не доходит:
  мы ловим её пер-элементно в §6.6, но политика оставлена как страховка).
- **DataIntegrity** (БД вернула что-то невозможное) → `Fail`. Прерываем стадию — дальше
  смотреть бесполезно, это баг.
- Всё остальное — `Skip`, кроме одиночного батча: если сегмент состоит из одного клиента и
  он падает, продолжать нечего.

Главное отличие от старого `OnError.handle`: классификатор получает **типизированный item**
(`List<String>`, а не `Any?`), поэтому решение может зависеть и от самих данных — без каста.

`Handle` принимает решение **один раз**, без backoff. Retry — уровень Kora `@Retry` внутри
клиента (см. `@Retry("amlVerify")` в §3): до `Handle` тогда долетит только финальный fail
после исчерпания попыток, и для HTTP его удобно разбирать по `e.code`.

Если сам классификатор бросит — его исключение уйдёт наверх, item будет аудитнут и посчитан
в `failed`. Не пиши в `decide` ничего, что может упасть.

### 6.10. Вторая стадия и `pages`

```kotlin
source(name = "vip-webhooks", parallel = 4, onItemError = ItemError.Skip, items = { pages(...) }) { ... }
```

Стадии идут строго последовательно, и провал одной не запускает следующие: webhook'и уйдут
только если ресверка закончилась. Как только стадий больше одной, **имя обязательно у каждой**
— план без имён просто не построится.

Источник второй стадии — `pages(...)` с курсором по `id`, а не `jdbc.stream`: `Sequence`,
которую отдаёт `stream`, валидна только внутри `consume`-блока, а `items = { }` обязан
вернуть последовательность, которую движок будет читать после возврата из лямбды. Побочная
выгода — в отчёте появляются `rawPages`/`rawRows`, то есть «сколько прочитано» отдельно от
«сколько дошло до обработчика»: разница здесь и есть `.filter { balance >= vipThreshold }`.

`continueWhen` и `nextCursor` смотрят на **сырую** страницу, до этого `filter`. Поэтому
страница, целиком отсеянная порогом, источник не завершает — завершает только пустая сырая
страница. Если курсор после непустой страницы не сдвинулся, движок бросит `CursorNotAdvancing`
вместо бесконечного цикла.

### 6.11. Итоговый отчёт

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
  ✓ Applied writes:          147  (legacy.vip-upgrade: 147)
  ✓ Acknowledged publishes:  966 500
  ⊘ Source rows dropped:     3
  ⌀ Source pages read:       12  (rows: 22 400)
───────────────────────────────────────────────────────────
Error details:  logs/SAMPLE-001/errors.csv
Error traces:   logs/SAMPLE-001/errors.log
═══════════════════════════════════════════════════════════
```

Что здесь надо уметь читать:

- **`Processed: 1 947` — это батчи**, не клиенты (размер батча 500, значит клиентов ~973 500).
  Плюс 147 item'ов второй стадии. Счётчики общие на весь прогон, а не на стадию — если нужен
  счётчик клиентов, веди свой `AtomicLong` и выводи его в прогресс-форматтере (мы так вывели
  `vipUpgrades` и `skippedCustomers`).
- **`Applied writes`** — сколько `write { }` вернули `Applied`, с разбивкой по меткам. Рядом
  появилась бы строка `Rejected writes`, если бы тело возвращало `WriteOutcome.Rejected`.
- **`Acknowledged publishes`** — сколько отправок подтвердил брокер, просуммировано по всем
  барьерам сегментов. Если что-то пошло не так, рядом встают `✗ Failed effects` (брокер
  отказал) и `⚠ Unconfirmed effects (abandoned: N, late: M)` — отправленное, но не
  подтверждённое. Последнее поднимает exit-код до 1 независимо от `ScriptPolicy`.
- **`Source rows dropped`** — строки, отброшенные `readCsv(onRowError = Skip)`. Отдельный
  счётчик от `Skipped (errors)`: до стадии они не дошли, в `processed` не входят и в
  `errorThreshold` не считаются.
- **`Source pages read`** — сырые страницы `pages(...)` и строк в них, до пользовательских
  фильтров.
- Числа с разделителем — неразрывающая группировка по тысячам (`1 947`), это формат
  `ReportFormatter`, а не опечатка. Каждая строка печатается только когда ей есть что
  показать: постоянные нули приучили бы глаз их пропускать.
- **Метки времени печатаются со смещением зоны** (`+03:00`). `errors.csv` пишет UTC — по
  смещению одно с другим сопоставляется без гадания.
- Строки `Error details:` / `Error traces:` появляются только если файлы созданы: авто-аудитор
  создаёт их лениво, на первой ошибке.
- Блок `⚠ Warnings:` собирает нефатальное: сбой `close()` у ресурса, не остановившийся за
  30 секунд пул, отсутствие logback на classpath.

Под dry-run вместо строк доставки появляется breakdown (метки отсортированы, порядок
детерминированный):
```
  ⌀ Dry-run skipped writes:    (customers.resync: 973500, jdbc.execute: 1947000, jdbc.transactional: 1947, legacy.vip-upgrade: 147)
```

И по этой строчке видно: сколько именно UPDATE/INSERT ушло бы в Postgres, сколько транзакций
открылось бы, сколько сообщений в Kafka и сколько webhook'ов. 147 VIP-апгрейдов — оценка
масштаба без единого реального сайд-эффекта.

Обратная ситуация — breakdown **пустой** при непустом `Processed` — означает, что ни одна
запись не была перехвачена: значит где-то забыт `write`/`publish` и вызов ушёл в бой
по-настоящему. Runner про это отдельно пишет WARN в лог и предупреждение в отчёт: под
dry-run это единственный наблюдаемый признак такой ошибки.

### 6.12. `outputFolder` — всё в одном месте

```
logs/SAMPLE-001/
├── migration.log          # slf4j root logger — всё что писалось в JVM во время миграции
├── errors.csv             # авто-аудит item-ошибок (CsvFileErrorReporter)
├── errors.log             # стектрейсы по тем же ошибкам
├── processed.csv          # наши output() — у каждого свой файл
├── failed.csv
└── vip-upgrades.csv
```

Папка определяется конфигом `migration.outputFolder` (HOCON, в том числе через
`${?MIGRATION_OUTPUT_FOLDER}`), или дефолтом `logs/<имя миграции>` относительно
CWD JVM. Все файлы внутри `TRUNCATE`-аются при ререн-е (один прогон = один
набор артефактов). Если нужна история — заархивируй папку после прогона
(или подставь timestamped путь через env). Если папку создать не удалось (нет прав,
битый путь) — это мисконфиг, exit-code 2.

`migration.log` собирается через programmatic logback `FileAppender` на root-логгере.
То есть в файле окажется не только наш `log.info/...`, но и весь output Kora, Hikari,
Kafka producer, Cassandra driver и т.д. Это сознательное решение в пользу
compliance-сценариев: хочется иметь полный аудит всего, что происходило в JVM во время
миграции. Если logback не на classpath — runner сообщит warn (и в лог, и в отчёт) и пойдёт
без file-логирования.

---

## 7. Вариации (мини-паттерны)

Ниже — короткие куски кода под разные конкретные случаи. Они не
складываются в один план, но показывают, как DSL ложится на типичные
подзадачи.

### Чтение миллиона строк из Postgres, без батчей

```kotlin
source(
    parallel = 8,
    onItemError = ItemError.Skip,
    items = {
        pages(
            first = { jdbc(orders).query("select id from orders where status = 'STUCK' order by id limit :n", "n" to 5000) { it.getLong("id") } },
            next = { afterId -> jdbc(orders).query("select id from orders where status = 'STUCK' and id > :a order by id limit :n", "a" to afterId, "n" to 5000) { it.getLong("id") } },
            nextCursor = { page -> page.last() },
            continueWhen = { page -> page.size >= 5000 },
        )
    },
) { id ->
    // обработка по одному, без материализации списка. Единица Skip — один заказ.
}
```

Чтение не убегает вперёд: при `parallel > 1` движок берёт permit семафора **до** `next()`,
поэтому источник вычитывается ровно настолько, насколько его успевают потреблять.

### Linear backoff для нестабильной внешней API

Backoff — на уровне типизированного Kora-клиента, не на уровне стадии:

```kotlin
@HttpClient(configPath = "httpClient.sync")
interface SyncClient {
    @HttpRoute(method = "POST", path = "/sync")
    @Retry("syncApi")                        // имя конфига; параметры в HOCON
    fun sync(@Json item: SyncItem)
}

// в обработчике:
write("sync", args = mapOf("id" to item.id)) {
    syncClient.sync(item.toSyncItem())
    WriteOutcome.Applied
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
`RetryManager.get("syncApi").retry(supplier)` со своим backoff. После исчерпания попыток
исключение вылетает в стадию, которая аудитит item в `errors.csv` через `ItemError.Skip`.

### Ресурс на границе сегмента

```kotlin
scoped(
    segments,
    parents = { loaded -> loaded.asSequence() },
    items = { segment ->
        val cursor = legacyStore.openCursor(segment)   // держит соединение
        scopedResource { cursor.close() }              // закроется на границе ЭТОГО сегмента
        cursor.asSequence()
    },
) { row -> /* ... */ }
```

`scopedResource` — это граница scope'а, а `register` — весь прогон. В `scoped`-стадии
разница принципиальна: соединение закроется перед переходом к следующему сегменту, а не
накопится сотней открытых курсоров к концу прогона. Порядок на границе строгий: сначала
барьер подтверждений, потом закрытие.

### Кастомный прогресс с несколькими метриками

```kotlin
val ok = AtomicLong(); val skip = AtomicLong()
val startMs = System.currentTimeMillis()

source(
    onItemError = ItemError.Skip,
    progress = Progress.Custom(1000) { done, total ->
        val elapsedS = ((System.currentTimeMillis() - startMs) / 1000).coerceAtLeast(1)
        "resync: done=$done/${total ?: "?"}  ok=${ok.get()}  skip=${skip.get()}  rate=${done / elapsedS}/s"
    },
    items = { loadItems() },
) { item ->
    try { doWork(item); ok.incrementAndGet() }
    catch (e: Exception) { skip.incrementAndGet(); throw e }
}
```

`total` в форматтере — `null`: источник стадии это `Sequence`, её длина заранее неизвестна,
и врать про проценты движок не станет. Кроме `Custom` есть `Progress.Every(n)` (дефолтный
формат с другим периодом), `Progress.Default` и `Progress.Off`.

### Comparison-архетип компактно

```kotlin
val diffs = output("diffs.csv", "id", "reason")

source(
    parallel = 4,
    onItemError = ItemError.Skip,
    items = { readCsv(config.inputFile()) { it.getValue("id") }.chunked(200) },
) { batch ->
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
только по id. Полная версия — [comparison-archetype.md](comparison-archetype.md).

### Запуск только read-части (dry-run) для smoke-теста на проде

```bash
MIGRATION_RUN=SAMPLE-001 MIGRATION_DRY_RUN=true ./gradlew run
```

Заблокировано всё, что проходит гейт: `write { }`, `publish { }`,
`jdbc.execute/batch/executeReturning`, `cassandra.execute/batch`, `kafka.publish`,
`http.post/put/patch/delete`. Всё, что read — выполняется. Тела `transactional`-блоков тоже
выполняются, пропускаются лишь записи внутри них. На проде читать можно, писать — нельзя.
Отчёт покажет, что именно произошло бы.

### Threshold-based abort

```hocon
migration.defaults.errorThreshold = 500
```

Если SKIP-ов накопится больше 500 — прогон прервётся с exit-code `1`. Защита от «тихо
пропустили 90% данных, но вроде прошли». Порог считается по стадии (в `scoped` — по
родителю) и сбрасывается на её границе; аргумент стадии `errorThreshold` перекрывает
значение из HOCON. Строки, отброшенные `readCsv(onRowError = Skip)`, сюда не входят —
у них свой счётчик `sourceSkipped`.

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
- `1` — `FAIL_FAST` (в нашем плане — `DataIntegrityException`), превышение `errorThreshold`,
  либо оставшиеся неподтверждённые эффекты.
- `2` — misconfiguration: имя скрипта не найдено, дубликат имён, битые значения
  `migration.defaults.*`, невозможно создать `outputFolder`, план не построился.

Код возврата отдаётся через `exitProcess`. Если runner поднимается внутри теста или внутри
приложения, которое живёт дальше, объяви в графе компонент `MigrationExit` — код уедет в него,
а JVM останется жива (см. [customization.md](customization.md) и
[`KoraWireUpTest`](../../example/src/test/kotlin/io/github/dsudomoin/migration/example/KoraWireUpTest.kt)).

---

## 9. Галочка: что покрыто

- [x] `MigrationDefinition` + `@Component` — регистрация в графе через `All<MigrationDefinition>`.
- [x] `plan()` как метод, `name` как константа — выбор миграции без построения планов.
- [x] `migration(name, author, onUnhandled = ScriptPolicy.LOG_AND_COMPLETE)`.
- [x] `input(name) { }` + `resolve(...)` — ленивое значение прогона.
- [x] `validate { }` — проверка конфига до первой стадии.
- [x] `output(filename, vararg headers)` × 3 — объявлены в билдере, auto-close.
- [x] `scoped(parents, completionTimeout, parallel, errorThreshold, onItemError, progress)` — барьер на родителя.
- [x] `source(name, parallel, onItemError)` — вторая стадия, запускается только после успеха первой.
- [x] `pages(first, next, nextCursor, continueWhen)` — курсорный источник, `rawPages`/`rawRows`.
- [x] `.chunked(N)` — батчинг обычным `Sequence.chunked`.
- [x] `readCsv(path, onRowError = ItemError.Skip)` — битая строка не валит прогон, `sourceSkipped`.
- [x] `jdbc(db).query/execute` + `transactional { }` — реальный tx-scope с rollback.
- [x] `cassandra(session).query(...)` через `CassandraDatabase.currentSession()`.
- [x] `publish(name, args) { CompletionStage }` — Kafka под барьером стадии.
- [x] `write(name, args) { WriteOutcome }` — типизированный Kora `@HttpClient` через dry-run гейт.
- [x] Kora типизированный `@HttpClient` для read — прямой вызов без обёртки.
- [x] `ItemError.Handle { e, item -> Skip/Fail }` — классификатор с типизированным item'ом.
- [x] `errorThreshold` на стадии — перекрывает HOCON, считается по родителю.
- [x] `auditError(e, item)` — пер-элементный аудит внутри батча.
- [x] `errors.includeItem<T> { ... }` — per-type сериализаторы (`Customer`, `List<String>`, `Map<String,String>`).
- [x] Kora `@Retry` на `AmlService.verify` — linear backoff внутри клиента, не в DSL.
- [x] `Progress.Custom(n) { done, total -> ... }` — кастомный форматтер.
- [x] `dryRunSkipped` / `appliedWrites` / `acknowledgedPublishes` / `sourceSkipped` / `rawPages` в отчёте.
- [x] Dry-run через env + строку `dryRun = ${?MIGRATION_DRY_RUN}`, без кода `if (dryRun)`.
- [x] `MIGRATION_RUN` env-override для HOCON `migration.run`.

**Что не покрыто в этом плане, но есть в либе:**
- `writeRows(name, args) { rowCount }` — запись, сообщающая число изменённых строк:
  `0` → `Rejected("no rows matched")`. Для `UPDATE ... WHERE`; см.
  [correction-archetype.md](correction-archetype.md).
- `WriteOutcome.Rejected(reason)` — ожидаемый отрицательный исход записи, отдельно от сбоя.
- `scopedResource { }` — закрытие на границе родителя (см. §7).
- `http(call).post/patch/put/delete` — функциональный wrapper для ad-hoc HTTP, гейт внутри op.
  Не-2xx поднимает `HttpStatusException`. См. [http-backfill-archetype.md](http-backfill-archetype.md).
- `jdbc(db).executeReturning(sql, ...) { rs -> ... }` — пишущий запрос, возвращающий строки.
- `jdbc(db).stream(sql, ...) { rows -> ... }` — курсор БД внутри обработчика; источником
  стадии быть не может (`Sequence` валидна только внутри `consume`).
- `topic(producer, name).send/sendAsync` и `kafka(producer).publish/publishAsync` — сырой
  продюсер; `sendAsync` отдаёт future, который принимает `publish { }`.
- `RunScope.shared(key) { ... }` — мемоизация собственных op-хендлов на прогон
  (см. [customization.md](customization.md) §1).
- `RunScope.register(closeable)` — свой ресурс на весь прогон.
- `guardWrite(label, args, dryRunDefault) { }` — dry-run гейт **внутри** своей операции.
- `Progress.Off` / `Progress.Every(n)` — другие режимы прогресса.
- `ScriptPolicy.FAIL_FAST` — дефолт, мы его переопределяем.
- `MigrationExit`, `@Tag(MigrationExecutor)` — перехват exit-кода и свой пул потоков.
