# Пример: Resend-архетип

**Задача.** По тикету SAMPLE-001 нужно заново отправить в Kafka события по
«провисшим» заказам из Postgres. Для каждого заказа — дёрнуть сервис обогащения
(узнать актуальный статус клиента), собрать событие и опубликовать в топик
`orders.resync`. Провальные элементы складываются в аудит-CSV, чтобы потом
разобраться руками.

Полноценный скрипт с использованием Migration DSL умещается в ~15 строк тела
`migrate()`. Ниже — всё необходимое для того, чтобы это работало в проекте на
Kora: код, конфиг и то, как запускать.

---

## 1. Структура проекта пользователя

```
my-service/
├── build.gradle.kts
├── src/main/kotlin/com/example/migrations/
│   ├── App.kt                          # @KoraApp entry-point
│   ├── OrderResyncEvent.kt             # event-схема (dto)
│   ├── EnrichmentService.kt            # Kora @HttpClient
│   ├── SampleConfig.kt              # @ConfigSource конфиг скрипта
│   └── SampleMigration.kt                    # сам скрипт: Migration + @Component
└── src/main/resources/application.conf
```

## 2. `build.gradle.kts` — подключаем либу

```kotlin
plugins {
    kotlin("jvm") version "2.1.20"
    kotlin("kapt") version "2.1.20"
    application
}

kotlin { jvmToolchain(21) }

application {
    mainClass = "com.example.migrations.AppKt"
}

dependencies {
    // Наша либа
    implementation("io.github.dsudomoin.migration:migration-dsl-core:0.2.0")
    implementation("io.github.dsudomoin.migration:migration-dsl-kora:0.2.0")

    // Kora — подключаем только то, что реально используем
    implementation("ru.tinkoff.kora:config-hocon:1.1.25")
    implementation("ru.tinkoff.kora:application-graph:1.1.25")
    implementation("ru.tinkoff.kora:database-jdbc:1.1.25")
    implementation("ru.tinkoff.kora:kafka:1.1.25")
    implementation("ru.tinkoff.kora:http-client-jdk:1.1.25")

    // Драйвер БД
    runtimeOnly("org.postgresql:postgresql:42.7.7")

    kapt("ru.tinkoff.kora:annotation-processors:1.1.25")
}
```

## 3. `application.conf` — глобальная настройка

```hocon
migration {
  run = ${?MIGRATION_RUN}           # env-override: MIGRATION_RUN=SAMPLE-001
  dryRun = false
  dryRun = ${?MIGRATION_DRY_RUN}    # MIGRATION_DRY_RUN=true для прогона без побочных эффектов

  # outputFolder опционально — дефолт logs/${migration.run}
  outputFolder = ${?MIGRATION_OUTPUT_FOLDER}

  defaults {
    onUnhandled    = FAIL_FAST
    progressEvery  = 500
    parallel       = 1
    errorThreshold = 0
  }

  report.asciiOnly = false
}

# Ваш JDBC datasource — стандартно для Kora
db {
  jdbcUrl  = "jdbc:postgresql://localhost:5432/orders"
  username = ${DB_USER}
  password = ${DB_PASSWORD}
  maxPoolSize = 4
}

# Kora @KafkaPublisher: bootstrap-серверы + имя resync-топика
kafka.orders.publisher {
  driverProperties { "bootstrap.servers" = ${KAFKA_BOOTSTRAP} }
  resyncTopic { topic = "orders.resync" }
}

# Kora HttpClient для сервиса обогащения
httpClient.enrichment {
  url = ${ENRICHMENT_URL}
  requestTimeout = 5s
}

# Наш пользовательский конфиг скрипта — отдельная секция
sample {
  batchSize  = 200
  parallel   = 4
  # Имя origin-а, которое включаем в событие. null = все.
  originFilter = null
  originFilter = ${?TASK42000_ORIGIN_FILTER}
}
```

## 4. Event-схема

```kotlin
// com/example/migrations/OrderResyncEvent.kt
package com.example.migrations

data class OrderResyncEvent(
    val orderId: Long,
    val customerId: String,
    val customerStatus: String,  // получаем из EnrichmentService
    val amount: Long,
    val origin: String,
)
```

## 5. Kora @KafkaPublisher для resync-топика

```kotlin
// com/example/migrations/OrdersPublisher.kt
package com.example.migrations

import org.apache.kafka.clients.producer.RecordMetadata
import ru.tinkoff.kora.json.common.annotation.Json
import ru.tinkoff.kora.kafka.common.annotation.KafkaPublisher

@KafkaPublisher("kafka.orders.publisher")
interface OrdersPublisher {

    @KafkaPublisher.Topic("kafka.orders.publisher.resyncTopic")
    fun publishResync(key: String, @Json value: OrderResyncEvent): RecordMetadata
}
```

## 5b. Kora HTTP client для сервиса обогащения

```kotlin
// com/example/migrations/EnrichmentService.kt
package com.example.migrations

import ru.tinkoff.kora.http.client.common.annotation.HttpClient
import ru.tinkoff.kora.http.common.annotation.HttpRoute
import ru.tinkoff.kora.http.common.annotation.Path

@HttpClient(configPath = "httpClient.enrichment")
interface EnrichmentService {

    @HttpRoute(method = "GET", path = "/customers/{id}/status")
    @Retry("httpClient.enrichment.status")    // имя конфига; параметры в HOCON
    fun customerStatus(@Path("id") id: String): StatusResponse

    data class StatusResponse(val status: String)
}
```

В `application.conf` соседним блоком — параметры retry (Kora resilient = linear backoff,
не exponential):

```hocon
resilient.retry.httpClient.enrichment.status {
  delay     = "500ms"   # пауза перед первым retry
  attempts  = 2         # 2 retry после оригинала → 3 попытки всего
  delayStep = "500ms"   # → waits: 500ms, 1000ms
}
```

## 6. Конфиг скрипта — типизированный

```kotlin
// com/example/migrations/SampleConfig.kt
package com.example.migrations

import ru.tinkoff.kora.config.common.annotation.ConfigSource

@ConfigSource("sample")
data class SampleConfig(
    var batchSize: Int,
    var parallel: Int,
    var originFilter: String?,
)
```

## 7. Сам скрипт

```kotlin
// com/example/migrations/SampleMigration.kt
package com.example.migrations

import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.forEach
import io.github.dsudomoin.migration.kora.ops.jdbc
import io.github.dsudomoin.migration.mutation
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory

@Component
class SampleMigration(
    private val orders: JdbcConnectionFactory,
    private val enrichment: EnrichmentService,
    private val publisher: OrdersPublisher,
    private val config: SampleConfig,
) : Migration(name = "SAMPLE-001", author = "team") {

    override fun MigrationContext.migrate() {
        errors.includeItem<Order> { "order=${it.id}, customer=${it.customerId}" }

        val stuck = jdbc(orders).query(
            """
            select id, customer_id, amount, origin
            from orders
            where status = 'STUCK'
              and (:origin::text is null or origin = :origin)
            """.trimIndent(),
            "origin" to config.originFilter,
        ) { rs ->
            Order(
                id = rs.getLong("id"),
                customerId = rs.getString("customer_id"),
                amount = rs.getLong("amount"),
                origin = rs.getString("origin"),
            )
        }

        forEach(stuck,
                chunk = config.batchSize,
                parallel = config.parallel,
                onError = OnError.Skip) { batch ->
            batch.forEach { order ->
                val status = enrichment.customerStatus(order.customerId).status   // ретраит Kora @Retry внутри
                mutation("orders.resync", args = mapOf("orderId" to order.id)) {
                    publisher.publishResync(
                        order.id.toString(),
                        OrderResyncEvent(order.id, order.customerId, status, order.amount, order.origin),
                    )
                }
            }
        }
    }

    private data class Order(val id: Long, val customerId: String, val amount: Long, val origin: String)
}
```

## 8. `@KoraApp` — собираем граф

```kotlin
// com/example/migrations/App.kt
package com.example.migrations

import io.github.dsudomoin.migration.kora.MigrationModule
import ru.tinkoff.kora.application.graph.KoraApplication
import ru.tinkoff.kora.common.KoraApp
import ru.tinkoff.kora.config.hocon.HoconConfigModule
import ru.tinkoff.kora.database.jdbc.JdbcDatabaseModule
import ru.tinkoff.kora.http.client.jdk.JdkHttpClientModule
import ru.tinkoff.kora.kafka.common.producer.KafkaProducerModule

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

(`AppGraph` — то, что Kora KSP сгенерирует из интерфейса `App`.)

---

## 9. Как запускать

### Прогон в dry-run (не шлёт в Kafka, не меняет БД, но читает orders и дёргает enrichment):

```bash
MIGRATION_RUN=SAMPLE-001 \
MIGRATION_DRY_RUN=true \
DB_USER=app DB_PASSWORD=secret \
KAFKA_BOOTSTRAP=kafka.prod:9092 \
ENRICHMENT_URL=https://enrichment.internal \
./gradlew run
```

В логе увидишь:
```
INFO  [SAMPLE-001] [forEach] progress: 500/8421 (5%)  elapsed=12s  rate=41/s
INFO  [SAMPLE-001] [DRY-RUN] mutation:orders.resync (orderId=42)
INFO  [SAMPLE-001] [DRY-RUN] mutation:orders.resync (orderId=43)
...
═══════════════════════════════════════════════════════════
Migration: SAMPLE-001  (author: team)
Started:   2026-04-24 14:15:32
Finished:  2026-04-24 14:18:04
Duration:  2m 32s
Mode:      DRY-RUN
───────────────────────────────────────────────────────────
Processed:                 42
  ✓ Successful:            42
  ⊘ Skipped (errors):       0
  ✗ Failed:                 0
  ⌀ Dry-run skipped writes: (mutation:orders.resync: 8421)
───────────────────────────────────────────────────────────
Error details:  logs/SAMPLE-001/errors.csv
Error traces:   logs/SAMPLE-001/errors.log
═══════════════════════════════════════════════════════════
```

Из `Dry-run skipped writes: (mutation:orders.resync: 8421)` видно, сколько сообщений
ушло бы в Kafka, если бы не dry-run. Ни одного реального сайд-эффекта.

### Боевой прогон:

```bash
MIGRATION_RUN=SAMPLE-001 \
DB_USER=app DB_PASSWORD=secret \
KAFKA_BOOTSTRAP=kafka.prod:9092 \
ENRICHMENT_URL=https://enrichment.internal \
./gradlew run
```

Отличие в отчёте:
```
Mode:      REAL
───────────────────────────────────────────────────────────
Processed:                 42           # батчей (по 200 = 8421 записей)
  ✓ Successful:            42
  ⊘ Skipped (errors):      0
  ✗ Failed:                0
───────────────────────────────────────────────────────────
```

Exit-code `0` — всё гладко. При `Fail` — `1`. При unknown migration / duplicate name / битом HOCON — `2`.

---

## 10. Что здесь показано

| Фича DSL | Где в скрипте |
|---|---|
| Kora-инъекция источников | 4 зависимости через constructor |
| Типизированный конфиг | `SampleConfig` как `@ConfigSource` |
| Автоматическая регистрация | `@Component` — runner находит через `All<Migration>` |
| Чтение из Postgres | `jdbc(orders).query(..., mapper)` |
| HTTP-вызов (read) | `enrichment.customerStatus(...)` напрямую, без обёрток |
| Публикация в Kafka с dry-run gate | `@KafkaPublisher.Topic` + `mutation("orders.resync", args = ...) { publisher.publishResync(...) }` |
| Batch + параллелизм | `forEach(chunk, parallel, onError = OnError.Skip)` |
| Retry для transient HTTP | Kora `@Retry` на `EnrichmentService.customerStatus` — backoff внутри клиента, не на уровне DSL |
| Авто error-reporting | `errors.includeItem<Order> { ... }` — одна строка |
| Авто progress-лог | ничего не пишем — Default каждые 500 элементов |
| Итоговый отчёт | печатает сам runner |
| Dry-run | `MIGRATION_DRY_RUN=true` — никакого `if` в коде |

## 11. Что НЕ нужно писать

- Никаких `try/catch` — `OnError.Skip` + Kora `@Retry` на клиенте делают всё. Провалившиеся заказы попадают в `logs/SAMPLE-001/errors.csv` автоматически.
- Никаких `Lists.partition` — `chunk = 200` в `forEach`.
- Никаких `ExecutorService` — `parallel = 4` в `forEach`.
- Никаких `if (dryRun)` — DSL сам решает, что делать с `mutation { publisher.publishResync(...) }`.
- Никаких ручных writer.close() / try-with-resources — нет CSV-файлов в этом скрипте (если бы были — `val out = openCsv(path, headers...)` сверху, а runner закрыл бы сам).
- Никакого ручного подсчёта processed/skipped — `MigrationReport` копит автоматически.

## 12. Вариации

**Correction-архетип** (чтение CSV + update в БД): меняешь `readCsv(...) { ... }` → `forEach { jdbc(orders).execute("update...") }`, убираешь Kafka.

**Comparison-архетип** (два кластера, пишем расхождения): `cassandra(primary).query(...)` + `cassandra(replica).query(...)` + `openCsv("failed.csv", "contract", "reason")` сверху для аудита. См. `kora/src/test/kotlin/io/migration/kora/pilot/ComparisonPilotTest.kt`.

**REST-operation**: `forEach(items) { http(client).post("/sync", body) }` или типизированный Kora-клиент внутри `mutation("sync.order", args = mapOf("id" to id)) { ... }` для write-вызовов (label — константа, контекст item'а — в args; см. USER_GUIDE.md §13).

**Command sending**: в теле `migrate()` генерируешь данные (например, из CSV) и сразу `kafka(producer).publish(...)` в `forEach`. Полезно когда из одного скрипта пишешь в **разные** топики и не хочешь городить `@KafkaPublisher.Topic` на каждый.
