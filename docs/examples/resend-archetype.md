# Пример: Resend-архетип

**Задача.** По тикету SAMPLE-001 нужно заново отправить в Kafka события по
«провисшим» заказам из Postgres. Для каждого заказа — дёрнуть сервис обогащения
(узнать актуальный статус клиента), собрать событие и опубликовать в топик
`orders.resync`. Провальные элементы складываются в аудит-CSV, чтобы потом
разобраться руками.

Полноценный скрипт с использованием Migration DSL умещается в ~15 строк тела
`migrate()`. Ниже — всё необходимое для того, чтобы это работало в проекте на
Kora: код, конфиг и то, как запускать.

> Живой аналог всего описанного ниже (без Kafka и HTTP, но с работающим графом и
> тестами) лежит в модуле `example/` этого репозитория.

---

## 1. Структура проекта пользователя

```
my-service/
├── build.gradle.kts
├── src/main/kotlin/com/example/migrations/
│   ├── App.kt                          # @KoraApp entry-point
│   ├── OrderResyncEvent.kt             # event-схема (dto)
│   ├── EnrichmentService.kt            # Kora @HttpClient
│   ├── OrdersPublisher.kt              # Kora @KafkaPublisher
│   ├── SampleConfig.kt                 # @ConfigSource конфиг скрипта
│   └── SampleMigration.kt              # сам скрипт: Migration + @Component
└── src/main/resources/application.conf
```

## 2. `build.gradle.kts` — подключаем либу

Кодогенерация Kora для Kotlin — **только KSP**. `kapt` и `annotationProcessor` не
используются: Kora-процессоры для Kotlin поставляются артефактом
`ru.tinkoff.kora:symbol-processors`.

```kotlin
plugins {
    kotlin("jvm") version "2.1.20"
    id("com.google.devtools.ksp") version "2.1.20-1.0.32"
    application
}

kotlin { jvmToolchain(21) }

application {
    mainClass = "com.example.migrations.AppKt"
}

dependencies {
    // Наша либа
    implementation("io.github.dsudomoin.migration:migration-dsl-core:0.1.0")
    implementation("io.github.dsudomoin.migration:migration-dsl-kora:0.1.0")

    // Kora — подключаем только то, что реально используем
    implementation("ru.tinkoff.kora:common:1.1.25")
    implementation("ru.tinkoff.kora:config-common:1.1.25")
    implementation("ru.tinkoff.kora:config-hocon:1.1.25")
    implementation("ru.tinkoff.kora:application-graph:1.1.25")
    implementation("ru.tinkoff.kora:database-jdbc:1.1.25")
    implementation("ru.tinkoff.kora:kafka:1.1.25")
    implementation("ru.tinkoff.kora:json-module:1.1.25")     // @Json-значение публикуемого события
    implementation("ru.tinkoff.kora:http-client-jdk:1.1.25")
    implementation("ru.tinkoff.kora:resilient-kora:1.1.25")  // @Retry на клиенте обогащения

    // Драйвер БД
    runtimeOnly("org.postgresql:postgresql:42.7.7")
    // Runner пишет logs/<migration>/migration.log через logback-аппендер на root-логгере.
    // Без logback на classpath он предупредит и продолжит без файлового лога.
    runtimeOnly("ch.qos.logback:logback-classic:1.5.16")

    ksp("ru.tinkoff.kora:symbol-processors:1.1.25")
}
```

## 3. `application.conf` — глобальная настройка

**Переменные окружения читаются только через HOCON-подстановку `${?VAR}`.** В либе
нет ни одного `System.getenv`: если строки `dryRun = ${?MIGRATION_DRY_RUN}` в конфиге
нет, запуск с `MIGRATION_DRY_RUN=true` пройдёт **в бою**, с реальными записями.
Поэтому обе подстановки — обязательный минимум любого конфига.

```hocon
migration {
  run    = ${?MIGRATION_RUN}         # env-override: MIGRATION_RUN=SAMPLE-001
  dryRun = ${?MIGRATION_DRY_RUN}     # MIGRATION_DRY_RUN=true — прогон без побочных эффектов

  # outputFolder опционально — дефолт logs/<имя миграции>
  outputFolder = ${?MIGRATION_OUTPUT_FOLDER}

  defaults {
    onUnhandled    = FAIL_FAST
    progressEvery  = 500
    # Дефолт аргумента forEach(parallel = ...), а НЕ размер пула: пул runner'а cached
    # и выдаёт столько воркеров, сколько запросил конкретный forEach.
    parallel       = 1
    errorThreshold = 0
  }

  errorReporting {
    includeStackTrace = true
    maxItemReprLength = 500
  }

  report.asciiOnly = false
}

# Ваш JDBC datasource — стандартно для Kora. poolName обязателен (в JdbcDatabaseConfig
# у него нет дефолта): без него сборка графа падает на «config value not found».
db {
  jdbcUrl     = "jdbc:postgresql://localhost:5432/orders"
  username    = ${DB_USER}
  password    = ${DB_PASSWORD}
  poolName    = "migration-orders"
  maxPoolSize = 4
}

# Kora @KafkaPublisher: секция продюсера и секция топика — соседи, не вложены друг в друга.
kafka.orders {
  publisher {
    driverProperties { "bootstrap.servers" = ${KAFKA_BOOTSTRAP} }
  }
  resyncTopic { topic = "orders.resync" }
}

# Kora HttpClient для сервиса обогащения
httpClient.enrichment {
  url = ${ENRICHMENT_URL}
  requestTimeout = 5s
}

# Наш пользовательский конфиг скрипта — отдельная секция
sample {
  batchSize = 200
  parallel  = 4
  # Имя origin-а, которое включаем в фильтр. Отсутствует = все.
  originFilter = ${?SAMPLE_ORIGIN_FILTER}
}
```

## 4. Event-схема

```kotlin
// com/example/migrations/OrderResyncEvent.kt
package com.example.migrations

import ru.tinkoff.kora.json.common.annotation.Json

@Json
data class OrderResyncEvent(
    val orderId: Long,
    val customerId: String,
    val customerStatus: String,  // получаем из EnrichmentService
    val amount: Long,
    val origin: String,
)
```

`@Json` на самом DTO обязателен: он заставляет KSP сгенерировать `JsonWriter` для типа.
Одной аннотации на параметре публикующего метода мало.

## 5. Kora @KafkaPublisher для resync-топика

```kotlin
// com/example/migrations/OrdersPublisher.kt
package com.example.migrations

import org.apache.kafka.clients.producer.RecordMetadata
import ru.tinkoff.kora.json.common.annotation.Json
import ru.tinkoff.kora.kafka.common.annotation.KafkaPublisher

@KafkaPublisher("kafka.orders.publisher")
interface OrdersPublisher {

    @KafkaPublisher.Topic("kafka.orders.resyncTopic")
    fun publishResync(key: String, @Json value: OrderResyncEvent): RecordMetadata
}
```

Чтобы это собралось и поднялось в графе, нужны три вещи: артефакт `kafka` +
`KafkaModule` в `@KoraApp`, артефакт `json-module` + `JsonModule` в `@KoraApp`
(из-за `@Json`-значения) и KSP-процессор из §2.

## 5b. Kora HTTP client для сервиса обогащения

```kotlin
// com/example/migrations/EnrichmentService.kt
package com.example.migrations

import ru.tinkoff.kora.http.client.common.annotation.HttpClient
import ru.tinkoff.kora.http.common.annotation.HttpRoute
import ru.tinkoff.kora.http.common.annotation.Path
import ru.tinkoff.kora.json.common.annotation.Json
import ru.tinkoff.kora.resilient.retry.annotation.Retry

@HttpClient(configPath = "httpClient.enrichment")
interface EnrichmentService {

    @HttpRoute(method = "GET", path = "/customers/{id}/status")
    @Json                                  // тело ответа читается JSON-ридером
    @Retry("enrichment")                   // имя конфига; параметры в HOCON
    fun customerStatus(@Path("id") id: String): StatusResponse

    @Json
    data class StatusResponse(val status: String)
}
```

`@Retry` живёт в отдельном модуле: артефакт `ru.tinkoff.kora:resilient-kora`,
импорт `ru.tinkoff.kora.resilient.retry.annotation.Retry`, плюс `ResilientModule`
в `@KoraApp` (см. §8). Без модуля граф не соберётся.

Параметры retry идут соседним блоком в `application.conf`. **Имя из аннотации — это
ключ в map'е `resilient.retry`,** поэтому берите односегментное имя: `resilient.retry.enrichment`
даёт ровно ключ `enrichment`, а вот `resilient.retry.httpClient.enrichment.status`
в HOCON развернётся во вложенные объекты и по имени `httpClient.enrichment.status`
уже не найдётся.

```hocon
resilient.retry.enrichment {
  delay     = "500ms"   # пауза перед первым retry
  attempts  = 2         # 2 retry после оригинала → 3 попытки всего
  delayStep = "500ms"   # linear backoff → waits: 500ms, 1000ms
}
```

Kora resilient умеет только linear backoff (`delay + (n-1)*delayStep`), настоящего
exponential встроенно нет.

## 6. Конфиг скрипта — типизированный

`@ConfigSource` в Kora описывается **интерфейсом с методами**: дефолт задаётся телом
default-метода, необязательное значение — nullable-типом. Дефолтные значения параметров
конструктора Kotlin-класса KSP не видит, и такой ключ становится обязательным.

```kotlin
// com/example/migrations/SampleConfig.kt
package com.example.migrations

import ru.tinkoff.kora.config.common.annotation.ConfigSource

@ConfigSource("sample")
interface SampleConfig {
    fun batchSize(): Int = 200
    fun parallel(): Int = 4
    fun originFilter(): String?      // отсутствует в конфиге → null
}
```

## 7. Сам скрипт

```kotlin
// com/example/migrations/SampleMigration.kt
package com.example.migrations

import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.error.includeItem
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
            "origin" to config.originFilter(),
        ) { rs ->
            Order(
                id = rs.getLong("id"),
                customerId = rs.getString("customer_id"),
                amount = rs.getLong("amount"),
                origin = rs.getString("origin"),
            )
        }

        forEach(stuck,
                chunk = config.batchSize(),
                parallel = config.parallel(),
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

**Важно про `chunk` + `OnError.Skip`.** Единицей учёта и единицей отката здесь является
**батч**, а не заказ: исключение на 137-м заказе из 200 обрывает весь батч, первые 136
уже отправлены, остальные 63 не будут отправлены никогда, а в `errors.csv` уедет одна
строка — весь `List<Order>`. Если такая гранулярность не устраивает, есть два пути:

- убрать `chunk` (`forEach(stuck, parallel = ...)`) — тогда единица = один заказ,
  и `Skip` теряет ровно один заказ;
- ловить ошибку внутри батча самому:

```kotlin
batch.forEach { order ->
    try {
        val status = enrichment.customerStatus(order.customerId).status
        mutation("orders.resync", args = mapOf("orderId" to order.id)) { /* ... */ }
    } catch (e: Exception) {
        auditError(e, order)     // строка в errors.csv по конкретному заказу
    }
}
```

`chunk` берут ради round-trip'ов (`where id in :ids`, bulk-insert). Если тело батча —
это цикл независимых вызовов, как здесь, честнее item-by-item.

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
import ru.tinkoff.kora.json.module.JsonModule
import ru.tinkoff.kora.kafka.common.KafkaModule
import ru.tinkoff.kora.resilient.ResilientModule

@KoraApp
interface App :
    HoconConfigModule,
    JdbcDatabaseModule,
    KafkaModule,
    JsonModule,
    JdkHttpClientModule,
    ResilientModule,
    MigrationModule

fun main() {
    KoraApplication.run { AppGraph.graph() }
}
```

(`AppGraph` — то, что Kora KSP сгенерирует из интерфейса `App`.)

`MigrationModule` — единственное, что нужно от нашей либы: секцию `migration { ... }`
он читает сам, а runner помечен `@Root` и создаётся графом без явных зависимостей.

**Никакого `KafkaProducerModule` в Kora не существует** — если он остался в старых
примерах, это опечатка, граф с ним не соберётся. Типизированному `@KafkaPublisher`
нужен `KafkaModule`. Если же нужен сырой `org.apache.kafka.clients.producer.Producer`
для `kafka(producer)` / `topic(producer, name)`, его надо принести в граф самому:

```kotlin
// bootstrap-серверы берём из HOCON, а не из System.getenv — как и всё остальное
@ConfigSource("rawProducer")
interface RawProducerConfig {
    fun bootstrapServers(): String
}

@Module
interface RawProducerModule {
    fun rawProducer(config: RawProducerConfig): Producer<String, ByteArray> =
        KafkaProducer(
            Properties().apply {
                put("bootstrap.servers", config.bootstrapServers())
                put("key.serializer", StringSerializer::class.java.name)
                put("value.serializer", ByteArraySerializer::class.java.name)
            },
        )
}
```

(`@Module`-интерфейс нужно добавить в список родителей `@KoraApp`, иначе граф о нём не узнает;
`rawProducer { bootstrapServers = ${KAFKA_BOOTSTRAP} }` — соседняя секция в `application.conf`.)

Второй вариант — взять продюсер у сгенерированного Kora-publisher'а: реализация
`@KafkaPublisher`-интерфейса имплементирует `ru.tinkoff.kora.kafka.common.producer.GeneratedPublisher`,
у которого есть `producer(): Producer<ByteArray, ByteArray>` (сериализация тогда на вас).

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
INFO  [SAMPLE-001] [forEach] progress: 20/43 (46%)  elapsed=12s  rate=1/s
INFO  [SAMPLE-001] [DRY-RUN] mutation:orders.resync (orderId=42)
INFO  [SAMPLE-001] [DRY-RUN] mutation:orders.resync (orderId=43)
...
═══════════════════════════════════════════════════════════
Migration: SAMPLE-001  (author: team)
Started:   2026-04-24 14:15:32 +03:00
Finished:  2026-04-24 14:18:04 +03:00
Duration:  2m 32s
Mode:      DRY-RUN
───────────────────────────────────────────────────────────
Processed:                 43
  ✓ Successful:            43
  ⊘ Skipped (errors):      0
  ✗ Failed:                0
  ⌀ Dry-run skipped writes:    (mutation:orders.resync: 8421)
───────────────────────────────────────────────────────────
═══════════════════════════════════════════════════════════
```

Что тут читать:

- `Processed: 43` — это **батчи** (8421 заказ по 200 в батче), не заказы.
- `Dry-run skipped writes` показывает, сколько сообщений ушло бы в Kafka. Ни одного
  реального сайд-эффекта.
- Метки времени печатаются со смещением зоны (`+03:00`) — `errors.csv` пишет UTC,
  и по смещению одно с другим сопоставляется без гадания.
- Строк `Error details:` / `Error traces:` нет, потому что `errors.csv` создаётся
  лениво — только на первой ошибке.
- Если бы breakdown оказался **пустым** при непустом `Processed`, runner напечатал бы
  WARN и добавил бы предупреждение в отчёт: под dry-run это единственный наблюдаемый
  признак того, что где-то забыли `mutation { }` и запись ушла в бой по-настоящему.

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
Processed:                 43           # батчей (по 200 = 8421 заказ)
  ✓ Successful:            43
  ⊘ Skipped (errors):      0
  ✗ Failed:                0
───────────────────────────────────────────────────────────
```

Exit-коды: `0` — всё гладко; `1` — `Fail`, превышен `errorThreshold` либо были отказы
async-доставки в Kafka; `2` — мисконфиг (неизвестное имя, дубликат имён, битые значения
`migration.defaults.*`, невозможно создать `outputFolder`).

---

## 10. Что здесь показано

| Фича DSL | Где в скрипте |
|---|---|
| Kora-инъекция источников | 4 зависимости через constructor |
| Типизированный конфиг | `SampleConfig` как `@ConfigSource`-интерфейс |
| Автоматическая регистрация | `@Component` — runner находит через `All<Migration>` |
| Чтение из Postgres | `jdbc(orders).query(..., mapper)` — только read-запросы |
| HTTP-вызов (read) | `enrichment.customerStatus(...)` напрямую, без обёрток |
| Публикация в Kafka с dry-run gate | `@KafkaPublisher.Topic` + `mutation("orders.resync", args = ...) { publisher.publishResync(...) }` |
| Batch + параллелизм | `forEach(chunk, parallel, onError = OnError.Skip)` — `parallel` даёт реальные воркеры |
| Retry для transient HTTP | Kora `@Retry` на `EnrichmentService.customerStatus` — backoff внутри клиента, не на уровне DSL |
| Авто error-reporting | `errors.includeItem<Order> { ... }` — одна строка |
| Авто progress-лог | ничего не пишем — `Progress.Default` каждые `progressEvery` обработанных элементов (на коротких циклах шаг сам уменьшается до ~1/10 от total, чтобы прогресс был виден) |
| Итоговый отчёт | печатает сам runner |
| Dry-run | `MIGRATION_DRY_RUN=true` + строка `dryRun = ${?MIGRATION_DRY_RUN}` в конфиге — никакого `if` в коде |

## 11. Что НЕ нужно писать

- Никаких `try/catch` вокруг всего — `OnError.Skip` + Kora `@Retry` на клиенте делают всё.
  Провалившиеся батчи попадают в `logs/SAMPLE-001/errors.csv` автоматически. Локальный
  `try/catch` нужен только там, где хочется пер-элементная гранулярность внутри батча (§7).
- Никаких `Lists.partition` — `chunk = 200` в `forEach`.
- Никаких `ExecutorService` — `parallel = 4` в `forEach`; пул runner'а cached и выдаст
  ровно столько воркеров, сколько попросили.
- Никаких `if (dryRun)` — DSL сам решает, что делать с `mutation { publisher.publishResync(...) }`.
- Никаких ручных `writer.close()` / try-with-resources — нет CSV-файлов в этом скрипте
  (если бы были — `val out = openCsv(path, headers...)` сверху, а runner закрыл бы сам).
- Никакого ручного подсчёта processed/skipped — `MigrationReport` копит автоматически.

## 12. Вариации

**Correction-архетип** (чтение CSV + update в БД): `readCsv(path, onRowError = OnError.Skip) { ... }`
+ `forEach { jdbc(orders).execute("update ...") }`, Kafka не нужна. `onRowError` обязателен,
если битая строка входного файла не должна валить прогон: по умолчанию там `OnError.Fail`.
В `errors.csv` такая строка уезжает как сырая `Map<String, String>` — если в колонках есть
чувствительные данные, добавь `errors.includeItem<Map<String, String>> { ... }`.

**Comparison-архетип** (два кластера, пишем расхождения): `cassandra(primary).query(...)` +
`cassandra(replica).query(...)` + `openCsv("failed.csv", "contract", "reason")` сверху для аудита.
См. `kora/src/test/kotlin/io/github/dsudomoin/migration/kora/pilot/ComparisonPilotTest.kt`.

**REST-operation**: `forEach(items) { http(client).post("/sync", body) }` или типизированный
Kora-клиент внутри `mutation("sync.order", args = mapOf("id" to id)) { ... }` для write-вызовов
(label — константа, контекст item'а — в args; см. USER_GUIDE.md §13). `http(...).post/put/...`
бросает `HttpStatusException` на любой ответ вне 2xx — «успешный» item при мёртвом бэкенде
получить нельзя.

**Запись с возвратом строк**: `jdbc(db).executeReturning("insert ... returning id") { it.getLong("id") }`.
Через `query(...)` пишущий запрос не пройдёт — он проверяет первое ключевое слово и бросает
`IllegalArgumentException`, потому что `query` не проходит dry-run gate и выполнился бы в бою
во время репетиции.

**Command sending**: в теле `migrate()` генерируешь данные (например, из CSV) и сразу
`kafka(producer).publish(...)` в `forEach`. Полезно когда из одного скрипта пишешь в
**разные** топики и не хочешь городить `@KafkaPublisher.Topic` на каждый. Хендл `kafka(producer)`
мемоизируется на продюсер (`MigrationContext.shared`), так что вызывать его прямо в теле цикла
не накладно.
