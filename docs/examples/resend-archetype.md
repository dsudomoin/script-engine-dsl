# Пример: Resend-архетип

> Пошаговое описание узлов — в [гайде](../USER_GUIDE.md).

**Задача.** По тикету SAMPLE-001 нужно заново отправить в Kafka события по
«провисшим» заказам из Postgres. Для каждого заказа — дёрнуть сервис обогащения
(узнать актуальный статус клиента), собрать событие и опубликовать в топик
`orders.resync`. Провальные элементы складываются в аудит-CSV, чтобы потом
разобраться руками.

Заказов много, они разбиты по системам-источникам (`origin`), и требование
эксплуатации звучит так: **пока все события одного origin не подтверждены брокером,
следующий не начинаем**. Ровно под это в DSL есть `scoped` — стадия с барьером
подтверждений на каждого родителя.

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
│   └── SampleMigration.kt              # сам скрипт: MigrationDefinition + @Component
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
    # Дефолт аргумента source/scoped(parallel = ...), а НЕ размер пула: пул runner'а
    # cached и выдаёт столько воркеров, сколько запросила конкретная стадия.
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
  pageSize = 500
  parallel = 4
  # Ждать подтверждений по одному origin не дольше этого.
  completionTimeout = 10m
  # Ограничить прогон конкретными origin'ами. Пусто = все.
  origins = []
  origins = ${?SAMPLE_ORIGINS}
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
import java.util.concurrent.CompletionStage

@KafkaPublisher("kafka.orders.publisher")
interface OrdersPublisher {

    /**
     * Возвращаем CompletionStage, а не RecordMetadata: это то, что барьер стадии умеет
     * ждать. Блокирующий вариант тоже допустим — см. «write или publish» ниже.
     */
    @KafkaPublisher.Topic("kafka.orders.resyncTopic")
    fun publishResync(key: String, @Json value: OrderResyncEvent): CompletionStage<RecordMetadata>
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
import java.time.Duration

@ConfigSource("sample")
interface SampleConfig {
    fun pageSize(): Int = 500
    fun parallel(): Int = 4
    fun completionTimeout(): Duration = Duration.ofMinutes(10)
    fun origins(): List<String> = emptyList()   // пусто → взять все из БД
}
```

## 7. Сам скрипт

```kotlin
// com/example/migrations/SampleMigration.kt
package com.example.migrations

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.MigrationDefinition
import io.github.dsudomoin.migration.error.includeItem
import io.github.dsudomoin.migration.kora.ops.jdbc
import io.github.dsudomoin.migration.migration
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import java.sql.ResultSet

@Component
class SampleMigration(
    private val orders: JdbcConnectionFactory,
    private val enrichment: EnrichmentService,
    private val publisher: OrdersPublisher,
    private val config: SampleConfig,
) : MigrationDefinition {

    override val name = "SAMPLE-001"

    override fun plan() = migration(name = name, author = "team") {
        val resent = output("resent.csv", "origin", "order_id", "customer_status")

        // Ленивое значение прогона: читается один раз при первом resolve и кэшируется.
        val origins = input("origins") {
            config.origins().ifEmpty {
                jdbc(orders).query("select distinct origin from orders where status = 'STUCK' order by origin") {
                    it.getString("origin")
                }
            }
        }

        validate {
            require(config.pageSize() > 0) { "sample.pageSize должен быть > 0" }
            require(config.parallel() > 0) { "sample.parallel должен быть > 0" }
        }

        scoped(
            parents = { resolve(origins).asSequence() },
            completionTimeout = config.completionTimeout(),
            parallel = config.parallel(),
            onItemError = ItemError.Skip,
            items = { origin ->
                // items вызывается на каждого родителя, значит и регистрация повторится —
                // она идемпотентна и стоит копейки, а другого места «до первого обработчика»
                // внутри стадии нет.
                errors.includeItem<Order> { "order=${it.id}, customer=${it.customerId}, origin=${it.origin}" }

                pages(
                    name = "orders-$origin",
                    first = {
                        jdbc(orders).query(
                            """
                            select id, customer_id, amount, origin from orders
                            where status = 'STUCK' and origin = :origin
                            order by id limit :n
                            """.trimIndent(),
                            "origin" to origin, "n" to config.pageSize(),
                        ) { order(it) }
                    },
                    next = { afterId ->
                        jdbc(orders).query(
                            """
                            select id, customer_id, amount, origin from orders
                            where status = 'STUCK' and origin = :origin and id > :after
                            order by id limit :n
                            """.trimIndent(),
                            "origin" to origin, "after" to afterId, "n" to config.pageSize(),
                        ) { order(it) }
                    },
                    nextCursor = { page -> page.last().id },
                    continueWhen = { page -> page.size >= config.pageSize() },
                )
            },
        ) { order ->
            val status = enrichment.customerStatus(order.customerId).status   // ретраит Kora @Retry внутри

            publish("orders.resync", args = mapOf("orderId" to order.id, "origin" to order.origin)) {
                publisher.publishResync(
                    order.id.toString(),
                    OrderResyncEvent(order.id, order.customerId, status, order.amount, order.origin),
                )
            }

            resent.row(order.origin, order.id, status)
        }
    }

    private fun order(rs: ResultSet) = Order(
        id = rs.getLong("id"),
        customerId = rs.getString("customer_id"),
        amount = rs.getLong("amount"),
        origin = rs.getString("origin"),
    )

    private data class Order(val id: Long, val customerId: String, val amount: Long, val origin: String)
}
```

### Что здесь делает `scoped` — и почему не `source`

`source` — одна граница на всю стадию: барьер подтверждений стоит в самом конце, когда
источник исчерпан. Для resend'а это значит «миллион сообщений в полёте и один момент истины
в конце». `scoped` режет ту же работу на родителей и ставит барьер на каждого:

1. открывается родитель `web` → читаются его страницы → воркеры публикуют;
2. источник родителя исчерпан → **барьер**: ждём подтверждения по всем `publish` этого
   origin'а (не дольше `completionTimeout`);
3. закрываются ресурсы, зарегистрированные через `scopedResource` внутри этого родителя;
4. только теперь открывается `mobile`.

Что это даёт на практике:

- **Осмысленный рестарт.** Прогон упал на третьем origin — первые два доставлены полностью,
  перезапускаешь с `SAMPLE_ORIGINS=[третий,четвёртый]`. С одним общим барьером в конце
  такого знания нет вовсе.
- **Ограниченный in-flight.** Незавершённых отправок не больше, чем успевает накопить один
  origin, а не весь прогон.
- **`completionTimeout` считается по родителю** — и ограничивает **только ожидание
  подтверждений** после исчерпания источника, а не время чтения и отправки. Десять минут
  здесь — это «сколько ждём ack'ов у брокера», а не «сколько работает origin».

Порядок в `scoped` строго последовательный: родители не идут параллельно. Параллелизм
живёт **внутри** родителя — `parallel = 4` это четыре одновременно обрабатываемых заказа
текущего origin'а.

### `publish` или `write`

| | `write(name, args) { ...: WriteOutcome }` | `publish(name, args) { ...: CompletionStage<*> }` |
|---|---|---|
| Когда исход известен | сразу, синхронно | позже, на барьере scope'а |
| Возвращает | `WriteResult` | ничего — под dry-run возвращать было бы нечего |
| Под dry-run | тело не вызывается, `DryRunSkipped` | тело не вызывается, фальшивый future не создаётся |
| Отказ обрабатывается | как ошибка item'а, через `ItemError` | **валит стадию на барьере**, мимо `ItemError` |
| Счётчики отчёта | `appliedWrites` / `rejectedWrites` / `dryRunSkipped` | `acknowledgedPublishes` / `failedEffects` / `abandonedPublishes` / `dryRunSkipped` |

Если твой `@KafkaPublisher`-метод объявлен блокирующим (возвращает `RecordMetadata`), то он
и есть синхронная запись — оборачивай в `write`:

```kotlin
write("orders.resync", args = mapOf("orderId" to order.id)) {
    publisher.publishResync(order.id.toString(), event)   // RecordMetadata, блокирующий
    WriteOutcome.Applied
}
```

Барьера при этом не будет — он и не нужен: метод уже вернулся с ack'ом.

**Почему отказ `publish` не проходит через `ItemError`.** К моменту, когда брокер ответил
ошибкой, item давно посчитан успешным и обработчик по нему закончился — `Decision.Skip` для
него физически неприменим. Поэтому любой отказ доставки валит стадию на барьере
(`ScopeEffectsFailed`), а каждый отказ поштучно уезжает в `errors.csv` вместе с тем item'ом,
на котором был отправлен. Не дождавшиеся за `completionTimeout` учитываются как
`abandonedPublishes` и поднимают exit-код до 1: отправленное не отзывается, и молча
исчезать оно не имеет права.

### Сырой продюсер вместо типизированного publisher'а

Если типизированный `@KafkaPublisher` не подходит (шлёшь в разные топики, нужен явный
контроль), возьми `topic(producer, name)` — его `sendAsync` тоже отдаёт future, который
принимает `publish`:

```kotlin
) { order ->
    // Хендл мемоизируется на пару (producer, name) через shared — звать прямо в теле не накладно.
    val resync = topic(producer, "orders.resync")
    publish("orders.resync", args = mapOf("orderId" to order.id)) {
        resync.sendAsync(order.id.toString(), order.toBytes())
    }
}
```

Сырой `Producer` в граф надо принести самому — см. §8.

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
Твой скрипт runner находит через `All<MigrationDefinition>` — достаточно `@Component`
на классе. Имя (`name`) — константа: по нему runner выбирает миграцию и проверяет
дубли, **не строя планов**; `plan()` вызывается ровно один раз и только у выбранной.

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

### Прогон в dry-run (не шлёт в Kafka, но читает orders и дёргает enrichment):

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
INFO  i.g.d.migration.SAMPLE-001 - progress: 500  elapsed=12s  rate=41/s
INFO  i.g.d.migration.SAMPLE-001 - [DRY-RUN] orders.resync (orderId=42, origin=web)
INFO  i.g.d.migration.SAMPLE-001 - [DRY-RUN] orders.resync (orderId=43, origin=web)
...
═══════════════════════════════════════════════════════════
Migration: SAMPLE-001  (author: team)
Started:   2026-04-24 14:15:32 +03:00
Finished:  2026-04-24 14:18:04 +03:00
Duration:  2m 32s
Mode:      DRY-RUN
───────────────────────────────────────────────────────────
Processed:                 8 421
  ✓ Successful:            8 421
  ⊘ Skipped (errors):      0
  ✗ Failed:                0
  ⌀ Source pages read:       18  (rows: 8 421)
  ⌀ Dry-run skipped writes:    (orders.resync: 8421)
───────────────────────────────────────────────────────────
═══════════════════════════════════════════════════════════
```

Что тут читать:

- `Processed: 8 421` — это **заказы**: item стадии здесь один заказ, а не батч.
- `Source pages read` — сколько страниц и сырых строк реально вычитал `pages(...)`,
  **до** любых пользовательских `filter`. Единственный способ увидеть разницу между
  «прочитано» и «дошло до обработчика».
- `Dry-run skipped writes` показывает, сколько сообщений ушло бы в Kafka. Ни одного
  реального сайд-эффекта: под репетицией лямбда `publish` не вызывается вообще.
- Метки времени печатаются со смещением зоны (`+03:00`) — `errors.csv` пишет UTC,
  и по смещению одно с другим сопоставляется без гадания.
- Строк `Error details:` / `Error traces:` нет, потому что `errors.csv` создаётся
  лениво — только на первой ошибке.
- Если бы breakdown оказался **пустым** при непустом `Processed`, runner напечатал бы
  WARN и добавил бы предупреждение в отчёт: под dry-run это единственный наблюдаемый
  признак того, что где-то забыли `write`/`publish` и запись ушла в бой по-настоящему.

### Боевой прогон:

```bash
MIGRATION_RUN=SAMPLE-001 \
DB_USER=app DB_PASSWORD=secret \
KAFKA_BOOTSTRAP=kafka.prod:9092 \
ENRICHMENT_URL=https://enrichment.internal \
./gradlew run
```

Отличие в отчёте — вместо dry-run-строки появляется учёт доставки:
```
Mode:      REAL
───────────────────────────────────────────────────────────
Processed:                 8 421
  ✓ Successful:            8 421
  ⊘ Skipped (errors):      0
  ✗ Failed:                0
  ✓ Acknowledged publishes:  8 421
  ⌀ Source pages read:       18  (rows: 8 421)
───────────────────────────────────────────────────────────
```

`Acknowledged publishes` — сколько отправок брокер реально подтвердил, просуммировано
по всем барьерам. Если бы что-то пошло не так, рядом появились бы строки
`✗ Failed effects` (брокер отказал) и `⚠ Unconfirmed effects (abandoned: N, late: M)`
(не дождались за таймаут / зарегистрировано после барьера).

Exit-коды: `0` — всё гладко; `1` — `FAIL_FAST`, превышен `errorThreshold` либо остались
неподтверждённые эффекты; `2` — мисконфиг (неизвестное имя, дубликат имён, битые значения
`migration.defaults.*`, невозможно создать `outputFolder`, план не построился).

Неподтверждённые эффекты поднимают код возврата **независимо от `ScriptPolicy`**:
`LOG_AND_COMPLETE` не имеет права превратить потерянные сообщения в ноль.

---

## 10. Что здесь показано

| Фича DSL | Где в скрипте |
|---|---|
| Kora-инъекция источников | 4 зависимости через constructor |
| Типизированный конфиг | `SampleConfig` как `@ConfigSource`-интерфейс |
| Автоматическая регистрация | `@Component` — runner находит через `All<MigrationDefinition>` |
| `input(name) { }` | Список origin'ов читается один раз на прогон и кэшируется |
| `validate { }` | Проверка конфига до первой стадии и до любого эффекта |
| `scoped(parents = ..., completionTimeout = ...)` | Барьер подтверждений на каждый origin: следующий не начнётся, пока предыдущий не доставлен |
| `pages(...)` | Курсорный источник внутри родителя; `rawPages`/`rawRows` в отчёте |
| Чтение из Postgres | `jdbc(orders).query(..., mapper)` — только read-запросы |
| HTTP-вызов (read) | `enrichment.customerStatus(...)` напрямую, без обёрток |
| Публикация в Kafka под барьером | `publish("orders.resync", args) { publisher.publishResync(...) }` |
| Параллелизм внутри родителя | `parallel = 4` — реальные воркеры, пул runner'а cached |
| Retry для transient HTTP | Kora `@Retry` на `EnrichmentService.customerStatus` — backoff внутри клиента, не на уровне DSL |
| Авто error-reporting | `errors.includeItem<Order> { ... }` — одна строка в `items` |
| Авто progress-лог | ничего не пишем — `Progress.Default` каждые `progressEvery` обработанных |
| Итоговый отчёт | печатает сам runner |
| Dry-run | `MIGRATION_DRY_RUN=true` + строка `dryRun = ${?MIGRATION_DRY_RUN}` в конфиге — никакого `if` в коде |

## 11. Что НЕ нужно писать

- Никаких `try/catch` вокруг всего — `ItemError.Skip` + Kora `@Retry` на клиенте делают всё.
  Провалившиеся заказы попадают в `logs/SAMPLE-001/errors.csv` автоматически. Локальный
  `try/catch` нужен только там, где хочется своё поведение внутри одного item'а.
- Никаких `Lists.partition` — если батчи нужны, это `.chunked(n)` прямо в `items`.
- Никаких `ExecutorService` — `parallel = 4` в `scoped`; пул runner'а cached и выдаст
  ровно столько воркеров, сколько попросили.
- Никаких `if (dryRun)` — DSL сам решает, что делать с `publish { }`.
- Никаких `CountDownLatch` / `producer.flush()` / сбора `List<Future>` — барьер scope'а
  и есть это ожидание, а счётчики подтверждений ведёт движок.
- Никаких ручных `writer.close()` — `output(...)` объявлен в билдере, закрывает движок.
- Никакого ручного подсчёта processed/skipped — `MigrationReport` копит автоматически.

## 12. Вариации

**Одна стадия вместо scoped.** Если группировать не по чему и барьер нужен один, это
обычный `source(...)` с тем же `items`/`handle`. Барьер встанет в конце стадии; всё
отправленное будет ждаться там же.

**Батчи вместо поштучной отправки.** `.chunked(n)` на результате `pages(...)` — тогда item
стадии становится `List<Order>`, а `publish` зовётся в цикле внутри обработчика. Берут ради
round-trip'ов (обогащение пачкой), а не ради Kafka: единица `Skip` при этом тоже становится
батчем, и в `errors.csv` уедет весь список.

**Correction-архетип** (чтение CSV + update в БД): `readCsv(path, onRowError = ItemError.Skip) { ... }`
+ `jdbc(orders).execute("update ...")`, Kafka не нужна. См.
[correction-archetype.md](correction-archetype.md).

**Comparison-архетип** (два кластера, пишем расхождения): `cassandra(primary).query(...)` +
`cassandra(replica).query(...)` + два `output(...)`. См.
[comparison-archetype.md](comparison-archetype.md) и
[pilot-тест](../../kora/src/test/kotlin/io/github/dsudomoin/migration/kora/pilot/ComparisonPilotTest.kt).

**REST-operation**: `http(client).post("/sync", body)` (гейт внутри op) или типизированный
Kora-клиент внутри `write("sync.order", args = mapOf("id" to id)) { ...; WriteOutcome.Applied }`
для write-вызовов. Метка — константа, контекст item'а — в `args`. См.
[http-backfill-archetype.md](http-backfill-archetype.md).

**Запись с возвратом строк**: `jdbc(db).executeReturning("insert ... returning id") { it.getLong("id") }`.
Через `query(...)` пишущий запрос не пройдёт — он проверяет первое ключевое слово и бросает
`IllegalArgumentException`, потому что `query` не проходит dry-run gate и выполнился бы в бою
во время репетиции.

**Command sending**: источником стадии может быть что угодно, включая CSV с командами —
читаешь `readCsv`, публикуешь `kafka(producer).publish(...)` (синхронно, гейт внутри op) или
`publish { topic.sendAsync(...) }` (под барьером). Полезно когда из одного скрипта пишешь в
**разные** топики и не хочешь заводить `@KafkaPublisher.Topic` на каждый. Хендлы
`kafka(producer)` и `topic(producer, name)` мемоизируются через `shared`, так что вызывать
их прямо в теле обработчика не накладно.
