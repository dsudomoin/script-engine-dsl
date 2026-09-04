# Migration DSL

Kotlin-библиотека для одноразовых миграционных скриптов на [Kora](https://kora-projects.github.io/kora-docs/ru/).

Миграция описывается как **неизменяемый план**: код объявляет узлы, а исполняет их runner —
только у выбранной миграции и только после старта. Движок берёт на себя то, что повторяется
в каждом таком скрипте:

- курсорную пагинацию источника (`pages`), без ручного `do/while`;
- границы обработки родителя (`scoped`) и барьер подтверждений на этой границе;
- параллелизм с error-policy (`Fail` / `Skip` / `Handle<T>`);
- запись сбойных элементов в `errors.csv` автоматически;
- dry-run без `if (dryRun)` в коде;
- счётчики: applied / rejected / acked / skipped — вручную их вести не нужно;
- итоговый отчёт + `migration.log` в одну папку.

Retry — **не задача движка**: для transient-ошибок сети используй Kora `@Retry`
на типизированном `@HttpClient` / `@KafkaPublisher` / repository-методе — она
классифицирует исключения и применяет backoff на правильном уровне (один
remote-вызов, а не вся миграция).

```kotlin
@KafkaPublisher("kafka.orders.publisher")
interface OrdersPublisher {
    @KafkaPublisher.Topic("kafka.orders.publisher.resyncTopic")
    fun publishResync(key: String, @Json value: OrderEvent)
}

@Component
class SampleMigration(
    private val orders: OrderRepository,            // обычный Kora @Repository
    private val publisher: OrdersPublisher,         // типизированный @KafkaPublisher Kora
) : MigrationDefinition {

    override val name = "SAMPLE-001"

    override fun plan() = migration(name = name, author = "team") {
        source(
            parallel = 4,
            onItemError = ItemError.Skip,
            items = { orders.findStuck().asSequence() },
        ) { order ->
            write("orders.resync", args = mapOf("orderId" to order.id)) {
                publisher.publishResync(order.id.toString(), OrderEvent.from(order))
                WriteOutcome.Applied
            }
        }
    }
}
```

Запускаешь:
```bash
MIGRATION_RUN=SAMPLE-001 ./gradlew run     # боевой
MIGRATION_DRY_RUN=true MIGRATION_RUN=SAMPLE-001 ./gradlew run   # без записей
```

Обе переменные работают только если проброшены в `application.conf` (`run` и
`dryRun` — см. [Установку](#установка)).

Получаешь в `logs/SAMPLE-001/`:
- `migration.log` — всё, что писалось в slf4j,
- `errors.csv` — элементы, попавшие в `Skip`-ветку,
- `errors.log` — стектрейсы тех же ошибок,
- итоговый отчёт в стандартный вывод.

## Когда использовать

✅ **Подходит:**
- Bulk-операции: пересверка статусов, бэкфилл API, выгрузка отчётов в CSV.
- Сервисы на Kora, где уже подключены `database-jdbc`/`kafka`/`http-client-jdk`.
- Скрипты на 10–100 строк, которые лень писать через Spring + ExecutorService + try/catch.

❌ **Не подходит:**
- Long-running / always-on задачи (это разовые скрипты).
- Streaming-ETL с непрерывным input'ом.
- Coroutines-first кодовая база (DSL блокирующий, executor — JVM threads).

## Установка

```kotlin
// build.gradle.kts
plugins {
    kotlin("jvm") version "2.1.20"
    id("com.google.devtools.ksp") version "2.1.20-1.0.32"
}

dependencies {
    implementation("io.github.dsudomoin.migration:migration-dsl-core:0.1.0")
    implementation("io.github.dsudomoin.migration:migration-dsl-kora:0.1.0")

    ksp("ru.tinkoff.kora:symbol-processors:1.2.20")
}
```

Кодогенерация Kora для Kotlin — только KSP. `kapt` с
`ru.tinkoff.kora:annotation-processors` не поддерживается.

В `@KoraApp`-интерфейсе подключи `MigrationModule`:

```kotlin
@KoraApp
interface App :
    HoconConfigModule,
    JdbcDatabaseModule,
    MigrationModule
```

Больше ничего подмешивать не нужно: секцию `migration { ... }` `MigrationModule`
читает сам, а runner помечен `@Root` и создаётся графом без внешних зависимостей.
Класса `KafkaProducerModule` в Kora не существует — для типизированного
`@KafkaPublisher` достаточно артефакта `ru.tinkoff.kora:kafka` и KSP; сырой
`Producer` для `kafka(...)` / `topic(...)` объявляешь своим `@Module`-компонентом
(или берёшь у Kora-паблишера через `producer()`).

В `application.conf`:
```hocon
migration {
  run = ${?MIGRATION_RUN}
  dryRun = ${?MIGRATION_DRY_RUN}   # без этой строки MIGRATION_DRY_RUN=true не включит репетицию
  defaults { parallel = 1 }        # значение source(parallel = ...) по умолчанию
}
```

Библиотека нигде не читает окружение сама (`System.getenv` в исходниках нет) —
переменные приезжают только через `${?VAR}` в HOCON. Строку `dryRun` легко
забыть, и тогда `MIGRATION_DRY_RUN=true ./gradlew run` спокойно уйдёт в бой.

`defaults.parallel` — это дефолт аргумента `source(parallel = ...)`, а не размер
пула: пул runner'а cached и выдаёт столько воркеров, сколько запросила конкретная
стадия. `source(parallel = 8)` даст восемь потоков независимо от этого ключа.

### Готовый рабочий пример

В репозитории лежит модуль [`example/`](example) — работающее приложение-миграция
(`@KoraApp` + `MigrationDefinition` + `application.conf`), собранное ровно так, как собирался
бы чужой сервис. Им же проверяется, что описанный выше wire-up действительно
поднимается на настоящем графе Kora:

```bash
MIGRATION_RUN=CUSTOMER-TIER-001 ./gradlew :example:run                        # боевой
MIGRATION_RUN=CUSTOMER-TIER-001 MIGRATION_DRY_RUN=true ./gradlew :example:run # репетиция
```

Дальше — [гайд](docs/USER_GUIDE.md) и [примеры](#примеры).

## Документация

| Документ | Что внутри |
|---|---|
| **[docs/USER_GUIDE.md](docs/USER_GUIDE.md)** | Полный гайд: setup, план миграции, `source`/`scoped`, `pages`, эффекты, `ItemError`, CSV, JDBC, Kafka, HTTP, HOCON, dry-run, отчёты, тестирование. С TOC и примерами. |
| KDoc | Все публичные классы и функции снабжены KDoc'ом — IDE даст подсказки по типам |

## Примеры

Архетипы по сложности — от самого простого к самому сложному.

| Архетип | Когда | Файл |
|---|---|---|
| Export | DB → CSV snapshot | [examples/export-archetype.md](docs/examples/export-archetype.md) |
| Correction | CSV → Postgres update + audit | [examples/correction-archetype.md](docs/examples/correction-archetype.md) |
| Comparison | Два source'а → diff в CSV | [examples/comparison-archetype.md](docs/examples/comparison-archetype.md) |
| Resend | DB → enrich → Kafka | [examples/resend-archetype.md](docs/examples/resend-archetype.md) |
| HTTP backfill | DB → внешний HTTP с retry | [examples/http-backfill-archetype.md](docs/examples/http-backfill-archetype.md) |
| **Full showcase** | Cassandra + Postgres tx + Kafka + HTTP + 3×CSV | [examples/full-showcase.md](docs/examples/full-showcase.md) |
| **Customization** | Свои ops, AutoCloseable, Executor, Progress | [examples/customization.md](docs/examples/customization.md) |
| **Рабочий модуль** | CSV → `write` → CSV, запускается `./gradlew :example:run` | [example/](example) |

## Архитектура

Два модуля:

- **`migration-dsl-core`** — pure Kotlin, без Kora-зависимостей. Содержит DSL плана
  (`MigrationDefinition`, `migration { }`, `source`, `scoped`, `input`, `output`, `validate`),
  scope'ы (`RunScope`/`SourceScope`/`HandlerScope`), `pages`, эффекты (`write`/`publish`),
  `ItemError`, `Progress`, CSV-read/write, `MigrationReport` и интерпретатор плана. Можно
  подключать в любой Kotlin/JVM проект.
- **`migration-dsl-kora`** — мост в Kora. Содержит `MigrationModule`, `MigrationRunner`
  (HOCON + `Lifecycle`), extension-функции `jdbc()`, `cassandra()`, `kafka()`, `topic()`,
  `http()`, `transactional { }`. Подключается отдельно — если ты не на Kora, тебе хватит
  только `core`.

## Требования

- Kotlin **2.1+**, JVM **21+**.
- Kora **1.2.20** (для `migration-dsl-kora`).
- Docker — только для контейнерных тестов самой либы; они помечены `@Tag("docker")`
  и по умолчанию исключены (`./gradlew build` проходит без Docker, прогнать их —
  `./gradlew test -PwithDocker`). Пользователю библиотеки Docker не нужен.

## Лицензия

[MIT](LICENSE).
