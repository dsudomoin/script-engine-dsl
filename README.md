# Migration DSL

Kotlin-библиотека для одноразовых миграционных скриптов на [Kora](https://kora-projects.github.io/kora-docs/ru/).

Сервис поднимается разово, выполняет одну названную миграцию и завершается с кодом возврата.
Миграция — обычный класс с обычным телом, читающий данные через **ваши же** компоненты графа
(JDBC-репозитории, HTTP-клиенты) и CSV-файлы. Библиотека берёт на себя ровно то, что иначе
пишется заново в каждом таком скрипте:

- цикл со счётчиками и политикой ошибок (`Fail` / `Skip` / `Handle<T>`);
- запись сбойных элементов в `errors.csv` со стектрейсами в `errors.log`;
- прогресс каждые N элементов;
- чтение CSV с любым разделителем и кодировкой, сборку строки в DTO и отказ до старта, если файл битый;
- папку артефактов с `migration.log`;
- итоговый отчёт и код возврата.

```kotlin
@Component
class BackfillCustomerTier(
    private val repo: CustomerRepository,          // обычный Kora @Repository
) : Migration("CUSTOMER-TIER-001") {

    override fun MigrationScope.run() {
        val out = csv("customer-tier.csv", "id", "spend", "tier")

        each(
            readCsvAs<Customer>("customers.csv", classpath = true, onRowError = ItemError.Skip),
            parallel = 4,
            onItemError = ItemError.Skip,
        ) { customer ->
            val tier = tierFor(customer.spend)
            if (!dryRun) repo.updateTier(customer.id, tier)
            out.row(customer.id, customer.spend, tier)
        }
    }
}

data class Customer(val id: Long, val email: String, val spend: Long)
```

Запуск:

```bash
MIGRATION_RUN=CUSTOMER-TIER-001 ./gradlew run                          # боевой
MIGRATION_RUN=CUSTOMER-TIER-001 MIGRATION_DRY_RUN=true ./gradlew run   # репетиция
```

Обе переменные работают только если проброшены в `application.conf` — библиотека сама
окружение не читает (см. [Установку](#установка)).

В `logs/CUSTOMER-TIER-001/` окажутся `migration.log`, `errors.csv`, `errors.log` и
пользовательские выходные CSV; итоговый отчёт уедет в лог.

## Три вещи, которые стоит знать сразу

**Dry-run — это флаг, а не перехват.** Библиотека не видит вызовов ваших компонентов и ничего
не блокирует. Пропустить запись обязана сама миграция: `if (!dryRun) repo.update(...)`.
Забытая проверка под `dryRun = true` запишет в целевую систему по-настоящему.

**Битый входной файл роняет прогон до первого элемента.** `readCsvAs<T>` разбирает файл целиком
прежде, чем отдать первый объект, и при ошибках печатает список всех битых строк с номерами,
не тронув целевую систему (код возврата `2`). Чтобы битые строки просто пропускались, это
нужно попросить явно: `onRowError = ItemError.Skip`.

**Retry — не задача библиотеки.** Для transient-ошибок сети берите Kora `@Retry` на
типизированном `@HttpClient` / `@KafkaPublisher` / методе репозитория: там правильный уровень
(один remote-вызов, а не вся миграция) и правильная классификация исключений.

## Когда использовать

✅ **Подходит:**
- Bulk-операции: пересверка статусов, бэкфилл через API, выгрузка отчётов в CSV.
- Сервисы на Kora, где уже подключены `database-jdbc` / `kafka` / `http-client-jdk`.
- Сервис, в который со временем накапливается много миграций: имя выбирает одну из них.

❌ **Не подходит:**
- Long-running / always-on задачи — это разовые скрипты.
- Streaming-ETL с непрерывным входом.
- Coroutines-first кодовая база: цикл блокирующий, параллелизм — на потоках JVM.

## Установка

```kotlin
// build.gradle.kts
plugins {
    kotlin("jvm") version "2.1.20"
    id("com.google.devtools.ksp") version "2.1.20-1.0.32"
}

dependencies {
    implementation("io.github.dsudomoin.migration:migration-dsl-core:0.2.0")
    implementation("io.github.dsudomoin.migration:migration-dsl-kora:0.2.0")

    ksp("ru.tinkoff.kora:symbol-processors:1.2.20")
}
```

Кодогенерация Kora для Kotlin — только KSP; `kapt` с `ru.tinkoff.kora:annotation-processors`
не поддерживается.

В `@KoraApp`-интерфейсе подключите `MigrationModule`:

```kotlin
@KoraApp
interface App :
    HoconConfigModule,
    JdbcDatabaseModule,
    MigrationModule
```

Больше подмешивать нечего: секцию `migration { ... }` модуль читает сам, а runner помечен
`@Root` и потому создаётся графом без внешних зависимостей.

В `application.conf`:

```hocon
migration {
  run = ${?MIGRATION_RUN}
  dryRun = ${?MIGRATION_DRY_RUN}   # без этой строки MIGRATION_DRY_RUN=true не включит репетицию
}
```

Библиотека нигде не читает окружение сама — переменные приезжают только через `${?VAR}` в HOCON.
Строку `dryRun` легко забыть, и тогда `MIGRATION_DRY_RUN=true ./gradlew run` спокойно уйдёт в бой.

### Готовый рабочий пример

В репозитории лежит модуль [`example/`](example) — работающее приложение-миграция
(`@KoraApp` + `Migration` + `application.conf`), собранное ровно так, как собирался бы чужой
сервис. Им же проверяется, что описанный wire-up поднимается на настоящем графе Kora:

```bash
MIGRATION_RUN=CUSTOMER-TIER-001 ./gradlew :example:run                        # боевой
MIGRATION_RUN=CUSTOMER-TIER-001 MIGRATION_DRY_RUN=true ./gradlew :example:run # репетиция
```

## Документация

| Документ | Что внутри |
|---|---|
| **[docs/USER_GUIDE.md](docs/USER_GUIDE.md)** | Полный гайд: установка, тело миграции, `each`, `ItemError`, CSV, dry-run, отчёт, коды возврата, тестирование |
| **[AGENTS.md](AGENTS.md)** | Тот же материал в виде справочника для кодового агента: сигнатуры, инварианты, антипаттерны |
| KDoc | Все публичные типы и функции снабжены KDoc'ом — IDE подскажет прямо на месте |

## Примеры

| Архетип | Когда | Файл |
|---|---|---|
| Export | БД → CSV-снимок | [examples/export-archetype.md](docs/examples/export-archetype.md) |
| Correction | CSV → UPDATE в БД | [examples/correction-archetype.md](docs/examples/correction-archetype.md) |
| Comparison | Два источника → расхождения в CSV | [examples/comparison-archetype.md](docs/examples/comparison-archetype.md) |
| Resend | БД → обогащение → Kafka | [examples/resend-archetype.md](docs/examples/resend-archetype.md) |
| HTTP backfill | БД → внешний HTTP | [examples/http-backfill-archetype.md](docs/examples/http-backfill-archetype.md) |
| **Full showcase** | Постраничное чтение, транзакция, Kafka и три CSV в одной миграции | [examples/full-showcase.md](docs/examples/full-showcase.md) |
| **Customization** | Свои компоненты, `db.inTx`, S3, свой `ErrorReporter`, курсорная пагинация | [examples/customization.md](docs/examples/customization.md) |
| **Рабочий модуль** | Запускается через `./gradlew :example:run` | [example/](example) |

## Архитектура

Два модуля:

- **`migration-dsl-core`** — чистый Kotlin, без Kora. `Migration`, `MigrationScope` (`each`,
  `dryRun`, `log`, `errors`, `outputFolder`), `ItemError`, `Progress`, чтение и запись CSV,
  `MigrationReport`, `MigrationTest`. Подключается в любой Kotlin/JVM-проект.
- **`migration-dsl-kora`** — мост в Kora: `MigrationModule`, `MigrationRunner` (HOCON,
  `Lifecycle`, коды возврата), `MigrationExit`. Если вы не на Kora — хватит одного `core`.

Доступа к данным библиотека не содержит: SQL, Kafka и HTTP живут в ваших компонентах, а
миграция просто их зовёт.

## Требования

- Kotlin **2.1+**, JVM **21+**.
- Kora **1.2.20** — только для `migration-dsl-kora`.

## Лицензия

[MIT](LICENSE).
