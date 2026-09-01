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
    id("com.google.devtools.ksp") version "2.1.20-1.0.32"
    application
}

kotlin { jvmToolchain(21) }

application { mainClass = "com.example.AppKt" }

dependencies {
    implementation("io.github.dsudomoin.migration:migration-dsl-core:0.1.0")
    implementation("io.github.dsudomoin.migration:migration-dsl-kora:0.1.0")

    // Kora — подключай только те модули, которые реально используешь в скриптах
    implementation("ru.tinkoff.kora:common:1.1.25")
    implementation("ru.tinkoff.kora:config-common:1.1.25")
    implementation("ru.tinkoff.kora:config-hocon:1.1.25")
    implementation("ru.tinkoff.kora:application-graph:1.1.25")
    implementation("ru.tinkoff.kora:database-jdbc:1.1.25")      // если нужен Postgres
    implementation("ru.tinkoff.kora:kafka:1.1.25")              // если нужна Kafka
    implementation("ru.tinkoff.kora:http-client-jdk:1.1.25")    // если нужен HTTP

    runtimeOnly("org.postgresql:postgresql:42.7.7")             // драйвер БД
    runtimeOnly("ch.qos.logback:logback-classic:1.5.16")        // logging backend

    // Кодогенерация Kora для Kotlin — KSP. Другого пути нет.
    ksp("ru.tinkoff.kora:symbol-processors:1.1.25")
}
```

**Только KSP, никакого kapt.** Для Kotlin Kora поддерживает единственный процессор —
`ru.tinkoff.kora:symbol-processors`, подключаемый конфигурацией `ksp`. Связка
`kotlin("kapt")` + `kapt("ru.tinkoff.kora:annotation-processors")` (как и голый
`annotationProcessor(...)`) для Kotlin-проекта не работает: аннотации не обрабатываются,
`@KoraApp`-граф не генерируется, сборка падает на отсутствующем `AppGraph`. Если такая связка
осталась в проекте с прежних версий гайда — вычищай её целиком.

Живой образец сборки потребителя — модуль [`example/build.gradle.kts`](../example/build.gradle.kts)
в этом репозитории: он подключает библиотеку ровно так, как это сделает чужой сервис.

### `@KoraApp`

```kotlin
// App.kt
@KoraApp
interface App :
    HoconConfigModule,
    JdbcDatabaseModule,     // если нужен Postgres
    JdkHttpClientModule,    // если нужен HTTP
    MigrationModule         // всегда

fun main() {
    KoraApplication.run { AppGraph.graph() }
}
```

Ради библиотеки нужна ровно одна строка — `MigrationModule`. Секцию `migration { ... }` модуль
читает сам (внутри лежит собственная фабрика конфига), а runner помечен `@Root`, поэтому граф
создаёт его без явных зависимостей. Никаких дополнительных модулей библиотеки подмешивать не надо.

Остальное — по потребности скрипта: `JdbcDatabaseModule` (артефакт `database-jdbc`),
`CassandraDatabaseModule` (`database-cassandra`), `JdkHttpClientModule` (`http-client-jdk`),
`KafkaModule` (`kafka`, нужен если объявляешь `@KafkaPublisher`).

> **`KafkaProducerModule` не существует.** Такого класса в Kora нет; если он остался в
> `@KoraApp` со старых версий гайда — модуль просто не скомпилируется. Модуль Kafka называется
> `KafkaModule`, а как достать raw `Producer` для `topic(...)` / `kafka(...)` — см. [§12](#12-kafka--topicproducer-name-и-kafkaproducerpublish).

### `application.conf`

Минимум:
```hocon
migration {
  run    = ${?MIGRATION_RUN}       # имя миграции
  dryRun = ${?MIGRATION_DRY_RUN}   # репетиция; без этой строки переменная не действует
}

db {
  jdbcUrl  = ${?DB_URL}
  username = ${?DB_USER}
  password = ${?DB_PASSWORD}
}
```

**Про `dryRun = ${?MIGRATION_DRY_RUN}` — это не украшение.** В библиотеке нет ни одного
`System.getenv`: окружение читает HOCON, и только через явную подстановку `${?VAR}`. Если строки
`dryRun` в конфиге нет, то `MIGRATION_DRY_RUN=true ./gradlew run` запустит **боевой** прогон —
переменная останется никем не прочитанной, а `migration.dryRun` возьмёт значение по умолчанию
`false`. Ровно та же логика у `run`, `outputFolder` и любого другого ключа: нет строки в
`application.conf` — нет env-override.

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
- **`forEach(ids) { ... }`** — число воркеров по умолчанию берётся из
  `migration.defaults.parallel` (из коробки `1`, то есть sequential). Явный `parallel = N`
  в вызове всегда сильнее конфига — см. [§5](#5-foreach--цикл-с-параллелизмом).

---

## 3. Запуск

```bash
MIGRATION_RUN=HELLO ./gradlew run                          # боевой прогон
MIGRATION_RUN=HELLO MIGRATION_DRY_RUN=true ./gradlew run   # репетиция (см. §15)
```

Или из IDE — IntelliJ Run Configuration с env `MIGRATION_RUN=HELLO`.

Обе переменные работают только потому, что в `application.conf` есть строки
`run = ${?MIGRATION_RUN}` и `dryRun = ${?MIGRATION_DRY_RUN}` (см. [§1](#1-установка-и-подключение)).

В консоль уйдёт лог + итоговый отчёт. В файловой системе появится `logs/HELLO/`:

```
logs/HELLO/
├── migration.log     # всё, что писалось через slf4j во время прогона
├── ids.csv           # наш output
├── errors.csv        # создаётся ЛЕНИВО — только если была хотя бы одна ошибка
└── errors.log        # стектрейсы к тем же ошибкам, тоже лениво
```

`errors.csv` / `errors.log` появляются в момент первой записанной ошибки. Чистый прогон не
оставляет ни пустых файлов, ни строк `Error details:` в отчёте — «файлов нет» здесь значит
«ошибок не было».

---

## 4. Что доступно в `migrate()` — `MigrationContext`

`MigrationContext` — receiver `migrate()`. Через него доступны:

| Свойство | Тип | Что |
|---|---|---|
| `dryRun` | `Boolean` | `true` если `migration.dryRun` = true (обычно через `dryRun = ${?MIGRATION_DRY_RUN}`) |
| `log` | `slf4j.Logger` | Логгер `io.github.dsudomoin.migration.<имя_миграции>` — пиши сюда свои INFO/WARN/DEBUG |
| `report` | `ReportBuilder` | Счётчики прогона. Обычно дёргают только в `Progress.Custom { ... }` для кастомных метрик |
| `executor` | `Executor` | Пул потоков для `forEach`. Runner создаёт **cached** pool. Обычно сам не используешь |
| `outputFolder` | `Path` | Путь к папке артефактов (см. [§16](#16-outputfolder-и-артефакты-прогона)) |
| `defaultParallel` | `Int` | Значение `parallel` у `forEach` по умолчанию. Из `migration.defaults.parallel` |
| `defaultProgressEvery` | `Int` | Период `Progress.Default`. Из `migration.defaults.progressEvery` |
| `errorThreshold` | `Long` | Порог SKIP-ов, после которого прогон прерывается. Из `migration.defaults.errorThreshold`, `0` = выключено |
| `errors` | `CsvFileErrorReporter` | Авто-аудитор. Используется в основном для `errors.includeItem<T> { ... }` (см. [§17](#17-errorscsv-и-errorsincludeitemt)) |

Доступны методы:
- `register(closeable)` — зарегистрировать свой `AutoCloseable` для авто-close после `migrate()`. Закрытие идёт в обратном порядке регистрации. Обычно сама либа делает это за тебя (через `openCsv`/`topic`).
- `shared(key) { factory() }` — ресурс, единственный на прогон для данного ключа: первый вызов создаёт и регистрирует, последующие отдают тот же инстанс. На этом построены `kafka(producer)` и `topic(producer, name)`, поэтому их безопасно звать прямо в теле `forEach` — реестр не растёт по объекту на item. Пригодится и для своих op-обёрток.
- `guardWrite(label, args, action)` — низкоуровневый dry-run gate. Снова, чаще всего сделано внутри DSL.
- `auditError(e, item)` — ручной вызов аудитора. В обычной жизни не нужен.

---

## 5. `forEach` — цикл с параллелизмом

Четыре перегрузки: item-by-item и chunked, каждая — для `Iterable` и для `Sequence`.

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

Для `Iterable`-источника разбиение материализует весь `List<List<T>>` до первой обработанной
пачки (сам источник и так в памяти, но пик потребления удваивается). Для потоковых источников
бери `Sequence`-перегрузку — она режет **лениво** и держит в памяти только текущий батч:

```kotlin
forEach(readCsv(path) { it }, chunk = 500, parallel = 4) { batch -> ... }
```

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
    chunk = 500,                              // только у chunked-перегрузок, дефолта нет
    parallel = 8,                             // воркеров. Дефолт — defaultParallel
    onError = OnError.Skip,                   // что делать при ошибке. Дефолт OnError.Fail
    onErrorLog = { e, item ->                 // доп. лог при ошибке (помимо авто-аудита). Дефолт null
        log.warn("Failed item=$item: ${e.message}")
    },
    logEach = { item ->                       // лог после успешной обработки. Дефолт null
        "Processed item=$item"
    },
    progress = Progress.Custom(1000) { done, total ->  // прогресс-лог. Дефолт Progress.Default
        "$done/$total processed"
    },
) { item -> ... }
```

| Параметр | Тип | Дефолт | Что |
|---|---|---|---|
| `items` | `Iterable<T>` / `Sequence<T>` | — | источник |
| `chunk` | `Int` | — (обязателен у chunked-перегрузок) | размер батча; блок получает `List<T>` |
| `parallel` | `Int` | `defaultParallel` (`migration.defaults.parallel`, из коробки `1`) | число воркеров. `<= 0` → `IllegalArgumentException` |
| `onError` | `OnError` | `OnError.Fail` | политика на ошибку item'а (см. [§6](#6-onerror--политика-обработки-ошибок)) |
| `onErrorLog` | `((Throwable, T) -> Unit)?` | `null` | доп. лог при ошибке |
| `logEach` | `((T) -> String)?` | `null` | строка в INFO-лог после успешного item'а |
| `progress` | `Progress` | `Progress.Default` | прогресс-лог (см. [§7](#7-progress--прогресс-логирование)) |

У chunked-перегрузок `T` в `onErrorLog` / `logEach` / блоке — это `List<T>`: единицей учёта
(и в `report.processed`, и в прогрессе) становится батч, а не отдельный элемент.

### Про параллелизм

- `parallel = N` даёт **ровно N одновременно работающих воркеров**. Пул runner'а — cached, он
  выдаёт столько потоков, сколько попросил конкретный цикл; число одновременных задач держит
  семафор самого `forEach`. Раньше потолком был размер общего фиксированного пула (по умолчанию
  один поток), и `parallel = 8` был декорацией — примеры в старом гайде обещали параллелизм,
  которого не было.
- `parallel = 1` (дефолт из коробки) исполняется прямо в вызывающем потоке, минуя executor.
- Вложенный `forEach(parallel > 1)` внутри другого параллельного `forEach` работает и **не
  встаёт в deadlock** — на фиксированном пуле это был вечный вис.
- `migration.defaults.parallel` задаёт только **значение аргумента по умолчанию**: явный
  `parallel = N` в коде всегда сильнее конфига.
- Диагностические колбэки не влияют на судьбу item'а: исключение из `logEach` / `onErrorLog`
  больше не превращает успешный item в отказ (и не обрывает прогон под `OnError.Fail` уже
  после выполненной записи). Про первый такой сбой уходит предупреждение в отчёт, дальше молча.

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

### `onRowError` — политика на битую строку

Обе перегрузки принимают `onRowError: OnError` (дефолт `OnError.Fail`):

```kotlin
readCsv(path, classpath = false, onRowError = OnError.Skip) { row -> ... }
readCsv(Path.of("/data/input.csv"), onRowError = OnError.Skip) { row -> ... }
```

Политика применяется к **каждой строке отдельно** и покрывает обе возможные беды: разбор
самого CSV (рваные кавычки, лишние колонки) и работу твоего mapper'а
(`row.getValue("spend").toLong()` на пустой ячейке).

- `OnError.Fail` (дефолт) — первая же плохая строка валит прогон. Прежнее поведение.
- `OnError.Skip` / `OnError.handle { ... -> Decision.Skip }` — строка уезжает в `errors.csv`,
  инкрементит `report.skipped` (то есть считается в `errorThreshold`) и до `forEach` не доходит.

Это важнее, чем кажется: раньше ошибка mapper'а летела **мимо** `OnError` цикла и валила весь
прогон, хотя export-архетип обещал обратное. Теперь обещание выполнимо — но только если
`onRowError` передан явно.

Открытый поток регистрируется в контексте, поэтому недопотреблённая `Sequence` (`take(n)`,
ранний выход, исключение выше по стеку) не оставляет открытый файловый дескриптор — runner
закроет его в `finally`.

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

`openCsv` возвращает [`CsvOutput`](../core/src/main/kotlin/io/github/dsudomoin/migration/csv/CsvWrite.kt) — handle с двумя методами:

```kotlin
fun row(vararg cells: Any?)
fun flush()                  // сбросить буфер, не закрывая файл
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

> **`query` и `stream` принимают только читающие запросы.** Первое ключевое слово должно быть
> `select` / `with` / `show` / `explain` / `values` / `table` / `describe`, иначе —
> `IllegalArgumentException` ещё до похода в базу. Причина простая: ни `query`, ни `stream` не
> проходят dry-run gate (и не должны — чтение под репетицией обязано работать), поэтому
> `query("insert ... returning id")` выполнялся бы **в бою во время dry-run прогона**.
> Нужны строки от пишущего запроса — бери `executeReturning` (ниже). Проверка одинакова в обоих
> режимах: гейт, срабатывающий только под dry-run, дал бы зелёную репетицию при падающем бое.
>
> Известное ограничение: `WITH ... INSERT` (data-modifying CTE) начинается с `with` и проверку
> пройдёт — такие запросы тоже отправляй в `executeReturning`.

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

```kotlin
// Пишущий запрос, возвращающий строки: INSERT/UPDATE ... RETURNING
val ids = jdbc(db).executeReturning(
    "insert into orders(customer_id) values (:c) returning id",
    "c" to customerId,
) { rs -> rs.getLong("id") }
```

`execute`, `batch` и `executeReturning` идут через `guardWrite` — под dry-run пропускаются, в
отчёте видны (метки `jdbc.execute`, `jdbc.batch`, `jdbc.executeReturning`). Под dry-run
`execute` возвращает `0`, `batch` — пустой `IntArray`, `executeReturning` — пустой список
(mapper при этом не вызывается: строк, которые ему можно отдать, не существует).

### Транзакция

```kotlin
transactional(jdbc(db)) {
    execute("update orders set status = 'PROCESSING' where id = :id", "id" to orderId)
    execute("insert into order_audit(order_id, event) values (:id, 'process_start')", "id" to orderId)
    // если любой execute упал — rollback всей пары
}
```

Внутри блока `this: SqlOps` — `execute/query/batch/executeReturning` без префикса. Все они
используют один `Connection`, открытый через `db.inTx` Kora. Commit на успехе, rollback на
исключении.

**Вложенный `transactional` бросает `IllegalStateException`** — нет smart-merge с outer-tx,
автор должен явно решить, что делать. Проверка работает **и под dry-run**: раньше она была
отключена в режиме репетиции, то есть dry-run проходил зелёным ровно там, где боевой прогон
падал. Ловятся оба случая: вложенный вызов на tx-bound `SqlOps` и вызов с внешним free-mode
`ops` изнутри уже открытой транзакции.

**Транзакционный `SqlOps` привязан к потоку, который открыл транзакцию.** `java.sql.Connection`
не потокобезопасен, а вот это компилируется и выглядит безобидно:

```kotlin
// НЕЛЬЗЯ: четыре воркера полезут в один Connection
transactional(jdbc(db)) {
    forEach(orders, parallel = 4) { o -> execute("update orders set ... where id = :id", "id" to o.id) }
}
```

Теперь такой код падает с внятным `IllegalStateException` («tx-bound jdbc ops used from thread
... but the transaction belongs to ...») вместо тихой порчи данных. Правильный порядок —
транзакция **внутри** тела цикла:

```kotlin
forEach(orders, parallel = 4) { o ->
    transactional(jdbc(db)) {
        execute("update orders set status = 'PROCESSING' where id = :id", "id" to o.id)
        execute("insert into order_audit(order_id, event) values (:id, 'process_start')", "id" to o.id)
    }
}
```

**Под dry-run `transactional` не пропускает блок целиком.** Тело выполняется: чтения работают,
а отдельные записи внутри него скипаются каждая своим `guardWrite`. Реальный `Connection` при
этом не открывается (не нужен BEGIN/COMMIT round-trip на каждый блок), в отчёт идёт метка
`jdbc.transactional`.

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

В Kora канонический способ публиковать — **типизированный `@KafkaPublisher`**-интерфейс:
объявляешь контракт, процессор генерирует реализацию. DSL-хендлы `topic(...)` / `kafka(...)`
работают поверх «сырого» `org.apache.kafka.clients.producer.Producer`, который в графе надо
завести самому (см. способ 2). Ниже — три способа, в порядке убывания частоты применения.

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

Чтобы `@KafkaPublisher` сгенерировался, в `@KoraApp` нужен `KafkaModule` (артефакт
`ru.tinkoff.kora:kafka`) и KSP-процессор из [§1](#1-установка-и-подключение).

`mutation("label", args = ...) { ... }` оборачивает вызов в dry-run gate. **`label` —
константа** (`"orders.resync"`), чтобы в `report.dryRunSkipped` получился чистый агрегат
`mutation:orders.resync: 8421`. **`args` — диагностика** для лога (`(orderId=42)` в каждой
INFO-строке).

Обёртка здесь не формальность: типизированный публишер — обычный компонент графа, DSL про него
ничего не знает и сам перехватить вызов не может. Без `mutation { }` репетиция отправит
сообщения по-настоящему; единственный признак — предупреждение «intercepted 0 writes» в отчёте
(см. [§15](#15-dry-run)).

Плюсы: типизированный API, `@Json` сериализация автоматом, имя топика в HOCON
(env-override возможен). Минусы: для каждого `mutation { publisher.method() }` нет
batch-flush-coordination (Kora-генерированный метод — sync per call).

### Способ 2: raw `Producer<K, V>` + `topic(...)` handle

Когда хочется явный handle со счётчиками DSL и авто-`flush()` на закрытии. Нужен «сырой»
`Producer<K, V>` — Kora такой бин сама по себе не публикует, поэтому его либо объявляют
компонентом графа:

```kotlin
@Module
interface OrdersProducerModule {

    fun ordersProducer(config: MyKafkaConfig): Producer<String, ByteArray> = KafkaProducer(
        mapOf("bootstrap.servers" to config.brokers(), "acks" to "all"),
        StringSerializer(),
        ByteArraySerializer(),
    )
}

@Component
class Task(private val producer: Producer<String, ByteArray>) : Migration(...) {
    override fun MigrationContext.migrate() {
        // Продюсер создан нами — нам его и закрывать: DSL делает flush(), но не close().
        register(AutoCloseable { producer.close() })

        val resync = topic(producer, "orders.resync")
        forEach(orders) { order ->
            resync.send(order.id.toString(), encode(order))
        }
    }
}
```

…либо берут у уже объявленного Kora-публишера: сгенерированная реализация отдаёт свой
`producer()` — но типы там `Producer<ByteArray, ByteArray>`, то есть сериализация ключа и
значения остаётся на тебе.

Порядок в примере не случаен: ресурсы закрываются в **обратном** порядке регистрации, поэтому
`register { producer.close() }` до `topic(...)` гарантирует, что продюсер закроется последним —
уже после `flush()` хендла.

`KafkaTopic.send(...)` — sync publish (`producer.send(...).get()`). Под dry-run возвращает
`null`, в отчёт идёт `kafka.publish` с разбивкой по topic+key. Есть и
`KafkaTopic.sendAsync(key, value)` — то же, но без ожидания ack (про учёт — ниже).

Хендл мемоизирован: `topic(producer, "orders.resync")` на пару (продюсер, имя) отдаёт один и
тот же объект, поэтому его можно звать хоть прямо в теле `forEach` — реестр ресурсов не
разрастётся. `close()` (его вызывает runner в `finally`) делает `producer.flush()`, сам
`Producer` не закрывает.

Плюсы: одна точка подачи топика, `flush()` один раз в `finally`, счётчики и dry-run-метки без
ручной обёртки. Минусы: нужно знать K/V типы заранее и нет `@Json`-сахара — сериализатор
задаёшь через конфиг продюсера.

### Способ 3: `kafka(producer).publish/publishAsync` ad-hoc

Когда из одного скрипта пишешь в **разные** топики и handle на каждый — оверкилл:

```kotlin
kafka(producer).publish("orders.resync", key, value)
kafka(producer).publish("orders.audit",  key, auditEvent)

// Или async:
val future = kafka(producer).publishAsync("orders.resync", key, value)
```

`kafka(producer)` тоже мемоизирован (по продюсеру), так что вызов в теле цикла ничего не плодит.
Sync-вариант блокируется на ack, async — возвращает `CompletableFuture`. Оба под dry-run ничего
не отправляют и инкрементят `report.dryRunSkipped` (`kafka.publish` / `kafka.publishAsync`).

**Про учёт async-доставки.** `forEach` засчитывает item успешным в момент отправки, а брокер
отвечает позже — и возвращаемый future в реальных скриптах почти никто не читает. Поэтому отказ
доставки обрабатывается в самом callback'е: строка уходит в `errors.csv`, растёт счётчик
`report.asyncFailed`, в отчёте появляется строка `✗ Async delivery failed`, а runner поднимает
exit-код до `1` (см. [§21](#21-exit-коды)). Раньше сообщение терялось молча при уже засчитанном
успешном item'е.

Чтобы к моменту печати отчёта все callback'и успели отработать, закрытие хендла делает
`flush()` — то есть считать `asyncFailed` можно после прогона, цифра финальная. Если отказы
были, в `warnings` отчёта уедет ещё и строка `kafka.publishAsync: N message(s) were rejected
by the broker`.

### Что выбирать

| Сценарий | Способ |
|---|---|
| Один скрипт, один топик, типизированный value (`@Json`) | **1** — `@KafkaPublisher.Topic` + `mutation` |
| Высокая нагрузка, один топик, нужен явный flush-handle и счётчики DSL | **2** — raw `Producer` + `topic(...)` |
| Несколько топиков ad-hoc, без оверкилла | **3** — `kafka(producer).publish(...)` |

---

## 13. HTTP — типизированный клиент + `mutation` + `http(call)`

Есть два способа сделать HTTP-вызов из миграции.

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

Под dry-run [мутация](../core/src/main/kotlin/io/github/dsudomoin/migration/Mutation.kt) **не выполнится**, в лог пойдёт
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

Write-методы (`post/patch/put/delete`) идут через dry-run gate автоматом (метки `http.post`,
`http.patch`, `http.put`, `http.delete`). Read (`get`) — нет, выполняется всегда.

### Код ответа проверяется всегда

Любой статус вне `200..299` поднимает `HttpStatusException(method, path, status)` — и у `get`, и
у всех write-методов. Без этого мёртвый бэкенд, отвечающий 500 на каждый запрос, дал бы отчёт
«100 000 successful» при нулевом эффекте бэкфилла: `HttpCall` возвращает просто `Int`, и без
проверки код 500 выглядел бы как удачная обработка item'а.

Исключение штатно доходит до `OnError` и до `errors.csv`, поэтому политику по коду ответа
пишут прямо в цикле:

```kotlin
forEach(
    users,
    onError = OnError.handle { e, _ ->
        if (e is HttpStatusException && e.status == 409) OnError.Decision.Skip else OnError.Decision.Fail
    },
) { user -> http(call).post("/v1/users/${user.id}/refresh") }
```

Под dry-run write-методы возвращают `200`, а не `0`: вызывающий код почти всегда смотрит на
статус, и ноль отправил бы репетицию в ветку ошибки — dry-run обязан идти тем же путём, что и
бой.

---

## 14. Конфигурация HOCON

Секция `migration { ... }` целиком опциональна — у каждого ключа есть значение по умолчанию.
Без `migration.run` runner просто ничего не делает (idle).

```hocon
migration {
  # Имя миграции к запуску. null = runner idle (полезно когда сервис ещё и API хостит).
  run = ${?MIGRATION_RUN}

  # Dry-run: write-операции через guardWrite пропускаются, в отчёт идёт разбивка skipped writes.
  dryRun = false
  dryRun = ${?MIGRATION_DRY_RUN}

  # Папка для артефактов. null → logs/<имя миграции>.
  outputFolder = ${?MIGRATION_OUTPUT_FOLDER}

  defaults {
    onUnhandled    = FAIL_FAST            # FAIL_FAST | LOG_AND_COMPLETE
    errorThreshold = 0                    # >0 — exit 1 если skipped > threshold
    progressEvery  = 1000                 # период дефолтного Progress.Default
    parallel       = 1                    # значение аргумента parallel у forEach по умолчанию
  }

  errorReporting {
    includeStackTrace = true              # писать ли стектрейсы в errors.log
    maxItemReprLength = 500               # обрезать itemRepr в errors.csv до этой длины
  }

  report {
    asciiOnly = false                     # для CI без UTF-8 терминала
  }
}
```

| Ключ | Тип | Дефолт | Что |
|---|---|---|---|
| `run` | `String?` | `null` | имя миграции (совпадает с `Migration(name = ...)`). `null` — runner idle |
| `dryRun` | `Boolean` | `false` | репетиция без записей (см. [§15](#15-dry-run)) |
| `outputFolder` | `String?` | `null` → `logs/<имя миграции>` | папка артефактов. Тип именно `String`, не `Path` |
| `defaults.onUnhandled` | `FAIL_FAST` / `LOG_AND_COMPLETE` | `FAIL_FAST` | политика на исключение вне `forEach`; перебивается аргументом `Migration(onUnhandled = ...)` |
| `defaults.errorThreshold` | `Long` | `0` (выключен) | если `skipped > threshold` — прогон прерывается, exit 1 |
| `defaults.progressEvery` | `Int` | `1000` | период `Progress.Default` |
| `defaults.parallel` | `Int` | `1` | **значение аргумента `parallel` у `forEach` по умолчанию** |
| `errorReporting.includeStackTrace` | `Boolean` | `true` | писать ли `errors.log` |
| `errorReporting.maxItemReprLength` | `Int` | `500` | обрезка `itemRepr` в `errors.csv` |
| `report.asciiOnly` | `Boolean` | `false` | ASCII-рендер отчёта для CI без UTF-8 |

Значения проверяются на старте: `parallel <= 0`, `progressEvery <= 0`, `errorThreshold < 0`,
`maxItemReprLength <= 0` — это мисконфиг, runner пишет в лог конкретный ключ и завершается с
кодом `2`, не начиная прогон.

Env-overrides через `${?VAR}` — стандартный HOCON-синтаксис, и **единственный** способ дотянуться
до окружения: `System.getenv` в библиотеке не вызывается нигде. Нет строки `ключ = ${?VAR}` —
переменная не действует.

### `defaults.parallel` сменил смысл

Раньше этот ключ задавал размер фиксированного пула потоков runner'а, и он же был потолком
параллелизма: при `parallel = 1` в конфиге вызов `forEach(parallel = 8)` всё равно работал в
один поток. Теперь ключ задаёт **значение аргумента `parallel` по умолчанию**, а пул runner'а —
cached и выдаёт столько воркеров, сколько запросил конкретный цикл.

Что это значит на практике:
- `forEach(parallel = 8)` даёт восемь воркеров независимо от конфига;
- поднимать `defaults.parallel` нужно только если хочешь, чтобы **все** циклы без явного
  аргумента шли параллельно;
- `defaults.parallel = 1` (дефолт) — по-прежнему безопасная последовательная обработка.

### Конфиг библиотеки — интерфейс, а не data class

`MigrationConfig` объявлен как `@ConfigValueExtractor interface` с default-методами (доступ
методами: `config.run()`, `config.defaults().parallel()`), а экстрактор приезжает вместе с
`MigrationModule`. Пользователю это менять не нужно — важно только следствие: **никаких
дополнительных модулей ради конфига подключать не надо**, HOCON-ключи те же.

Для программной сборки конфига без HOCON (тесты, встраивание runner'а) есть data-классы
`MigrationConfigValues`, `DefaultsValues`, `ErrorReportingValues`, `ReportValues` — см.
[§20](#20-тестирование-скриптов).

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

Осторожно с дефолтами в Kotlin: KSP **не видит** значений по умолчанию у параметров конструктора
(`var batchSize: Int = 200` всё равно станет обязательным ключом). Если ключ должен быть
опциональным — объявляй конфиг интерфейсом с default-методами, как это сделано у самой
библиотеки.

---

## 15. Dry-run

```bash
MIGRATION_RUN=SAMPLE-001 MIGRATION_DRY_RUN=true ./gradlew run
```

Работает это только если в `application.conf` есть строка `dryRun = ${?MIGRATION_DRY_RUN}` —
иначе переменную никто не прочитает и прогон пойдёт боевым (см. [§1](#1-установка-и-подключение)).

Что происходит:
- **Read-операции** (`jdbc.query/stream`, `cassandra.query/stream`, `readCsv`, `http(call).get`,
  типизированный `@HttpClient` GET) — выполняются обычно.
- **Write через DSL** (`jdbc.execute`, `jdbc.batch`, `jdbc.executeReturning`,
  `cassandra.execute`, `kafka.publish`, `topic.send`, `mutation { }`, `http(call).post`) —
  **не** выполняются. В лог `INFO [DRY-RUN] <label> ...`, в отчёт
  `report.dryRunSkipped[label] += 1`. Возвращается нейтральное значение: `execute` → `0`,
  `executeReturning` → пустой список, `publish` → `null`, HTTP-write → `200`.
- **`transactional { }`** — блок **выполняется**, реальный `Connection` не открывается,
  пропускаются отдельные записи внутри него. Запрет вложенного `transactional` под dry-run
  тоже действует (см. [§10](#10-postgres--jdbc--jdbcdb-и-transactional)).
- **`openCsv` файлы** — пишутся (диагностический артефакт).

В отчёте под dry-run появится дополнительная строка:
```
  ⌀ Dry-run skipped writes:    (jdbc.execute: 1947000, kafka.publish: 973500, mutation:auth.refresh: 147)
```

Из этой строки видно: сколько именно UPDATE/INSERT улетело бы в БД, сколько в Kafka, и т.д.
Готовая оценка масштаба перед боевым прогоном.

**Что НЕ под dry-run-gate:**
- Кастомный код, который ты пишешь сам без обёрток: вызов repository-метода, типизированного
  `@KafkaPublisher` или `@HttpClient`, вручную созданный `KafkaProducer`. DSL про эти вызовы
  ничего не знает и перехватить их не может — под репетицией они выполняются **по-настоящему**.
  Оборачивай в `mutation("...") { ... }` или используй DSL-функции.

Забытый `mutation { }` — самый дорогой тихий промах, поэтому у него есть наблюдаемый признак:
если под dry-run обработан хоть один item, но не перехвачено ни одной записи, runner пишет WARN
в лог и в `warnings` отчёта:

```
DRY-RUN processed 8393 item(s) but intercepted 0 writes. If this migration writes anything,
those writes went through FOR REAL — wrap typed client calls in mutation("label") { ... }
```

Увидел эту строку в репетиции — считай, что прогон был боевым, и разбирайся до повтора.

---

## 16. `outputFolder` и артефакты прогона

```
logs/SAMPLE-001/
├── migration.log          # slf4j root logger — всё, что писалось в JVM за время прогона
├── processed.csv          # твои openCsv(...) — каждый свой файл
├── failed.csv
├── errors.csv             # авто-аудит item-уровневых ошибок — ЛЕНИВО, с первой ошибки
├── errors.log             # стектрейсы по тем же ошибкам — тоже лениво
└── ...
```

`errors.csv` и `errors.log` создаются **не всегда**: аудитор открывает их в момент первой
записанной ошибки. Прогон без единого SKIP-а не оставляет пустых файлов, и строк
`Error details:` / `Error traces:` в отчёте тоже не будет.

Обратная сторона: если ошибки были в **прошлом** прогоне, а в текущем их нет — старые файлы
никто не перезапишет (перезапись происходит при первой ошибке), и отчёт покажет `Error details:`
со ссылкой на вчерашние данные. Хочешь чистую картину — чисти папку между прогонами или задавай
`MIGRATION_OUTPUT_FOLDER` с timestamp'ом.

Папка определяется так (в порядке приоритета):

1. `migration.outputFolder` в HOCON.
2. Env-override `MIGRATION_OUTPUT_FOLDER`.
3. Дефолт `logs/${migration.name}` относительно JVM CWD.

Создаётся (`mkdir -p`) на старте runner'ом. Файлы `TRUNCATE`-аются при ререн — один прогон =
один набор артефактов: `migration.log` и `openCsv`-файлы в момент открытия, `errors.csv` /
`errors.log` — в момент первой ошибки (см. выше). Если нужна история — заархивируй папку после
прогона (или подставь timestamped путь через env).

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
2026-05-15T10:42:11.318Z,SAMPLE-001,team,"Order(id=42, customerId=cust-1)",NetworkException,timeout
```

`timestamp` — ISO-8601 в **UTC**. В итоговом отчёте времена печатаются в зоне JVM со смещением
(`2026-05-15 13:42:11 +03:00`) — именно затем, чтобы одно с другим сходилось без гадания.

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
Started:   2026-05-15 13:40:00 +03:00
Finished:  2026-05-15 13:47:15 +03:00
Duration:  7m 15s
Mode:      REAL
───────────────────────────────────────────────────────────
Processed:                 8 393
  ✓ Successful:            8 380
  ⊘ Skipped (errors):      13
  ✗ Failed:                0
───────────────────────────────────────────────────────────
Error details:  logs/SAMPLE-001/errors.csv
Error traces:   logs/SAMPLE-001/errors.log
═══════════════════════════════════════════════════════════
```

Разбор строк:

- **Started / Finished** — со смещением зоны (`+03:00`). Зона — та, в которой живёт JVM;
  смещение печатается затем, чтобы метки сходились с `errors.csv`, который пишет UTC.
- **Duration** — человекочитаемо: `45s`, `7m 15s`, `2h 3m 11s`.
- **Mode** — `REAL` или `DRY-RUN`.
- **Processed** — item'ов (или батчей, если `forEach` с `chunk`). Числа от 1000 разделяются
  пробелом.
- **Error details / Error traces** — печатаются только если файлы существуют, то есть если была
  хотя бы одна ошибка (см. [§16](#16-outputfolder-и-артефакты-прогона)). Количества строк в
  отчёте нет — смотри `Skipped`.

Если были отказы асинхронной доставки в Kafka, добавляется строка (**только когда значение
больше нуля** — постоянного нуля в отчёте нет):
```
  ✗ Async delivery failed:  17
```
Это сообщения, которые брокер отверг уже после того, как item был засчитан успешным. Они не
входят в `Failed`, но поднимают exit-код до `1` (см. [§21](#21-exit-коды)).

Под dry-run:
```
  ⌀ Dry-run skipped writes:    (jdbc.execute: 16842, kafka.publish: 8421)
```
Разбивка отсортирована по label'у, поэтому отчёты двух прогонов сравнимы построчно. Пробелами
разделяются только счётчики сверху (`8 393`); внутри разбивки числа печатаются как есть.

Если при закрытии ресурсов что-то падало (или runner заметил что-то подозрительное) — появится
блок:
```
  ⚠ Warnings:
    - resource close failed: KafkaTopic(orders.resync) (TimeoutException: flush timed out)
```

Сюда же уезжают: предупреждение про dry-run без единой перехваченной записи
([§15](#15-dry-run)), отказы async-доставки, невставший за 30 секунд пул потоков (с числом
брошенных задач) и отсутствие logback на classpath.

Warnings — нефатальные. На exit-code не влияют.

В CI без UTF-8 терминала включай `migration.report.asciiOnly = true` — глифы заменяются на
`[OK]` / `[SK]` / `[FL]` / `[!]` / `[--]`, рамки на `=` и `-`.

---

## 19. `ScriptPolicy` — что делать при неожиданном падении

Если внутри `migrate()` (за пределами любого `forEach`) вылетит исключение, поведение зависит
от параметра `onUnhandled` в `Migration(...)`.

```kotlin
class Task : Migration(
    name = "TASK-X",
    author = "you",
    onUnhandled = ScriptPolicy.LOG_AND_COMPLETE,  // не задан → migration.defaults.onUnhandled
)
```

Аргумент `onUnhandled` по умолчанию `null` — тогда политику берут из
`migration.defaults.onUnhandled` (из коробки `FAIL_FAST`). Заданный в коде аргумент сильнее
конфига.

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
- Принимает те же дефолты, что и конфиг: `dryRun`, `defaultParallel`, `defaultProgressEvery`,
  `errorThreshold` — так тестируют поведение под репетицией и под порогом ошибок.

### Конфиг в тестах — без HOCON

Когда тест поднимает настоящий граф или конструирует `MigrationRunner` руками, конфиг собирают
data-классами:

```kotlin
val config = MigrationConfigValues(
    run = "SAMPLE-001",
    dryRun = true,
    outputFolder = tmp.toString(),
    defaults = DefaultsValues(parallel = 4, errorThreshold = 10),
)
```

Есть и `ErrorReportingValues`, `ReportValues` — у всех дефолты совпадают с HOCON-дефолтами.

Чтобы прогон не убил JVM тест-раннера `exitProcess`'ом, положи в граф компонент
`MigrationExit` — runner отдаст код возврата в него:

```kotlin
@Component
class TestExit : MigrationExit {
    override fun exit(code: Int) { Probe.exitCode = code }
}
```

Без такого компонента поведение прежнее — runner завершает процесс.

### Контейнерные тесты — под тегом `docker`

Тесты на реальных JDBC/Cassandra/Kafka помечены `@Tag("docker")` и **по умолчанию исключены**:
обычный `./gradlew build` проходит на машине без Docker. Прогнать их:

```bash
./gradlew test -PwithDocker
```

Образцы — `kora/src/test/.../ops/SqlOpsIntegrationTest.kt`, `KafkaOpsIntegrationTest.kt`,
`CassandraOpsIntegrationTest.kt`. Тем же тегом стоит помечать и собственные контейнерные тесты.

### Модуль `:example` — рабочий образец

В репозитории есть модуль [`example/`](../example) — целое приложение-миграция, собранное ровно
так, как соберётся чужой сервис (KSP, `@KoraApp : HoconConfigModule, MigrationModule`, `@Component`-
миграция, `application.conf`). Это одновременно и шаблон для копирования, и проверка того, что
документированный wire-up действительно работает на настоящем графе Kora:

- `example/src/main/kotlin/.../ExampleApp.kt` — `@KoraApp`, `main()`, компонент-репозиторий;
- `example/src/main/kotlin/.../BackfillCustomerTier.kt` — миграция `CUSTOMER-TIER-001`
  (`readCsv(onRowError = Skip)` + `forEach(parallel = 4)` + `mutation { }` + `openCsv`);
- `example/src/test/kotlin/.../KoraWireUpTest.kt` — тесты на сборку графа, реальный параллелизм,
  отсутствие deadlock'а во вложенном `forEach`, проброс `dryRun` и коды возврата.

Запуск:

```bash
MIGRATION_RUN=CUSTOMER-TIER-001 ./gradlew :example:run                        # боевой прогон
MIGRATION_RUN=CUSTOMER-TIER-001 MIGRATION_DRY_RUN=true ./gradlew :example:run # репетиция
```

---

## 21. Exit-коды

| Код | Что значит |
|---|---|
| `0` | Success. Включая `LOG_AND_COMPLETE`-сценарии (было unhandled-исключение, но runner довёл прогон до конца). Без `migration.run` runner вообще не трогает код возврата: процесс живёт дальше по своим правилам. |
| `1` | `FAIL_FAST` от unhandled-исключения; `errorThreshold` превышен (проверка и в реальном времени внутри `forEach`, и post-mortem в конце); были отказы async-доставки в Kafka (`report.asyncFailed > 0`). |
| `2` | Мисконфиг — прогон не начинался: неизвестное имя миграции; дубликат имён `Migration` в графе; недопустимые значения конфига (`defaults.parallel <= 0`, `defaults.progressEvery <= 0`, `defaults.errorThreshold < 0`, `errorReporting.maxItemReprLength <= 0`); не удалось создать `outputFolder`. |

Отказы async-доставки поднимают код до `1` уже после того, как тело миграции отработало: они
прилетают в callback'ах продюсера и учитываются на `flush()` при закрытии хендла. Прогон,
потерявший сообщения, не имеет права закончиться нулём.

Прод-инфраструктура (CI/CD, K8s Job) должна читать exit-code и решать ретраить/алертить.

Если код возврата нужно перехватить вместо завершения процесса (тесты, встраивание runner'а в
живущее дальше приложение) — положи в граф компонент `MigrationExit`, см.
[§20](#20-тестирование-скриптов).

---

## 22. Шпаргалка-FAQ

**Q: Как пропустить миграцию через env, не правя HOCON?**
A: Просто не передавай `MIGRATION_RUN` — тогда ключа `migration.run` в конфиге нет, он `null`,
runner идёт в idle. На пустое значение (`MIGRATION_RUN= ./gradlew run`) рассчитывать нельзя:
`${?VAR}` пропускает подстановку только для **неопределённой** переменной, а определённая, но
пустая, подставится пустой строкой — и runner будет искать миграцию с пустым именем.

**Q: Поставил `MIGRATION_DRY_RUN=true`, а прогон всё равно писал в базу. Почему?**
A: Потому что в `application.conf` не было строки `dryRun = ${?MIGRATION_DRY_RUN}`. Библиотека
не читает окружение сама — это делает HOCON, и только по явной подстановке. Проверь §1, а перед
боем смотри строку `Mode:` в отчёте: под репетицией там `DRY-RUN`.

**Q: Почему `query("insert ... returning id")` больше не работает?**
A: Так и задумано: `query`/`stream` принимают только читающие запросы, потому что они не проходят
dry-run gate — такой «read» выполнялся бы в бою во время репетиции. Пиши
`executeReturning(...)` — он проходит гейт как обычная запись и под dry-run возвращает пустой
список. См. §10.

**Q: Почему `transactional { forEach(...) }` падает с `IllegalStateException`?**
A: `java.sql.Connection` не потокобезопасен, а транзакционный `SqlOps` привязан к потоку,
открывшему транзакцию. Разверни конструкцию: `forEach(...) { transactional(jdbc(db)) { ... } }`.
Вложенный `transactional` тоже запрещён — и в бою, и под dry-run. См. §10.

**Q: Как кастомизировать ThreadFactory у `forEach`?**
A: Опубликуй свой `Executor`-бин с `@Tag(MigrationExecutor::class)`. Runner подхватит и не будет
его гасить (это твой ресурс). Учти: реальный параллелизм ограничен возможностями твоего пула —
если это `newFixedThreadPool(2)`, то `forEach(parallel = 8)` получит два воркера, а вложенный
параллельный `forEach` может встать намертво. Дефолтный (cached) пул этой проблемы не имеет.

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
и `topic(p2, "...")`. Хендлы мемоизируются по паре (продюсер, имя), так что звать их можно и в
теле цикла — объектов не прибавится.

**Q: Отчёт зелёный, но в Kafka сообщений меньше, чем items. Где смотреть?**
A: Строка `✗ Async delivery failed` в отчёте (печатается только когда значение > 0) и сами
отказы в `errors.csv`. `publishAsync` / `sendAsync` считают item успешным в момент отправки,
поэтому отказ доставки учитывается отдельным счётчиком и поднимает exit-код до 1. См. §12 и §21.

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
A: Да. Иначе runner залогирует warn `migration.log file output disabled: Logback not on classpath`
(он же уедет в `warnings` отчёта), и `migration.log` не появится. Лучше держать `runtimeOnly("ch.qos.logback:logback-classic:...")`.

**Q: Хочу свой S3-клиент / Redis / etc — как добавить в DSL?**
A: Пишешь свой extension `MigrationContext.myOps(client)` с обёрткой над `guardWrite` для
write-операций. Подробно с примерами — [examples/customization.md](examples/customization.md).

**Q: Хочу свой Executor с MDC / метриками — как?**
A: `@Component class { @Tag(MigrationExecutor::class) fun executor(): Executor = ... }`.
Runner подхватит. См. [examples/customization.md §3](examples/customization.md#3-кастомный-executor-для-foreach).

**Q: Хочу свой формат отчёта об ошибках (JSON / Kibana) вместо CSV — как?**
A: Сложнее. `CsvFileErrorReporter` сейчас захардкожен в `MigrationRunner`. Если нужно
— можно сделать decorator над `DefaultMigrationContext` через свой `internalCreate`-аналог
(`internalCreate` публичный). Открой issue или подумаем как сделать pluggable.
