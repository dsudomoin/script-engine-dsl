# Migration DSL — гайд по использованию

Большой документ для тех, кто пишет миграционный скрипт первый раз. Внутри —
по шагам от Gradle-зависимостей до запуска на проде и разбора отчёта.

Главная идея, вокруг которой построено всё остальное: миграция — это **план**, а не процедура.
Код объявляет узлы (`input`, `output`, `validate`, `source`, `scoped`), а исполняет их runner —
один раз, только у выбранной миграции и только после старта приложения. Построение плана не
выполняет ни одной пользовательской лямбды.

## Содержание

1. [Установка и подключение](#1-установка-и-подключение)
2. [Первый скрипт](#2-первый-скрипт)
3. [Запуск](#3-запуск)
4. [План миграции — узлы билдера](#4-план-миграции--узлы-билдера)
5. [Контексты прогона — `RunScope` и его наследники](#5-контексты-прогона--runscope-и-его-наследники)
6. [`source` — стадия с источником и обработчиком](#6-source--стадия-с-источником-и-обработчиком)
7. [`scoped` — стадии с барьером на родителя](#7-scoped--стадии-с-барьером-на-родителя)
8. [`pages` — курсорная пагинация](#8-pages--курсорная-пагинация)
9. [`write` и `writeRows` — контролируемая запись](#9-write-и-writerows--контролируемая-запись)
10. [`publish` — асинхронная отправка и барьер](#10-publish--асинхронная-отправка-и-барьер)
11. [`ItemError` — политика ошибок элемента](#11-itemerror--политика-ошибок-элемента)
12. [`input` и `validate` — конфиг прогона](#12-input-и-validate--конфиг-прогона)
13. [`output` — CSV-выход прогона](#13-output--csv-выход-прогона)
14. [`Progress` — прогресс-логирование](#14-progress--прогресс-логирование)
15. [Чтение CSV — `readCsv`](#15-чтение-csv--readcsv)
16. [Postgres / JDBC — `jdbc(db)` и `transactional`](#16-postgres--jdbc--jdbcdb-и-transactional)
17. [Cassandra — `cassandra(session)`](#17-cassandra--cassandrasession)
18. [Kafka — `topic`, `kafka` и типизированный publisher](#18-kafka--topic-kafka-и-типизированный-publisher)
19. [HTTP — типизированный клиент и `http(call)`](#19-http--типизированный-клиент-и-httpcall)
20. [Конфигурация HOCON](#20-конфигурация-hocon)
21. [Dry-run](#21-dry-run)
22. [`outputFolder` и артефакты прогона](#22-outputfolder-и-артефакты-прогона)
23. [`errors.csv` и `errors.includeItem<T>`](#23-errorscsv-и-errorsincludeitemt)
24. [Итоговый отчёт](#24-итоговый-отчёт)
25. [`ScriptPolicy` — что делать при неожиданном падении](#25-scriptpolicy--что-делать-при-неожиданном-падении)
26. [Тестирование скриптов](#26-тестирование-скриптов)
27. [Exit-коды](#27-exit-коды)
28. [Шпаргалка-FAQ](#28-шпаргалка-faq)

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
    implementation("ru.tinkoff.kora:common:1.2.20")
    implementation("ru.tinkoff.kora:config-common:1.2.20")
    implementation("ru.tinkoff.kora:config-hocon:1.2.20")
    implementation("ru.tinkoff.kora:application-graph:1.2.20")
    implementation("ru.tinkoff.kora:database-jdbc:1.2.20")      // если нужен Postgres
    implementation("ru.tinkoff.kora:kafka:1.2.20")              // если нужна Kafka
    implementation("ru.tinkoff.kora:http-client-jdk:1.2.20")    // если нужен HTTP

    runtimeOnly("org.postgresql:postgresql:42.7.7")             // драйвер БД
    runtimeOnly("ch.qos.logback:logback-classic:1.5.16")        // logging backend

    // Кодогенерация Kora для Kotlin — KSP. Другого пути нет.
    ksp("ru.tinkoff.kora:symbol-processors:1.2.20")
}
```

**Только KSP, никакого kapt.** Для Kotlin Kora поддерживает единственный процессор —
`ru.tinkoff.kora:symbol-processors`, подключаемый конфигурацией `ksp`. Связка
`kotlin("kapt")` + `kapt("ru.tinkoff.kora:annotation-processors")` (как и голый
`annotationProcessor(...)`) для Kotlin-проекта не работает: аннотации не обрабатываются,
`@KoraApp`-граф не генерируется, сборка падает на отсутствующем `AppGraph`.

Модуль `migration-dsl-core` не зависит от Kora вовсе: в нём план, scope'ы, эффекты, CSV и
интерпретатор. `migration-dsl-kora` — мост в Kora: runner, HOCON-конфиг и операции
(`jdbc`, `cassandra`, `kafka`, `topic`, `http`).

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

Все миграции runner собирает через `All<MigrationDefinition>` — достаточно пометить свой класс
`@Component`.

Остальное — по потребности скрипта: `JdbcDatabaseModule` (артефакт `database-jdbc`),
`CassandraDatabaseModule` (`database-cassandra`), `JdkHttpClientModule` (`http-client-jdk`),
`KafkaModule` (`kafka`, нужен если объявляешь `@KafkaPublisher`).

> **`KafkaProducerModule` не существует.** Такого класса в Kora нет; если он остался в
> `@KoraApp` со старых версий гайда — модуль просто не скомпилируется. Модуль Kafka называется
> `KafkaModule`, а как достать raw `Producer` для `topic(...)` / `kafka(...)` —
> см. [§18](#18-kafka--topic-kafka-и-типизированный-publisher).

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

Полную форму конфига см. в [§20](#20-конфигурация-hocon).

---

## 2. Первый скрипт

```kotlin
package com.example.migrations

import io.github.dsudomoin.migration.MigrationDefinition
import io.github.dsudomoin.migration.migration
import io.github.dsudomoin.migration.kora.ops.jdbc
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory

@Component
class HelloMig(private val db: JdbcConnectionFactory) : MigrationDefinition {

    override val name = "HELLO"

    override fun plan() = migration(name = name, author = "you") {
        val out = output("ids.csv", "id", "status")

        source(
            items = {
                jdbc(db).query("select id from customers where tier is null") { it.getLong("id") }
                    .asSequence()
            },
        ) { id ->
            writeRows("customer.tier", args = mapOf("id" to id)) {
                jdbc(db).execute("update customers set tier = 'SILVER' where id = :id", "id" to id)
            }
            out.row(id, "ok")
        }
    }
}
```

Что здесь происходит:

- **`@Component`** — Kora зарегистрирует класс в графе автоматически. Runner найдёт его через
  `All<MigrationDefinition>`.
- **`MigrationDefinition`** — интерфейс из двух членов: `val name` и `fun plan()`.
- **`name = "HELLO"`** — константа, совпадающая с `migration.run` в HOCON / env'е. По ней runner
  выбирает миграцию и проверяет дубли, **не строя ни одного плана**.
- **`plan()`** вызывается ровно один раз и только у выбранной миграции. Это метод, а не свойство:
  инициализатор свойства выполнился бы у всех компонентов при сборке графа.
- **`migration(name, author) { ... }`** — построитель. Тело выполняется сразу, но только
  регистрирует узлы: ни `items`, ни обработчик при этом не вызываются.
- **`output("ids.csv", "id", "status")`** — объявление CSV-выхода. Файл откроется в
  [`outputFolder`](#22-outputfolder-и-артефакты-прогона) (по умолчанию `logs/HELLO/`) один раз за
  прогон, движок его закроет.
- **`source(items = { ... }) { id -> ... }`** — стадия. `items` строит источник, лямбда за
  скобками — обработчик одного элемента. Число воркеров по умолчанию берётся из
  `migration.defaults.parallel` (из коробки `1`, то есть последовательно).
- **`writeRows("customer.tier") { ... }`** — запись через dry-run-гейт: под репетицией тело не
  выполняется, в отчёт идёт `dryRunSkipped["customer.tier"]`. Ноль изменённых строк трактуется как
  `Rejected`, а не как успех.

---

## 3. Запуск

```bash
MIGRATION_RUN=HELLO ./gradlew run                          # боевой прогон
MIGRATION_RUN=HELLO MIGRATION_DRY_RUN=true ./gradlew run   # репетиция (см. §21)
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

## 4. План миграции — узлы билдера

```kotlin
fun migration(
    name: String,
    author: String,
    onUnhandled: ScriptPolicy? = null,      // см. §25
    build: MigrationBuilder.() -> Unit,
): MigrationPlan
```

Внутри `build` доступны пять узлов:

| Узел | Сигнатура | Что делает |
|---|---|---|
| `input` | `input(name) { load }: Input<I>` | лениво загружаемое значение, общее на прогон ([§12](#12-input-и-validate--конфиг-прогона)) |
| `output` | `output(filename, vararg headers): OutputHandle` | CSV-выход прогона ([§13](#13-output--csv-выход-прогона)) |
| `validate` | `validate { check }` | проверка конфига до первой стадии ([§12](#12-input-и-validate--конфиг-прогона)) |
| `source` | `source(..., items) { handle }` | плоская стадия ([§6](#6-source--стадия-с-источником-и-обработчиком)) |
| `scoped` | `scoped(..., parents, items) { handle }` | стадия с барьером на каждого родителя ([§7](#7-scoped--стадии-с-барьером-на-родителя)) |

Полный скелет:

```kotlin
override fun plan() = migration(name = name, author = "team") {
    val shards = input("shards") { repo.activeShards() }
    val processed = output("processed.csv", "id", "result")

    validate {
        require(config.pageSize() in 1..10_000) { "pageSize out of range: ${config.pageSize()}" }
    }

    source(name = "warm-up", items = { warmUpIds().asSequence() }) { id ->
        processed.row(id, "warm")
    }

    scoped(
        name = "resend",
        parents = { resolve(shards).asSequence() },
        items = { shard -> shardItems(shard) },
    ) { item ->
        publish("items.resync", mapOf("id" to item.id)) { send(item) }
    }
}
```

### Что проверяется при построении плана

Всё это — `IllegalArgumentException` прямо из `migration { }`, то есть на `plan()`. Для runner'а
это мисконфиг: exit-код `2`, прогон не начинается ([§27](#27-exit-коды)).

- в плане должна быть **хотя бы одна** стадия;
- если стадий больше одной — **имя обязательно у каждой**;
- имена стадий не дублируются; имена `input`'ов не дублируются; файлы `output`'ов не дублируются;
- `validate { }` объявляется не более одного раза;
- `parallel`, если задан, строго `> 0`.

Единственная стадия может быть безымянной — это ровно тот случай, когда имя ничего не добавляет.

### `@DslMarker` — стадию нельзя объявить во время исполнения

Билдер помечен `@MigrationDsl`, поэтому вызвать `source` / `scoped` / `input` изнутри `items` или
обработчика **не получится: это ошибка компиляции**. План неизменяем, и мутировать его во время
исполнения нельзя даже случайно.

Практическое следствие: вложенных циклов в старом смысле не бывает. Нужна вложенность — это
`scoped` ([§7](#7-scoped--стадии-с-барьером-на-родителя)) или вторая стадия.

### Порядок исполнения

1. `validate { }` — один раз, до всего остального;
2. стадии **строго последовательно**, в порядке объявления; провал стадии не запускает следующие;
3. внутри стадии: построение источника → обработка элементов (при `parallel > 1` окно ограничено
   семафором) → ожидание всех воркеров → **барьер подтверждений**
   ([§10](#10-publish--асинхронная-отправка-и-барьер)) → закрытие ресурсов scope'а;
4. в `scoped` шаг 3 повторяется для каждого родителя: следующий родитель не открывается, пока
   предыдущий не подтвердил все `publish` и не закрыл свои ресурсы;
5. `output`-файлы закрываются в самом конце, после всех стадий.

---

## 5. Контексты прогона — `RunScope` и его наследники

У кода миграции три контекста, и различаются они только тем, что в них можно вызвать.

```kotlin
interface RunScope : ResourceRegistry     // общий предок
interface InputScope : RunScope           // input { } и validate { }
interface SourceScope : RunScope          // items { } и parents { }
interface HandlerScope : RunScope         // тело обработчика
```

### `RunScope` — доступно везде

| Член | Тип | Что |
|---|---|---|
| `dryRun` | `Boolean` | `true` если `migration.dryRun` = true |
| `log` | `slf4j.Logger` | Логгер `io.github.dsudomoin.migration.<имя_миграции>` — пиши сюда свои INFO/WARN/DEBUG |
| `report` | `ReportBuilder` | Счётчики прогона. Инкрементирует движок; тебе он нужен разве что в `Progress.Custom` |
| `outputFolder` | `Path` | Путь к папке артефактов ([§22](#22-outputfolder-и-артефакты-прогона)) |
| `errors` | `CsvFileErrorReporter` | Авто-аудитор; сюда регистрируются `errors.includeItem<T> { ... }` ([§23](#23-errorscsv-и-errorsincludeitemt)) |
| `register(c)` | `AutoCloseable → Unit` | зарегистрировать свой ресурс на **весь прогон**; закрытие в обратном порядке регистрации |
| `shared(key) { }` | `Any, () -> T → T` | ресурс, единственный на прогон для данного ключа |
| `guardWrite(label, args) { }` | — | низкоуровневый dry-run-гейт; нужен только авторам своих операций |
| `auditError(e, item)` | — | ручная запись в `errors.csv`; в обычной жизни не нужна |

Операции разделены по scope'ам: **чтение** объявлено на `RunScope` и доступно везде,
**запись** — на `HandlerScope`, то есть только в обработчике элемента. Источник описывает данные,
а не меняет внешний мир, и следит за этим компилятор: `jdbc(db).execute(...)` внутри `items { }`
не компилируется.

| Операция | `input` / `validate` / `parents` / `items` | `handle` |
|---|---|---|
| `jdbc(db).query` / `.stream` | да | да |
| `jdbc(db).execute` / `.batch` / `.executeReturning` | нет | да |
| `transactional(ops) { }` | нет | да |
| `cassandra(s).query` | да | да |
| `cassandra(s).execute` / `.batch` | нет | да |
| `http(call).get` | да | да |
| `http(call).post` / `.put` / `.patch` / `.delete` | нет | да |
| `kafka(p)` / `topic(p, name)` | нет | да |
| `readCsv` / `openCsv` / `pages` | да | да |
| `guardWrite` | да — низкоуровневый escape hatch, в гайдах не используется | да |

`kafka` и `topic` переехали целиком: читающей половины у них нет, а raw `publishAsync` из
источника вообще не доходил до барьера подтверждений.

`shared(key) { factory }` — на нём построены `kafka(producer)` и `topic(producer, name)`:
повторный вызов с тем же ключом отдаёт тот же объект. Именно поэтому их безопасно звать прямо в
теле обработчика — реестр не растёт по объекту на элемент. Тот же приём пригодится для своих
op-обёрток ([§28](#28-шпаргалка-faq)).

### `InputScope` — `input { }` и `validate { }`

Ничего сверх `RunScope`. В частности, **`resolve` здесь недоступен**: input не может разрешить
другой input, а `validate` не может заглянуть в загруженные данные. Валидируй то, что известно до
прогона — поля конфига миграции, окружение, права на папку.

### `SourceScope` — `items { }` и `parents { }`

| Член | Что |
|---|---|
| `resolve(input)` | значение `Input<I>`: вычисляется при первом обращении и кэшируется на весь прогон |
| `pages(first, next, nextCursor, continueWhen)` | ленивый курсорный источник ([§8](#8-pages--курсорная-пагинация)) |
| `scopedResource { close }` | закрытие на **границе текущего scope'а**: в `scoped` — на границе родителя, а не в конце прогона |

### `HandlerScope` — тело обработчика

| Член | Что |
|---|---|
| `write(name, args) { WriteOutcome }` | контролируемая запись, возвращает `WriteResult` ([§9](#9-write-и-writerows--контролируемая-запись)) |
| `writeRows(name, args) { Int }` | то же, но исход выводится из числа изменённых строк |
| `publish(name, args) { CompletionStage<*> }` | асинхронная отправка с подтверждением на барьере ([§10](#10-publish--асинхронная-отправка-и-барьер)) |

Тело билдера (`migration { ... }`) — **не** `RunScope`. `errors.includeItem<T> { }`, `log`,
`dryRun` там не видны: билдер только объявляет узлы. Регистрируй сериализаторы в `validate { }`
или в начале `items`.

---

## 6. `source` — стадия с источником и обработчиком

```kotlin
fun <T> source(
    name: String? = null,
    onItemError: ItemError<T> = ItemError.Fail,
    completionTimeout: Duration? = null,
    parallel: Int? = null,
    progress: Progress = Progress.Default,
    errorThreshold: Long? = null,
    items: SourceScope.() -> Sequence<T>,
    handle: HandlerScope.(T) -> Unit,
)
```

| Параметр | Дефолт | Что |
|---|---|---|
| `name` | `null` | имя стадии. Обязательно, если стадий больше одной |
| `onItemError` | `ItemError.Fail` | политика на ошибку элемента ([§11](#11-itemerror--политика-ошибок-элемента)) |
| `completionTimeout` | `null` (без ограничения) | сколько ждать подтверждений `publish` **после** исчерпания источника ([§10](#10-publish--асинхронная-отправка-и-барьер)) |
| `parallel` | `migration.defaults.parallel` (из коробки `1`) | число одновременных воркеров. `<= 0` — `IllegalArgumentException` при построении плана |
| `progress` | `Progress.Default` | прогресс-лог ([§14](#14-progress--прогресс-логирование)) |
| `errorThreshold` | `migration.defaults.errorThreshold` | порог SKIP-ов **по этой стадии**; превышение обрывает прогон |
| `items` | — | строит источник. Вызывается один раз в начале стадии |
| `handle` | — | обработчик одного элемента. Идёт последним, поэтому пишется за скобками |

`items` возвращает `Sequence<T>` и потребляется **лениво**: элемент читается тогда, когда есть
свободный воркер. Материализовать источник целиком (`.toList()`) стоит только если он заведомо
маленький.

### Параллелизм

```kotlin
source(name = "resync", parallel = 8, items = { ids().asSequence() }) { id ->
    // до восьми элементов одновременно, каждый в своём потоке пула runner'а
}
```

- `parallel = N` даёт **ровно N одновременно работающих воркеров**: пул runner'а cached и выдаёт
  столько потоков, сколько попросила стадия, а окно in-flight держит семафор.
- `parallel = 1` (дефолт из коробки) исполняется прямо в вызывающем потоке, минуя executor.
- Permit берётся **до** `next()` источника, поэтому ленивый источник читается ровно настолько,
  насколько его успевают потреблять — на курсорном источнике это естественный backpressure.
- `migration.defaults.parallel` задаёт только **значение аргумента по умолчанию**: явный
  `parallel = N` в коде всегда сильнее конфига.
- Стадии идут последовательно, поэтому две параллельные стадии подряд не конкурируют за пул и не
  встают в deadlock.

**При ошибке элемента уже запущенные воркеры доводятся до конца**, а не прерываются: новые задачи
не сабмитятся, но стадия ждёт, пока каждый освободит свой permit. Без этого «зомби»-воркер писал
бы в уже закрытый выход и регистрировал эффекты после барьера.

### Батчи — это `chunked`

Отдельного параметра `chunk` нет: батчинг — обычная операция над `Sequence`.

```kotlin
source(
    name = "compare",
    onItemError = ItemError.Skip,
    items = { readCsv("contracts.csv") { it.getValue("contract") }.chunked(200) },
) { batch ->            // batch: List<String>
    val values = cassandra(session).query(
        "select id, value from t.items where id in :ids", "ids" to batch,
    ) { it.getString("id") to it.getString("value") }
    ...
}
```

`chunked` на `Sequence` режет лениво и держит в памяти только текущий батч. Единицей учёта при
этом становится батч: `report.processed`, прогресс и `errorThreshold` считают батчи, а не строки.

Уместно там, где нужен `WHERE id IN :ids`, bulk-insert или меньше round-trip'ов в HTTP API.

### Чего в источнике делать нельзя

`items` возвращает `Sequence`, которая потребляется **после** возврата из `items`. Значит,
источник не может быть привязан к ресурсу, живущему только внутри callback'а. Главный пример —
`jdbc(db).stream(...)`: его `Sequence` валидна только внутри `consume`-блока
([§16](#16-postgres--jdbc--jdbcdb-и-transactional)). Варианты:

```kotlin
// ❌ так нельзя: ResultSet закроется до первого элемента стадии
items = { jdbc(db).stream("select id from orders", mapper = { it.getLong("id") }) { rows -> rows } }

// ✅ либо фолдим внутри consume и отдаём наружу материализованное
items = {
    jdbc(db).stream("select id from orders", mapper = { it.getLong("id") }) { rows -> rows.toList() }
        .asSequence()
}

// ✅ либо (для больших выгрузок) — курсорный источник, см. §8
items = { pages(first = { firstPage() }, next = { c -> nextPage(c) }, ...) }
```

Cassandra-`stream` этим не страдает: он ленив поверх пагинации драйвера и спокойно отдаётся
наружу ([§17](#17-cassandra--cassandrasession)).

> **Про retry.** Item-уровневого retry в DSL **нет** — это сознательный выбор: повтор всего тела
> обработчика ломает non-idempotent шаги (повторный publish в Kafka, повторный INSERT с auto-PK).
> Для transient-ошибок сети — Kora `@Retry` на типизированном `@HttpClient` / `@KafkaPublisher` /
> repository-методе (см. [§11](#11-itemerror--политика-ошибок-элемента)).

---

## 7. `scoped` — стадии с барьером на родителя

```kotlin
fun <P, T> scoped(
    name: String? = null,
    parents: SourceScope.() -> Sequence<P>,
    completionTimeout: Duration? = null,
    onItemError: ItemError<T> = ItemError.Fail,
    parallel: Int? = null,
    progress: Progress = Progress.Default,
    errorThreshold: Long? = null,
    items: SourceScope.(P) -> Sequence<T>,
    handle: HandlerScope.(T) -> Unit,
)
```

`scoped` — это `source`, разбитый на **scope'ы по родителям**. Родители обходятся строго
последовательно, и граница родителя — настоящий барьер:

1. читаются элементы текущего родителя и обрабатываются (внутри — тот же `parallel`);
2. стадия ждёт всех воркеров;
3. стадия ждёт **подтверждения всех `publish`** этого родителя;
4. закрываются `scopedResource` этого родителя;
5. только теперь открывается следующий родитель.

Зачем: когда «пачка» имеет смысл в целевой системе (шард, стратегия, клиент, день), нельзя
начинать следующую, пока предыдущая не долетела. Иначе отчёт покажет успех, а половина сообщений
будет ещё в буфере продюсера.

```kotlin
override fun plan() = migration(name = name, author = "team") {
    val strategies = input("strategies") { repo.activeStrategies() }
    val report = output("resent.csv", "strategy", "position", "version")

    scoped(
        name = "resend-by-strategy",
        parents = { resolve(strategies).asSequence() },
        completionTimeout = Duration.ofMinutes(10),
        parallel = 8,
        onItemError = ItemError.Skip,
        items = { strategy ->
            log.info("strategy ${strategy.id}: начинаем")
            scopedResource { log.info("strategy ${strategy.id}: закончили") }

            pages(
                name = "positions",
                first = { repo.page(strategy.id, null, pageSize) },
                next = { version -> repo.page(strategy.id, version, pageSize) },
                nextCursor = { rows -> rows.last().version },
                continueWhen = { rows -> rows.size >= pageSize },
            ).filter { it.updatedAt >= from }
        },
    ) { position ->
        publish("positions.resync", mapOf("id" to position.id)) {
            topic(producer, "positions.resync").sendAsync(position.id, encode(position))
        }
        report.row(position.strategyId, position.id, position.version)
    }
}
```

Что важно знать про `scoped`:

- **`parents` читается в собственном scope'е.** Его `scopedResource` (например, курсор по списку
  родителей) закрывается в конце всей стадии, а не на границе родителя.
- **`completionTimeout` применяется к каждому родителю отдельно** и ограничивает только ожидание
  подтверждений после исчерпания его источника — не время чтения и не время отправки.
- **`errorThreshold` считается по родителю** и сбрасывается на его границе. У `source` — по всей
  стадии.
- **`progress` общий на стадию**: счётчик не обнуляется между родителями.
- **Ошибка внутри родителя останавливает стадию целиком** — следующий родитель не открывается.
  А вот `ItemError.Skip` внутри родителя обход не прерывает.
- `output` объявлен в билдере и открывается один раз на прогон — именно поэтому его нельзя
  заменить на `openCsv` внутри `items`: тот открывал бы файл на каждого родителя и затирал строки
  предыдущего.

---

## 8. `pages` — курсорная пагинация

```kotlin
fun <C : Any, T> pages(
    name: String? = null,
    first: () -> List<T>,
    next: (C) -> List<T>,
    nextCursor: (List<T>) -> C,
    continueWhen: (List<T>) -> Boolean,
): Sequence<T>
```

Доступен в `items` и `parents` (`SourceScope`). Отдаёт ленивую последовательность: следующая
страница читается только тогда, когда потреблена текущая.

```kotlin
items = {
    pages(
        name = "orders",
        first = {
            jdbc(db).query(
                "select id, created_at from orders where status = :s order by id limit :n",
                "s" to "NEW", "n" to pageSize,
            ) { Order(it.getLong("id"), it.getTimestamp("created_at").toInstant()) }
        },
        next = { cursor ->
            jdbc(db).query(
                "select id, created_at from orders where status = :s and id > :cursor order by id limit :n",
                "s" to "NEW", "cursor" to cursor, "n" to pageSize,
            ) { Order(it.getLong("id"), it.getTimestamp("created_at").toInstant()) }
        },
        nextCursor = { rows -> rows.last().id },
        continueWhen = { rows -> rows.size >= pageSize },
    ).filter { it.createdAt >= from }
}
```

Правила:

- **`first` и `next` разделены не косметически.** Первая страница читается без предиката по
  курсору, последующие — строго `>` либо `<` от него. Побочно это разрывает цикл вывода типов,
  поэтому аннотации типов на стороне вызова не нужны.
- **Решения принимаются по сырой странице** — до твоих `filter` / `map`, которых движок не видит.
  Поэтому страница, целиком отсеянная фильтром, источник не завершает, а пустая сырая — завершает
  всегда, даже если `continueWhen` истинен.
- **`continueWhen`** обычно значит «страница полная»: `rows.size >= pageSize`. Вернул `false` —
  следующая страница не запрашивается.
- **Курсор обязан двигаться.** Если `nextCursor` вернул то же значение, что и на прошлой странице,
  бросается `CursorNotAdvancing` вместо бесконечного цикла. Если курсор законно повторяется
  (одинаковый `updated_at` у пачки строк) — сделай его составным:
  `data class Cut(val at: Instant, val id: UUID)`. Тип курсора произвольный, требуется только
  не-nullable (`C : Any`).
- **Сырые страницы попадают в отчёт** как `rawPages` / `rawRows` — единственный способ увидеть
  «прочитано» до пользовательских фильтров ([§24](#24-итоговый-отчёт)).

`pages` — рекомендуемый способ читать большие таблицы: в отличие от `jdbc(db).stream` он не держит
открытый курсор на всё время стадии и переживает возврат из `items`.

---

## 9. `write` и `writeRows` — контролируемая запись

```kotlin
fun write(name: String, args: Map<String, Any?> = emptyMap(), action: () -> WriteOutcome): WriteResult
fun writeRows(name: String, args: Map<String, Any?> = emptyMap(), action: () -> Int): WriteResult

sealed interface WriteOutcome { Applied; Rejected(reason) }                  // что сообщила операция
sealed interface WriteResult  { Applied; Rejected(reason); DryRunSkipped }   // что сделал движок
```

`write { }` — **единственный способ провести вызов чужого компонента через dry-run-гейт**.
Репозиторий Kora, типизированный `@KafkaPublisher`, `@HttpClient` — обычные компоненты графа, DSL
про них ничего не знает и сам перехватить вызов не может.

```kotlin
write("customer.tier", args = mapOf("id" to customer.id, "tier" to tier)) {
    repository.updateTier(customer.id, tier)
    WriteOutcome.Applied
}
```

- **`name` — константа** (`"customer.tier"`), чтобы в отчёте получился чистый агрегат
  `customer.tier: 8421`, а не по строке на каждый id.
- **`args` — диагностика**: попадают в INFO-строку лога и в `[DRY-RUN]`-строку.
- Под dry-run **тело не вызывается вовсе**, результат — `WriteResult.DryRunSkipped`, счётчик
  `dryRunSkipped[name]`.
- Исключение из тела — обычный сбой элемента: идёт через `ItemError`
  ([§11](#11-itemerror--политика-ошибок-элемента)) и в `errors.csv`.

### `Rejected` — это не ошибка

`Rejected` — ожидаемый отрицательный исход условного апдейта: ни одна строка не подошла, версия
разошлась, сущность уже в нужном состоянии. Элемент при этом считается успешным, а в отчёте растёт
`rejectedWrites[name]`.

```kotlin
when (val r = writeRows("orders.status", mapOf("id" to order.id)) {
    jdbc(db).execute(
        "update orders set status = 'DONE' where id = :id and status = 'PROCESSING'",
        "id" to order.id,
    )
}) {
    is WriteResult.Applied       -> processed.row(order.id, "done")
    is WriteResult.Rejected      -> rejected.row(order.id, r.reason)   // "no rows matched"
    is WriteResult.DryRunSkipped -> Unit
}
```

`writeRows` берёт исход из числа изменённых строк: `> 0` → `Applied`, `0` →
`Rejected("no rows matched")`. Использовать его можно **только там, где число строк действительно
есть** — `UPDATE ... WHERE`, `DELETE ... WHERE`. Для Cassandra-INSERT, delete+insert и вызовов
чужих компонентов числа строк не существует: там нужен `write { }` с явным `WriteOutcome`.

### Прямой вызов операций мимо `write`

`jdbc(db).execute(...)`, `cassandra(session).execute(...)`, `http(call).post(...)`,
`topic(...).send(...)` и так проходят dry-run-гейт своими метками (`jdbc.execute`, `http.post`,
...). Заворачивать их в `write { }` нужно, когда нужна доменная метка и учёт applied/rejected на
уровне бизнес-операции, а не SQL-стейтмента. Двойного гейта при этом не возникает: под dry-run
тело `write { }` не вызывается вовсе.

---

## 10. `publish` — асинхронная отправка и барьер

```kotlin
fun publish(name: String, args: Map<String, Any?> = emptyMap(), send: () -> CompletionStage<*>)
```

`publish` регистрирует эффект в барьере scope'а и запускает `send`. Стадия (в `scoped` —
родитель) не завершится, пока не придут подтверждения по всем зарегистрированным эффектам.

```kotlin
source(
    name = "resend",
    completionTimeout = Duration.ofMinutes(5),
    items = { orders.findStuck().asSequence() },
) { order ->
    publish("orders.resync", mapOf("id" to order.id)) {
        topic(producer, "orders.resync").sendAsync(order.id.toString(), encode(order))
    }
}
```

Что здесь важно:

- **Метод ничего не возвращает.** Под dry-run `send` не вызывается вообще, и фальшивый future не
  создаётся — возвращать было бы нечего. В отчёт идёт `dryRunSkipped[name]`.
- **Счётчик растёт до вызова `send`**, а не после: иначе между отправкой и регистрацией было бы
  окно, в котором запись уже в буфере продюсера, а барьер видит ноль и проходит.
- **Синхронный бросок из `send`** (ошибка сериализации, переполнение буфера) — обычный сбой
  элемента: идёт через `ItemError`.
- **Отказ доставки через `ItemError` НЕ проходит.** К моменту, когда известен исход, элемент давно
  посчитан, и `Skip` для него физически неприменим. Любой такой отказ валит стадию на барьере:
  `ScopeEffectsFailed` (первый отказ — причина, остальные в `addSuppressed`), каждый отказ поштучно
  уезжает в `errors.csv`, в отчёте растёт `failedEffects`.
- **`completionTimeout`** ограничивает только ожидание подтверждений после исчерпания источника.
  Не дождались — `ScopeCompletionTimeout`; уже отправленное не отзывается и учитывается как
  `abandonedPublishes`.
- **Поздняя регистрация** (эффект, зарегистрированный после того, как барьер закрылся) считается
  отдельно — `lateRegistered`. Оба этих счётчика поднимают код возврата до `1`
  ([§27](#27-exit-коды)).

### Когда `publish`, а когда `write`

| Ситуация | Что брать |
|---|---|
| Синхронный вызов, исход известен сразу (`repo.update`, `producer.send().get()`) | `write` / `writeRows` |
| Асинхронная отправка, исход приходит колбэком (`sendAsync`, future-паблишер) | `publish` |

Async-отправка **мимо** `publish { }` — самая дорогая ошибка учёта: элемент засчитывается успешным
в момент отправки, барьер про эффект не знает, и потерянное сообщение не поднимет код возврата.
Пишешь `kafka(p).publishAsync(...)` — заворачивай в `publish { }`.

---

## 11. `ItemError` — политика ошибок элемента

```kotlin
sealed interface ItemError<in T> {
    enum class Decision { Skip, Fail }
    data object Fail : ItemError<Any?>                                   // умолчание
    data object Skip : ItemError<Any?>
    class Handle<T>(val decide: (Throwable, T) -> Decision) : ItemError<T>
}
```

Политика применяется к исключению из **обработчика элемента** и к сбойной строке `readCsv`
(параметр `onRowError`, см. [§15](#15-чтение-csv--readcsv)).

### `ItemError.Fail` (дефолт)

Прервать стадию на первой же ошибке. Дальше решает `onUnhandled`
([§25](#25-scriptpolicy--что-делать-при-неожиданном-падении)) — по умолчанию exit-code 1. Уместно
для критичных миграций, где «частично сделать» хуже, чем «не сделать ничего».

### `ItemError.Skip`

Аудитнуть элемент в `errors.csv`, инкрементить `report.skipped`, продолжить.

```kotlin
source(onItemError = ItemError.Skip, items = { ... }) { ... }
```

### `ItemError.Handle` — классификатор

Получает **типизированный** элемент, а не `Any?`:

```kotlin
source(
    onItemError = ItemError.Handle { e, order: Order ->
        when {
            e is HttpStatusException && e.status == 409 -> ItemError.Decision.Skip
            e is DataIntegrityException                 -> ItemError.Decision.Fail
            else                                        -> ItemError.Decision.Skip
        }
    },
    items = { orders.asSequence() },
) { order -> ... }
```

Решение принимается **один раз** по типу ошибки — без backoff'а и без retry. `Decision.Skip` даёт
тот же контракт, что `ItemError.Skip`; `Decision.Fail` — что `ItemError.Fail`.

Тип элемента в лямбде стоит подписывать явно (`order: Order`): `onItemError` объявлен раньше
`items`, и вывод типов на него не всегда дотягивается.

### `errorThreshold` — предохранитель на массовые SKIP-ы

Счётчик skip'ов считается **по стадии** (в `scoped` — по родителю) и сбрасывается на её границе.
Превышение бросает `ErrorThresholdExceeded` и обрывает прогон. Значение по умолчанию —
`migration.defaults.errorThreshold` (`0` = выключено), аргумент стадии сильнее конфига.

Модель ровно одна: порог принадлежит фазе и проверяется в реальном времени. Второй,
post-mortem, проверки нет намеренно — она сравнивала суммарный `report.skipped` всего прогона с
глобальным дефолтом и тем самым отменяла собственный `errorThreshold` стадии: одно имя означало
две разные области действия.

Строки, отброшенные при чтении источника (`readCsv(onRowError = Skip)`), в этот счётчик **не
входят** — они считаются отдельно, как `sourceSkipped` ([§15](#15-чтение-csv--readcsv)).

### Что попадает в `errors.csv`

Каждый элемент, ушедший в Skip-ветку, каждая отброшенная строка источника и каждый отказ
асинхронного эффекта. Колонки: `timestamp, migration, author, itemRepr, errorClass, errorMessage` —
подробнее [§23](#23-errorscsv-и-errorsincludeitemt).

### Retry — на другом уровне

Item-level retry в DSL **нет**. Правильные места:

1. **Kora `@Retry`** на типизированном `@HttpClient` / `@KafkaPublisher` / repository-методе —
   ретраит только failing remote-вызов, не всю стадию. Умеет classify + backoff из коробки.

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

   Backoff в Kora resilient **линейный** (`delay + (n-1)*delayStep`), режима `EXPONENTIAL` у
   `@Retry` нет. Аннотация принимает **только имя конфига** — никаких `attempts = N` в самой
   аннотации. Чтобы она заработала: `ResilientModule` в `@KoraApp` и зависимость
   `ru.tinkoff.kora:resilient-kora`.

2. **Локальный `try/catch`** вокруг конкретного примитива внутри обработчика, когда нужна узкая
   логика и `@Retry` не подходит.

---

## 12. `input` и `validate` — конфиг прогона

### `input` — значение, общее на прогон

```kotlin
fun <I> input(name: String, load: InputScope.() -> I): Input<I>
```

`Input<I>` — ленивая ссылка. Значение вычисляется при **первом** `resolve` и кэшируется на весь
прогон, поэтому две стадии, читающие один и тот же справочник, не сходят за ним дважды.

```kotlin
override fun plan() = migration(name = name, author = "team") {
    val shards = input("shards") {
        jdbc(db).query("select id from shards where active") { it.getLong("id") }
    }

    source(name = "warm", items = { resolve(shards).asSequence() }) { shard -> ... }

    scoped(
        name = "process",
        parents = { resolve(shards).asSequence() },     // тот же список, повторного запроса нет
        items = { shard -> itemsOf(shard) },
    ) { item -> ... }
}
```

- `resolve` доступен **только** в `items` / `parents` (`SourceScope`).
- Идентичность — сам объект `Input`, а не имя: план неизменяем, и повторное исполнение того же
  плана не получит кэш прошлого прогона.
- Имена `input`'ов уникальны в пределах плана.
- Загрузка идёт в `InputScope`, то есть операции (`jdbc`, `readCsv`, ...) доступны.

### `validate` — проверка до первого эффекта

```kotlin
validate {
    require(config.pageSize() > 0) { "pageSize must be > 0, got ${config.pageSize()}" }
    require(config.from() < config.to()) { "empty period: ${config.from()}..${config.to()}" }

    errors.includeItem<Order> { "order=${it.id}, customer=${it.customerId}" }
    log.info("режим прогона: ${if (dryRun) "DRY-RUN" else "REAL"}")
}
```

- выполняется **один раз, до первой стадии и до любого эффекта**; исключение из неё завершает
  прогон, не тронув данные;
- объявляется не более одного раза;
- это отдельный узел, а не «первый input»: порядок разрешения input'ов задаёт первый `resolve`, и
  стадия успела бы выполнить эффекты с непроверенным конфигом;
- `resolve` здесь **недоступен** — проверять загруженные данные тут нельзя, только конфиг и
  окружение;
- удобное место для `errors.includeItem<T> { }` — регистрация гарантированно раньше первой ошибки.

---

## 13. `output` — CSV-выход прогона

```kotlin
fun output(filename: String, vararg headers: String): OutputHandle
```

Объявляется в билдере, открывается один раз за прогон в
[`outputFolder`](#22-outputfolder-и-артефакты-прогона), закрывается движком.

```kotlin
override fun plan() = migration(name = name, author = "team") {
    val processed = output("processed.csv", "id", "status")
    val mismatch  = output("mismatch/by-shard.csv", "id", "primary", "replica")   // подкаталоги создадутся

    source(items = { ... }) { item ->
        if (item.ok) processed.row(item.id, "ok") else mismatch.row(item.id, item.a, item.b)
    }
}
```

`OutputHandle` умеет ровно одно: `row(vararg cells: Any?)`.

- **Header пишется сразу** при открытии, даже если `row(...)` ни разу не вызвали.
- **`row(...)` thread-safe** — можно звать из параллельных воркеров. На `parallel > 32` запись в
  один файл становится точкой сериализации: тогда пиши в несколько файлов или агрегируй в памяти.
- **Пишется и под dry-run** — выход не меняет целевую систему, это диагностический артефакт.
- **TRUNCATE на ререн** — при повторном запуске файл перезаписывается.
- **Вне прогона `row` бросает `IllegalStateException`.** Привязка к файлу живёт только во время
  прогона и снимается в `finally`, поэтому «сохранённый» хендл не напишет в чужой файл.
- Имена файлов в пределах плана уникальны.

Запятые, кавычки и переводы строк в значениях (и в заголовках) квотируются по RFC 4180.

### Почему не `openCsv` внутри стадии

`openCsv(...)` никуда не делся — это extension на `RunScope`, и он нужен для файлов вне
`outputFolder` (`openCsv(Path.of("/shared/export.csv"), "id", "name")`). Но для выходов прогона
объявляй `output`: в `scoped`-стадии `openCsv` внутри `items` открывался бы заново на каждого
родителя и затирал строки предыдущего.

---

## 14. `Progress` — прогресс-логирование

```kotlin
source(progress = Progress.Default, items = { ... }) { ... }    // дефолт: каждые ~1000
source(progress = Progress.Off, items = { ... }) { ... }        // тишина
source(progress = Progress.Every(100), items = { ... }) { ... } // дефолтный формат, каждые 100

source(
    progress = Progress.Custom(500) { done, _ -> "processed=$done vip=${vips.get()}" },
    items = { ... },
) { ... }
```

Дефолтный формат: `progress: 5000  elapsed=1m 12s  rate=69/s`.

- **`total` всегда `null`.** Источник стадии — `Sequence`, движок не знает заранее, сколько в нём
  элементов, поэтому в `Progress.Custom` второй аргумент рендерить нечем.
- Тикает на **успехе и на Skip-аудите**. На терминальном `Fail` тика не происходит — исключение
  пробивает наверх раньше счётчика.
- Единица — элемент стадии. Если источник `chunked`, то батч.
- В `scoped` тикер общий на стадию: счётчик не обнуляется между родителями.
- Период `Progress.Default` берётся из `migration.defaults.progressEvery` (по умолчанию `1000`).
- Пишет в slf4j на уровне INFO — то же, что попадает в `migration.log`.

---

## 15. Чтение CSV — `readCsv`

```kotlin
// Из файловой системы (относительно CWD JVM)
readCsv("input/customer-ids.csv") { row -> row.getValue("customer_id") }

// Из classpath (для test-fixture)
readCsv("fixtures/seed.csv", classpath = true) { row -> row.getValue("id") }

// По абсолютному Path
readCsv(Path.of("/data/input.csv")) { it.getValue("id") }
```

Возвращает `Sequence<T>` — стрим, материализуется по мере итерации, подходит для миллионных
файлов. Mapper получает `Map<String, String>` (ключ — имя колонки из header'а). Отдавать эту
последовательность прямо из `items` — штатный сценарий:

```kotlin
source(
    parallel = 4,
    onItemError = ItemError.Skip,
    items = {
        errors.includeItem<Customer> { "id=${it.id}" }
        errors.includeItem<Map<String, String>> { "id=${it["id"]}" }

        readCsv("customers.csv", classpath = true, onRowError = ItemError.Skip) { row ->
            Customer(
                id = row.getValue("id").toLong(),
                email = row.getValue("email"),
                spend = row.getValue("spend").toLong(),
            )
        }
    },
) { customer -> ... }
```

Две регистрации сериализаторов не случайны: до обработчика строка может сломаться ещё на разборе —
тогда аудит получает сырую `Map`, а не `Customer`.

Если файла нет — `IOException` на первой итерации.

### `onRowError` — политика на битую строку

Обе перегрузки принимают `onRowError: ItemError<Map<String, String>>` (дефолт `ItemError.Fail`).
Политика применяется к **каждой строке отдельно** и покрывает обе беды: разбор самого CSV (рваные
кавычки, лишние колонки) и работу твоего mapper'а (`row.getValue("spend").toLong()` на пустой
ячейке).

- `ItemError.Fail` (дефолт) — первая же плохая строка валит прогон.
- `ItemError.Skip` / `Handle → Skip` — строка уезжает в `errors.csv`, инкрементит
  `report.sourceSkipped` (строка `⊘ Source rows dropped` в отчёте) и до обработчика стадии не
  доходит.

Отброшенная строка **не** считается item-skip'ом: до стадии она не дошла, элементом не была и в
`processed` не входит. Тождество `processed = successful + skipped + failed` благодаря этому
держится, а битые строки видны отдельной строкой отчёта ([§24](#24-итоговый-отчёт)). Из этого же
следует, что `errorThreshold` их не считает — порог про элементы стадии.

Классификатор `Handle` получает саму разобранную строку (`Map<String, String>`), поэтому по ней
можно отличить битую запись от записи с недопустимым значением.

В аудит по умолчанию уезжает вся строка целиком. Если в колонках есть чувствительные данные —
сократи представление через `errors.includeItem<Map<String, String>> { "id=" + it["id"] }`.

Открытый поток регистрируется в контексте, поэтому недопотреблённая `Sequence` (`take(n)`, ранний
выход, исключение выше по стеку) не оставляет открытый файловый дескриптор — движок закроет его в
`finally`.

---

## 16. Postgres / JDBC — `jdbc(db)` и `transactional`

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
val total: Long = jdbc(db).stream(
    "select id from orders where status = :s",
    "s" to "STUCK",
    fetchSize = 5000,
    mapper = { it.getLong("id") },
) { rows -> rows.count().toLong() }
```

> **Важно про `stream`.** API намеренно callback-style: `rows: Sequence<T>` валиден **только
> внутри** `consume`-блока. После возврата из `consume` `ResultSet` / `PreparedStatement` /
> `Connection` уже закрыты — итерация снаружи даст `SQLException("ResultSet is closed")`. Это
> сознательный выбор: физически невозможно leak'нуть JDBC-handle за пределы `db.inTx`-scope.
>
> Следствие для плана: **`stream` нельзя отдать из `items`** — источник потребляется после
> возврата из него. Внутри `consume` фолди в нужное значение (`count`, `toList`, агрегат), а для
> потоковых источников бери [`pages`](#8-pages--курсорная-пагинация): он ленив и не держит
> открытый курсор на всё время стадии.
>
> `sequence { while(rs.next()) yield(...) }` single-pass — повторная итерация даст 0 элементов.
> Compose (`.map` / `.filter`) — внутри `consume`.

Параметры — named placeholders `:name`. Биндинг через `setObject` — Kora/Postgres сам приводит
типы для большинства случаев. Для специфических — кастуй явно: `"since"::timestamp`. Парсер
корректно пропускает `:name` внутри литералов, комментариев, `::cast` и `$$dollar-quoted$$`.

> **`query` и `stream` принимают только читающие запросы.** Первое ключевое слово должно быть
> `select` / `with` / `show` / `explain` / `values` / `table` / `describe`, иначе —
> `IllegalArgumentException` ещё до похода в базу. Причина простая: ни `query`, ни `stream` не
> проходят dry-run gate (и не должны — чтение под репетицией обязано работать), поэтому
> `query("insert ... returning id")` выполнялся бы **в бою во время dry-run прогона**. Нужны
> строки от пишущего запроса — бери `executeReturning` (ниже). Проверка одинакова в обоих режимах:
> гейт, срабатывающий только под dry-run, дал бы зелёную репетицию при падающем бое.
>
> Известное ограничение: `WITH ... INSERT` (data-modifying CTE) начинается с `with` и проверку
> пройдёт — такие запросы тоже отправляй в `executeReturning`.

### Запись

```kotlin
// Одиночный UPDATE/INSERT/DELETE → число изменённых строк
jdbc(db).execute(
    "update orders set status = :s where id = :id",
    "s" to "DONE", "id" to 42L,
)

// Batch insert
jdbc(db).batch("insert into log(id, msg) values (?, ?)", entries) { ps, entry ->
    ps.setLong(1, entry.id); ps.setString(2, entry.msg)
}

// Пишущий запрос, возвращающий строки: INSERT/UPDATE ... RETURNING
val ids = jdbc(db).executeReturning(
    "insert into orders(customer_id) values (:c) returning id",
    "c" to customerId,
) { rs -> rs.getLong("id") }
```

`execute`, `batch` и `executeReturning` идут через `guardWrite` — под dry-run пропускаются, в
отчёте видны (метки `jdbc.execute`, `jdbc.batch`, `jdbc.executeReturning`). Под dry-run `execute`
возвращает `0`, `batch` — пустой `IntArray`, `executeReturning` — пустой список (mapper при этом
не вызывается: строк, которые ему можно отдать, не существует).

Чтобы исход был виден как доменная операция — заворачивай в `writeRows` / `write`
([§9](#9-write-и-writerows--контролируемая-запись)).

### Транзакция

```kotlin
source(name = "process", parallel = 8, items = { stuckOrders() }) { order ->
    transactional(jdbc(db)) {
        execute("update orders set status = 'PROCESSING' where id = :id", "id" to order.id)
        execute("insert into order_audit(order_id, event) values (:id, 'process_start')", "id" to order.id)
        // если любой execute упал — rollback всей пары
    }
}
```

Внутри блока `this: SqlOps` — `execute/query/batch/executeReturning` без префикса. Все они
используют один `Connection`, открытый через `db.inTx` Kora. Commit на успехе, rollback на
исключении.

**Транзакция открывается внутри обработчика, на один элемент.** Открыть её «на стадию» нельзя:
`java.sql.Connection` не потокобезопасен, а обработчики при `parallel > 1` живут в разных потоках.
Tx-bound `SqlOps`, использованный из чужого потока, бросает внятный `IllegalStateException`
(«tx-bound jdbc ops used from thread ... but the transaction belongs to ...») вместо тихой порчи
данных.

**Вложенный `transactional` бросает `IllegalStateException`** — нет smart-merge с outer-tx, автор
должен явно решить, что делать. Проверка работает **и под dry-run**: раньше она была отключена в
режиме репетиции, то есть dry-run проходил зелёным ровно там, где боевой прогон падал. Ловятся оба
случая: вложенный вызов на tx-bound `SqlOps` и вызов с внешним free-mode `ops` изнутри уже
открытой транзакции.

**Под dry-run `transactional` не пропускает блок целиком.** Тело выполняется: чтения работают, а
отдельные записи внутри него скипаются каждая своим `guardWrite`. Реальный `Connection` при этом
не открывается (не нужен BEGIN/COMMIT round-trip на каждый блок), в отчёт идёт метка
`jdbc.transactional`.

Если завернуть весь `transactional` в `write { }`, под dry-run не выполнится и тело блока — это
нормальный выбор, когда транзакция и есть та самая доменная запись:

```kotlin
write("orders.process", mapOf("id" to order.id)) {
    transactional(jdbc(db)) {
        execute("update orders set status = 'PROCESSING' where id = :id", "id" to order.id)
        execute("insert into order_audit(order_id, event) values (:id, 'process_start')", "id" to order.id)
    }
    WriteOutcome.Applied
}
```

---

## 17. Cassandra — `cassandra(session)`

```kotlin
val rows = cassandra(session).query(
    "select id, value from t.items where id in :ids",
    "ids" to listOf("A-1", "A-2", "A-3"),
) { row -> row.getString("id") to row.getString("value") }

// Write
cassandra(session).execute(
    "update t.items set value = :v where id = :id",
    "v" to "new", "id" to "A-1",
)
```

`stream` у `CassandraOps` намеренно **нет**. Раньше он отдавал `Sequence` поверх авто-пагинации
драйвера: выглядело удобно, но размер страницы задавался конфигом Kora, в отчёт не попадало
ничего, а курсор был невидим. Большую таблицу читай через `pages(...)` — явный курсор по
кластерному ключу, страницы считаются в `rawPages` / `rawRows`:

```kotlin
source(
    name = "scan",
    parallel = 4,
    items = {
        pages(
            first = { cassandra(session).query(FIRST, "limit" to pageSize) { it.getString("id") } },
            next = { after -> cassandra(session).query(NEXT, "after" to after, "limit" to pageSize) { it.getString("id") } },
            nextCursor = { raw -> raw.last() },
            continueWhen = { raw -> raw.size >= pageSize },
        )
    },
) { id -> ... }
```

Для небольшой ограниченной выборки хватит `cassandra(session).query(...)` — он материализует
результат в `List`.

Особенности биндинга: скаляры идут по runtime-классу значения, коллекции — через
`setList`/`setSet` с классом элемента, выведенным из первого не-null значения. Пустая или all-null
коллекция даёт `IllegalArgumentException` (тип элемента вывести нельзя). Map / UDT / tuple этим
путём не покрыты — для них пиши свою операцию ([§28](#28-шпаргалка-faq)).

`execute` и `batch` идут через dry-run-гейт (`cassandra.execute`, `cassandra.batch`). `batch` —
это цикл `session.execute(...)` по prepared statement: true batch'а в JDBC-смысле у Cassandra нет.

Учёт исхода: у Cassandra-INSERT нет числа изменённых строк, поэтому `writeRows` для него не
подходит — только `write { ...; WriteOutcome.Applied }`.

Multi-кластерная миграция — два разных `CqlSession` в графе через `@Tag`, потом
`cassandra(primary)` и `cassandra(replica)`.

---

## 18. Kafka — `topic`, `kafka` и типизированный publisher

В Kora канонический способ публиковать — **типизированный `@KafkaPublisher`**-интерфейс:
объявляешь контракт, процессор генерирует реализацию. DSL-хендлы `topic(...)` / `kafka(...)`
работают поверх «сырого» `org.apache.kafka.clients.producer.Producer`, который в графе надо завести
самому. Ниже — три способа, в порядке убывания частоты применения.

### Способ 1: `@KafkaPublisher.Topic` + `write { }` (рекомендуемый)

```kotlin
@KafkaPublisher("kafka.orders.publisher")
interface OrdersPublisher {

    @KafkaPublisher.Topic("kafka.orders.publisher.resyncTopic")
    fun publishResync(key: String, @Json value: OrderEvent)
}

@Component
class ResendOrders(
    private val orders: OrderRepository,
    private val publisher: OrdersPublisher,
) : MigrationDefinition {

    override val name = "ORDERS-RESYNC-001"

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

`@KafkaPublisher.Topic("...")` указывает на HOCON-путь, откуда берётся имя топика:

```hocon
kafka.orders.publisher {
  driverProperties { "bootstrap.servers" = ${KAFKA_BROKERS} }
  resyncTopic { topic = "orders.resync" }
}
```

Чтобы `@KafkaPublisher` сгенерировался, в `@KoraApp` нужен `KafkaModule` (артефакт
`ru.tinkoff.kora:kafka`) и KSP-процессор из [§1](#1-установка-и-подключение).

Обёртка `write { }` здесь не формальность: типизированный публишер — обычный компонент графа, DSL
про него ничего не знает и сам перехватить вызов не может. Без неё репетиция отправит сообщения
по-настоящему; единственный признак — предупреждение «intercepted 0 writes» в отчёте
([§21](#21-dry-run)).

Если метод публишера объявлен с future-возвратом (`CompletionStage<RecordMetadata>`), отдавай его
в `publish { }` вместо `write { }` — тогда подтверждение доставки попадёт на барьер стадии.

### Способ 2: raw `Producer<K, V>` + `topic(...)` handle

Когда хочется явный handle со счётчиками DSL и авто-`flush()` на закрытии. Сырой `Producer<K, V>`
Kora сама по себе не публикует, поэтому его объявляют компонентом графа:

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
class ResendOrders(
    private val orders: OrderRepository,
    private val producer: Producer<String, ByteArray>,
) : MigrationDefinition {

    override val name = "ORDERS-RESYNC-002"

    override fun plan() = migration(name = name, author = "team") {
        source(
            completionTimeout = Duration.ofMinutes(5),
            items = {
                // Продюсер создан нами — нам его и закрывать: DSL делает flush(), но не close().
                register(AutoCloseable { producer.close() })
                orders.findStuck().asSequence()
            },
        ) { order ->
            publish("orders.resync", mapOf("id" to order.id)) {
                topic(producer, "orders.resync").sendAsync(order.id.toString(), encode(order))
            }
        }
    }
}
```

…либо берут `producer()` у уже объявленного Kora-публишера — но типы там
`Producer<ByteArray, ByteArray>`, то есть сериализация ключа и значения остаётся на тебе.

Порядок регистрации не случаен: ресурсы закрываются в **обратном** порядке, поэтому
`register { producer.close() }` до первого `topic(...)` гарантирует, что продюсер закроется
последним — уже после `flush()` хендла.

- `KafkaTopic.send(key, value)` — sync publish (`producer.send(...).get()`); его место — внутри
  `write { }`.
- `KafkaTopic.sendAsync(key, value)` — возвращает `CompletableFuture`; его место — внутри
  `publish { }`.
- Хендл мемоизирован на пару (продюсер, имя), поэтому его можно звать прямо в теле обработчика —
  реестр ресурсов не разрастётся. `close()` делает `producer.flush()`, сам `Producer` не закрывает.
- Под dry-run обе отправки не выполняются (`kafka.publish` / `kafka.publishAsync` в breakdown'е),
  а `close()` не делает даже flush.

### Способ 3: `kafka(producer)` ad-hoc

Когда из одного скрипта пишешь в **разные** топики и handle на каждый — оверкилл:

```kotlin
write("orders.audit", mapOf("id" to id)) {
    kafka(producer).publish("orders.audit", id.toString(), auditEvent)
    WriteOutcome.Applied
}

publish("orders.resync", mapOf("id" to id)) {
    kafka(producer).publishAsync("orders.resync", id.toString(), payload)
}
```

`kafka(producer)` тоже мемоизирован (по продюсеру), так что вызов в теле обработчика ничего не
плодит.

> **Про учёт async-доставки.** Обработчик засчитывает элемент успешным в момент отправки, а брокер
> отвечает позже. Единственный способ связать исход доставки с прогоном — отдать future в
> `publish { }`: тогда отказ валит стадию на барьере, попадает в `errors.csv` и в
> `report.failedEffects`. `publishAsync`, вызванный **мимо** `publish { }`, барьеру не виден: при
> закрытии хендла в `warnings` отчёта уедет строка `kafka.publishAsync: N message(s) were rejected
> by the broker`, но код возврата останется нулём.

### Что выбирать

| Сценарий | Способ |
|---|---|
| Один скрипт, один топик, типизированный value (`@Json`) | **1** — `@KafkaPublisher.Topic` + `write` |
| Нужно подтверждение доставки на границе стадии или родителя | **2** — `topic(...).sendAsync` внутри `publish` |
| Несколько топиков ad-hoc, без оверкилла | **3** — `kafka(producer)` |

---

## 19. HTTP — типизированный клиент и `http(call)`

### Способ 1: типизированный Kora `@HttpClient` (рекомендуемый)

```kotlin
@HttpClient(configPath = "httpClient.enrichment")
interface EnrichmentService {
    @HttpRoute(method = "GET", path = "/customers/{id}/status")
    fun status(@Path("id") id: String): StatusResponse
}
```

Чтение — без обёрток, под dry-run выполняется как обычно:

```kotlin
val s = enrichment.status(customerId)
```

Запись — через `write { }`:

```kotlin
write("auth.refresh", args = mapOf("userId" to userId)) {
    auth.refreshToken(userId)
    WriteOutcome.Applied
}
```

Под dry-run тело не выполнится, в лог пойдёт `INFO [DRY-RUN] auth.refresh (userId=42)`, в отчёт —
`dryRunSkipped["auth.refresh"]` (агрегат по константной метке; если бы метка включала `$userId`, в
отчёте была бы запись на каждого юзера).

### Способ 2: функциональный `http(call)`

Если типизированного клиента нет (legacy / нет OpenAPI):

```kotlin
val client = HttpClient.newHttpClient()
val call: HttpCall = { method, path, body, headers ->
    val req = HttpRequest.newBuilder(URI.create("$baseUrl$path"))
        .method(method, HttpRequest.BodyPublishers.ofByteArray(body ?: ByteArray(0)))
        .build()
    client.send(req, HttpResponse.BodyHandlers.discarding()).statusCode()
}

// в обработчике:
http(call).post("/v1/users/42/refresh", body = payload)
http(call).get("/v1/users/42")
```

Write-методы (`post/patch/put/delete`) идут через dry-run gate автоматом (метки `http.post`,
`http.patch`, `http.put`, `http.delete`). Read (`get`) — нет, выполняется всегда.

### Код ответа проверяется всегда

Любой статус вне `200..299` поднимает `HttpStatusException(method, path, status)` — и у `get`, и у
всех write-методов. Без этого мёртвый бэкенд, отвечающий 500 на каждый запрос, дал бы отчёт
«100 000 successful» при нулевом эффекте бэкфилла: `HttpCall` возвращает просто `Int`.

Исключение штатно доходит до `ItemError` и до `errors.csv`, поэтому политику по коду ответа пишут
прямо в стадии:

```kotlin
source(
    onItemError = ItemError.Handle { e, _: User ->
        if (e is HttpStatusException && e.status == 409) ItemError.Decision.Skip
        else ItemError.Decision.Fail
    },
    items = { users.asSequence() },
) { user -> http(call).post("/v1/users/${user.id}/refresh") }
```

Под dry-run write-методы возвращают `200`, а не `0`: вызывающий код почти всегда смотрит на
статус, и ноль отправил бы репетицию в ветку ошибки — dry-run обязан идти тем же путём, что и бой.

---

## 20. Конфигурация HOCON

Секция `migration { ... }` целиком опциональна — у каждого ключа есть значение по умолчанию.
Без `migration.run` runner просто ничего не делает (idle).

```hocon
migration {
  # Имя миграции к запуску. null = runner idle (полезно когда сервис ещё и API хостит).
  run = ${?MIGRATION_RUN}

  # Dry-run: write/publish не выполняются, в отчёт идёт разбивка skipped writes.
  dryRun = false
  dryRun = ${?MIGRATION_DRY_RUN}

  # Папка для артефактов. null → logs/<имя миграции>.
  outputFolder = ${?MIGRATION_OUTPUT_FOLDER}

  defaults {
    onUnhandled    = FAIL_FAST            # FAIL_FAST | LOG_AND_COMPLETE
    errorThreshold = 0                    # >0 — прогон обрывается, если skipped превысил порог
    progressEvery  = 1000                 # период дефолтного Progress.Default
    parallel       = 1                    # значение аргумента parallel у source/scoped по умолчанию
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
| `run` | `String?` | `null` | имя миграции (совпадает с `MigrationDefinition.name`). `null` — runner idle |
| `dryRun` | `Boolean` | `false` | репетиция без записей ([§21](#21-dry-run)) |
| `outputFolder` | `String?` | `null` → `logs/<имя миграции>` | папка артефактов. Тип именно `String`, не `Path` |
| `defaults.onUnhandled` | `FAIL_FAST` / `LOG_AND_COMPLETE` | `FAIL_FAST` | политика на ошибку, вышедшую за пределы стадии; перебивается аргументом `migration(onUnhandled = ...)` |
| `defaults.errorThreshold` | `Long` | `0` (выключен) | порог SKIP-ов по стадии; аргумент стадии сильнее |
| `defaults.progressEvery` | `Int` | `1000` | период `Progress.Default` |
| `defaults.parallel` | `Int` | `1` | **значение аргумента `parallel` по умолчанию** |
| `errorReporting.includeStackTrace` | `Boolean` | `true` | писать ли `errors.log` |
| `errorReporting.maxItemReprLength` | `Int` | `500` | обрезка `itemRepr` в `errors.csv` |
| `report.asciiOnly` | `Boolean` | `false` | ASCII-рендер отчёта для CI без UTF-8 |

Значения проверяются на старте: `parallel <= 0`, `progressEvery <= 0`, `errorThreshold < 0`,
`maxItemReprLength <= 0` — это мисконфиг, runner пишет в лог конкретный ключ и завершается с кодом
`2`, не начиная прогон.

Env-overrides через `${?VAR}` — стандартный HOCON-синтаксис и **единственный** способ дотянуться
до окружения: `System.getenv` в библиотеке не вызывается нигде. Нет строки `ключ = ${?VAR}` —
переменная не действует.

### `defaults.parallel` — это дефолт аргумента, а не размер пула

Пул runner'а cached: он выдаёт столько потоков, сколько запросила конкретная стадия, а окно
in-flight держит её собственный семафор.

- `source(parallel = 8)` даёт восемь воркеров независимо от конфига;
- поднимать `defaults.parallel` нужно только если хочешь, чтобы **все** стадии без явного
  аргумента шли параллельно;
- `defaults.parallel = 1` (дефолт) — безопасная последовательная обработка.

### Конфиг библиотеки — интерфейс, а не data class

`MigrationConfig` объявлен как `@ConfigValueExtractor interface` с default-методами (доступ
методами: `config.run()`, `config.defaults().parallel()`), а экстрактор приезжает вместе с
`MigrationModule`. Пользователю это менять не нужно — важно только следствие: **никаких
дополнительных модулей ради конфига подключать не надо**, HOCON-ключи те же.

Для программной сборки конфига без HOCON (тесты, встраивание runner'а) есть data-классы
`MigrationConfigValues`, `DefaultsValues`, `ErrorReportingValues`, `ReportValues` — см.
[§26](#26-тестирование-скриптов).

### Кастомный конфиг скрипта

Свои настройки — отдельная секция, через `@ConfigSource`:

```hocon
sample {
  pageSize = 200
  parallel = 8
}
```

```kotlin
@ConfigSource("sample")
data class SampleConfig(
    var pageSize: Int,
    var parallel: Int,
)
```

`var` — Kora требует setter'ов. Поля без default'ов — обязательные, отсутствие в HOCON = ошибка на
старте.

Осторожно с дефолтами в Kotlin: KSP **не видит** значений по умолчанию у параметров конструктора
(`var pageSize: Int = 200` всё равно станет обязательным ключом). Если ключ должен быть
опциональным — объявляй конфиг интерфейсом с default-методами, как это сделано у самой библиотеки.

Проверять значения такого конфига удобно в `validate { }`
([§12](#12-input-и-validate--конфиг-прогона)): ошибка вылетит до первого эффекта.

---

## 21. Dry-run

```bash
MIGRATION_RUN=SAMPLE-001 MIGRATION_DRY_RUN=true ./gradlew run
```

Работает это только если в `application.conf` есть строка `dryRun = ${?MIGRATION_DRY_RUN}` —
иначе переменную никто не прочитает и прогон пойдёт боевым (см. [§1](#1-установка-и-подключение)).

Что происходит:

| Что | Под dry-run |
|---|---|
| `write { }` / `writeRows { }` | тело **не вызывается**, результат `WriteResult.DryRunSkipped`, счётчик `dryRunSkipped[name]` |
| `publish { }` | `send` **не вызывается**, эффект в барьере не регистрируется, счётчик `dryRunSkipped[name]` |
| `jdbc.execute` / `batch` / `executeReturning`, `cassandra.execute` / `batch`, `kafka.publish` / `publishAsync`, `topic.send` / `sendAsync`, `http.post/patch/put/delete` | пропускаются, в лог `INFO [DRY-RUN] <label> ...`. Нейтральные значения: `execute` → `0`, `executeReturning` → пустой список, publish → `null`, HTTP-write → `200` |
| `transactional { }` | блок **выполняется**, реальный `Connection` не открывается, пропускаются отдельные записи внутри него. Запрет вложенного `transactional` действует |
| `query` / `stream`, `readCsv`, `http.get`, типизированный `@HttpClient` GET | выполняются как обычно |
| `output(...)` | пишется — это диагностический артефакт |
| `input` / `validate` | выполняются как обычно |

В отчёте появится дополнительная строка:
```
  ⌀ Dry-run skipped writes:    (jdbc.execute: 1947000, kafka.publish: 973500, auth.refresh: 147)
```

Из неё видно: сколько именно UPDATE/INSERT улетело бы в БД, сколько в Kafka, и т.д. Готовая оценка
масштаба перед боевым прогоном.

**Что НЕ под dry-run-гейтом:** кастомный код без обёрток — вызов repository-метода,
типизированного `@KafkaPublisher` или `@HttpClient`, вручную созданный `KafkaProducer`. DSL про
эти вызовы ничего не знает: под репетицией они выполняются **по-настоящему**. Оборачивай в
`write("...") { ... }` / `publish("...") { ... }` или используй DSL-операции.

Забытая обёртка — самый дорогой тихий промах, поэтому у неё есть наблюдаемый признак: если под
dry-run обработан хоть один элемент, но не перехвачено ни одной записи, runner пишет WARN в лог и
в `warnings` отчёта:

```
DRY-RUN processed 8393 item(s) but intercepted 0 writes. If this migration writes anything,
those writes went through FOR REAL — wrap typed client calls in write("label") { ... }
```

Увидел эту строку в репетиции — считай, что прогон был боевым, и разбирайся до повтора.

---

## 22. `outputFolder` и артефакты прогона

```
logs/SAMPLE-001/
├── migration.log          # slf4j root logger — всё, что писалось в JVM за время прогона
├── processed.csv          # твои output(...) — каждый свой файл
├── mismatch.csv
├── errors.csv             # авто-аудит item-уровневых ошибок — ЛЕНИВО, с первой ошибки
├── errors.log             # стектрейсы по тем же ошибкам — тоже лениво
└── ...
```

`errors.csv` и `errors.log` создаются **не всегда**: аудитор открывает их в момент первой
записанной ошибки. Прогон без единого SKIP-а не оставляет пустых файлов, и строк `Error details:` /
`Error traces:` в отчёте тоже не будет.

Обратная сторона: если ошибки были в **прошлом** прогоне, а в текущем их нет — старые файлы никто
не перезапишет (перезапись происходит при первой ошибке), и отчёт покажет `Error details:` со
ссылкой на вчерашние данные. Хочешь чистую картину — чисти папку между прогонами или задавай
`MIGRATION_OUTPUT_FOLDER` с timestamp'ом.

Папка определяется так (в порядке приоритета):

1. `migration.outputFolder` в HOCON.
2. Env-override `MIGRATION_OUTPUT_FOLDER` (если строка с `${?...}` есть в конфиге).
3. Дефолт `logs/${migration.name}` относительно JVM CWD.

Создаётся (`mkdir -p`) на старте runner'ом; не удалось создать — exit 2, прогон не начинается.
Файлы `TRUNCATE`-аются при ререн: один прогон = один набор артефактов.

`outputFolder: Path` доступен во всех scope'ах — удобно для своих файлов:

```kotlin
source(items = { ... }) { item ->
    Files.writeString(outputFolder.resolve("custom-stuff.json"), json)
}
```

### `migration.log` — capture-all slf4j

Runner программно подключает logback `FileAppender` к **root**-логгеру. То есть в файле окажется
не только наш `log.info/...`, но и весь output Kora, Hikari, Kafka producer, Cassandra driver и
т.д. Это сознательное решение в пользу compliance: хочется иметь полный аудит того, что
происходило в JVM.

Если logback **не на classpath** (нестандартная конфигурация slf4j) — runner залогирует warn,
поднимет его в `warnings` отчёта и продолжит без файла. На прод стоит держать `logback-classic` в
`runtimeOnly` зависимостях.

---

## 23. `errors.csv` и `errors.includeItem<T>`

Когда элемент уходит в Skip-ветку ([§11](#11-itemerror--политика-ошибок-элемента)), когда
отбрасывается строка источника ([§15](#15-чтение-csv--readcsv)) или когда отказывает асинхронный
эффект ([§10](#10-publish--асинхронная-отправка-и-барьер)), запись попадает в `errors.csv`:

```csv
timestamp,migration,author,itemRepr,errorClass,errorMessage
2026-05-15T10:42:11.318Z,SAMPLE-001,team,"Order(id=42, customerId=cust-1)",NetworkException,timeout
```

`timestamp` — ISO-8601 в **UTC**. В итоговом отчёте времена печатаются в зоне JVM со смещением
(`2026-05-15 13:42:11 +03:00`) — именно затем, чтобы одно с другим сходилось без гадания.

`itemRepr` по умолчанию — `item.toString()`. Для data class это нормально, для строк-id тоже. Для
`Map`-объектов, больших структур и всего, где есть чувствительные поля, — не очень.

**Кастомный сериализатор** — `errors.includeItem<T> { it -> "репрезентация" }`:

```kotlin
validate {
    errors.includeItem<Order> { "order=${it.id}, customer=${it.customerId}" }
    errors.includeItem<Map<String, String>> { "id=${it["id"]}" }
}
```

- Регистрируй **до того, как пойдут ошибки**: в `validate { }` (лучше всего) или в начале `items`.
  Метод thread-safe, но регистрация во время уже идущей параллельной обработки инвалидирует кэш, и
  несколько первых записей могут уехать через `toString()`.
- Lookup идёт сначала точно по классу, потом по супертипам и интерфейсам — `includeItem<Map>`
  сработает и для `LinkedHashMap`.
- Длина обрезается до `errorReporting.maxItemReprLength` (дефолт 500) с `...`.

`errors.log` параллельно содержит стектрейсы (`errorReporting.includeStackTrace = true`, дефолт).

---

## 24. Итоговый отчёт

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
  ✓ Applied writes:          8 371  (customer.tier: 8371)
  ⊘ Rejected writes:         9  (customer.tier: 9)
  ✓ Acknowledged publishes:  8 380
  ⊘ Source rows dropped:     4
  ⌀ Source pages read:       17  (rows: 8400)
───────────────────────────────────────────────────────────
Error details:  logs/SAMPLE-001/errors.csv
Error traces:   logs/SAMPLE-001/errors.log
═══════════════════════════════════════════════════════════
```

Шапка:

- **Started / Finished** — со смещением зоны (`+03:00`). Зона — та, в которой живёт JVM; смещение
  печатается затем, чтобы метки сходились с `errors.csv`, который пишет UTC.
- **Duration** — человекочитаемо: `45s`, `7m 15s`, `2h 3m 11s`.
- **Mode** — `REAL` или `DRY-RUN`.

Счётчики. Первые четыре строки печатаются всегда, остальные — **только когда им есть что
показать**: постоянные нулевые строки приучили бы глаз их пропускать.

| Строка | Что | Откуда |
|---|---|---|
| `Processed` | элементов стадий (или батчей, если источник `chunked`) | движок |
| `✓ Successful` / `⊘ Skipped (errors)` / `✗ Failed` | исход элементов | `ItemError` ([§11](#11-itemerror--политика-ошибок-элемента)) |
| `✓ Applied writes` | сумма + разбивка по меткам | `write` / `writeRows` ([§9](#9-write-и-writerows--контролируемая-запись)) |
| `⊘ Rejected writes` | ожидаемые отрицательные исходы (`Rejected`) | там же |
| `✓ Acknowledged publishes` | подтверждённые на барьере эффекты | `publish` ([§10](#10-publish--асинхронная-отправка-и-барьер)) |
| `✗ Failed effects` | эффекты, отказавшие на барьере | там же |
| `⚠ Unconfirmed effects` | отправлено, но исход неизвестен: `(abandoned: N, late: M)` | таймаут барьера / поздняя регистрация |
| `✗ Unhandled failures` | отказы стадии или прогона, у которых элемента могло не быть | ошибка источника, `validate`, барьера |
| `Phases:` | по каждой фазе: processed / ok / skipped / failed, applied, acked, отказы | печатается только у многофазной миграции |
| `⊘ Source rows dropped` | строки, отброшенные при чтении источника (битый CSV) | `readCsv(onRowError = Skip)` ([§15](#15-чтение-csv--readcsv)) |
| `⌀ Source pages read` | сырые страницы и строки — «прочитано» до фильтров | `pages(...)` ([§8](#8-pages--курсорная-пагинация)) |
| `⌀ Dry-run skipped writes` | разбивка пропущенных под репетицией записей | dry-run ([§21](#21-dry-run)) |

`Processed = Successful + Skipped + Failed` — это тождество, на нём держится читаемость отчёта как
баланса. Поэтому `Source rows dropped` стоит отдельно: битая строка до стадии не дошла и элементом
не была. По той же причине отдельно стоят эффекты: их исход известен уже после того, как элемент
посчитан.

`Source pages read` против `Processed` — самый быстрый способ увидеть, сколько данных отсеял твой
`filter` в источнике.

Разбивки (`(customer.tier: 8371)`, dry-run-строка) отсортированы по метке, поэтому отчёты двух
прогонов сравнимы построчно. Пробелами разделяются только суммы (`8 393`); внутри разбивки числа
печатаются как есть.

Под dry-run добавляется строка:
```
  ⌀ Dry-run skipped writes:    (jdbc.execute: 16842, orders.resync: 8421)
```

Если при закрытии ресурсов что-то падало (или runner заметил что-то подозрительное) — появится
блок:
```
  ⚠ Warnings:
    - resource close failed: KafkaTopic(orders.resync) (TimeoutException: flush timed out)
```

Сюда же уезжают: предупреждение про dry-run без единой перехваченной записи ([§21](#21-dry-run)),
отказы `publishAsync` мимо `publish { }`, невставший за 30 секунд пул потоков (с числом брошенных
задач), ошибки закрытия `scopedResource` и отсутствие logback на classpath. Warnings нефатальны и
на exit-code не влияют.

**Error details / Error traces** печатаются только если файлы существуют, то есть если была хотя бы
одна ошибка ([§22](#22-outputfolder-и-артефакты-прогона)). Количества строк в отчёте нет — смотри
`Skipped` и `Source rows dropped`.

Всё то же самое доступно программно: `report.build()` возвращает `MigrationReport` со всеми полями
(`appliedWrites`, `rejectedWrites`, `acknowledgedPublishes`, `failedEffects`, `abandonedPublishes`,
`lateRegistered`, `sourceSkipped`, `rawPages`, `rawRows`, `dryRunSkipped`, `warnings`) — на этом
удобно писать ассерты в тестах ([§26](#26-тестирование-скриптов)).

В CI без UTF-8 терминала включай `migration.report.asciiOnly = true` — глифы заменяются на
`[OK]` / `[SK]` / `[FL]` / `[!]` / `[--]`, рамки на `=` и `-`.

---

## 25. `ScriptPolicy` — что делать при неожиданном падении

Если ошибка вышла за пределы стадии (провал стадии, `ScopeEffectsFailed`,
`ScopeCompletionTimeout`, `CursorNotAdvancing`, `ErrorThresholdExceeded`, исключение из `validate`
или из `items`), поведение runner'а определяет `onUnhandled`.

```kotlin
override fun plan() = migration(
    name = name,
    author = "you",
    onUnhandled = ScriptPolicy.LOG_AND_COMPLETE,   // не задан → migration.defaults.onUnhandled
) { ... }
```

Аргумент по умолчанию `null` — тогда политику берут из `migration.defaults.onUnhandled` (из
коробки `FAIL_FAST`). Заданный в коде аргумент сильнее конфига.

- **`FAIL_FAST`** (дефолт) — залогировать, инкрементить `report.unhandledFailures`, exit-code 1.
  Уместно когда «не пишем дальше».
- **`LOG_AND_COMPLETE`** — залогировать, инкрементить `report.unhandledFailures`, **дойти до
  конца** (закрыть ресурсы, напечатать отчёт), вернуть exit-code 0. Уместно для compliance-сценариев: хоть какой-то
  отчёт нужнее, чем абортированный прогон.

Чего `ScriptPolicy` **не** делает:

- не влияет на ошибки элементов — там действует `ItemError`
  ([§11](#11-itemerror--политика-ошибок-элемента));
- не позволяет продолжить следующие стадии: провал стадии всегда останавливает прогон, политика
  меняет только код возврата;
- не отменяет подъём кода до `1` из-за любого отказа эффекта — провалившегося, неподтверждённого
  или зарегистрированного после барьера ([§27](#27-exit-коды)): потерянные сообщения не имеют
  права закончиться нулём;
- не отменяет подъём кода до `1` при прерывании прогона.

---

## 26. Тестирование скриптов

DSL даёт фабрику контекста и публичный интерпретатор — граф Kora для теста поднимать не нужно.

```kotlin
import io.github.dsudomoin.migration.internal.RunContext
import io.github.dsudomoin.migration.internal.PlanInterpreter

class BackfillCustomerTierTest {

    @Test
    fun `проставляет тариф и пишет отчётный CSV`(@TempDir tmp: Path) {
        val ctx = RunContext.test(outputFolder = tmp)
        val migration = BackfillCustomerTier(repository)

        PlanInterpreter(ctx).execute(migration.plan())

        ctx.closeRegistered()   // закрыть ресурсы прогона — runner'а в тесте нет
        ctx.errors.close()      // flush errors.csv, если проверяем его содержимое

        val report = ctx.report.build()
        assertThat(report.successful).isEqualTo(8)
        assertThat(report.sourceSkipped).isEqualTo(2)   // две битые строки во входном CSV
        assertThat(report.skipped).isZero()             // до обработчика они не дошли
        assertThat(report.appliedWrites["customer.tier"]).isEqualTo(8)
        assertThat(tmp.resolve("customer-tier.csv").readLines()).hasSize(9)  // header + 8
    }
}
```

`RunContext.test(...)`:

- создаёт `CsvFileErrorReporter` + `ReportBuilder`;
- `executor` — `ForkJoinPool.commonPool()` (shared, daemon threads, без shutdown). При
  `parallel = 1` не задействуется вовсе; при `parallel > 1` обработка идёт в общем пуле и порядок
  не детерминирован. Нужен свой executor — собирай контекст через
  `RunContext.internalCreate(...)`;
- `outputFolder` — если не передал, создаст в системной tmp;
- принимает те же дефолты, что и конфиг: `dryRun`, `defaultParallel`, `defaultProgressEvery`,
  `errorThreshold` — так тестируют поведение под репетицией и под порогом ошибок.

Проверять план можно и **не исполняя** его: `plan()` сам по себе валидирует структуру (имена
стадий, дубли, `parallel > 0`) и не выполняет ни одной пользовательской лямбды.

```kotlin
@Test
fun `план объявляет две именованные стадии и ничего не выполняет`() {
    val plan = ResendOrders(orders, publisher).plan()

    assertThat(plan.stages.map { it.name }).containsExactly("warm-up", "resend")
    assertThat(plan.outputs.map { it.filename }).containsExactly("processed.csv")
    verifyNoInteractions(orders)
}
```

### Конфиг в тестах — без HOCON

Когда тест поднимает настоящий граф или конструирует `MigrationRunner` руками:

```kotlin
val config = MigrationConfigValues(
    run = "SAMPLE-001",
    dryRun = true,
    outputFolder = tmp.toString(),
    defaults = DefaultsValues(parallel = 4, errorThreshold = 10),
)

val codes = mutableListOf<Int>()
MigrationRunner(config, listOf(migration), null) { code -> codes += code }.init()
```

Есть и `ErrorReportingValues`, `ReportValues` — у всех дефолты совпадают с HOCON-дефолтами.

Чтобы прогон на настоящем графе не убил JVM тест-раннера `exitProcess`'ом, положи в граф компонент
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
`CassandraOpsIntegrationTest.kt`, `pilot/ComparisonPilotTest.kt`. Тем же тегом стоит помечать и
собственные контейнерные тесты.

### Модуль `:example` — рабочий образец

В репозитории есть модуль [`example/`](../example) — целое приложение-миграция, собранное ровно
так, как соберётся чужой сервис (KSP, `@KoraApp : HoconConfigModule, MigrationModule`,
`@Component`-миграция, `application.conf`). Это одновременно и шаблон для копирования, и проверка
того, что документированный wire-up действительно работает на настоящем графе Kora:

- `example/src/main/kotlin/.../ExampleApp.kt` — `@KoraApp`, `main()`, компонент-репозиторий;
- `example/src/main/kotlin/.../BackfillCustomerTier.kt` — миграция `CUSTOMER-TIER-001`
  (`output` + `readCsv(onRowError = Skip)` + `source(parallel = 4)` + `write { }`);
- `example/src/test/kotlin/.../KoraWireUpTest.kt` — тесты на сборку графа, реальный параллелизм,
  две параллельные стадии подряд, проброс `dryRun` и коды возврата.

Запуск:

```bash
MIGRATION_RUN=CUSTOMER-TIER-001 ./gradlew :example:run                        # боевой прогон
MIGRATION_RUN=CUSTOMER-TIER-001 MIGRATION_DRY_RUN=true ./gradlew :example:run # репетиция
```

---

## 27. Exit-коды

| Код | Что значит |
|---|---|
| `0` | Success. Включая `LOG_AND_COMPLETE`-сценарии (ошибка была, но runner довёл прогон до конца). Без `migration.run` runner вообще не трогает код возврата: процесс живёт дальше по своим правилам. |
| `1` | `FAIL_FAST` от ошибки, вышедшей за пределы стадии (провал стадии, `ScopeEffectsFailed`, `ScopeCompletionTimeout`, `CursorNotAdvancing`, `ErrorThresholdExceeded`, ошибка `validate`); **любой** отказ эффекта (`failedEffects + abandonedPublishes + lateRegistered > 0`); прерывание прогона. |
| `2` | Мисконфиг — прогон не начинался: неизвестное имя миграции; дубликат имён в графе; расхождение `MigrationDefinition.name` и имени плана; ошибка построения плана (`plan()` бросил); недопустимые значения конфига (`defaults.parallel <= 0`, `defaults.progressEvery <= 0`, `defaults.errorThreshold < 0`, `errorReporting.maxItemReprLength <= 0`); не удалось создать `outputFolder`. |

Отказ эффекта и прерывание поднимают код **независимо от `ScriptPolicy`**: `LOG_AND_COMPLETE`
покрывает допустимые отказы скрипта, но не нарушенную гарантию доставки и не наполовину сделанную
работу. Отдельной post-mortem проверки порога больше нет — порог принадлежит фазе и срабатывает в
реальном времени.

Прод-инфраструктура (CI/CD, K8s Job) должна читать exit-code и решать ретраить/алертить.

Если код возврата нужно перехватить вместо завершения процесса (тесты, встраивание runner'а в
живущее дальше приложение) — положи в граф компонент `MigrationExit`, см.
[§26](#26-тестирование-скриптов).

---

## 28. Шпаргалка-FAQ

**Q: Как пропустить миграцию через env, не правя HOCON?**
A: Просто не передавай `MIGRATION_RUN` — тогда ключа `migration.run` в конфиге нет, он `null`,
runner идёт в idle. На пустое значение (`MIGRATION_RUN= ./gradlew run`) рассчитывать нельзя:
`${?VAR}` пропускает подстановку только для **неопределённой** переменной, а определённая, но
пустая, подставится пустой строкой — и runner будет искать миграцию с пустым именем.

**Q: Поставил `MIGRATION_DRY_RUN=true`, а прогон всё равно писал в базу. Почему?**
A: Либо в `application.conf` нет строки `dryRun = ${?MIGRATION_DRY_RUN}` (библиотека не читает
окружение сама), либо запись шла мимо `write { }` / `publish { }` — прямой вызов репозитория или
типизированного клиента DSL перехватить не может. Смотри строку `Mode:` в отчёте и предупреждение
«intercepted 0 writes» ([§21](#21-dry-run)).

**Q: Почему в `items` нельзя вернуть `jdbc(db).stream(...)`?**
A: Потому что `Sequence` из `stream` валидна только внутри `consume`-блока: после возврата
`ResultSet` и `Connection` закрыты, а источник стадии потребляется уже после `items`. Для больших
чтений бери [`pages`](#8-pages--курсорная-пагинация); для маленьких — сфолди внутри `consume`.

**Q: Почему `query("insert ... returning id")` не работает?**
A: Так и задумано: `query`/`stream` принимают только читающие запросы, потому что они не проходят
dry-run gate — такой «read» выполнялся бы в бою во время репетиции. Пиши `executeReturning(...)`.
См. [§16](#16-postgres--jdbc--jdbcdb-и-transactional).

**Q: Как сделать батчи по 500?**
A: `items = { source.chunked(500) }`. Отдельного параметра у стадии нет — это обычная операция над
`Sequence`. Элементом стадии становится `List<T>`: `processed`, прогресс и `errorThreshold` считают
батчи.

**Q: Где открывать транзакцию?**
A: Внутри обработчика, на один элемент: `transactional(jdbc(db)) { ... }`. «На стадию» нельзя —
`java.sql.Connection` не потокобезопасен, и tx-bound `SqlOps` из чужого потока бросит
`IllegalStateException`. Вложенный `transactional` запрещён и в бою, и под dry-run.

**Q: Отчёт зелёный, но в Kafka сообщений меньше, чем элементов. Где смотреть?**
A: Скорее всего async-отправка шла мимо `publish { }`. Заверни future в `publish` — тогда стадия
дождётся подтверждений на барьере, отказ попадёт в `errors.csv` и в `failedEffects`, а прогон не
закончится нулём. См. [§10](#10-publish--асинхронная-отправка-и-барьер) и
[§18](#18-kafka--topic-kafka-и-типизированный-publisher).

**Q: Стадия висит и не завершается.**
A: Барьер ждёт подтверждения зарегистрированных `publish`. Поставь `completionTimeout` — тогда
вместо виса будет `ScopeCompletionTimeout` и строка `abandonedPublishes` в отчёте.

**Q: Прогон крутится вечно на `pages`.**
A: Не должен: если `nextCursor` вернул то же значение, бросается `CursorNotAdvancing`. Увидел
его — сделай курсор составным (например `data class Cut(val at: Instant, val id: UUID)`), потому
что по одному полю страница повторяется.

**Q: Нужно обработать «пачки» так, чтобы следующая не начиналась до подтверждения предыдущей.**
A: Это ровно `scoped`: родитель = пачка, барьер стоит на его границе
([§7](#7-scoped--стадии-с-барьером-на-родителя)).

**Q: Как разложить миграцию на несколько шагов?**
A: Объяви несколько `source` / `scoped` с именами — они идут строго последовательно, и провал
одной не запустит следующие. Общие данные между шагами — через `input`.

**Q: Retry совместим с `write { }`?**
A: Item-level retry в DSL нет ([§11](#11-itemerror--политика-ошибок-элемента)). Ретраить сам
publish / HTTP-call внутри `write` — через `@Retry` на типизированном Kora-методе, который из этого
блока вызывается. `write` остаётся dry-run-гейтом и в retry не вмешивается.

**Q: А что если внутри `transactional { }` сделать `publish { }`?**
A: Можно, **но осторожно**. Отправка выполняется inline, до коммита tx. Если tx откатится —
JDBC-изменения rollback'нутся, **а сообщение уже улетело**. Правильно либо не публиковать внутри
транзакции, либо писать в outbox-таблицу внутри tx, а публиковать отдельной стадией.

**Q: Как делать миграцию idempotent на повторе?**
A: На уровне SQL — `INSERT ... ON CONFLICT DO NOTHING`, `WHERE NOT EXISTS`,
`UPDATE ... WHERE status = 'OLD'` (тогда повтор даст `Rejected`, а не двойную запись). На уровне
DSL — пиши `processed.csv` через `output`, читай его при старте в `input` и фильтруй источник.

**Q: На большой выгрузке OOM — что делать?**
A: Не материализуй источник. `pages(...)` тянет страницами и не держит открытый курсор; `readCsv`
ленив. Для Cassandra это единственный ленивый путь: `cassandra(...).query(...)` собирает всё в
`List`. Плюс помни, что permit берётся до `next()`: источник читается ровно настолько, насколько
успевают воркеры.

**Q: Как два кластера Cassandra держать в одном скрипте?**
A: Два разных `CqlSession` в графе с `@Tag`, `@Tag(Primary::class) primary: CqlSession` в
конструкторе скрипта. Дальше — `cassandra(primary).query(...)`, `cassandra(replica).query(...)`.

**Q: Несколько Kafka producer (разные кластеры)?**
A: Два разных `Producer<...>` бина с `@Tag`, конструктор скрипта — два параметра, два
`topic(p1, "...")` и `topic(p2, "...")`. Хендлы мемоизируются по паре (продюсер, имя), звать их
можно и в теле обработчика.

**Q: Как кастомизировать ThreadFactory / пул?**
A: Опубликуй свой `Executor`-бин с `@Tag(MigrationExecutor::class)`. Runner подхватит и не будет
его гасить (это твой ресурс). Учти: реальный параллелизм ограничен возможностями твоего пула —
`newFixedThreadPool(2)` даст стадии два воркера, сколько бы она ни просила. Дефолтный (cached) пул
этой проблемы не имеет.

**Q: Хочу свой S3-клиент / Redis / etc — как добавить в DSL?**
A: Пиши extension на `RunScope` и проводи записи через `guardWrite` — тогда твоя операция будет
уважать dry-run и попадёт в отчёт:

```kotlin
class S3Ops(
    private val ctx: RunScope,
    private val client: S3Client,
    private val bucket: String,
) : AutoCloseable {

    fun put(key: String, body: ByteArray) =
        ctx.guardWrite("s3.put", mapOf("bucket" to bucket, "key" to key)) {
            client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), fromBytes(body))
        }

    override fun close() { /* если есть что закрывать */ }
}

fun RunScope.s3(client: S3Client, bucket: String): S3Ops =
    shared(client to bucket) { S3Ops(this, client, bucket) }
```

`shared` мемоизирует хендл на прогон и регистрирует его на закрытие — поэтому вызывать
`s3(client, bucket)` можно прямо в теле обработчика.

**Q: Можно ли запускать миграцию в Docker с WORKDIR=/app — куда упадут логи?**
A: Дефолт `logs/TASK-X` → `/app/logs/TASK-X`. Меняется через
`MIGRATION_OUTPUT_FOLDER=/var/log/migrations/$RUN_ID`.

**Q: Тесты подтягивают `logback-classic`. А в проде нужно?**
A: Да. Иначе runner залогирует warn `migration.log file output disabled: Logback not on classpath`
(он же уедет в `warnings` отчёта), и `migration.log` не появится. Держи
`runtimeOnly("ch.qos.logback:logback-classic:...")`.

**Q: Хочу свой формат отчёта об ошибках (JSON / Kibana) вместо CSV — как?**
A: Сложнее. `CsvFileErrorReporter` захардкожен в `MigrationRunner`. Обходной путь — собрать
контекст самому через `RunContext.internalCreate(...)` (он публичный) и исполнить
план `PlanInterpreter`'ом. Нужна штатная точка расширения — заводи issue.

## 29. Готовые архетипы

Гайд объясняет узлы по отдельности; архетипы показывают их вместе, на законченной задаче.
Все примеры в них проверены компилятором против `:core` и `:kora`.

| Архетип | Когда | Что показывает |
|---|---|---|
| [Export](examples/export-archetype.md) | DB → CSV snapshot | `pages` как источник, `output`, `sourceSkipped` |
| [Correction](examples/correction-archetype.md) | CSV → UPDATE + аудит | `readCsv`, `.chunked`, `writeRows`, `Rejected` |
| [Comparison](examples/comparison-archetype.md) | два источника → diff | два `output`, батчинг, гранулярность учёта |
| [Resend](examples/resend-archetype.md) | DB → обогащение → Kafka | `scoped` + `pages` + `publish`, барьер на родителя |
| [HTTP backfill](examples/http-backfill-archetype.md) | DB → внешний POST | `ItemError.Handle` по коду ответа, `write` |
| [Full showcase](examples/full-showcase.md) | всё сразу | двухстадийный план, транзакции, publish за пределами транзакции |
| [Customization](examples/customization.md) | точки расширения | свой op на `RunScope`, `scopedResource`, `MigrationExit`, свой `Executor` |

