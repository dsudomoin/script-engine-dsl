# Migration DSL — Agent Guide

Справочник для кодового агента, пишущего миграции на этой библиотеке или дорабатывающего её саму.
Всё описанное здесь проверено против исходников; если код и этот файл разошлись — прав код,
и файл нужно поправить.

Читать целиком не обязательно: §5 и §7 хватает, чтобы написать миграцию. §11–§13 — то, на чём
обычно ошибаются.

## Contents

1. [Что это за библиотека](#1-что-это-за-библиотека)
2. [Когда подходит, когда нет](#2-когда-подходит-когда-нет)
3. [Структура репозитория](#3-структура-репозитория)
4. [Подключение](#4-подключение)
5. [Первая миграция](#5-первая-миграция)
6. [Модель исполнения](#6-модель-исполнения)
7. [API — `core`](#7-api--core)
8. [API — `kora`](#8-api--kora)
9. [Конфиг (HOCON) и коды возврата](#9-конфиг-hocon-и-коды-возврата)
10. [Dry-run](#10-dry-run)
11. [Тестирование](#11-тестирование)
12. [Архетипы](#12-архетипы)
13. [Антипаттерны и грабли](#13-антипаттерны-и-грабли)
14. [Карта файлов](#14-карта-файлов)
15. [Чеклист перед первой миграцией в чужом проекте](#15-чеклист-перед-первой-миграцией-в-чужом-проекте)

---

## 1. Что это за библиотека

Kotlin-библиотека для одноразовых миграционных скриптов на Kora. Сервис поднимается разово,
выполняет одну названную миграцию и завершается с кодом возврата.

Миграция — обычный класс с обычным императивным телом. Данные она берёт через **компоненты
графа этого же сервиса** (репозитории, HTTP-клиенты, паблишеры) и через CSV-файлы. Своего
доступа к данным библиотека не содержит вовсе: ни SQL, ни Kafka, ни HTTP внутри неё нет.

Библиотека даёт ровно шесть вещей:

| Что | Где |
|---|---|
| Цикл со счётчиками, политикой ошибок, порогом и прогрессом | `MigrationScope.each` |
| Автоматический аудит сбойных элементов в `errors.csv` / `errors.log` | `MigrationScope.errors` |
| Чтение CSV: разделители, кодировки, BOM, сборка строки в DTO, отказ до старта на битом файле | `readCsv`, `readCsvAs` |
| Запись CSV с закрытием в конце прогона | `MigrationScope.csv` |
| Папка артефактов с `migration.log` | `MigrationScope.outputFolder` |
| Итоговый отчёт и код возврата | `MigrationRunner` |

## 2. Когда подходит, когда нет

**Подходит:** bulk-операции (пересверка статусов, бэкфилл через API, выгрузка в CSV) в сервисе
на Kora; сервис-накопитель, куда со временем добавляются десятки миграций, а `migration.run`
выбирает одну.

**Не подходит:** always-on задачи; streaming-ETL; coroutines-first код — цикл блокирующий,
параллелизм на потоках JVM.

## 3. Структура репозитория

```
core/       migration-dsl-core — чистый Kotlin, Kora не нужна
kora/       migration-dsl-kora — мост в Kora: модуль, runner, конфиг
example/    работающее приложение-миграция; им же проверяется wire-up на живом графе
docs/       USER_GUIDE.md и examples/*
```

`core` можно подключать без `kora`, если запускать миграции самому через `MigrationExecution`.

## 4. Подключение

```kotlin
dependencies {
    implementation("io.github.dsudomoin.migration:migration-dsl-core:0.2.0")
    implementation("io.github.dsudomoin.migration:migration-dsl-kora:0.2.0")
    ksp("ru.tinkoff.kora:symbol-processors:1.2.20")
}
```

Кодогенерация Kora для Kotlin — **только KSP**. `kapt` с `ru.tinkoff.kora:annotation-processors`
не поддерживается.

```kotlin
@KoraApp
interface App : HoconConfigModule, JdbcDatabaseModule, MigrationModule
```

Подмешивать больше нечего: секцию `migration { ... }` `MigrationModule` читает сам,
а `MigrationRunner` помечен `@Root` и потому создаётся графом без внешних зависимостей.

```hocon
migration {
  run = ${?MIGRATION_RUN}
  dryRun = ${?MIGRATION_DRY_RUN}
}
```

Библиотека не читает окружение (в исходниках нет `System.getenv`) — переменные приезжают
только через `${?VAR}`.

## 5. Первая миграция

```kotlin
@Component
class BackfillCustomerTier(
    private val repo: CustomerRepository,
) : Migration("CUSTOMER-TIER-001") {

    override fun MigrationScope.run() {
        errors.includeItem<Customer> { "id=${it.id}" }
        val out = csv("customer-tier.csv", "id", "spend", "tier")

        each(
            readCsvAs<Customer>("customers.csv", classpath = true),
            parallel = 4,
            onItemError = ItemError.Skip,
        ) { customer ->
            val tier = tierFor(customer.spend)
            if (!dryRun) repo.updateTier(customer.id, tier)
            out.row(customer.id, customer.spend, tier)
        }
    }

    private fun tierFor(spend: Long) = when {
        spend >= 10_000 -> "PLATINUM"
        spend >= 1_000 -> "GOLD"
        else -> "SILVER"
    }
}

data class Customer(val id: Long, val email: String, val spend: Long)
```

Запуск: `MIGRATION_RUN=CUSTOMER-TIER-001 ./gradlew run`.

## 6. Модель исполнения

1. Kora поднимает граф целиком. `MigrationRunner.init()` вызывается как `Lifecycle`.
2. Runner читает `migration.run`, находит среди `All<Migration>` компонент с таким `name`,
   собирает `RunSettings`, создаёт папку артефактов и подключает к ней `migration.log`.
3. `MigrationExecution.execute` создаёт `MigrationRun` (единственная реализация
   `MigrationScope`) и зовёт тело миграции.
4. По выходу — закрываются ресурсы прогона (CSV-выходы, пул потоков), затем аудитор;
   строится отчёт; runner печатает его и завершает процесс кодом возврата.

Что из этого следует:

- **Граф поднимается весь.** Компоненты всех ста миграций будут созданы, даже если запускается
  одна. Тяжёлую инициализацию в конструкторе миграции делать нельзя.
- **Тело — обычный код.** Никакого плана, интерпретатора и отложенного исполнения. Стектрейс
  падения указывает на строку миграции.
- **Библиотека не видит ваших вызовов.** Из этого следует вся модель dry-run (§10).
- **`run()` объявлен extension-членом** (`fun MigrationScope.run()`). Внутри одновременно видны
  и зависимости класса, и хелперы движка, без префиксов. Длинное тело разбивается приватными
  extension-функциями на `MigrationScope` — обычный приватный метод хелперов не увидит:

```kotlin
override fun MigrationScope.run() {
    prepare()
    apply()
}

private fun MigrationScope.prepare() { log.info("...") }
private fun MigrationScope.apply() { each(repo.all()) { } }
```

## 7. API — `core`

### 7.1 `Migration`

```kotlin
abstract class Migration(val name: String) {
    abstract fun MigrationScope.run()
}
```

`name` — то, что сравнивается с `migration.run`. Дублирующиеся имена в графе валят запуск
кодом `2` ещё до выбора миграции.

### 7.2 `MigrationScope`

```kotlin
interface MigrationScope {
    val dryRun: Boolean          // режим репетиции; это просто флаг, см. §10
    val log: Logger              // SLF4J этой миграции; уходит и в migration.log
    val errors: ErrorReporter    // авто-аудитор, см. 7.6
    val outputFolder: Path       // папка артефактов прогона
    fun <T> each(...): EachResult
}
```

Скоуп один на всё тело: и в `run()`, и внутри лямбды `each`, и в приватных extension-шагах
доступно одно и то же. Помечен `@DslMarker`-аннотацией `@MigrationDsl`.

### 7.3 `each`

```kotlin
fun <T> each(
    items: Sequence<T>,                       // либо Iterable<T>
    parallel: Int = 1,
    onItemError: ItemError<T> = ItemError.Fail,
    progress: Progress = Progress.Default,
    errorThreshold: Long? = null,
    handle: (T) -> Unit,
): EachResult

data class EachResult(val processed: Long, val successful: Long, val skipped: Long, val failed: Long)
```

- `parallel = 1` (дефолт) — обычный `for` в текущем потоке: ни пула, ни семафора, чистый
  стектрейс. Это подавляющий случай.
- `parallel > 1` — пул создаётся лениво при первом таком вызове и закрывается в конце прогона
  (ожидание 30 с, недоигравшие задачи попадают в предупреждения отчёта). `handle` обязан быть
  потокобезопасным; ленивый источник читается ровно настолько, насколько его успевают
  потреблять.
- `errorThreshold` — `null` берёт дефолт прогона (`migration.defaults.errorThreshold`), `0`
  выключает. Порог считает **skip'ы этого вызова**: при `skipped > threshold` летит
  `ErrorThresholdExceeded` и прогон падает (код `1`).
- Возвращённый `EachResult` — счётчики этого вызова; в отчёте они суммируются по всем вызовам.

Держится тождество `processed = successful + skipped + failed`.

### 7.4 `ItemError`

```kotlin
sealed interface ItemError<in T> {
    enum class Decision { Skip, Fail }
    data object Fail : ItemError<Any?>                                   // дефолт
    data object Skip : ItemError<Any?>
    class Handle<T>(val decide: (Throwable, T) -> Decision) : ItemError<T>
}
```

- `Fail` — ошибка элемента валит прогон. Элемент попадает в `errors.csv` перед этим.
- `Skip` — ошибка аудитируется, цикл продолжается, счётчик `skipped`.
- `Handle` — решает ваш классификатор, видящий типизированный элемент:

```kotlin
onItemError = ItemError.Handle { e, customer ->
    if (e is HttpClientException && e.code() == 404) ItemError.Decision.Skip
    else ItemError.Decision.Fail
}
```

Если сам классификатор бросил — его исключение становится основным, исходное уезжает
в `suppressed`, элемент считается `failed`.

### 7.5 `Progress`

```kotlin
Progress.Default              // каждые migration.defaults.progressEvery (из коробки 1000)
Progress.Off
Progress.Every(n)
Progress.Custom(n) { done, total -> "..." }
```

Формат зависит от того, известен ли размер источника. У коллекции известен —
`progress: 300/1200 (25%)  elapsed=12s  rate=25/s`, и на коротком цикле шаг укорачивается сам.
У `Sequence` неизвестен — `progress: 300  elapsed=12s  rate=25/s`, и `total` в `Custom` равен
`null`.

Тик происходит на успехе и на Skip-аудите. На терминальном `Fail` тика нет: исключение
пробивает наверх раньше счётчика.

### 7.6 `errors` — аудит

```kotlin
interface ErrorReporter : AutoCloseable {
    fun report(e: Throwable, item: Any?)
    fun registerSerializer(cls: Class<*>, f: (Any) -> String)
}

inline fun <reified T : Any> ErrorReporter.includeItem(noinline f: (T) -> String)
```

Движок сам пишет каждую ошибку элемента в `errors.csv`
(`timestamp,migration,itemRepr,errorClass,errorMessage`) и стектрейс в `errors.log`. Файлы
создаются лениво — у чистого прогона их не будет.

`itemRepr` по умолчанию — `toString()` элемента, обрезанный до `maxItemReprLength` (500).
**Если в элементе есть персональные данные, зарегистрируйте сериализатор** первой же строкой
`run()`, до первого цикла:

```kotlin
errors.includeItem<Customer> { "id=${it.id}" }
errors.includeItem<CsvRow> { "id=${it["id"]}" }   // строка могла сломаться ещё на разборе
```

Поиск сериализатора идёт по классу, затем по супертипам и интерфейсам.

### 7.7 CSV — чтение

```kotlin
fun <T> MigrationScope.readCsv(
    path: String,
    classpath: Boolean = false,
    delimiter: Char = ',',
    quote: Char = '"',
    charset: Charset = Charsets.UTF_8,
    onRowError: ItemError<CsvRow> = ItemError.Fail,
    map: (CsvRow) -> T,
): Sequence<T>

inline fun <reified T : Any> MigrationScope.readCsvAs(
    path: String,
    classpath: Boolean = false,
    delimiter: Char = ',',
    quote: Char = '"',
    charset: Charset = Charsets.UTF_8,
    onRowError: ItemError<CsvRow> = ItemError.Fail,
    streaming: Boolean = false,
): Sequence<T>
```

Обе ленивые: файл не поднимается в память целиком. Поток регистрируется в реестре прогона,
поэтому недопотреблённая последовательность (`take(n)`, ранний выход) не оставляет открытого
дескриптора.

**`CsvRow`** — одна строка:

```kotlin
row["spend_amount"]        // String; нет такой колонки → CsvStructureException со списком колонок
row.getOrNull("comment")   // String? — null, если колонки нет
row.lineNumber             // заголовок 1, первая запись 2
row.columns                // имена колонок как в файле
row.toString()             // "line 2: id=1, name=A" — то, что увидит человек в errors.csv
```

Имя колонки ищется без учёта регистра и способа записи: `spend_amount`, `SPEND_AMOUNT`,
`Spend Amount`, `spendAmount` — одно и то же. Две колонки, схлопнувшиеся в одно имя, отвергают
файл. Отсутствующая ячейка у короткой строки — пустая строка, а не ошибка.

**BOM** срезается всегда; BOM от UTF-16 заодно задаёт кодировку, перекрывая параметр `charset`.

**`readCsvAs<T>`** собирает строку в DTO (`data class`) через Jackson. Колонка ищется по имени
свойства той же нормализацией. Лишние колонки файла игнорируются. Правила для полей, которых
в строке нет или чья ячейка пуста:

| Поле | Пустая ячейка / нет колонки |
|---|---|
| со значением по умолчанию | умолчание |
| nullable | `null` |
| non-null `String` | пустая строка |
| остальные non-null | ошибка строки |

Последняя строка таблицы — не педантизм: сам Jackson на пустой ячейке и на отсутствующей
колонке отдаёт под `Long` ноль, под `Boolean` — `false`, и такой ноль молча уезжает в целевую
систему как настоящее значение.

**Предпроверка.** `readCsvAs` по умолчанию разбирает файл целиком прежде, чем отдать первый
объект, и при ошибках бросает `CsvValidationException` со списком всех битых строк, не тронув
целевую систему (код возврата `2`). Проверка пропускается сама, когда `onRowError` и так
разрешает битые строки, и выключается через `streaming = true`. Дважды разбирается только сам
файл — прикладной код не вызывается ни разу; поэтому у `readCsv` с пользовательской лямбдой
предпроверки нет и быть не может.

Строка, отброшенная при чтении, считается в `sourceSkipped`, а не в `skipped`: до цикла она
не дошла и никогда не была `processed`.

### 7.8 CSV — запись

```kotlin
fun MigrationScope.csv(
    filename: String,
    vararg headers: String,
    delimiter: Char = ',',
    charset: Charset = Charsets.UTF_8,
    bom: Boolean = false,
): CsvOutput

interface CsvOutput : AutoCloseable {
    fun row(vararg cells: Any?)   // toString(), null → пусто; квотирование по своему разделителю
    fun flush()
}
```

- Файл открывается в `outputFolder`, **закрывается движком** в конце прогона. Закрывать руками
  не нужно.
- Повторный вызов с тем же `filename` возвращает тот же handle: в императивном теле `csv(...)`
  легко оказывается внутри цикла, и переоткрытие затирало бы написанное.
- Имя обязано остаться внутри `outputFolder` (абсолютный путь и `..` отвергаются) и не совпадать
  с `errors.csv`, `errors.log`, `migration.log`.
- `row` потокобезопасен, но сериализует записи через общий lock. На `parallel > 32` это точка
  сериализации — пишите тогда в несколько файлов.
- `row` **не делает flush**: миллион записей — миллион syscall'ов. Всё уезжает на диск при
  `flush()` или на закрытии.
- Под dry-run файл всё равно пишется: выход — диагностический артефакт, а не изменение целевой
  системы.
- `bom = true` нужен, чтобы Excel не прочитал UTF-8 как ANSI. В кодировке, которая BOM не умеет,
  запрос отвергается до создания файла.

### 7.9 Отчёт

```kotlin
data class MigrationReport(
    val name: String, val startedAt: Instant, val finishedAt: Instant, val duration: Duration,
    val dryRun: Boolean,
    val processed: Long, val successful: Long, val skipped: Long, val failed: Long,
    val sourceSkipped: Long, val unhandledFailures: Long,
    val errorsFile: Path?, val tracesFile: Path?, val warnings: List<String>,
)
```

`processed = successful + skipped + failed`. В тождество **не входят**: `sourceSkipped`
(строки, отброшенные при чтении — до цикла они не дошли) и `unhandledFailures` (отказ уровня
прогона, у которого обрабатываемого элемента могло не быть).

`warnings` — нефатальное: ошибка закрытия ресурса, не вставший пул, сбой аудитора. На код
возврата не влияют.

## 8. API — `kora`

### 8.1 `MigrationModule`

`@Module`-интерфейс с двумя фабриками: конфиг из секции `migration` и сам `MigrationRunner`,
собирающий все миграции графа через `All<Migration>`.

`@Root` на фабрике runner'а **обязателен и не является украшением**: от runner'а не зависит
ни один компонент пользовательского графа, а Kora резолвит граф исключительно от корневого
набора. Без `@Root` компонент не создался бы вообще — приложение стартовало бы, `init()` не
вызвался бы, миграция молча не выполнилась бы, процесс завершился бы кодом `0`, и K8s Job
выглядел бы успешным.

### 8.2 `MigrationExit`

```kotlin
fun interface MigrationExit { fun exit(code: Int) }
```

По умолчанию runner зовёт `exitProcess(code)`. Компонент этого типа в графе перехватывает код
возврата — нужен тестам, поднимающим настоящий граф (иначе `exitProcess` убьёт JVM тест-раннера).

### 8.3 `MigrationConfig`

Объявлен интерфейсом с default-методами под `@ConfigValueExtractor`, а **не** `@ConfigSource`.
Это канонический для Kora способ описать конфиг библиотеки, приезжающий в чужой сервис
(так устроены `JdbcDatabaseConfig`/`JdbcDatabaseModule` в самой Kora). Причины проверены на
живом графе:

- `@ConfigSource` генерирует модуль для конфига *самого приложения*; у потребителя сборка графа
  падала с «Component was expected to be generated by extension but was not»;
- KSP не видит дефолтов параметров конструктора Kotlin-класса, поэтому `dryRun: Boolean = false`
  в `data class` превращался в обязательный ключ, и отсутствие `migration.dryRun` роняло старт.
  Дефолт в теле метода интерфейса генератор учитывает.

Для программной сборки (тесты, запуск без HOCON) есть `MigrationConfigValues`.

## 9. Конфиг (HOCON) и коды возврата

```hocon
migration {
  run = ${?MIGRATION_RUN}          # имя миграции; null = runner простаивает
  dryRun = ${?MIGRATION_DRY_RUN}   # default false
  outputFolder = "logs/my-run"     # default logs/<имя миграции>

  defaults {
    errorThreshold = 0             # skipped > N в одном each валит прогон; 0 = выключено
    progressEvery = 1000           # период Progress.Default
  }

  errorReporting {
    includeStackTrace = true       # false = только errors.csv, без errors.log
    maxItemReprLength = 500        # обрезка itemRepr в CSV
  }

  report {
    asciiOnly = false              # true заменит ═ ✓ ⊘ ⚠ на ASCII — для CI без UTF-8
  }
}
```

Коды возврата:

| Код | Что произошло | Состояние целевой системы |
|---|---|---|
| `0` | прогон завершён | изменения применены |
| `1` | исключение из тела, превышенный порог ошибок, прерывание | **может быть частичным** |
| `2` | забраковано до первого элемента: неизвестное или дублирующееся имя, битые значения конфига, недоступный `outputFolder`, отвергнутый входной файл | не тронуто |

Код `2` выдаётся, когда исключение помечено маркером `MigrationPrecondition` **и** счётчик
`processed` нулевой. Второе условие — не перестраховка: маркер обещает лишь то, что бросили
на предусловии, а бросить такое можно и после работы.

Собственное предусловие объявляется так:

```kotlin
class InputMissing(message: String) : RuntimeException(message), MigrationPrecondition
```

## 10. Dry-run

`dryRun` — **флаг на скоупе, а не перехват**. Библиотека не видит вызовов ваших компонентов
и ничего не блокирует:

```kotlin
if (!dryRun) repo.updateTier(customer.id, tier)
```

Забытая проверка под `dryRun = true` запишет в целевую систему по-настоящему — предупреждения
«обработано N элементов, но перехвачено 0 записей» здесь не будет, потому что перехватывать
нечего. Это сознательный размен: перехват требовал бы, чтобы весь доступ к данным шёл через
обёртки библиотеки, а он идёт через ваши компоненты.

Что под dry-run происходит как обычно: чтение, счётчики, `errors.csv`, выходные CSV, отчёт.

## 11. Тестирование

```kotlin
val outcome = MigrationTest.run(
    BackfillCustomerTier(repo),
    outputFolder = tmp,
    dryRun = false,
    errorThreshold = 0,
    progressEvery = 1000,
)

assertThat(outcome.failure).isNull()
assertThat(outcome.report.successful).isEqualTo(9998)
assertThat(outcome.report.sourceSkipped).isEqualTo(2)
```

`MigrationTest.run` не требует графа Kora: зависимости подставляются конструктором самой
миграции. `ExecutionOutcome.failure` — исключение из тела, если оно было; оно **возвращается,
а не пробрасывается**, потому что счётчики упавшего прогона нужны и тесту, и отчёту.

Проверять wire-up на настоящем графе стоит отдельным тестом с компонентом `MigrationExit`
(см. `example/src/test`).

## 12. Архетипы

Полные разборы — в `docs/examples/`. Скелеты:

**Export (БД → CSV).**

```kotlin
override fun MigrationScope.run() {
    val out = csv("orders.csv", "id", "status", "total")
    each(repo.findAll()) { o -> out.row(o.id, o.status, o.total) }
}
```

**Correction (CSV → UPDATE).**

```kotlin
override fun MigrationScope.run() {
    errors.includeItem<Fix> { "id=${it.id}" }
    each(readCsvAs<Fix>("fixes.csv"), onItemError = ItemError.Skip) { fix ->
        if (!dryRun) repo.setStatus(fix.id, fix.status)
    }
}
```

**Backfill через HTTP.**

```kotlin
override fun MigrationScope.run() {
    each(repo.missingProfiles(), parallel = 8, onItemError = ItemError.Skip) { id ->
        val profile = client.fetch(id)          // @Retry живёт на самом клиенте
        if (!dryRun) repo.save(id, profile)
    }
}
```

**Сравнение двух источников.**

```kotlin
override fun MigrationScope.run() {
    val diff = csv("diff.csv", "id", "left", "right")
    val right = other.findAll().associateBy { it.id }
    each(mine.findAll()) { l ->
        val r = right[l.id]
        if (r?.total != l.total) diff.row(l.id, l.total, r?.total)
    }
}
```

**Курсорная пагинация** (в библиотеке нет `pages` — это обычный `generateSequence`):

```kotlin
val all = generateSequence(repo.page(after = 0L, limit = 500)) { prev ->
    if (prev.size < 500) null else repo.page(after = prev.last().id, limit = 500)
}.flatten()

each(all, progress = Progress.Every(5_000)) { row -> ... }
```

## 13. Антипаттерны и грабли

**Не оборачивайте `each` в свой try/catch «на всякий случай».** Политика ошибок — параметр
цикла; свой catch отнимет у движка и аудит, и счётчики.

**Не пишите `if (dryRun) return` в начале тела.** Смысл репетиции — прогнать чтение и счётчики.
Проверяется каждая запись отдельно.

**Не делайте тяжёлую работу в конструкторе миграции.** Граф поднимается весь: конструктор
выполнится, даже если запускается другая миграция.

**Не забывайте `errors.includeItem<T>` для элементов с персональными данными.** По умолчанию
в `errors.csv` уедет `toString()` целиком.

**`parallel` — редкое исключение, а не улучшение по умолчанию.** Он имеет смысл, когда узкое
место — сетевая задержка. На записи в одну БД он обычно только добавляет конкуренцию, а цена
— обязательная потокобезопасность `handle` и перепутанные строки в логе.

**Не ловите `CsvStructureException` в `onRowError`.** Оно летит мимо политики намеренно: файл
не той формы одинаково сломает каждую следующую строку, и `Skip` превратил бы прогон в тихий
пустой.

**Не считайте элементы вручную.** `EachResult` и отчёт уже всё считают, и считают согласованно.

**Retry — не задача библиотеки.** Kora `@Retry` на `@HttpClient` / `@KafkaPublisher` / методе
репозитория работает на правильном уровне (один remote-вызов) и умеет классифицировать
исключения. Ретрай всей миграции почти всегда неверен.

**Транзакции — тоже не задача библиотеки.** Kora даёт `db.inTx { ... }`; ручные
`autoCommit`/`commit`/`rollback` писать не нужно.

**Помните, что `errorThreshold` считает skip'ы одного вызова `each`**, а не всего прогона.

## 14. Карта файлов

```
core/src/main/kotlin/io/github/dsudomoin/migration/
  Migration.kt              абстрактный класс миграции
  MigrationScope.kt         контракт скоупа, each, EachResult, @MigrationDsl
  MigrationExecution.kt     RunSettings, ExecutionOutcome, сборка и закрытие прогона
  MigrationTest.kt          запуск в тесте без Kora
  MigrationPrecondition.kt  маркер «забраковано до первого элемента»
  ItemError.kt              Fail / Skip / Handle
  Progress.kt               Default / Off / Every / Custom
  csv/CsvRead.kt            readCsv, ленивая последовательность строк
  csv/CsvRow.kt             CsvRow, CsvHeader, нормализация имён, CsvStructureException
  csv/CsvBind.kt            readCsvAs, CsvBinder, правила пустых ячеек
  csv/CsvValidation.kt      предпроверка файла, CsvValidationException
  csv/CsvWrite.kt           csv(...), CsvOutput
  csv/CsvEscape.kt          квотирование по RFC 4180
  csv/CsvStreams.kt         срез BOM
  error/ErrorReporter.kt    контракт аудитора + includeItem
  error/CsvFileErrorReporter.kt  дефолтный аудитор в errors.csv/errors.log
  internal/MigrationRun.kt  единственная реализация MigrationScope: циклы, пул, реестр ресурсов
  internal/ProgressTicker.kt
  internal/ErrorThresholdExceeded.kt
  report/                   MigrationReport, ReportBuilder, ReportFormatter

kora/src/main/kotlin/io/github/dsudomoin/migration/kora/
  MigrationModule.kt        @Module: конфиг + @Root-фабрика runner'а
  MigrationRunner.kt        Lifecycle: выбор миграции, RunSettings, файловый лог, коды возврата
  MigrationConfig.kt        @ConfigValueExtractor-интерфейс + программные значения
  MigrationExit.kt          перехват кода возврата
```

## 15. Чеклист перед первой миграцией в чужом проекте

- [ ] KSP подключён, `kapt` не используется.
- [ ] `MigrationModule` в списке родителей `@KoraApp`.
- [ ] В `application.conf` есть `migration.run = ${?MIGRATION_RUN}` **и**
      `migration.dryRun = ${?MIGRATION_DRY_RUN}` — вторая строка забывается чаще всего.
- [ ] Класс миграции помечен `@Component` и наследует `Migration("ИМЯ")`.
- [ ] Имя уникально среди всех миграций сервиса.
- [ ] Каждая запись в целевую систему обёрнута в `if (!dryRun)`.
- [ ] Для элементов с персональными данными зарегистрирован `errors.includeItem<T>`.
- [ ] Есть тест через `MigrationTest.run`, проверяющий счётчики и отсутствие `failure`.
- [ ] Прогон проверен с `MIGRATION_DRY_RUN=true` до боевого.
