# Migration DSL — руководство

Библиотека для одноразовых миграционных скриптов на Kora. Сервис поднимается разово,
выполняет одну названную миграцию и завершается с кодом возврата.

- [1. Что библиотека делает за вас](#1-что-библиотека-делает-за-вас)
- [2. Установка](#2-установка)
- [3. Первая миграция](#3-первая-миграция)
- [4. Тело миграции](#4-тело-миграции)
- [5. Цикл `each`](#5-цикл-each)
- [6. Что делать с ошибкой элемента](#6-что-делать-с-ошибкой-элемента)
- [7. Прогресс](#7-прогресс)
- [8. Чтение CSV](#8-чтение-csv)
- [9. Запись CSV](#9-запись-csv)
- [10. Dry-run](#10-dry-run)
- [11. Артефакты прогона и отчёт](#11-артефакты-прогона-и-отчёт)
- [12. Коды возврата](#12-коды-возврата)
- [13. Тестирование](#13-тестирование)
- [14. Частые вопросы](#14-частые-вопросы)

---

## 1. Что библиотека делает за вас

Миграция здесь — обычный класс с обычным телом. Данные она берёт через **ваши же** компоненты
сервиса: JDBC-репозитории, HTTP-клиенты, Kafka-паблишеры. Своего доступа к данным библиотека
не содержит — ни SQL, ни Kafka, ни HTTP внутри неё нет, и учить её вашей схеме не нужно.

Берёт она на себя то, что иначе пишется заново в каждом таком скрипте:

- цикл со счётчиками, политикой ошибок, порогом и прогрессом;
- запись сбойных элементов в `errors.csv` и стектрейсов в `errors.log`;
- чтение CSV с любым разделителем и кодировкой, сборку строки в DTO и отказ до старта, если
  файл битый;
- запись выходных CSV, которые закроются сами;
- папку артефактов с `migration.log`;
- итоговый отчёт и код возврата, по которому оркестратор поймёт, что случилось.

Ста строк ручного `try/catch`, счётчиков и `BufferedWriter` в каждом скрипте больше нет.

## 2. Установка

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

Кодогенерация Kora для Kotlin работает только через KSP. `kapt` с
`ru.tinkoff.kora:annotation-processors` не поддерживается — если он есть в проекте, миграции
просто не соберутся.

Подключите модуль:

```kotlin
@KoraApp
interface App :
    HoconConfigModule,
    JdbcDatabaseModule,
    MigrationModule
```

Одной строки достаточно. Секцию `migration { ... }` модуль читает сам, а runner помечен `@Root`
и потому создаётся графом без внешних зависимостей.

В `application.conf`:

```hocon
migration {
  run = ${?MIGRATION_RUN}
  dryRun = ${?MIGRATION_DRY_RUN}
}
```

**Вторую строку забывают чаще всего.** Библиотека нигде не читает окружение сама — переменные
приезжают только через `${?VAR}`. Без неё `MIGRATION_DRY_RUN=true ./gradlew run` спокойно
уйдёт в бой.

## 3. Первая миграция

Задача: проставить клиентам тариф по сумме покупок, взяв список из CSV.

```kotlin
data class Customer(val id: Long, val email: String, val spend: Long)

@Component
class BackfillCustomerTier(
    private val repo: CustomerRepository,
) : Migration("CUSTOMER-TIER-001") {

    override fun MigrationScope.run() {
        errors.includeItem<Customer> { "id=${it.id}" }

        val out = csv("customer-tier.csv", "id", "spend", "tier")

        each(
            readCsvAs<Customer>("customers.csv", classpath = true),
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
```

Запуск:

```bash
MIGRATION_RUN=CUSTOMER-TIER-001 MIGRATION_DRY_RUN=true ./gradlew run   # сначала репетиция
MIGRATION_RUN=CUSTOMER-TIER-001 ./gradlew run                          # потом боевой
```

Разберём по строкам:

- `Migration("CUSTOMER-TIER-001")` — имя, которое runner сравнивает с `migration.run`. Оно же
  попадёт в отчёт и в каждую строку `errors.csv`.
- `@Component` — обычная регистрация в графе Kora. Runner собирает все миграции сервиса и
  выбирает нужную по имени.
- `errors.includeItem<Customer>` — как элемент будет показан в `errors.csv`. Без этой строки
  туда уедет `toString()` целиком, вместе с почтой.
- `csv(...)` — выходной файл; закроет его движок.
- `readCsvAs<Customer>` — строки файла сразу приезжают объектами.
- `if (!dryRun)` — единственный способ не тронуть целевую систему на репетиции. Подробности
  в §10.

## 4. Тело миграции

`run()` объявлен extension-членом:

```kotlin
abstract class Migration(val name: String) {
    abstract fun MigrationScope.run()
}
```

Благодаря этому внутри одновременно видны и зависимости класса, и хелперы движка, без
префиксов вроде `scope.each` или `ctx.log`.

Длинное тело разбивается приватными extension-функциями на `MigrationScope`:

```kotlin
override fun MigrationScope.run() {
    val stale = collectStale()
    applyFixes(stale)
}

private fun MigrationScope.collectStale(): List<Order> {
    log.info("ищем зависшие заказы")
    return repo.findStuck()
}

private fun MigrationScope.applyFixes(orders: List<Order>) {
    each(orders) { if (!dryRun) repo.fix(it.id) }
}
```

Обычный приватный метод (без `MigrationScope.`) хелперов не увидит — это не ограничение,
а подсказка компилятора, что шаг миграции должен быть шагом миграции.

Из скоупа доступны четыре вещи:

| | |
|---|---|
| `dryRun: Boolean` | режим репетиции |
| `log: Logger` | SLF4J этой миграции; всё уходит и в `migration.log` |
| `errors: ErrorReporter` | авто-аудитор сбойных элементов |
| `outputFolder: Path` | папка артефактов прогона |

## 5. Цикл `each`

```kotlin
each(
    items,                              // Sequence<T> или Iterable<T>
    parallel = 1,
    onItemError = ItemError.Fail,
    progress = Progress.Default,
    errorThreshold = null,
) { item ->
    // тело обработчика
}
```

Возвращает `EachResult(processed, successful, skipped, failed)` — счётчики этого вызова.
В отчёте прогона они суммируются по всем циклам.

**Однопоточный режим — умолчание и подавляющий случай.** При `parallel = 1` это буквально
обычный `for` в текущем потоке: ни пула, ни семафора, и стектрейс падения указывает на строку
вашей миграции, а не на кишки движка.

**`parallel > 1` — редкое исключение.** Пул создаётся лениво при первом таком вызове и
закрывается в конце прогона. Он оправдан, когда узкое место — сетевая задержка: восемь
параллельных HTTP-запросов вместо восьми последовательных. На записи в одну БД он обычно
только добавляет конкуренцию.

Что придётся учесть, включив его:

- обработчик обязан быть потокобезопасным;
- строки в логе перемешаются;
- `CsvOutput.row` потокобезопасен, но сериализует записи через общий lock — на `parallel > 32`
  это станет узким местом, и лучше писать в несколько файлов.

Ленивый источник читается ровно настолько, насколько его успевают потреблять, так что
миллионный CSV не окажется в памяти целиком.

**Порог ошибок.** `errorThreshold` считает пропущенные элементы **этого вызова**. При
`skipped > threshold` цикл прерывается, прогон падает с кодом `1`. `null` берёт значение из
конфига (`migration.defaults.errorThreshold`), `0` выключает порог.

Порог — способ не узнать наутро, что «миграция прошла», пропустив 900 тысяч записей из миллиона.

## 6. Что делать с ошибкой элемента

Каждая ошибка элемента в любом случае попадает в `errors.csv` и `errors.log`. Политика решает
только, продолжать ли цикл.

```kotlin
ItemError.Fail    // умолчание: ошибка валит прогон
ItemError.Skip    // ошибка аудитируется, цикл идёт дальше
ItemError.Handle<T> { e, item -> ... }   // решает ваш классификатор
```

`Fail` по умолчанию выбран намеренно: пропускать ошибки — осознанное решение, а не то, что
случается само.

`Handle` видит типизированный элемент:

```kotlin
onItemError = ItemError.Handle { e, order ->
    when {
        e is HttpClientException && e.code() == 404 -> ItemError.Decision.Skip  // нет во внешней системе
        e is HttpClientException && e.code() >= 500 -> ItemError.Decision.Fail  // сервис лёг, дальше бессмысленно
        else -> ItemError.Decision.Fail
    }
}
```

### Что попадёт в `errors.csv`

Колонки: `timestamp,migration,itemRepr,errorClass,errorMessage`. `itemRepr` по умолчанию —
`toString()` элемента, обрезанный до 500 символов.

Если в элементе есть персональные данные, задайте представление первой же строкой `run()`:

```kotlin
errors.includeItem<Customer> { "id=${it.id}" }
errors.includeItem<CsvRow> { "id=${it["id"]}" }
```

Вторая строка нужна потому, что строка файла может сломаться ещё до сборки в `Customer` —
тогда аудит получит `CsvRow`, а не ваш объект.

Регистрировать нужно **до первого цикла**: метод потокобезопасен, но регистрация во время
параллельной обработки на несколько записей может не успеть примениться.

## 7. Прогресс

```kotlin
Progress.Default               // каждые 1000 элементов (настраивается конфигом)
Progress.Off                   // тишина
Progress.Every(500)
Progress.Custom(1000) { done, total -> "переложено $done из ${total ?: "?"}" }
```

Формат зависит от того, известен ли размер источника. У коллекции известен:

```
progress: 300/1200 (25%)  elapsed=12s  rate=25/s
```

У `Sequence` (например, у `readCsv`) размера нет, и доли не будет:

```
progress: 300  elapsed=12s  rate=25/s
```

На коротком списке `Progress.Default` укорачивает шаг сам, примерно до десяти строк за цикл —
иначе список на 300 элементов не дал бы ни одной строки, и вы смотрели бы в тишину, гадая,
не завис ли прогон.

## 8. Чтение CSV

### Строки как есть

```kotlin
each(readCsv("orders.csv") { row -> row["id"].toLong() }) { id -> ... }
```

`row` — это `CsvRow`:

```kotlin
row["spend_amount"]        // значение; нет колонки → падение со списком колонок файла
row.getOrNull("comment")   // null, если колонки нет
row.lineNumber             // заголовок — 1, первая запись — 2
row.columns                // имена колонок как в файле
```

Имя ищется без учёта регистра и способа записи: `spend_amount`, `SPEND_AMOUNT`, `Spend Amount`
и `spendAmount` — одна и та же колонка. Пустая ячейка у короткой строки — пустая строка, а не
ошибка: выгрузки регулярно обрывают хвостовые разделители.

### Чужие форматы

```kotlin
readCsv(
    "выгрузка.csv",
    delimiter = ';',
    charset = Charset.forName("windows-1251"),
) { row -> ... }
```

Excel в русской локали пишет `;`, 1С — windows-1251. BOM срезается всегда, так что имя первой
колонки не превратится в невидимо-испорченное; BOM от UTF-16 заодно сам задаёт кодировку.

### Сразу в DTO

```kotlin
data class Customer(val id: Long, val email: String, val spendAmount: BigDecimal, val registeredAt: LocalDate)

each(readCsvAs<Customer>("customers.csv")) { customer -> ... }
```

Колонка ищется по имени свойства той же нормализацией, поэтому `spend_amount` попадёт
в `spendAmount` без единой аннотации. Лишние колонки файла игнорируются — в чужой выгрузке
их обычно вчетверо больше, чем нужно миграции.

Что происходит с пустой ячейкой или отсутствующей колонкой:

| Поле | Результат |
|---|---|
| со значением по умолчанию | умолчание |
| nullable (`Long?`) | `null` |
| non-null `String` | пустая строка |
| остальные non-null (`Long`, `Boolean`, `BigDecimal`, …) | ошибка строки |

Последняя строка — главная. Сам Jackson на пустой ячейке и на отсутствующей колонке отдаёт под
`Long` ноль, под `Boolean` — `false`. Такой ноль неотличим от настоящего значения ни в отчёте,
ни в целевой системе, и библиотека этого не допускает.

### Битый файл роняет прогон до старта

`readCsvAs` разбирает файл целиком, прежде чем отдать первый объект. Если хоть одна строка не
собирается, прогон падает, ничего не тронув, и печатает список всех битых строк:

```
customers.csv: 3 строки не прошли проверку — миграция не запускалась, целевая система не тронута
  строка 3: Cannot deserialize value of type `long` from String "НЕ-ЧИСЛО": not a valid `long` value
  строка 4: колонка 'spend' пуста, а поле 'spend' типа Long обязательно — заполните ячейку, сделайте поле nullable или задайте значение по умолчанию
  строка 6: Cannot deserialize value of type `long` from String "пусто": not a valid `long` value
```

Код возврата — `2`. Чинить файл по одной строке за прогон не придётся.

Проверка стоит второго чтения файла. Она отключается двумя способами:

- `onRowError = ItemError.Skip` — битые строки для того и разрешены, проверка пропускается сама;
- `streaming = true` — если файл настолько велик, что второе чтение дорого.

Прикладной код при проверке не вызывается ни разу — дважды разбирается только сам файл.
Поэтому у `readCsv` с вашей лямбдой предпроверки нет: библиотека не может знать, что в лямбде
нет побочных эффектов.

Строка, отброшенная при чтении, попадает в отчёт отдельной строкой `Source rows dropped` —
до цикла она не дошла и никогда не была обработанной.

## 9. Запись CSV

```kotlin
val out = csv("result.csv", "id", "status", "amount")
out.row(order.id, "OK", order.amount)
```

- Файл создаётся в `outputFolder` и **закрывается движком** в конце прогона. Закрывать руками
  не нужно.
- Повторный вызов с тем же именем возвращает тот же handle, так что `csv(...)` внутри цикла
  не затрёт написанное раньше.
- Значения проходят через `toString()`, `null` становится пустой ячейкой, а всё, что порвало бы
  строку, квотируется по RFC 4180.
- `row` не делает flush на каждую строку: миллион записей — миллион системных вызовов. Всё
  уезжает на диск при `flush()` или на закрытии.

Для Excel:

```kotlin
csv("отчёт.csv", "имя", "сумма", delimiter = ';', bom = true)
```

Без BOM Excel прочитает UTF-8 как ANSI и покажет кириллицу кракозябрами. В кодировке, которая
BOM не умеет, запрос будет отвергнут до создания файла — иначе BOM уехал бы туда символом `?`
и сломал имя первой колонки.

Имя файла обязано остаться внутри `outputFolder` и не совпадать со служебными `errors.csv`,
`errors.log`, `migration.log`.

## 10. Dry-run

`dryRun` — **флаг, а не перехват**:

```kotlin
if (!dryRun) repo.updateTier(customer.id, tier)
```

Библиотека не видит вызовов ваших компонентов и не может их заблокировать. Забытая проверка
под `MIGRATION_DRY_RUN=true` запишет в целевую систему по-настоящему, и предупреждения об этом
не будет — предупреждать не о чем, перехватывать нечего.

Это сознательный размен. Чтобы перехватывать записи, библиотеке пришлось бы владеть всем
доступом к данным: своими обёртками над SQL, Kafka и HTTP. Тогда переиспользовать обычные
репозитории сервиса стало бы нельзя, а ради этого всё и затевалось.

Под репетицией как обычно работает всё остальное: чтение, счётчики, `errors.csv`, выходные CSV
и отчёт. Отчёт помечен `Mode: DRY-RUN`.

Практика: сначала репетиция, потом боевой прогон, и оба раза с одним и тем же именем миграции.

## 11. Артефакты прогона и отчёт

Всё складывается в `migration.outputFolder` (по умолчанию `logs/<имя миграции>`):

| Файл | Что внутри |
|---|---|
| `migration.log` | всё, что писалось в SLF4J за прогон |
| `errors.csv` | по строке на каждую сбойную запись |
| `errors.log` | стектрейсы тех же ошибок |
| ваши файлы | всё, что открыто через `csv(...)` |

`errors.csv` и `errors.log` создаются лениво: у чистого прогона их не будет. Один прогон —
один свежий набор артефактов, старые затираются.

В конце в лог уезжает отчёт:

```
═══════════════════════════════════════════════════════════
Migration: CUSTOMER-TIER-001
Started:   2026-09-08 21:18:43 +03:00
Finished:  2026-09-08 21:18:43 +03:00
Duration:  0s
Mode:      REAL
───────────────────────────────────────────────────────────
Processed:                 8
  ✓ Successful:            8
  ⊘ Skipped (errors):      0
  ✗ Failed:                0
  ⊘ Source rows dropped:     2
───────────────────────────────────────────────────────────
Error details:  build/example-output/run/errors.csv
Error traces:   build/example-output/run/errors.log
═══════════════════════════════════════════════════════════
```

Держится тождество `Processed = Successful + Skipped + Failed`. В него намеренно не входят:

- **Source rows dropped** — строки, отброшенные при чтении файла: до цикла они не дошли;
- **Unhandled failures** — отказ уровня прогона, у которого обрабатываемого элемента могло
  и не быть.

Строки сверх обязательных печатаются, только когда им есть что показать: постоянные нули
приучили бы глаз их пропускать.

Для CI без UTF-8 терминала: `migration.report.asciiOnly = true`.

## 12. Коды возврата

| Код | Что произошло | Состояние целевой системы |
|---|---|---|
| `0` | прогон завершён | изменения применены |
| `1` | исключение из тела, превышенный порог ошибок, прерывание | **может быть частичным** |
| `2` | забраковано до первого элемента | не тронуто |

Код `2` — это неизвестное или дублирующееся имя миграции, битые значения конфига, недоступный
`outputFolder` или отвергнутый входной файл. Общее у них одно: ни один элемент не обработан,
и перезапускать после починки безопасно.

Код `1` требует разбора: часть работы могла быть сделана.

Своё предусловие объявляется маркером:

```kotlin
class InputMissing(message: String) : RuntimeException(message), MigrationPrecondition

override fun MigrationScope.run() {
    if (!Files.exists(input)) throw InputMissing("нет файла выгрузки: $input")
    ...
}
```

## 13. Тестирование

```kotlin
@Test
fun `тариф проставляется по сумме покупок`(@TempDir tmp: Path) {
    val repo = FakeCustomerRepository()

    val outcome = MigrationTest.run(BackfillCustomerTier(repo), outputFolder = tmp)

    assertThat(outcome.failure).isNull()
    assertThat(outcome.report.successful).isEqualTo(8)
    assertThat(outcome.report.sourceSkipped).isEqualTo(2)
    assertThat(repo.tierOf(5)).isEqualTo("PLATINUM")
}
```

Граф Kora для этого не нужен: зависимости подставляются конструктором самой миграции.

`outcome.failure` — исключение из тела, если оно было. Оно возвращается, а не пробрасывается:
счётчики упавшего прогона обычно и есть то, что нужно проверить.

Репетиция проверяется тем же способом:

```kotlin
val outcome = MigrationTest.run(BackfillCustomerTier(repo), outputFolder = tmp, dryRun = true)

assertThat(outcome.report.successful).isEqualTo(8)
assertThat(repo.writes()).describedAs("на репетиции записей быть не должно").isEmpty()
```

Отдельно стоит проверить, что миграция вообще видна графу. Для этого поднимите настоящий
`@KoraApp` и добавьте компонент `MigrationExit`, иначе runner убьёт JVM тест-раннера:

```kotlin
@Component
class RecordingExit : MigrationExit {
    @Volatile var code: Int? = null
    override fun exit(code: Int) { this.code = code }
}
```

Готовый пример такого теста лежит в модуле `example/`.

## 14. Частые вопросы

**Как сделать retry?** Не через эту библиотеку. Kora `@Retry` на типизированном `@HttpClient`,
`@KafkaPublisher` или методе репозитория работает на правильном уровне — один remote-вызов,
с классификацией исключений и backoff. Ретрай всей миграции почти всегда неверен: он повторит
и то, что уже применилось.

**Как сделать транзакцию?** Через Kora: `db.inTx { ... }`. Ручные `autoCommit`/`commit`/
`rollback` писать не нужно.

**Как читать источник постранично?** Обычным `generateSequence` — специального `pages`
в библиотеке нет:

```kotlin
val all = generateSequence(repo.page(after = 0L, limit = 500)) { prev ->
    if (prev.size < 500) null else repo.page(after = prev.last().id, limit = 500)
}.flatten()

each(all, progress = Progress.Every(5_000)) { row -> ... }
```

**Граф поднимается весь, даже ради одной миграции?** Да. Поэтому тяжёлую работу в конструкторе
миграции делать нельзя: он выполнится, даже если запускается другая.

**Можно без Kora?** Да, `core` от неё не зависит. Соберите прогон сами:

```kotlin
val outcome = MigrationExecution.execute(
    MyMigration(repo),
    RunSettings(dryRun = false, outputFolder = Path.of("logs/my-run")),
)
```

Коды возврата, выбор миграции по имени и файловый лог придётся сделать самому — это работа
`kora`-модуля.

**Куда смотреть дальше?** [Примеры по архетипам](examples/) — экспорт, исправление по файлу,
бэкфилл через HTTP, сравнение двух источников, а также `customization.md` про свои компоненты,
транзакции и подмену аудитора.
