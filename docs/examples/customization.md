# Кастомизация

> Базовые понятия — в [руководстве](../USER_GUIDE.md).

Библиотека намеренно маленькая: она даёт цикл, аудит, CSV и отчёт, а всё остальное — ваш
обычный код. Этот документ про то, где проходит граница и что делать с той стороны.

- [1. Свой компонент — это просто компонент](#1-свой-компонент--это-просто-компонент)
- [2. Транзакции](#2-транзакции)
- [3. Курсорная пагинация](#3-курсорная-пагинация)
- [4. Свои ресурсы](#4-свои-ресурсы)
- [5. Представление элемента в аудите](#5-представление-элемента-в-аудите)
- [6. Своё предусловие](#6-своё-предусловие)
- [7. Перехват кода возврата](#7-перехват-кода-возврата)
- [8. Запуск без Kora](#8-запуск-без-kora)
- [9. Что настраивается конфигом](#9-что-настраивается-конфигом)
- [10. Чего кастомизировать нельзя](#10-чего-кастомизировать-нельзя)

---

## 1. Свой компонент — это просто компонент

Главное, что стоит понять про эту библиотеку: **у неё нет плагинов для источников данных.**
Нет `s3(...)`, нет `sql(...)`, нет `http(...)`. Есть ваш компонент графа и вызов его метода
из тела миграции.

Пример: выгрузить отчёт не в локальный файл, а в S3.

```kotlin
@Component
class S3Uploader(
    private val client: S3Client,
    private val config: S3Config,
) {
    fun upload(key: String, file: Path) {
        client.putObject(
            PutObjectRequest.builder().bucket(config.bucket()).key(key).build(),
            RequestBody.fromFile(file),
        )
    }
}
```

```kotlin
@Component
class ExportToS3(
    private val repo: OrderRepository,
    private val s3: S3Uploader,
) : Migration("EXPORT-S3-001") {

    override fun MigrationScope.run() {
        val out = csv("orders.csv", "id", "status", "total")
        each(repo.findAll()) { o -> out.row(o.id, o.status, o.total) }

        // Файл ещё пишется движком буферизованно — до выгрузки его надо дописать на диск.
        out.flush()
        if (!dryRun) s3.upload("exports/orders-${LocalDate.now()}.csv", outputFolder.resolve("orders.csv"))
    }
}
```

Три вещи, на которые здесь стоит смотреть:

- **`S3Uploader` не знает про миграции.** Это обычный компонент сервиса, пригодный и для
  других задач. Библиотека о нём тоже ничего не знает.
- **`out.flush()` обязателен.** `row` намеренно не флашит на каждой строке, а движок закроет
  файл только в конце прогона — то есть после вашей выгрузки. Без `flush()` в S3 уехал бы
  обрезанный файл.
- **`if (!dryRun)` — на вас.** Загрузка в S3 — запись в чужую систему, и перехватить её
  библиотека не может.

## 2. Транзакции

Транзакции даёт Kora, не библиотека:

```kotlin
@Component
class ApplyTiers(
    private val db: JdbcDatabase,
    private val repo: LoyaltyRepository,
) : Migration("TIERS-001") {

    override fun MigrationScope.run() {
        each(repo.pending()) { customer ->
            if (!dryRun) db.inTx { connection ->
                repo.setTier(connection, customer.id, customer.newTier)
                repo.addHistory(connection, customer.id, customer.oldTier, customer.newTier)
            }
        }
    }
}
```

`db.inTx` открывает соединение, коммитит на выходе и откатывает на исключении. Ручные
`autoCommit = false` / `commit()` / `rollback()` писать не нужно.

**Транзакция на элемент, а не на цикл.** Обернуть весь `each` в одну транзакцию технически
можно, но почти всегда не нужно: миллион изменений в одной транзакции — это распухший WAL,
долгий откат и заблокированные строки на всё время прогона. Если атомарность нужна на группу —
группируйте источник (`chunked`) и делайте транзакцию на группу.

**`inTx` и `parallel` несовместимы наивно.** Соединение нельзя делить между потоками; при
`parallel > 1` каждый воркер должен брать своё, а размер пула соединений — быть не меньше
`parallel`.

## 3. Курсорная пагинация

Специального API для страниц в библиотеке нет — обычный `generateSequence` делает то же самое
и читается без документации:

```kotlin
private fun MigrationScope.allOrders(): Sequence<Order> {
    val size = 500
    return generateSequence(repo.page(afterId = 0, limit = size)) { prev ->
        // Неполная страница = дошли до конца; следующий запрос вернул бы пусто.
        if (prev.size < size) null else repo.page(prev.last().id, size)
    }.flatten()
}
```

Курсор по `id > :afterId`, а не `OFFSET`: под конец большой таблицы `OFFSET` заставляет базу
пролистывать всё уже отданное.

Последовательность ленивая, и `each` читает её ровно настолько, насколько успевает
обрабатывать — в том числе при `parallel > 1`. Цена — размер источника неизвестен, и прогресс
покажет счётчик без доли.

## 4. Свои ресурсы

Движок закрывает то, что открыл сам: выходные CSV, входные потоки `readCsv`, свой пул потоков.
Ваши ресурсы — на вас, и обычного `use` достаточно:

```kotlin
override fun MigrationScope.run() {
    Files.newBufferedWriter(outputFolder.resolve("report.txt")).use { w ->
        each(repo.findAll()) { o -> w.write("${o.id}\n") }
    }
}
```

`outputFolder` — та же папка, куда движок кладёт `migration.log` и `errors.csv`, так что
артефакты прогона останутся в одном месте.

Если нужен именно CSV — берите `csv(...)`: он и закроется сам, и проквотирует значения.

## 5. Представление элемента в аудите

По умолчанию в `errors.csv` уезжает `toString()` элемента, обрезанный до 500 символов. Для
объектов с персональными данными это неприемлемо:

```kotlin
override fun MigrationScope.run() {
    errors.includeItem<Customer> { "id=${it.id}" }
    errors.includeItem<CsvRow> { "line=${it.lineNumber}" }
    ...
}
```

Регистрируйте **в первых строках `run()`**, до первого цикла: метод потокобезопасен, но
регистрация во время параллельной обработки на несколько записей может не успеть примениться.

Поиск идёт по классу, затем по супертипам и интерфейсам, так что `includeItem<Map<*, *>>`
сработает и для `LinkedHashMap`.

Аудитор доступен и напрямую — для собственной отчётности:

```kotlin
errors.report(IllegalStateException("нет курса на ${order.currency}"), order)
```

Это добавит строку в `errors.csv`, но не сделает элемент пропущенным: счётчики ведёт цикл,
и решает он по исключению из обработчика, а не по записям в аудит.

## 6. Своё предусловие

Отказ до первого обработанного элемента — это код возврата `2` и обещание «целевая система
не тронута». Своё такое условие помечается маркером:

```kotlin
class InputMissing(message: String) : RuntimeException(message), MigrationPrecondition

@Component
class ImportFromDrop(private val repo: OrderRepository) : Migration("IMPORT-001") {

    override fun MigrationScope.run() {
        val input = Path.of("/mnt/drop/orders.csv")
        if (!Files.exists(input)) throw InputMissing("нет файла выгрузки: $input")

        each(readCsvAs<Order>(input.toString())) { ... }
    }
}
```

Маркер сам по себе кода `2` не гарантирует: runner выдаёт его, только если счётчик обработанных
нулевой. Бросить такое исключение можно и в середине работы — тогда состояние уже частичное,
и честный ответ `1`.

## 7. Перехват кода возврата

По умолчанию runner зовёт `exitProcess(code)`: одноразовый скрипт живёт отдельным процессом,
и код возврата — единственное, что видит оркестратор.

Компонент `MigrationExit` в графе перехватывает его. Нужен в двух случаях: тесты, поднимающие
настоящий граф (иначе `exitProcess` убьёт JVM тест-раннера), и встраивание runner'а
в приложение, которое продолжает жить после миграции.

```kotlin
@Component
class RecordingExit : MigrationExit {
    @Volatile var code: Int? = null
    override fun exit(code: Int) { this.code = code }
}
```

```kotlin
@Test
fun `миграция видна графу и отрабатывает`() {
    val graph = AppGraph.graph().init().block()
    val exit = graph.get(AppGraph.graph().recordingExit)

    assertThat(exit.code).isEqualTo(0)
}
```

## 8. Запуск без Kora

`core` от Kora не зависит. Соберите прогон сами:

```kotlin
val outcome = MigrationExecution.execute(
    ExportVipCustomers(repo, config),
    RunSettings(
        dryRun = false,
        outputFolder = Path.of("logs/vip-export"),
        errorThreshold = 100,
        progressEvery = 5_000,
        maxItemReprLength = 200,
        includeStackTrace = true,
    ),
)

println(ReportFormatter().format(outcome.report))
if (outcome.failure != null) exitProcess(1)
```

Что придётся сделать самому: выбрать миграцию по имени, подключить файловый лог, превратить
исход в код возврата. Ровно это и делает `MigrationRunner` — посмотрите на него как на образец.

## 9. Что настраивается конфигом

```hocon
migration {
  defaults {
    errorThreshold = 500      # дефолт для each(errorThreshold = null)
    progressEvery = 5000      # шаг Progress.Default
  }
  errorReporting {
    includeStackTrace = false # только errors.csv, без errors.log
    maxItemReprLength = 200   # обрезка itemRepr
  }
  report {
    asciiOnly = true          # для CI без UTF-8 терминала
  }
}
```

Всё это — дефолты уровня прогона; на месте вызова они перебиваются параметрами `each`.

Формат CSV конфигом не задаётся намеренно: каждая миграция читает свой файл со своим форматом,
и разделитель должен быть виден в той же строке, что и имя файла.

## 10. Чего кастомизировать нельзя

**Подставить свой `ErrorReporter`.** Аудитор создаёт сам прогон, точки подключения нет.
Настраивается представление элемента (`includeItem`) и содержимое файлов
(`migration.errorReporting`). Если нужна интеграция с Sentry — пишите в него из обработчика
или из классификатора `ItemError.Handle`.

**Заменить пул потоков.** Пул создаётся лениво при первом `each(parallel > 1)` и живёт до
конца прогона. Если нужен свой executor — не используйте `parallel`, а разложите работу сами.

**Перехватывать записи под dry-run.** Библиотека не видит вызовов ваших компонентов —
на этом стоит вся её модель. `if (!dryRun)` пишется руками, и это осознанный размен: иначе
переиспользовать обычные репозитории сервиса было бы нельзя.

**Ретраить.** Kora `@Retry` на клиенте работает на правильном уровне — один remote-вызов,
с классификацией исключений и backoff. Ретрай миграции повторил бы и то, что уже применилось.
