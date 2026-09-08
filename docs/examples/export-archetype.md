# Export: БД → CSV

> Базовые понятия — в [руководстве](../USER_GUIDE.md).

**Задача.** Финансовому отделу нужен снимок всех VIP-клиентов с балансом и датой регистрации,
чтобы открыть его в Excel. Источник — Postgres, выход — один CSV.

Самый короткий из архетипов: ничего не пишется, `dryRun` тут ничего не меняет.

## Компоненты сервиса

Репозиторий — обычный Kora `@Repository`, к миграциям отношения не имеющий:

```kotlin
@Repository
interface CustomerRepository : JdbcRepository {

    @Query("SELECT id, name, balance, joined_at FROM customers WHERE status = 'VIP' AND balance >= :minBalance ORDER BY id")
    fun findVip(minBalance: Long): List<CustomerRow>

    @Query("SELECT id, name, balance, joined_at FROM customers WHERE status = 'VIP' AND balance >= :minBalance AND id > :afterId ORDER BY id LIMIT :limit")
    fun pageVip(minBalance: Long, afterId: Long, limit: Int): List<CustomerRow>
}

data class CustomerRow(val id: Long, val name: String, val balance: Long, val joinedAt: LocalDate)
```

## Скрипт

```kotlin
@Component
class ExportVipCustomers(
    private val repo: CustomerRepository,
    private val config: ExportConfig,
) : Migration("EXPORT-VIP-001") {

    override fun MigrationScope.run() {
        val out = csv(
            "vip-customers.csv",
            "id", "name", "balance", "joinedAt",
            delimiter = ';',
            bom = true,
        )

        val result = each(repo.findVip(config.minBalance())) { customer ->
            out.row(customer.id, customer.name, customer.balance, customer.joinedAt)
        }

        log.info("выгружено ${result.successful} клиентов в ${outputFolder.resolve("vip-customers.csv")}")
    }
}
```

`delimiter = ';'` и `bom = true` — не украшение: без них Excel в русской локали положит всю
строку в одну ячейку и покажет кириллицу кракозябрами.

Файл закроет движок в конце прогона. Закрывать его руками не нужно, и `flush()` вызывать тоже:
`row` буферизует намеренно, иначе миллион строк дал бы миллион системных вызовов.

## Большая таблица

`findVip()` поднимает весь результат в память. Пока это тысячи строк — нормально; на миллионах
нужен курсор. Специального `pages` в библиотеке нет, потому что обычный `generateSequence`
делает то же самое и читается лучше:

```kotlin
override fun MigrationScope.run() {
    val out = csv("vip-customers.csv", "id", "name", "balance", "joinedAt")
    val pageSize = config.pageSize()

    val pages = generateSequence(repo.pageVip(config.minBalance(), afterId = 0, limit = pageSize)) { prev ->
        // Неполная страница означает, что мы дошли до конца: следующий запрос вернул бы пусто.
        if (prev.size < pageSize) null else repo.pageVip(config.minBalance(), prev.last().id, pageSize)
    }.flatten()

    each(pages, progress = Progress.Every(10_000)) { customer ->
        out.row(customer.id, customer.name, customer.balance, customer.joinedAt)
    }
}
```

Курсор идёт по `id > :afterId`, а не по `OFFSET`: под конец большой таблицы `OFFSET` заставляет
базу пролистывать всё, что уже отдано.

`Progress.Every(10_000)` здесь нужен потому, что источник — `Sequence`, и размер его неизвестен:
прогресс покажет счётчик без доли (`progress: 30000  elapsed=42s  rate=714/s`). У списка,
как в первом варианте, доля была бы известна.

## Разбивка на несколько файлов

`csv(...)` кеширует handle по имени, поэтому вызов внутри цикла безопасен — файл откроется
один раз на каждое имя:

```kotlin
each(repo.findVip(config.minBalance())) { customer ->
    val out = csv("vip-${customer.segment}.csv", "id", "name", "balance")
    out.row(customer.id, customer.name, customer.balance)
}
```

Все открытые файлы закроются в конце прогона, в порядке, обратном открытию.

## Запуск

```bash
MIGRATION_RUN=EXPORT-VIP-001 ./gradlew run
```

```hocon
migration {
  run = ${?MIGRATION_RUN}
  dryRun = ${?MIGRATION_DRY_RUN}
  outputFolder = "reports/vip"
}
```

Результат — `reports/vip/vip-customers.csv`, рядом `migration.log`. `errors.csv` не появится:
у чистого прогона его не создают.

## Что демонстрирует архетип

- Выход CSV с закрытием на стороне движка и форматом под Excel.
- Курсорную пагинацию обычным `generateSequence`, без специального API.
- Разницу прогресса на коллекции (доля известна) и на `Sequence` (неизвестна).
- Что read-only миграция ничего не должна делать с `dryRun`: проверять нечего.

## Чего здесь намеренно нет

**Порога ошибок.** Выгрузка либо проходит, либо падает; пропускать строки в отчёте для
финансистов нельзя, а `ItemError.Fail` по умолчанию как раз это и обеспечивает.

**`parallel`.** Узкое место — база и диск, а не задержка. Четыре потока здесь только
перемешали бы порядок строк в файле.
