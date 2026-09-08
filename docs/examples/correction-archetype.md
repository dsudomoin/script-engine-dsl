# Correction: CSV → UPDATE в БД

> Базовые понятия — в [руководстве](../USER_GUIDE.md).

**Задача.** Аналитики прислали файл: список заказов, которым нужно проставить правильный статус
и сумму. Файл собран в Excel, разделитель `;`, кодировка windows-1251, и в нём наверняка есть
мусор — его собирали руками.

Это самый частый архетип и единственный, где по-настоящему важны и предпроверка файла,
и `dryRun`.

## DTO и компонент сервиса

```kotlin
data class Fix(
    val orderId: Long,
    val newStatus: String,
    val amount: BigDecimal,
    val comment: String?,       // необязательная колонка
)

@Repository
interface OrderRepository : JdbcRepository {
    @Query("UPDATE orders SET status = :status, amount = :amount WHERE id = :id")
    fun applyFix(id: Long, status: String, amount: BigDecimal)

    @Query("SELECT status FROM orders WHERE id = :id")
    fun statusOf(id: Long): String?
}
```

Файл от аналитиков:

```
order_id;new_status;amount;comment
1001;SHIPPED;150.00;
1002;CANCELLED;0.00;клиент отказался
```

Колонки в файле — `snake_case`, поля DTO — `camelCase`. Ничего сопоставлять руками не нужно:
`order_id` и `orderId` для библиотеки одно и то же.

## Скрипт

```kotlin
@Component
class ApplyOrderFixes(
    private val repo: OrderRepository,
) : Migration("ORDER-FIX-001") {

    override fun MigrationScope.run() {
        errors.includeItem<Fix> { "orderId=${it.orderId}" }
        errors.includeItem<CsvRow> { "line=${it.lineNumber}" }

        val applied = csv("applied.csv", "orderId", "oldStatus", "newStatus")

        val result = each(
            readCsvAs<Fix>(
                "fixes.csv",
                delimiter = ';',
                charset = Charset.forName("windows-1251"),
            ),
            onItemError = ItemError.Skip,
            errorThreshold = 50,
        ) { fix ->
            val old = repo.statusOf(fix.orderId)
                ?: throw IllegalStateException("заказа ${fix.orderId} нет в базе")

            if (!dryRun) repo.applyFix(fix.orderId, fix.newStatus, fix.amount)
            applied.row(fix.orderId, old, fix.newStatus)
        }

        log.info("применено ${result.successful}, пропущено ${result.skipped}")
    }
}
```

## Почему две регистрации `includeItem`

Строка файла проходит два этапа: сборку в `Fix` и обработку. Сломаться она может на любом,
и аудит получит разные объекты:

- упала сборка → в `errors.csv` уедет `CsvRow`;
- упала обработка → уедет `Fix`.

Без второй регистрации в файле окажется `line 42: order_id=1001, new_status=SHIPPED, ...` —
вся строка целиком, включая колонки, которых миграция даже не читает.

## Битый файл: два режима

**По умолчанию** (`onRowError = ItemError.Fail`) файл проверяется целиком до старта. Первая же
непарсящаяся строка означает, что прогон не начнётся вовсе:

```
fixes.csv: 2 строки не прошли проверку — миграция не запускалась, целевая система не тронута
  строка 5: Cannot deserialize value of type `java.math.BigDecimal` from String "—": not a valid representation
  строка 9: колонка 'new_status' пуста, а поле 'newStatus' типа String обязательно — ...
```

Код возврата `2`, база не тронута, чинить файл можно сразу весь.

**В примере выше** стоит `onRowError = ItemError.Skip`: файл собран руками, отдельные битые
строки ожидаемы, и ронять из-за них полезную работу не хочется. Тогда предпроверки нет —
искать в файле то, что заведомо простят, незачем, — а битые строки уезжают в `errors.csv`
и считаются в отчёте отдельной строкой `Source rows dropped`.

Выбирать между режимами стоит по одному признаку: **это данные, которым можно не доверять
частично, или файл, который целиком должен быть правильным?**

## Порог ошибок

`errorThreshold = 50` считает пропущенные элементы этого цикла. Пятьдесят первый пропуск валит
прогон с кодом `1`.

Порог — защита от «миграция прошла», при которой из десяти тысяч заказов применились сто.
Без него `ItemError.Skip` тихо стерпит что угодно, вплоть до полностью не того файла.

Обратите внимание: порог считает `skipped` — ошибки **обработки**. Строки, отброшенные при
чтении файла, идут в `sourceSkipped` и порог не трогают: до цикла они не дошли.

## Репетиция

```bash
MIGRATION_RUN=ORDER-FIX-001 MIGRATION_DRY_RUN=true ./gradlew run
```

Что произойдёт: файл прочитается, `statusOf` сходит в базу, `applied.csv` заполнится целиком,
отчёт покажет настоящие счётчики. Не выполнится единственная строка — `repo.applyFix`.

`applied.csv` после репетиции — это и есть план изменений: его можно показать аналитикам
до боевого прогона.

Проверять `dryRun` нужно у каждой записи отдельно. Соблазн написать `if (dryRun) return`
в начале тела стоит погасить: тогда репетиция не проверит ни чтение файла, ни наличие заказов
в базе, то есть ровно то, ради чего затевалась.

## Что демонстрирует архетип

- Чужой формат файла (`;`, windows-1251) одним параметром.
- Сопоставление `snake_case`-колонок с `camelCase`-полями без аннотаций.
- Выбор между «файл целиком обязан быть правильным» и «терпим отдельные битые строки».
- Порог ошибок как защиту от тихого недо-прогона.
- `if (!dryRun)` у каждой записи и выходной CSV как план изменений.
