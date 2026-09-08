# Comparison: два источника → расхождения в CSV

> Базовые понятия — в [руководстве](../USER_GUIDE.md).

**Задача.** Биллинг и CRM разошлись: у части клиентов в двух системах разные суммы. Нужно
получить файл расхождений, ничего не исправляя.

Read-only архетип, но с двумя источниками — и с ловушкой, о которой ниже.

## Компоненты сервиса

```kotlin
@Repository
interface BillingRepository : JdbcRepository {
    @Query("SELECT customer_id, total FROM billing_totals")
    fun totals(): List<Total>
}

@HttpClient("crm")
interface CrmClient {
    @HttpRoute(method = "GET", path = "/customers/{id}/total")
    fun total(@Path id: Long): CrmTotal
}

data class Total(val customerId: Long, val total: BigDecimal)
data class CrmTotal(val total: BigDecimal)
```

## Скрипт

```kotlin
@Component
class CompareBillingWithCrm(
    private val billing: BillingRepository,
    private val crm: CrmClient,
) : Migration("COMPARE-TOTALS-001") {

    override fun MigrationScope.run() {
        errors.includeItem<Total> { "customerId=${it.customerId}" }

        val diff = csv("diff.csv", "customerId", "billing", "crm", "delta")
        val mismatches = AtomicLong()

        val result = each(
            billing.totals(),
            parallel = 8,
            onItemError = ItemError.Skip,
            progress = Progress.Custom(1_000) { done, total ->
                "сверено $done/${total ?: "?"}, расхождений ${mismatches.get()}"
            },
        ) { row ->
            val theirs = crm.total(row.customerId).total
            if (theirs.compareTo(row.total) != 0) {
                mismatches.incrementAndGet()
                diff.row(row.customerId, row.total, theirs, row.total - theirs)
            }
        }

        log.info("сверено ${result.successful}, расхождений ${mismatches.get()}")
    }
}
```

## Ловушка: `compareTo`, а не `equals`

`BigDecimal("10.0") == BigDecimal("10.00")` — это `false`: `equals` у `BigDecimal` учитывает
масштаб. Сравнение сумм из двух систем через `!=` дало бы файл, полный расхождений, которых нет.

Это не про библиотеку, но именно на таком архетипе ошибка стоит дороже всего: отчёт выглядит
достоверно, и разбираться в нём будут долго.

## Почему здесь уместен `parallel`

Каждый элемент — один HTTP-запрос к CRM. Узкое место — сетевая задержка, и восемь параллельных
запросов действительно дают восьмикратное ускорение. Это редкий случай, когда `parallel`
оправдан.

Что он потребовал взамен:

- `mismatches` — `AtomicLong`, а не `var`: считают восемь потоков;
- `diff.row` потокобезопасен сам, но порядок строк в файле теперь произвольный. Если нужен
  устойчивый порядок — отсортируйте файл после прогона или пишите однопоточно.

## Прогресс со своим счётчиком

`Progress.Custom` получает `done` и `total` и может подмешать что угодно своё:

```
сверено 4000/25000, расхождений 37
```

`total` известен, потому что источник — `List`. Если бы источник был `Sequence` (курсорная
пагинация), `total` был бы `null` — отсюда `?: "?"` в шаблоне.

## Вторая сторона тоже большая

Если в CRM тоже можно сходить пачкой, `parallel` не нужен вовсе — достаточно одного запроса
на всё и сравнения в памяти:

```kotlin
override fun MigrationScope.run() {
    val diff = csv("diff.csv", "customerId", "billing", "crm")
    val theirs = crm.allTotals().associateBy { it.customerId }

    each(billing.totals()) { row ->
        val other = theirs[row.customerId]
        when {
            other == null -> diff.row(row.customerId, row.total, "НЕТ В CRM")
            other.total.compareTo(row.total) != 0 -> diff.row(row.customerId, row.total, other.total)
        }
    }
}
```

Это и быстрее, и проще: один сетевой вызов вместо десятков тысяч. Проверьте, есть ли у второй
системы такой метод, прежде чем разгонять цикл потоками.

## Запуск

```bash
MIGRATION_RUN=COMPARE-TOTALS-001 ./gradlew run
```

Прогон ничего не меняет, поэтому `dryRun` для него бессмысленен: проверять нечего, а `diff.csv`
пишется в обоих режимах — это диагностический артефакт, а не изменение целевой системы.

## Что демонстрирует архетип

- Два источника в одном теле — без всякого специального API для «джойна».
- Случай, когда `parallel` действительно оправдан, и что он требует взамен.
- `Progress.Custom` со своим счётчиком.
- Что пачка вместо цикла почти всегда лучше параллелизма.
