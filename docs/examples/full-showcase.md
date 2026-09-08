# Full showcase: всё вместе

> Базовые понятия — в [руководстве](../USER_GUIDE.md). Здесь предполагается, что остальные
> архетипы уже прочитаны.

**Задача.** Годовой пересчёт лояльности. Одна миграция должна:

1. прочитать файл исключений от бизнеса (кого не трогать);
2. постранично пройти всех активных клиентов;
3. посчитать новый уровень по сумме покупок из другой системы;
4. записать уровень и историю **одной транзакцией**;
5. отправить событие в Kafka тем, у кого уровень вырос;
6. положить рядом три файла: применённое, исключённое и сводку.

Это не образец «как надо усложнять» — это проверка, что все части складываются в одно тело
без специального API для их склейки.

## Компоненты сервиса

```kotlin
@Repository
interface LoyaltyRepository : JdbcRepository {
    @Query("SELECT id, tier, spend FROM customers WHERE active = true AND id > :afterId ORDER BY id LIMIT :limit")
    fun page(afterId: Long, limit: Int): List<Customer>

    @Query("UPDATE customers SET tier = :tier WHERE id = :id")
    fun setTier(connection: Connection, id: Long, tier: String)

    @Query("INSERT INTO tier_history(customer_id, old_tier, new_tier, changed_at) VALUES (:id, :old, :new, :at)")
    fun addHistory(connection: Connection, id: Long, old: String, new: String, at: Instant)
}

@KafkaPublisher("kafka.loyalty.publisher")
interface LoyaltyPublisher {
    @KafkaPublisher.Topic("kafka.loyalty.publisher.upgradeTopic")
    fun upgraded(key: String, @Json value: TierUpgraded)
}

data class Exclusion(val customerId: Long, val reason: String)
```

## Скрипт

```kotlin
@Component
class RecalculateLoyalty(
    private val db: JdbcDatabase,
    private val repo: LoyaltyRepository,
    private val publisher: LoyaltyPublisher,
    private val config: LoyaltyConfig,
) : Migration("LOYALTY-RECALC-2026") {

    override fun MigrationScope.run() {
        errors.includeItem<Customer> { "id=${it.id}" }

        val excluded = loadExclusions()
        val applied = csv("applied.csv", "id", "oldTier", "newTier")
        val skipped = csv("excluded.csv", "id", "reason")
        val upgrades = AtomicLong()

        val result = each(
            activeCustomers(),
            onItemError = ItemError.Skip,
            errorThreshold = config.maxFailures(),
            progress = Progress.Custom(5_000) { done, _ ->
                "пересчитано $done, повышений ${upgrades.get()}"
            },
        ) { customer ->
            val reason = excluded[customer.id]
            if (reason != null) {
                skipped.row(customer.id, reason)
                return@each
            }

            val newTier = tierFor(customer.spend)
            if (newTier == customer.tier) return@each

            if (!dryRun) applyTier(customer, newTier)
            applied.row(customer.id, customer.tier, newTier)

            if (isUpgrade(customer.tier, newTier)) {
                upgrades.incrementAndGet()
                if (!dryRun) publisher.upgraded(customer.id.toString(), TierUpgraded(customer.id, newTier))
            }
        }

        writeSummary(result, upgrades.get(), excluded.size)
    }
}
```

## Шаг 1: файл исключений

```kotlin
private fun MigrationScope.loadExclusions(): Map<Long, String> {
    val list = readCsvAs<Exclusion>(
        "exclusions.csv",
        delimiter = ';',
        charset = Charset.forName("windows-1251"),
    ).toList()

    log.info("исключено из пересчёта: ${list.size}")
    return list.associateBy({ it.customerId }, { it.reason })
}
```

Здесь предпроверка работает по умолчанию, и это ровно то, что нужно: файл маленький, собран
бизнесом вручную, и одна опечатка в нём должна остановить прогон **до** первого изменения
в базе. Код возврата `2`, база не тронута, список всех битых строк — в логе.

Обратите внимание на `.toList()`: список исключений нужен целиком и заранее, потому что дальше
он используется как справочник. Это единственное место, где файл поднимается в память —
и он для этого достаточно мал.

## Шаг 2: постраничное чтение

```kotlin
private fun MigrationScope.activeCustomers(): Sequence<Customer> {
    val pageSize = config.pageSize()
    return generateSequence(repo.page(afterId = 0, limit = pageSize)) { prev ->
        if (prev.size < pageSize) null else repo.page(prev.last().id, pageSize)
    }.flatten()
}
```

Специального `pages` в библиотеке нет: обычный `generateSequence` делает то же самое, читается
без документации и легко отлаживается.

Источник ленивый, поэтому миллион клиентов не окажется в памяти. Цена — размер источника
неизвестен, и `total` в прогрессе будет `null`; поэтому в `Progress.Custom` выше он и не
используется.

## Шаг 3: транзакция

```kotlin
private fun applyTier(customer: Customer, newTier: String) {
    db.inTx { connection ->
        repo.setTier(connection, customer.id, newTier)
        repo.addHistory(connection, customer.id, customer.tier, newTier, Instant.now())
    }
}
```

Транзакции — забота Kora, а не библиотеки. `db.inTx` открывает соединение, коммитит на выходе
и откатывает на исключении; ручные `autoCommit`/`commit`/`rollback` писать не нужно и не стоит.

Метод намеренно объявлен **не** как extension на `MigrationScope`: он ничего из скоупа
не использует, и лишний receiver только скрывал бы это.

## Шаг 4: сводка

```kotlin
private fun MigrationScope.writeSummary(result: EachResult, upgrades: Long, exclusions: Int) {
    csv("summary.csv", "metric", "value").apply {
        row("processed", result.processed)
        row("changed", result.successful)
        row("upgrades", upgrades)
        row("exclusions", exclusions)
        row("skipped", result.skipped)
        row("mode", if (dryRun) "DRY-RUN" else "REAL")
    }
}
```

`EachResult` отдаёт счётчики цикла, так что вести их вручную не нужно — и они гарантированно
сойдутся с итоговым отчётом.

## `return@each` — это continue

```kotlin
val reason = excluded[customer.id]
if (reason != null) {
    skipped.row(customer.id, reason)
    return@each
}
```

Элемент при этом считается **успешным**: он обработан, просто работа для него оказалась
пустой. Это не `ItemError.Skip` — тот считает ошибки, и путать их не стоит: в отчёте
`Skipped (errors)` должен означать «сломалось», а не «не подошло по условию».

Если исключённых нужно видеть отдельным счётчиком — заведите свой `AtomicLong`, как сделано
с `upgrades`.

## Порядок эффектов внутри элемента

```kotlin
if (!dryRun) applyTier(customer, newTier)      // 1. база, транзакционно
applied.row(customer.id, customer.tier, newTier)  // 2. файл применённого
if (isUpgrade(...)) {
    if (!dryRun) publisher.upgraded(...)       // 3. событие
}
```

Порядок выбран так, чтобы файл не врал: строка в `applied.csv` появляется **после** успешной
записи в базу. Упади транзакция — исключение уйдёт в аудит, а строки в файле не будет.

Kafka идёт последней, потому что её нельзя откатить: если она упадёт, база уже согласована,
и разбор сводится к «переотправить события по `applied.csv`» — то есть к архетипу
[resend](resend-archetype.md).

## Репетиция

```bash
MIGRATION_RUN=LOYALTY-RECALC-2026 MIGRATION_DRY_RUN=true ./gradlew run
```

Что произойдёт: файл исключений прочитается и проверится, все страницы клиентов будут прочитаны,
уровни посчитаны, все три CSV заполнены целиком, счётчики настоящие. Не выполнятся ровно две
строки — `applyTier` и `publisher.upgraded`.

`applied.csv` после репетиции — полный план изменений на годовой пересчёт. Его можно отдать
бизнесу на утверждение, ничего не тронув.

Три места с `if (!dryRun)` — это три места, где легко ошибиться. Их стоит перечитать отдельно
перед боевым прогоном: библиотека не перехватывает вызовы ваших компонентов и предупредить
о забытой проверке не может.

## Что демонстрирует showcase

- Что файл, постраничная база, транзакция и Kafka складываются в одно обычное тело без
  какого-либо API для их склейки.
- Предпроверку маленького файла до первого изменения в базе.
- `generateSequence` вместо специальной пагинации.
- `db.inTx` как единственный способ делать транзакции.
- `return@each` как `continue`, не путаемый со `Skip`.
- Порядок эффектов, при котором выходной файл не врёт.
