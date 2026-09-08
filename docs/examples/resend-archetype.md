# Resend: БД → обогащение → Kafka

> Базовые понятия — в [руководстве](../USER_GUIDE.md).

**Задача.** Из-за инцидента часть событий заказов не доехала до потребителей. Нужно перечитать
заказы за период, обогатить их данными клиента и переотправить в топик.

Архетип интересен тем, что здесь публикация — настоящая запись в чужую систему, отменить
которую нельзя. Всё остальное подчинено этому.

## Компоненты сервиса

Паблишер — обычный типизированный Kora `@KafkaPublisher`. Библиотека о нём ничего не знает:

```kotlin
@KafkaPublisher("kafka.orders.publisher")
interface OrdersPublisher {
    @KafkaPublisher.Topic("kafka.orders.publisher.resyncTopic")
    fun publish(key: String, @Json value: OrderEvent)
}

@Repository
interface OrderRepository : JdbcRepository {
    @Query("SELECT id, customer_id, status, updated_at FROM orders WHERE updated_at BETWEEN :from AND :to ORDER BY id")
    fun inWindow(from: Instant, to: Instant): List<Order>
}

@Repository
interface CustomerRepository : JdbcRepository {
    @Query("SELECT id, segment, email FROM customers WHERE id IN (:ids)")
    fun byIds(ids: List<Long>): List<Customer>
}
```

## Скрипт

```kotlin
@Component
class ResendOrderEvents(
    private val orders: OrderRepository,
    private val customers: CustomerRepository,
    private val publisher: OrdersPublisher,
    private val config: ResendConfig,
) : Migration("RESEND-ORDERS-001") {

    override fun MigrationScope.run() {
        errors.includeItem<Order> { "orderId=${it.id}" }

        val window = orders.inWindow(config.from(), config.to())
        log.info("в окне ${window.size} заказов")

        // Один запрос на всех вместо запроса на заказ: обогащение не должно превращаться
        // в N+1 просто потому, что цикл удобно читается.
        val segments = customers.byIds(window.map { it.customerId }.distinct())
            .associateBy({ it.id }, { it.segment })

        val sent = csv("resent.csv", "orderId", "customerId", "segment")

        val result = each(
            window,
            onItemError = ItemError.Skip,
            errorThreshold = config.maxFailures(),
        ) { order ->
            val segment = segments[order.customerId]
                ?: throw IllegalStateException("нет клиента ${order.customerId} для заказа ${order.id}")

            val event = OrderEvent(order.id, order.customerId, segment, order.status)
            if (!dryRun) publisher.publish(order.id.toString(), event)
            sent.row(order.id, order.customerId, segment)
        }

        log.info("отправлено ${result.successful}, пропущено ${result.skipped}")
    }
}
```

## Обогащение — одним запросом

Соблазн написать `customers.byId(order.customerId)` внутри цикла велик: строчка короче и читается
линейно. Ценой будет N запросов вместо одного, и на пятидесяти тысячах заказов миграция будет
идти часы вместо минут.

Правило простое: **всё, что можно достать пачкой до цикла, достаётся до цикла.** Библиотека
здесь не помогает и не мешает — это обычный код, и отвечать за него вам.

## Почему нет `parallel`

Kafka-паблишер и так батчит и отправляет асинхронно; узкого места, которое разошьют потоки,
здесь нет. Восемь потоков дали бы только перепутанный порядок в `resent.csv` и требование
потокобезопасности к обогащению.

Параллелизм стоит включать, когда узкое место — синхронная сетевая задержка на элемент
(см. [http-backfill](http-backfill-archetype.md)), а не когда «так быстрее по ощущению».

## Отправка не откатывается

Отсюда три следствия, которые стоит держать в голове:

**Порог обязателен.** `errorThreshold` не даст миграции переотправить половину топика, тихо
пропустив вторую.

**Код возврата `1` означает разбор.** Часть событий уже в топике, часть — нет. `resent.csv`
и есть список того, что уехало: он пишется сразу после успешной публикации.

**Идемпотентность на стороне потребителя.** Ключ сообщения — `order.id`, так что при повторном
прогоне компактящийся топик схлопнёт дубли. Если потребитель не идемпотентен, повторный прогон
после падения сделает двойную работу, и это нужно знать до, а не после.

## Репетиция

```bash
MIGRATION_RUN=RESEND-ORDERS-001 MIGRATION_DRY_RUN=true ./gradlew run
```

Под репетицией выполняется всё, кроме `publisher.publish`: заказы читаются, клиенты
обогащаются, `resent.csv` заполняется целиком. Это и есть список того, что уедет в топик
боевым прогоном — его можно просмотреть глазами.

Библиотека не перехватывает вызов паблишера и не может его перехватить: это ваш компонент,
о котором она ничего не знает. Забытая проверка `if (!dryRun)` отправит события по-настоящему.

## Что демонстрирует архетип

- Работу с чужим паблишером как с обычным компонентом графа.
- Обогащение пачкой до цикла вместо N+1 внутри.
- Почему `parallel` здесь не нужен, хотя выглядит уместным.
- Что делать, когда запись нельзя откатить: порог, файл отправленного, идемпотентный ключ.
