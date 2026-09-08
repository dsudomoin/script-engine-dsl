# HTTP backfill: БД → внешний API

> Базовые понятия — в [руководстве](../USER_GUIDE.md).

**Задача.** У части клиентов не заполнен профиль: данные лежат в системе партнёра, и их нужно
забрать по HTTP и разложить к себе. Клиентов — сто тысяч, партнёрский API отвечает за ~200 мс.

Единственный архетип, где `parallel` действительно решает: сто тысяч × 200 мс — это пять
с половиной часов в один поток.

## Компоненты сервиса

```kotlin
@HttpClient("partner")
interface PartnerClient {

    @Retry(attempts = 3, delay = 200)     // ретрай живёт здесь, а не в миграции
    @HttpRoute(method = "GET", path = "/profiles/{id}")
    fun profile(@Path id: Long): PartnerProfile
}

@Repository
interface ProfileRepository : JdbcRepository {
    @Query("SELECT id FROM customers WHERE profile_filled = false ORDER BY id LIMIT :limit OFFSET :offset")
    fun missing(limit: Int, offset: Int): List<Long>

    @Query("UPDATE customers SET profile = :json, profile_filled = true WHERE id = :id")
    fun fill(id: Long, json: String)
}
```

## Скрипт

```kotlin
@Component
class BackfillProfiles(
    private val repo: ProfileRepository,
    private val partner: PartnerClient,
) : Migration("PROFILE-BACKFILL-001") {

    override fun MigrationScope.run() {
        val notFound = csv("not-found.csv", "customerId")
        val missing = AtomicLong()

        val result = each(
            repo.missing(limit = 100_000, offset = 0),
            parallel = 16,
            onItemError = ItemError.Handle { e, id ->
                // 404 у партнёра — нормальный ответ «такого клиента у нас нет».
                // 5xx означает, что партнёр лёг, и дальше идти бессмысленно.
                if (e is HttpClientResponseException && e.code() == 404) {
                    missing.incrementAndGet()
                    notFound.row(id)
                    ItemError.Decision.Skip
                } else {
                    ItemError.Decision.Fail
                }
            },
            errorThreshold = 5_000,
            progress = Progress.Custom(500) { done, total ->
                "заполнено $done/${total ?: "?"}, нет у партнёра ${missing.get()}"
            },
        ) { id ->
            val profile = partner.profile(id)
            if (!dryRun) repo.fill(id, profile.toJson())
        }

        log.info("заполнено ${result.successful}, нет у партнёра ${result.skipped}")
    }
}
```

## Ретрай — не здесь

`@Retry` стоит на методе HTTP-клиента, и это единственное правильное место:

- он повторяет **один вызов**, а не всю миграцию;
- он умеет классифицировать исключения и выдерживать backoff;
- он не повторит то, что уже успело записаться в вашу базу.

Ретрай на уровне миграции переделал бы всю работу заново, включая уже применённые изменения.
Библиотека поэтому ретраев не содержит вовсе.

## Классификатор внутри `ItemError.Handle`

Классификатор видит и исключение, и типизированный элемент. В примере он делает три вещи:
решает судьбу элемента, считает 404-е своим счётчиком и пишет их в отдельный файл.

Две оговорки:

- классификатор зовут **из воркеров**, все шестнадцать потоков — поэтому `AtomicLong`,
  а не `var`. `notFound.row` потокобезопасен сам;
- если сам классификатор бросит, его исключение станет основным, а исходное уедет
  в `suppressed`. Держите его простым.

Ошибка элемента попадает в `errors.csv` независимо от решения классификатора — Skip не значит
«молча».

## Сколько ставить в `parallel`

`parallel` — это размер окна одновременных элементов, и он же нижняя граница числа потоков
в пуле. Ориентиры:

- **Партнёрский rate limit важнее вашей арифметики.** Шестнадцать потоков против API
  с лимитом 50 rps дадут 429-е, а не ускорение.
- **Пул соединений HTTP-клиента должен быть не меньше.** Иначе воркеры будут ждать соединение,
  и параллелизм окажется декорацией.
- **Пул соединений к БД — тоже.** Запись `repo.fill` берёт соединение на каждый элемент.

Начните с 8, посмотрите на `rate=` в прогрессе, увеличьте вдвое, сравните. Если rate не растёт —
упёрлись не в задержку, и потоки только добавляют конкуренцию.

## Порог на сто тысяч элементов

`errorThreshold = 5_000` — это «пять процентов клиентов у партнёра отсутствуют, это ожидаемо;
десять процентов означают, что мы взяли не тот список».

Порог считает пропуски одного цикла. Пять тысяч первый пропуск валит прогон с кодом `1` —
и это правильный исход: продолжать бессмысленно.

## Репетиция

```bash
MIGRATION_RUN=PROFILE-BACKFILL-001 MIGRATION_DRY_RUN=true ./gradlew run
```

Под репетицией выполняются все сто тысяч запросов к партнёру — не выполняется только запись
в свою базу. Это дорого по времени, зато показывает настоящую долю 404-х и настоящий rate.

Если партнёрский API тарифицируется, гоняйте репетицию на срезе: временно сузьте `LIMIT`
в запросе или добавьте параметр в конфиг миграции.

## Что демонстрирует архетип

- Единственный случай, когда `parallel` оправдан, и как выбрать его значение.
- Ретрай на клиенте, а не в миграции.
- `ItemError.Handle` как классификатор ответов внешней системы.
- Потокобезопасность собственных счётчиков внутри классификатора.
