package io.github.dsudomoin.migration

/**
 * Политика обработки исключения, возникшего внутри `forEach`-блока для конкретного item'а / батча.
 *
 * Передаётся параметром `onError` в `forEach`. По умолчанию — [Fail] (прервать миграцию на первой
 * ошибке).
 *
 * Все варианты, кроме [Fail], означают «продолжить миграцию»; неуспешный item уходит в
 * `errors.csv` через авто-аудитор (см. [MigrationContext.errors]).
 *
 * **Дизайн-нота про retry.** В DSL нет встроенного retry-механизма item-уровня. Item-уровневый
 * retry повторяет ВСЁ тело `forEach`-блока, что часто ломает корректность для non-idempotent
 * шагов (повторный publish в Kafka, повторный INSERT с auto-generated PK). Реальное место для
 * retry — это уровень примитива: либо Kora `@Retry` на `@HttpClient`/`@KafkaPublisher`/repository
 * методах (умеет classify + backoff из коробки), либо локальный `try/catch` вокруг конкретного
 * фейлящего вызова в теле `forEach`-блока.
 */
sealed interface OnError {

    /** Решение, возвращаемое из [Handle]-классификатора. */
    enum class Decision {
        /** Пропустить item, аудитнуть в `errors.csv`, продолжить миграцию. */
        Skip,
        /** Остановить миграцию. */
        Fail,
    }

    /** Прервать миграцию на первой ошибке. Exit-code 1. Дефолт. */
    object Fail : OnError

    /** Аудитнуть item в `errors.csv`, инкрементить `report.skipped`, продолжить. */
    object Skip : OnError

    /**
     * Кастомная классификация ошибки по типу. Решение принимается на каждый failed item на основе
     * `(throwable, item)`-пары.
     *
     * @see handle для удобной фабрики.
     */
    class Handle(val decide: (Throwable, Any?) -> Decision) : OnError

    companion object {
        /**
         * Удобная фабрика для [Handle]:
         * ```
         * onError = OnError.handle { e, _ -> when (e) {
         *     is ValidationException -> OnError.Decision.Skip
         *     else                   -> OnError.Decision.Fail
         * }}
         * ```
         */
        fun handle(decide: (Throwable, Any?) -> Decision): Handle = Handle(decide)
    }
}
