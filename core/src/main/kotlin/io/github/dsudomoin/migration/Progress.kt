package io.github.dsudomoin.migration

/**
 * Стратегия прогресс-логирования цикла. Передаётся параметром `progress` в
 * [MigrationScope.each].
 *
 * Пишет в SLF4J на уровне INFO, то есть попадает и в `migration.log`. На исполнение не влияет
 * никак — это чистая диагностика.
 *
 * Формат зависит от того, известен ли размер источника. У коллекции он известен:
 * `progress: 300/1200 (25%)  elapsed=12s  rate=25/s`. У [Sequence] — нет, и доли не будет:
 * `progress: 300  elapsed=12s  rate=25/s`.
 *
 * **Что считается «обработанным»**: тик происходит на успехе и на Skip-аудите — то есть когда
 * элемент штатно вернулся из обработчика. На терминальном [ItemError.Fail] тика **нет**:
 * исключение пробивает наверх раньше счётчика. Значит «прошло N элементов» включает Skip'и,
 * но не последний, упавший. Чистый success-rate считайте своим `AtomicLong` в [Custom].
 */
sealed interface Progress {
    /**
     * Дефолт: каждые `migration.defaults.progressEvery` элементов (из коробки — 1000).
     *
     * На коротком цикле с известным размером шаг укорачивается сам, до примерно десяти строк
     * за цикл: иначе список на 300 элементов не дал бы ни одной строки, и человек смотрел бы
     * в тишину, гадая, не завис ли прогон.
     */
    object Default : Progress

    /** Полная тишина: у вас свой лог внутри обработчика или цикл заведомо короткий. */
    object Off : Progress

    /**
     * Логировать каждые [n] обработанных элементов (успех + skip) дефолтным форматом.
     * [n] должен быть `> 0`, иначе [IllegalArgumentException].
     */
    data class Every(val n: Int) : Progress {
        init { require(n > 0) { "Progress.Every(n) requires n > 0, got $n. Use Progress.Off to disable progress." } }
    }

    /**
     * Каждые [n] обработанных элементов вызвать [format] и записать результат. Годится, чтобы
     * подмешать в строку собственные счётчики:
     * ```
     * progress = Progress.Custom(10) { done, total ->
     *     "resync: $done/${total ?: "?"} vipUpgrades=${vips.get()}"
     * }
     * ```
     *
     * @param n период вызова, `> 0`.
     * @param format `(done, total) → строка`. `total` равен `null`, когда источник —
     *               [Sequence] или [Iterable] без известного размера.
     * @throws IllegalArgumentException если [n] не положительный.
     */
    data class Custom(val n: Int, val format: (done: Long, total: Long?) -> String) : Progress {
        init { require(n > 0) { "Progress.Custom(n) requires n > 0, got $n. Use Progress.Off to disable progress." } }
    }

    companion object {
        /** Фабрика [Custom] с периодом по умолчанию. */
        fun custom(n: Int = 1000, format: (Long, Long?) -> String): Custom = Custom(n, format)
    }
}
