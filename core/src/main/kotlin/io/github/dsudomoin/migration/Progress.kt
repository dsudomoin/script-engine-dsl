package io.github.dsudomoin.migration

/**
 * Стратегия прогресс-логирования стадии. Передаётся параметром `progress` в `source`/`scoped`.
 *
 * Логгер пишет в SLF4J на уровне INFO; формат различается по варианту. Прогресс никак не влияет
 * на исполнение — это чистая диагностика.
 *
 * **Что считается «обработанным»**: тикает на success'е и на Skip-аудите (item штатно вернулся
 * из тела обработчика). На терминальном `ItemError.Fail`-исключении тика **не происходит** —
 * исключение пробивает наверх до того, как тикнет счётчик. То есть «прошлось N задач»,
 * включая SKIP-ы, но не самый последний fail-item. Для чистого success-rate'а считай его в
 * `Progress.Custom` через свой `AtomicLong`.
 */
sealed interface Progress {
    /** Авто-режим: каждые ~1000 обработанных item'ов, формат `"progress: X/Y (Z%)"`. Дефолт. */
    object Default : Progress

    /** Полная тишина. Уместно когда у тебя свой прогресс-лог в `logEach` или ты просто не хочешь шума. */
    object Off : Progress

    /**
     * Логировать каждые [n] **обработанных** item'ов (success + skip), дефолтным форматом.
     * [n] должен быть > 0; иначе [IllegalArgumentException].
     */
    data class Every(val n: Int) : Progress {
        init { require(n > 0) { "Progress.Every(n) requires n > 0, got $n. Use Progress.Off to disable progress." } }
    }

    /**
     * Каждые [n] **обработанных** item'ов вызывать [format] и логировать результат. Можно тянуть
     * внешний state (атомики счётчиков и т.д.) для составного лога:
     * ```
     * progress = Progress.Custom(10) { done, total ->
     *     "resync: batches=$done/${total ?: "?"} vipUpgrades=${vips.get()}"
     * }
     * ```
     *
     * @param n период вызова. Тикает на каждом `n`-ом обработанном item'е (success или skip;
     *          терминальный Fail сам аборт-сигнал и тик не доходит). Должен быть `> 0`.
     * @param format `(done, total) → строка`. `total` = `null` для `Sequence`-источников (неизвестно).
     * @throws IllegalArgumentException если [n] не положительный.
     */
    data class Custom(val n: Int, val format: (done: Long, total: Long?) -> String) : Progress {
        init { require(n > 0) { "Progress.Custom(n) requires n > 0, got $n. Use Progress.Off to disable progress." } }
    }

    companion object {
        /** Удобная фабрика для [Custom] с дефолтом периода. */
        fun custom(n: Int = 1000, format: (Long, Long?) -> String): Custom = Custom(n, format)
    }
}
