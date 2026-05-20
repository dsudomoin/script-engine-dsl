package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.Progress
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

class ProgressTicker(
    private val mode: Progress,
    private val total: Long?,
    private val clock: Clock = Clock.systemDefaultZone(),
    private val defaultEvery: Int = 1000,
    private val sink: (String) -> Unit,
) {
    private val processed = AtomicLong()
    private val startedAt: Instant = clock.instant()

    fun tick() {
        // Short-circuit под Off — даже incrementAndGet не нужен.
        if (mode === Progress.Off) return
        val n = processed.incrementAndGet()
        val every = everyFor(mode)
        if (every <= 0) return
        if (n % every != 0L) return
        sink(render(n))
    }

    private fun everyFor(m: Progress): Int = when (m) {
        // Progress.Default: на маленьких циклах с известным total дефолтный шаг (1000) даст
        // 0 тиков, и пользователь будет смотреть в тишину. Если total известен и `defaultEvery`
        // даст меньше ~10 тиков на весь цикл — уменьшаем шаг до total/10 (минимум 1). Для
        // Sequence (total = null) и больших циклов поведение прежнее.
        is Progress.Default -> {
            val t = total
            if (t != null && t < defaultEvery.toLong() * 10) {
                maxOf(1, (t / 10).toInt())
            } else defaultEvery
        }

        is Progress.Off -> 0
        is Progress.Every -> m.n     // явный шаг от пользователя — не трогаем
        is Progress.Custom -> m.n     // явный шаг от пользователя — не трогаем
    }

    private fun render(done: Long): String {
        if (mode is Progress.Custom) return mode.format(done, total)

        val elapsed = Duration.between(startedAt, clock.instant())
        val rate = if (elapsed.toMillis() > 0) done * 1000 / elapsed.toMillis() else 0
        return if (total != null) {
            val pct = done * 100 / total
            "progress: $done/$total ($pct%)  elapsed=${humanizeDuration(elapsed)}  rate=${rate}/s"
        } else {
            "progress: $done  elapsed=${humanizeDuration(elapsed)}  rate=${rate}/s"
        }
    }
}
