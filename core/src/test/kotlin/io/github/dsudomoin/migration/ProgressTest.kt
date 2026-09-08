package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.internal.ProgressTicker
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

class ProgressTest {
    private fun fixedClock(now: Instant) = Clock.fixed(now, ZoneOffset.UTC)

    @Test
    fun `Default срабатывает каждые 1000 элементов`() {
        val lines = mutableListOf<String>()
        val ticker = ProgressTicker(Progress.Default, total = 10000, clock = fixedClock(Instant.EPOCH)) { lines += it }

        repeat(1000) { ticker.tick() }
        assertThat(lines).hasSize(1)

        repeat(1000) { ticker.tick() }
        assertThat(lines).hasSize(2)
    }

    @Test
    fun `Off не логирует вообще`() {
        val lines = mutableListOf<String>()
        val ticker = ProgressTicker(Progress.Off, total = null, clock = fixedClock(Instant.EPOCH)) { lines += it }
        repeat(5000) { ticker.tick() }
        assertThat(lines).isEmpty()
    }

    @Test
    fun `формат с total показывает процент`() {
        val lines = mutableListOf<String>()
        val ticker = ProgressTicker(Progress.Every(100), total = 1000, clock = fixedClock(Instant.EPOCH)) { lines += it }
        repeat(100) { ticker.tick() }
        assertThat(lines[0]).contains("progress: 100/1000", "10%")
    }

    @Test
    fun `формат без total опускает процент`() {
        val lines = mutableListOf<String>()
        val ticker = ProgressTicker(Progress.Every(50), total = null, clock = fixedClock(Instant.EPOCH)) { lines += it }
        repeat(50) { ticker.tick() }
        assertThat(lines[0]).contains("progress: 50").doesNotContain("%")
    }

    @Test
    fun `Custom с total = null - format получает null`() {
        val lines = mutableListOf<String>()
        val ticker = ProgressTicker(
            Progress.Custom(10) { done, tot -> "done=$done of ${tot ?: "?"}" },
            total = null,
            clock = fixedClock(Instant.EPOCH),
        ) { lines += it }

        repeat(10) { ticker.tick() }
        assertThat(lines).containsExactly("done=10 of ?")
    }

    @Test
    fun `defaultEvery параметр переопределяет дефолт 1000`() {
        val lines = mutableListOf<String>()
        val ticker = ProgressTicker(
            Progress.Default,
            total = 100,
            clock = fixedClock(Instant.EPOCH),
            defaultEvery = 7,
        ) { lines += it }
        repeat(21) { ticker.tick() }
        // tick'и 7, 14, 21 → 3 строки
        assertThat(lines).hasSize(3)
    }

    @Test
    fun `custom форматтер используется вместо стандартного`() {
        val lines = mutableListOf<String>()
        val ticker = ProgressTicker(
            Progress.Custom(100) { done, tot -> "done=$done of ${tot ?: "?"}" },
            total = 200,
            clock = fixedClock(Instant.EPOCH),
        ) { lines += it }

        repeat(100) { ticker.tick() }
        assertThat(lines[0]).isEqualTo("done=100 of 200")
    }

    @Test
    fun `Default адаптируется на маленьких циклах - total меньше defaultEvery x10`() {
        // total = 100, defaultEvery = 1000. Без адаптации — 0 тиков за весь цикл.
        // Адаптированный шаг: max(1, 100 / 10) = 10 → ~10 тиков.
        val lines = mutableListOf<String>()
        val ticker = ProgressTicker(
            Progress.Default,
            total = 100,
            clock = fixedClock(Instant.EPOCH),
            defaultEvery = 1000,
        ) { lines += it }

        repeat(100) { ticker.tick() }

        assertThat(lines).hasSize(10)
        assertThat(lines[0]).contains("progress: 10/100", "10%")
        assertThat(lines.last()).contains("progress: 100/100", "100%")
    }

    @Test
    fun `Default адаптируется не ниже 1 для очень маленького total`() {
        // total = 3, defaultEvery = 1000. Адаптированный шаг: max(1, 3 / 10) = max(1, 0) = 1.
        val lines = mutableListOf<String>()
        val ticker = ProgressTicker(
            Progress.Default,
            total = 3,
            clock = fixedClock(Instant.EPOCH),
            defaultEvery = 1000,
        ) { lines += it }

        repeat(3) { ticker.tick() }

        assertThat(lines).hasSize(3)
    }

    @Test
    fun `Default не адаптируется при total = null (Sequence)`() {
        // Sequence-источник: total неизвестен, адаптация невозможна — остаётся дефолтный шаг.
        val lines = mutableListOf<String>()
        val ticker = ProgressTicker(
            Progress.Default,
            total = null,
            clock = fixedClock(Instant.EPOCH),
            defaultEvery = 1000,
        ) { lines += it }

        repeat(999) { ticker.tick() }
        assertThat(lines).isEmpty()

        ticker.tick()    // 1000-й
        assertThat(lines).hasSize(1)
    }

    @Test
    fun `Default не адаптируется когда total больше defaultEvery x10`() {
        // total = 10000, defaultEvery = 1000. 10000 >= 1000*10 → стандартное поведение.
        val lines = mutableListOf<String>()
        val ticker = ProgressTicker(
            Progress.Default,
            total = 10000,
            clock = fixedClock(Instant.EPOCH),
            defaultEvery = 1000,
        ) { lines += it }

        repeat(10000) { ticker.tick() }

        // 10 тиков по defaultEvery=1000, без адаптации
        assertThat(lines).hasSize(10)
        assertThat(lines[0]).contains("progress: 1000/10000")
    }
}

/**
 * Прогресс со стороны миграции: `total` попадает в тикер только у коллекции.
 *
 * Проверяется через [Progress.Custom] — единственный публичный способ увидеть, что движок
 * передал тикеру. Раньше `each` всегда передавал `null`, и вся работа тикера с долей и
 * укороченным шагом была мертва в бою, хотя и покрыта юнит-тестами.
 */
class ProgressTotalTest {

    @org.junit.jupiter.api.io.TempDir
    lateinit var tmp: java.nio.file.Path

    private fun totalSeenBy(body: MigrationScope.(Progress) -> Unit): Long? {
        var seen: Long? = -1L
        val probe = Progress.Custom(1) { _, total -> seen = total; "tick" }
        val migration = object : Migration("PROGRESS-TOTAL") {
            override fun MigrationScope.run() = body(probe)
        }
        assertThat(MigrationTest.run(migration, outputFolder = tmp).failure).isNull()
        return seen
    }

    @Test
    fun `цикл по коллекции знает её размер`() {
        assertThat(totalSeenBy { p -> each(listOf(1, 2, 3), progress = p) { } }).isEqualTo(3L)
    }

    @Test
    fun `у Sequence размера нет`() {
        assertThat(totalSeenBy { p -> each(sequenceOf(1, 2, 3), progress = p) { } }).isNull()
    }

    @Test
    fun `у Iterable без размера тоже нет`() {
        val notACollection = Iterable { listOf(1, 2, 3).iterator() }
        assertThat(totalSeenBy { p -> each(notACollection, progress = p) { } }).isNull()
    }
}
