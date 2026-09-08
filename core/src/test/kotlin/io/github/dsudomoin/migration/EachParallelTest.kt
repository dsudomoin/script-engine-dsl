package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.internal.MigrationRun
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class EachParallelTest {

    @TempDir
    lateinit var tmp: Path

    @Test
    fun `обрабатывает все элементы в нескольких потоках`() {
        val threads = ConcurrentHashMap.newKeySet<String>()
        val processed = AtomicInteger()
        val migration = object : Migration("PARALLEL") {
            override fun MigrationScope.run() {
                each((1..100).toList(), parallel = 4) {
                    threads += Thread.currentThread().name
                    processed.incrementAndGet()
                }
            }
        }
        val run = MigrationRun.forTest(outputFolder = tmp)

        val result = run.executeBodyAndClose(migration)

        assertThat(processed.get()).isEqualTo(100)
        assertThat(threads.size).isGreaterThan(1)
        assertThat(threads).allSatisfy { assertThat(it).startsWith("migration-test-") }
        assertThat(run.report.build().processed).isEqualTo(100)
        assertThat(result).isNull()
    }

    @Test
    fun `окно in-flight не превышает parallel`() {
        val inFlight = AtomicInteger()
        val peak = AtomicInteger()
        val migration = object : Migration("WINDOW") {
            override fun MigrationScope.run() {
                each((1..50).toList(), parallel = 3) {
                    val now = inFlight.incrementAndGet()
                    peak.updateAndGet { prev -> maxOf(prev, now) }
                    Thread.sleep(5)
                    inFlight.decrementAndGet()
                }
            }
        }
        val run = MigrationRun.forTest(outputFolder = tmp)

        run.executeBodyAndClose(migration)

        assertThat(peak.get()).isLessThanOrEqualTo(3)
    }

    @Test
    fun `без parallel работа идёт в вызывающем потоке и пул не создаётся`() {
        val seen = ConcurrentHashMap.newKeySet<String>()
        val migration = object : Migration("NO-POOL") {
            override fun MigrationScope.run() {
                each((1..20).toList()) { seen += Thread.currentThread().name }
            }
        }
        val run = MigrationRun.forTest(outputFolder = tmp)

        run.executeBodyAndClose(migration)

        assertThat(seen).containsExactly(Thread.currentThread().name)
    }

    @Test
    fun `уже запущенные воркеры доводятся до конца после ошибки`() {
        val active = AtomicInteger()
        val migration = object : Migration("DRAIN") {
            override fun MigrationScope.run() {
                each((1..40).toList(), parallel = 4) { item ->
                    active.incrementAndGet()
                    try {
                        if (item == 5) error("boom")
                        Thread.sleep(20)
                    } finally {
                        active.decrementAndGet()
                    }
                }
            }
        }
        val run = MigrationRun.forTest(outputFolder = tmp)

        val failure = run.executeBodyAndClose(migration)

        // Цикл не имеет права вернуться, пока хотя бы один воркер ещё работает:
        // иначе он писал бы в уже закрытые ресурсы прогона.
        assertThat(active.get()).isZero()
        assertThat(failure).isInstanceOf(IllegalStateException::class.java)
        assertThat(failure).hasMessage("boom")
    }

    @Test
    fun `Skip в параллельном цикле сохраняет тождество счётчиков`() {
        var result: EachResult? = null
        val migration = object : Migration("PARALLEL-SKIP") {
            override fun MigrationScope.run() {
                result = each((1..60).toList(), parallel = 4, onItemError = ItemError.Skip) {
                    if (it % 5 == 0) error("boom")
                }
            }
        }
        val run = MigrationRun.forTest(outputFolder = tmp)

        run.executeBodyAndClose(migration)

        val r = result!!
        assertThat(r.processed).isEqualTo(60)
        assertThat(r.skipped).isEqualTo(12)
        assertThat(r.successful).isEqualTo(48)
        assertThat(r.processed).isEqualTo(r.successful + r.skipped + r.failed)
    }

    @Test
    fun `parallel ноль отвергается`() {
        val migration = object : Migration("BAD-PARALLEL") {
            override fun MigrationScope.run() {
                each(listOf(1), parallel = 0) { }
            }
        }
        val run = MigrationRun.forTest(outputFolder = tmp)

        assertThatThrownBy { run.executeBody(migration) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("parallel must be > 0")
    }

    /**
     * Запускает тело и закрывает ресурсы прогона (в том числе пул), возвращая ошибку
     * тела вместо её проброса — чтобы проверять состояние после упавшего прогона.
     */
    private fun MigrationRun.executeBodyAndClose(migration: Migration): Throwable? {
        val failure = runCatching { executeBody(migration) }.exceptionOrNull()
        closeRegistered()
        return failure
    }
}
