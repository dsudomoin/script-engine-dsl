package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.error.CsvFileErrorReporter
import io.github.dsudomoin.migration.internal.DefaultMigrationContext.Companion.internalCreate
import io.github.dsudomoin.migration.internal.DefaultMigrationContext.Companion.test
import io.github.dsudomoin.migration.report.ReportBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Executor
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Стандартная реализация [MigrationContext]. Создаётся только через фабрики на companion-объекте:
 * [test] для юнит-тестов, [internalCreate] для runner'а.
 *
 * Хранит реестр зарегистрированных `AutoCloseable`-ресурсов (CSV-output'ы, Kafka-topic-handle и т.д.),
 * закрывает их в [closeRegistered] (вызывается runner'ом в `finally`).
 *
 * Не наследуется пользовательским кодом: конструктор `internal`.
 */
class DefaultMigrationContext internal constructor(
    override val dryRun: Boolean,
    override val log: Logger,
    override val report: ReportBuilder,
    override val executor: Executor,
    override val outputFolder: Path,
    override val errors: CsvFileErrorReporter,
    override val defaultProgressEvery: Int = 1000,
    override val errorThreshold: Long = 0,
) : MigrationContext {

    private val resources = ConcurrentLinkedDeque<AutoCloseable>()
    private val closed = AtomicBoolean(false)

    override fun <R> guardWrite(label: String, args: Map<String, Any?>, dryRunDefault: R, action: () -> R): R {
        if (dryRun) {
            log.info("[DRY-RUN] $label ${renderArgs(args)}")
            report.incDryRunSkipped(label)
            return dryRunDefault
        }
        return action()
    }

    override fun guardWrite(label: String, args: Map<String, Any?>, action: () -> Unit) {
        // Делегируем в R-перегрузку с Unit, чтобы избежать дублирования dry-run-логики.
        guardWrite(label, args, dryRunDefault = Unit) { action() }
    }

    override fun auditError(e: Throwable, item: Any?) {
        errors.report(e, item)
    }

    override fun register(c: AutoCloseable) {
        if (closed.get()) {
            closeOne(c, "late-registered ")
            return
        }
        resources.addFirst(c)
    }

    /**
     * Закрыть все зарегистрированные ресурсы в обратном порядке регистрации. Идемпотентно
     * (повторный вызов — no-op). Исключения от `close()` не пробрасываются: логируются WARN'ом
     * + добавляются в `report.warnings`. Вызывается runner'ом в `finally` после `migrate()`.
     */
    fun closeRegistered() {
        if (!closed.compareAndSet(false, true)) return
        val iter = resources.iterator()
        while (iter.hasNext()) {
            closeOne(iter.next(), "")
        }
        resources.clear()
    }

    private fun closeOne(c: AutoCloseable, prefix: String) {
        try {
            c.close()
        } catch (e: Throwable) {
            log.warn("${prefix}resource close failed: $c", e)
            report.addWarning("${prefix}resource close failed: $c (${e.javaClass.simpleName}: ${e.message ?: "no message"})")
        }
    }

    private fun renderArgs(args: Map<String, Any?>): String =
        if (args.isEmpty()) "" else args.entries.joinToString(prefix = "(", postfix = ")") { "${it.key}=${it.value}" }

    companion object {
        /**
         * Тестовая фабрика — для написания юнит-тестов на ops-расширения (`jdbc`, `cassandra`,
         * `openCsv`, `mutation`, и т.д.) без поднятия Kora-графа.
         *
         * Создаёт `CsvFileErrorReporter` + `ReportBuilder`, `Executor` = same-thread (sequential),
         * `log` = slf4j логгер `test.<name>`.
         *
         * @param outputFolder если `null`, создаётся свежая папка в системной tmp.
         */
        fun test(
            dryRun: Boolean = false,
            name: String = "test",
            author: String = "test",
            outputFolder: Path? = null,
            defaultProgressEvery: Int = 1000,
            errorThreshold: Long = 0,
        ): DefaultMigrationContext {
            val of = outputFolder ?: Files.createTempDirectory("migration-test-")
            Files.createDirectories(of)
            val report = ReportBuilder(name, author, dryRun)
            val reporter = CsvFileErrorReporter(
                name, author,
                of.resolve("errors.csv"),
                of.resolve("errors.log"),
            )
            return DefaultMigrationContext(
                dryRun = dryRun,
                log = LoggerFactory.getLogger("test.$name"),
                report = report,
                // ForkJoinPool.commonPool() — shared, daemon threads, no shutdown нужен. Для
                // `parallel = 1`-тестов не задействуется (forEach обходит executor).
                executor = ForkJoinPool.commonPool(),
                outputFolder = of,
                errors = reporter,
                defaultProgressEvery = defaultProgressEvery,
                errorThreshold = errorThreshold,
            )
        }

        /**
         * Низкоуровневая фабрика — принимает все зависимости явно. Используется в трёх местах:
         *
         * - [io.github.dsudomoin.migration.kora.MigrationRunner] в боевом сценарии (главный потребитель);
         * - тесты, которым нужны кастомный `Executor` / `ErrorReporter` (см. `ForEachTest.customCtx`);
         * - пользовательский код, который хочет нестандартный `ErrorReporter` (Sentry, Kibana,
         *   JSON Lines) — на уровне приложения, см. `docs/examples/customization.md`.
         *
         * Для обычных юнит-тестов проще использовать [test], которая собирает дефолтные
         * `ReportBuilder` + `CsvFileErrorReporter` + `ForkJoinPool.commonPool()` за тебя.
         *
         * `public` из-за пакета `internal` Kotlin-видимости: kora-модуль и пользовательские
         * extension'ы не видят `internal`-членов core-модуля.
         */
        fun internalCreate(
            dryRun: Boolean,
            name: String,
            report: ReportBuilder,
            executor: Executor,
            outputFolder: Path,
            errors: CsvFileErrorReporter,
            defaultProgressEvery: Int = 1000,
            errorThreshold: Long = 0,
        ): DefaultMigrationContext = DefaultMigrationContext(
            dryRun = dryRun,
            log = LoggerFactory.getLogger("io.github.dsudomoin.migration.$name"),
            report = report,
            executor = executor,
            outputFolder = outputFolder,
            errors = errors,
            defaultProgressEvery = defaultProgressEvery,
            errorThreshold = errorThreshold,
        )
    }
}
