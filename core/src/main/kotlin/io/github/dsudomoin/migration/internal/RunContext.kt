package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.RunScope
import io.github.dsudomoin.migration.error.CsvFileErrorReporter
import io.github.dsudomoin.migration.internal.RunContext.Companion.internalCreate
import io.github.dsudomoin.migration.internal.RunContext.Companion.test
import io.github.dsudomoin.migration.report.ReportBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor
import java.util.concurrent.ForkJoinPool

/**
 * Стандартная реализация [RunScope]. Создаётся только через фабрики на companion-объекте:
 * [test] для юнит-тестов, [internalCreate] для runner'а.
 *
 * Хранит реестр зарегистрированных `AutoCloseable`-ресурсов (CSV-output'ы, Kafka-topic-handle и т.д.),
 * закрывает их в [closeRegistered] (вызывается runner'ом в `finally`).
 *
 * Не наследуется пользовательским кодом: конструктор `internal`.
 */
class RunContext internal constructor(
    override val dryRun: Boolean,
    override val log: Logger,
    override val report: ReportBuilder,
    val executor: Executor,
    override val outputFolder: Path,
    override val errors: CsvFileErrorReporter,
    val defaultProgressEvery: Int = 1000,
    val errorThreshold: Long = 0,
    val defaultParallel: Int = 1,
) : RunScope {

    // Флаг и очередь меняются под одним монитором: раздельные ConcurrentLinkedDeque и AtomicBoolean
    // позволяли закрытию целиком уложиться между чтением флага и добавлением в очередь, и такой
    // ресурс не закрывался никогда.
    private val stateLock = Any()
    private val resources = ArrayDeque<AutoCloseable>()
    private val sharedResources = ConcurrentHashMap<Any, AutoCloseable>()
    private var closed = false

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

    override fun <T : AutoCloseable> shared(key: Any, factory: () -> T): T {
        // После закрытия реестра не кэшируем: register ниже закроет созданное сразу, и класть
        // закрытый объект в мапу как «общий на прогон» было бы враньём.
        if (isClosed()) {
            val late = factory()
            register(late)
            return late
        }
        val created = sharedResources.computeIfAbsent(key) { factory().also { register(it) } }
        // Реестр мог закрыться, пока работала фабрика: тогда register уже закрыл объект, и держать
        // его в кэше нельзя — следующий вызов обязан получить свежий, а не мёртвый.
        if (isClosed()) sharedResources.remove(key, created)
        @Suppress("UNCHECKED_CAST")
        return created as T
    }

    override fun register(c: AutoCloseable) {
        val closeNow = synchronized(stateLock) {
            if (closed) true else { resources.addFirst(c); false }
        }
        // Закрываем вне монитора: close() может сам обратиться к реестру, и держать лок на время
        // пользовательского кода незачем.
        if (closeNow) closeOne(c, "late-registered ")
    }

    /**
     * Закрыть все зарегистрированные ресурсы в обратном порядке регистрации. Идемпотентно
     * (повторный вызов — no-op). Исключения от `close()` не пробрасываются: логируются WARN'ом
     * + добавляются в `report.warnings`. Вызывается runner'ом в `finally` после исполнения плана.
     *
     * Снимок очереди берётся одним действием вместе с установкой флага: иначе ресурс, добавленный
     * между обходом и очисткой, не закрылся бы никогда. Всё, что зарегистрируют после этого,
     * закроет сам [register].
     */
    fun closeRegistered() {
        val snapshot = synchronized(stateLock) {
            if (closed) return
            closed = true
            resources.toList().also { resources.clear() }
        }
        sharedResources.clear()
        snapshot.forEach { closeOne(it, "") }
    }

    private fun isClosed(): Boolean = synchronized(stateLock) { closed }

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
         * `openCsv`, `write`, и т.д.) без поднятия Kora-графа.
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
            defaultParallel: Int = 1,
        ): RunContext {
            val of = outputFolder ?: Files.createTempDirectory("migration-test-")
            Files.createDirectories(of)
            val report = ReportBuilder(name, author, dryRun)
            val reporter = CsvFileErrorReporter(
                name, author,
                of.resolve("errors.csv"),
                of.resolve("errors.log"),
            )
            return RunContext(
                dryRun = dryRun,
                log = LoggerFactory.getLogger("test.$name"),
                report = report,
                // ForkJoinPool.commonPool() — shared, daemon threads, no shutdown нужен. Для
                // `parallel = 1`-тестов не задействуется (стадия обходит executor).
                executor = ForkJoinPool.commonPool(),
                outputFolder = of,
                errors = reporter,
                defaultProgressEvery = defaultProgressEvery,
                errorThreshold = errorThreshold,
                defaultParallel = defaultParallel,
            )
        }

        /**
         * Низкоуровневая фабрика — принимает все зависимости явно. Используется в трёх местах:
         *
         * - [io.github.dsudomoin.migration.kora.MigrationRunner] в боевом сценарии (главный потребитель);
         * - тесты, которым нужны кастомный `Executor` / `ErrorReporter` (см. `ParallelStageTest.ctx`);
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
            defaultParallel: Int = 1,
        ): RunContext = RunContext(
            dryRun = dryRun,
            log = LoggerFactory.getLogger("io.github.dsudomoin.migration.$name"),
            report = report,
            executor = executor,
            outputFolder = outputFolder,
            errors = errors,
            defaultProgressEvery = defaultProgressEvery,
            errorThreshold = errorThreshold,
            defaultParallel = defaultParallel,
        )
    }
}
