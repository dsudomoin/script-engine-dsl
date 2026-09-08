package io.github.dsudomoin.migration.kora

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.FileAppender
import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationExecution
import io.github.dsudomoin.migration.MigrationPrecondition
import io.github.dsudomoin.migration.RunSettings
import io.github.dsudomoin.migration.report.MigrationReport
import io.github.dsudomoin.migration.report.ReportFormatter
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.tinkoff.kora.application.graph.Lifecycle
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Collections
import java.util.IdentityHashMap
import java.util.UUID
import kotlin.system.exitProcess

/**
 * Kora [Lifecycle]-компонент, исполняющий миграцию на старте графа. Регистрируется
 * автоматически через [MigrationModule].
 *
 * Отвечает за четыре вещи: выбрать миграцию по имени, собрать [RunSettings] из HOCON,
 * подключить файловый лог и превратить исход в код возврата. Само исполнение живёт в
 * [MigrationExecution] и Kora не требует.
 *
 * Exit-коды:
 * - `0` — прогон завершён, изменения применены;
 * - `1` — исключение из тела миграции, превышенный порог ошибок или прерывание;
 *   состояние системы может быть частичным;
 * - `2` — прогон забракован до первого элемента: неизвестное имя, дублирующиеся имена,
 *   битые значения конфига, недоступный `outputFolder`, отвергнутый входной файл
 *   ([MigrationPrecondition]). Ни один эффект не выполнен.
 *
 * @param onReport инжекция для тестов: готовый отчёт до того, как он уйдёт в лог.
 * @param exit инжекция для тестов — по умолчанию `System.exit`. Стоит последним намеренно:
 *             trailing-лямбда на месте вызова обязана означать именно завершение процесса.
 */
class MigrationRunner(
    private val config: MigrationConfig,
    private val migrations: List<Migration>,
    private val onReport: (MigrationReport) -> Unit = {},
    private val exit: (Int) -> Unit = { exitProcess(it) },
) : Lifecycle {

    private val log = LoggerFactory.getLogger("io.github.dsudomoin.migration.runner")

    override fun init() {
        val name = config.run()
        if (name == null) {
            log.info("migration.run не задан — runner idle")
            return
        }

        if (!requireUniqueNames()) return
        if (!validateConfig()) return

        val migration = migrations.firstOrNull { it.name == name }
        if (migration == null) {
            val available = migrations.joinToString(", ") { it.name }
            log.error("Unknown migration '$name'. Available: [$available]")
            exit(2)
            return
        }

        // Создание папки под обработкой ошибки: битый путь в HOCON или отсутствие прав иначе
        // пробили бы init() насквозь необработанным IOException — мимо заявленных exit-кодов.
        val outputFolder = try {
            val folder = config.outputFolder()?.let { Paths.get(it) } ?: Paths.get("logs", migration.name)
            Files.createDirectories(folder)
            folder
        } catch (e: Exception) {
            log.error("Cannot create migration.outputFolder '${config.outputFolder() ?: "logs/${migration.name}"}'", e)
            exit(2)
            return
        }

        // exit(code) вызывается ПОСЛЕ возврата из execute() — то есть после того, как finally
        // отцепил FileAppender. Внутри try вызов System.exit прервал бы поток до finally, и
        // последние строки отчёта могли бы не доехать на диск.
        val code = execute(migration, outputFolder)
        exit(code)
    }

    override fun release() {}

    private fun execute(migration: Migration, outputFolder: Path): Int {
        val fileLogHandle = attachFileLogger(outputFolder)
        try {
            val outcome = MigrationExecution.execute(
                migration,
                RunSettings(
                    dryRun = config.dryRun(),
                    outputFolder = outputFolder,
                    errorThreshold = config.defaults().errorThreshold(),
                    progressEvery = config.defaults().progressEvery(),
                    maxItemReprLength = config.errorReporting().maxItemReprLength(),
                    includeStackTrace = config.errorReporting().includeStackTrace(),
                ),
            )
            onReport(outcome.report)
            log.info("\n" + ReportFormatter(config.report().asciiOnly()).format(outcome.report))

            val failure = outcome.failure ?: return 0
            // Счётчик обработанных — часть условия, а не перестраховка: маркер обещает только
            // то, что бросили на предусловии, а вызвать такое можно и после работы. Тогда
            // состояние уже частичное, и код 2 («ничего не сделано») был бы враньём.
            if (failure is MigrationPrecondition && outcome.report.processed == 0L) {
                log.error("Migration ${migration.name} REJECTED before start", failure)
                return 2
            }
            if (isInterruption(failure)) {
                // Прерванный одноразовый процесс не завершился успешно: часть работы не сделана.
                Thread.currentThread().interrupt()
                log.error("Migration ${migration.name} was INTERRUPTED", failure)
            } else {
                log.error("Migration ${migration.name} FAILED", failure)
            }
            return 1
        } finally {
            fileLogHandle?.close()
        }
    }

    /**
     * Sanity-check на значения HOCON-конфига: генератор Kora не покрывает «> 0»-семантику,
     * а мисконфиг иначе выявляется глубоко — уже после того, как создана папка артефактов.
     */
    private fun validateConfig(): Boolean {
        val errors = mutableListOf<String>()
        if (config.defaults().progressEvery() <= 0) {
            errors += "migration.defaults.progressEvery must be > 0, got ${config.defaults().progressEvery()}"
        }
        if (config.defaults().errorThreshold() < 0) {
            errors += "migration.defaults.errorThreshold must be >= 0 (0 = disabled), got ${config.defaults().errorThreshold()}"
        }
        if (config.errorReporting().maxItemReprLength() <= 0) {
            errors += "migration.errorReporting.maxItemReprLength must be > 0, got ${config.errorReporting().maxItemReprLength()}"
        }
        if (errors.isNotEmpty()) {
            log.error("Invalid migration config:\n  - ${errors.joinToString("\n  - ")}")
            exit(2)
            return false
        }
        return true
    }

    private fun requireUniqueNames(): Boolean {
        val dups = migrations.groupingBy { it.name }.eachCount().filter { it.value > 1 }
        if (dups.isNotEmpty()) {
            log.error("Duplicate migration names: ${dups.keys}")
            exit(2)
            return false
        }
        return true
    }

    // InterruptedException приезжает завёрнутым: параллельный цикл вешает остальные ошибки в
    // suppressed. Идентичность в множестве — защита от циклов cause.
    private fun isInterruption(e: Throwable): Boolean {
        val seen = Collections.newSetFromMap(IdentityHashMap<Throwable, Boolean>())
        fun walk(t: Throwable?): Boolean {
            if (t == null || !seen.add(t)) return false
            if (t is InterruptedException) return true
            return walk(t.cause) || t.suppressed.any { walk(it) }
        }
        return walk(e)
    }

    /**
     * Подключить файловый аппендер, если на classpath есть Logback.
     *
     * Обёртка вокруг [attachLogbackFileAppender] нужна из-за неочевидного: ветка «Logback нет»
     * сама ссылается на его классы. Проверка `factory !is LoggerContext` компилируется в
     * instanceof, и JVM обязана разрешить ссылку на класс из constant pool. С другой
     * реализацией slf4j это `NoClassDefFoundError`, который пробивает `init()` насквозь:
     * миграция не стартует вообще вместо работы без `migration.log`.
     */
    private fun attachFileLogger(outputFolder: Path): AutoCloseable? =
        try {
            attachLogbackFileAppender(outputFolder)
        } catch (e: LinkageError) {
            log.warn("migration.log file output disabled: Logback not on classpath (${e.javaClass.simpleName})")
            null
        }

    private fun attachLogbackFileAppender(outputFolder: Path): AutoCloseable? {
        val factory = LoggerFactory.getILoggerFactory()
        if (factory !is LoggerContext) {
            log.warn("migration.log file output disabled: Logback not on classpath (found ${factory.javaClass.name})")
            return null
        }
        val logFile = outputFolder.resolve("migration.log").toString()
        val encoder = PatternLayoutEncoder().apply {
            context = factory
            pattern = "%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
            start()
        }
        val appender = FileAppender<ILoggingEvent>().apply {
            context = factory
            name = "migration-file-" + UUID.randomUUID()
            file = logFile
            isAppend = false
            this.encoder = encoder
            start()
        }
        val rootLogger = factory.getLogger(Logger.ROOT_LOGGER_NAME)
        rootLogger.addAppender(appender)
        return AutoCloseable {
            rootLogger.detachAppender(appender.name)
            appender.stop()
            encoder.stop()
        }
    }
}
