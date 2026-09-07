package io.github.dsudomoin.migration.kora

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.FileAppender
import io.github.dsudomoin.migration.MigrationDefinition
import io.github.dsudomoin.migration.MigrationPlan
import io.github.dsudomoin.migration.ScriptPolicy
import io.github.dsudomoin.migration.error.CsvFileErrorReporter
import io.github.dsudomoin.migration.internal.RunContext
import io.github.dsudomoin.migration.internal.PlanInterpreter
import io.github.dsudomoin.migration.report.MigrationReport
import io.github.dsudomoin.migration.report.ReportBuilder
import io.github.dsudomoin.migration.report.ReportFormatter
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.tinkoff.kora.application.graph.Lifecycle
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.exitProcess

/**
 * Kora [Lifecycle]-компонент, исполняющий миграцию на старте графа. Регистрируется
 * автоматически через [MigrationModule] — пользователю напрямую инстанцировать не нужно
 * (тестовый конструктор с `exit`-callback'ом существует для unit-тестов).
 *
 * Поведение `init()`:
 * 1. Если `config.run() == null` — runner idle, возврат без действий.
 * 2. Проверка уникальности имён [Migration]'ов в графе → exit 2 при дубликате.
 * 3. Lookup миграции по имени → exit 2 при отсутствии.
 * 4. Создание `outputFolder`, attach logback `FileAppender` к root-логгеру.
 * 5. Создание [io.github.dsudomoin.migration.error.CsvFileErrorReporter], [io.github.dsudomoin.migration.report.ReportBuilder],
 *    `Executor` (из `@Tag(MigrationExecutor)` или дефолт-FixedThreadPool).
 * 6. Исполнение плана интерпретатором в try/catch (политика по [MigrationPlan.onUnhandled]).
 * 7. В `finally`: `ctx.closeRegistered()` (все user-registered ресурсы), потом reporter.close().
 * 8. Печать отчёта в лог, exit с кодом 0 / 1 / 2.
 *
 * Exit-коды:
 * - `0` — success.
 * - `1` — `FAIL_FAST` от unhandled-исключения; превышенный `errorThreshold` фазы; любой отказ
 *   асинхронного эффекта (провалившийся, неподтверждённый или зарегистрированный после барьера);
 *   прерывание прогона. Последние два — независимо от [ScriptPolicy]: `LOG_AND_COMPLETE` покрывает
 *   допустимые отказы скрипта, но не нарушение гарантии доставки.
 * - `2` — misconfiguration: unknown name, duplicate name, битый `outputFolder`, ошибка построения
 *   плана, расхождение [MigrationDefinition.name] и имени плана.
 *
 * @param customExecutor если не `null`, используется вместо дефолт-FixedThreadPool.
 * @param onReport инжекция для тестов: готовый отчёт до того, как он уйдёт в лог.
 * @param exit инжекция для тестов — по умолчанию `System.exit`. Стоит последним намеренно:
 *             trailing-лямбда на месте вызова обязана означать именно завершение процесса.
 */
class MigrationRunner(
    private val config: MigrationConfig,
    private val definitions: List<MigrationDefinition>,
    private val customExecutor: Executor?,
    private val onReport: (MigrationReport) -> Unit = {},
    private val exit: (Int) -> Unit = { exitProcess(it) },
) : Lifecycle {

    private val log = LoggerFactory.getLogger("io.github.dsudomoin.migration.runner")

    private companion object {
        const val SHUTDOWN_WAIT_SECONDS = 30L
    }

    override fun init() {
        val name = config.run()
        if (name == null) {
            log.info("migration.run не задан — runner idle")
            return
        }

        if (!requireUniqueNames()) return
        if (!validateConfig()) return

        val definition = definitions.firstOrNull { it.name == name }
        if (definition == null) {
            val available = definitions.joinToString(", ") { it.name }
            log.error("Unknown migration '$name'. Available: [$available]")
            exit(2)
            return
        }

        // Создание папки вынесено под обработку ошибки: битый путь в HOCON или отсутствие прав
        // иначе пробили бы init() насквозь необработанным IOException — мимо заявленных exit-кодов.
        val outputFolder = try {
            val folder = config.outputFolder()?.let { Paths.get(it) } ?: Paths.get("logs", definition.name)
            Files.createDirectories(folder)
            folder
        } catch (e: Exception) {
            log.error("Cannot create migration.outputFolder '${config.outputFolder() ?: "logs/${definition.name}"}'", e)
            exit(2)
            return
        }

        // Важно: exit(code) вызывается ПОСЛЕ возврата из executeMigration() — то есть после
        // того, как finally закрыл fileLogHandle (детач + stop FileAppender'а). Если бы exit
        // стоял внутри try, System.exit прервал бы текущий thread до finally, и последние
        // строки отчёта могли бы не доехать на диск (FileAppender буферизирован).
        val code = executeMigration(definition, outputFolder)
        exit(code)
    }

    private fun executeMigration(definition: MigrationDefinition, outputFolder: Path): Int {
        val errorsFile = outputFolder.resolve("errors.csv")
        val traceFile = outputFolder.resolve("errors.log")
        val plan = try {
            definition.plan()
        } catch (e: Exception) {
            log.error("Cannot build plan of migration '${definition.name}'", e)
            return 2
        }

        // Выбор миграции, папка артефактов и проверка дублей идут по definition.name, а отчёт и
        // контекст логов — по plan.name. Разъехавшиеся имена означают, что запустили не то, что
        // назвали: это мисконфиг, а не деталь оформления.
        if (plan.name != definition.name) {
            log.error(
                "Migration '${definition.name}' builds a plan named '${plan.name}': " +
                    "MigrationDefinition.name and migration(name = ...) must match",
            )
            return 2
        }

        // Reporter создаёт файлы лениво, поэтому чистый прогон в той же папке иначе унаследовал бы
        // errors.csv прошлого запуска и приложил бы его к своему отчёту. Один прогон — один свежий
        // набор артефактов, как у migration.log (FileAppender.isAppend = false).
        try {
            val stale = listOf(errorsFile, traceFile).count { Files.deleteIfExists(it) }
            if (stale > 0) log.info("removed $stale stale error artifact(s) left by a previous run")
        } catch (e: Exception) {
            log.error("Cannot clear stale error artifacts in '$outputFolder'", e)
            return 2
        }

        val report = ReportBuilder(plan.name, plan.author, config.dryRun())
        val fileLogHandle = attachFileLogger(outputFolder, report)
        try {
            val reporter = CsvFileErrorReporter(
                plan.name, plan.author,
                errorsFile, traceFile,
                maxItemReprLength = config.errorReporting().maxItemReprLength(),
                includeStackTrace = config.errorReporting().includeStackTrace(),
            )
            // Cached, а не fixed: реальный параллелизм задаёт `source(parallel = N)` через
            // свой семафор, и пул обязан уметь выдать N потоков — иначе `parallel` был бы
            // декорацией. Потоки daemon и переиспользуются, простаивающие отмирают сами.
            val threadNo = AtomicInteger()
            val executor = customExecutor ?: Executors.newCachedThreadPool { r ->
                Thread(r, "migration-${plan.name}-${threadNo.incrementAndGet()}").apply { isDaemon = true }
            }
            val ctx = RunContext.internalCreate(
                dryRun = config.dryRun(),
                name = plan.name,
                report = report,
                executor = executor,
                outputFolder = outputFolder,
                errors = reporter,
                defaultProgressEvery = config.defaults().progressEvery(),
                errorThreshold = config.defaults().errorThreshold(),
                defaultParallel = config.defaults().parallel(),
            )
            // Дефолтный пул owned runner'ом — регистрируем shutdown как AutoCloseable, чтобы
            // ctx.closeRegistered() (в finally ниже) его остановил. Custom executor лежит на
            // ответственности пользователя — не трогаем.
            if (customExecutor == null && executor is ExecutorService) {
                ctx.register(AutoCloseable { shutdownPool(executor, report) })
            }

            var code = try {
                runMigration(plan, ctx)
            } finally {
                // Закрываем в обратном порядке создания: сначала юзер-ресурсы (CSV, Kafka-topic
                // handle, executor) через ctx, потом — наш auto-аудитор. Если что-то в блоке
                // post-mortem рендеринга упадёт (теоретически — OOM в report.build/format),
                // reporter всё равно flush'нется и file descriptors отдадутся.
                try {
                    ctx.closeRegistered()
                } finally {
                    reporter.close()
                }
            }
            // CsvFileErrorReporter создаёт файлы лениво (только на первой ошибке). Если миграция
            // прошла без SKIP-ок — файлов нет; не показываем «Error details: <путь>» с дохлым
            // путём в финальном отчёте.
            // Забытый `write { }` — самый дорогой тихий промах библиотеки: под dry-run прямой
            // вызов репозитория или паблишера мимо гейта выполняется по-настоящему. Наблюдаемый
            // признак ровно один — пустой breakdown при непустом processed. Делаем его громким.
            if (config.dryRun() && report.processedCount() > 0 && report.noWritesGated()) {
                val msg = "DRY-RUN processed ${report.processedCount()} item(s) but intercepted 0 writes. " +
                    "If this migration writes anything, those writes went through FOR REAL — " +
                    "wrap typed client calls in write(\"label\") { ... }"
                log.warn(msg)
                report.addWarning(msg)
            }

            // Отказы доставки прилетают асинхронно и учитываются в момент flush'а продюсера,
            // то есть уже после того, как runMigration вернул код. Прогон, потерявший сообщения,
            // не имеет права закончиться нулём — ни при какой ScriptPolicy: LOG_AND_COMPLETE
            // покрывает допустимые отказы скрипта, а не нарушение гарантии доставки.
            val effectFailures = report.effectFailureCount()
            if (effectFailures > 0 && code == 0) {
                log.error("$effectFailures async effect(s) failed or were never confirmed; see the report")
                code = 1
            }

            val errorsFileOrNull = errorsFile.takeIf { Files.exists(it) }
            val traceFileOrNull = traceFile.takeIf { Files.exists(it) }
            val built = report.build(errorsFileOrNull, traceFileOrNull)
            onReport(built)
            log.info("\n" + ReportFormatter(config.report().asciiOnly()).format(built))
            return code
        } finally {
            fileLogHandle?.close()
        }
    }

    override fun release() {}

    /**
     * Останов собственного пула. `shutdown()` + ожидание вместо голого `shutdownNow()`: к этому
     * моменту план уже исполнен, но асинхронные хвосты (callback'и продюсера, задачи,
     * досылаемые из пользовательских ресурсов при закрытии) ещё могут доигрывать. Если за
     * [SHUTDOWN_WAIT_SECONDS] пул не встал — гасим принудительно и поднимаем это в отчёт,
     * иначе потеря задач осталась бы невидимой.
     */
    private fun shutdownPool(pool: ExecutorService, report: ReportBuilder) {
        pool.shutdown()
        val terminated = try {
            pool.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            false
        }
        if (!terminated) {
            val dropped = pool.shutdownNow().size
            val msg = "migration thread pool did not terminate in ${SHUTDOWN_WAIT_SECONDS}s; " +
                "$dropped queued task(s) dropped"
            log.warn(msg)
            report.addWarning(msg)
        }
    }

    /**
     * Sanity-check на значения HOCON-конфига. `data class`-валидация Kora не покрывает «> 0»-
     * семантику, поэтому проверяем вручную и валим с exit 2 + понятным сообщением, ссылаясь
     * на конкретный HOCON-ключ. Без этой проверки мисконфиг выявляется глубоко — например,
     * `Executors.newFixedThreadPool(0)` бросит `IllegalArgumentException` уже после того, как
     * runner создал `outputFolder` и подключил file-логгер.
     */
    private fun validateConfig(): Boolean {
        val errors = mutableListOf<String>()
        if (config.defaults().parallel() <= 0) {
            errors += "migration.defaults.parallel must be > 0, got ${config.defaults().parallel()}"
        }
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
        val dups = definitions.groupingBy { it.name }.eachCount().filter { it.value > 1 }
        if (dups.isNotEmpty()) {
            log.error("Duplicate migration names: ${dups.keys}")
            exit(2)
            return false
        }
        return true
    }

    /**
     * Порог ошибок принадлежит фазе и проверяется интерпретатором в реальном времени
     * ([io.github.dsudomoin.migration.internal.ErrorThresholdExceeded] валит стадию), а
     * `migration.defaults.errorThreshold` — лишь дефолт для фазы без своего значения. Второй,
     * post-mortem, проверки здесь нет намеренно: она сравнивала общий `skipped` всего прогона с
     * глобальным дефолтом и отменяла собственный override стадии — одно имя означало две разные
     * области действия.
     */
    private fun runMigration(plan: MigrationPlan, ctx: RunContext): Int {
        return try {
            PlanInterpreter(ctx).execute(plan)
            0
        } catch (e: Throwable) {
            // Безопасный аудит — если errors.csv недоступен, не теряем оригинальный e.
            try {
                ctx.auditError(e, item = null)
            } catch (auditErr: Throwable) {
                log.warn("auditError failed during unhandled-exception handling; original preserved", auditErr)
                ctx.report.addWarning(
                    "auditError failed: ${auditErr.javaClass.simpleName}: ${auditErr.message ?: ""}"
                )
            }
            // Отдельно от incFailed(): у отказа стадии могло не быть обрабатываемого элемента,
            // и общий счётчик ломал бы тождество processed = successful + skipped + failed.
            ctx.report.recordRunFailure()

            if (isInterruption(e)) {
                // Прерванный одноразовый процесс не завершился успешно, какой бы ни была политика:
                // часть работы не сделана, часть эффектов не подтверждена.
                Thread.currentThread().interrupt()
                log.error("Migration ${plan.name} was INTERRUPTED", e)
                return 1
            }
            when (plan.onUnhandled ?: config.defaults().onUnhandled()) {
                ScriptPolicy.FAIL_FAST -> {
                    log.error("Migration ${plan.name} FAILED", e); 1
                }

                ScriptPolicy.LOG_AND_COMPLETE -> {
                    log.error("Migration ${plan.name} failed (LOG_AND_COMPLETE)", e); 0
                }
            }
        }
    }

    // InterruptedException приезжает завёрнутым: барьер вешает свои ошибки в suppressed, а ops
    // оборачивают чужие исключения в свои. Идентичность в множестве — защита от циклов cause.
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
     * инструкцию instanceof, и JVM обязана разрешить символическую ссылку на класс из constant
     * pool. С `log4j-slf4j2-impl` или `slf4j-jdk14` вместо Logback это `NoClassDefFoundError`,
     * который пробивает `init()` насквозь: миграция не стартует вообще — вместо обещанного
     * документацией предупреждения и работы без `migration.log`.
     */
    private fun attachFileLogger(outputFolder: Path, report: ReportBuilder? = null): AutoCloseable? =
        try {
            attachLogbackFileAppender(outputFolder, report)
        } catch (e: LinkageError) {
            val msg = "migration.log file output disabled: Logback not on classpath (${e.javaClass.simpleName})"
            log.warn(msg)
            report?.addWarning(msg)
            null
        }

    private fun attachLogbackFileAppender(outputFolder: Path, report: ReportBuilder?): AutoCloseable? {
        val factory = LoggerFactory.getILoggerFactory()
        if (factory !is LoggerContext) {
            val msg = "migration.log file output disabled: Logback not on classpath (found ${factory.javaClass.name})"
            log.warn(msg)
            // Поднимаем warn в финальный stdout-отчёт — без этого в проде «куда делся migration.log?»
            // обнаруживается только по факту отсутствия файла, без следов в самом отчёте.
            report?.addWarning(msg)
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
