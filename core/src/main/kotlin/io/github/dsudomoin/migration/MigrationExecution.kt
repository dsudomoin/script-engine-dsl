package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.error.CsvFileErrorReporter
import io.github.dsudomoin.migration.internal.MigrationRun
import io.github.dsudomoin.migration.report.MigrationReport
import io.github.dsudomoin.migration.report.ReportBuilder
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path

/**
 * Настройки одного прогона. В бою собираются Kora-runner'ом из HOCON, в тестах — руками.
 */
data class RunSettings(
    val dryRun: Boolean,
    val outputFolder: Path,
    val errorThreshold: Long = 0,
    val progressEvery: Int = 1000,
    val maxItemReprLength: Int = 500,
    val includeStackTrace: Boolean = true,
)

/**
 * Итог прогона: отчёт есть всегда, [failure] не `null`, если тело миграции упало.
 *
 * Исключение возвращается, а не пробрасывается: счётчики упавшего прогона нужны и тесту,
 * и итоговому отчёту в логе — а решение про код возврата принимает вызывающий.
 */
data class ExecutionOutcome(
    val report: MigrationReport,
    val failure: Throwable?,
)

/**
 * Исполнение миграции без Kora: готовит папку артефактов, создаёт контекст, зовёт тело,
 * закрывает ресурсы и строит отчёт.
 *
 * Коды возврата, выбор миграции по имени и файловый лог — забота Kora-слоя.
 */
object MigrationExecution {

    fun execute(migration: Migration, settings: RunSettings): ExecutionOutcome {
        Files.createDirectories(settings.outputFolder)
        val errorsFile = settings.outputFolder.resolve("errors.csv")
        val traceFile = settings.outputFolder.resolve("errors.log")
        // Один прогон — один свежий набор артефактов: аудитор создаёт файлы лениво, и чистый
        // прогон иначе унаследовал бы errors.csv прошлого запуска и приложил его к своему отчёту.
        Files.deleteIfExists(errorsFile)
        Files.deleteIfExists(traceFile)

        val report = ReportBuilder(migration.name, "", settings.dryRun)
        val reporter = CsvFileErrorReporter(
            migration.name,
            "",
            errorsFile,
            traceFile,
            maxItemReprLength = settings.maxItemReprLength,
            includeStackTrace = settings.includeStackTrace,
        )
        val run = MigrationRun(
            dryRun = settings.dryRun,
            log = LoggerFactory.getLogger("io.github.dsudomoin.migration.${migration.name}"),
            outputFolder = settings.outputFolder,
            errors = reporter,
            report = report,
            name = migration.name,
            defaultProgressEvery = settings.progressEvery,
            defaultErrorThreshold = settings.errorThreshold,
        )

        var failure: Throwable? = null
        try {
            run.executeBody(migration)
        } catch (e: Throwable) {
            failure = e
            try {
                reporter.report(e, null)
            } catch (auditError: Throwable) {
                report.addWarning("auditError failed: ${auditError.javaClass.simpleName}: ${auditError.message ?: ""}")
            }
            // Отдельно от incFailed(): у отказа прогона могло не быть обрабатываемого элемента,
            // и общий счётчик ломал бы тождество processed = successful + skipped + failed.
            report.recordRunFailure()
            if (e is InterruptedException) Thread.currentThread().interrupt()
        } finally {
            // Сначала ресурсы прогона (CSV-выходы, пул), потом аудитор: воркер, доигрывающий
            // при остановке пула, ещё может записать ошибку элемента.
            try {
                run.closeRegistered()
            } finally {
                reporter.close()
            }
        }

        val built = report.build(
            errorsFile.takeIf { Files.exists(it) },
            traceFile.takeIf { Files.exists(it) },
        )
        return ExecutionOutcome(built, failure)
    }
}
