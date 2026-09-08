package io.github.dsudomoin.migration

import java.nio.file.Path

/**
 * Запуск миграции в тесте без Kora-графа: зависимости подставляются конструктором
 * самой миграции.
 *
 * ```
 * val outcome = MigrationTest.run(FixOrders(repo), outputFolder = tmp)
 *
 * assertThat(outcome.failure).isNull()
 * assertThat(outcome.report.successful).isEqualTo(9998)
 * ```
 */
object MigrationTest {

    fun run(
        migration: Migration,
        outputFolder: Path,
        dryRun: Boolean = false,
        errorThreshold: Long = 0,
        progressEvery: Int = 1000,
    ): ExecutionOutcome = MigrationExecution.execute(
        migration,
        RunSettings(
            dryRun = dryRun,
            outputFolder = outputFolder,
            errorThreshold = errorThreshold,
            progressEvery = progressEvery,
        ),
    )
}
