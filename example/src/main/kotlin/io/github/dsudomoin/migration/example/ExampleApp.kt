package io.github.dsudomoin.migration.example

import ru.tinkoff.kora.application.graph.KoraApplication
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.common.KoraApp
import ru.tinkoff.kora.config.hocon.HoconConfigModule
import io.github.dsudomoin.migration.kora.MigrationModule
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

/**
 * Приложение-миграция целиком. Запуск:
 * ```
 * MIGRATION_RUN=CUSTOMER-TIER-001 ./gradlew :example:run                        # боевой прогон
 * MIGRATION_RUN=CUSTOMER-TIER-001 MIGRATION_DRY_RUN=true ./gradlew :example:run # репетиция
 * ```
 *
 * Подключение библиотеки — одна строка: `MigrationModule` в списке родителей `@KoraApp`.
 * Конфиг-модуль приезжает вместе с ним, runner помечен `@Root` и потому создаётся графом сам.
 */
@KoraApp
interface ExampleApp : HoconConfigModule, MigrationModule

fun main() {
    KoraApplication.run { ExampleAppGraph.graph() }
}

/**
 * Стенд-ин вместо настоящего репозитория: пишет строки в TSV-файл. Важна не реализация, а то,
 * что это **обычный компонент графа**, о котором библиотека ничего не знает: под dry-run
 * его вызов выполнится по-настоящему, если миграция не проверит флаг сама.
 */
@Component
class CustomerTierRepository {

    private val log = LoggerFactory.getLogger(javaClass)
    private val file: Path = Paths.get("build", "example-output", "customer-tier.tsv")

    init {
        Files.createDirectories(file.parent)
        Files.write(file, ByteArray(0), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    }

    /** Запись «в БД». Синхронизация нужна: `each(parallel = 4)` зовёт это из четырёх потоков. */
    @Synchronized
    fun updateTier(customerId: Long, tier: String) {
        Files.write(
            file,
            "$customerId\t$tier\n".toByteArray(),
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND,
        )
    }

    fun rows(): List<String> = if (Files.exists(file)) Files.readAllLines(file).filter { it.isNotBlank() } else emptyList()

    fun path(): Path = file

    init {
        log.debug("customer tier sink: {}", file.toAbsolutePath())
    }
}
