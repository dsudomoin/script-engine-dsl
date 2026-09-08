package io.github.dsudomoin.migration.kora

import io.github.dsudomoin.migration.MigrationDefinition
import ru.tinkoff.kora.application.graph.All
import ru.tinkoff.kora.common.Module
import ru.tinkoff.kora.common.annotation.Root
import ru.tinkoff.kora.config.common.Config
import ru.tinkoff.kora.config.common.extractor.ConfigValueExtractor
import java.util.Optional

/**
 * Kora-модуль, регистрирующий [MigrationRunner] в графе. Подключается через
 * `@KoraApp interface App : ..., MigrationModule` — и этого достаточно: секцию `migration`
 * модуль читает сам.
 *
 * Runner забирает все [Migration]-компоненты графа через `All<MigrationDefinition>` (т.е. достаточно
 * пометить пользовательский скрипт `@Component`).
 */
@Module
interface MigrationModule {

    /**
     * Достаёт секцию `migration { ... }` из `application.conf`. Фабрика написана руками, а не
     * сгенерирована `@ConfigSource`: конфиг библиотеки обязан приезжать вместе с её модулем,
     * иначе потребителю пришлось бы подмешивать себе ещё один, нигде не документированный модуль.
     * Тот же приём, что у `JdbcDatabaseModule.jdbcDataBaseConfig` в самой Kora.
     */
    fun migrationConfig(config: Config, extractor: ConfigValueExtractor<MigrationConfig>): MigrationConfig =
        extractor.extract(config.get("migration"))
            ?: MigrationConfigValues()

    /**
     * Фабрика [MigrationRunner] — вызывается Kora при сборке графа. Override'ить не нужно.
     *
     * `@Root` здесь обязателен и не является украшением. От runner'а в пользовательском графе
     * не зависит ни один компонент — его тянет только сам `@KoraApp`. Kora резолвит граф
     * исключительно от корневого набора, поэтому без `@Root` компонент не создаётся вообще:
     * приложение стартует, `init()` не вызывается, миграция молча не выполняется, процесс
     * завершается кодом 0. K8s Job при этом выглядит успешным.
     */
    @Root
    fun migrationRunner(
        config: MigrationConfig,
        definitions: All<MigrationDefinition>,
        exit: Optional<MigrationExit>,
    ): MigrationRunner {
        val handler = exit.orElse(null)
        val list = definitions.toList()
        // Без компонента [MigrationExit] runner завершает процесс сам — это штатный режим
        // одноразового скрипта. С компонентом код возврата уезжает в него: так граф можно
        // поднять в тесте, не убивая JVM.
        return if (handler == null) {
            MigrationRunner(config, list)
        } else {
            MigrationRunner(config, list) { code -> handler.exit(code) }
        }
    }
}
