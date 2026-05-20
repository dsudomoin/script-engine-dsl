package io.github.dsudomoin.migration.kora

import io.github.dsudomoin.migration.Migration
import ru.tinkoff.kora.application.graph.All
import ru.tinkoff.kora.common.Module
import ru.tinkoff.kora.common.Tag
import java.util.Optional
import java.util.concurrent.Executor

/**
 * Kora-модуль, регистрирующий [MigrationRunner] в графе. Подключается через
 * `@KoraApp interface App : ..., MigrationModule`.
 *
 * Runner забирает все [Migration]-компоненты графа через `All<Migration>` (т.е. достаточно
 * пометить пользовательский скрипт `@Component`). Опционально подхватывает кастомный `Executor`,
 * помеченный `@Tag(MigrationExecutor::class)` — иначе создаёт свой `FixedThreadPool`.
 */
@Module
interface MigrationModule {

    /** Фабрика [MigrationRunner] — вызывается Kora при сборке графа. Override'ить не нужно. */
    fun migrationRunner(
        config: MigrationConfig,
        migrations: All<Migration>,
        @Tag(MigrationExecutor::class) executor: Optional<Executor>,
    ): MigrationRunner = MigrationRunner(config, migrations.toList(), executor.orElse(null))
}
