package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.error.CsvFileErrorReporter
import io.github.dsudomoin.migration.report.ReportBuilder
import org.slf4j.Logger
import java.nio.file.Path
import java.util.concurrent.Executor

/**
 * Контекст исполнения миграции. Передаётся как receiver в [Migration.migrate].
 *
 * Делает доступными в `migrate()`:
 * - core-расширения (`io.github.dsudomoin.migration.*`): `forEach`, `mutation`, `openCsv`, `readCsv`.
 * - kora-расширения (`io.github.dsudomoin.migration.kora.ops.*`): `jdbc`, `transactional`, `cassandra`, `kafka`,
 *   `topic`, `http`. Они живут в модуле `migration-dsl-kora`; если ты подключил только
 *   `migration-dsl-core` без bridge'а, эти расширения недоступны.
 *
 * Не имплементируется пользовательским кодом — единственная реализация
 * [io.github.dsudomoin.migration.internal.DefaultMigrationContext] создаётся либо runner'ом, либо
 * `DefaultMigrationContext.test(...)` в тестах.
 */
interface MigrationContext : ResourceRegistry {
    /**
     * `true` если миграция запущена в режиме dry-run. Все write-операции через [guardWrite]
     * (включая `jdbc.execute`, `kafka.publish`, `topic.send`, `mutation { }`, `http(call).post`)
     * пропускаются и инкрементят `report.dryRunSkipped`. Read-операции (`jdbc.query/stream`,
     * `cassandra.query/stream`, `openCsv`-запись CSV-output'ов) под dry-run работают обычно.
     */
    val dryRun: Boolean

    /** SLF4J-логгер привязанный к этой миграции (`io.github.dsudomoin.migration.<name>`). */
    val log: Logger

    /** Аккумулятор статистики прогона. Инкрементируется автоматически из `forEach` / `guardWrite`. */
    val report: ReportBuilder

    /**
     * Пул потоков для воркеров `forEach`. Runner создаёт cached pool: сколько воркеров реально
     * работает, решает `forEach(parallel = N)` через свой семафор, а пул обязан уметь выдать
     * столько потоков, сколько попросили. Подменяется целиком через `@Tag(MigrationExecutor::class)`.
     */
    val executor: Executor

    /**
     * Папка, куда runner кладёт все артефакты миграции: `migration.log` (slf4j root),
     * `errors.csv`/`errors.log` (авто-аудитор), пользовательские CSV из `openCsv(filename, ...)`.
     *
     * Источник пути: `migration.outputFolder` в HOCON (env `MIGRATION_OUTPUT_FOLDER`) или
     * дефолт `logs/${migration.name}` относительно JVM CWD. Создаётся (`mkdir -p`) на старте.
     */
    val outputFolder: Path

    /**
     * Период тика для [Progress.Default] (каждые N успешно обработанных item'ов). Берётся из
     * `migration.defaults.progressEvery` в HOCON (дефолт 1000). Используется внутри `forEach`
     * при `progress = Progress.Default`.
     */
    val defaultProgressEvery: Int

    /**
     * Значение аргумента `parallel` у `forEach`, если он не задан явно. Берётся из
     * `migration.defaults.parallel` в HOCON (дефолт 1 — последовательная обработка).
     */
    val defaultParallel: Int

    /**
     * Порог количества `SKIP`-овок, после которого `forEach` прерывает миграцию (бросает
     * [io.github.dsudomoin.migration.internal.ErrorThresholdExceeded], runner возвращает exit-code 1). Берётся из
     * `migration.defaults.errorThreshold` в HOCON. `0` (дефолт) — проверка отключена.
     *
     * Проверяется в реальном времени после каждого `incSkipped` — миграция останавливается
     * как только `report.skipped > errorThreshold`, не дожидаясь конца прохода.
     */
    val errorThreshold: Long

    /**
     * Авто-аудитор ошибок: [CsvFileErrorReporter], пишущий `errors.csv` + `errors.log` под
     * [outputFolder]. Используется автоматически из `forEach` через `OnError.Skip` /
     * `handle→Skip`.
     *
     * Тип намеренно — конкретный класс, а не [io.github.dsudomoin.migration.error.ErrorReporter] интерфейс.
     * Стандартный [io.github.dsudomoin.migration.kora.MigrationRunner] всегда инстанцирует
     * `CsvFileErrorReporter` — pluggable replacement (Sentry, Kibana, JSON Lines) требует
     * форка runner'а или собственного `Lifecycle`-компонента. Интерфейс `ErrorReporter`
     * остаётся в API для таких форков и для extension [includeItem].
     *
     * В теле `migrate()` пользователь обычно регистрирует per-type сериализаторы:
     * ```
     * errors.includeItem<Customer> { "id=${it.id}, status=${it.status}" }
     * ```
     * — тогда в `errors.csv` `Customer` пойдёт человекочитаемо, а не через `toString()`.
     *
     * `includeItem<T>` — inline-reified extension на `ErrorReporter` (супертип
     * `CsvFileErrorReporter`), импорт: `import io.github.dsudomoin.migration.error.includeItem`.
     */
    val errors: CsvFileErrorReporter

    /**
     * Запускает [action] с защитой по dry-run. Под dry-run [action] не вызывается, возвращается
     * [dryRunDefault], в лог идёт `INFO [DRY-RUN] <label> <args>`, в отчёт — инкремент
     * `dryRunSkipped[label]`. Используется внутри DSL-расширений (`SqlOps.execute`,
     * `KafkaOps.publish`, `mutation`, `http`); напрямую дёргать редко нужно.
     *
     * @param label человекочитаемая метка операции (например, `"kafka.publish"`).
     * @param args карта параметров для лога. Под dry-run выводится в формате `(k1=v1, k2=v2)`.
     * @param dryRunDefault значение, возвращаемое под dry-run вместо результата [action].
     */
    fun <R> guardWrite(
        label: String,
        args: Map<String, Any?> = emptyMap(),
        dryRunDefault: R,
        action: () -> R,
    ): R

    /**
     * `Unit`-перегрузка [guardWrite] для write-операций без возвращаемого значения.
     * Подходит для `kafka.publishAsync`, `mutation { }`, custom void-write.
     */
    fun guardWrite(
        label: String,
        args: Map<String, Any?> = emptyMap(),
        action: () -> Unit,
    )

    /**
     * Записать ошибку в `errors.csv` + стектрейс в `errors.log`. Уже вызывается
     * автоматически из `forEach` при `OnError.Skip` / `handle→Skip`;
     * пользовательский вызов нужен только для ad-hoc-отчётности из своего кода.
     *
     * @param e исключение
     * @param item элемент, на котором споткнулись — рендерится через `errors.includeItem<T>`
     *             сериализаторы, либо `toString()` по умолчанию.
     */
    fun auditError(e: Throwable, item: Any?)

    /**
     * Ресурс, единственный на весь прогон для данного [key]. Первый вызов создаёт его через
     * [factory] и регистрирует в реестре (как [register]); последующие вызовы с тем же ключом
     * отдают тот же инстанс.
     *
     * Нужен op-хендлам, которые естественно дёргать прямо в теле `forEach`: без мемоизации
     * `topic(producer, "t")` внутри цикла на каждый item клал бы в реестр новый объект, и на
     * миллионе item'ов сам реестр стал бы утечкой. Используется внутри `kafka(...)` и
     * `topic(...)`; в пользовательском коде уместен для собственных op-обёрток.
     *
     * [factory] не должна вызывать `shared` — вложенный вызов на той же мапе заблокируется.
     *
     * @param key идентичность ресурса. Для op-хендлов — сам клиент (`Producer`, `CqlSession`)
     *            либо `data`-ключ из клиента и имени.
     */
    fun <T : AutoCloseable> shared(key: Any, factory: () -> T): T
}
