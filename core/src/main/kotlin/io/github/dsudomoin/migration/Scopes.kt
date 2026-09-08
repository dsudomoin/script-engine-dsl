package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.error.CsvFileErrorReporter
import io.github.dsudomoin.migration.report.ReportBuilder
import org.slf4j.Logger
import java.nio.file.Path

/**
 * Общее для всех контекстов прогона: то, что доступно и при загрузке input'а, и при построении
 * источника, и в обработчике.
 *
 * На этот тип опираются extension'ы операций (`jdbc`, `cassandra`, `kafka`, `topic`, `http`,
 * `openCsv`, `readCsv`): именно поэтому они работают во всех трёх контекстах без дублирования.
 */
@MigrationDsl
interface RunScope : ResourceRegistry {
    /** `true`, если прогон запущен в режиме репетиции: `write` и `publish` не выполняются. */
    val dryRun: Boolean

    /** SLF4J-логгер этой миграции. */
    val log: Logger

    /** Авто-аудитор ошибок; сюда же регистрируются per-type сериализаторы через `includeItem`. */
    val errors: CsvFileErrorReporter

    /** Папка артефактов прогона. */
    val outputFolder: Path

    /** Аккумулятор статистики прогона. Инкрементируется движком, а не кодом миграции. */
    val report: ReportBuilder

    /** Ресурс, единственный на прогон для данного [key]. */
    fun <T : AutoCloseable> shared(key: Any, factory: () -> T): T

    /**
     * Низкоуровневый dry-run-гейт. Пользовательский код пишет `write`/`publish`, а не этот метод.
     *
     * Остаётся в API, потому что на нём держится защита прямых вызовов ops: `jdbc(db).execute(...)`,
     * написанный мимо `write { }`, под dry-run иначе выполнился бы по-настоящему. Двойного гейта не
     * возникает: внутри `write { }` под dry-run тело вообще не вызывается.
     */
    fun <R> guardWrite(
        label: String,
        args: Map<String, Any?> = emptyMap(),
        dryRunDefault: R,
        action: () -> R,
    ): R

    /** `Unit`-перегрузка [guardWrite]. */
    fun guardWrite(
        label: String,
        args: Map<String, Any?> = emptyMap(),
        action: () -> Unit,
    )

    /** Записать ошибку в `errors.csv` плюс стектрейс в `errors.log`. */
    fun auditError(e: Throwable, item: Any?)
}

/** Контекст загрузки [Input] и валидации прогона. */
@MigrationDsl
interface InputScope : RunScope

/** Контекст построения источника стадии. */
@MigrationDsl
interface SourceScope : RunScope {
    /** Разрешает [input], вычисляя его при первом обращении и кэшируя на весь прогон. */
    fun <I> resolve(input: Input<I>): I

    /**
     * Ленивый курсорный источник. Решения принимаются по сырой странице, до пользовательских
     * `filter`/`map`. Страницы считаются в отчёт как `rawPages`/`rawRows`.
     */
    fun <C : Any, T> pages(
        name: String? = null,
        first: () -> List<T>,
        next: (C) -> List<T>,
        nextCursor: (List<T>) -> C,
        continueWhen: (List<T>) -> Boolean,
    ): Sequence<T>

    /**
     * Регистрирует действие закрытия на границе текущего scope'а.
     *
     * В `scoped`-стадии это граница родителя — ресурс закроется перед переходом к следующему,
     * а не в конце прогона. Для ресурсов, живущих весь прогон, есть `register`.
     */
    fun scopedResource(close: () -> Unit)
}

/** Контекст обработки одного элемента стадии. */
@MigrationDsl
interface HandlerScope : RunScope {

    /**
     * Контролируемая запись во внешнюю систему.
     *
     * Под dry-run [action] не вызывается и результатом будет [WriteResult.DryRunSkipped].
     * Исключение из [action] — обычный сбой элемента и идёт через [ItemError].
     */
    fun write(
        name: String,
        args: Map<String, Any?> = emptyMap(),
        action: () -> WriteOutcome,
    ): WriteResult

    /**
     * Запись, сообщающая число изменённых строк: `0` трактуется как [WriteOutcome.Rejected].
     *
     * Подходит для `UPDATE ... WHERE`. НЕ подходит там, где числа строк нет вовсе — например
     * для Cassandra-INSERT или delete+insert; там нужен [write].
     */
    fun writeRows(
        name: String,
        args: Map<String, Any?> = emptyMap(),
        action: () -> Int,
    ): WriteResult

    /**
     * Асинхронная публикация. Созданный [send] stage регистрируется в барьере scope'а: стадия
     * не завершится, пока все отправленное не подтвердится.
     *
     * Под dry-run [send] не вызывается вообще, и фальшивый future не создаётся: метод ничего
     * не возвращает, поэтому возвращать было бы нечего.
     */
    fun publish(
        name: String,
        args: Map<String, Any?> = emptyMap(),
        send: () -> java.util.concurrent.CompletionStage<*>,
    )
}
