package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.error.ErrorReporter
import org.slf4j.Logger
import java.nio.file.Path

/**
 * Ограничитель DSL. Без него внешние receiver'ы остаются видны внутри вложенных лямбд.
 */
@DslMarker
annotation class MigrationDsl

/**
 * Счётчики одного вызова [MigrationScope.each].
 *
 * Держится тождество `processed = successful + skipped + failed`.
 */
data class EachResult(
    val processed: Long,
    val successful: Long,
    val skipped: Long,
    val failed: Long,
)

/**
 * Всё, что движок даёт телу миграции.
 *
 * Скоуп один на всё тело: и в `run()`, и внутри лямбды [each], и в приватных
 * extension-шагах доступно одно и то же.
 */
@MigrationDsl
interface MigrationScope {

    /**
     * `true`, если прогон запущен в режиме репетиции.
     *
     * Запись пишется как `if (!dryRun) repo.update(...)`. Движок ничего не перехватывает:
     * вызов чужого компонента ему не виден, поэтому забытая проверка под dry-run пишет
     * в целевую систему по-настоящему.
     */
    val dryRun: Boolean

    /** SLF4J-логгер этой миграции. Всё, что в него пишется, попадает и в `migration.log`. */
    val log: Logger

    /**
     * Авто-аудитор ошибок. Сюда же регистрируются per-type сериализаторы:
     * `errors.includeItem<Customer> { "id=${it.id}" }`.
     */
    val errors: ErrorReporter

    /** Папка артефактов прогона. */
    val outputFolder: Path

    /**
     * Цикл по элементам со счётчиками, политикой ошибок, порогом и прогрессом.
     *
     * При [parallel] = 1 (дефолт) это обычный `for` в текущем потоке: ни пула, ни
     * семафора, стектрейс падения указывает на строку миграции. При [parallel] > 1 пул
     * создаётся лениво, а [handle] обязан быть потокобезопасным.
     *
     * @param errorThreshold `null` — взять дефолт прогона; `0` — выключено. Превышение валит
     *        прогон через `ErrorThresholdExceeded`.
     */
    fun <T> each(
        items: Sequence<T>,
        parallel: Int = 1,
        onItemError: ItemError<T> = ItemError.Fail,
        progress: Progress = Progress.Default,
        errorThreshold: Long? = null,
        handle: (T) -> Unit,
    ): EachResult

    /**
     * Перегрузка для коллекций: репозитории возвращают `List`, и требовать от каждой
     * миграции `.asSequence()` значило бы добавлять шум ради типов.
     *
     * У коллекции известен размер, поэтому прогресс здесь показывает долю
     * (`progress: 300/1200 (25%)`) и сам укорачивает шаг на коротких циклах. У [Sequence]
     * размера нет, и прогресс ограничивается счётчиком.
     */
    fun <T> each(
        items: Iterable<T>,
        parallel: Int = 1,
        onItemError: ItemError<T> = ItemError.Fail,
        progress: Progress = Progress.Default,
        errorThreshold: Long? = null,
        handle: (T) -> Unit,
    ): EachResult
}
