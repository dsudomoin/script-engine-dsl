package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.internal.ForEachEngine

/**
 * Параллельная обработка коллекции с error-policy и прогресс-логом.
 *
 * Каждый [items]-item исполняется в [block] на одном из потоков [executor]'а. На каждой удачной
 * итерации — `report.successful++`; при ошибке — поведение определяется [onError]. Прогресс
 * пишется в лог согласно [progress].
 *
 * @param parallel число параллельных воркеров. `1` (дефолт) = sequential.
 * @param onError политика при ошибке. Дефолт [OnError.Fail] — прервать миграцию на первой.
 * @param onErrorLog дополнительный (помимо авто-аудита) лог-колбэк при ошибке. Получает item,
 *                   на котором споткнулись. Полезно для warn-level логов с контекстом.
 * @param logEach колбэк, формирующий строку для INFO-лога после каждого успешного item'а.
 *                `null` = молчать.
 * @param progress стратегия прогресс-логирования (см. [Progress]).
 */
fun <T> MigrationContext.forEach(
    items: Iterable<T>,
    parallel: Int = 1,
    onError: OnError = OnError.Fail,
    onErrorLog: ((Throwable, T) -> Unit)? = null,
    logEach: ((T) -> String)? = null,
    progress: Progress = Progress.Default,
    block: MigrationContext.(T) -> Unit,
) {
    ForEachEngine(this).runSingle(items, parallel, onError, onErrorLog, logEach, progress, block)
}

/**
 * Параллельная обработка с батчингом: `items` разбивается на куски по [chunk] элементов, каждый
 * батч уходит на один воркер целиком. Размер последнего батча может быть меньше [chunk].
 *
 * Используется для запросов с `IN`-clause (`SELECT ... WHERE id IN :ids`), для bulk-insert'ов в БД,
 * для уменьшения round-trip'ов в HTTP-сервисы.
 *
 * @param chunk размер батча. На 1M item'ов и `chunk=500` будет ~2000 батчей.
 * @see forEach (Iterable, без chunk) для item-by-item обработки.
 */
@JvmName("forEachChunked")
fun <T> MigrationContext.forEach(
    items: Iterable<T>,
    chunk: Int,
    parallel: Int = 1,
    onError: OnError = OnError.Fail,
    onErrorLog: ((Throwable, List<T>) -> Unit)? = null,
    logEach: ((List<T>) -> String)? = null,
    progress: Progress = Progress.Default,
    block: MigrationContext.(List<T>) -> Unit,
) {
    ForEachEngine(this).runChunked(items, chunk, parallel, onError, onErrorLog, logEach, progress, block)
}

/**
 * Перегрузка для [Sequence]-источников (`jdbc.stream`, `cassandra.stream`, `readCsv`). Полезна
 * когда полный `List<T>` не помещается в память — обработка идёт по мере поступления.
 */
fun <T> MigrationContext.forEach(
    items: Sequence<T>,
    parallel: Int = 1,
    onError: OnError = OnError.Fail,
    onErrorLog: ((Throwable, T) -> Unit)? = null,
    logEach: ((T) -> String)? = null,
    progress: Progress = Progress.Default,
    block: MigrationContext.(T) -> Unit,
) = forEach(items.asIterable(), parallel, onError, onErrorLog, logEach, progress, block)

/**
 * Chunked-перегрузка для [Sequence]-источника. Использует **ленивый** [Sequence.chunked]:
 * исходный stream итерируется батчами по [chunk] элементов и материализуется только текущий батч
 * (не весь поток в `List<List<T>>`). Уместно для миллионных JDBC/Cassandra-стримов под
 * `WHERE id IN :ids` запросы.
 *
 * Без этой перегрузки `forEach(seq.asIterable(), chunk = ...)` через `Iterable.chunked` свалил бы
 * исходный Sequence в полный `List<List<T>>` (всё в памяти) — теряется главный плюс streaming-источника.
 */
@JvmName("forEachSequenceChunked")
fun <T> MigrationContext.forEach(
    items: Sequence<T>,
    chunk: Int,
    parallel: Int = 1,
    onError: OnError = OnError.Fail,
    onErrorLog: ((Throwable, List<T>) -> Unit)? = null,
    logEach: ((List<T>) -> String)? = null,
    progress: Progress = Progress.Default,
    block: MigrationContext.(List<T>) -> Unit,
) {
    require(chunk > 0) { "chunk must be > 0, got $chunk" }
    // Делегируем в single-overload c T = List<T>: батч — это «один item». Так не плодим
    // отдельный engine-path; cancellation / error-policy / progress работают без изменений.
    ForEachEngine(this).runSingle(
        items.chunked(chunk).asIterable(),
        parallel, onError, onErrorLog, logEach, progress, block,
    )
}
