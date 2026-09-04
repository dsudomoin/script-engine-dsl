package io.github.dsudomoin.migration

/** Идентификация одного вызова эффекта: метка, диагностические аргументы и обрабатываемый элемент. */
data class EffectRef(
    val name: String,
    val args: Map<String, Any?> = emptyMap(),
    val item: Any? = null,
)

/** Отказ асинхронного эффекта вместе с контекстом, в котором он был зарегистрирован. */
data class EffectFailure(val effect: EffectRef, val error: Throwable)

/**
 * Хотя бы один асинхронный эффект scope'а завершился ошибкой.
 *
 * Первый отказ идёт причиной, остальные сохранённые — через `addSuppressed`. В отчёт попадают все
 * отказы счётчиком, а в `errors.csv` — каждый поштучно.
 */
class ScopeEffectsFailed internal constructor(
    message: String,
    cause: Throwable,
) : RuntimeException(message, cause)

/**
 * Асинхронные эффекты scope'а не успели подтвердиться за отведённый таймаут.
 *
 * Уже отправленное не отзывается: незавершённые эффекты учитываются как `abandoned` и попадают в отчёт
 * отдельной строкой — молча исчезать они не должны.
 */
class ScopeCompletionTimeout internal constructor(message: String) : RuntimeException(message)
