package io.github.dsudomoin.migration

/**
 * Курсор пагинатора не сдвинулся после непустой страницы.
 *
 * Проверка стоит одно сравнение на страницу, а её отсутствие — бесконечный цикл в проде, который
 * выглядит как «миграция зависла». Если курсор законно повторяется, его нужно сделать составным
 * (например `data class Cut(val cut: Instant, val id: UUID)`) — тип курсора произвольный.
 */
class CursorNotAdvancing internal constructor(
    sourceName: String?,
    cursor: Any?,
) : RuntimeException(
    "pages(${sourceName ?: "<unnamed>"}): cursor did not advance past '$cursor'; " +
        "the next page would repeat the previous one forever",
)
