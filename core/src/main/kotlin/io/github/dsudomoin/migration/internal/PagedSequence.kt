package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.CursorNotAdvancing

/**
 * Ленивый курсорный генератор, на котором построен `SourceScope.pages`.
 *
 * Решения принимаются по СЫРОЙ странице — до пользовательских `filter`/`map`, которые движок не
 * видит вовсе. Поэтому страница, целиком отсеянная фильтром, источник не завершает, а пустая сырая —
 * завершает всегда.
 *
 * Разделение [first] и [next] не косметическое: первая страница читается без предиката по курсору,
 * последующие — строго `>` либо `<` от него. Побочно это разрывает цикл вывода типов: [first] даёт `T`,
 * [nextCursor] даёт `C`, и аннотации типов на стороне вызова не нужны.
 *
 * @param onPage вызывается на каждую сырую страницу с числом строк в ней — это единственный способ показать
 *               в отчёте разницу между «прочитано» и «дошло до handler»
 */
internal fun <C : Any, T> pagedSequence(
    name: String? = null,
    first: () -> List<T>,
    next: (C) -> List<T>,
    nextCursor: (List<T>) -> C,
    continueWhen: (List<T>) -> Boolean,
    onPage: (rows: Int) -> Unit = {},
): Sequence<T> = sequence {
    var cursor: C? = null
    while (true) {
        val page = cursor?.let(next) ?: first()
        onPage(page.size)
        if (page.isEmpty()) return@sequence

        yieldAll(page)

        if (!continueWhen(page)) return@sequence

        val advanced = nextCursor(page)
        if (advanced == cursor) throw CursorNotAdvancing(name, advanced)
        cursor = advanced
    }
}
