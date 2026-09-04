package io.github.dsudomoin.migration.internal

import io.github.dsudomoin.migration.CursorNotAdvancing
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class PagedSequenceTest {

    private data class Row(val version: Int)

    @Test
    fun `отдаёт элементы всех страниц по порядку`() {
        val pages = listOf(
            listOf(Row(1), Row(2)),
            listOf(Row(3), Row(4)),
            listOf(Row(5)),
        )

        val result = pagedSequence(
            first = { pages[0] },
            next = { cursor: Int -> pages.getOrElse(cursor / 2) { emptyList() } },
            nextCursor = { raw -> raw.last().version + 1 },
            continueWhen = { raw -> raw.size >= 2 },
        ).toList()

        assertThat(result.map { it.version }).containsExactly(1, 2, 3, 4, 5)
    }

    @Test
    fun `пустая первая страница завершает источник и не вызывает nextCursor`() {
        var nextCursorCalls = 0

        val result = pagedSequence<Int, Row>(
            first = { emptyList() },
            next = { error("next не должен вызываться") },
            nextCursor = { nextCursorCalls++; 1 },
            continueWhen = { true },
        ).toList()

        assertThat(result).isEmpty()
        assertThat(nextCursorCalls).isZero()
    }

    @Test
    fun `пустая сырая страница завершает источник даже когда continueWhen истинен`() {
        val result = pagedSequence(
            first = { listOf(Row(1)) },
            next = { _: Int -> emptyList<Row>() },
            nextCursor = { raw -> raw.last().version },
            continueWhen = { true },
        ).toList()

        assertThat(result.map { it.version }).containsExactly(1)
    }

    @Test
    fun `continueWhen ложен — следующая страница не запрашивается`() {
        var nextCalls = 0

        val result = pagedSequence(
            first = { listOf(Row(1), Row(2)) },
            next = { _: Int -> nextCalls++; listOf(Row(3)) },
            nextCursor = { raw -> raw.last().version },
            continueWhen = { false },
        ).toList()

        assertThat(result.map { it.version }).containsExactly(1, 2)
        assertThat(nextCalls).isZero()
    }

    @Test
    fun `следующая страница не читается, пока текущая не потреблена`() {
        var fetched = 0

        val taken = pagedSequence(
            first = { fetched++; listOf(Row(1), Row(2)) },
            next = { _: Int -> fetched++; listOf(Row(3), Row(4)) },
            nextCursor = { raw -> raw.last().version },
            continueWhen = { true },
        ).take(2).toList()

        assertThat(taken.map { it.version }).containsExactly(1, 2)
        assertThat(fetched).isEqualTo(1)
    }

    @Test
    fun `неподвижный курсор завершает прогон ошибкой вместо бесконечного цикла`() {
        assertThatThrownBy {
            pagedSequence(
                name = "portfolios",
                first = { listOf(Row(7)) },
                next = { _: Int -> listOf(Row(7)) },
                nextCursor = { raw -> raw.last().version },
                continueWhen = { true },
            ).toList()
        }
            .isInstanceOf(CursorNotAdvancing::class.java)
            .hasMessageContaining("portfolios")
            .hasMessageContaining("7")
    }

    @Test
    fun `считает сырые страницы и сырые строки до пользовательского фильтра`() {
        var pages = 0L
        var rows = 0L

        val kept = pagedSequence(
            first = { listOf(Row(1), Row(2), Row(3)) },
            next = { _: Int -> listOf(Row(4), Row(5)) },
            nextCursor = { raw -> raw.last().version },
            continueWhen = { raw -> raw.size >= 3 },
            onPage = { rowCount -> pages++; rows += rowCount },
        ).filter { it.version == 5 }.toList()

        assertThat(kept.map { it.version }).containsExactly(5)
        assertThat(pages).isEqualTo(2)
        assertThat(rows).isEqualTo(5)
    }
}
