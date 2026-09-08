package io.github.dsudomoin.migration.csv

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationScope
import io.github.dsudomoin.migration.MigrationTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalDate

/**
 * Разбор строки в DTO.
 *
 * Главное здесь — не «оно маппится», а то, чего Jackson сам по себе не делает: пустая ячейка
 * и отсутствующая колонка для `Long` молча дают `0`, и такой ноль уезжает в целевую систему
 * как настоящее значение.
 */
class CsvBindTest {

    @TempDir
    lateinit var tmp: Path

    enum class Tier { GOLD, SILVER }

    data class Customer(
        val id: Long,
        val email: String,
        val spendAmount: BigDecimal,
        val registeredAt: LocalDate,
        val tier: Tier,
        val active: Boolean,
    )

    data class Partial(val id: Long, val comment: String?, val bonus: Long?, val tier: String = "SILVER")

    class NoPrimaryConstructor

    private inline fun <reified T : Any> read(
        content: String,
        onRowError: ItemError<CsvRow> = ItemError.Fail,
    ): Pair<List<T>, Throwable?> {
        val file = tmp.resolve("in.csv")
        Files.writeString(file, content)
        val seen = mutableListOf<T>()
        val migration = object : Migration("BIND") {
            override fun MigrationScope.run() {
                readCsvAs<T>(file.toString(), onRowError = onRowError).forEach { seen += it }
            }
        }
        val outcome = MigrationTest.run(migration, outputFolder = tmp.resolve("out"))
        return seen to outcome.failure
    }

    @Test
    fun `data class собирается из строки со всеми типами`() {
        val (rows, failure) = read<Customer>(
            "id,email,spend_amount,registered_at,tier,active\n" +
                "1,a@b.ru,10.50,2026-01-01,GOLD,true\n",
        )

        assertThat(failure).isNull()
        assertThat(rows).containsExactly(
            Customer(1, "a@b.ru", BigDecimal("10.50"), LocalDate.of(2026, 1, 1), Tier.GOLD, true),
        )
    }

    @Test
    fun `имена колонок сопоставляются с полями через нормализацию`() {
        val (rows, failure) = read<Customer>(
            " ID , E-Mail ,SPEND_AMOUNT,RegisteredAt,tier,Active\n" +
                "2,c@d.ru,1,2026-02-02,SILVER,false\n",
        )

        assertThat(failure).isNull()
        assertThat(rows.single().email).isEqualTo("c@d.ru")
        assertThat(rows.single().spendAmount).isEqualByComparingTo("1")
    }

    @Test
    fun `лишние колонки файла игнорируются`() {
        val (rows, failure) = read<Partial>("id,junk,another\n1,x,y\n")

        assertThat(failure).isNull()
        assertThat(rows).containsExactly(Partial(1, null, null, "SILVER"))
    }

    @Test
    fun `нет колонки под обязательное поле — падение со списком недостающих`() {
        val (rows, failure) = read<Customer>("id,email\n1,a@b.ru\n")

        assertThat(rows).describedAs("ни одна строка не обработана").isEmpty()
        assertThat(failure).isInstanceOf(CsvStructureException::class.java)
        assertThat(failure)
            .hasMessageContaining("spendAmount")
            .hasMessageContaining("registeredAt")
            .hasMessageContaining("tier")
            .hasMessageContaining("active")
            .hasMessageContaining("id, email")
    }

    @Test
    fun `пустая ячейка под non-null число — ошибка строки, а не тихий ноль`() {
        val (rows, failure) = read<Customer>(
            "id,email,spend_amount,registered_at,tier,active\n" +
                "1,a@b.ru,,2026-01-01,GOLD,true\n",
        )

        assertThat(rows).isEmpty()
        assertThat(failure).isInstanceOf(IllegalArgumentException::class.java)
        assertThat(failure).hasMessageContaining("spend_amount").hasMessageContaining("2")
    }

    @Test
    fun `пустая ячейка под nullable поле даёт null, а под поле с дефолтом — дефолт`() {
        val (rows, failure) = read<Partial>("id,comment,bonus,tier\n1,,,\n")

        assertThat(failure).isNull()
        assertThat(rows).containsExactly(Partial(1, null, null, "SILVER"))
    }

    @Test
    fun `пустая ячейка под non-null String — пустая строка`() {
        val (rows, failure) = read<Customer>(
            "id,email,spend_amount,registered_at,tier,active\n" +
                "1,,1,2026-01-01,GOLD,true\n",
        )

        assertThat(failure).isNull()
        assertThat(rows.single().email).isEmpty()
    }

    @Test
    fun `непарсящееся значение подчиняется onRowError`() {
        val content = "id,comment,bonus,tier\n1,x,10,GOLD\n2,y,НЕ-ЧИСЛО,GOLD\n3,z,30,GOLD\n"

        val (skipped, noFailure) = read<Partial>(content, ItemError.Skip)
        assertThat(noFailure).isNull()
        assertThat(skipped.map { it.id }).containsExactly(1L, 3L)

        val (failed, failure) = read<Partial>(content, ItemError.Fail)
        assertThat(failed.map { it.id }).containsExactly(1L)
        assertThat(failure).isNotNull()
    }

    @Test
    fun `класс без первичного конструктора отвергается понятной ошибкой`() {
        val (_, failure) = read<NoPrimaryConstructor>("id\n1\n")

        assertThat(failure).isInstanceOf(IllegalArgumentException::class.java)
        assertThat(failure).hasMessageContaining("NoPrimaryConstructor")
    }
}
