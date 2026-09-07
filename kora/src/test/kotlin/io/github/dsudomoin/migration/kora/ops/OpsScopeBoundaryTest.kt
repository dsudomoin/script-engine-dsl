package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.HandlerScope
import io.github.dsudomoin.migration.RunScope
import io.github.dsudomoin.migration.migration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory

/**
 * Граница read/write проходит по типу scope'а и держится компилятором.
 *
 * Проверять её «попыткой вызвать `execute` из `items`» нельзя: это ошибка компиляции, а не
 * падающий тест. Поэтому граница фиксируется двумя фактами, которые проверяемы во время
 * исполнения: у читающего типа нет пишущих методов, а фабрики объявлены на разных receiver'ах и
 * возвращают разные типы. Сломать разделение, не уронив этот тест, нельзя.
 */
class OpsScopeBoundaryTest {

    // Соединение не открывается: проверяются типы, а не выполнение запроса.
    private val db: JdbcConnectionFactory = TestJdbcConnectionFactory("jdbc:unused", "u", "p")

    @Test
    fun `у читающего ops нет пишущих методов`() {
        val declared = SqlReadOps::class.java.declaredMethods.map { it.name }

        assertThat(declared).contains("query", "stream")
        assertThat(declared).doesNotContain("execute", "batch", "executeReturning")
    }

    @Test
    fun `фабрика на RunScope возвращает читающий ops, на HandlerScope — пишущий`() {
        val factories = Class.forName("io.github.dsudomoin.migration.kora.ops.SqlOpsKt")
            .declaredMethods
            .filter { it.name == "jdbc" }
            .associate { it.parameterTypes.first() to it.returnType }

        assertThat(factories)
            .describedAs("две перегрузки: по одной на каждый scope")
            .hasSize(2)
        assertThat(factories[RunScope::class.java]).isEqualTo(SqlReadOps::class.java)
        assertThat(factories[HandlerScope::class.java]).isEqualTo(SqlOps::class.java)
    }

    @Test
    fun `источник получает читающий ops, обработчик — пишущий`() {
        val plan = migration("SCOPES", "t") {
            source(
                items = {
                    // В items receiver — SourceScope, поэтому применима только RunScope-перегрузка.
                    val read: SqlReadOps = jdbc(db)
                    assertThat(read).isNotNull()
                    sequenceOf(1L)
                },
            ) { id ->
                // В handle receiver — HandlerScope: побеждает перегрузка с записью.
                val write: SqlOps = jdbc(db)
                assertThat(write).isInstanceOf(SqlReadOps::class.java)
                assertThat(id).isEqualTo(1L)
            }
        }

        assertThat(plan.stages).hasSize(1)
    }
}
