package io.github.dsudomoin.migration.kora.ops

import ru.tinkoff.kora.common.Context
import ru.tinkoff.kora.database.jdbc.ConnectionContext
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction1
import ru.tinkoff.kora.database.jdbc.RuntimeSqlException
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

// Minimal JdbcConnectionFactory for tests — bypasses DataBaseTelemetryFactory/Hikari wiring.
//
// ConnectionContext хранится в Kora Context ровно как в JdbcDatabase, и это обязательно, а не
// для симметрии: начиная с Kora 1.2.20 default-реализация JdbcConnectionFactory.inTx
// разыменовывает currentConnectionContext() без проверки на null — хотя объявляет метод
// @Nullable и сама возвращает null по умолчанию. Фабрика, не переопределившая его, падает
// NPE на любом inTx.
class TestJdbcConnectionFactory(
    private val url: String,
    private val user: String,
    private val password: String,
) : JdbcConnectionFactory {

    // Context.Key сравнивается по ссылке (equals финализирован как obj === this), поэтому ключ
    // обязан быть один на фабрику. copy → null: форкнутый контекст не наследует чужое
    // соединение, как и в JdbcDatabase.
    private val key = object : Context.Key<ConnectionContext>() {
        override fun copy(value: ConnectionContext): ConnectionContext? = null
    }

    override fun currentConnection(): Connection? = Context.current().get(key)?.connection()

    override fun currentConnectionContext(): ConnectionContext? = Context.current().get(key)

    override fun newConnection(): Connection = DriverManager.getConnection(url, user, password)

    override fun telemetry() = throw UnsupportedOperationException("no telemetry in test factory")

    override fun <T : Any?> withConnection(callback: SqlFunction1<Connection, T>): T {
        val ctx = Context.current()
        // Вложенный withConnection переиспользует уже открытое соединение — на этом держится
        // видимость собственных uncommitted-записей внутри транзакции.
        val active = ctx.get(key)
        if (active != null) {
            return try {
                callback.apply(active.connection())
            } catch (e: SQLException) {
                throw RuntimeSqlException(e)
            }
        }
        return try {
            ctx.set(key, ConnectionContext(newConnection())).connection().use { callback.apply(it) }
        } catch (e: SQLException) {
            throw RuntimeSqlException(e)
        } finally {
            ctx.remove(key)
        }
    }
}
