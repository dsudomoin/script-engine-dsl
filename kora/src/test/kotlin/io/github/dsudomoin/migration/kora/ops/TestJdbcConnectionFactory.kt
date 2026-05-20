package io.github.dsudomoin.migration.kora.ops

import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction1
import java.sql.Connection
import java.sql.DriverManager

// Minimal JdbcConnectionFactory for tests — bypasses DataBaseTelemetryFactory/Hikari wiring.
class TestJdbcConnectionFactory(
    private val url: String,
    private val user: String,
    private val password: String,
) : JdbcConnectionFactory {

    override fun currentConnection(): Connection? = null

    override fun newConnection(): Connection = DriverManager.getConnection(url, user, password)

    override fun telemetry() = throw UnsupportedOperationException("no telemetry in test factory")

    override fun <T : Any?> withConnection(callback: SqlFunction1<Connection, T>): T =
        newConnection().use { callback.apply(it) }
}
