package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.internal.DefaultMigrationContext
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction1
import java.lang.reflect.Proxy
import java.sql.Connection
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Защиты SqlOps, которые обязаны срабатывать до похода в базу — поэтому и проверяются без неё.
 */
class SqlOpsGuardTest {

    /** Соединение, которое взорвётся при любой попытке им воспользоваться. */
    private val explodingConnection: Connection = Proxy.newProxyInstance(
        Connection::class.java.classLoader,
        arrayOf(Connection::class.java),
    ) { _, method, _ ->
        when (method.name) {
            "toString" -> "exploding-connection"
            "hashCode" -> 1
            "equals" -> false
            else -> throw AssertionError("соединение не должно использоваться, вызван ${method.name}")
        }
    } as Connection

    private val db = object : JdbcConnectionFactory {
        override fun currentConnection(): Connection? = null
        override fun newConnection(): Connection = explodingConnection
        override fun telemetry() = throw UnsupportedOperationException("no telemetry in guard test")
        override fun <T : Any?> withConnection(callback: SqlFunction1<Connection, T>): T =
            callback.apply(explodingConnection)
    }

    @Test
    fun `query отказывается выполнять пишущий запрос`() {
        val ctx = DefaultMigrationContext.test()

        val thrown = catchThrowable {
            with(ctx) { jdbc(db).query("insert into orders(id) values (1) returning id") { it.getLong(1) } }
        }

        // Иначе под dry-run этот запрос выполнился бы В БОЮ: query не проходит гейт и не должен.
        assertThat(thrown)
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("read statements only")
            .hasMessageContaining("executeReturning")
    }

    @Test
    fun `query пропускает select с ведущим комментарием`() {
        val ctx = DefaultMigrationContext.test(dryRun = true)

        // Дойти до соединения не должно — падение здесь означало бы, что запрос отвергнут разбором.
        val thrown = catchThrowable {
            with(ctx) { jdbc(db).query("-- отчёт за месяц\n  select 1") { it.getLong(1) } }
        }

        assertThat(thrown).isInstanceOf(AssertionError::class.java)
    }

    @Test
    fun `executeReturning гейтится dry-run'ом и не ходит в базу`() {
        val ctx = DefaultMigrationContext.test(dryRun = true)

        val ids = with(ctx) {
            jdbc(db).executeReturning("insert into orders(id) values (1) returning id") { it.getLong(1) }
        }

        assertThat(ids).isEmpty()
        assertThat(ctx.report.build().dryRunSkipped).containsEntry("jdbc.executeReturning", 1L)
    }

    @Test
    fun `tx-bound ops не даёт работать с транзакцией из чужого потока`() {
        val ctx = DefaultMigrationContext.test()
        val txOps = SqlOps(ctx, db, txConn = explodingConnection, inTx = true)
        val pool = Executors.newSingleThreadExecutor()

        val thrown = try {
            catchThrowable { pool.submit { txOps.execute("update orders set status = 'OK'") }.get(10, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }

        // java.sql.Connection не потокобезопасен: forEach внутри transactional раздал бы одно
        // соединение воркерам, и statement'ы перемешались бы в одной транзакции.
        assertThat(thrown.cause)
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("not thread-safe")
            .hasMessageContaining("transactional")
    }

    @Test
    fun `вложенный transactional запрещён и под dry-run тоже`() {
        val ctx = DefaultMigrationContext.test(dryRun = true)

        val thrown = catchThrowable {
            with(ctx) {
                transactional(jdbc(db)) {
                    transactional(jdbc(db)) { 1 }
                }
            }
        }

        // Проверка, работающая только в бою, давала бы зелёную репетицию при падающем прогоне.
        assertThat(thrown)
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("nested transactional")
    }
}
