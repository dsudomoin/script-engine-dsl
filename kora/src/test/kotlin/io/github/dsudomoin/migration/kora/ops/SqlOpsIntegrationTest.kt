package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.internal.DefaultMigrationContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.PostgreSQLContainer
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import java.sql.Connection
import java.sql.DriverManager

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("docker")
class SqlOpsIntegrationTest {

    private val pg = PostgreSQLContainer("postgres:16-alpine").apply { start() }
    private lateinit var db: JdbcConnectionFactory

    @BeforeAll
    fun setup() {
        db = TestJdbcConnectionFactory(pg.jdbcUrl, pg.username, pg.password)
        withSetupConn { c ->
            c.createStatement().use { it.executeUpdate("create table orders(id bigserial primary key, status int)") }
        }
    }

    @BeforeEach
    fun resetData() {
        withSetupConn { c ->
            c.createStatement().use { it.executeUpdate("truncate orders restart identity") }
            c.prepareStatement("insert into orders(status) values (?)").use { ps ->
                ps.setInt(1, 1); ps.executeUpdate()
                ps.setInt(1, 1); ps.executeUpdate()
                ps.setInt(1, 2); ps.executeUpdate()
            }
        }
    }

    @AfterAll
    fun teardown() {
        pg.stop()
    }

    @Test
    fun `query читает строки`() {
        val ctx = DefaultMigrationContext.test()
        val ids = with(ctx) {
            jdbc(db).query("select id from orders where status = :s", "s" to 1) { it.getLong("id") }
        }
        assertThat(ids).hasSize(2)
    }

    @Test
    fun `execute под dry-run не меняет БД`() {
        val dryCtx = DefaultMigrationContext.test(dryRun = true)
        with(dryCtx) { jdbc(db).execute("update orders set status = 99") }

        val realCtx = DefaultMigrationContext.test(dryRun = false)
        val count = with(realCtx) {
            jdbc(db).query("select count(*) as c from orders where status = 99") { it.getInt("c") }
        }.first()
        assertThat(count).isEqualTo(0)
    }

    @Test
    fun `validate - missing parameter бросает IllegalArgumentException`() {
        val ctx = DefaultMigrationContext.test()
        val ex = runCatching {
            with(ctx) {
                jdbc(db).query("select id from orders where status = :s") { it.getLong("id") }
            }
        }.exceptionOrNull()
        assertThat(ex).isInstanceOf(IllegalArgumentException::class.java)
        assertThat(ex!!.message).contains("Missing SQL parameters: [s]")
    }

    @Test
    fun `validate - extra parameter бросает IllegalArgumentException`() {
        val ctx = DefaultMigrationContext.test()
        val ex = runCatching {
            with(ctx) {
                jdbc(db).query("select id from orders", "extra" to 1) { it.getLong("id") }
            }
        }.exceptionOrNull()
        assertThat(ex).isInstanceOf(IllegalArgumentException::class.java)
        assertThat(ex!!.message).contains("Unused parameters: [extra]")
    }

    @Test
    fun `validate - duplicate keys бросают IllegalArgumentException`() {
        val ctx = DefaultMigrationContext.test()
        val ex = runCatching {
            with(ctx) {
                jdbc(db).query("select id from orders where status = :s", "s" to 1, "s" to 2) { it.getLong("id") }
            }
        }.exceptionOrNull()
        assertThat(ex).isInstanceOf(IllegalArgumentException::class.java)
        assertThat(ex!!.message).contains("Duplicate parameter keys: [s]")
    }

    @Test
    fun `parseSql - colon name внутри строки не парсится как параметр`() {
        val ctx = DefaultMigrationContext.test()
        val result = with(ctx) {
            jdbc(db).query("select ':fake' as v") { it.getString("v") }
        }
        assertThat(result).containsExactly(":fake")
    }

    @Test
    fun `parseSql - colon name внутри block comment не парсится как параметр`() {
        val ctx = DefaultMigrationContext.test()
        // `:fake` спрятан внутри /* ... */ — не должен попасть в список named-params,
        // SQL валиден, query выполняется без ошибки про missing parameter.
        val result = with(ctx) {
            jdbc(db).query("select /* :fake */ 'literal' as v") { it.getString("v") }
        }
        assertThat(result).containsExactly("literal")
    }

    @Test
    fun `parseSql - colon name внутри dollar-quoted строки не парсится`() {
        val ctx = DefaultMigrationContext.test()
        // `:fake` спрятан внутри $$...$$ — PostgreSQL читает как литерал, наш парсер тоже.
        val result = with(ctx) {
            jdbc(db).query("select \$\$:fake\$\$ as v") { it.getString("v") }
        }
        assertThat(result).containsExactly(":fake")
    }

    @Test
    fun `parseSql - colon name внутри tagged dollar-quoted строки не парсится`() {
        val ctx = DefaultMigrationContext.test()
        // `$body$:fake$body$` — tagged variant, тоже не должен зацепиться.
        val result = with(ctx) {
            jdbc(db).query("select \$body\$:fake\$body\$ as v") { it.getString("v") }
        }
        assertThat(result).containsExactly(":fake")
    }

    @Test
    fun `parseSql - block comment не мешает реальному named-параметру вне него`() {
        val ctx = DefaultMigrationContext.test()
        // /* :fake */ комментарий, а :s — настоящий параметр.
        val result = with(ctx) {
            jdbc(db).query(
                "select /* :fake */ :s::int as v",
                "s" to 42,
            ) { it.getInt("v") }
        }
        assertThat(result).containsExactly(42)
    }

    @Test
    fun `transactional - оба execute коммитятся при успешном выходе`() {
        val ctx = DefaultMigrationContext.test()
        with(ctx) {
            transactional(jdbc(db)) {
                execute("insert into orders(status) values (:s)", "s" to 50)
                execute("insert into orders(status) values (:s)", "s" to 51)
            }
        }
        val count = with(ctx) {
            jdbc(db).query("select count(*) as c from orders where status in (50, 51)") { it.getInt("c") }
        }.first()
        assertThat(count).isEqualTo(2)
    }

    @Test
    fun `transactional - rollback при throw, ни одной строки`() {
        val ctx = DefaultMigrationContext.test()
        try {
            with(ctx) {
                transactional(jdbc(db)) {
                    execute("insert into orders(status) values (:s)", "s" to 60)
                    throw RuntimeException("boom")
                }
            }
        } catch (e: RuntimeException) {
            assertThat(e.message).isEqualTo("boom")
        }
        val count = with(ctx) {
            jdbc(db).query("select count(*) as c from orders where status = 60") { it.getInt("c") }
        }.first()
        assertThat(count).isEqualTo(0)
    }

    @Test
    fun `transactional - query внутри tx видит свои uncommitted inserts`() {
        val ctx = DefaultMigrationContext.test()
        val visible = with(ctx) {
            transactional(jdbc(db)) {
                execute("insert into orders(status) values (:s)", "s" to 70)
                query("select count(*) as c from orders where status = 70") { it.getInt("c") }.first()
            }
        }
        assertThat(visible).isEqualTo(1)
    }

    @Test
    fun `nested transactional - бросает IllegalStateException`() {
        val ctx = DefaultMigrationContext.test()
        val ops = with(ctx) { jdbc(db) }
        try {
            with(ctx) {
                transactional(ops) {
                    transactional(this) {
                        execute("insert into orders(status) values (:s)", "s" to 80)
                    }
                }
            }
            assertThat(false).withFailMessage("ожидался IllegalStateException").isTrue()
        } catch (e: IllegalStateException) {
            assertThat(e.message).contains("nested transactional")
        }
        val count = with(ctx) {
            jdbc(db).query("select count(*) as c from orders where status = 80") { it.getInt("c") }
        }.first()
        assertThat(count).isEqualTo(0)
    }

    @Test
    fun `nested transactional через внешний free-mode ops тоже бросает`() {
        // Тонкий escape hatch: пользователь сохраняет ссылку на free-mode `ops` (inTx == false)
        // и вызывает `transactional(ops)` ПОВТОРНО изнутри уже открытой tx. Без ThreadLocal-guard'а
        // в SqlOps это открывало бы вторую независимую tx — нарушение «nested не поддерживается».
        val ctx = DefaultMigrationContext.test()
        val outerOps = with(ctx) { jdbc(db) }                   // inTx == false
        try {
            with(ctx) {
                transactional(outerOps) {
                    // Внутри — `this` это tx-bound SqlOps. Но юзер передаёт *внешний* outerOps:
                    transactional(outerOps) {
                        execute("insert into orders(status) values (:s)", "s" to 81)
                    }
                }
            }
            assertThat(false).withFailMessage("ожидался IllegalStateException").isTrue()
        } catch (e: IllegalStateException) {
            assertThat(e.message).contains("nested transactional")
            assertThat(e.message).contains("already open on this thread")
        }
        // Ничего не записалось: outer tx тоже не закоммитилась (исключение вылетело наверх).
        val count = with(ctx) {
            jdbc(db).query("select count(*) as c from orders where status = 81") { it.getInt("c") }
        }.first()
        assertThat(count).isEqualTo(0)
    }

    @Test
    fun `последовательные transactional на одном потоке не блокируют друг друга`() {
        // Sanity: ThreadLocal-флаг должен сбрасываться по выходу из transactional, иначе второй
        // (не вложенный, а *последующий*) `transactional` упал бы false-positive'ом.
        val ctx = DefaultMigrationContext.test()
        with(ctx) {
            transactional(jdbc(db)) {
                execute("insert into orders(status) values (:s)", "s" to 82)
            }
            transactional(jdbc(db)) {
                execute("insert into orders(status) values (:s)", "s" to 83)
            }
        }
        val count = with(ctx) {
            jdbc(db).query("select count(*) as c from orders where status in (82, 83)") { it.getInt("c") }
        }.first()
        assertThat(count).isEqualTo(2)
    }

    @Test
    fun `transactional под dry-run - execute не пишет, реальный tx не открывается`() {
        val dryCtx = DefaultMigrationContext.test(dryRun = true)
        with(dryCtx) {
            transactional(jdbc(db)) {
                execute("insert into orders(status) values (:s)", "s" to 91)
            }
            transactional(jdbc(db)) {
                execute("insert into orders(status) values (:s)", "s" to 92)
            }
        }
        // 1) Ни одна строка не записана
        val realCtx = DefaultMigrationContext.test()
        val count = with(realCtx) {
            jdbc(db).query("select count(*) as c from orders where status in (91, 92)") { it.getInt("c") }
        }.first()
        assertThat(count).isEqualTo(0)
        // 2) transactional-блоки попали в dryRunSkipped breakdown (2 вызова)
        val report = dryCtx.report.build()
        assertThat(report.dryRunSkipped).containsEntry("jdbc.transactional", 2L)
    }

    @Test
    fun `batch update меняет множество строк`() {
        val ctx = DefaultMigrationContext.test(dryRun = false)
        val ids = with(ctx) {
            jdbc(db).query("select id from orders") { it.getLong("id") }
        }
        with(ctx) {
            jdbc(db).batch("update orders set status = ? where id = ?", ids) { ps, id ->
                ps.setInt(1, 7); ps.setLong(2, id)
            }
        }
        val after = with(ctx) {
            jdbc(db).query("select count(*) as c from orders where status = 7") { it.getInt("c") }
        }.first()
        assertThat(after).isEqualTo(3)
    }

    @Test
    fun `stream проходит по всем строкам с fetchSize меньше total`() {
        val ctx = DefaultMigrationContext.test()
        val ids = mutableListOf<Long>()
        with(ctx) {
            jdbc(db).stream(
                "select id from orders order by id",
                fetchSize = 1,                              // принудительно постраничный курсор
                mapper = { it.getLong("id") },
            ) { rows -> rows.forEach { ids += it } }
        }
        assertThat(ids).hasSize(3)
    }

    @Test
    fun `stream возвращает значение из consume callback`() {
        val ctx = DefaultMigrationContext.test()
        val sum = with(ctx) {
            jdbc(db).stream(
                "select id from orders",
                mapper = { it.getLong("id") },
            ) { rows -> rows.sum() }
        }
        // 3 строки с id 1,2,3 после truncate restart identity
        assertThat(sum).isEqualTo(1L + 2L + 3L)
    }

    @Test
    fun `stream с named-параметрами фильтрует и валидирует placeholders`() {
        val ctx = DefaultMigrationContext.test()
        val ids = with(ctx) {
            jdbc(db).stream(
                "select id from orders where status = :s",
                "s" to 1,
                mapper = { it.getLong("id") },
            ) { rows -> rows.toList() }
        }
        assertThat(ids).hasSize(2)
    }

    @Test
    fun `stream - missing parameter бросает IllegalArgumentException`() {
        val ctx = DefaultMigrationContext.test()
        val ex = runCatching {
            with(ctx) {
                jdbc(db).stream(
                    "select id from orders where status = :s",
                    mapper = { it.getLong("id") },
                ) { rows -> rows.count() }
            }
        }.exceptionOrNull()
        assertThat(ex).isInstanceOf(IllegalArgumentException::class.java)
        assertThat(ex!!.message).contains("Missing SQL parameters: [s]")
    }

    @Test
    fun `stream - fetchSize не положительный бросает IllegalArgumentException`() {
        val ctx = DefaultMigrationContext.test()
        val ex = runCatching {
            with(ctx) {
                jdbc(db).stream(
                    "select id from orders",
                    fetchSize = 0,
                    mapper = { it.getLong("id") },
                ) { rows -> rows.count() }
            }
        }.exceptionOrNull()
        assertThat(ex).isInstanceOf(IllegalArgumentException::class.java)
        assertThat(ex!!.message).contains("fetchSize must be > 0")
    }

    @Test
    fun `stream - exception в consume пробрасывается и connection корректно освобождается`() {
        val ctx = DefaultMigrationContext.test()
        val ex = runCatching {
            with(ctx) {
                jdbc(db).stream(
                    "select id from orders",
                    mapper = { it.getLong("id") },
                ) { rows ->
                    rows.first()                    // вытащили первую строку
                    throw RuntimeException("abort")  // прерываемся
                }
            }
        }.exceptionOrNull()
        assertThat(ex).isInstanceOf(RuntimeException::class.java)
        assertThat(ex!!.message).isEqualTo("abort")

        // Connection освобождён — следующая операция на БД работает.
        val count = with(ctx) {
            jdbc(db).query("select count(*) as c from orders") { it.getInt("c") }
        }.first()
        assertThat(count).isEqualTo(3)
    }

    @Test
    fun `stream внутри transactional видит uncommitted insert`() {
        val ctx = DefaultMigrationContext.test()
        val visible = with(ctx) {
            transactional(jdbc(db)) {
                execute("insert into orders(status) values (:s)", "s" to 88)
                stream(
                    "select status from orders where status = :s", "s" to 88,
                    mapper = { it.getInt("status") }) { rows -> rows.toList() }
            }
        }
        assertThat(visible).containsExactly(88)
    }

    @Test
    fun `stream - sequence невалидна после возврата из consume`() {
        val ctx = DefaultMigrationContext.test()
        val leaked: Sequence<Long> = with(ctx) {
            jdbc(db).stream(
                "select id from orders",
                mapper = { it.getLong("id") },
            ) { rows -> rows }                          // пользователь утащил ссылку наружу
        }
        // Любая попытка итерации после возврата из consume → SQL-исключение про closed RS.
        val ex = runCatching { leaked.toList() }.exceptionOrNull()
        assertThat(ex)
            .isInstanceOf(java.sql.SQLException::class.java)
    }

    private fun withSetupConn(block: (Connection) -> Unit) {
        DriverManager.getConnection(pg.jdbcUrl, pg.username, pg.password).use(block)
    }
}
