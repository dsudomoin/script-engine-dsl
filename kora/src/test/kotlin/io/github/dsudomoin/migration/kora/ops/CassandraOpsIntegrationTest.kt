package io.github.dsudomoin.migration.kora.ops

import com.datastax.oss.driver.api.core.CqlSession
import io.github.dsudomoin.migration.internal.RunContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.CassandraContainer
import java.net.InetSocketAddress

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("docker")
class CassandraOpsIntegrationTest {

    private val cassandra = CassandraContainer("cassandra:4.1").apply { start() }
    private lateinit var session: CqlSession

    @BeforeAll
    fun setup() {
        session = CqlSession.builder()
            .addContactPoint(InetSocketAddress(cassandra.host, cassandra.firstMappedPort))
            .withLocalDatacenter(cassandra.localDatacenter)
            .build()

        session.execute(
            "create keyspace if not exists t with replication = {'class':'SimpleStrategy','replication_factor':1}"
        )
        session.execute("use t")
        session.execute("create table if not exists items(id text primary key, value int)")
    }

    @BeforeEach
    fun reset() {
        session.execute("truncate t.items")
        session.execute("insert into t.items(id, value) values ('a', 1)")
        session.execute("insert into t.items(id, value) values ('b', 2)")
        session.execute("insert into t.items(id, value) values ('c', 3)")
    }

    @AfterAll
    fun teardown() {
        session.close()
        cassandra.stop()
    }

    @Test
    fun `query читает строки`() {
        val ctx = RunContext.test()
        val ids = with(handler(ctx)) {
            cassandra(session).query("select id from t.items") { it.getString("id")!! }
        }
        assertThat(ids).containsExactlyInAnyOrder("a", "b", "c")
    }

    @Test
    fun `execute под dry-run не меняет таблицу`() {
        val dry = RunContext.test(dryRun = true)
        with(handler(dry)) { cassandra(session).execute("update t.items set value = 99 where id = 'a'") }

        val real = RunContext.test()
        val v = with(handler(real)) {
            cassandra(session).query("select value from t.items where id = 'a'") { it.getInt("value") }
        }.first()
        assertThat(v).isEqualTo(1)
    }

    @Test
    fun `query with IN ids - List bound через setList с inferred element class`() {
        val ctx = RunContext.test()
        val ids = listOf("a", "c")     // 'b' намеренно пропущен
        val result = with(handler(ctx)) {
            cassandra(session).query(
                "select id, value from t.items where id in :ids",
                "ids" to ids,
            ) { it.getString("id")!! to it.getInt("value") }
        }
        assertThat(result).containsExactlyInAnyOrder("a" to 1, "c" to 3)
    }

    @Test
    fun `write into set column - Set bound через setSet`() {
        // CQL IN-clause всегда требует list<...>, поэтому Set→IN тестировать бессмысленно
        // (driver вернёт CodecNotFound). Реальный use case Set-ветки в setExplicit — write
        // в колонку типа set<text>.
        session.execute("create table if not exists t.tagged(id text primary key, tags set<text>)")
        val ctx = RunContext.test()
        with(handler(ctx)) {
            cassandra(session).execute(
                "insert into t.tagged(id, tags) values (:id, :tags)",
                "id" to "row1",
                "tags" to setOf("red", "green"),
            )
        }
        val tags = with(handler(ctx)) {
            cassandra(session).query(
                "select tags from t.tagged where id = :id",
                "id" to "row1",
            ) { row -> row.getSet("tags", String::class.java) }
        }.single()
        assertThat(tags).containsExactlyInAnyOrder("red", "green")
    }

    @Test
    fun `query with IN - пустой List бросает IllegalArgumentException с понятной message`() {
        val ctx = RunContext.test()
        val ex = runCatching {
            with(handler(ctx)) {
                cassandra(session).query(
                    "select id from t.items where id in :ids",
                    "ids" to emptyList<String>(),
                ) { it.getString("id")!! }
            }
        }.exceptionOrNull()
        assertThat(ex).isInstanceOf(IllegalArgumentException::class.java)
        assertThat(ex!!.message).contains("empty or all-null", ":ids", "element type cannot be inferred")
    }

    @Test
    fun `batch insert - множество строк за раз`() {
        val real = RunContext.test()
        val items = listOf("d" to 4, "e" to 5)
        with(handler(real)) {
            cassandra(session).batch("insert into t.items(id, value) values (:id, :v)", items) { b, it ->
                b.setString("id", it.first); b.setInt("v", it.second)
            }
        }
        val all = with(handler(real)) {
            cassandra(session).query("select id from t.items") { it.getString("id")!! }
        }
        assertThat(all).contains("d", "e")
    }
}
