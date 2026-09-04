package io.github.dsudomoin.migration.kora.pilot

import com.datastax.oss.driver.api.core.CqlSession
import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.MigrationDefinition
import io.github.dsudomoin.migration.csv.readCsv
import io.github.dsudomoin.migration.internal.RunContext
import io.github.dsudomoin.migration.internal.PlanInterpreter
import io.github.dsudomoin.migration.kora.ops.cassandra
import io.github.dsudomoin.migration.migration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.TempDir
import org.testcontainers.containers.CassandraContainer
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("docker")
class ComparisonPilotTest {

    private val primaryC = CassandraContainer("cassandra:4.1").apply { start() }
    private val replicaC = CassandraContainer("cassandra:4.1").apply { start() }

    private lateinit var primary: CqlSession
    private lateinit var replica: CqlSession

    data class Config(val batchSize: Int, val initialContractId: String?)

    /**
     * Архетип сравнения двух кластеров на новом API.
     *
     * Батчинг — обычный `Sequence.chunked`, отдельного параметра движка больше нет.
     */
    class TaskPilot(
        private val primary: CqlSession,
        private val replica: CqlSession,
        private val config: Config,
    ) : MigrationDefinition {

        override val name = "PILOT-1"

        override fun plan() = migration(name = name, author = "plan") {
            val processed = output("processed.csv", "contract")
            val failed = output("failed.csv", "contract", "reason")

            source(
                onItemError = ItemError.Skip,
                items = {
                    readCsv("pilot/contracts.csv", classpath = true) { it.getValue("contract") }
                        .filter { config.initialContractId == null || it > config.initialContractId }
                        .chunked(config.batchSize)
                },
            ) { batch ->
                batch.forEach { contract ->
                    val p = valueOf(primary, contract)
                    val r = valueOf(replica, contract)
                    if (p != r) failed.row(contract, "primary=$p, replica=$r")
                    processed.row(contract)
                }
            }
        }

        private fun io.github.dsudomoin.migration.HandlerScope.valueOf(session: CqlSession, contract: String): String? =
            cassandra(session)
                .query("select value from t.items where contract = :c", "c" to contract) { it.getString("value") }
                .firstOrNull()
    }

    @BeforeAll
    fun setup() {
        primary = session(primaryC)
        replica = session(replicaC)
        initSchema(primary)
        initSchema(replica)
        insert(primary, listOf("A-1" to "1", "A-2" to "2", "A-3" to "3", "A-4" to "4"))
        insert(replica, listOf("A-1" to "1", "A-2" to "X", "A-3" to "3", "A-4" to "4"))
    }

    @AfterAll
    fun teardown() {
        primary.close(); replica.close()
        primaryC.stop(); replicaC.stop()
    }

    private fun session(c: CassandraContainer<*>): CqlSession =
        CqlSession.builder()
            .addContactPoint(InetSocketAddress(c.host, c.firstMappedPort))
            .withLocalDatacenter(c.localDatacenter)
            .build()

    private fun initSchema(s: CqlSession) {
        s.execute("create keyspace if not exists t with replication = {'class':'SimpleStrategy','replication_factor':1}")
        s.execute("create table if not exists t.items(contract text primary key, value text)")
    }

    private fun insert(s: CqlSession, rows: List<Pair<String, String>>) {
        rows.forEach { (c, v) -> s.execute("insert into t.items(contract, value) values ('$c', '$v')") }
    }

    @Test
    fun `pilot фиксирует mismatch для A-2, обрабатывает все контракты`(@TempDir tmp: Path) {
        val ctx = RunContext.test(outputFolder = tmp)
        val pilot = TaskPilot(primary, replica, Config(batchSize = 2, initialContractId = null))

        PlanInterpreter(ctx).execute(pilot.plan())
        ctx.closeRegistered()

        val processedLines = Files.readAllLines(tmp.resolve("processed.csv"))
        val failedLines = Files.readAllLines(tmp.resolve("failed.csv"))

        assertThat(processedLines).hasSize(5)
        assertThat(processedLines[0]).isEqualTo("contract")
        assertThat(processedLines.drop(1)).containsExactlyInAnyOrder("A-1", "A-2", "A-3", "A-4")

        assertThat(failedLines).hasSize(2)
        assertThat(failedLines[0]).isEqualTo("contract,reason")
        assertThat(failedLines[1]).startsWith("A-2,")
        assertThat(failedLines[1]).contains("primary=2", "replica=X")

        val report = ctx.report.build()
        // Элемент стадии — батч, а не контракт: 4 контракта по 2 дают 2 элемента.
        assertThat(report.successful).isEqualTo(2)
        assertThat(report.skipped).isZero()
    }

    @Test
    fun `initialContractId фильтрует - обрабатывается только A-3 и A-4`(@TempDir tmp: Path) {
        val ctx = RunContext.test(outputFolder = tmp)
        val pilot = TaskPilot(primary, replica, Config(batchSize = 10, initialContractId = "A-2"))

        PlanInterpreter(ctx).execute(pilot.plan())
        ctx.closeRegistered()

        val processed = Files.readAllLines(tmp.resolve("processed.csv")).drop(1)
        assertThat(processed).containsExactly("A-3", "A-4")
    }
}
