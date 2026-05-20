package io.github.dsudomoin.migration.kora.pilot

import com.datastax.oss.driver.api.core.CqlSession
import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.csv.openCsv
import io.github.dsudomoin.migration.csv.readCsv
import io.github.dsudomoin.migration.forEach
import io.github.dsudomoin.migration.internal.DefaultMigrationContext
import io.github.dsudomoin.migration.kora.ops.cassandra
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.TempDir
import org.testcontainers.containers.CassandraContainer
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ComparisonPilotTest {

    private val primaryC = CassandraContainer("cassandra:4.1").apply { start() }
    private val replicaC = CassandraContainer("cassandra:4.1").apply { start() }

    private lateinit var primary: CqlSession
    private lateinit var replica: CqlSession

    data class Config(val batchSize: Int, val concurrencyLevel: Int, val initialContractId: String?, val output: Path)

    class TaskPilot(
        private val primary: CqlSession,
        private val replica: CqlSession,
        private val config: Config,
    ) : Migration(name = "PILOT-1", author = "plan") {

        override fun MigrationContext.migrate() {
            val contracts = readCsv("pilot/contracts.csv", classpath = true) { it["contract"]!! }
                .filter { config.initialContractId == null || it > config.initialContractId!! }
                .toList()

            val processed = openCsv(config.output.resolve("processed.csv"), "contract")
            val failed = openCsv(config.output.resolve("failed.csv"), "contract", "reason")

            forEach(contracts, chunk = config.batchSize, parallel = config.concurrencyLevel, onError = OnError.Skip) { batch ->
                batch.forEach { c ->
                    val p = cassandra(primary).query("select value from t.items where contract = :c", "c" to c) { it.getString("value") }.firstOrNull()
                    val r = cassandra(replica).query("select value from t.items where contract = :c", "c" to c) { it.getString("value") }.firstOrNull()
                    if (p != r) failed.row(c, "primary=$p, replica=$r")
                    processed.row(c)
                }
            }
        }
    }

    @BeforeAll
    fun setup() {
        primary = session(primaryC)
        replica = session(replicaC)
        initSchema(primary)
        initSchema(replica)
        // Populate: A-1, A-2, A-3, A-4 same values; A-2 differs on replica to trigger mismatch
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
        val ctx = DefaultMigrationContext.test(outputFolder = tmp)
        val pilot = TaskPilot(
            primary, replica,
            config = Config(batchSize = 2, concurrencyLevel = 2, initialContractId = null, output = tmp),
        )

        with(ctx) { pilot.run { migrate() } }
        ctx.closeRegistered()

        val processedLines = Files.readAllLines(tmp.resolve("processed.csv"))
        val failedLines = Files.readAllLines(tmp.resolve("failed.csv"))

        // processed.csv — header + 4 contracts (in any order because of parallel)
        assertThat(processedLines).hasSize(5)
        assertThat(processedLines[0]).isEqualTo("contract")
        assertThat(processedLines.drop(1)).containsExactlyInAnyOrder("A-1", "A-2", "A-3", "A-4")

        // failed.csv — header + 1 mismatch row for A-2
        assertThat(failedLines).hasSize(2)
        assertThat(failedLines[0]).isEqualTo("contract,reason")
        assertThat(failedLines[1]).startsWith("A-2,")
        assertThat(failedLines[1]).contains("primary=2", "replica=X")

        val report = ctx.report.build()
        assertThat(report.successful).isEqualTo(2) // 2 batches (2 chunks of 2)
        assertThat(report.skipped).isEqualTo(0)
    }

    @Test
    fun `initialContractId фильтрует - обрабатывается только A-3 и A-4`(@TempDir tmp: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = tmp)
        val pilot = TaskPilot(
            primary, replica,
            config = Config(batchSize = 10, concurrencyLevel = 1, initialContractId = "A-2", output = tmp),
        )

        with(ctx) { pilot.run { migrate() } }
        ctx.closeRegistered()

        val processed = Files.readAllLines(tmp.resolve("processed.csv")).drop(1)
        assertThat(processed).containsExactly("A-3", "A-4") // A-1, A-2 filtered by initialContractId > "A-2"
    }
}
