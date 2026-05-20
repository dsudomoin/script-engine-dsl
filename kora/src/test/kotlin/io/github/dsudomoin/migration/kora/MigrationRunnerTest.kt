package io.github.dsudomoin.migration.kora

import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationContext
import io.github.dsudomoin.migration.OnError
import io.github.dsudomoin.migration.ScriptPolicy
import io.github.dsudomoin.migration.csv.openCsv
import io.github.dsudomoin.migration.forEach
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

class MigrationRunnerTest {

    class DummyMig(private val hit: AtomicInteger) : Migration("DUMMY-1", "test") {
        override fun MigrationContext.migrate() {
            hit.incrementAndGet()
        }
    }

    @Test
    fun `runner запускает миграцию по имени и возвращает exit 0`(@TempDir tmp: Path) {
        val hit = AtomicInteger()
        val config = MigrationConfig(run = "DUMMY-1", outputFolder = tmp)
        var exitCode = -1
        val runner = MigrationRunner(config, listOf(DummyMig(hit)), customExecutor = null) { exitCode = it }

        runner.init()

        assertThat(hit.get()).isEqualTo(1)
        assertThat(exitCode).isEqualTo(0)
    }

    @Test
    fun `unknown migration name - exit равен 2`(@TempDir tmp: Path) {
        val config = MigrationConfig(run = "NO-SUCH", outputFolder = tmp)
        var exitCode = -1
        val runner = MigrationRunner(config, emptyList(), customExecutor = null) { exitCode = it }
        runner.init()
        assertThat(exitCode).isEqualTo(2)
    }

    @Test
    fun `run not set - runner idle, migration не вызывается`(@TempDir tmp: Path) {
        val hit = AtomicInteger()
        val config = MigrationConfig(run = null)
        var exitCode = -1
        val runner = MigrationRunner(config, listOf(DummyMig(hit)), customExecutor = null) { exitCode = it }
        runner.init()
        assertThat(hit.get()).isEqualTo(0)
        assertThat(exitCode).isEqualTo(-1)
    }

    @Test
    fun `migration log файл создаётся при наличии logback`(@TempDir tmp: Path) {
        val config = MigrationConfig(run = "DUMMY-1", outputFolder = tmp)
        val runner = MigrationRunner(config, listOf(DummyMig(AtomicInteger())), customExecutor = null) { }

        runner.init()

        val logFile = tmp.resolve("migration.log")
        assertThat(Files.exists(logFile)).isTrue()
        assertThat(Files.size(logFile)).isGreaterThan(0)
    }

    @Test
    fun `outputFolder доступен в migrate как ctx outputFolder`(@TempDir tmp: Path) {
        var observed: Path? = null
        val mig = object : Migration("OBS-1", "test") {
            override fun MigrationContext.migrate() {
                observed = outputFolder
            }
        }
        val config = MigrationConfig(run = "OBS-1", outputFolder = tmp)
        val runner = MigrationRunner(config, listOf(mig), customExecutor = null) { }
        runner.init()
        assertThat(observed).isEqualTo(tmp)
    }

    @Test
    fun `openCsv по имени резолвится в outputFolder`(@TempDir tmp: Path) {
        val mig = object : Migration("CSV-1", "test") {
            override fun MigrationContext.migrate() {
                val out = openCsv("processed.csv", "a", "b")
                out.row(1, 2)
            }
        }
        val config = MigrationConfig(run = "CSV-1", outputFolder = tmp)
        val runner = MigrationRunner(config, listOf(mig), customExecutor = null) { }
        runner.init()

        val csvFile = tmp.resolve("processed.csv")
        assertThat(Files.exists(csvFile)).isTrue()
        assertThat(Files.readAllLines(csvFile)).containsExactly("a,b", "1,2")
    }

    @Test
    fun `outputFolder автоматически создаётся mkdir -p если не существует`(@TempDir parent: Path) {
        val sub = parent.resolve("a/b/c")
        assertThat(Files.exists(sub)).isFalse()

        val config = MigrationConfig(run = "MKDIR-1", outputFolder = sub)
        val runner = MigrationRunner(config, listOf(object : Migration("MKDIR-1", "test") {
            override fun MigrationContext.migrate() { /* no-op */
            }
        }), customExecutor = null) { }
        runner.init()

        assertThat(Files.isDirectory(sub)).isTrue()
    }

    class MigWithResources(
        name: String,
        private val closedCount: AtomicInteger,
        private val throwAfterRegister: Boolean = false,
        policy: ScriptPolicy? = ScriptPolicy.FAIL_FAST,
    ) : Migration(name, "test", policy) {
        override fun MigrationContext.migrate() {
            register(AutoCloseable { closedCount.incrementAndGet() })
            register(AutoCloseable { closedCount.incrementAndGet() })
            register(AutoCloseable { closedCount.incrementAndGet() })
            if (throwAfterRegister) throw RuntimeException("boom from migrate")
        }
    }

    class MigWithBadResource(
        name: String,
    ) : Migration(name, "test") {
        override fun MigrationContext.migrate() {
            register(AutoCloseable { throw RuntimeException("close failed") })
        }
    }

    private fun config(name: String, tmp: Path) = MigrationConfig(run = name, outputFolder = tmp)

    @Test
    fun `registered resources закрываются после успешного migrate`(@TempDir tmp: Path) {
        val closed = AtomicInteger()
        var exitCode = -1
        val runner = MigrationRunner(
            config("RES-1", tmp),
            listOf(MigWithResources("RES-1", closed)),
            customExecutor = null,
        ) { exitCode = it }

        runner.init()

        assertThat(closed.get()).isEqualTo(3)
        assertThat(exitCode).isEqualTo(0)
    }

    @Test
    fun `registered resources закрываются когда migrate бросает с FAIL_FAST`(@TempDir tmp: Path) {
        val closed = AtomicInteger()
        var exitCode = -1
        val runner = MigrationRunner(
            config("RES-FF", tmp),
            listOf(MigWithResources("RES-FF", closed, throwAfterRegister = true, policy = ScriptPolicy.FAIL_FAST)),
            customExecutor = null,
        ) { exitCode = it }

        runner.init()

        assertThat(closed.get()).isEqualTo(3)
        assertThat(exitCode).isEqualTo(1)
    }

    @Test
    fun `registered resources закрываются когда migrate бросает с LOG_AND_COMPLETE`(@TempDir tmp: Path) {
        val closed = AtomicInteger()
        var exitCode = -1
        val runner = MigrationRunner(
            config("RES-LAC", tmp),
            listOf(
                MigWithResources(
                    "RES-LAC",
                    closed,
                    throwAfterRegister = true,
                    policy = ScriptPolicy.LOG_AND_COMPLETE
                )
            ),
            customExecutor = null,
        ) { exitCode = it }

        runner.init()

        assertThat(closed.get()).isEqualTo(3)
        assertThat(exitCode).isEqualTo(0)
    }

    @Test
    fun `Migration onUnhandled null - подхватывает config defaults onUnhandled`(@TempDir tmp: Path) {
        // Migration без явного onUnhandled (null) + config.defaults.onUnhandled = LOG_AND_COMPLETE
        // → миграция бросает unhandled, exit 0 (не fail-fast).
        val mig = object : Migration("DEFAULT-POL", "test", onUnhandled = null) {
            override fun MigrationContext.migrate() {
                throw RuntimeException("boom")
            }
        }
        val config = MigrationConfig(
            run = "DEFAULT-POL",
            outputFolder = tmp,
            defaults = MigrationConfig.Defaults(onUnhandled = ScriptPolicy.LOG_AND_COMPLETE),
        )
        var exitCode = -1
        val runner = MigrationRunner(config, listOf(mig), customExecutor = null) { exitCode = it }
        runner.init()
        assertThat(exitCode).isEqualTo(0)
    }

    @Test
    fun `Migration onUnhandled явный - перекрывает config`(@TempDir tmp: Path) {
        // Migration явно FAIL_FAST + config = LOG_AND_COMPLETE → миграция падает с exit 1.
        val mig = object : Migration("EXPLICIT-POL", "test", onUnhandled = ScriptPolicy.FAIL_FAST) {
            override fun MigrationContext.migrate() {
                throw RuntimeException("boom")
            }
        }
        val config = MigrationConfig(
            run = "EXPLICIT-POL",
            outputFolder = tmp,
            defaults = MigrationConfig.Defaults(onUnhandled = ScriptPolicy.LOG_AND_COMPLETE),
        )
        var exitCode = -1
        val runner = MigrationRunner(config, listOf(mig), customExecutor = null) { exitCode = it }
        runner.init()
        assertThat(exitCode).isEqualTo(1)
    }

    @Test
    fun `config defaults progressEvery wires в ctx defaultProgressEvery`(@TempDir tmp: Path) {
        var observed = -1
        val mig = object : Migration("PROG-1", "test") {
            override fun MigrationContext.migrate() {
                observed = defaultProgressEvery
            }
        }
        val config = MigrationConfig(
            run = "PROG-1",
            outputFolder = tmp,
            defaults = MigrationConfig.Defaults(progressEvery = 77),
        )
        val runner = MigrationRunner(config, listOf(mig), customExecutor = null) { }
        runner.init()
        assertThat(observed).isEqualTo(77)
    }

    @Test
    fun `errorThreshold realtime end-to-end - exit 1 после превышения`(@TempDir tmp: Path) {
        val mig = object : Migration("THR-1", "test") {
            override fun MigrationContext.migrate() {
                forEach((1..100).toList(), onError = OnError.Skip) { _ -> error("boom") }
            }
        }
        val config = MigrationConfig(
            run = "THR-1",
            outputFolder = tmp,
            defaults = MigrationConfig.Defaults(errorThreshold = 3),
        )
        var exitCode = -1
        val runner = MigrationRunner(config, listOf(mig), customExecutor = null) { exitCode = it }
        runner.init()
        assertThat(exitCode).isEqualTo(1)
    }

    @Test
    fun `customExecutor не выключается runner'ом - его жизнь принадлежит пользователю`(@TempDir tmp: Path) {
        // Runner регистрирует shutdownNow() как AutoCloseable ТОЛЬКО для дефолтного пула
        // (когда customExecutor == null). Custom executor от @Tag(MigrationExecutor) бин —
        // owned пользователем; runner не должен его выключать.
        val custom = Executors.newFixedThreadPool(2) { r ->
            Thread(r, "custom-mig-pool").apply { isDaemon = true }
        }
        val config = MigrationConfig(run = "CUSTOM-EXEC", outputFolder = tmp)
        val runner = MigrationRunner(
            config,
            listOf(object : Migration("CUSTOM-EXEC", "test") {
                override fun MigrationContext.migrate() { /* no-op */
                }
            }),
            customExecutor = custom,
        ) { }
        try {
            runner.init()
            // Главное: пул жив. shutdownNow() runner'ом НЕ вызывался.
            assertThat(custom.isShutdown).isFalse()
        } finally {
            custom.shutdownNow()
        }
    }

    @Test
    fun `defaults parallel = 0 - exit 2 с понятным сообщением`(@TempDir tmp: Path) {
        val config = MigrationConfig(
            run = "BAD-CONF",
            outputFolder = tmp,
            defaults = MigrationConfig.Defaults(parallel = 0),
        )
        var exitCode = -1
        val runner = MigrationRunner(
            config,
            listOf(object : Migration("BAD-CONF", "test") {
                override fun MigrationContext.migrate() { /* unreachable */
                }
            }),
            customExecutor = null,
        ) { exitCode = it }
        runner.init()
        assertThat(exitCode).isEqualTo(2)
    }

    @Test
    fun `падающий close не меняет exit code и не пробрасывает`(@TempDir tmp: Path) {
        var exitCode = -1
        val runner = MigrationRunner(
            config("RES-BAD", tmp),
            listOf(MigWithBadResource("RES-BAD")),
            customExecutor = null,
        ) { exitCode = it }

        runner.init()

        assertThat(exitCode).isEqualTo(0)
    }
}
