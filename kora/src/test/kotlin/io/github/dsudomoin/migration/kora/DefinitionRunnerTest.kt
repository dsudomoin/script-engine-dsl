package io.github.dsudomoin.migration.kora

import io.github.dsudomoin.migration.MigrationDefinition
import io.github.dsudomoin.migration.MigrationPlan
import io.github.dsudomoin.migration.ScriptPolicy
import io.github.dsudomoin.migration.migration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.concurrent.CompletableFuture

class DefinitionRunnerTest {

    @TempDir
    lateinit var folder: Path

    private class Def(
        override val name: String,
        private val build: () -> MigrationPlan,
    ) : MigrationDefinition {
        var planBuilt = 0
            private set

        override fun plan(): MigrationPlan {
            planBuilt++
            return build()
        }
    }

    private fun config(run: String?, dryRun: Boolean = false) = MigrationConfigValues(
        run = run,
        dryRun = dryRun,
        outputFolder = folder.toString(),
    )

    private fun runner(config: MigrationConfig, vararg definitions: MigrationDefinition): Pair<MigrationRunner, MutableList<Int>> {
        val codes = mutableListOf<Int>()
        val runner = MigrationRunner(config, definitions.toList(), null) { codes += it }
        return runner to codes
    }

    @Test
    fun `выбранная миграция исполняется и даёт exit 0`() {
        val seen = mutableListOf<Int>()
        val def = Def("RUN-ME") {
            migration("RUN-ME", "t") {
                source(items = { sequenceOf(1, 2) }) { item -> seen += item }
            }
        }

        val (runner, codes) = runner(config("RUN-ME"), def)
        runner.init()

        assertThat(seen).containsExactly(1, 2)
        assertThat(codes).containsExactly(0)
    }

    @Test
    fun `план невыбранной миграции вообще не строится`() {
        val chosen = Def("CHOSEN") { migration("CHOSEN", "t") { source(items = { emptySequence<Int>() }) { } } }
        val other = Def("OTHER") { error("plan of a non-selected migration must not be built") }

        val (runner, codes) = runner(config("CHOSEN"), chosen, other)
        runner.init()

        assertThat(codes).containsExactly(0)
        assertThat(chosen.planBuilt).isEqualTo(1)
        assertThat(other.planBuilt).isZero()
    }

    @Test
    fun `неизвестное имя — exit 2`() {
        val def = Def("KNOWN") { migration("KNOWN", "t") { source(items = { emptySequence<Int>() }) { } } }

        val (runner, codes) = runner(config("NOPE"), def)
        runner.init()

        assertThat(codes).containsExactly(2)
        assertThat(def.planBuilt).isZero()
    }

    @Test
    fun `дублирующиеся имена — exit 2`() {
        val a = Def("SAME") { migration("SAME", "t") { source(items = { emptySequence<Int>() }) { } } }
        val b = Def("SAME") { migration("SAME", "t") { source(items = { emptySequence<Int>() }) { } } }

        val (runner, codes) = runner(config("SAME"), a, b)
        runner.init()

        assertThat(codes).containsExactly(2)
    }

    @Test
    fun `без migration_run runner простаивает`() {
        val def = Def("ANY") { error("must not build") }

        val (runner, codes) = runner(config(null), def)
        runner.init()

        assertThat(codes).isEmpty()
    }

    @Test
    fun `необработанная ошибка — exit 1 по FAIL_FAST`() {
        val def = Def("BOOM") {
            migration("BOOM", "t") { source(items = { sequenceOf(1) }) { error("kaboom") } }
        }

        val (runner, codes) = runner(config("BOOM"), def)
        runner.init()

        assertThat(codes).containsExactly(1)
    }

    @Test
    fun `LOG_AND_COMPLETE даёт exit 0 на необработанной ошибке`() {
        val def = Def("SOFT") {
            migration("SOFT", "t", onUnhandled = ScriptPolicy.LOG_AND_COMPLETE) {
                source(items = { sequenceOf(1) }) { error("kaboom") }
            }
        }

        val (runner, codes) = runner(config("SOFT"), def)
        runner.init()

        assertThat(codes).containsExactly(0)
    }

    @Test
    fun `неподтверждённые эффекты поднимают exit до 1 даже при LOG_AND_COMPLETE`() {
        val def = Def("LATE") {
            migration("LATE", "t", onUnhandled = ScriptPolicy.LOG_AND_COMPLETE) {
                source(
                    completionTimeout = java.time.Duration.ofMillis(200),
                    items = { sequenceOf(1) },
                ) {
                    publish("kafka.send") { CompletableFuture<String>() }
                }
            }
        }

        val (runner, codes) = runner(config("LATE"), def)
        runner.init()

        assertThat(codes).containsExactly(1)
    }

    @Test
    fun `dry-run из конфига доезжает до контекста`() {
        var seenDryRun: Boolean? = null
        val def = Def("DRY") {
            migration("DRY", "t") {
                source(items = { seenDryRun = dryRun; emptySequence<Int>() }) { }
            }
        }

        val (runner, codes) = runner(config("DRY", dryRun = true), def)
        runner.init()

        assertThat(seenDryRun).isTrue()
        assertThat(codes).containsExactly(0)
    }
}
