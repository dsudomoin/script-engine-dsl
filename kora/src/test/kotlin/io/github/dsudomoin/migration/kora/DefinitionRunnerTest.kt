package io.github.dsudomoin.migration.kora

import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.MigrationDefinition
import io.github.dsudomoin.migration.MigrationPlan
import io.github.dsudomoin.migration.ScriptPolicy
import io.github.dsudomoin.migration.migration
import io.github.dsudomoin.migration.report.MigrationReport
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

    private fun config(
        run: String?,
        dryRun: Boolean = false,
        defaults: DefaultsValues = DefaultsValues(),
    ) = MigrationConfigValues(
        run = run,
        dryRun = dryRun,
        outputFolder = folder.toString(),
        defaults = defaults,
    )

    private fun runner(
        config: MigrationConfig,
        vararg definitions: MigrationDefinition,
        onReport: (MigrationReport) -> Unit = {},
    ): Pair<MigrationRunner, MutableList<Int>> {
        val codes = mutableListOf<Int>()
        val runner = MigrationRunner(config, definitions.toList(), null, onReport) { codes += it }
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

    @Test
    fun `провалившийся publish даёт exit 1 даже при LOG_AND_COMPLETE`() {
        val def = Def("EFF") {
            migration("EFF", "t", onUnhandled = ScriptPolicy.LOG_AND_COMPLETE) {
                source(items = { sequenceOf(1) }) {
                    publish("kafka.send") {
                        CompletableFuture.failedFuture<String>(RuntimeException("broker down"))
                    }
                }
            }
        }

        val (runner, codes) = runner(config("EFF"), def)
        runner.init()

        assertThat(codes).describedAs("потерянное сообщение не имеет права дать ноль").containsExactly(1)
    }

    @Test
    fun `терминальный отказ элемента считается один раз`() {
        val reports = mutableListOf<MigrationReport>()
        val def = Def("ONCE") {
            migration("ONCE", "t") { source(items = { sequenceOf(1) }) { error("boom") } }
        }

        val (runner, codes) = runner(config("ONCE"), def, onReport = { reports += it })
        runner.init()

        assertThat(codes).containsExactly(1)
        val r = reports.single()
        assertThat(r.processed).isEqualTo(1)
        assertThat(r.failed).describedAs("item посчитан один раз, а не дважды").isEqualTo(1)
        assertThat(r.unhandledFailures).isEqualTo(1)
        assertThat(r.processed).isEqualTo(r.successful + r.skipped + r.failed)
    }

    @Test
    fun `порог фазы главнее глобального дефолта`() {
        val def = Def("THR") {
            migration("THR", "t") {
                source(onItemError = ItemError.Skip, errorThreshold = 10, items = { sequenceOf(1, 2) }) {
                    error("skip me")
                }
            }
        }

        val (runner, codes) = runner(config("THR", defaults = DefaultsValues(errorThreshold = 1)), def)
        runner.init()

        assertThat(codes).describedAs("стадия разрешила 10 пропусков — прогон успешен").containsExactly(0)
    }

    @Test
    fun `глобальный порог применяется к фазе без своего значения`() {
        val def = Def("THR2") {
            migration("THR2", "t") {
                source(onItemError = ItemError.Skip, items = { sequenceOf(1, 2) }) { error("skip me") }
            }
        }

        val (runner, codes) = runner(config("THR2", defaults = DefaultsValues(errorThreshold = 1)), def)
        runner.init()

        assertThat(codes).containsExactly(1)
    }

    @Test
    fun `расхождение имени плана и definition — exit 2`() {
        val def = Def("SELECTED") { migration("OTHER", "t") { source(items = { sequenceOf(1) }) { } } }

        val (runner, codes) = runner(config("SELECTED"), def)
        runner.init()

        assertThat(codes).containsExactly(2)
    }

    @Test
    fun `успешный повторный прогон не наследует артефакты ошибок`() {
        val failing = Def("STALE") {
            migration("STALE", "t") {
                source(onItemError = ItemError.Skip, items = { sequenceOf(1) }) { error("boom") }
            }
        }
        runner(config("STALE"), failing).first.init()
        assertThat(folder.resolve("errors.csv")).exists()

        val reports = mutableListOf<MigrationReport>()
        val clean = Def("STALE") { migration("STALE", "t") { source(items = { sequenceOf(1) }) { } } }
        runner(config("STALE"), clean, onReport = { reports += it }).first.init()

        assertThat(folder.resolve("errors.csv")).doesNotExist()
        assertThat(folder.resolve("errors.log")).doesNotExist()
        assertThat(reports.single().errorsFile).isNull()
        assertThat(reports.single().tracesFile).isNull()
    }
}
