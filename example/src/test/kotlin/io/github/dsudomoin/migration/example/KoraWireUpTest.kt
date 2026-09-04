package io.github.dsudomoin.migration.example

import io.github.dsudomoin.migration.MigrationDefinition
import io.github.dsudomoin.migration.WriteOutcome
import io.github.dsudomoin.migration.kora.DefaultsValues
import io.github.dsudomoin.migration.kora.MigrationConfig
import io.github.dsudomoin.migration.kora.MigrationConfigValues
import io.github.dsudomoin.migration.kora.MigrationExit
import io.github.dsudomoin.migration.kora.MigrationModule
import io.github.dsudomoin.migration.migration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.io.TempDir
import ru.tinkoff.kora.application.graph.Node
import ru.tinkoff.kora.common.Component
import ru.tinkoff.kora.common.KoraApp
import ru.tinkoff.kora.config.hocon.HoconConfigModule
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.TimeUnit

/**
 * Проверяет то, чего не видит ни один тест модуля `:kora`: **работает ли документированный
 * wire-up на настоящем графе Kora**. Остальные тесты конструируют `MigrationRunner` руками и
 * потому зелены даже когда компонент не попадает в граф, конфиг не резолвится или у поля конфига
 * нет экстрактора — а у пользователя приложение при этом либо не собирается, либо стартует и
 * молча ничего не делает.
 *
 * Модуль `:example` для этого и заведён: KSP-расширение конфига Kora не может сгенерировать
 * экстрактор в том же модуле, где он уже сгенерирован, поэтому потребителя надо изображать
 * отдельным модулем — что заодно ближе к реальности.
 */
@KoraApp
interface ExampleTestApp : HoconConfigModule, MigrationModule

/** Наблюдаемое состояние прогона: компоненты создаёт граф, достать их иначе неоткуда. */
object Probe {
    val workerThreads: MutableSet<String> = ConcurrentHashMap.newKeySet()
    val written = ConcurrentLinkedQueue<Int>()
    val nestedSeen = ConcurrentLinkedQueue<String>()

    @Volatile
    var exitCode: Int? = null

    @Volatile
    var sawDryRun: Boolean? = null

    @Volatile
    var parallelismReached = false

    fun reset() {
        workerThreads.clear()
        written.clear()
        nestedSeen.clear()
        exitCode = null
        sawDryRun = null
        parallelismReached = false
    }
}

/** Перехватывает код возврата вместо `exitProcess` — иначе прогон убил бы JVM тест-раннера. */
@Component
class TestExit : MigrationExit {
    override fun exit(code: Int) {
        Probe.exitCode = code
    }
}

@Component
class ParallelProbeMigration : MigrationDefinition {
    override val name = "WIRE-UP-PARALLEL"

    override fun plan() = migration(name = name, author = "test") {
        // Барьер на четырёх участников — жёсткая проверка настоящего параллелизма. На пуле
        // фиксированного размера в один поток он не соберётся и тест упадёт по таймауту. Тихая
        // деградация «работает, просто в один поток» здесь недопустима.
        val barrier = CyclicBarrier(4)

        source(
            parallel = 4,
            items = {
                Probe.sawDryRun = dryRun
                (1..4).asSequence()
            },
        ) { n ->
            Probe.workerThreads.add(Thread.currentThread().name)
            barrier.await(20, TimeUnit.SECONDS)
            Probe.parallelismReached = true
            write("probe.write", args = mapOf("n" to n)) {
                Probe.written.add(n)
                WriteOutcome.Applied
            }
        }
    }
}

/** Две параллельные стадии подряд: пул обязан выдержать обе, не залипнув между ними. */
@Component
class NestedProbeMigration : MigrationDefinition {
    override val name = "WIRE-UP-NESTED"

    override fun plan() = migration(name = name, author = "test") {
        source(name = "outer", parallel = 2, items = { sequenceOf("a", "b") }) { outer ->
            Probe.nestedSeen.add("${outer}1")
        }
        source(name = "inner", parallel = 2, items = { sequenceOf("a", "b") }) { outer ->
            Probe.nestedSeen.add("${outer}2")
        }
    }
}

/** Пишет мимо `write { }` — под dry-run запись уходит в бой, и отчёт обязан это заметить. */
@Component
class UngatedProbeMigration : MigrationDefinition {
    override val name = "WIRE-UP-UNGATED"

    override fun plan() = migration(name = name, author = "test") {
        source(items = { (1..3).asSequence() }) { n -> Probe.written.add(n) }
    }
}

class KoraWireUpTest {

    @BeforeEach
    fun setUp() = Probe.reset()

    @Test
    @Timeout(60)
    fun `граф собирается по документированному wire-up и миграция реально выполняется`(@TempDir tmp: Path) {
        runGraph(MigrationConfigValues(run = "WIRE-UP-PARALLEL", outputFolder = tmp.toString()))

        assertThat(Probe.exitCode)
            .describedAs("runner обязан отработать при сборке графа и вернуть код возврата")
            .isEqualTo(0)
        assertThat(Probe.written)
            .describedAs("план должен выполниться целиком")
            .containsExactlyInAnyOrder(1, 2, 3, 4)
        assertThat(Probe.sawDryRun)
            .describedAs("migration.dryRun из конфига доезжает до контекста")
            .isFalse()
    }

    @Test
    @Timeout(60)
    fun `source с parallel = 4 действительно занимает четыре потока`(@TempDir tmp: Path) {
        runGraph(MigrationConfigValues(run = "WIRE-UP-PARALLEL", outputFolder = tmp.toString()))

        assertThat(Probe.parallelismReached)
            .describedAs("барьер на четырёх участников собрался")
            .isTrue()
        assertThat(Probe.workerThreads)
            .describedAs("четыре одновременно работающих воркера — четыре разных потока")
            .hasSize(4)
    }

    @Test
    @Timeout(60)
    fun `две параллельные стадии подряд не встают в deadlock`(@TempDir tmp: Path) {
        runGraph(MigrationConfigValues(run = "WIRE-UP-NESTED", outputFolder = tmp.toString()))

        assertThat(Probe.exitCode).isEqualTo(0)
        assertThat(Probe.nestedSeen).containsExactlyInAnyOrder("a1", "a2", "b1", "b2")
    }

    @Test
    @Timeout(60)
    fun `dryRun из конфига долетает до контекста и гейтит записи`(@TempDir tmp: Path) {
        runGraph(MigrationConfigValues(run = "WIRE-UP-PARALLEL", dryRun = true, outputFolder = tmp.toString()))

        assertThat(Probe.sawDryRun)
            .describedAs("проброс config.dryRun -> ctx.dryRun; регрессия здесь означает боевые записи под MIGRATION_DRY_RUN=true")
            .isTrue()
        assertThat(Probe.written)
            .describedAs("под dry-run записи не выполняются")
            .isEmpty()
        assertThat(Probe.exitCode).isEqualTo(0)
    }

    @Test
    @Timeout(60)
    fun `dry-run без единой гейтнутой записи поднимает предупреждение в лог прогона`(@TempDir tmp: Path) {
        runGraph(MigrationConfigValues(run = "WIRE-UP-UNGATED", dryRun = true, outputFolder = tmp.toString()))

        assertThat(Probe.written)
            .describedAs("запись мимо write { } под dry-run выполняется по-настоящему — это и есть ловушка")
            .containsExactlyInAnyOrder(1, 2, 3)

        val log = tmp.resolve("migration.log").toFile().readText()
        assertThat(log)
            .describedAs("единственный наблюдаемый признак забытого write { } должен быть громким")
            .contains("intercepted 0 writes")
    }

    @Test
    @Timeout(60)
    fun `неизвестное имя миграции даёт код возврата 2`(@TempDir tmp: Path) {
        runGraph(MigrationConfigValues(run = "NO-SUCH-MIGRATION", outputFolder = tmp.toString()))

        assertThat(Probe.exitCode).isEqualTo(2)
    }

    /**
     * Поднимает настоящий граф, подменив в нём только узел конфига. Подмена — штатный механизм
     * Kora (`ApplicationGraphDraw.replaceNode`); он позволяет гонять сценарии с разными
     * `migration.*` без отдельного `application.conf` на каждый.
     */
    private fun runGraph(config: MigrationConfig) {
        val draw = ExampleTestAppGraph.graph()

        @Suppress("UNCHECKED_CAST")
        val configNode = draw.findNodeByType(MigrationConfig::class.java) as Node<MigrationConfig>
        draw.replaceNode(configNode) { config }

        val graph = draw.init()
        graph.release()
    }
}
