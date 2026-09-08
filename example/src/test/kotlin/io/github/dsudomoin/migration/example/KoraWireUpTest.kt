package io.github.dsudomoin.migration.example

import io.github.dsudomoin.migration.Migration
import io.github.dsudomoin.migration.MigrationScope
import io.github.dsudomoin.migration.kora.MigrationConfig
import io.github.dsudomoin.migration.kora.MigrationConfigValues
import io.github.dsudomoin.migration.kora.MigrationExit
import io.github.dsudomoin.migration.kora.MigrationModule
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
 * потому зелены даже когда компонент не попадает в граф, конфиг не резолвится или у поля
 * конфига нет экстрактора — а у пользователя приложение при этом либо не собирается, либо
 * стартует и молча ничего не делает.
 *
 * Модуль `:example` для этого и заведён: KSP-расширение конфига Kora не может
 * сгенерировать экстрактор в том же модуле, где он уже сгенерирован, поэтому потребителя
 * надо изображать отдельным модулем — что заодно ближе к реальности.
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
class ParallelProbeMigration : Migration("WIRE-UP-PARALLEL") {

    override fun MigrationScope.run() {
        Probe.sawDryRun = dryRun

        // Барьер на четырёх участников — жёсткая проверка настоящего параллелизма. В один поток
        // он не соберётся и тест упадёт по таймауту. Тихая деградация «работает, просто в один
        // поток» здесь недопустима.
        val barrier = CyclicBarrier(4)

        each((1..4).toList(), parallel = 4) { n ->
            Probe.workerThreads.add(Thread.currentThread().name)
            barrier.await(20, TimeUnit.SECONDS)
            Probe.parallelismReached = true
            if (!dryRun) Probe.written.add(n)
        }
    }
}

/** Два параллельных цикла подряд: пул обязан выдержать оба, не залипнув между ними. */
@Component
class NestedProbeMigration : Migration("WIRE-UP-NESTED") {

    override fun MigrationScope.run() {
        each(listOf("a", "b"), parallel = 2) { outer -> Probe.nestedSeen.add("${outer}1") }
        each(listOf("a", "b"), parallel = 2) { outer -> Probe.nestedSeen.add("${outer}2") }
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
            .describedAs("тело миграции должно выполниться целиком")
            .containsExactlyInAnyOrder(1, 2, 3, 4)
        assertThat(Probe.sawDryRun)
            .describedAs("migration.dryRun из конфига доезжает до скоупа")
            .isFalse()
    }

    @Test
    @Timeout(60)
    fun `each с parallel = 4 действительно занимает четыре потока`(@TempDir tmp: Path) {
        runGraph(MigrationConfigValues(run = "WIRE-UP-PARALLEL", outputFolder = tmp.toString()))

        assertThat(Probe.parallelismReached)
            .describedAs("барьер на четырёх участниках собрался")
            .isTrue()
        assertThat(Probe.workerThreads)
            .describedAs("четыре одновременно работающих воркера — четыре разных потока")
            .hasSize(4)
    }

    @Test
    @Timeout(60)
    fun `два параллельных цикла подряд не встают в deadlock`(@TempDir tmp: Path) {
        runGraph(MigrationConfigValues(run = "WIRE-UP-NESTED", outputFolder = tmp.toString()))

        assertThat(Probe.exitCode).isEqualTo(0)
        assertThat(Probe.nestedSeen).containsExactlyInAnyOrder("a1", "a2", "b1", "b2")
    }

    @Test
    @Timeout(60)
    fun `dryRun из конфига долетает до скоупа`(@TempDir tmp: Path) {
        runGraph(MigrationConfigValues(run = "WIRE-UP-PARALLEL", dryRun = true, outputFolder = tmp.toString()))

        assertThat(Probe.sawDryRun)
            .describedAs("проброс config.dryRun -> scope.dryRun; регрессия здесь означает боевые записи под MIGRATION_DRY_RUN=true")
            .isTrue()
        assertThat(Probe.written)
            .describedAs("миграция сама не пишет под dry-run")
            .isEmpty()
        assertThat(Probe.exitCode).isEqualTo(0)
    }

    @Test
    @Timeout(60)
    fun `неизвестное имя миграции даёт код возврата 2`(@TempDir tmp: Path) {
        runGraph(MigrationConfigValues(run = "NO-SUCH-MIGRATION", outputFolder = tmp.toString()))

        assertThat(Probe.exitCode).isEqualTo(2)
    }

    @Test
    @Timeout(60)
    fun `артефакты прогона ложатся в outputFolder`(@TempDir tmp: Path) {
        runGraph(MigrationConfigValues(run = "WIRE-UP-NESTED", outputFolder = tmp.toString()))

        assertThat(tmp.resolve("migration.log")).exists()
    }

    /**
     * Поднимает настоящий граф, подменив в нём только узел конфига. Подмена — штатный
     * механизм Kora (`ApplicationGraphDraw.replaceNode`); он позволяет гонять сценарии с разными
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
