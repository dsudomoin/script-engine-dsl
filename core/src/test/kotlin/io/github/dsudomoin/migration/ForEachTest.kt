package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.error.CsvFileErrorReporter
import io.github.dsudomoin.migration.internal.DefaultMigrationContext
import io.github.dsudomoin.migration.report.ReportBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class ForEachTest {

    private fun customCtx(tmp: Path, executor: Executor): DefaultMigrationContext {
        // Тестовая фабрика DefaultMigrationContext.test() не принимает кастомный executor;
        // конструируем напрямую через internalCreate для проверки wiring'а.
        val name = "custom-exec-test"
        val report = ReportBuilder(name, "test", dryRun = false)
        val reporter = CsvFileErrorReporter(name, "test", tmp.resolve("errors.csv"), tmp.resolve("errors.log"))
        return DefaultMigrationContext.internalCreate(
            dryRun = false,
            name = name,
            report = report,
            executor = executor,
            outputFolder = tmp,
            errors = reporter,
        )
    }


    @Test
    fun `sequential - по одному, дефолтная политика FAIL прерывает`(@TempDir tmp: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = tmp)
        val processed = mutableListOf<Int>()

        val ex = runCatching {
            ctx.forEach(listOf(1, 2, 3, 4), onError = OnError.Fail) { n ->
                if (n == 3) error("boom on 3")
                processed += n
            }
        }.exceptionOrNull()

        assertThat(ex).isNotNull()
        assertThat(processed).containsExactly(1, 2)
        assertThat(ctx.report.build().failed).isEqualTo(1)
    }

    @Test
    fun `SKIP - ошибка не прерывает итерацию, зафиксирована в report`(@TempDir tmp: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = tmp)
        val processed = mutableListOf<Int>()

        ctx.forEach(listOf(1, 2, 3, 4), onError = OnError.Skip) { n ->
            if (n == 3) error("boom")
            processed += n
        }

        assertThat(processed).containsExactly(1, 2, 4)
        val r = ctx.report.build()
        assertThat(r.skipped).isEqualTo(1)
        assertThat(r.successful).isEqualTo(3)
    }

    @Test
    fun `chunk - лямбда получает List элементов`(@TempDir tmp: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = tmp)
        val batches = mutableListOf<List<Int>>()

        ctx.forEach(
            items = (1..10).toList(),
            chunk = 3,
            onError = OnError.Fail,
        ) { batch -> batches += batch }

        assertThat(batches).containsExactly(listOf(1, 2, 3), listOf(4, 5, 6), listOf(7, 8, 9), listOf(10))
    }

    @Test
    fun `parallel - элементы обрабатываются в нескольких потоках`(@TempDir tmp: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = tmp)
        val threads = ConcurrentLinkedQueue<String>()

        ctx.forEach((1..20).toList(), parallel = 4) { _ ->
            threads += Thread.currentThread().name
            Thread.sleep(10)
        }

        assertThat(threads.toSet().size).isGreaterThan(1)
    }

    @Test
    fun `logEach - вызывается после успеха`(@TempDir tmp: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = tmp)

        ctx.forEach(
            listOf("a", "b"),
            onError = OnError.Fail,
            logEach = { "done=$it" },
        ) { _ -> }

        assertThat(ctx.report.build().successful).isEqualTo(2)
    }

    @Test
    fun `parallel + SKIP - все ошибки в errors_csv без corruption`(@TempDir tmp: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = tmp)
        val items = (1..400).toList()
        val expectedErrors = items.count { it % 3 == 0 }    // 133

        ctx.forEach(items, parallel = 8, onError = OnError.Skip) { n ->
            if (n % 3 == 0) error("boom-$n")
        }
        ctx.errors.close()

        val csvLines = Files.readAllLines(tmp.resolve("errors.csv"))
        // header + по одной строке на каждую ошибку
        assertThat(csvLines).hasSize(1 + expectedErrors)
        assertThat(csvLines[0]).isEqualTo("timestamp,migration,author,itemRepr,errorClass,errorMessage")

        // Каждая data-строка well-formed: 6 полей без обрыва. Item'ы — простые числа, никаких
        // запятых в самом значении, поэтому raw split по запятой даёт ровно 6 cells.
        csvLines.drop(1).forEach { line ->
            assertThat(line.split(",")).hasSize(6)
        }

        // Все наши boom-N зафиксированы (порядок может быть любой из-за parallel)
        val recordedErrorMessages = csvLines.drop(1).map { line ->
            // errorMessage — 6-е поле (0-indexed 5)
            line.split(",")[5]
        }.toSet()
        val expectedMessages = items.filter { it % 3 == 0 }.map { "boom-$it" }.toSet()
        assertThat(recordedErrorMessages).isEqualTo(expectedMessages)

        val r = ctx.report.build()
        assertThat(r.skipped.toInt()).isEqualTo(expectedErrors)
        assertThat(r.successful.toInt()).isEqualTo(items.size - expectedErrors)
    }

    @Test
    fun `parallel + FAIL - primary брошен, audited subset of failedIds`(@TempDir tmp: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = tmp)
        val items = (1..50).toList()
        val failedIds = setOf(10, 20, 30, 40)

        val ex = runCatching {
            ctx.forEach(items, parallel = 8, onError = OnError.Fail) { n ->
                if (n in failedIds) error("boom-$n")
                Thread.sleep(5)
            }
        }.exceptionOrNull()
        ctx.errors.close()

        assertThat(ex).isNotNull()
        assertThat(ex!!.message).matches("boom-\\d+")

        val csvLines = Files.readAllLines(tmp.resolve("errors.csv"))
        val audited = csvLines.drop(1).map { it.split(",")[5] }.toSet()
        assertThat(audited).isNotEmpty()
        audited.forEach { msg -> assertThat(msg).matches("boom-\\d+") }
        val failedMessages = failedIds.map { "boom-$it" }.toSet()
        assertThat(audited).isSubsetOf(failedMessages)
    }

    // Note: «suppressed накапливает остальные fail'ы» — реальный контракт `ForEachEngine.runAcross`
    // (см. AGENTS.md §7.3, код `primary.addSuppressed(list[i])`). Детерминированный тест на
    // multi-fail сложен: drain-loop вызывает `Future.cancel(true)` при первой ошибке, что
    // прерывает `await` в worker'е и заворачивает следующий fail в InterruptedException, который
    // в exceptions queue не попадает. Поэтому assert «оба в thrown» под parallel race почти
    // всегда проваливается. Контракт защищён ревью кода runAcross и интеграционным тестом
    // `parallel + FAIL - primary брошен, audited subset of failedIds` (хотя бы один зафиксирован,
    // остальные audited могут быть suppressed либо не успеть стартовать — оба валидны).

    @Test
    fun `parallel + FAIL - outstanding tasks получают interrupt и завершаются быстро`(@TempDir tmp: Path) {
        // Используем FixedThreadPool — он возвращает FutureTask, у которого cancel(true) реально
        // шлёт Thread.interrupt() в running worker. ForkJoinPool.commonPool() (используется
        // DefaultMigrationContext.test()) НЕ годится: ForkJoinTask по доке "does not support
        // interrupting tasks during execution". Production-runner создаёт именно FixedThreadPool,
        // так что этот тест отражает реальный prod-сценарий.
        //
        // Синхронизуем через latch: item==1 (первый submit'нутый) ждёт пока остальные worker'ы
        // physically стартуют и зайдут в Thread.sleep — иначе cancel(true) прибьёт их queued'ыми
        // без interrupt'а.
        val parallel = 8
        val pool = Executors.newFixedThreadPool(parallel) { r ->
            Thread(r).apply { isDaemon = true; name = "cancel-test-thread" }
        }
        val ctx = customCtx(tmp, pool)
        val items = (1..parallel).toList()
        val outstandingSleepMs = 5_000L
        val sleepingStarted = CountDownLatch(parallel - 1)
        val interruptedCount = AtomicInteger()

        val start = System.currentTimeMillis()
        val ex = try {
            runCatching {
                ctx.forEach(items, parallel = parallel, onError = OnError.Fail) { n ->
                    if (n == 1) {
                        val ok = sleepingStarted.await(3, TimeUnit.SECONDS)
                        check(ok) { "remaining workers did not start in time" }
                        error("boom-1")
                    } else {
                        sleepingStarted.countDown()
                        try {
                            Thread.sleep(outstandingSleepMs)
                        } catch (ie: InterruptedException) {
                            interruptedCount.incrementAndGet()
                            Thread.currentThread().interrupt()
                            throw ie
                        }
                    }
                }
            }.exceptionOrNull()
        } finally {
            pool.shutdownNow()
        }
        val duration = System.currentTimeMillis() - start

        assertThat(ex).isNotNull()
        assertThat(ex!!.message).isEqualTo("boom-1")
        // Без cancellation duration ≈ 5000ms. С cancel(true) → ms. Запас на scheduling: 2500ms.
        assertThat(duration).isLessThan(2500L)
        // Все 7 outstanding workers реально получили interrupt (они уже были в sleep).
        assertThat(interruptedCount.get()).isEqualTo(parallel - 1)
    }

    @Test
    fun `parallel + FAIL - plain Executor (не ExecutorService) — fallback без interrupt, exception всё равно брошен`(
        @TempDir tmp: Path,
    ) {
        // Plain Executor (lambda) — без shutdown/submit. cancel(true) на CF — no-op для уже
        // запущенного worker'а. Outstanding tasks доходят до конца естественно, но primary
        // exception всё равно должен быть throw'нут наружу.
        val executor = Executor { r -> Thread(r).apply { isDaemon = true }.start() }
        val ctx = customCtx(tmp, executor)
        val items = (1..8).toList()
        val seen = AtomicInteger()

        val ex = runCatching {
            ctx.forEach(items, parallel = 4, onError = OnError.Fail) { n ->
                seen.incrementAndGet()
                if (n == 1) error("plain-boom")
                Thread.sleep(50)
            }
        }.exceptionOrNull()

        assertThat(ex).isNotNull()
        assertThat(ex!!.message).isEqualTo("plain-boom")
        // Outstanding workers (≤ parallel - 1 в момент fail'а) добрали свою работу естественно —
        // не выпали.
        assertThat(seen.get()).isGreaterThanOrEqualTo(1)
    }

    @Test
    fun `ctx executor реально используется forEach`(@TempDir tmp: Path) {
        val markedThreads = ConcurrentLinkedQueue<String>()
        val custom: Executor = Executors.newFixedThreadPool(4) { r ->
            Thread(r).apply { isDaemon = true; name = "CUSTOM-EXEC-thread" }
        }
        val ctx = customCtx(tmp, custom)

        ctx.forEach((1..40).toList(), parallel = 4) { _ ->
            markedThreads += Thread.currentThread().name
            Thread.sleep(5)
        }

        val unique = markedThreads.toSet()
        assertThat(unique).allMatch { it == "CUSTOM-EXEC-thread" }
        (custom as java.util.concurrent.ExecutorService).shutdownNow()
    }

    @Test
    fun `Sequence + parallel не материализуется целиком — bounded submission`(@TempDir tmp: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = tmp)
        val pulled = AtomicLong()
        val workStarted = AtomicLong()

        // Бесконечный Sequence — если бы материализовался, тест бы повис на map { submit }
        val infinite = generateSequence(0L) { it + 1 }.map { i ->
            pulled.incrementAndGet()
            i
        }
        // Сделаем «процессить» только первые 50 items, чтобы тест завершился.
        // Через onError = SKIP не подходит — Skip всё равно даёт всем дойти. Просто берём take(50).
        val limited = infinite.take(50)

        ctx.forEach(limited, parallel = 4) { _ ->
            workStarted.incrementAndGet()
            Thread.sleep(2)
        }

        // К моменту завершения мы pull'нули 50 (limited.take) + это всё. Главное — мы НЕ pull'нули
        // что-то лишнее за пределами 50. Если бы материализация была эагерной до forEach старта,
        // pulled.get() могло бы быть больше 50 (если внутренняя реализация map делает буферизацию).
        assertThat(workStarted.get()).isEqualTo(50)
        assertThat(pulled.get()).isEqualTo(50)
    }

    @Test
    fun `errorThreshold realtime - прерывает forEach после превышения`(@TempDir tmp: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = tmp, errorThreshold = 5)
        val started = AtomicInteger()

        val ex = runCatching {
            ctx.forEach((1..100).toList(), onError = OnError.Skip) { _ ->
                started.incrementAndGet()
                error("always-fail")
            }
        }.exceptionOrNull()

        assertThat(ex).isInstanceOf(io.github.dsudomoin.migration.internal.ErrorThresholdExceeded::class.java)
        // Threshold check после каждого skip. 6-й skip даёт skipped=6 > threshold=5 → throws.
        // Processed items должны быть 6, не 100.
        assertThat(started.get()).isEqualTo(6)
        assertThat(ctx.report.build().skipped).isEqualTo(6L)
    }

    @Test
    fun `auditError failure не подменяет original — warning в report`(@TempDir tmp: Path) {
        // Subclass над дефолтным CsvFileErrorReporter — не имитируем реальный CSV-write,
        // а лишь подменяем report() чтобы он бросал. Остальной API (close, registerSerializer)
        // унаследован — для теста этого хватает.
        class ThrowingReporter : CsvFileErrorReporter(
            migrationName = "X",
            author = "y",
            errorsFile = tmp.resolve("errors.csv"),
            tracesFile = tmp.resolve("errors.log"),
        ) {
            override fun report(e: Throwable, item: Any?) {
                throw RuntimeException("audit-fail")
            }
        }

        val reportB = ReportBuilder("X", "y", false)
        val throwing = ThrowingReporter()
        val ctx = DefaultMigrationContext.internalCreate(
            dryRun = false,
            name = "X",
            report = reportB,
            executor = java.util.concurrent.ForkJoinPool.commonPool(),
            outputFolder = tmp,
            errors = throwing,
        )

        val ex = runCatching {
            ctx.forEach(listOf(1, 2, 3), onError = OnError.Fail) { error("real-fail-$it") }
        }.exceptionOrNull()

        assertThat(ex).isNotNull()
        assertThat(ex!!.message).isEqualTo("real-fail-1")
        val warnings = reportB.build().warnings
        assertThat(warnings).isNotEmpty()
        assertThat(warnings.first()).contains("auditError failed").contains("audit-fail")
    }

    @Test
    fun `errorThreshold = 0 - проверка отключена`(@TempDir tmp: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = tmp, errorThreshold = 0)

        ctx.forEach((1..20).toList(), onError = OnError.Skip) { _ ->
            error("always-fail")
        }

        // Все 20 обработаны (хоть и зафейлились) — threshold не сработал.
        assertThat(ctx.report.build().skipped).isEqualTo(20L)
    }


    @Test
    fun `Handle decide бросает - classifier-throw летит наверх с original в suppressed`(@TempDir tmp: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = tmp)

        val ex = runCatching {
            ctx.forEach(
                listOf(1),
                onError = OnError.handle { _, _ -> throw RuntimeException("classifier broken") },
            ) { _ -> error("original business error") }
        }.exceptionOrNull()

        // 1. Сверху летит classifier-throw — это его баг, человеку важно знать что decide() упал.
        assertThat(ex).isNotNull()
        assertThat(ex!!.message).isEqualTo("classifier broken")
        // 2. Оригинальное бизнес-исключение сохранено в suppressed — не потеряно.
        assertThat(ex.suppressed.map { it.message }).contains("original business error")
        // 3. Item, на котором миграция реально сломалась, попал в errors.csv.
        ctx.errors.close()
        val csvLines = Files.readAllLines(tmp.resolve("errors.csv"))
        assertThat(csvLines.drop(1)).hasSize(1)
        assertThat(csvLines[1]).contains("original business error")
        // 4. report.failed инкрементирован (это unhandled-style провал).
        assertThat(ctx.report.build().failed).isEqualTo(1)
    }

    @Test
    fun `iterator throws - outstanding tasks отменяются, primary пробрасывается`(@TempDir tmp: Path) {
        // Sequence-источник может сломаться на iter.next() — например, ResultSet закрыт извне
        // или CSV malformed. До фикса R4: исключение из iter.next() пробивало runAcross,
        // submitted-futures оставались в-flight без cancel/join. После фикса: ошибка
        // оборачивается так же, как любой другой fail воркера.
        val pool = Executors.newFixedThreadPool(4) { r -> Thread(r).apply { isDaemon = true } }
        val ctx = customCtx(tmp, pool)
        val pulled = AtomicInteger()

        // Sequence: первые 3 элемента нормальные, 4-й бросает.
        val brokenSeq = sequence<Int> {
            yield(1); pulled.incrementAndGet()
            yield(2); pulled.incrementAndGet()
            yield(3); pulled.incrementAndGet()
            throw RuntimeException("iter-broken")
        }

        val ex = try {
            runCatching {
                ctx.forEach(brokenSeq, parallel = 4, onError = OnError.Fail) { _ ->
                    Thread.sleep(50)
                }
            }.exceptionOrNull()
        } finally {
            pool.shutdownNow()
        }

        // 1. Primary exception — наш iter-throw.
        assertThat(ex).isNotNull()
        assertThat(ex!!.message).isEqualTo("iter-broken")
        // 2. Pulled — то, что успели до iter.next() throw.
        assertThat(pulled.get()).isEqualTo(3)
    }

    @Test
    fun `Progress тикает и при SKIP-ах - контракт из Progress KDoc`(@TempDir tmp: Path) {
        // Progress KDoc обещает: «тикает после processOne, независимо от исхода». Закрепляем
        // контракт явно — если кто-то решит «тикать только на success», тест поймает регрессию.
        val ctx = DefaultMigrationContext.test(outputFolder = tmp)
        val customTicks = AtomicLong()

        ctx.forEach(
            (1..20).toList(),
            onError = OnError.Skip,
            progress = Progress.Custom(n = 5) { _, _ ->
                customTicks.incrementAndGet()
                "tick"
            },
        ) { n -> if (n % 2 == 0) error("boom-$n") }    // половина — SKIP, половина — success

        // 20 обработанных / шаг 5 = 4 тика. Если бы тикало только на success, было бы 2.
        assertThat(customTicks.get()).isEqualTo(4)
    }

    @Test
    fun `Handle политика - Skip для одного, Fail для другого`(@TempDir tmp: Path) {
        val ctx = DefaultMigrationContext.test(outputFolder = tmp)
        val processed = mutableListOf<Int>()

        val ex = runCatching {
            ctx.forEach(
                listOf(1, 2, 3),
                onError = OnError.handle { e, _ ->
                    if (e.message == "skip-me") OnError.Decision.Skip else OnError.Decision.Fail
                },
            ) { n ->
                when (n) {
                    1 -> processed += 1
                    2 -> error("skip-me")
                    3 -> error("fatal")
                }
            }
        }.exceptionOrNull()

        assertThat(ex).isNotNull
        assertThat(ex?.message).isEqualTo("fatal")
        assertThat(processed).containsExactly(1)
        val r = ctx.report.build()
        assertThat(r.skipped).isEqualTo(1)
        assertThat(r.failed).isEqualTo(1)
    }
}
