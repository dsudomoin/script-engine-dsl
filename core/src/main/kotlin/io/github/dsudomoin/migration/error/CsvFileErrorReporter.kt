package io.github.dsudomoin.migration.error

import io.github.dsudomoin.migration.csv.csvEscape
import java.io.BufferedWriter
import java.io.PrintWriter
import java.io.StringWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap

/**
 * Авто-аудитор item-уровневых ошибок. Пишет CSV-строку в [errorsFile] и стектрейс в [tracesFile]
 * для каждой ошибки, прошедшей через `ItemError.Skip` / `Handle→Skip` в стадии.
 *
 * Файлы создаются лениво (на первой ошибке) — если миграция пройдёт без ошибок, файлов не будет.
 * Доступен на [io.github.dsudomoin.migration.RunScope.errors]; пользователь обычно вызывает только
 * [includeItem] для per-type сериализации, всё остальное — автоматика.
 *
 * **Thread-safe.** [report] и [registerSerializer] безопасно вызывать из параллельных воркеров
 * стадии. [includeItem] фактически делегирует в [registerSerializer].
 *
 * @param maxItemReprLength максимальная длина строки `itemRepr` в CSV. Длинные значения
 *                          обрезаются с `...`. Дефолт 500.
 * @param includeStackTrace писать ли стектрейсы в [tracesFile]. `false` — только CSV (компактнее).
 */
open class CsvFileErrorReporter(
    private val migrationName: String,
    private val author: String,
    val errorsFile: Path,
    val tracesFile: Path,
    private val maxItemReprLength: Int = 500,
    private val includeStackTrace: Boolean = true,
) : ErrorReporter {

    private val writeLock = Any()

    // Защищены writeLock. Volatile, чтобы read без lock увидел проинициализированное значение
    // в fast-path (если уже создан).
    @Volatile
    private var csvWriter: BufferedWriter? = null

    @Volatile
    private var logWriter: BufferedWriter? = null

    @Volatile
    private var closed = false

    private val serializers = ConcurrentHashMap<Class<*>, (Any) -> String>()

    // Memoized resolved serializers — findSerializer обходит superclasses/interfaces, что для
    // глубоких иерархий и миллионов ошибок дорого. Кэшируем уже-найденный (или его отсутствие).
    private val serializerCache = ConcurrentHashMap<Class<*>, ResolvedSerializer>()

    private data class ResolvedSerializer(val f: ((Any) -> String)?)

    /**
     * Низкоуровневая регистрация сериализатора по `Class<*>`. Обычно используй inline-reified
     * extension [includeItem] из [ErrorReporter].
     *
     * Lookup в [report] идёт сначала точно по классу, потом по супертипам и интерфейсам —
     * `includeItem<Map>` сработает и для `LinkedHashMap`.
     *
     * **Регистрируй сериализаторы в `validate { }` или в `items { }`, до первой обработки.** Метод thread-safe,
     * но регистрация во время параллельной обработки (когда воркеры уже могли закешировать
     * resolved-сериализаторы) приводит к инвалидации кэша и transient cache miss — корректность
     * не страдает, но рендеринг item'а в `errors.csv` может на нескольких записях пойти через
     * `toString()` вместо нового сериализатора, пока кэш не перестроится.
     */
    override fun registerSerializer(cls: Class<*>, f: (Any) -> String) {
        serializers[cls] = f
        // Регистрация после первых ошибок — инвалидируем кэш resolved serializers, чтобы новый
        // сериализатор подхватился даже для типов, по которым уже спрашивали.
        serializerCache.clear()
    }

    /**
     * Записать ошибку в `errors.csv` + (опционально) стектрейс в `errors.log`. Уже вызывается
     * автоматически интерпретатором плана и [io.github.dsudomoin.migration.RunScope.auditError].
     *
     * **Thread-safe, сериализуется через общий lock.** Все воркеры пишут в общий файл через
     * `synchronized(writeLock)`. На типичном error rate (десятки в минуту) это незаметно; если
     * SKIP-ы массовые (десятки тысяч на параллельных воркерах) — это точка сериализации.
     * Mitigation: подними `errorThreshold`, чтобы массовые SKIP-ы вообще прерывали миграцию.
     *
     * `open` — переопределяй в тестах (например, чтобы симулировать падающий аудитор и
     * проверить, что `ForEachEngine` ловит ошибку аудита в warning, не подменяя original).
     */
    override fun report(e: Throwable, item: Any?) {
        val ts = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
        val itemRepr = renderItem(item)
        val errorClass = e.javaClass.simpleName
        val errorMessage = e.message ?: ""
        val trace = if (includeStackTrace) stackTrace(e) else null

        synchronized(writeLock) {
            if (closed) return
            val csv = ensureCsv()
            appendCsvRow(csv, ts, migrationName, author, itemRepr, errorClass, errorMessage)
            if (trace != null) {
                val log = ensureLog()
                log.write("[$ts] [$migrationName] item=$itemRepr\n")
                log.write(trace)
                log.write("\n")
                log.flush()
            }
        }
    }

    private fun renderItem(item: Any?): String {
        if (item == null) return ""
        val serializer = findSerializer(item.javaClass)
        val raw = serializer?.invoke(item) ?: item.toString()
        return if (raw.length > maxItemReprLength) raw.take(maxItemReprLength) + "..." else raw
    }

    private fun findSerializer(cls: Class<*>): ((Any) -> String)? {
        serializerCache[cls]?.let { return it.f }
        val resolved = resolveSerializer(cls)
        serializerCache[cls] = ResolvedSerializer(resolved)
        return resolved
    }

    private fun resolveSerializer(cls: Class<*>): ((Any) -> String)? {
        serializers[cls]?.let { return it }
        var c: Class<*>? = cls
        while (c != null) {
            serializers[c]?.let { return it }
            for (i in c.interfaces) {
                findSerializerInInterface(i)?.let { return it }
            }
            c = c.superclass
        }
        return null
    }

    private fun findSerializerInInterface(i: Class<*>): ((Any) -> String)? {
        serializers[i]?.let { return it }
        for (sup in i.interfaces) {
            findSerializerInInterface(sup)?.let { return it }
        }
        return null
    }

    // Должен вызываться под writeLock.
    private fun ensureCsv(): BufferedWriter {
        val existing = csvWriter
        if (existing != null) return existing
        Files.createDirectories(errorsFile.parent ?: Path.of("."))
        // `TRUNCATE_EXISTING` — при re-run миграции старые errors.csv затирается. Без этого header
        // дописывался бы посередине существующего файла → битый CSV. Аналогично с `migration.log`
        // (FileAppender.isAppend = false в MigrationRunner) и пользовательскими `openCsv`-файлами:
        // один прогон = один свежий набор артефактов.
        val w = Files.newBufferedWriter(errorsFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
        w.write("timestamp,migration,author,itemRepr,errorClass,errorMessage")
        w.newLine()
        csvWriter = w
        return w
    }

    // Должен вызываться под writeLock.
    private fun ensureLog(): BufferedWriter {
        val existing = logWriter
        if (existing != null) return existing
        Files.createDirectories(tracesFile.parent ?: Path.of("."))
        val w = Files.newBufferedWriter(tracesFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
        logWriter = w
        return w
    }

    // Должен вызываться под writeLock. Flush per write — намеренный выбор: errors редкие,
    // durability важнее перфа (если JVM умрёт между ошибкой и close(), хочется видеть
    // прошлые ошибки в файле).
    private fun appendCsvRow(w: BufferedWriter, vararg fields: String) {
        w.write(fields.joinToString(",") { csvEscape(it) })
        w.newLine()
        w.flush()
    }

    private fun stackTrace(e: Throwable): String {
        val sw = StringWriter()
        e.printStackTrace(PrintWriter(sw))
        return sw.toString()
    }

    override fun close() {
        synchronized(writeLock) {
            if (closed) return
            closed = true
            csvWriter?.let { it.flush(); it.close() }
            csvWriter = null
            logWriter?.let { it.flush(); it.close() }
            logWriter = null
        }
    }
}
