package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.RunScope
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

/**
 * JDBC-операции миграции: `query/stream/execute/batch` поверх Kora [JdbcConnectionFactory].
 *
 * Создаётся через [jdbc] на [RunScope]. В двух режимах:
 * - **Free** (вне [transactional]): каждый вызов `query/execute/batch` открывает свою
 *   транзакцию через `db.inTx`. Удобно для ad-hoc read'ов.
 * - **Tx-bound** (внутри [transactional]): все вызовы переиспользуют один [Connection],
 *   `commit` на успешном выходе из блока, `rollback` на исключении.
 *
 * Write-операции (`execute`, `batch`) идут через `guardWrite` — под dry-run пропускаются.
 */
class SqlOps internal constructor(
    private val ctx: RunScope,
    private val db: JdbcConnectionFactory,
    private val txConn: Connection?,
    internal val inTx: Boolean,
) {
    /**
     * Поток, открывший транзакцию. `java.sql.Connection` не потокобезопасен, а
     * `transactional { }` вокруг параллельной стадии компилируется и выглядит
     * безобидно: четыре воркера начинают готовить statement'ы на одном соединении. ThreadLocal-
     * guard `txInProgress` этот случай не ловит принципиально — воркеры сидят на других потоках,
     * поэтому владельца запоминаем явно.
     */
    private val txOwner: Thread? = if (txConn != null) Thread.currentThread() else null

    /**
     * Выполнить SELECT, материализовать результат в `List<T>`. Подходит для запросов с
     * известным конечным размером. Для миллионных выгрузок используй [stream].
     *
     * Named-параметры через `:name`-плейсхолдеры, передавай парами `"name" to value`:
     * ```
     * jdbc(db).query("select id from t where status = :s", "s" to "OK") { it.getLong("id") }
     * ```
     */
    fun <T> query(sql: String, vararg params: Pair<String, Any?>, mapper: (ResultSet) -> T): List<T> {
        requireReadOnly(sql, "query")
        val (rendered, values) = validateAndRender(sql, params)
        return withConn { conn ->
            conn.prepareStatement(rendered).use { ps ->
                bindValues(ps, values)
                ps.executeQuery().use { rs ->
                    val out = ArrayList<T>()
                    while (rs.next()) out += mapper(rs)
                    out
                }
            }
        }
    }

    /**
     * Настоящее JDBC-streaming через scoped Connection и cursor (`setFetchSize`). Подходит для
     * выгрузок, не помещающихся в RAM, — драйвер тянет страницы по [fetchSize] строк за раз,
     * не материализуя результат целиком.
     *
     * API намеренно **callback-style**: возвращаемая [Sequence] валидна **только внутри** [consume].
     * После возврата из [consume] `ResultSet` / `PreparedStatement` / `Connection` уже закрыты
     * (`.use {}`-обёртки), любое iteration снаружи бросит SQL-исключение про закрытый RS. Это
     * сознательный выбор: невозможно случайно leak'нуть Connection или вернуть Sequence,
     * пережившую `db.inTx`-scope.
     *
     * Lifecycle:
     * - **Free-mode** (вне [transactional]): `db.inTx` открывает Connection на время вызова,
     *   делает commit на возврате (для read-only выгрузки no-op). При throw из [consume]
     *   — rollback.
     * - **Tx-bound** (внутри [transactional]): использует connection транзакции.
     *
     * Cursor mode требует `autoCommit = false` (для PostgreSQL); внутри `db.inTx` это всегда так.
     *
     * ```
     * // 1. Side-effect-only consumption: возвращаем Unit
     * jdbc(db).stream(
     *     "select id from orders where status = :s", "s" to "STUCK",
     *     fetchSize = 5000,
     *     mapper = { it.getLong("id") },
     * ) { rows ->
     *     // обрабатываем строки внутри одной транзакции, последовательно
     * }
     *
     * // 2. Возврат значения наружу: фолдим в R прямо внутри consume
     * val total: Long = jdbc(db).stream(
     *     "select id from orders", mapper = { it.getLong("id") },
     * ) { rows -> rows.count().toLong() }
     * ```
     *
     * NB: `sequence { while(rs.next()) yield(...) }` — single-pass (как любой generator-sequence).
     * Повторная iteration после первого прохода даст 0 элементов (RS уже в конце). Для compose
     * (`map`/`filter`) — оборачивай прямо внутри `consume`.
     *
     * @param sql SQL с `:name`-плейсхолдерами (см. [parseSql] про escaping).
     * @param params parameters в порядке любом. Дубликаты/missing/extra → IllegalArgumentException.
     * @param fetchSize размер страницы курсора. Дефолт 1000. Должен быть > 0.
     * @param mapper строка → доменный объект.
     * @param consume получает lazy `Sequence<T>` и возвращает результат типа [R].
     */
    fun <T, R> stream(
        sql: String,
        vararg params: Pair<String, Any?>,
        fetchSize: Int = 1000,
        mapper: (ResultSet) -> T,
        consume: (Sequence<T>) -> R,
    ): R {
        require(fetchSize > 0) { "fetchSize must be > 0, got $fetchSize" }
        requireReadOnly(sql, "stream")
        val (rendered, values) = validateAndRender(sql, params)
        return withConn { conn ->
            conn.prepareStatement(rendered).use { ps ->
                ps.fetchSize = fetchSize
                bindValues(ps, values)
                ps.executeQuery().use { rs ->
                    consume(sequence { while (rs.next()) yield(mapper(rs)) })
                }
            }
        }
    }

    /**
     * `INSERT`/`UPDATE`/`DELETE`. Возвращает число затронутых строк. Под dry-run — возвращает
     * `0`, в лог `INFO [DRY-RUN] jdbc.execute (sql=...)`.
     */
    fun execute(sql: String, vararg params: Pair<String, Any?>): Int {
        val (rendered, values) = validateAndRender(sql, params)
        return ctx.guardWrite("jdbc.execute", mapOf("sql" to sql.take(80)), dryRunDefault = 0) {
            withConn { conn ->
                conn.prepareStatement(rendered).use { ps ->
                    bindValues(ps, values)
                    ps.executeUpdate()
                }
            }
        }
    }

    /**
     * `INSERT ... VALUES (...)` batch с executeBatch. Параметры — позиционные `?`, биндинг
     * через [binder]:
     * ```
     * jdbc(db).batch("insert into t(id, v) values (?, ?)", rows) { ps, row ->
     *     ps.setLong(1, row.id); ps.setString(2, row.v)
     * }
     * ```
     */
    fun <T> batch(sql: String, items: Iterable<T>, binder: (PreparedStatement, T) -> Unit): IntArray =
        ctx.guardWrite("jdbc.batch", mapOf("sql" to sql.take(80)), dryRunDefault = IntArray(0)) {
            withConn { conn ->
                conn.prepareStatement(sql).use { ps ->
                    for (item in items) {
                        binder(ps, item)
                        ps.addBatch()
                    }
                    ps.executeBatch()
                }
            }
        }

    /**
     * Пишущий запрос, возвращающий строки (`INSERT ... RETURNING`, `UPDATE ... RETURNING`).
     * Проходит dry-run gate как обычная запись: под dry-run [mapper] не вызывается и
     * возвращается пустой список.
     *
     * Существует именно для того, чтобы [query] можно было держать строго читающим: раньше
     * `query("insert ... returning id")` был единственным способом получить сгенерированные
     * идентификаторы — и выполнялся в том числе под dry-run, мимо всякого гейта.
     */
    fun <T> executeReturning(sql: String, vararg params: Pair<String, Any?>, mapper: (ResultSet) -> T): List<T> {
        val (rendered, values) = validateAndRender(sql, params)
        return ctx.guardWrite(
            "jdbc.executeReturning",
            mapOf("sql" to sql.take(80)),
            dryRunDefault = emptyList(),
        ) {
            withConn { conn ->
                conn.prepareStatement(rendered).use { ps ->
                    bindValues(ps, values)
                    ps.executeQuery().use { rs ->
                        val out = ArrayList<T>()
                        while (rs.next()) out += mapper(rs)
                        out
                    }
                }
            }
        }
    }

    /**
     * Транзакционный scope под dry-run: реальное соединение не открывается (это дало бы
     * BEGIN/COMMIT round-trip на каждый блок), но ThreadLocal-флаг взводится тот же самый.
     * Без него вложенный `transactional` детектировался бы только в бою — репетиция проходила
     * бы зелёной ровно там, где боевой прогон падает.
     */
    internal fun <R> runDryRunTx(block: SqlOps.() -> R): R {
        txInProgress.set(true)
        try {
            return block()
        } finally {
            txInProgress.set(false)
        }
    }

    internal fun <R> runRealTx(block: SqlOps.() -> R): R {
        if (txInProgress.get()) {
            throw IllegalStateException(
                "nested transactional not supported (a transaction is already open on this thread)",
            )
        }
        txInProgress.set(true)
        try {
            return db.inTx<R> { conn ->
                val txOps = SqlOps(ctx, db, txConn = conn, inTx = true)
                txOps.block()
            }
        } finally {
            txInProgress.set(false)
        }
    }

    /**
     * Есть ли уже открытая транзакция на текущем потоке. Проверяет два источника:
     *
     * 1. **ThreadLocal-флаг** [txInProgress], который мы взводим в [runRealTx] на время блока —
     *    это primary guard, работает одинаково в проде и в тестах.
     * 2. **`db.currentConnection()`** — secondary защита для случаев, когда tx открыта внешним
     *    коду (не через наш `transactional`), но на том же `JdbcConnectionFactory`. В тестовой
     *    `TestJdbcConnectionFactory` всегда `null`, в production-Kora — non-null внутри `inTx`.
     */
    internal fun hasActiveTxConnection(): Boolean = txInProgress.get() || db.currentConnection() != null

    private fun <R> withConn(block: (Connection) -> R): R =
        if (txConn != null) {
            check(Thread.currentThread() === txOwner) {
                "tx-bound jdbc ops used from thread '${Thread.currentThread().name}', but the " +
                    "transaction belongs to '${txOwner?.name}'. java.sql.Connection is not thread-safe: " +
                    "do not run a parallel stage inside transactional { } — put transactional { } " +
                    "inside the handler body instead."
            }
            block(txConn)
        } else {
            db.inTx<R> { conn -> block(conn) }
        }

    companion object {
        // Per-thread флаг «мы внутри transactional». Используется как guard против вложенных
        // `transactional(outerOps) { transactional(outerOps) { ... } }` — без этого второй
        // вызов проходил бы по `ops.inTx == false`-branch и открывал бы вторую независимую tx.
        // Static (companion), чтобы один SqlOps-инстанс не "видел" tx другого: jdbc(db) каждый
        // раз создаёт новый SqlOps, флаг должен быть общий для всех SqlOps на потоке.
        private val txInProgress: ThreadLocal<Boolean> = ThreadLocal.withInitial { false }

        private val READ_ONLY_KEYWORDS = setOf("select", "with", "show", "explain", "values", "table", "describe")
    }

    /**
     * Парсит SQL с named-параметрами `:name`, **пропуская** их внутри:
     * - single-quoted string literals (`'...'` с escape `''`);
     * - double-quoted identifiers (`"..."`);
     * - однострочных комментариев (`-- ...`);
     * - блочных комментариев (`/* ... */`);
     * - PostgreSQL dollar-quoted strings (`$$ ... $$`, `$tag$ ... $tag$` — содержимое
     *   передаётся как литерал внутрь plpgsql `DO`/`CREATE FUNCTION` блоков);
     * - PostgreSQL `::cast` (двойное двоеточие — не named-параметр).
     *
     * Возвращает `(rendered SQL, ordered list of param names)`. Используется query/execute;
     * batch принимает raw SQL с `?` сам.
     */
    private fun parseSql(sql: String): Pair<String, List<String>> {
        val out = StringBuilder(sql.length)
        val names = mutableListOf<String>()
        var i = 0
        while (i < sql.length) {
            val c = sql[i]
            when {
                c == '\'' -> {
                    // single-quoted string — содержимое не парсим. '' внутри = escape, остаёмся внутри.
                    out.append(c); i++
                    while (i < sql.length) {
                        val cc = sql[i]
                        out.append(cc); i++
                        if (cc == '\'') {
                            if (i < sql.length && sql[i] == '\'') {
                                out.append(sql[i]); i++
                            } else break
                        }
                    }
                }

                c == '"' -> {
                    // double-quoted identifier — то же, не парсим
                    out.append(c); i++
                    while (i < sql.length) {
                        val cc = sql[i]
                        out.append(cc); i++
                        if (cc == '"') break
                    }
                }

                c == '-' && i + 1 < sql.length && sql[i + 1] == '-' -> {
                    // single-line comment — сохраняем содержимое как есть, `:name` внутри не трогаем
                    while (i < sql.length && sql[i] != '\n') {
                        out.append(sql[i]); i++
                    }
                }

                c == '/' && i + 1 < sql.length && sql[i + 1] == '*' -> {
                    // block comment — пропускаем `:name` внутри. Незакрытый `/*` (нет `*/`) —
                    // SQL невалидный, но мы выходим штатно (дойдём до конца строки), пусть БД ругается.
                    out.append("/*"); i += 2
                    while (i + 1 < sql.length && !(sql[i] == '*' && sql[i + 1] == '/')) {
                        out.append(sql[i]); i++
                    }
                    if (i + 1 < sql.length) {
                        out.append("*/"); i += 2
                    } else {
                        while (i < sql.length) {
                            out.append(sql[i]); i++
                        }
                    }
                }

                c == ':' && i + 1 < sql.length && sql[i + 1] == ':' -> {
                    // PostgreSQL `::cast` (`value::int`, `now()::timestamptz`) — пара двоеточий
                    // не named-параметр. Пропускаем обе как литералы; парсер дальше не должен
                    // зацепиться за второе `:` как старт `:name`.
                    out.append("::"); i += 2
                }

                c == '$' -> {
                    // PostgreSQL dollar-quoted string: `$$...$$` (anonymous) или `$tag$...$tag$`
                    // (тегированный). Используется в `DO $$ ... $$;` и `CREATE FUNCTION ... AS $$ ... $$`.
                    // Содержимое — литерал; `:name` внутри не должен быть named-параметром.
                    //
                    // Tag — последовательность букв/цифр/_ между двумя `$`. Если после первого `$`
                    // идёт что-то, что не может быть началом тэга (например, число, или это просто
                    // `:money`-операция типа `$1`), — трактуем `$` как обычный символ.
                    val tagEnd = parseDollarTag(sql, i)
                    if (tagEnd < 0) {
                        out.append(c); i++
                    } else {
                        val opener = sql.substring(i, tagEnd + 1)        // `$$` или `$tag$`
                        val closeAt = sql.indexOf(opener, tagEnd + 1)
                        if (closeAt < 0) {
                            // Незакрытый dollar-quote — SQL невалидный, дописываем остаток как есть,
                            // пусть БД отлуплется.
                            out.append(sql, i, sql.length); i = sql.length
                        } else {
                            val end = closeAt + opener.length
                            out.append(sql, i, end); i = end
                        }
                    }
                }

                c == ':' && i + 1 < sql.length && (sql[i + 1].isLetter() || sql[i + 1] == '_') -> {
                    i++
                    val start = i
                    while (i < sql.length && (sql[i].isLetterOrDigit() || sql[i] == '_')) i++
                    names += sql.substring(start, i)
                    out.append('?')
                }

                else -> {
                    out.append(c); i++
                }
            }
        }
        return out.toString() to names
    }

    /**
     * Распознаёт открывающую часть PostgreSQL dollar-quote (`$$` или `$tag$`).
     * @return индекс закрывающего `$` opener'а (включительно), или `-1` если это не валидный
     *         dollar-opener (тогда `$` трактуется как обычный символ — `$1` / `$arg` плейсхолдеры
     *         не наш случай, PreparedStatement их не использует).
     */
    private fun parseDollarTag(sql: String, start: Int): Int {
        if (start + 1 >= sql.length) return -1
        val next = sql[start + 1]
        if (next == '$') return start + 1                                     // `$$` — anonymous
        if (!next.isLetter() && next != '_') return -1                        // `$1`, `$ ` — не opener
        var j = start + 2
        while (j < sql.length && (sql[j].isLetterOrDigit() || sql[j] == '_')) j++
        if (j >= sql.length || sql[j] != '$') return -1
        return j                                                              // `$tag$` — closing `$` opener'а
    }

    /**
     * Валидация params vs SQL placeholders. Бросает [IllegalArgumentException] на:
     * - дубликаты ключей в params;
     * - SQL содержит `:name`, отсутствующий в params;
     * - params содержит ключ, не использованный в SQL.
     *
     * Возвращает (rendered SQL, упорядоченный список значений в порядке `?`).
     */
    private fun validateAndRender(sql: String, params: Array<out Pair<String, Any?>>): Pair<String, List<Any?>> {
        val providedKeys = params.map { it.first }
        val duplicates = providedKeys.groupingBy { it }.eachCount().filter { it.value > 1 }.keys
        if (duplicates.isNotEmpty()) {
            throw IllegalArgumentException("Duplicate parameter keys: $duplicates in SQL: ${sql.take(120)}")
        }
        val (rendered, names) = parseSql(sql)
        val providedSet = providedKeys.toSet()
        val usedSet = names.toSet()
        val missing = usedSet - providedSet
        if (missing.isNotEmpty()) {
            throw IllegalArgumentException("Missing SQL parameters: $missing in SQL: ${sql.take(120)}")
        }
        val extra = providedSet - usedSet
        if (extra.isNotEmpty()) {
            throw IllegalArgumentException("Unused parameters: $extra in SQL: ${sql.take(120)}")
        }
        val map = params.toMap()
        return rendered to names.map { map[it] }
    }

    /**
     * Пускает в [query] / [stream] только читающие запросы. Ни то, ни другое не проходит
     * dry-run gate (и не должно — чтение под репетицией обязано работать), поэтому пишущий
     * запрос, просунутый в `query`, выполнялся бы В БОЮ во время dry-run прогона.
     *
     * Проверка одинакова в обоих режимах: гейт, срабатывающий только под dry-run, дал бы
     * зелёную репетицию при падающем бое (или наоборот) — худший вид расхождения.
     *
     * Известное ограничение: `WITH ... INSERT` (data-modifying CTE) начинается с `with` и
     * проверку пройдёт. Для таких запросов используй [executeReturning].
     */
    private fun requireReadOnly(sql: String, method: String) {
        val kw = firstKeyword(sql)
        if (kw !in READ_ONLY_KEYWORDS) {
            throw IllegalArgumentException(
                "jdbc().$method() accepts read statements only, got '${kw.ifEmpty { "?" }} ...'. " +
                    "Use execute(...) for writes, or executeReturning(...) if you need the rows back — " +
                    "both go through the dry-run gate, while $method() would run for real during a dry run. " +
                    "SQL: ${sql.take(120)}",
            )
        }
    }

    /** Первое ключевое слово запроса в нижнем регистре, с пропуском ведущих комментариев. */
    private fun firstKeyword(sql: String): String {
        var i = 0
        while (i < sql.length) {
            val c = sql[i]
            when {
                c.isWhitespace() -> i++
                c == '-' && i + 1 < sql.length && sql[i + 1] == '-' -> {
                    while (i < sql.length && sql[i] != '\n') i++
                }

                c == '/' && i + 1 < sql.length && sql[i + 1] == '*' -> {
                    val end = sql.indexOf("*/", i + 2)
                    i = if (end < 0) sql.length else end + 2
                }

                c == '(' -> i++          // `(select ...) union ...`
                else -> {
                    val start = i
                    while (i < sql.length && (sql[i].isLetter() || sql[i] == '_')) i++
                    return sql.substring(start, i).lowercase()
                }
            }
        }
        return ""
    }

    private fun bindValues(ps: PreparedStatement, values: List<Any?>) {
        values.forEachIndexed { idx, v -> ps.setObject(idx + 1, v) }
    }
}

/**
 * Фабрика [SqlOps] для конкретного [JdbcConnectionFactory]. Идиоматический вызов:
 * ```
 * jdbc(db).execute("update ...")
 * transactional(jdbc(db)) { execute("..."); execute("...") }
 * ```
 */
fun RunScope.jdbc(db: JdbcConnectionFactory): SqlOps = SqlOps(this, db, txConn = null, inTx = false)

/**
 * Реальный транзакционный scope. Открывает один [Connection] через `db.inTx`, создаёт
 * tx-bound [SqlOps], запускает [block] с ним. Все `execute/query/batch` внутри переиспользуют
 * этот connection — commit на успехе, rollback на любом throw.
 *
 * Вложенный `transactional` бросает [IllegalStateException] — нет smart merge с outer-tx,
 * пользователь должен сам решить как это устроить.
 *
 * @throws IllegalStateException если [ops] уже находится внутри активной транзакции.
 */
fun <R> RunScope.transactional(ops: SqlOps, block: SqlOps.() -> R): R {
    if (ops.inTx) throw IllegalStateException("nested transactional not supported (already in tx-bound SqlOps)")
    // Доп. guard: пользователь мог передать **внешний** free-mode `ops` внутри уже открытого
    // `transactional` — тогда `ops.inTx == false`, но Kora уже держит открытую tx на текущем
    // потоке. Без этой проверки открылась бы ВТОРАЯ независимая tx — два connection'а, разные
    // commit'ы, противоречит API-обещанию «nested transactional не поддерживается».
    if (ops.hasActiveTxConnection()) {
        throw IllegalStateException(
            "nested transactional not supported (a Kora transaction is already open on this thread — " +
                    "do not call transactional() again from inside a transactional block)",
        )
    }
    if (dryRun) {
        // Под dry-run не открываем connection через db.inTx — это даёт реальный round-trip
        // BEGIN/COMMIT в драйвер на каждый блок. Вместо этого выполняем block на free-mode
        // SqlOps: writes (execute/batch) silently skip через guardWrite, reads (query/stream)
        // работают (каждый в своей mini-tx). Tx semantic не нужна, т.к. ничего не пишется.
        // ВАЖНО: тело блока при этом ВЫПОЛНЯЕТСЯ — под dry-run пропускаются отдельные записи
        // внутри него, а не блок целиком.
        log.info("[DRY-RUN] jdbc.transactional")
        report.incDryRunSkipped("jdbc.transactional")
        return ops.runDryRunTx(block)
    }
    return ops.runRealTx(block)
}
