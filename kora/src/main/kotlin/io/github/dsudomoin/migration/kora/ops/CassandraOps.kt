package io.github.dsudomoin.migration.kora.ops

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.*
import io.github.dsudomoin.migration.HandlerScope
import io.github.dsudomoin.migration.RunScope

/**
 * Cassandra-операции: `query/execute/batch` поверх [CqlSession]. Параметры —
 * named placeholders `:name`, биндинг автоматический.
 *
 * Ленивого `stream` здесь нет намеренно: он отдавал `Sequence` поверх авто-paging'а драйвера —
 * без влияния на размер страницы, без учёта в отчёте и без контроля курсора. Для источника
 * стадии есть `pages(...)`, для небольшой выборки — [query]. Два способа читать Cassandra —
 * ровно то, из-за чего миграции получались императивными.
 *
 * Write-операции (`execute`, `batch`) идут через `guardWrite` — под dry-run пропускаются.
 *
 * **Prepared statements:** не кэшируются в `CassandraOps` — фабрика `cassandra(session)` создаёт
 * новый инстанс на каждый вызов, локальный кэш всё равно был бы пуст. DataStax driver 4.x
 * держит собственный per-session prepare-cache (keyed by CQL + protocol version): повторный
 * `session.prepare(sameCql)` отдаёт тот же `PreparedStatement` из driver-cache. Дополнительный
 * слой здесь не нужен.
 */
open class CassandraReadOps internal constructor(
    protected val ctx: RunScope,
    protected val session: CqlSession,
) {
    protected fun prepared(cql: String): PreparedStatement = session.prepare(cql)

    /**
     * Полное чтение результата в `List<T>`.
     *
     * Для большой выборки бери `pages(...)`: он читает страницами по явному курсору, считает
     * сырые страницы в отчёт и ловит неподвижный курсор. Авто-paging драйвера ничего из этого не
     * умеет: размер страницы задаётся конфигом Kora и из DSL невидим.
     */
    fun <T> query(cql: String, vararg params: Pair<String, Any?>, mapper: (Row) -> T): List<T> =
        session.execute(bind(cql, params)).map(mapper).toList()

    protected fun bind(cql: String, params: Array<out Pair<String, Any?>>): Statement<*> {
        val ps = prepared(cql)
        val bb = ps.boundStatementBuilder()
        for ((k, v) in params) setExplicit(bb, k, v)
        return bb.build()
    }

    /**
     * Биндинг named-параметра. Скаляры идут через `bb.set(name, value, value.javaClass)` —
     * DataStax driver 4.x ищет кодек по runtime-классу значения (String, Long, Instant, UUID,
     * primitives и обёртки).
     *
     * Для коллекций (`List<T>` / `Set<T>`) `value.javaClass` (например, `ArrayList`) бесполезен
     * — driver не знает кодек на raw-класс реализации. Используем явные `setList`/`setSet` с
     * **классом элемента**, выведенным из первого не-null значения коллекции. Кейс «IN :ids»:
     *
     * ```
     * cassandra(session).query("select id from t where id in :ids", "ids" to listOf("A", "B"))
     * ```
     *
     * Пустая / all-null коллекция → `IllegalArgumentException`, потому что тип элемента вывести
     * нельзя, а Cassandra-кодек требует конкретный `Class<T>`. Если действительно нужно
     * привязать пустой `List` — собирай `BoundStatementBuilder` сам в custom-op (см.
     * `docs/examples/customization.md`) и явно передай `GenericType.listOf(...)`.
     *
     * Map/UDT/tuple — не покрыты этим путём; пиши свой custom op.
     */
    private fun setExplicit(bb: BoundStatementBuilder, k: String, v: Any?) {
        if (v == null) {
            bb.setToNull(k)
            return
        }
        when (v) {
            is List<*> -> {
                val elementClass = inferElementClass(v, k)
                @Suppress("UNCHECKED_CAST")
                bb.setList(k, v as List<Any?>, elementClass as Class<Any?>)
            }

            is Set<*> -> {
                val elementClass = inferElementClass(v, k)
                @Suppress("UNCHECKED_CAST")
                bb.setSet(k, v as Set<Any?>, elementClass as Class<Any?>)
            }

            else -> bb.set(k, v, v.javaClass)
        }
    }

    private fun inferElementClass(c: Collection<*>, k: String): Class<*> =
        c.firstOrNull { it != null }?.javaClass
            ?: throw IllegalArgumentException(
                "Cannot bind empty or all-null ${c.javaClass.simpleName} to Cassandra parameter ':$k' " +
                        "— element type cannot be inferred. Build BoundStatementBuilder manually " +
                        "and pass GenericType.listOf(...) for empty collections.",
            )
}

/**
 * Фабрика [CassandraOps]. Для multi-кластерной миграции — два разных `CqlSession` через
 * `@Tag(...)` и `cassandra(primary)`, `cassandra(replica)`.
 */
/**
 * Cassandra-операции с записью: [CassandraOps.execute], [CassandraOps.batch] и всё чтение из
 * [CassandraReadOps].
 *
 * Доступна только из обработчика элемента: внешнее изменение обязано иметь границу элемента и
 * попадать в учёт эффектов, а источник по определению описывает данные, а не меняет их.
 */
class CassandraOps internal constructor(
    ctx: RunScope,
    session: CqlSession,
) : CassandraReadOps(ctx, session) {

    /** `INSERT/UPDATE/DELETE` (Cassandra: CRUD-семантика немного другая, но guardWrite те же). */
    fun execute(cql: String, vararg params: Pair<String, Any?>): ResultSet? =
        ctx.guardWrite("cassandra.execute", mapOf("cql" to cql.take(80)), dryRunDefault = null) {
            session.execute(bind(cql, params))
        }

    /**
     * Bulk-execute через prepared statement. Каждый item вызывается на свой `boundStatementBuilder`,
     * биндинг через [binder]. NB: Cassandra не имеет true batch'а в JDBC-смысле — это просто цикл
     * `session.execute(...)` по подготовленному statement.
     */
    fun <T> batch(cql: String, items: Iterable<T>, binder: (BoundStatementBuilder, T) -> Unit) {
        ctx.guardWrite("cassandra.batch", mapOf("cql" to cql.take(80))) {
            val ps = prepared(cql)
            for (item in items) {
                val b = ps.boundStatementBuilder()
                binder(b, item)
                session.execute(b.build())
            }
        }
    }
}

/** Чтение из Cassandra — доступно в любом scope'е прогона. */
fun RunScope.cassandra(session: CqlSession): CassandraReadOps = CassandraReadOps(this, session)

/** Чтение и запись — только в обработчике элемента. */
fun HandlerScope.cassandra(session: CqlSession): CassandraOps = CassandraOps(this, session)
