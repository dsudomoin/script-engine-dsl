# Migration DSL — Agent Guide

A guide for an AI coding agent integrating this library into a downstream
Kora-based service. Self-contained: you do not need to read any other file
in this repo to write a correct migration. User-facing docs
(`docs/USER_GUIDE.md`, `docs/examples/*`) are written in Russian for humans
and are not aligned with this guide — when they disagree, this file wins.

Version: `0.1.0`. Kotlin 2.1+, JVM 21+, Kora 1.1+.

---

## 1. What this library is

A Kotlin DSL for **one-shot data-migration scripts** that run inside a Kora
application graph. A migration is a single `Migration` subclass that owns a
`migrate()` block; the library owns the cross-cutting concerns:

- parallel item processing with bounded fan-out;
- item-level error policy (`Fail` / `Skip` / `Handle`);
- auto-audit of failed items to `errors.csv` + `errors.log`;
- dry-run that elides every side-effect without touching user code;
- structured progress logs and a final stdout report;
- a per-migration `outputFolder` collecting all artifacts.

Retry is **not** a DSL primitive — see §7.4 for why and where to put it.

A migration is **not** a long-running service, an ETL pipeline, or a
coroutine-friendly Flow. It is a blocking script that runs once, prints a
report, and exits with a status code.

## 2. When to use / when to skip

Use when:
- Bulk reads/updates against existing Postgres / Cassandra / Kafka /
  external HTTP services already wired into a Kora graph.
- One-off scripts that would otherwise reinvent `ExecutorService` +
  `try/catch` + ad-hoc CSV writers.
- A script that needs an audit trail of failed items and a deterministic
  exit code in CI.

Skip when:
- The task is always-on (cron, scheduled, streaming). Build a normal Kora
  component.
- The codebase is coroutine-first and you need structured concurrency. The
  DSL is blocking and runs on JVM threads.
- You do not have Kora. The `core` module is pure Kotlin and could be
  consumed standalone, but every example in this guide assumes the
  `kora` bridge module.

## 3. Repository layout

```
core/   pure Kotlin, no Kora. DSL primitives, CSV, error audit, report.
kora/   Kora bridge: MigrationRunner, MigrationModule, MigrationConfig,
        ops/* (jdbc, kafka, http, cassandra).
docs/   Russian user guide + archetype examples. Not your target audience.
```

You publish or consume both modules together for a Kora service:

```kotlin
dependencies {
    implementation("io.github.dsudomoin.migration:migration-dsl-core:0.1.0")
    implementation("io.github.dsudomoin.migration:migration-dsl-kora:0.1.0")
}
```

## 4. Wire-up

Add `MigrationModule` to the `@KoraApp` interface alongside whatever
infrastructure modules the migration touches:

```kotlin
@KoraApp
interface App :
    HoconConfigModule,
    JdbcDatabaseModule,
    KafkaProducerModule,
    MigrationModule
```

In `application.conf`, the minimum is one line:

```hocon
migration {
  run = ${?MIGRATION_RUN}   # name of the migration to execute; null = idle
}
```

Each migration is a `@Component`-annotated subclass of `Migration`. The
runner finds them via `All<Migration>` and picks the one whose `name`
matches `migration.run`. Duplicate names abort with exit code 2.

## 5. Hello migration

A minimal complete script:

```kotlin
@Component
class FixOrderStatuses(
    private val db: JdbcConnectionFactory,
) : Migration(name = "FIX-ORDERS-001", author = "agent") {

    override fun MigrationContext.migrate() {
        val stuck = jdbc(db).query("select id from orders where status = 'STUCK'") {
            it.getLong("id")
        }

        errors.includeItem<Long> { "orderId=$it" }

        forEach(stuck, parallel = 4, onError = OnError.Skip) { id ->
            jdbc(db).execute("update orders set status = 'OK' where id = :id", "id" to id)
        }
    }
}
```

Run it:
```bash
MIGRATION_RUN=FIX-ORDERS-001 ./gradlew run
MIGRATION_RUN=FIX-ORDERS-001 MIGRATION_DRY_RUN=true ./gradlew run
```

Artifacts land in `logs/FIX-ORDERS-001/` and the final report prints to
stdout.

## 6. Mental model

```
+-----------------------------+
| @Component class FooMig     |     declared by user
|   : Migration(name, author) |
|   override migrate() = ...  |
+--------------+--------------+
               |
               | MigrationRunner.init() — Kora Lifecycle
               v
+--------------+--------------+
| MigrationContext (receiver) |     created by runner per run
|   dryRun, log, report,      |
|   errors, outputFolder,     |
|   executor, register(...)   |
+--------------+--------------+
               |
               | extensions on MigrationContext
               v
   forEach(...)        ops:
   mutation { ... }      jdbc(db).query/execute/batch/stream
   openCsv(...)          transactional(jdbc(db)) { ... }
   readCsv(...)          kafka(producer).publish[Async]
   ctx.register(...)     topic(producer, name).send
                         cassandra(session).query/stream/execute/batch
                         http(call).get/post/patch/put/delete
```

Lifecycle:
1. Runner reads HOCON, resolves migration by name.
2. Creates `outputFolder` (`mkdir -p`), attaches a Logback `FileAppender`
   to the root logger so everything in slf4j ends up in `migration.log`.
3. Builds `MigrationContext` with a `CsvFileErrorReporter`, a
   `ReportBuilder`, and either a custom `@Tag(MigrationExecutor::class)`
   `Executor` or a fresh `FixedThreadPool`.
4. Invokes `migration.migrate()` with the context as the receiver.
5. In `finally`, closes every `AutoCloseable` the user registered (CSVs,
   Kafka topic handles, custom holders) **in reverse registration order**.
6. Closes the reporter, builds the final `MigrationReport`, prints it,
   detaches the file appender, calls `System.exit(code)`.

You never construct `MigrationContext` yourself in production. For tests:
`DefaultMigrationContext.test(dryRun = ..., outputFolder = ...)`.

## 7. API reference — `core`

### 7.1 `Migration` base class

```kotlin
abstract class Migration(
    val name: String,                       // unique within the graph
    val author: String,                     // human-readable
    val onUnhandled: ScriptPolicy? = null,  // null delegates to HOCON
) {
    abstract fun MigrationContext.migrate()
}

enum class ScriptPolicy { FAIL_FAST, LOG_AND_COMPLETE }
```

`onUnhandled` controls behavior when `migrate()` throws past every
`forEach`. `FAIL_FAST` → log + exit 1. `LOG_AND_COMPLETE` → log + exit 0
(report still printed, resources still closed). Item-level errors inside
`forEach` are handled by `OnError`, not this.

### 7.2 `MigrationContext` (receiver)

Available inside `migrate()` without qualifier:

| Member | Meaning |
|---|---|
| `dryRun: Boolean` | True if the run was started in dry-run mode. |
| `log: Logger` | SLF4J logger named `io.github.dsudomoin.migration.<name>`. |
| `report: ReportBuilder` | Counters (`successful`, `skipped`, `failed`, `dryRunSkipped` map). Mutated automatically. |
| `executor: Executor` | Pool used by `forEach(parallel > 1)`. Do not use for arbitrary async work. |
| `outputFolder: Path` | All artifacts go here. Resolves `logs/<name>` by default. |
| `errors: CsvFileErrorReporter` | Audit sink for failed items. Concrete class, not the `ErrorReporter` interface — see §15.2 for the rationale. See 7.6 for usage. |
| `defaultProgressEvery: Int` | Tick period for `Progress.Default`. From HOCON. |
| `errorThreshold: Long` | Abort run when `report.skipped > threshold`. `0` = disabled. |
| `register(c: AutoCloseable)` | Schedule LIFO close after `migrate()`. |
| `guardWrite(label, args, default, action)` | Dry-run gate. Wraps every write op in the library. |
| `auditError(e, item)` | Manual audit to `errors.csv`. Rarely called directly. |

### 7.3 `forEach` — three overloads

```kotlin
// item-by-item
fun <T> MigrationContext.forEach(
    items: Iterable<T>,
    parallel: Int = 1,
    onError: OnError = OnError.Fail,
    onErrorLog: ((Throwable, T) -> Unit)? = null,
    logEach: ((T) -> String)? = null,
    progress: Progress = Progress.Default,
    block: MigrationContext.(T) -> Unit,
)

// chunked: each worker receives a List<T> of size `chunk`
@JvmName("forEachChunked")
fun <T> MigrationContext.forEach(
    items: Iterable<T>, chunk: Int, parallel: Int = 1,
    onError: OnError = OnError.Fail,
    onErrorLog: ((Throwable, List<T>) -> Unit)? = null,
    logEach: ((List<T>) -> String)? = null,
    progress: Progress = Progress.Default,
    block: MigrationContext.(List<T>) -> Unit,
)

// Sequence — adapter; semantically equivalent to .asIterable()
fun <T> MigrationContext.forEach(
    items: Sequence<T>, parallel: Int = 1, /* same kwargs */
    block: MigrationContext.(T) -> Unit,
)
```

Parallel execution: when `parallel > 1`, items are submitted to
`ctx.executor` and bounded by a `Semaphore(parallel)` so that a long
`Sequence` does not materialize into the pool queue. The first exception
collected by a worker triggers `Future.cancel(true)` on every other
outstanding submission — see §11.1 "Cancellation in parallel `forEach` is
cooperative" for the caveats. All collected exceptions are returned as
`addSuppressed` on the primary throw — caller sees one throw chain.

### 7.4 `OnError`

```kotlin
sealed interface OnError {
    object Fail : OnError                                            // default — abort on first error
    object Skip : OnError                                            // audit + continue
    class Handle(val decide: (Throwable, Any?) -> Decision) : OnError
    enum class Decision { Skip, Fail }
    companion object { fun handle(decide: ...): Handle }             // factory
}
```

There is **no built-in retry primitive at the item level** — that is a
deliberate design decision. Item-level retry re-runs the entire `forEach`
block for one item, which silently breaks correctness for non-idempotent
steps (a Kafka publish that already happened, an INSERT with auto-PK).

If you need retry semantics:
- **Network calls** (HTTP, Kafka, JDBC): use Kora's `@Retry` annotation on
  the typed client (`@HttpClient`, `@KafkaPublisher`, repository methods).
  It classifies exceptions and applies backoff at the right scope — the
  individual remote call, not the whole pipeline.
- **Custom transient logic**: a local `try { ... } catch { Thread.sleep; ... }`
  loop around the specific failing primitive inside the `forEach`-block.
- **Per-call retries on `SqlOps`/`HttpOps`/`KafkaOps`**: not implemented in
  v0.2; if you genuinely need them at this layer, wrap the primitive call
  yourself.

`Handle` classifies failures by type without retrying — useful when
different exception classes deserve different verdicts:
```kotlin
forEach(orders, onError = OnError.handle { e, _ -> when (e) {
    is ValidationException -> OnError.Decision.Skip
    else                   -> OnError.Decision.Fail
}}) { ... }
```

### 7.5 `Progress`

```kotlin
sealed interface Progress {
    object Default : Progress                                    // adaptive — see below
    object Off : Progress                                        // silent
    data class Every(val n: Int) : Progress
    data class Custom(val n: Int, val format: (Long, Long?) -> String) : Progress
}
```

`format` receives `(doneCount, totalOrNull)`. `total` is `null` for
`Sequence` sources because the size is unknown.

`Progress.Default` uses `defaults.progressEvery` from HOCON (default 1000)
as the base period, but auto-adapts for small known-size cycles: if
`total < progressEvery * 10`, the period drops to `max(1, total / 10)` so
even a 100-item cycle still gets ~10 ticks instead of zero. Explicit
`Every(n)` / `Custom(n, ...)` are honored as-is — no adaptation.

### 7.6 `errors` audit

`ctx.errors: CsvFileErrorReporter` is thread-safe; written from every
`forEach`-worker that triggers a `Skip`. To make item rendering useful in
`errors.csv`, register a per-type serializer once at the top of `migrate()`:

```kotlin
import io.github.dsudomoin.migration.error.includeItem

errors.includeItem<Customer> { "id=${it.id}, segment=${it.segment}" }
errors.includeItem<Long>     { "orderId=$it" }
```

Without a serializer, `item.toString()` is used (and may be huge or
useless). The reporter writes `errors.csv` (one row per failed item) and
`errors.log` (stack traces if `errorReporting.includeStackTrace = true`).

### 7.7 `mutation { ... }` — dry-run gate for untyped writes

When a write happens through a typed Kora client (e.g.
`@HttpClient`, `@KafkaPublisher`) that the DSL cannot intercept, wrap it:

```kotlin
mutation("orders.resync", args = mapOf("orderId" to order.id)) {
    publisher.publishResync(order.id.toString(), OrderEvent.from(order))
}
```

**Critical invariant**: `label` must be a **constant string** (per logical
operation). Per-item context goes in `args`. The runner aggregates dry-run
counts by label — a label of `"resync $id"` produces millions of distinct
entries in `report.dryRunSkipped` instead of one row.

`mutation` has two overloads. The default returns `Unit` — the lambda's
return value is discarded:

```kotlin
fun MigrationContext.mutation(
    label: String,
    args: Map<String, Any?> = emptyMap(),
    action: () -> Unit,
)
```

The generic overload takes a `dryRunDefault` and returns whatever the lambda
returns — use it for typed Kora clients that produce values you need
(HTTP status, repository result, `RETURNING`-id):

```kotlin
fun <R> MigrationContext.mutation(
    label: String,
    args: Map<String, Any?> = emptyMap(),
    dryRunDefault: R,
    action: () -> R,
): R

// usage:
val status = mutation("billing.refund",
    args = mapOf("orderId" to id), dryRunDefault = 202) {
    billingClient.refund(id)   // returns Int (HTTP status)
}
```

Kotlin resolves the overload by the presence of `dryRunDefault` — a lambda
without it falls back to the `Unit` overload (return value is discarded),
which is the same contract as before. `ctx.guardWrite<R>(label, args,
dryRunDefault, action)` is the underlying primitive if you need an even
more specific label scheme.

### 7.8 CSV

```kotlin
// read: lazy Sequence<T>
fun <T> MigrationContext.readCsv(path: String, classpath: Boolean = false,
                                 mapper: (Map<String, String>) -> T): Sequence<T>
fun <T> MigrationContext.readCsv(path: Path, mapper: (Map<String, String>) -> T): Sequence<T>

// write: handle, registered as AutoCloseable
interface CsvOutput : AutoCloseable {
    fun row(vararg cells: Any?)   // thread-safe; RFC 4180 quoting
    fun flush()                    // explicit; close() flushes too
}

fun MigrationContext.openCsv(path: Path, vararg headers: String): CsvOutput
fun MigrationContext.openCsv(filename: String, vararg headers: String): CsvOutput
```

The `filename` overload resolves relative to `ctx.outputFolder` — that is
the idiomatic way for migration outputs:

```kotlin
val out = openCsv("processed.csv", "id", "old_status", "new_status")
forEach(rows) { r ->
    jdbc(db).execute("update t set status='OK' where id=:id", "id" to r.id)
    out.row(r.id, r.oldStatus, "OK")
}
// runner flushes & closes `out` after migrate() returns
```

`row()` is buffered; no per-row flush. The runner closes the writer in
`finally` and the buffer reaches disk then. If you need durability
mid-run (e.g. checkpoints), call `out.flush()` explicitly.

Under dry-run, `openCsv` still creates the file and `row()` still writes —
the CSV is treated as a diagnostic artifact, not a side-effect on a
external system.

### 7.9 `ctx.register(AutoCloseable)`

Register any resource you open inside `migrate()` so the runner closes it
in reverse order after the script ends. `openCsv` and `topic(...)` already
register themselves; you only call this directly for custom holders.

## 8. API reference — `kora`

### 8.1 SQL (`jdbc`, `transactional`)

```kotlin
fun MigrationContext.jdbc(db: JdbcConnectionFactory): SqlOps

class SqlOps {
    fun <T> query(sql: String, vararg params: Pair<String, Any?>,
                  mapper: (ResultSet) -> T): List<T>

    // real JDBC cursor streaming via setFetchSize. Sequence is valid only inside `consume`.
    fun <T, R> stream(sql: String, vararg params: Pair<String, Any?>,
                      fetchSize: Int = 1000,
                      mapper: (ResultSet) -> T,
                      consume: (Sequence<T>) -> R): R

    fun execute(sql: String, vararg params: Pair<String, Any?>): Int
    fun <T> batch(sql: String, items: Iterable<T>,
                  binder: (PreparedStatement, T) -> Unit): IntArray
}

fun <R> MigrationContext.transactional(ops: SqlOps, block: SqlOps.() -> R): R
```

`stream` is callback-style by design — the `Sequence<T>` is only valid inside
`consume`. After `consume` returns, the `ResultSet`/`PreparedStatement`/
`Connection` are closed (`.use {}`-scoped); any iteration on a leaked
reference throws `SQLException("ResultSet is closed")`. This makes it
physically impossible to leak a JDBC handle past `db.inTx`.

Typical use:
```kotlin
jdbc(db).stream(
    "select id from orders where status = :s", "s" to "STUCK",
    fetchSize = 5000,
    mapper = { it.getLong("id") },
) { rows ->
    forEach(rows, parallel = 4, onError = OnError.Skip) { id -> ... }
}
```

PostgreSQL cursor mode requires `autoCommit = false` — `db.inTx` does that
unconditionally, so `setFetchSize` actually pages instead of materializing.

Named parameters are `:name` style. The parser ignores `:name` inside
single-quoted strings, double-quoted identifiers, and `--` line comments
(Postgres semantics). Mismatches throw `IllegalArgumentException`:
duplicate keys, missing keys (in SQL but not provided), extra keys
(provided but not in SQL).

`batch` is positional (`?`), not named — `executeBatch` semantics.

`transactional` opens one `Connection` via Kora `db.inTx`, runs the block
with a tx-bound `SqlOps`. Commit on normal return, rollback on throw.
Nested `transactional` calls throw `IllegalStateException` — there is no
smart merge.

Under dry-run, `transactional` does **not** open a real connection — it
runs the block on the free-mode `SqlOps`. Writes are skipped via
`guardWrite`; reads (`query`, `stream`) still work, each in its own
mini-tx.

### 8.2 Kafka (`kafka`, `topic`)

```kotlin
fun <K, V> MigrationContext.kafka(producer: Producer<K, V>): KafkaOps<K, V>
fun <K, V> MigrationContext.topic(producer: Producer<K, V>, name: String): KafkaTopic<K, V>

class KafkaOps<K, V> {
    fun publish(topic: String, key: K, value: V): PublishResult?     // sync
    fun publishAsync(topic: String, key: K, value: V): CompletableFuture<PublishResult?>
}
class KafkaTopic<K, V> : AutoCloseable {
    fun send(key: K, value: V): KafkaOps.PublishResult?              // sync
    override fun close()                                              // producer.flush() once
}
```

`topic(...)` registers itself. The producer object itself is owned by the
Kora graph; the DSL never closes it. On run end, the topic handle calls
`producer.flush()` once (CAS-guarded).

When the consumer side uses an idiomatic typed Kora `@KafkaPublisher`,
publish through it directly and wrap the call in `mutation`:

```kotlin
@KafkaPublisher("kafka.orders.publisher")
interface OrdersPublisher {
    @KafkaPublisher.Topic("kafka.orders.publisher.resyncTopic")
    fun publishResync(key: String, @Json value: OrderEvent)
}

// in migrate():
mutation("orders.resync", args = mapOf("orderId" to order.id)) {
    publisher.publishResync(order.id.toString(), OrderEvent.from(order))
}
```

`kafka(producer)` / `topic(producer, name)` are for raw
`Producer<K, V>` access (multi-topic ad-hoc publishing or per-script
serialization). For a single typed topic, the `@KafkaPublisher` route is
cleaner.

### 8.3 HTTP (`http`)

```kotlin
typealias HttpCall =
    (method: String, path: String, body: ByteArray?, headers: Map<String, String>) -> Int

class HttpOps {
    fun get(path: String, headers: Map<String, String> = emptyMap()): Int   // no dry-run gate
    fun post(path: String, body: ByteArray = ByteArray(0), ...): Int        // gated
    fun patch(path: String, body: ByteArray = ByteArray(0), ...): Int       // gated
    fun put(path: String, body: ByteArray = ByteArray(0), ...): Int         // gated
    fun delete(path: String, headers: Map<String, String> = emptyMap()): Int // gated
}

fun MigrationContext.http(call: HttpCall): HttpOps
```

`HttpCall` is a function reference; adapt any HTTP client to it. For a
typed Kora `@HttpClient`, prefer calling the client directly and wrapping
the write in `mutation`. `HttpOps` is the convenience layer for one-off
scripts that do not want to declare a typed client.

### 8.4 Cassandra (`cassandra`)

```kotlin
fun MigrationContext.cassandra(session: CqlSession): CassandraOps

class CassandraOps {
    fun <T> query(cql: String, vararg params: Pair<String, Any?>,
                  mapper: (Row) -> T): List<T>
    fun <T> stream(cql: String, vararg params: Pair<String, Any?>,
                   mapper: (Row) -> T): Sequence<T>     // true paged streaming
    fun execute(cql: String, vararg params: Pair<String, Any?>): ResultSet?
    fun <T> batch(cql: String, items: Iterable<T>,
                  binder: (BoundStatementBuilder, T) -> Unit)
}
```

Prepared statements are **not** cached by `CassandraOps` itself — the factory
`cassandra(session)` builds a new instance each call, so a local cache would
always be empty. The DataStax driver 4.x keeps its own per-session
prepare-cache (keyed by CQL string + protocol version): a second
`session.prepare(sameCql)` returns the same `PreparedStatement` from the
driver cache. Named parameters bind via `setExplicit` — works for primitives,
strings, UUID, Instant, plus `List<*>`/`Set<*>` (element class inferred from
the first non-null element). Cassandra UDTs / tuple / Map / custom codecs
need their own `bb.set(name, value, GenericType)` call written by the user
inside a custom op (see customization).

`stream` is real lazy streaming over the driver cursor (paged). `batch` is
a sequential `execute` loop — Cassandra has no JDBC-style batch.

## 9. Configuration (HOCON)

```hocon
migration {
  run = ${?MIGRATION_RUN}                       # name; null = idle
  dryRun = false                                 # also ${?MIGRATION_DRY_RUN}
  outputFolder = ${?MIGRATION_OUTPUT_FOLDER}     # default logs/<name>

  defaults {
    onUnhandled = FAIL_FAST                      # or LOG_AND_COMPLETE
    errorThreshold = 0                            # >0 enables real-time abort
    progressEvery = 1000                          # Progress.Default tick
    parallel = 1                                  # default FixedThreadPool size
  }

  errorReporting {
    includeStackTrace = true
    maxItemReprLength = 500
  }

  report {
    asciiOnly = false                             # true for terminals without unicode
  }
}
```

Custom executor (overrides the FixedThreadPool):

```kotlin
@Component
@Tag(MigrationExecutor::class)
fun migrationExecutor(): Executor =
    Executors.newWorkStealingPool(8)
```

`MigrationExecutor` is the marker class exported by the kora module.
Lifetime of a custom executor is the user's responsibility; the runner
does not call `shutdown()` on it.

## 10. Dry-run model

Setting `migration.dryRun = true` (or `MIGRATION_DRY_RUN=true`) flips
`ctx.dryRun` to `true`. Each guarded write turns into:

- skip executing the action,
- return the supplied `dryRunDefault` (`0` for SQL row counts, `null` for
  `RecordMetadata`, `IntArray(0)` for batch, etc.),
- log `INFO [DRY-RUN] <label> (<args>)`,
- increment `report.dryRunSkipped[label]` by 1.

What is guarded:

| Op | Label |
|---|---|
| `jdbc.execute` | `jdbc.execute` |
| `jdbc.batch` | `jdbc.batch` |
| `cassandra.execute` | `cassandra.execute` |
| `cassandra.batch` | `cassandra.batch` |
| `kafka.publish` | `kafka.publish` |
| `kafka.publishAsync` | `kafka.publishAsync` |
| `http.post/patch/put/delete` | `http.post`, ... |
| `mutation(label) { ... }` | `mutation:<label>` |
| `transactional { ... }` (whole block) | `jdbc.transactional` |

What is **not** guarded:

- `jdbc.query` / `jdbc.stream` / `cassandra.query` / `cassandra.stream` —
  reads pass through.
- `http.get` — read-only.
- `openCsv(...).row(...)` — file writes are diagnostic artifacts.
- `errors.report(...)` — audit trail is independent of dry-run.

## 11. Known limitations & footguns

This section is for things the DSL **cannot fix** (inherent constraints
of the underlying systems or deliberate design choices) and things that
are **easy to do wrong** (subtle API contracts you have to internalize).

Features that are not gotchas — exit codes, idle mode, `errorThreshold`
semantics, `Progress.Default` adaptation, callback-scope of
`SqlOps.stream` — are documented in the relevant API sections (§7, §9)
rather than repeated here.

### 11.1 Known limitations (inherent — cannot be removed in the DSL)

**No two-phase commit for `mutation` inside `transactional`.** Kafka
`send()` enqueues into the producer accumulator immediately; the record
is only flushed when the `topic(...)` handle is closed at run end (or the
producer buffer fills). The Postgres tx commits / rolls back **before**
that flush. If the tx rolls back, the message may still reach Kafka.
There is no Kafka+JDBC two-phase commit in the standard ecosystem. Treat
`mutation` inside `transactional` as "best-effort outbox", not as
transactional publish — for actual transactional semantics, write to an
outbox table inside the tx and let a separate dispatcher publish.

**`transactional` does not nest.** A nested call throws
`IllegalStateException` — there is no smart merge with an outer tx. If a
sub-routine needs to be tx-aware, take `SqlOps` as a parameter and let
the caller pass a tx-bound one; do not call `transactional` again inside.

**Cancellation in parallel `forEach` is cooperative.** When the first
worker fails, `runAcross` calls `Future.cancel(true)` on every outstanding
submission. Two consequences:

1. The interrupt only stops cooperative user code — `Thread.sleep`,
   `Object.wait`, NIO interruptible channels. Plain blocking JDBC reads,
   native HTTP clients, or tight CPU loops keep running until they hit a
   yield point. The JDK gives no portable way to force-stop a thread.
2. Real interrupt requires the executor to be an `ExecutorService` whose
   `submit()` returns a `FutureTask`-like Future. The production runner
   uses `Executors.newFixedThreadPool` — interrupt works. `ForkJoinPool`
   tasks and `CompletableFuture.runAsync` **ignore**
   `mayInterruptIfRunning` — for them cancel is best-effort (pending
   tasks removed from queue, running ones finish naturally). Custom
   executors via `@Tag(MigrationExecutor::class)` should be
   `ExecutorService` for fast-cancel; if you pass a plain `Executor`
   lambda, the DSL falls back to the no-interrupt path automatically.

**No retry primitive at the item level.** Deliberate — item-level retry
re-runs the whole `forEach` block, silently breaking correctness for
non-idempotent steps. Use Kora `@Retry` on typed `@HttpClient` /
`@KafkaPublisher` / repository methods for transient remote failures, or
write a `try/catch` around the specific failing primitive inside the
`forEach` body. See §7.4 for the rationale.

**`SqlOps.stream` Sequence is callback-scoped.** The `Sequence<T>` is
only valid inside the `consume` block; after it returns, the JDBC
`Connection` / `PreparedStatement` / `ResultSet` are closed. A leaked
reference iterated outside throws `SQLException("ResultSet is closed")`.
Compose (`.map` / `.filter` / `.groupBy`) **inside** `consume`, or fold
to a concrete return value (`R`) and let `stream` propagate it out. This
is by design — physically impossible to leak a JDBC handle past
`db.inTx`. See §8.1 for the rationale.

**Parallel `forEach` keeps one `Future` per submitted item.** The engine
collects every submitted `Future` into a list and joins them at the end.
For an `Iterable` / `Sequence` of N items submitted via `parallel > 1`,
heap retains O(N) `Future` objects until the loop completes — typically
a few hundred bytes each, so ~100 MB at 1M items. Bounded concurrency
via the `Semaphore(parallel)` only limits *running* tasks, not retained
ones. Mitigation for very large inputs (>1M items): use the chunked
overload (`forEach(items, chunk = 500, parallel = ...) { batch -> ... }`)
— each `Future` then represents a batch, so the retained set drops by
the chunk factor. The engine does not drain completed futures
incrementally; doing so would complicate cancellation semantics and is
not worth it at typical migration scale.

### 11.2 Footguns (easy to do wrong)

**`mutation` label must be a constant.** Per-item context goes in `args`.
Anti-example: `mutation("publish $id") { ... }` creates one entry per
item in `report.dryRunSkipped` instead of one aggregate row. Use
`mutation("publish", args = mapOf("id" to id)) { ... }`.

**`mutation` returns `Unit` unless you use the generic overload.** The
default overload discards the lambda's return value. For typed Kora client
calls that need a return (HTTP status, repository result):
`mutation<R>(label, args, dryRunDefault) { ... }` — or
`ctx.guardWrite<R>(label, args, dryRunDefault) { ... }` directly.

**`errors.includeItem<T> { ... }` must be inside `migrate()`.** Calling
it in a field initializer of the `Migration` subclass fails — `ctx.errors`
is only bound when the runner invokes `migrate()`. Put serializer
registration at the top of `migrate()`, before the first `forEach` that
might trigger a Skip.

**`ctx.executor` belongs to `forEach`.** Do not submit unrelated async
work to it from `migrate()` — if your work outlives `migrate()`, the
runner's `finally` closes its `AutoCloseable`s and (for the runner-owned
pool) calls `shutdownNow()`. Custom executors declared via
`@Tag(MigrationExecutor::class)` are **not** shut down by the runner; the
user owns their lifetime.

**`LOG_AND_COMPLETE` does not silence item-level `OnError.Fail`.**
`OnError.Fail` in a `forEach` re-throws past `migrate()`, where
`onUnhandled` policy applies. If you want "log everything, exit 0,
continue past errors", combine `forEach(... onError = OnError.Skip)` with
`onUnhandled = LOG_AND_COMPLETE`. `onUnhandled` only catches the
`migrate()`-level escape — it is not a global "ignore all errors" switch.

**Dry-run still writes CSV files to disk.** CSVs are diagnostic
artifacts, not side-effects on external systems. After a dry-run the
file at `outputFolder/<name>.csv` exists. If you treat the CSV as the
migration's deliverable (export archetype), this is intended; if not,
expect a leftover file.

## 12. Anti-patterns

- **Do not** wrap `jdbc.execute` in `mutation`. `jdbc.execute` is already
  guarded; double-wrapping double-counts `dryRunSkipped`.
- **Do not** call `producer.close()` from `migrate()` — the producer
  belongs to the Kora graph. Only `topic(...).close()` (auto-called by
  the runner) is safe, and it flushes without closing the producer.
- **Do not** instantiate `DefaultMigrationContext` directly in production
  code. Use the runner; for tests use `DefaultMigrationContext.test()`.
- **Do not** rely on `forEach` to fully interrupt mid-flight tasks on
  `OnError.Fail`. The DSL calls `Future.cancel(true)` on outstanding
  submissions, but interrupt is cooperative — see §11.1.
- **Do not** rely on the DSL for retry. There is no built-in retry; do
  not write `try { ... } catch { ... }` ad-hoc loops that simulate
  one — use Kora `@Retry` on the typed client method, which classifies
  exceptions and applies backoff at the right scope (the failing remote
  call, not the whole `forEach`-block).
- **Do not** put per-row `flush()` on `CsvOutput.row` in a hot loop —
  the close-time flush is the design. Call `flush()` only for explicit
  checkpoints.
- **Do not** mutate `report` fields directly. Use the increment methods
  on `ReportBuilder` if you need ad-hoc counters in a custom op.

## 13. Patterns / archetypes

### 13.1 Export — DB to CSV

```kotlin
override fun MigrationContext.migrate() {
    val out = openCsv("orders.csv", "id", "customer_id", "total")
    jdbc(db).stream(
        "select id, customer_id, total from orders",
        fetchSize = 5000,
        mapper = { Triple(it.getLong(1), it.getString(2), it.getBigDecimal(3)) },
    ) { rows ->
        forEach(rows, parallel = 1, progress = Progress.Every(10000)) { (id, c, total) ->
            out.row(id, c, total)
        }
    }
}
```

### 13.2 Correction — CSV to DB UPDATE

```kotlin
override fun MigrationContext.migrate() {
    data class Row(val id: Long, val newStatus: String)
    errors.includeItem<Row> { "id=${it.id}, target=${it.newStatus}" }

    val input = readCsv("input.csv", classpath = true)
        { Row(it["id"]!!.toLong(), it["status"]!!) }

    forEach(input, parallel = 4, onError = OnError.Skip) { row ->
        jdbc(db).execute(
            "update orders set status = :s where id = :id",
            "s" to row.newStatus, "id" to row.id,
        )
    }
}
```

If transient deadlocks on Postgres need a retry, wire a Kora-level
retry on the JDBC repository method instead of looping here.

### 13.3 Resend — DB to enriched Kafka publish

```kotlin
@KafkaPublisher("kafka.orders.publisher")
interface OrdersPublisher {
    @KafkaPublisher.Topic("kafka.orders.publisher.resyncTopic")
    @Retry("kafka.orders.publisher.resync")    // Kora annotation — retries the publish call only
    fun publishResync(key: String, @Json value: OrderEvent)
}

// HOCON: resilient.retry.kafka.orders.publisher.resync {
//   delay = "1s", attempts = 2, delayStep = "1s"   # 2 retries, linear backoff
// }

@Component
class Resync(
    private val db: JdbcConnectionFactory,
    private val publisher: OrdersPublisher,
) : Migration("RESYNC-001", "agent") {
    override fun MigrationContext.migrate() {
        val ids = jdbc(db).query("select id from orders where dirty = true")
            { it.getLong("id") }
        errors.includeItem<Long> { "orderId=$it" }
        forEach(ids, parallel = 8, onError = OnError.Skip) { id ->
            val order = jdbc(db).query("select * from orders where id = :id", "id" to id) {
                Order.from(it)
            }.single()
            mutation("orders.resync", args = mapOf("orderId" to id)) {
                publisher.publishResync(id.toString(), OrderEvent.from(order))
            }
        }
    }
}
```

The `@Retry` lives on the typed publisher method — the publish call (the
only step that might fail transiently) retries at the right scope, not
the entire `forEach`-block. Item-level retry would also re-execute the
JDBC enrichment query above, which is wasteful and would compound
failures.

### 13.4 HTTP backfill — DB to external POST

```kotlin
@HttpClient(configPath = "clients.backfill")
interface BackfillClient {
    @HttpRoute(method = HttpMethod.POST, path = "/api/backfill/{id}")
    @Retry("clients.backfill")                 // Kora retry, per call. Kora has only linear backoff.
    fun backfill(@Path id: Long, @Body payload: ByteArray): HttpResponse
}

// HOCON: resilient.retry.clients.backfill {
//   delay = "1s", attempts = 4, delayStep = "1s"   # 4 retries → 5 calls total
// }

override fun MigrationContext.migrate() {
    val items = jdbc(db).query("select id, payload from outbox") {
        it.getLong(1) to it.getString(2)
    }
    forEach(items, parallel = 4, onError = OnError.Skip) { (id, payload) ->
        mutation("backfill.post", args = mapOf("id" to id)) {
            val resp = backfillClient.backfill(id, payload.toByteArray())
            check(resp.code() in 200..299) { "non-2xx from /backfill/$id: ${resp.code()}" }
        }
    }
}
```

For ad-hoc / one-off HTTP without a typed client, use `http(httpCall).post`
directly and accept that you give up Kora's retry; either wrap in
`mutation` (no return value but dry-run-gated) or `ctx.guardWrite<Int>`
to get the status code back.

### 13.5 Two-source comparison — diff to CSV

```kotlin
override fun MigrationContext.migrate() {
    val diff = openCsv("diff.csv", "id", "pg_status", "cass_status")
    val pgRows = jdbc(db).query("select id, status from orders") {
        it.getLong(1) to it.getString(2)
    }.toMap()

    forEach(cassandra(c).stream("select id, status from orders")
        { it.getLong("id") to it.getString("status") },
        parallel = 1
    ) { (id, cassStatus) ->
        val pgStatus = pgRows[id]
        if (pgStatus != cassStatus) diff.row(id, pgStatus ?: "MISSING", cassStatus)
    }
}
```

## 14. Testing migrations

```kotlin
class FixOrderStatusesTest {
    @Test fun `dry run does not touch db`() {
        val ctx = DefaultMigrationContext.test(dryRun = true)
        val db = mockJdbc(/* fixtures */)
        val mig = FixOrderStatuses(db)

        with(ctx) { mig.run { migrate() } }

        val report = ctx.report.build(
            ctx.outputFolder.resolve("errors.csv"),
            ctx.outputFolder.resolve("errors.log"),
        )
        assertThat(report.dryRunSkipped).containsKey("jdbc.execute")
        assertThat(report.failed).isZero()
    }
}
```

`DefaultMigrationContext.test()` defaults: `dryRun = false`, output to a
fresh `Files.createTempDirectory("migration-test-")`, executor =
`ForkJoinPool.commonPool()`. Override any of these per test.

For integration tests against a real Postgres/Kafka, use Testcontainers
the same way as you would for any Kora component. Construct a real
`JdbcConnectionFactory` against the container, instantiate the migration
directly, pass `DefaultMigrationContext.test(dryRun = false, outputFolder
= tmp)` as the receiver.

## 15. Customization

### 15.1 Custom executor

```kotlin
@Component
@Tag(MigrationExecutor::class)
fun executor(): Executor = Executors.newWorkStealingPool(16)
```

### 15.2 Custom `ErrorReporter`

`MigrationContext.errors` is typed as the concrete `CsvFileErrorReporter`
(open class), **not** the `ErrorReporter` interface. The shipped
`MigrationRunner` always instantiates `CsvFileErrorReporter` — there is
no `@Tag`-based replacement hook out of the box. This is intentional:
keep the API honest about what the runner actually does. The
`ErrorReporter` interface is still exported for fork-and-customize use
cases and for the `includeItem<T>` extension; the concrete class is
`open` and `report(...)` is overridable, so you can subclass without
forking the file:

```kotlin
class JsonLinesReporter(
    name: String, author: String,
    errorsFile: Path, tracesFile: Path,
    private val jsonOut: BufferedWriter,
) : CsvFileErrorReporter(name, author, errorsFile, tracesFile) {
    override fun report(e: Throwable, item: Any?) {
        // write JSON line for each error; ignore the inherited CSV path.
    }
}
```

To wire it in, you still need to bypass the shipped runner (fork
`MigrationRunner` or write your own `Lifecycle` component that calls
`DefaultMigrationContext.internalCreate(...)` with your subclass).
There is no pluggable factory bean; if you need one, the cleanest path
is a custom `Lifecycle` mirroring `MigrationRunner.executeMigration(...)`.

### 15.3 Custom op extension

```kotlin
fun MigrationContext.redis(client: RedisClient): RedisOps = RedisOps(this, client)

class RedisOps internal constructor(private val ctx: MigrationContext, ...) {
    fun set(k: String, v: String) =
        ctx.guardWrite("redis.set", mapOf("key" to k)) { client.set(k, v) }
}
```

Pass through `ctx.guardWrite` so dry-run, logging, and report aggregation
work out of the box. Long-lived state should be registered:
`ctx.register(AutoCloseable { closeRedis() })`.

## 16. File map (for studying the source)

```
core/src/main/kotlin/io/migration/
  Migration.kt                  Migration base + ScriptPolicy
  MigrationContext.kt            receiver interface
  ResourceRegistry.kt            register(AutoCloseable) contract
  ForEach.kt                     three forEach overloads
  OnError.kt                     Fail/Skip/Handle, Decision
  Progress.kt                    Default/Off/Every/Custom
  Mutation.kt                    mutation(label, args) { }
  csv/CsvRead.kt                  readCsv → Sequence<T>
  csv/CsvWrite.kt                 openCsv → CsvOutput
  error/ErrorReporter.kt          interface + includeItem<T> reified ext
  error/CsvFileErrorReporter.kt   default impl (errors.csv / errors.log)
  report/MigrationReport.kt       immutable snapshot
  report/ReportBuilder.kt         mutable counters during run
  report/ReportFormatter.kt       text rendering (ascii / unicode)
  internal/
    DefaultMigrationContext.kt    only impl of MigrationContext
    ForEachEngine.kt              parallel execution + cancellation
    ProgressTicker.kt             progress tick logic
    ErrorThresholdExceeded.kt     sentinel for real-time threshold abort

kora/src/main/kotlin/io/migration/kora/
  MigrationModule.kt              @Module registering MigrationRunner
  MigrationRunner.kt              Lifecycle, owns init() flow
  MigrationConfig.kt              @ConfigSource("migration") shape
  MigrationExecutor.kt            @Tag marker for custom Executor
  ops/SqlOps.kt                    jdbc + transactional
  ops/KafkaOps.kt                  kafka + topic
  ops/HttpOps.kt                   http(call) wrapper
  ops/CassandraOps.kt              cassandra
```

## 17. Pre-flight checklist (before writing migrations in another project)

1. The library is published to a Maven repo (mavenLocal,
   GitHub Packages, internal Nexus) or wired via `includeBuild`.
2. Consumer project has Logback on the runtime classpath (the runner
   attaches a `FileAppender` programmatically; without Logback,
   `migration.log` is silently disabled).
3. `@KoraApp` mixes in `MigrationModule` plus whatever
   infrastructure modules the migration touches.
4. `application.conf` has at least `migration.run = ${?MIGRATION_RUN}`.
5. The migration class is `@Component` and its `name` is unique across
   the graph.
6. The consumer has run one **dry-run** end-to-end and inspected
   `logs/<name>/migration.log` + the stdout report.
7. The consumer has decided on `defaults.errorThreshold` — `0` means
   "tolerate any number of skips"; for compliance scripts, set it.
