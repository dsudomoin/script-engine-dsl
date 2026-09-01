# Migration DSL — Agent Guide

A guide for an AI coding agent integrating this library into a downstream
Kora-based service. Self-contained: you do not need to read any other file
in this repo to write a correct migration.

Companion documents, same API, different audience: `docs/USER_GUIDE.md` is
the long-form Russian guide for humans, `docs/examples/*` are full
copy-paste archetypes, and `example/` is a runnable consumer application
(§3). They are kept in sync with this file; if you ever find a disagreement,
the source wins — check `core/src/main/kotlin` and `kora/src/main/kotlin`.

Version: `0.1.0`. Kotlin 2.1+, JVM 21+, Kora 1.1+.

---

## Contents

| § | Section | Read it when |
|---|---|---|
| [1](#1-what-this-library-is) | What this library is | deciding whether the DSL fits at all |
| [2](#2-when-to-use--when-to-skip) | When to use / when to skip | same |
| [3](#3-repository-layout) | Repository layout | wiring the dependency into a build |
| [4](#4-wire-up) | Wire-up | first five minutes: `@KoraApp`, HOCON, env vars |
| [5](#5-hello-migration) | Hello migration | you want one compiling script now |
| [6](#6-mental-model) | Mental model | you need to know what runs in what order |
| [7](#7-api-reference--core) | API reference — `core` | `forEach`, `OnError`, `Progress`, `mutation`, CSV |
| [8](#8-api-reference--kora) | API reference — `kora` | `jdbc`, `kafka`, `http`, `cassandra` |
| [9](#9-configuration-hocon) | Configuration (HOCON) | every `migration.*` key and its default |
| [9.1](#91-exit-codes) | Exit codes | wiring the run into CI or a K8s Job |
| [10](#10-dry-run-model) | Dry-run model | before trusting a rehearsal run |
| [11](#11-known-limitations--footguns) | Limitations & footguns | something behaves surprisingly |
| [12](#12-anti-patterns) | Anti-patterns | reviewing a migration someone wrote |
| [13](#13-patterns--archetypes) | Archetypes | export / correction / resend / backfill / comparison |
| [14](#14-testing-migrations) | Testing migrations | you need the run covered by tests |
| [15](#15-customization) | Customization | custom executor, reporter, or op |
| [16](#16-file-map-for-studying-the-source) | File map | you are reading the library source |
| [17](#17-pre-flight-checklist-before-writing-migrations-in-another-project) | Pre-flight checklist | final check before shipping |

---

## 1. What this library is

A Kotlin DSL for **one-shot data-migration scripts** that run inside a Kora
application graph. A migration is a single `Migration` subclass that owns a
`migrate()` block; the library owns the cross-cutting concerns:

- parallel item processing with bounded fan-out;
- item-level error policy (`Fail` / `Skip` / `Handle`);
- auto-audit of failed items to `errors.csv` + `errors.log`;
- a dry-run gate on every write **the library itself performs**;
- structured progress logs and a final stdout report;
- a per-migration `outputFolder` collecting all artifacts.

Dry-run is **not** transparent to user code. It intercepts the library's own
ops (`jdbc.execute`, `kafka.publish`, `http.post`, …). A direct call into
your own repository, `@KafkaPublisher` or `@HttpClient` executes for real
during a dry run unless you wrap it in `mutation("label") { ... }` (§7.7).
The runner prints a loud WARN — `DRY-RUN processed N item(s) but intercepted
0 writes` — when a dry run processed items but gated nothing; that line is
the only observable symptom of a forgotten `mutation { }`. See §10.

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
core/     pure Kotlin, no Kora. DSL primitives, CSV, error audit, report.
kora/     Kora bridge: MigrationRunner, MigrationModule, MigrationConfig,
          MigrationExit, ops/* (jdbc, kafka, http, cassandra).
example/  a runnable migration app. It is also the compile-and-run proof
          that the wire-up in §4 works on a real Kora graph — that check
          cannot live in kora/, because Kora's config KSP extension refuses
          to generate an extractor in the module that already has one.
docs/     Russian user guide + archetype examples. Not your target audience.
```

Consumer build. Kora's Kotlin code generation is **KSP only** — `kapt`,
`kapt("ru.tinkoff.kora:annotation-processors")` and `annotationProcessor`
are wrong and will not produce a graph:

```kotlin
plugins {
    kotlin("jvm") version "2.1.20"
    id("com.google.devtools.ksp") version "2.1.20-1.0.32"
}

dependencies {
    implementation("io.github.dsudomoin.migration:migration-dsl-core:0.1.0")
    implementation("io.github.dsudomoin.migration:migration-dsl-kora:0.1.0")
    ksp("ru.tinkoff.kora:symbol-processors:1.1.25")
}
```

Both artifacts are produced by `./gradlew publishToMavenLocal` in this repo
under exactly those coordinates.

## 4. Wire-up

Add `MigrationModule` to the `@KoraApp` interface alongside whatever
infrastructure modules the migration touches:

```kotlin
@KoraApp
interface App :
    HoconConfigModule,
    JdbcDatabaseModule,
    MigrationModule
```

`MigrationModule` alone is enough for the library: it reads the `migration`
section itself (`MigrationModule.migrationConfig`) and registers the runner
as `@Root`, so the graph instantiates it without anything depending on it.
There is no second, separate config module to mix in.

`KafkaProducerModule` **does not exist in Kora** — do not put it in
`@KoraApp`. To obtain the raw `org.apache.kafka.clients.producer.Producer`
that `kafka(...)` / `topic(...)` need, pick one of:
- declare it yourself in your own `@Module` (a factory returning
  `KafkaProducer(props)`);
- take it from a generated Kora publisher: `GeneratedPublisher.producer()`
  returns `Producer<ByteArray, ByteArray>`;
- or skip the raw producer entirely — use a typed `@KafkaPublisher` and wrap
  the call in `mutation("label") { ... }`. This stays the recommended route.

In `application.conf`, the minimum is:

```hocon
migration {
  run = ${?MIGRATION_RUN}          # name of the migration to execute; null = idle
  dryRun = ${?MIGRATION_DRY_RUN}   # without this line MIGRATION_DRY_RUN does nothing
}
```

**Environment variables reach the runner only through `${?VAR}` in HOCON.**
There is not a single `System.getenv` in the library. If `dryRun` has no
`${?MIGRATION_DRY_RUN}` substitution in the config, then
`MIGRATION_DRY_RUN=true ./gradlew run` performs **real writes** — the flag
is silently ignored. The same holds for `run` and `outputFolder`.

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

Run it (the second line only does anything if `application.conf` contains
`dryRun = ${?MIGRATION_DRY_RUN}` — see §4):
```bash
MIGRATION_RUN=FIX-ORDERS-001 ./gradlew run
MIGRATION_RUN=FIX-ORDERS-001 MIGRATION_DRY_RUN=true ./gradlew run
```

Artifacts land in `logs/FIX-ORDERS-001/` and the final report prints to
stdout.

A complete, runnable version of the same shape lives in `example/`
(`ExampleApp.kt` + `BackfillCustomerTier.kt` + `application.conf`):
`MIGRATION_RUN=CUSTOMER-TIER-001 ./gradlew :example:run`.

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
|   executor, defaultParallel,|
|   register(...), shared(...)|
+--------------+--------------+
               |
               | extensions on MigrationContext
               v
   forEach(...)        ops:
   mutation { ... }      jdbc(db).query/stream/execute/batch/executeReturning
   openCsv(...)          transactional(jdbc(db)) { ... }
   readCsv(...)          kafka(producer).publish[Async]
   ctx.register(...)     topic(producer, name).send[Async]
   ctx.shared(...)       cassandra(session).query/stream/execute/batch
                         http(call).get/post/patch/put/delete
```

Lifecycle:
1. Runner reads HOCON. `migration.run == null` → it logs "runner idle" and
   returns without calling exit at all. Then it validates config values and
   migration-name uniqueness, and resolves the migration by name.
2. Creates `outputFolder` (`mkdir -p`), attaches a Logback `FileAppender`
   to the root logger so everything in slf4j ends up in `migration.log`.
3. Builds `MigrationContext` with a `CsvFileErrorReporter`, a
   `ReportBuilder`, and either a custom `@Tag(MigrationExecutor::class)`
   `Executor` or a fresh **cached** thread pool (daemon threads). Cached,
   not fixed: `forEach(parallel = N)` bounds itself with its own semaphore,
   and the pool must be able to hand out N threads on demand.
4. Invokes `migration.migrate()` with the context as the receiver.
5. In `finally`, closes every `AutoCloseable` the user registered (CSVs,
   Kafka topic handles, custom holders) **in reverse registration order**,
   then the runner-owned pool (`shutdown()` + 30 s wait; if it does not
   terminate, `shutdownNow()` and the dropped-task count goes into the
   report as a warning).
6. Closes the reporter; under dry-run, warns if nothing was intercepted
   (§10); raises the exit code to 1 if any async Kafka delivery failed;
   builds the final `MigrationReport`, prints it, detaches the file
   appender, and exits with the code.
7. Exit means `exitProcess(code)` unless a `MigrationExit` component is in
   the graph, in which case the code is handed to it and the JVM survives
   (§14). Exit codes: §9.1.

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
| `report: ReportBuilder` | Counters (`successful`, `skipped`, `failed`, `asyncFailed`, `dryRunSkipped` map). Mutated automatically. |
| `executor: Executor` | Pool used by `forEach(parallel > 1)`. Cached pool owned by the runner. Do not use for arbitrary async work. |
| `outputFolder: Path` | All artifacts go here. Resolves `logs/<name>` by default. |
| `errors: CsvFileErrorReporter` | Audit sink for failed items. Concrete class, not the `ErrorReporter` interface — see §15.2 for the rationale. See 7.6 for usage. |
| `defaultProgressEvery: Int` | Tick period for `Progress.Default`. From `migration.defaults.progressEvery`. |
| `defaultParallel: Int` | Default value of the `parallel` argument of `forEach`. From `migration.defaults.parallel` (default `1`). |
| `errorThreshold: Long` | Abort run when `report.skipped > threshold`. `0` = disabled. |
| `register(c: AutoCloseable)` | Schedule LIFO close after `migrate()`. |
| `shared(key, factory): T : AutoCloseable` | Run-scoped memoized resource: created once per `key`, registered for close. Used inside `kafka(...)` / `topic(...)`; use it for your own op handles that get called from inside a `forEach` body. The factory must not call `shared` again. |
| `guardWrite(label, args, default, action)` | Dry-run gate. Wraps every write op in the library. |
| `auditError(e, item)` | Manual audit to `errors.csv`. Rarely called directly. |

### 7.3 `forEach` — four overloads

```kotlin
// item-by-item
fun <T> MigrationContext.forEach(
    items: Iterable<T>,
    parallel: Int = defaultParallel,   // migration.defaults.parallel, out of the box 1
    onError: OnError = OnError.Fail,
    onErrorLog: ((Throwable, T) -> Unit)? = null,
    logEach: ((T) -> String)? = null,
    progress: Progress = Progress.Default,
    block: MigrationContext.(T) -> Unit,
)

// chunked: each worker receives a List<T> of size `chunk`
@JvmName("forEachChunked")
fun <T> MigrationContext.forEach(
    items: Iterable<T>, chunk: Int, parallel: Int = defaultParallel,
    onError: OnError = OnError.Fail,
    onErrorLog: ((Throwable, List<T>) -> Unit)? = null,
    logEach: ((List<T>) -> String)? = null,
    progress: Progress = Progress.Default,
    block: MigrationContext.(List<T>) -> Unit,
)

// Sequence — adapter; semantically equivalent to .asIterable()
fun <T> MigrationContext.forEach(
    items: Sequence<T>, parallel: Int = defaultParallel, /* same kwargs */
    block: MigrationContext.(T) -> Unit,
)

// Sequence + chunk — LAZY chunking via Sequence.chunked: only the current batch
// is materialized, not the whole stream. For million-row JDBC/Cassandra streams
// feeding an `IN (:ids)` query or a bulk insert.
@JvmName("forEachSequenceChunked")
fun <T> MigrationContext.forEach(
    items: Sequence<T>, chunk: Int, parallel: Int = defaultParallel, /* same kwargs as forEachChunked */
    block: MigrationContext.(List<T>) -> Unit,
)
```

Parallel execution: `parallel = 1` runs inline on the calling thread (the
executor is not touched at all). When `parallel > 1`, items are submitted to
`ctx.executor` and bounded by a `Semaphore(parallel)` so that a long
`Sequence` does not materialize into the pool queue. `parallel` must be
`> 0` — otherwise `IllegalArgumentException`.

`parallel = N` really gives N concurrent workers: the runner's pool is
cached, so it grows to whatever the loop asks for. Consequences worth
knowing: `migration.defaults.parallel` no longer caps anything (it is only
the *default value* of this argument), and a nested `forEach(parallel > 1)`
inside another parallel `forEach` no longer deadlocks. Both were true of
earlier builds; do not carry over that mental model.

The first exception collected by a worker triggers `Future.cancel(true)` on
every other outstanding submission — see §11.1 "Cancellation in parallel
`forEach` is cooperative" for the caveats. All collected exceptions are
returned as `addSuppressed` on the primary throw — caller sees one throw
chain.

`logEach` and `onErrorLog` are diagnostics only: if one of them throws, the
item's outcome is unchanged (no double counting, no abort). The first such
failure is reported once as a warning in the report; later ones are silent.

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
  v0.1.0; if you genuinely need them at this layer, wrap the primitive call
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
    data class Every(val n: Int) : Progress                       // n > 0
    data class Custom(val n: Int, val format: (Long, Long?) -> String) : Progress
    companion object { fun custom(n: Int = 1000, format: ...): Custom }
}
```

`format` receives `(doneCount, totalOrNull)`. `total` is `null` for
`Sequence` sources because the size is unknown. `Every(n)` / `Custom(n, …)`
require `n > 0` and throw `IllegalArgumentException` otherwise — use
`Progress.Off` to disable.

The ticker counts items that came back from the block normally: successes
plus audited skips. A terminal `OnError.Fail` aborts before its tick.

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
useless — it is truncated at `errorReporting.maxItemReprLength`). The
reporter writes `errors.csv` (one row per failed item) and `errors.log`
(stack traces if `errorReporting.includeStackTrace = true`). Both files are
created lazily on the first error, so a clean run leaves neither behind.
Besides `forEach` skips, the sink also receives skipped `readCsv` rows,
failed async Kafka deliveries, and the exception that escapes `migrate()`.
Timestamps in the final report carry a zone offset; `errors.csv` is in UTC.

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
                                 onRowError: OnError = OnError.Fail,
                                 mapper: (Map<String, String>) -> T): Sequence<T>
fun <T> MigrationContext.readCsv(path: Path,
                                 onRowError: OnError = OnError.Fail,
                                 mapper: (Map<String, String>) -> T): Sequence<T>

// write: handle, registered as AutoCloseable
interface CsvOutput : AutoCloseable {
    fun row(vararg cells: Any?)   // thread-safe; RFC 4180 quoting
    fun flush()                    // explicit; close() flushes too
}

fun MigrationContext.openCsv(path: Path, vararg headers: String): CsvOutput
fun MigrationContext.openCsv(filename: String, vararg headers: String): CsvOutput
```

`onRowError` covers a single row, both the CSV parse and your `mapper`.
The default is `OnError.Fail`: **the first malformed row aborts the whole
run.** To get the "bad rows go to `errors.csv`, the run continues" behavior
you have to ask for it explicitly:

```kotlin
readCsv("input.csv", classpath = true, onRowError = OnError.Skip) { row -> ... }
```

Under `Skip` (or `Handle → Skip`) the row is audited, increments
`report.skipped` — so it counts towards `errorThreshold` — and never
reaches `forEach`. The `onError` of the surrounding `forEach` has no say
here: it only sees rows that were mapped successfully.

The underlying stream is registered in the context, so an under-consumed
sequence (`take(n)`, early return, a throw upstream) does not leak the file
descriptor.

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

### 7.9 `ctx.register(AutoCloseable)` and `ctx.shared(key) { ... }`

`register` schedules any resource you open inside `migrate()` for a
reverse-order close after the script ends. `openCsv`, `kafka(...)` and
`topic(...)` already register themselves; you only call `register` directly
for custom holders. A `close()` that throws is not propagated: it is logged
and lands in `report.warnings`, exit code unchanged.

`shared(key) { factory }` is `register` plus memoization by `key` — the
factory runs once per run and every later call with the same key returns
the same instance. Use it for op handles that are naturally called from
inside a `forEach` body: without it, a `topic(producer, "t")` call per item
would push a million objects into the registry. `kafka(...)` and `topic(...)`
are built on it (keyed by the producer, and by producer + topic name).

## 8. API reference — `kora`

### 8.1 SQL (`jdbc`, `transactional`)

```kotlin
fun MigrationContext.jdbc(db: JdbcConnectionFactory): SqlOps

class SqlOps {
    // READ-ONLY (not gated by dry-run, so writes are rejected up front)
    fun <T> query(sql: String, vararg params: Pair<String, Any?>,
                  mapper: (ResultSet) -> T): List<T>

    // real JDBC cursor streaming via setFetchSize. Sequence is valid only inside `consume`.
    fun <T, R> stream(sql: String, vararg params: Pair<String, Any?>,
                      fetchSize: Int = 1000,
                      mapper: (ResultSet) -> T,
                      consume: (Sequence<T>) -> R): R

    // WRITES (dry-run gated)
    fun execute(sql: String, vararg params: Pair<String, Any?>): Int          // dry-run: 0
    fun <T> batch(sql: String, items: Iterable<T>,
                  binder: (PreparedStatement, T) -> Unit): IntArray           // dry-run: IntArray(0)
    fun <T> executeReturning(sql: String, vararg params: Pair<String, Any?>,
                             mapper: (ResultSet) -> T): List<T>               // dry-run: emptyList()
}

fun <R> MigrationContext.transactional(ops: SqlOps, block: SqlOps.() -> R): R
```

`query` and `stream` accept **read statements only**. The first keyword
(after leading comments/parens) must be one of `select`, `with`, `show`,
`explain`, `values`, `table`, `describe`; anything else throws
`IllegalArgumentException` in both real and dry-run mode. The reason is
exactly the dry-run gate: reads are not gated (they must work during a
rehearsal), so `query("insert ... returning id")` used to execute **for
real** during a dry run. Use `executeReturning` when a write has to give you
rows back — it goes through the gate like any other write and returns an
empty list under dry-run. Known hole: a data-modifying CTE
(`with ... insert ...`) starts with `with` and passes the check; route those
through `executeReturning` too.

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
single-quoted strings, double-quoted identifiers, `--` line comments,
`/* */` block comments, Postgres dollar-quoted bodies (`$$ ... $$`,
`$tag$ ... $tag$`) and `::casts`. Mismatches throw
`IllegalArgumentException`: duplicate keys, missing keys (in SQL but not
provided), extra keys (provided but not in SQL).

`batch` is positional (`?`), not named — `executeBatch` semantics.

`transactional` opens one `Connection` via Kora `db.inTx`, runs the block
with a tx-bound `SqlOps`. Commit on normal return, rollback on throw.
Nested `transactional` calls throw `IllegalStateException` — there is no
smart merge. The check fires under dry-run too, so a rehearsal cannot pass
green on code that aborts for real.

A tx-bound `SqlOps` remembers the thread that opened the transaction.
Using it from another thread — i.e. a parallel `forEach` **inside** a
`transactional { }` block — throws `IllegalStateException` instead of
corrupting data on a shared `Connection`. Put `transactional { }` inside
the `forEach` body, not the other way round.

Under dry-run, `transactional` does **not** open a real connection — it
runs the block on the free-mode `SqlOps`. **The block body still executes**:
what dry-run skips is the individual writes inside it, not the block as a
whole. Reads (`query`, `stream`) still work, each in its own mini-tx.

### 8.2 Kafka (`kafka`, `topic`)

```kotlin
fun <K, V> MigrationContext.kafka(producer: Producer<K, V>): KafkaOps<K, V>
fun <K, V> MigrationContext.topic(producer: Producer<K, V>, name: String): KafkaTopic<K, V>

class KafkaOps<K, V> : AutoCloseable {
    data class PublishResult(val topic: String, val partition: Int, val offset: Long)

    fun publish(topic: String, key: K, value: V): PublishResult?     // sync
    fun publishAsync(topic: String, key: K, value: V): CompletableFuture<PublishResult?>
    override fun close()                                              // producer.flush() once
}
class KafkaTopic<K, V> : AutoCloseable {
    fun send(key: K, value: V): KafkaOps.PublishResult?               // sync
    fun sendAsync(key: K, value: V): CompletableFuture<KafkaOps.PublishResult?>
    override fun close()                                              // delegates to KafkaOps.close()
}
```

Both factories memoize through `ctx.shared` — `kafka(producer)` per
producer, `topic(producer, name)` per (producer, name) pair — and register
the handle, so calling either one directly inside a `forEach` body is safe
and does not grow the registry. The producer object itself is owned by the
Kora graph; the DSL never closes it. On run end the handle calls
`producer.flush()` once (idempotent, CAS-guarded).

**Async delivery is accounted for.** `forEach` counts an item successful the
moment `publishAsync` hands the record to the producer, but the broker
answers later — and almost nobody reads the returned future. So the failure
callback does it for you: the record is written to `errors.csv`,
`report.asyncFailed` is incremented, and the runner raises the exit code to
1. `close()` flushes before the report is rendered, so those counters are
final by the time you see them. `asyncFailed` is deliberately not folded
into `failed` (it would break `processed = successful + skipped + failed`),
and the report prints the `✗ Async delivery failed:` line only when the
count is non-zero.

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

class HttpStatusException(val method: String, val path: String, val status: Int)
    : RuntimeException("$method $path -> HTTP $status")

class HttpOps {
    fun get(path: String, headers: Map<String, String> = emptyMap()): Int   // no dry-run gate
    fun post(path: String, body: ByteArray = ByteArray(0), ...): Int        // gated
    fun patch(path: String, body: ByteArray = ByteArray(0), ...): Int       // gated
    fun put(path: String, body: ByteArray = ByteArray(0), ...): Int         // gated
    fun delete(path: String, headers: Map<String, String> = emptyMap()): Int // gated
}

fun MigrationContext.http(call: HttpCall): HttpOps
```

**Every method checks the status code.** Anything outside `200..299` throws
`HttpStatusException(method, path, status)`, so a dead backend answering 500
cannot produce a report full of "successful" items. Classify by code in
`OnError.handle`:

```kotlin
onError = OnError.handle { e, _ ->
    if (e is HttpStatusException && e.status == 409) OnError.Decision.Skip
    else OnError.Decision.Fail
}
```

Under dry-run the gated methods return `200`, not `0` — a rehearsal must
take the same branch as the real run.

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
  run = ${?MIGRATION_RUN}                        # String?; name of the migration. null = idle
  dryRun = ${?MIGRATION_DRY_RUN}                 # Boolean, default false
  outputFolder = ${?MIGRATION_OUTPUT_FOLDER}     # String?; default logs/<name>

  defaults {
    onUnhandled = FAIL_FAST                      # enum, default FAIL_FAST; or LOG_AND_COMPLETE
    errorThreshold = 0                           # Long,  default 0 (>0 enables real-time abort)
    progressEvery = 1000                         # Int,   default 1000 (Progress.Default tick)
    parallel = 1                                 # Int,   default 1 — default value of forEach(parallel = ...)
  }

  errorReporting {
    includeStackTrace = true                     # Boolean, default true
    maxItemReprLength = 500                      # Int,     default 500
  }

  report {
    asciiOnly = false                            # Boolean, default false (true for non-unicode terminals)
  }
}
```

That is the complete key set — there is nothing else under `migration`.
Only `run`, `dryRun` and `outputFolder` are meant to be driven from the
environment, and only through `${?VAR}` substitution (§4).

`migration.defaults.parallel` is **not** a pool size. It is the default
value of the `parallel` argument of `forEach`; the runner's own pool is
cached and hands out as many threads as a given loop asks for.

`MigrationConfig` is a Kora `@ConfigValueExtractor` interface with default
methods (`config.run()`, `config.defaults().parallel()`), not a
`@ConfigSource` data class — that is the canonical shape for config a
library ships into someone else's app. For programmatic construction (tests,
embedding without HOCON) use the data classes `MigrationConfigValues`,
`DefaultsValues`, `ErrorReportingValues`, `ReportValues`. HOCON keys are
identical either way.

Values are sanity-checked at startup: `parallel > 0`, `progressEvery > 0`,
`errorThreshold >= 0`, `maxItemReprLength > 0`. A violation is exit code 2
with the offending key named in the log.

Custom executor — a factory method in your own `@Module` (`@Component`
annotates classes, not functions):

```kotlin
@Module
interface MigrationExecutorModule {
    @Tag(MigrationExecutor::class)
    fun migrationExecutor(): Executor = Executors.newCachedThreadPool()
}
```

`MigrationExecutor` is the marker class exported by the kora module.
Lifetime of a custom executor is the user's responsibility; the runner does
not call `shutdown()` on it (it only shuts down a pool it created itself).
Prefer an unbounded/cached `ExecutorService`: a fixed-size pool caps real
concurrency below the `parallel` you asked for and re-introduces the
deadlock on nested parallel `forEach`, and a plain `Executor` lambda loses
interrupt-based cancellation (§11.1).

### 9.1 Exit codes

| Code | When |
|---|---|
| `0` | Success. Also a `LOG_AND_COMPLETE` run that logged a failure. |
| `1` | `FAIL_FAST` on an unhandled exception out of `migrate()`; `errorThreshold` exceeded; one or more async Kafka deliveries failed. |
| `2` | Misconfiguration, before any work: unknown `migration.run` name, duplicate migration names in the graph, invalid `migration.*` values, `outputFolder` cannot be created. |

Idle (`migration.run` unset) is not an exit code: the runner logs "runner
idle" and returns without exiting at all — the surrounding Kora application
continues its normal lifecycle.

Exiting means `exitProcess(code)` unless a `MigrationExit` component is
present in the graph (§14).

## 10. Dry-run model

Setting `migration.dryRun = true` (via `dryRun = ${?MIGRATION_DRY_RUN}` —
the env var alone does nothing, §4) flips `ctx.dryRun` to `true`. Each
guarded write turns into:

- skip executing the action,
- return the supplied `dryRunDefault`,
- log `INFO [DRY-RUN] <label> (<args>)`,
- increment `report.dryRunSkipped[label]` by 1.

What is guarded:

| Call | Label | Value returned under dry-run |
|---|---|---|
| `jdbc(db).execute(...)` | `jdbc.execute` | `0` |
| `jdbc(db).batch(...)` | `jdbc.batch` | `IntArray(0)` |
| `jdbc(db).executeReturning(...)` | `jdbc.executeReturning` | `emptyList()` |
| `cassandra(s).execute(...)` | `cassandra.execute` | `null` |
| `cassandra(s).batch(...)` | `cassandra.batch` | `Unit` |
| `kafka(p).publish(...)` / `topic(...).send(...)` | `kafka.publish` | `null` |
| `kafka(p).publishAsync(...)` / `topic(...).sendAsync(...)` | `kafka.publishAsync` | completed future of `null` |
| `http(call).post/patch/put/delete` | `http.post`, … | `200` |
| `mutation(label) { ... }` | `mutation:<label>` | `Unit`, or your `dryRunDefault` |
| `transactional(ops) { ... }` | `jdbc.transactional` | result of the block |

`transactional` is the one row that is **not** a skip: the label is counted
and no real `Connection` is opened, but **the block body runs**. Writes
inside it are skipped one by one through their own labels; reads execute.

What is **not** guarded:

- `jdbc.query` / `jdbc.stream` / `cassandra.query` / `cassandra.stream` —
  reads pass through (which is why `query`/`stream` reject write statements,
  §8.1).
- `http.get` — read-only.
- `openCsv(...).row(...)` — file writes are diagnostic artifacts.
- `errors.report(...)` — audit trail is independent of dry-run.
- **anything you call yourself**: a repository method, a typed
  `@KafkaPublisher` / `@HttpClient`, an SDK client. The library cannot see
  those calls. Under dry-run they execute for real unless wrapped in
  `mutation("label") { ... }`.

Because of that last item, the runner emits a WARN — into the log and into
the report's warnings — when a dry run finished with
`processed > 0` and an **empty** `dryRunSkipped` map:

```
DRY-RUN processed 1000 item(s) but intercepted 0 writes. If this migration
writes anything, those writes went through FOR REAL — wrap typed client
calls in mutation("label") { ... }
```

Read every dry-run report for that line before trusting the rehearsal. It
is the only signal the library can give for a forgotten `mutation { }`.

## 11. Known limitations & footguns

This section is for things the DSL **cannot fix** (inherent constraints
of the underlying systems or deliberate design choices) and things that
are **easy to do wrong** (subtle API contracts you have to internalize).

Features that are not gotchas — exit codes and idle mode (§9.1),
`errorThreshold` semantics (§7.2), `Progress.Default` adaptation (§7.5),
callback-scope of `SqlOps.stream` (§8.1) — are documented in those sections
rather than repeated here.

### 11.1 Known limitations (inherent — cannot be removed in the DSL)

**No two-phase commit for a Kafka publish inside `transactional`.** Kafka
`send()` enqueues into the producer accumulator; the record leaves for the
broker on `linger.ms` / buffer pressure / a flush (the DSL flushes when the
handle closes at run end), and a synchronous `publish`/`send` blocks until
the broker acks it. Either way the send is not tied to the Postgres tx,
which commits or rolls back independently. If the tx rolls back, the message
may still be in Kafka.
There is no Kafka+JDBC two-phase commit in the standard ecosystem. Treat
`mutation` inside `transactional` as "best-effort outbox", not as
transactional publish — for actual transactional semantics, write to an
outbox table inside the tx and let a separate dispatcher publish.

**`transactional` does not nest, and is single-threaded.** A nested call
throws `IllegalStateException` (in dry-run too) — there is no smart merge
with an outer tx. If a sub-routine needs to be tx-aware, take `SqlOps` as a
parameter and let the caller pass a tx-bound one; do not call
`transactional` again inside. A tx-bound `SqlOps` also refuses to be used
from a thread other than the one that opened the transaction, because
`java.sql.Connection` is not thread-safe: `transactional { forEach(items,
parallel = 4) { execute(...) } }` throws. Invert it — `forEach { transactional
{ ... } }`.

**`WITH ... INSERT` passes the read-only check.** The guard on
`query`/`stream` looks at the first keyword, and a data-modifying CTE starts
with `with`. Such a statement will execute for real during a dry run. Route
it through `executeReturning` instead.

**`readCsv` fails the whole run on a malformed row by default.**
`onRowError` defaults to `OnError.Fail`; the resilient behaviour the export
and correction archetypes describe requires passing
`onRowError = OnError.Skip` explicitly (§7.8). The `onError` of the
surrounding `forEach` does not cover parsing or mapping.

**Cancellation in parallel `forEach` is cooperative.** When the first
worker fails, `runAcross` calls `Future.cancel(true)` on every outstanding
submission. Two consequences:

1. The interrupt only stops cooperative user code — `Thread.sleep`,
   `Object.wait`, NIO interruptible channels. Plain blocking JDBC reads,
   native HTTP clients, or tight CPU loops keep running until they hit a
   yield point. The JDK gives no portable way to force-stop a thread.
2. Real interrupt requires the executor to be an `ExecutorService` whose
   `submit()` returns a `FutureTask`-like Future. The production runner
   uses `Executors.newCachedThreadPool` — interrupt works. `ForkJoinPool`
   tasks (including `newWorkStealingPool` and `commonPool`, which is what
   `DefaultMigrationContext.test()` uses) and `CompletableFuture.runAsync`
   **ignore** `mayInterruptIfRunning` — for them cancel is best-effort
   (pending tasks removed from queue, running ones finish naturally).
   Custom executors via `@Tag(MigrationExecutor::class)` should be
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

**Parallel `forEach` retains a bounded set of `Future`s.** The engine
compacts completed futures out of its list whenever the list grows past
`max(parallel * 4, 64)` entries, and the `Semaphore(parallel)` caps how
many can be in flight, so the retained set stays on the order of the
concurrency level rather than O(N). It is still an in-memory list joined at
the end of the loop; for very large inputs the chunked overload
(`forEach(items, chunk = 500, parallel = ...) { batch -> ... }`) remains
the cheaper shape, since one future then covers a whole batch.

### 11.2 Footguns (easy to do wrong)

**A dry run does not protect calls the library cannot see.** Direct
repository / `@KafkaPublisher` / `@HttpClient` / SDK calls execute for real
under `dryRun = true`. Wrap every one of them in `mutation("label") { ... }`
and check the run's report for the `intercepted 0 writes` warning (§10).

**`MIGRATION_DRY_RUN=true` is inert without the HOCON line.** The library
never reads the environment itself; `dryRun = ${?MIGRATION_DRY_RUN}` must be
in `application.conf` or the "rehearsal" is a real run (§4).

**`jdbc().query` / `stream` reject non-read statements.** They are not
dry-run gated, so a write pushed through them would run during a rehearsal.
`IllegalArgumentException` at the call site; use `execute` or
`executeReturning`.

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
pool) calls `shutdown()`, waits 30 s, then `shutdownNow()`; dropped tasks
are only reported as a warning. Custom executors declared via
`@Tag(MigrationExecutor::class)` are **not** shut down by the runner; the
user owns their lifetime.

**`LOG_AND_COMPLETE` does not silence item-level `OnError.Fail`.**
`OnError.Fail` in a `forEach` re-throws past `migrate()`, where
`onUnhandled` policy applies. If you want "log everything, exit 0,
continue past errors", combine `forEach(... onError = OnError.Skip)` with
`onUnhandled = LOG_AND_COMPLETE`. `onUnhandled` only catches the
`migrate()`-level escape — it is not a global "ignore all errors" switch.

**Dry-run still writes CSV files to disk.** CSVs are diagnostic
artifacts, not side-effects on external systems. After a dry run the file
at `outputFolder/<filename>` exists and is fully populated. If you treat
the CSV as the migration's deliverable (export archetype), this is
intended; if not, expect a leftover file. A corollary for the export
archetype: an export writes nothing through `guardWrite`, so a dry run of
it always trips the `intercepted 0 writes` warning (§10) — that one is
expected.

## 12. Anti-patterns

- **Do not** wrap `jdbc.execute` in `mutation`. `jdbc.execute` is already
  guarded; double-wrapping double-counts `dryRunSkipped`.
- **Do not** call a repository, `@KafkaPublisher`, `@HttpClient` or any
  other client of your own without `mutation { }` if the migration is ever
  going to be dry-run. Those calls are invisible to the gate and will hit
  production during the rehearsal.
- **Do not** smuggle a write through `jdbc().query(...)` — including
  `insert ... returning`. It throws now; use `executeReturning`.
- **Do not** start a parallel `forEach` inside `transactional { }`. The
  tx-bound `SqlOps` is pinned to the opening thread and throws. Nest the
  other way: `forEach { transactional { ... } }`.
- **Do not** call `producer.close()` from `migrate()` — the producer
  belongs to the Kora graph. Only `topic(...).close()` (auto-called by
  the runner) is safe, and it flushes without closing the producer.
- **Do not** treat `publishAsync` / `sendAsync` as fire-and-forget you can
  ignore: delivery failures do not fail the item, they land in
  `report.asyncFailed` and push the exit code to 1. Check the report.
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

    // onRowError is required for bad rows to be skipped — the default is Fail
    // and would abort the run on the first unparsable line.
    val input = readCsv("input.csv", classpath = true, onRowError = OnError.Skip) {
        Row(it.getValue("id").toLong(), it.getValue("status"))
    }

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
// Imports: ru.tinkoff.kora.http.client.common.annotation.HttpClient,
//          ru.tinkoff.kora.http.client.common.response.HttpClientResponse,
//          ru.tinkoff.kora.http.common.HttpMethod,
//          ru.tinkoff.kora.http.common.annotation.{HttpRoute, Path}
@HttpClient(configPath = "clients.backfill")
interface BackfillClient {
    @HttpRoute(method = HttpMethod.POST, path = "/api/backfill/{id}")
    @Retry("clients.backfill")                 // Kora retry, per call. Kora has only linear backoff.
    fun backfill(@Path("id") id: Long, payload: ByteArray): HttpClientResponse
}

// Kora has no @Body annotation: the request body is the unannotated parameter,
// serialized by an HttpClientRequestMapper. `@Path` takes the placeholder name.
// The response type is HttpClientResponse (code(), headers(), body(), close()) —
// there is no `HttpResponse` type in the client API.

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
directly — it is already dry-run gated and already raises
`HttpStatusException` on a non-2xx, so no `mutation` wrapper and no manual
`check(...)` are needed. What you give up is Kora's `@Retry`.

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

```kotlin
// DefaultMigrationContext.Companion
fun test(
    dryRun: Boolean = false,
    name: String = "test",
    author: String = "test",
    outputFolder: Path? = null,        // null → fresh Files.createTempDirectory("migration-test-")
    defaultProgressEvery: Int = 1000,
    errorThreshold: Long = 0,
    defaultParallel: Int = 1,
): DefaultMigrationContext
```

The test context uses `ForkJoinPool.commonPool()` as its executor, so
`forEach` cancellation there is best-effort (§11.1).

For integration tests against a real Postgres/Kafka, use Testcontainers
the same way as you would for any Kora component. Construct a real
`JdbcConnectionFactory` against the container, instantiate the migration
directly, pass `DefaultMigrationContext.test(dryRun = false, outputFolder
= tmp)` as the receiver.

**Testing the wire-up on a real graph.** A context-level test never proves
the component reached the graph or that the config resolved. To test that,
build the actual `@KoraApp` graph — but first put a `MigrationExit`
component in it, otherwise the runner calls `exitProcess` and kills the test
JVM:

```kotlin
@Component
class TestExit : MigrationExit {
    @Volatile var code: Int? = null
    override fun exit(code: Int) { this.code = code }
}

// swap the config node so one graph can drive many scenarios
val draw = AppGraph.graph()
@Suppress("UNCHECKED_CAST")
val node = draw.findNodeByType(MigrationConfig::class.java) as Node<MigrationConfig>
draw.replaceNode(node) { MigrationConfigValues(run = "MY-001", dryRun = true, outputFolder = tmp.toString()) }
draw.init().release()
```

`example/src/test/kotlin/.../KoraWireUpTest.kt` in this repo is that test:
graph assembly, real parallelism, nested `forEach`, dry-run propagation, the
`intercepted 0 writes` warning, and exit code 2 on an unknown name.

In this repo, container-backed tests are tagged `@Tag("docker")` and
excluded by default — `./gradlew build` needs no Docker. Run them with
`./gradlew test -PwithDocker`.

## 15. Customization

### 15.1 Custom executor

A factory method in a `@Module` — `@Component` annotates classes, not
functions, and a local `@Module` is auto-discovered by Kora:

```kotlin
@Module
interface MigrationExecutorModule {
    @Tag(MigrationExecutor::class)
    fun executor(): Executor = Executors.newCachedThreadPool()
}
```

Keep it unbounded: `forEach(parallel = N)` bounds itself, and a fixed-size
pool silently caps concurrency and can deadlock a nested parallel `forEach`.
Keep it an `ExecutorService` if you want `OnError.Fail` to interrupt
in-flight workers (§11.1). The runner never shuts down an executor it did
not create.

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
class RedisOps(private val ctx: MigrationContext, private val client: RedisClient) : AutoCloseable {
    fun get(k: String): String? = client.get(k)                  // read — not gated

    fun set(k: String, v: String) =
        ctx.guardWrite("redis.set", mapOf("key" to k)) { client.set(k, v) }

    override fun close() { /* flush your own buffers; the client belongs to the graph */ }
}

// memoized per client, registered for close — safe to call inside a forEach body
fun MigrationContext.redis(client: RedisClient): RedisOps =
    shared(client) { RedisOps(this, client) }
```

Pass every write through `ctx.guardWrite` so dry-run, logging, and report
aggregation work out of the box; leave reads ungated. Build the handle with
`ctx.shared(key) { ... }` (§7.9) if it is something a script would naturally
call per item — that gets you both memoization and the automatic close. For
a one-off holder that needs no memoization, `ctx.register(AutoCloseable {
... })` is enough.

## 16. File map (for studying the source)

```
core/src/main/kotlin/io/github/dsudomoin/migration/
  Migration.kt                  Migration base + ScriptPolicy
  MigrationContext.kt            receiver interface
  ResourceRegistry.kt            register(AutoCloseable) contract
  ForEach.kt                     four forEach overloads
  OnError.kt                     Fail/Skip/Handle, Decision
  Progress.kt                    Default/Off/Every/Custom
  Mutation.kt                    mutation(label, args) { }
  csv/CsvRead.kt                  readCsv → Sequence<T>, onRowError policy
  csv/CsvWrite.kt                 openCsv → CsvOutput
  csv/CsvEscape.kt                shared RFC 4180 quoting
  error/ErrorReporter.kt          interface + includeItem<T> reified ext
  error/CsvFileErrorReporter.kt   default impl (errors.csv / errors.log)
  report/MigrationReport.kt       immutable snapshot
  report/ReportBuilder.kt         mutable counters during run
  report/ReportFormatter.kt       text rendering (ascii / unicode)
  internal/
    DefaultMigrationContext.kt    only impl of MigrationContext (+ shared/register)
    ForEachEngine.kt              parallel execution + cancellation
    ProgressTicker.kt             progress tick logic
    ErrorThresholdExceeded.kt     sentinel for real-time threshold abort
    Time.kt                       humanizeDuration for the report

kora/src/main/kotlin/io/github/dsudomoin/migration/kora/
  MigrationModule.kt              @Module: config factory + @Root runner
  MigrationRunner.kt              Lifecycle, owns init() flow and exit codes
  MigrationConfig.kt              @ConfigValueExtractor interface for the
                                  `migration` section + *Values data classes
  MigrationExecutor.kt            @Tag marker for custom Executor
  MigrationExit.kt                fun interface intercepting the exit code
  ops/SqlOps.kt                    jdbc + transactional
  ops/KafkaOps.kt                  kafka + topic
  ops/HttpOps.kt                   http(call) wrapper + HttpStatusException
  ops/CassandraOps.kt              cassandra

example/src/main/kotlin/io/github/dsudomoin/migration/example/
  ExampleApp.kt                   @KoraApp + main() + a plain graph component
  BackfillCustomerTier.kt         CUSTOMER-TIER-001, the reference migration
example/src/main/resources/       application.conf, customers.csv
example/src/test/kotlin/.../KoraWireUpTest.kt   wire-up on a real graph
```

## 17. Pre-flight checklist (before writing migrations in another project)

1. The library is published to a Maven repo (mavenLocal via
   `./gradlew publishToMavenLocal`, GitHub Packages, internal Nexus) or
   wired via `includeBuild`.
2. The consumer build applies the **KSP** plugin and
   `ksp("ru.tinkoff.kora:symbol-processors:<version>")`. No `kapt`, no
   `annotationProcessor` — without KSP no graph is generated.
3. Consumer project has Logback on the runtime classpath (the runner
   attaches a `FileAppender` programmatically; without Logback,
   `migration.log` is silently disabled and the report says so).
4. `@KoraApp` mixes in `MigrationModule` plus whatever
   infrastructure modules the migration touches. No `KafkaProducerModule`
   (it does not exist) and no extra config module.
5. `application.conf` has `migration.run = ${?MIGRATION_RUN}` **and**
   `migration.dryRun = ${?MIGRATION_DRY_RUN}` — the second line is what
   makes the dry-run flag work at all.
6. The migration class is `@Component` and its `name` is unique across
   the graph.
7. Every call into a client the library does not own is wrapped in
   `mutation("label") { ... }`.
8. The consumer has run one **dry-run** end-to-end, inspected
   `logs/<name>/migration.log` + the stdout report, and confirmed the
   report does **not** contain `intercepted 0 writes`.
9. The consumer has decided on `defaults.errorThreshold` — `0` means
   "tolerate any number of skips"; for compliance scripts, set it.
10. If the runner is embedded in a process that must survive the run (or
    in a test), a `MigrationExit` component is in the graph; otherwise the
    runner calls `exitProcess`.
