# Migration DSL — Agent Guide

A guide for an AI coding agent integrating this library into a downstream
Kora-based service. Self-contained: you do not need to read any other file
in this repo to write a correct migration.

Companion documents, same API, different audience: `docs/USER_GUIDE.md` is
the long-form Russian guide for humans, `docs/examples/*` are full
copy-paste archetypes, and `example/` is a runnable consumer application
(§3). They are kept in sync with this file; if you ever find a disagreement,
the source wins — check `core/src/main/kotlin` and `kora/src/main/kotlin`.

Version: `0.1.0`. Kotlin 2.1+, JVM 21+, Kora 1.2.20.

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
| [7](#7-api-reference--core) | API reference — `core` | `migration { }`, `source`, `scoped`, `pages`, `write`, `publish`, `ItemError`, CSV |
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
application graph. A migration is a `MigrationDefinition` component that
returns an **immutable plan**; the runner picks one plan by name and
interprets it.

The split between *declaring* and *executing* is the central idea:

```kotlin
@Component
class BackfillCustomerTier(private val repo: CustomerTierRepository) : MigrationDefinition {
    override val name = "CUSTOMER-TIER-001"                 // constant, no I/O
    override fun plan() = migration(name, author = "team") { /* nodes only */ }
}
```

`plan()` registers nodes — inputs, outputs, a validation block, stages — and
**does not run a single one of the lambdas inside them**. The runner calls
`plan()` exactly once, and only on the migration selected by
`migration.run`; every other definition in the graph stays untouched.

The library owns the cross-cutting concerns:

- stages executed strictly in declaration order, with a failed stage
  stopping the ones after it;
- bounded parallel fan-out per stage;
- cursor pagination of a source (`pages`) with raw-page accounting;
- per-parent scopes (`scoped`) with an **acknowledgement barrier** at each
  boundary: the next parent does not open until the previous one's
  asynchronous effects have settled and its resources are closed;
- item-level error policy (`ItemError.Fail` / `Skip` / `Handle<T>`);
- auto-audit of failed items to `errors.csv` + `errors.log`;
- a dry-run gate on `write` / `publish` and on every op the library itself
  performs;
- structured progress logs, a final stdout report, and a per-migration
  `outputFolder` collecting all artifacts.

Dry-run is **not** transparent to user code. It intercepts `write { }`,
`writeRows { }`, `publish { }` and the library's own ops (`jdbc.execute`,
`kafka.publish`, `http.post`, …). A direct call into your own repository,
`@KafkaPublisher` or `@HttpClient` executes for real during a dry run
unless you wrap it in `write("label") { ... }` (§7.7). The runner prints a
loud WARN — `DRY-RUN processed N item(s) but intercepted 0 writes` — when a
dry run processed items but gated nothing; that line is the only observable
symptom of a forgotten `write { }`. See §10.

Retry is **not** a DSL primitive — see §7.8 for why and where to put it.

A migration is **not** a long-running service, an ETL pipeline, or a
coroutine-friendly Flow. It is a blocking script that runs once, prints a
report, and exits with a status code.

## 2. When to use / when to skip

Use when:
- Bulk reads/updates against existing Postgres / Cassandra / Kafka /
  external HTTP services already wired into a Kora graph.
- One-off scripts that would otherwise reinvent `ExecutorService` +
  `try/catch` + a hand-rolled `do { } while (cursor != null)` pager + ad-hoc
  CSV writers.
- A migration that must not move to the next tenant / partition / strategy
  until everything published for the previous one is acknowledged
  (`scoped`, §7.4).
- A script that needs an audit trail of failed items and a deterministic
  exit code in CI.

Skip when:
- The task is always-on (cron, scheduled, streaming). Build a normal Kora
  component.
- The codebase is coroutine-first and you need structured concurrency. The
  DSL is blocking and runs on JVM threads.
- You do not have Kora. The `core` module is pure Kotlin and could be
  consumed standalone (build a plan, hand it to `PlanInterpreter`), but
  every example in this guide assumes the `kora` bridge module.

## 3. Repository layout

```
core/     pure Kotlin, no Kora. Plan DSL, scopes, effects, cursor pagination,
          CSV, error audit, report, plan interpreter.
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
    ksp("ru.tinkoff.kora:symbol-processors:1.2.20")
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
  the call in `write("label") { ... }` (sync) or `publish("label") { ... }`
  (async, barrier-tracked). This stays the recommended route.

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

Each migration is a `@Component` implementing `MigrationDefinition`. The
runner collects them via `All<MigrationDefinition>` and picks the one whose
`name` matches `migration.run`. Selection and duplicate detection read
`name` only — **no plan is built for the migrations you did not select**,
so a definition whose `plan()` would throw costs nothing while it is not
the one being run. Duplicate names abort with exit code 2.

## 5. Hello migration

A minimal complete script:

```kotlin
@Component
class FixOrderStatuses(
    private val db: JdbcConnectionFactory,
) : MigrationDefinition {

    override val name = "FIX-ORDERS-001"

    override fun plan() = migration(name = name, author = "agent") {

        validate { errors.includeItem<Long> { "orderId=$it" } }

        source(
            parallel = 4,
            onItemError = ItemError.Skip,
            items = {
                jdbc(db).query("select id from orders where status = 'STUCK'") {
                    it.getLong("id")
                }.asSequence()
            },
        ) { id ->
            writeRows("orders.fix", args = mapOf("id" to id)) {
                jdbc(db).execute("update orders set status = 'OK' where id = :id", "id" to id)
            }
        }
    }
}
```

What each piece buys you:

- `migration(name, author) { }` builds the plan. The body registers nodes;
  nothing inside `validate`, `items` or the handler runs here.
- `validate { }` runs once, before the first stage — the right place for
  config assertions and `errors.includeItem<T>` registrations.
- `source(items = { … }) { item -> … }` is one stage: the `items` lambda
  produces the `Sequence`, the trailing lambda handles one element.
- `writeRows` turns the row count of an `UPDATE ... WHERE` into
  `Applied` / `Rejected("no rows matched")` and is skipped under dry-run.

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
@Component class FooMigration : MigrationDefinition        declared by user
    val name  = "FOO-001"  ──────────► runner matches migration.run against this
    fun plan(): MigrationPlan ───────► built once, only for the selected migration
        │
        ├─ input("cfg") { … }            lazy value, resolved once per run
        ├─ validate { … }                runs first, exactly once
        ├─ output("out.csv", "a", "b")   file opened once per run, closed by the engine
        └─ stages, in declaration order
             source(items = { … }) { item -> … }              one scope for the stage
             scoped(parents = { … }, items = { p -> … }) { … } one scope per parent
                       │
                       │  MigrationRunner.init() — Kora Lifecycle
                       v
             +---------+-----------------+
             | RunContext (the run)      |   created by the runner per run
             |  dryRun, log, report,     |
             |  errors, outputFolder,    |
             |  executor, defaults       |
             +---------+-----------------+
                       │ exposed to plan lambdas as
                       │   RunScope   ⊂ InputScope / SourceScope / HandlerScope
                       v
   in items/parents:  resolve(input), pages(…), scopedResource { }
   in the handler:    write { }, writeRows { }, publish { }
   everywhere:        jdbc(db) / cassandra(s) / kafka(p) / topic(p, n) / http(call)
                      openCsv(…) / readCsv(…) / register(…) / shared(key) { … }
                      errors / log / report / outputFolder / dryRun / guardWrite
```

Lifecycle, in the order the runner performs it:

1. Reads HOCON. `migration.run == null` → it logs "runner idle" and returns
   **without calling exit at all**. Then it validates config values and
   migration-name uniqueness, and resolves the definition by `name`.
2. Creates `outputFolder` (`mkdir -p`), attaches a Logback `FileAppender`
   to the root logger so everything in slf4j ends up in `migration.log`.
3. Calls `definition.plan()`. A throw here is a misconfiguration: log +
   exit 2, before any effect.
4. Builds the `RunContext` with a `CsvFileErrorReporter`, a `ReportBuilder`,
   and either a custom `@Tag(MigrationExecutor::class)` `Executor` or a
   fresh **cached** thread pool (daemon threads). Cached, not fixed:
   `source(parallel = N)` bounds itself with its own semaphore, and the
   pool must be able to hand out N threads on demand.
5. Hands the plan to `PlanInterpreter`, which:
   - opens every declared `output(...)` and binds its handle;
   - runs `validate { }`;
   - runs the stages **strictly sequentially**; a stage that throws stops
     the run — the following stages do not start;
   - closes every output in `finally`, whatever happened.
6. In `finally`, closes every `AutoCloseable` registered during the run
   (CSVs, Kafka topic handles, custom holders) **in reverse registration
   order**, then the runner-owned pool (`shutdown()` + 30 s wait; if it does
   not terminate, `shutdownNow()` and the dropped-task count goes into the
   report as a warning).
7. Closes the reporter; under dry-run, warns if nothing was intercepted
   (§10); raises the exit code to 1 if any effect was never confirmed;
   builds the final `MigrationReport`, prints it, detaches the file
   appender, and exits with the code.
8. Exit means `exitProcess(code)` unless a `MigrationExit` component is in
   the graph, in which case the code is handed to it and the JVM survives
   (§14). Exit codes: §9.1.

Anatomy of one **scope unit** — the thing `source` runs once and `scoped`
runs per parent:

```
open the source sequence
  │
  ├─ handle items  (parallel > 1: submitted to the pool, window = Semaphore(parallel))
  │
  ├─ wait for every started worker to finish            ← never abandons a running worker
  │
  ├─ BARRIER: wait for every publish() of this scope to settle
  │      ok        → acked++
  │      failure   → audited + failedEffects++, stage fails with ScopeEffectsFailed
  │      timeout   → abandoned++, stage fails with ScopeCompletionTimeout
  │
  └─ close this scope's scopedResource { } closures, reverse order
```

You never construct the run context yourself in production. For tests:
`RunContext.test(dryRun = ..., outputFolder = ...)` plus
`PlanInterpreter(ctx).execute(definition.plan())` — §14.

## 7. API reference — `core`

### 7.1 `MigrationDefinition` and the `migration { }` builder

```kotlin
interface MigrationDefinition {
    val name: String            // constant; unique within the graph
    fun plan(): MigrationPlan
}

fun migration(
    name: String,
    author: String,
    onUnhandled: ScriptPolicy? = null,   // null delegates to HOCON
    build: MigrationBuilder.() -> Unit,
): MigrationPlan

enum class ScriptPolicy { FAIL_FAST, LOG_AND_COMPLETE }
```

`name` is a property and `plan()` is a **method** on purpose. The runner
needs the name of every definition in the graph to select one and to detect
duplicates; if the plan were a property, its initializer would run for every
component while the graph is being built — every migration in the service
would construct its plan on every startup. As a method it is called once,
on the selected migration only.

`build` executes immediately, but it only registers nodes: `input`'s loader,
`validate`'s check, every `items` / `parents` / handler lambda stays
un-invoked until the interpreter reaches it. Anything you do *directly* in
the builder body (a query, a file read, a `require`) runs at plan-build
time, in the runner's `plan()` call, before `outputFolder` artifacts have
any meaning — put it in `validate { }` or in `items { }` instead.

Rules checked while building the plan; each throws
`IllegalArgumentException`, which the runner reports as exit code 2:

- at least one stage;
- with more than one stage, **every** stage needs a `name` (a single stage
  may stay anonymous);
- stage names unique; `input` names unique; `output` filenames unique;
- `validate { }` declared at most once;
- `parallel > 0` where given.

`onUnhandled` controls what the runner does when something escapes the
stages — including `ScopeEffectsFailed`, `ScopeCompletionTimeout`,
`CursorNotAdvancing` and `ErrorThresholdExceeded`. `FAIL_FAST` → log +
exit 1. `LOG_AND_COMPLETE` → log + exit 0 (report still printed, resources
still closed). It never lets the run continue into the next stage — a
failed stage always stops the run; the policy only picks the exit code.
Item-level errors are handled by `ItemError` (§7.8), not by this.

### 7.2 Builder nodes: `input`, `validate`, `output`

```kotlin
fun <I> input(name: String, load: InputScope.() -> I): Input<I>
fun validate(check: InputScope.() -> Unit)
fun output(filename: String, vararg headers: String): OutputHandle
```

**`input`** is a lazy, run-scoped value. It is resolved by
`SourceScope.resolve(input)` on first use and cached for the rest of the
run; the cache lives in the run, not in the plan, so the same plan object
can be interpreted twice without inheriting the previous run's values.

```kotlin
val strategies = input("strategies") {
    jdbc(db).query("select id, kind from strategies where active") { Strategy.from(it) }
}

source(
    items = { resolve(strategies).asSequence() },
) { strategy -> … }
```

`resolve` exists **only on `SourceScope`** — i.e. inside `items` and
`parents`. A handler cannot resolve an input, and an input's own loader
cannot resolve another input. Get a value into the handler by putting it
into the element type, or by making it the parent of a `scoped` stage.

The cache is a `ConcurrentHashMap`, so an input whose loader returns `null`
is not memoized and its loader runs again on every `resolve`. Load a
nullable value into a wrapper (or a default) if the load is expensive.

**`validate`** runs once, before the first stage and before any effect. Its
failure ends the run with nothing done. Use it for config assertions and
for registering item serializers:

```kotlin
validate {
    require(pageSize > 0) { "pageSize must be > 0" }
    errors.includeItem<Order> { "id=${it.id}" }
}
```

It is a separate node rather than "the first input" because the resolution
order of inputs is decided by the first `resolve`, not by declaration order
— a stage could otherwise perform effects with an unvalidated config.
`validate` sees `InputScope`: everything on `RunScope`, but no `resolve`.

**`output`** declares a CSV artifact of the run:

```kotlin
val processed = output("processed.csv", "id", "old_status", "new_status")
…
) { row ->
    processed.row(row.id, row.old, "OK")
}
```

Headers are fixed at declaration; the file is opened once per run in
`outputFolder` and closed by the engine (twice, idempotently: by the
interpreter in `finally`, then by the resource registry). This is why the
output is declared in the builder rather than opened inside a stage — in a
`scoped` stage, opening per parent would truncate the previous parent's
rows.

`OutputHandle.row(...)` is bound to a file only while the run is in
progress. Calling it at plan-build time or after the run throws
`IllegalStateException` naming the file, instead of silently writing into
someone else's handle. Outputs are written **under dry-run too** — they do
not change a target system.

### 7.3 `source` — the flat stage

```kotlin
fun <T> source(
    name: String? = null,
    onItemError: ItemError<T> = ItemError.Fail,
    completionTimeout: Duration? = null,     // java.time.Duration
    parallel: Int? = null,                   // null → migration.defaults.parallel
    progress: Progress = Progress.Default,
    errorThreshold: Long? = null,            // null → migration.defaults.errorThreshold
    items: SourceScope.() -> Sequence<T>,
    handle: HandlerScope.(T) -> Unit,
)
```

One stage = one scope: the source is opened, items are handled, all workers
are joined, the barrier waits for the scope's `publish` acknowledgements,
then the scope's `scopedResource` closures run.

Parallelism: `parallel <= 1` runs inline on the calling thread and never
touches the executor. With `parallel > 1` items are submitted to the run's
executor behind a `Semaphore(parallel)`, and the permit is taken **before**
`iterator.next()` — so a lazy source is read only as fast as it is consumed
and a million-row stream never piles up in the pool queue.

On failure the engine stops submitting new work but **lets every started
worker finish**; it re-acquires all permits before returning. There is no
`Future.cancel`, no interrupt: a zombie worker would otherwise write into a
closed output and register effects after the barrier. The first exception
is rethrown with the others attached via `addSuppressed`.

Batching is plain Kotlin — there is no `chunk` parameter any more:

```kotlin
source(
    items = { readCsv("ids.csv") { it.getValue("id") }.chunked(500) },
) { batch -> jdbc(db).batch("insert into t(id) values (?)", batch) { ps, id -> ps.setString(1, id) } }
```

Note what that does to the counters: the stage element is the batch, so
`processed` counts batches, not rows, and `ItemError.Skip` skips a whole
batch.

### 7.4 `scoped` — one scope per parent, with a barrier at each boundary

```kotlin
fun <P, T> scoped(
    name: String? = null,
    parents: SourceScope.() -> Sequence<P>,
    completionTimeout: Duration? = null,
    onItemError: ItemError<T> = ItemError.Fail,
    parallel: Int? = null,
    progress: Progress = Progress.Default,
    errorThreshold: Long? = null,
    items: SourceScope.(P) -> Sequence<T>,
    handle: HandlerScope.(T) -> Unit,
)
```

Parents are traversed sequentially, and each parent gets the full scope
unit of §6: source → workers → **barrier** → scope resources closed. The
next parent does not open until the previous parent's `publish` effects
have all settled and its `scopedResource` closures have run. That ordering
is the reason `scoped` exists — a per-tenant, per-partition or
per-strategy migration that must not race ahead of its own delivery
confirmations.

```kotlin
scoped(
    name = "resend-by-strategy",
    parents = { resolve(strategies).asSequence() },
    completionTimeout = Duration.ofMinutes(10),
    parallel = 8,
    items = { strategy ->
        pages(
            first = { repo.firstPage(strategy.id, pageSize) },
            next = { cursor: Instant -> repo.nextPage(strategy.id, cursor, pageSize) },
            nextCursor = { page -> page.last().createdAt },
            continueWhen = { page -> page.size >= pageSize },
        )
    },
) { item ->
    publish("orders.resend", args = mapOf("id" to item.id)) {
        publisher.send(item.id.toString(), item.toEvent())
    }
}
```

Details worth knowing:

- The `parents` sequence is read in its **own** scope, which lives for the
  whole stage; a `scopedResource` registered inside `parents` closes at the
  end of the stage, one registered inside `items` closes at the end of that
  parent.
- `errorThreshold` counts per parent and resets at the boundary (§7.8).
- The progress ticker is created once for the stage and keeps counting
  across parents.
- A failure anywhere in a parent (item, barrier, resource) ends the whole
  stage: later parents are not opened.

### 7.5 `pages` — cursor pagination

```kotlin
fun <C : Any, T> SourceScope.pages(
    name: String? = null,
    first: () -> List<T>,
    next: (C) -> List<T>,
    nextCursor: (List<T>) -> C,
    continueWhen: (List<T>) -> Boolean,
): Sequence<T>
```

A lazy sequence over pages. Every decision is made on the **raw** page —
before any `filter`/`map` you chain onto the result, which the engine
cannot see:

- an empty raw page always ends the source, even if `continueWhen` says
  otherwise;
- a page that your filter empties completely does **not** end the source;
- `continueWhen(page) == false` ends the source without asking for another;
- `nextCursor(page)` must differ from the previous cursor. If it does not,
  the engine throws `CursorNotAdvancing` instead of looping forever. When
  the cursor legitimately repeats (equal timestamps), make it composite:
  `data class Cut(val at: Instant, val id: UUID)` — `C` is any type.
- each raw page adds to `report.rawPages` / `report.rawRows`. That is the
  only place where "rows read" is visible separately from "items handled".

`first` and `next` are separate for a reason beyond cosmetics: the first
page is read with no cursor predicate, the later ones strictly `>` or `<`
the cursor. As a side effect it breaks the type-inference cycle, so no
explicit type arguments are needed at the call site (annotate the `next`
parameter, as in the §7.4 example, if inference still needs a nudge).

The next page is fetched only when the current one is consumed, so
`pages(...)` composed with a parallel stage keeps at most one page plus the
in-flight window in memory.

### 7.6 Scopes and the DSL marker

Three interfaces, all extending `RunScope`, all annotated `@MigrationDsl`:

```kotlin
interface RunScope : ResourceRegistry {
    val dryRun: Boolean
    val log: Logger
    val errors: CsvFileErrorReporter
    val outputFolder: Path
    val report: ReportBuilder
    fun register(c: AutoCloseable)                                   // from ResourceRegistry
    fun <T : AutoCloseable> shared(key: Any, factory: () -> T): T
    fun <R> guardWrite(label: String, args: Map<String, Any?> = emptyMap(),
                       dryRunDefault: R, action: () -> R): R
    fun guardWrite(label: String, args: Map<String, Any?> = emptyMap(), action: () -> Unit)
    fun auditError(e: Throwable, item: Any?)
}

interface InputScope : RunScope                                      // input load, validate

interface SourceScope : RunScope {                                   // items, parents
    fun <I> resolve(input: Input<I>): I
    fun <C : Any, T> pages(…): Sequence<T>
    fun scopedResource(close: () -> Unit)
}

interface HandlerScope : RunScope {                                  // the item handler
    fun write(name: String, args: Map<String, Any?> = emptyMap(),
              action: () -> WriteOutcome): WriteResult
    fun writeRows(name: String, args: Map<String, Any?> = emptyMap(),
                  action: () -> Int): WriteResult
    fun publish(name: String, args: Map<String, Any?> = emptyMap(),
                send: () -> CompletionStage<*>)
}
```

| Member | Meaning |
|---|---|
| `dryRun` | True if the run was started in dry-run mode. Read it for diagnostics; do not branch business logic on it — that is what the gate is for. |
| `log` | SLF4J logger named `io.github.dsudomoin.migration.<name>`. |
| `errors` | Audit sink for failed items (§7.10). Concrete class, not the `ErrorReporter` interface — see §15.2. |
| `outputFolder` | All artifacts go here. `logs/<name>` by default. |
| `report` | Counters, mutated by the engine (§7.13). Read it in `Progress.Custom` and in tests. |
| `register(c)` | Schedule a LIFO close for the whole run. |
| `shared(key) { }` | Run-scoped memoized `AutoCloseable`: created once per key, registered for close. Used by `kafka(...)` / `topic(...)`; use it for your own handles that get called per item. The factory must not call `shared` again. |
| `guardWrite(...)` | The low-level dry-run gate the ops are built on. User code writes `write` / `publish`; you need `guardWrite` only when authoring a custom op (§15.3). |
| `auditError(e, item)` | Manual audit into `errors.csv`. Rarely called directly. |

At run time all three scopes are the **same object**; they are split only
by type at the DSL boundary, which is what makes the operation extensions
(`jdbc`, `transactional`, `cassandra`, `kafka`, `topic`, `http`, `openCsv`,
`readCsv`) usable in all three contexts — they are declared on `RunScope`.

Notice what is *not* on any scope: the executor, `defaultParallel`,
`defaultProgressEvery` and `errorThreshold` live on the run context and are
not reachable from migration code. Parallelism is requested through the
stage argument, never by touching the pool.

**`@DslMarker`.** `MigrationBuilder` and the scopes share the
`@MigrationDsl` marker, so the builder's receiver is invisible inside
`items` / `parents` / `handle`: writing `source { }` or `input(...)` inside
a running stage is a **compile error**, not a plan mutated mid-flight. If
you need a second phase over the results of the first, declare a second
stage — they run in order and the plan stays immutable.

### 7.7 Effects: `write`, `writeRows`, `publish`

```kotlin
sealed interface WriteOutcome {
    data object Applied : WriteOutcome
    data class Rejected(val reason: String) : WriteOutcome
}

sealed interface WriteResult {
    data object Applied : WriteResult
    data class Rejected(val reason: String) : WriteResult
    data object DryRunSkipped : WriteResult
}
```

**`write(name, args) { … : WriteOutcome }`** is the general form. The body
reports its own outcome:

```kotlin
val result = write("customer.tier", args = mapOf("id" to customer.id, "tier" to tier)) {
    repository.updateTier(customer.id, tier)
    WriteOutcome.Applied
}
```

- `Applied` → `report.appliedWrites[name]++`.
- `Rejected(reason)` → `report.rejectedWrites[name]++` and a WARN line.
  Rejected is the *expected negative outcome* of a conditional write (no row
  matched, version drifted), **not** a failure. A failure is an exception
  out of the body, and that goes through `ItemError` like any item error.
- Under dry-run the body is **not called at all**; the result is
  `DryRunSkipped` and `report.dryRunSkipped[name]++`.

**`writeRows(name, args) { … : Int }`** is the same thing for operations
that really return a number of affected rows — `UPDATE ... WHERE`,
`DELETE ... WHERE`. `0` becomes `Rejected("no rows matched")`.

Do **not** use `writeRows` where no row count exists: a Cassandra `INSERT`
(always "applied", no count), a delete+insert pair, a Kafka publish, an
HTTP call. Use `write` and decide the outcome yourself.

**`publish(name, args) { … : CompletionStage<*> }`** registers an
asynchronous effect in the scope's barrier and returns nothing:

```kotlin
publish("orders.resend", args = mapOf("id" to order.id)) {
    publisher.sendAsync(order.id.toString(), order.toEvent())   // CompletionStage<*>
}
```

- The effect is counted **before** `send` is invoked, so there is no window
  in which the record is already in a producer buffer while the barrier
  sees zero.
- A synchronous throw out of `send` (serializer error, full buffer) is an
  ordinary item failure and goes through `ItemError`.
- A *delivery* failure does **not** go through `ItemError`: by the time the
  broker answers, the item is long since counted and `Skip` is physically
  meaningless for it. Each failure is audited to `errors.csv` individually
  and counted in `report.failedEffects`; at the barrier the scope throws
  `ScopeEffectsFailed` with the first failure as cause and the rest (up to
  32 retained) as suppressed.
- Under dry-run `send` is never called; no fake future is created; the
  counter `report.dryRunSkipped[name]++`. This is exactly why the method
  returns `Unit` — there would be nothing honest to return.

**Naming.** `name` must be a **constant per logical operation**; per-item
context belongs in `args`. Counters are aggregated by name, so
`write("resync $id")` produces millions of one-row entries in the report
instead of one line. `args` is only rendered into log lines and into the
effect's failure context.

**Wrapping an already-gated op is fine.** `writeRows("orders.fix") {
jdbc(db).execute(...) }` does not double-count: under dry-run the outer
gate returns before `execute` is reached, and outside dry-run `guardWrite`
is a pass-through. What you gain is applied/rejected accounting; what you
give up is seeing the `jdbc.execute` label in the dry-run breakdown — the
outer name is what gets counted.

### 7.8 `ItemError` — per-item error policy

```kotlin
sealed interface ItemError<in T> {
    enum class Decision { Skip, Fail }
    data object Fail : ItemError<Any?>                             // default
    data object Skip : ItemError<Any?>
    class Handle<T>(val decide: (Throwable, T) -> Decision) : ItemError<T>
}
```

- `Fail` (default): audit the item, count it in `report.failed`, rethrow —
  the stage ends and the run stops before the next stage.
- `Skip`: audit the item, count it in `report.skipped`, continue.
- `Handle<T>`: your classifier decides, and it receives the **typed** item,
  not `Any?`:

```kotlin
source(
    onItemError = ItemError.Handle<Order> { e, order ->
        if (e is HttpStatusException && e.status == 409) ItemError.Decision.Skip
        else ItemError.Decision.Fail
    },
    items = { orders.asSequence() },
) { order -> … }
```

Give `Handle` its type argument explicitly (`ItemError.Handle<Order> { }`)
— the element type is inferred from a later parameter, and the compiler
does not always get there on its own.

**`errorThreshold`** aborts the run when a stage produces more skips than
allowed: `skipped > threshold` throws `ErrorThresholdExceeded`. `0`
disables it. The counter is **per stage** — and in `scoped`, per **parent**
— and is reset at that boundary; the increment-and-compare is atomic, so
exactly one worker observes the breach and the number of skips that
actually happened does not depend on the scheduler. The stage argument
overrides `migration.defaults.errorThreshold` for that stage only.

Two things this does not cover:

- rows dropped by `readCsv(onRowError = ItemError.Skip)` increment
  `report.skipped` but not the *stage* counter (they never reach a handler);
- after a run that ended normally the runner performs one more check with
  the **global** skip count against `migration.defaults.errorThreshold`
  (per-stage overrides do not apply there) and returns 1 if it is exceeded.

There is **no built-in retry primitive at the item level** — deliberately.
Item-level retry re-runs the entire handler for one item, which silently
breaks correctness for non-idempotent steps (a publish that already
happened, an INSERT with an auto PK). Where to put retry instead:

- **Network calls** (HTTP, Kafka, JDBC): Kora's `@Retry` on the typed
  client (`@HttpClient`, `@KafkaPublisher`, repository methods). It
  classifies exceptions and applies backoff at the right scope — the
  individual remote call, not the whole pipeline.
- **Custom transient logic**: a local `try/catch` + sleep loop around the
  specific failing primitive inside the handler.
- **Per-call retries inside `SqlOps` / `HttpOps` / `KafkaOps`**: not
  implemented in v0.1.0.

### 7.9 `Progress`

```kotlin
sealed interface Progress {
    object Default : Progress                                     // every defaults.progressEvery
    object Off : Progress                                         // silent
    data class Every(val n: Int) : Progress                       // n > 0
    data class Custom(val n: Int, val format: (Long, Long?) -> String) : Progress
    companion object { fun custom(n: Int = 1000, format: (Long, Long?) -> String): Custom }
}
```

`format` receives `(done, total)`. **`total` is always `null`** for a stage
— a plan source is a `Sequence` and its size is unknown — so the adaptive
small-cycle branch of `Progress.Default` never engages here and the period
is `migration.defaults.progressEvery` (1000 out of the box). For a short
migration pass `Progress.Every(n)` explicitly, as the `example/` migration
does.

`Every(n)` / `Custom(n, …)` require `n > 0` and throw
`IllegalArgumentException` otherwise — use `Progress.Off` to disable.

The ticker counts items that came back from the handler normally:
successes plus audited skips. A terminal `Fail` aborts before its tick. In
a `scoped` stage there is one ticker for the whole stage, so the count runs
across parents.

### 7.10 `errors` audit

`errors: CsvFileErrorReporter` is thread-safe and is written by every
worker that audits an item. To make `errors.csv` readable, register a
per-type serializer once:

```kotlin
import io.github.dsudomoin.migration.error.includeItem

validate {
    errors.includeItem<Customer> { "id=${it.id}" }
    errors.includeItem<Map<String, String>> { "id=${it["id"]}" }   // raw CSV rows
    errors.includeItem<Long> { "orderId=$it" }
}
```

Lookup goes by exact class, then supertypes and interfaces, so
`includeItem<Map<*, *>>` also covers `LinkedHashMap`. Without a serializer
the item's `toString()` is used and truncated at
`errorReporting.maxItemReprLength`.

`validate { }` is the natural home for those registrations: it runs once.
Registering inside `items { }` also works (the shipped example does it),
but in a `scoped` stage that lambda runs per parent, and every registration
clears the reporter's resolved-serializer cache.

The reporter writes `errors.csv` (one row per audited item:
`timestamp,migration,author,itemRepr,errorClass,errorMessage`, UTC) and
`errors.log` (stack traces if `errorReporting.includeStackTrace = true`).
Both files are created lazily on the first error, so a clean run leaves
neither behind. Besides item errors, the sink receives skipped `readCsv`
rows, every failed `publish` effect, and the exception that escapes the
stages.

### 7.11 CSV

```kotlin
// read: lazy Sequence<T>
fun <T> RunScope.readCsv(path: String, classpath: Boolean = false,
                         onRowError: ItemError<Map<String, String>> = ItemError.Fail,
                         mapper: (Map<String, String>) -> T): Sequence<T>
fun <T> RunScope.readCsv(path: Path,
                         onRowError: ItemError<Map<String, String>> = ItemError.Fail,
                         mapper: (Map<String, String>) -> T): Sequence<T>

// write: ad-hoc handle, registered as AutoCloseable
interface CsvOutput : AutoCloseable {
    fun row(vararg cells: Any?)    // thread-safe; RFC 4180 quoting
    fun flush()                    // explicit; close() flushes too
}
fun RunScope.openCsv(path: Path, vararg headers: String): CsvOutput
fun RunScope.openCsv(filename: String, vararg headers: String): CsvOutput
```

**Prefer `output(...)` in the builder over `openCsv(...)` in a stage** for
anything that lives for the whole run: it is opened once, closed by the
engine, and cannot be opened twice by a `scoped` stage. `openCsv` remains
for files whose name is only known at run time (sharding, per-parent
files) — it registers itself for close and resolves `filename` against
`outputFolder`.

`onRowError` covers one row, both the CSV parse and your `mapper`. The
default is `ItemError.Fail`: **the first malformed row aborts the whole
run.** The resilient behaviour has to be asked for:

```kotlin
readCsv("input.csv", classpath = true, onRowError = ItemError.Skip) { row -> … }
```

Under `Skip` (or `Handle → Skip`) the row is audited with the parsed
`Map<String, String>` as the item, increments `report.skipped`, and never
reaches the handler. The stage's own `onItemError` has no say here: it only
sees rows that mapped successfully. A `Handle` classifier for rows receives
the parsed row (or an empty map if the parse itself failed).

The stream is opened on the first `next()`, not at `readCsv(...)`, and is
registered in the run, so an under-consumed sequence (`take(n)`, early
return, a throw upstream) does not leak the descriptor.

`row()` is buffered; there is no per-row flush (a million syscalls on a
million-row export). The engine flushes and closes at the end of the run.
Call `flush()` yourself only for mid-run checkpoints. Writing is
serialized on a lock: fine at `parallel = 4..16`, a contention point past
~32 workers — shard into several files if you get there.

Under dry-run CSV files are still created and written: they are diagnostic
artifacts, not side effects on an external system.

### 7.12 Resources: `register`, `shared`, `scopedResource`

```kotlin
fun register(c: AutoCloseable)                      // RunScope — closed at end of run, LIFO
fun <T : AutoCloseable> shared(key: Any, factory: () -> T): T   // RunScope — memoized + registered
fun scopedResource(close: () -> Unit)               // SourceScope — closed at the scope boundary
```

- `register` is for anything you open yourself. `openCsv`, `kafka(...)` and
  `topic(...)` already register themselves. A `close()` that throws is not
  propagated: it is logged and lands in `report.warnings`; the exit code
  does not change.
- `shared(key) { }` is `register` plus memoization — the factory runs once
  per key per run. Use it for handles that a script naturally calls per
  item; without it, `topic(producer, "t")` per item would push a million
  objects into the registry.
- `scopedResource { }` is the **narrow** lifetime: it closes at the end of
  the current scope unit. In a `scoped` stage that means "before the next
  parent opens" — the whole point of registering a per-parent cursor or
  connection there instead of with `register`. A closure registered while
  reading `parents` belongs to the stage-wide parent scope and closes at
  the end of the stage. Closures run in reverse registration order, and a
  throwing one is logged into `report.warnings` without stopping the
  others.

### 7.13 The report

`ReportBuilder` accumulates, `MigrationReport` is the immutable snapshot
(`report.build(errorsFile, tracesFile)`):

| Field | Filled by |
|---|---|
| `processed` / `successful` / `skipped` / `failed` | the stage engine, per handled element |
| `appliedWrites` / `rejectedWrites` | `write` / `writeRows`, keyed by name |
| `dryRunSkipped` | every gated call under dry-run, keyed by name/label |
| `acknowledgedPublishes` / `failedEffects` / `abandonedPublishes` / `lateRegistered` | the barrier of each scope |
| `rawPages` / `rawRows` | `pages(...)`, before your filters |
| `warnings` | non-fatal problems: failed `close()`, dropped pool tasks, the dry-run "0 writes" alarm |
| `sourceSkipped` | rows dropped while reading the source (broken CSV); they never reached a stage |

`processed = successful + skipped + failed` holds by construction. Delivery
failures are deliberately kept out of `failed` so the identity survives, and
rows dropped by the source reader go to `sourceSkipped` for the same reason —
they were never `processed`.

The **printed** report is a subset: header, mode, `processed` /
`successful` / `skipped` / `failed`, warnings, the dry-run breakdown, and
the paths of `errors.csv` / `errors.log`. Write, barrier and paging
counters are in the `MigrationReport` object — read them in tests, or log
them yourself from a `Progress.Custom` formatter — but they are not in the
printed block yet.

## 8. API reference — `kora`

Every op is an extension on `RunScope`, so all of them work in `validate`,
in an `input` loader, in `items` / `parents` and in the handler. Their
signatures did not change with the plan API; only the receiver did.

### 8.1 SQL (`jdbc`, `transactional`)

```kotlin
fun RunScope.jdbc(db: JdbcConnectionFactory): SqlOps

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

fun <R> RunScope.transactional(ops: SqlOps, block: SqlOps.() -> R): R
```

`query` and `stream` accept **read statements only**. The first keyword
(after leading comments/parens) must be one of `select`, `with`, `show`,
`explain`, `values`, `table`, `describe`; anything else throws
`IllegalArgumentException` in both real and dry-run mode. The reason is
exactly the dry-run gate: reads are not gated (they must work during a
rehearsal), so `query("insert ... returning id")` would execute **for
real** during a dry run. Use `executeReturning` when a write has to give you
rows back — it goes through the gate like any other write and returns an
empty list under dry-run. Known hole: a data-modifying CTE
(`with ... insert ...`) starts with `with` and passes the check; route those
through `executeReturning` too.

**`stream` cannot be a stage source.** Its `Sequence<T>` is valid only
inside `consume`; after `consume` returns, the `ResultSet` /
`PreparedStatement` / `Connection` are closed (`.use {}`-scoped) and any
iteration on a leaked reference throws `SQLException("ResultSet is
closed")`. An `items = { jdbc(db).stream(…) { it } }` would hand the engine
a dead sequence. Two correct shapes:

```kotlin
// 1. Large source → keyset pagination, not stream. This is the idiomatic form.
source(
    items = {
        pages(
            first = { jdbc(db).query(
                "select id, status from orders where id > 0 order by id limit :n", "n" to 5000
            ) { it.getLong("id") } },
            next = { after: Long -> jdbc(db).query(
                "select id from orders where id > :after order by id limit :n",
                "after" to after, "n" to 5000,
            ) { it.getLong("id") } },
            nextCursor = { page -> page.last() },
            continueWhen = { page -> page.size >= 5000 },
        )
    },
) { id -> … }

// 2. stream stays useful for a fold or a nested read inside a handler
val total: Long = jdbc(db).stream("select id from orders", mapper = { it.getLong(1) }) { rows ->
    rows.count().toLong()
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

`batch` is positional (`?`), not named — `executeBatch` semantics. It pairs
naturally with `Sequence.chunked(n)` in `items`.

`transactional` opens one `Connection` via Kora `db.inTx`, runs the block
with a tx-bound `SqlOps`. Commit on normal return, rollback on throw.
Nested `transactional` calls throw `IllegalStateException` — there is no
smart merge. The check fires under dry-run too, so a rehearsal cannot pass
green on code that aborts for real.

A tx-bound `SqlOps` remembers the thread that opened the transaction.
Using it from another thread — i.e. a parallel stage running **inside** a
`transactional { }` block — throws `IllegalStateException` instead of
corrupting data on a shared `Connection`. Put `transactional { }` inside
the handler, not around the stage:

```kotlin
source(parallel = 8, items = { … }) { order ->
    transactional(jdbc(db)) {
        execute("update orders set status = 'OK' where id = :id", "id" to order.id)
        execute("insert into audit(order_id) values (:id)", "id" to order.id)
    }
}
```

Under dry-run, `transactional` does **not** open a real connection — it
runs the block on the free-mode `SqlOps`. **The block body still executes**:
what dry-run skips is the individual writes inside it, not the block as a
whole. Reads (`query`, `stream`) still work, each in its own mini-tx.

### 8.2 Kafka (`kafka`, `topic`)

```kotlin
fun <K, V> RunScope.kafka(producer: Producer<K, V>): KafkaOps<K, V>
fun <K, V> RunScope.topic(producer: Producer<K, V>, name: String): KafkaTopic<K, V>

class KafkaOps<K, V> : AutoCloseable {
    data class PublishResult(val topic: String, val partition: Int, val offset: Long)

    fun publish(topic: String, key: K, value: V): PublishResult?     // sync, blocks on ack
    fun publishAsync(topic: String, key: K, value: V): CompletableFuture<PublishResult?>
    override fun close()                                              // producer.flush() once
}
class KafkaTopic<K, V> : AutoCloseable {
    fun send(key: K, value: V): KafkaOps.PublishResult?               // sync
    fun sendAsync(key: K, value: V): CompletableFuture<KafkaOps.PublishResult?>
    override fun close()                                              // delegates to KafkaOps.close()
}
```

Both factories memoize through `shared` — `kafka(producer)` per producer,
`topic(producer, name)` per (producer, name) pair — and register the
handle, so calling either one directly inside a handler is safe and does
not grow the registry. The producer object itself is owned by the Kora
graph; the DSL never closes it. On run end the handle calls
`producer.flush()` once (idempotent, CAS-guarded).

**Route asynchronous sends through `publish { }`.** That is what makes them
visible to the barrier: counted before the send, audited individually on
failure, and the stage (or the parent, in `scoped`) does not finish until
the broker has answered for all of them.

```kotlin
val orders = topic(producer, "orders.resync")          // memoized, closed by the runner

source(parallel = 8, items = { … }) { order ->
    publish("orders.resync", args = mapOf("id" to order.id)) {
        orders.sendAsync(order.id.toString(), order.toEvent())      // CompletableFuture
    }
}
```

A raw `sendAsync` **outside** `publish { }` is fire-and-forget as far as
the run is concerned: `KafkaOps` counts the rejection internally and adds a
line to `report.warnings` when the handle flushes at the end of the run
(`kafka.publishAsync: N message(s) were rejected by the broker`), but the
item is already counted successful, nothing reaches `errors.csv`, and the
exit code stays 0. `KafkaOps` deliberately keeps no counters of its own —
otherwise a rejection would be counted twice, once there and once in the
barrier. Use `publish { }`, or `send`/`publish` (synchronous, blocks on the
ack, so a delivery failure is an ordinary item error).

With a typed Kora `@KafkaPublisher`, publish through it and pick the
wrapper by the signature:

```kotlin
@KafkaPublisher("kafka.orders.publisher")
interface OrdersPublisher {
    @KafkaPublisher.Topic("kafka.orders.publisher.resyncTopic")
    fun publishResync(key: String, @Json value: OrderEvent)                       // void → write { }

    @KafkaPublisher.Topic("kafka.orders.publisher.resyncTopic")
    fun publishResyncAsync(key: String, @Json value: OrderEvent): CompletionStage<RecordMetadata>
}

// void signature — synchronous, gate it:
write("orders.resync", args = mapOf("orderId" to order.id)) {
    publisher.publishResync(order.id.toString(), OrderEvent.from(order))
    WriteOutcome.Applied
}

// CompletionStage signature — hand it to the barrier:
publish("orders.resync", args = mapOf("orderId" to order.id)) {
    publisher.publishResyncAsync(order.id.toString(), OrderEvent.from(order))
}
```

`kafka(producer)` / `topic(producer, name)` are for raw `Producer<K, V>`
access (multi-topic ad-hoc publishing or per-script serialization). For a
single typed topic, the `@KafkaPublisher` route is cleaner.

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

fun RunScope.http(call: HttpCall): HttpOps
```

**Every method checks the status code.** Anything outside `200..299` throws
`HttpStatusException(method, path, status)`, so a dead backend answering 500
cannot produce a report full of "successful" items. Classify by code with
`ItemError.Handle`:

```kotlin
onItemError = ItemError.Handle<Order> { e, _ ->
    if (e is HttpStatusException && e.status == 409) ItemError.Decision.Skip
    else ItemError.Decision.Fail
}
```

Under dry-run the gated methods return `200`, not `0` — a rehearsal must
take the same branch as the real run.

`HttpCall` is a function reference; adapt any HTTP client to it. For a
typed Kora `@HttpClient`, prefer calling the client directly and wrapping
the write in `write("label") { … }`. `HttpOps` is the convenience layer for
one-off scripts that do not want to declare a typed client.

### 8.4 Cassandra (`cassandra`)

```kotlin
fun RunScope.cassandra(session: CqlSession): CassandraOps

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

`CassandraOps` has **no** `stream`. It used to return a lazy `Sequence` over
the driver's auto-paging cursor, which looked convenient but gave you no
control: the page size came from Kora config, nothing reached the report, and
the cursor was invisible. Read a large table with `pages(...)` instead — an
explicit cursor over the clustering key, counted as `rawPages` / `rawRows`:

```kotlin
source(
    items = {
        pages(
            first = { cassandra(session).query(FIRST_PAGE, "limit" to pageSize) { it.toOrder() } },
            next = { after -> cassandra(session).query(NEXT_PAGE, "after" to after, "limit" to pageSize) { it.toOrder() } },
            nextCursor = { raw -> raw.last().id },
            continueWhen = { raw -> raw.size >= pageSize },
        )
    },
) { order -> … }
```

For a small, bounded result set `cassandra(session).query(...)` is fine — it
materialises everything into a `List`.

Prepared statements are **not** cached by `CassandraOps` itself — the factory
`cassandra(session)` builds a new instance each call, so a local cache would
always be empty. The DataStax driver 4.x keeps its own per-session
prepare-cache (keyed by CQL string + protocol version): a second
`session.prepare(sameCql)` returns the same `PreparedStatement` from the
driver cache. Named parameters bind via `setExplicit` — works for primitives,
strings, UUID, Instant, plus `List<*>`/`Set<*>` (element class inferred from
the first non-null element; an empty or all-null collection throws, because
the element type cannot be inferred). Cassandra UDTs / tuple / Map / custom
codecs need their own `bb.set(name, value, GenericType)` call written by the
user inside a custom op (§15.3).

`batch` is a sequential `execute` loop — Cassandra has no JDBC-style batch —
and it is gated as one write labelled `cassandra.batch`. Because a Cassandra
`INSERT` reports no row count, wrap Cassandra writes in `write { … }`, never
in `writeRows { … }`.

## 9. Configuration (HOCON)

```hocon
migration {
  run = ${?MIGRATION_RUN}                        # String?; name of the migration. null = idle
  dryRun = ${?MIGRATION_DRY_RUN}                 # Boolean, default false
  outputFolder = ${?MIGRATION_OUTPUT_FOLDER}     # String?; default logs/<name>

  defaults {
    onUnhandled = FAIL_FAST                      # enum, default FAIL_FAST; or LOG_AND_COMPLETE
    errorThreshold = 0                           # Long,  default 0 (>0 enables the per-stage abort)
    progressEvery = 1000                         # Int,   default 1000 (Progress.Default tick)
    parallel = 1                                 # Int,   default 1 — default of source(parallel = ...)
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

That is the complete key set — there is nothing else under `migration`, and
none of it changed with the plan API. Only `run`, `dryRun` and
`outputFolder` are meant to be driven from the environment, and only
through `${?VAR}` substitution (§4).

`migration.defaults.parallel` is **not** a pool size. It is the default
value of the `parallel` argument of `source` / `scoped`; the runner's own
pool is cached and hands out as many threads as a given stage asks for.
`migration.defaults.errorThreshold` and `progressEvery` are likewise
per-stage defaults that a stage argument overrides.

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
Prefer an unbounded/cached pool: a fixed-size one silently caps real
concurrency below the `parallel` a stage asked for, so a stage that reads
as "eight workers" runs two at a time and the run takes four times as long
with nothing in the report to say why.

### 9.1 Exit codes

| Code | When |
|---|---|
| `0` | Success. Also a `LOG_AND_COMPLETE` run that logged a failure. |
| `1` | `FAIL_FAST` on anything that escaped the stages — a stage failure, `ScopeEffectsFailed`, `ScopeCompletionTimeout`, `CursorNotAdvancing`, `ErrorThresholdExceeded`; a post-mortem global skip count above `defaults.errorThreshold`; **or** any unconfirmed effect (`abandonedPublishes + lateRegistered > 0`), which raises 0 → 1 regardless of `onUnhandled`. |
| `2` | Misconfiguration, before any work: unknown `migration.run` name, duplicate migration names in the graph, invalid `migration.*` values, `outputFolder` cannot be created, or `plan()` itself threw. |

Idle (`migration.run` unset) is not an exit code: the runner logs "runner
idle" and returns without exiting at all — the surrounding Kora application
continues its normal lifecycle.

Exiting means `exitProcess(code)` unless a `MigrationExit` component is
present in the graph (§14).

## 10. Dry-run model

Setting `migration.dryRun = true` (via `dryRun = ${?MIGRATION_DRY_RUN}` —
the env var alone does nothing, §4) flips `dryRun` to `true` on every
scope. Each guarded call then:

- skips executing the action,
- returns the declared dry-run value,
- logs `INFO [DRY-RUN] <label> (<args>)`,
- increments `report.dryRunSkipped[label]` by 1.

What is guarded:

| Call | Label | Value returned under dry-run |
|---|---|---|
| `write(name) { … }` | the `name` you passed, verbatim | `WriteResult.DryRunSkipped` |
| `writeRows(name) { … }` | the `name` you passed | `WriteResult.DryRunSkipped` |
| `publish(name) { … }` | the `name` you passed | nothing; `send` is never called and no effect is registered |
| `jdbc(db).execute(...)` | `jdbc.execute` | `0` |
| `jdbc(db).batch(...)` | `jdbc.batch` | `IntArray(0)` |
| `jdbc(db).executeReturning(...)` | `jdbc.executeReturning` | `emptyList()` |
| `cassandra(s).execute(...)` | `cassandra.execute` | `null` |
| `cassandra(s).batch(...)` | `cassandra.batch` | `Unit` |
| `kafka(p).publish(...)` / `topic(...).send(...)` | `kafka.publish` | `null` |
| `kafka(p).publishAsync(...)` / `topic(...).sendAsync(...)` | `kafka.publishAsync` | completed future of `null` |
| `http(call).post/patch/put/delete` | `http.post`, … | `200` |
| `transactional(ops) { ... }` | `jdbc.transactional` | result of the block |
| `guardWrite(label, …, dryRunDefault) { }` | your `label` | your `dryRunDefault` |

`transactional` is the one row that is **not** a skip: the label is counted
and no real `Connection` is opened, but **the block body runs**. Writes
inside it are skipped one by one through their own labels; reads execute.

Nesting a gated op inside `write { }` does not double-count: the outer gate
returns before the inner call happens, and outside dry-run the inner
`guardWrite` is a pass-through (§7.7).

What is **not** guarded:

- `jdbc.query` / `jdbc.stream` / `cassandra.query` — reads pass through
  (which is why the JDBC pair rejects write statements, §8.1).
- `http.get` — read-only.
- `output(...).row(...)` and `openCsv(...).row(...)` — file writes are
  diagnostic artifacts.
- `errors.report(...)` — the audit trail is independent of dry-run.
- **anything you call yourself**: a repository method, a typed
  `@KafkaPublisher` / `@HttpClient`, an SDK client. The library cannot see
  those calls. Under dry-run they execute for real unless wrapped in
  `write("label") { … }` or `publish("label") { … }`.

Because of that last item, the runner emits a WARN — into the log and into
the report's warnings — when a dry run finished with `processed > 0` and an
**empty** `dryRunSkipped` map:

```
DRY-RUN processed 1000 item(s) but intercepted 0 writes. If this migration
writes anything, those writes went through FOR REAL — wrap typed client
calls in write("label") { ... }
```

Read every dry-run report
for that line before trusting the rehearsal. It is the only signal the
library can give for a forgotten `write { }`.

## 11. Known limitations & footguns

This section is for things the DSL **cannot fix** (inherent constraints of
the underlying systems or deliberate design choices) and things that are
**easy to do wrong** (subtle API contracts you have to internalize).

Features that are not gotchas — exit codes and idle mode (§9.1), the
barrier (§7.4), `errorThreshold` semantics (§7.8), the callback scope of
`SqlOps.stream` (§8.1) — are documented in those sections rather than
repeated here.

### 11.1 Known limitations (inherent — cannot be removed in the DSL)

**An async delivery failure cannot be an item error.** By the time the
broker answers, the item has been counted and the handler has returned;
`Skip` for it is physically meaningless. Every such failure is audited and
counted, and the scope fails at the barrier with `ScopeEffectsFailed`.
If you want per-item classification of delivery outcomes, publish
synchronously (`send` / `publish` on `KafkaOps`) inside `write { }` and pay
the latency.

**A timeout does not recall what was already sent.**
`ScopeCompletionTimeout` means the scope stopped waiting, not that the
effects were cancelled: unfinished ones are counted as
`abandonedPublishes`, effects that settle afterwards are counted as
`lateRegistered`, and both raise the exit code to 1. `completionTimeout`
bounds **only** the acknowledgement wait after the source is exhausted —
never the time spent reading the source or sending.

**Aborting a stage does not interrupt running workers.** When an item
fails, the engine stops submitting new work and then waits for every
started worker to return (it re-acquires all semaphore permits). Nothing is
interrupted or cancelled: a worker killed mid-flight would write into an
output that is about to close and could register an effect after the
barrier had already passed. Cost: a stage that fails while eight slow HTTP
calls are in flight takes as long as the slowest of them to unwind.

**No two-phase commit for a Kafka publish inside `transactional`.** Kafka
`send()` enqueues into the producer accumulator; the record leaves for the
broker on `linger.ms` / buffer pressure / a flush, and a synchronous send
blocks until the broker acks it. Either way the send is not tied to the
Postgres tx, which commits or rolls back independently. If the tx rolls
back, the message may still be in Kafka. Treat a publish inside
`transactional` as "best-effort outbox"; for real transactional semantics
write to an outbox table inside the tx and let a dispatcher publish.

**`transactional` does not nest, and is single-threaded.** A nested call
throws `IllegalStateException` (in dry-run too) — there is no smart merge
with an outer tx. If a sub-routine needs to be tx-aware, take `SqlOps` as a
parameter and let the caller pass a tx-bound one. A tx-bound `SqlOps` also
refuses to be used from a thread other than the one that opened the
transaction, because `java.sql.Connection` is not thread-safe. Put
`transactional { }` inside the handler, not around the stage (§8.1).

**`SqlOps.stream` cannot be a stage source.** Its sequence dies with the
`consume` block. Use `pages(...)` for a large source; keep `stream` for a
fold or a nested read (§8.1).

**`WITH ... INSERT` passes the read-only check.** The guard on
`query`/`stream` looks at the first keyword, and a data-modifying CTE starts
with `with`. Such a statement will execute for real during a dry run. Route
it through `executeReturning` instead.

**`readCsv` fails the whole run on a malformed row by default.**
`onRowError` defaults to `ItemError.Fail`; the resilient behaviour the
export and correction archetypes describe requires passing
`onRowError = ItemError.Skip` explicitly (§7.11). The stage's
`onItemError` does not cover parsing or mapping.

**`pages` judges the raw page, not your filtered view.** An empty raw page
always ends the source; a page your filter empties completely does not. A
cursor that does not advance is a `CursorNotAdvancing` failure rather than
an infinite loop — make the cursor composite when the key legitimately
repeats (§7.5).

**No retry primitive at the item level.** Deliberate — item-level retry
re-runs the whole handler, silently breaking correctness for non-idempotent
steps. Use Kora `@Retry` on typed clients or a local loop around the failing
primitive. See §7.8.

**`Progress.Default` cannot adapt to a small run.** Stage sources are
`Sequence`s, so `total` is always `null` and the ticker uses
`defaults.progressEvery` unchanged. A 50-item migration with the default
period logs nothing; pass `Progress.Every(n)`.

**The printed report hides zero-valued effect lines on purpose.** Applied and
rejected writes, barrier counters (`acked` / `failedEffects` / unconfirmed),
`sourceSkipped` and `rawPages` / `rawRows` are rendered only when non-zero, so
a constant row of zeros never trains the eye to skip them. Everything is always
present on the `MigrationReport` object regardless.

### 11.2 Footguns (easy to do wrong)

**A dry run does not protect calls the library cannot see.** Direct
repository / `@KafkaPublisher` / `@HttpClient` / SDK calls execute for real
under `dryRun = true`. Wrap every one of them in `write("label") { … }` (or
`publish`) and check the report for the `intercepted 0 writes` warning
(§10).

**`MIGRATION_DRY_RUN=true` is inert without the HOCON line.** The library
never reads the environment itself; `dryRun = ${?MIGRATION_DRY_RUN}` must be
in `application.conf` or the "rehearsal" is a real run (§4).

**Effect names must be constants.** Per-item context goes in `args`.
Anti-example: `write("publish $id") { … }` creates one entry per item in
`report.dryRunSkipped` / `appliedWrites` instead of one aggregate row.

**`writeRows` is only for statements that return a row count.** A Cassandra
`INSERT`, a delete+insert pair, a Kafka publish and an HTTP call have no
such number; using `writeRows` there either fails to compile or lies about
`Rejected`. Use `write` and return the outcome yourself.

**The plan builder is invisible inside stage lambdas — on purpose.**
`@DslMarker` makes `source` / `scoped` / `input` a compile error inside
`items` / `parents` / `handle`. Declare a second stage instead. (Kotlin
still lets you force it with an explicit receiver label such as
`this@migration.source { }` — do not; it mutates a plan that is already
running.)

**`resolve` is `SourceScope`-only.** Handlers and input loaders cannot
resolve an input (§7.2). Carry the value in the element type or make it a
`scoped` parent; do not stash it in a `var` captured from the builder body
— that is per-plan state in an object designed to be re-executable.

**An `output` handle only works during the run.** `csv.row(...)` at
plan-build time or after the run throws `IllegalStateException`. And do not
name the local `report` (as in `val report = output(...)`): inside the
handler it shadows `RunScope.report`.

**`errorThreshold` is per stage, per parent, and is reset at each
boundary.** Two stages with a threshold of 2 tolerate two skips each, not
two in total. Rows dropped by `readCsv` never touch the stage counter,
though they do count towards the runner's global post-mortem check — which
uses only `migration.defaults.errorThreshold` and ignores per-stage
overrides (§7.8).

**`ErrorThresholdExceeded` obeys `onUnhandled`.** It escapes the stage like
any other failure, so with `onUnhandled = LOG_AND_COMPLETE` a run that blew
through its threshold still exits 0. Keep `FAIL_FAST` if the threshold is
meant to be a hard stop.

**`errors.includeItem<T> { … }` must run inside a scope.** A field
initializer of the definition class has no scope; put registrations in
`validate { }` (once) or at the top of `items { }` (§7.10).

**`scopedResource` and `register` have different lifetimes.** A
per-parent connection registered with `register` stays open until the end
of the run — that is exactly what `scoped` was built to avoid. Use
`scopedResource { }` inside `items`.

**Do not submit your own work to the run's executor.** It is not on
`RunScope` any more, and for good reason: work that outlives the stages
meets the runner's `finally`, which closes resources and shuts down its own
pool (30 s, then `shutdownNow()`; dropped tasks are only a warning).

**`LOG_AND_COMPLETE` does not silence item-level `Fail`.** An item error
with `ItemError.Fail` ends its stage and the run; `onUnhandled` only picks
the exit code afterwards. For "log everything, continue, exit 0", combine
`onItemError = ItemError.Skip` with `onUnhandled = LOG_AND_COMPLETE`.

**Dry-run still writes CSV files to disk.** After a dry run the file at
`outputFolder/<filename>` exists and is fully populated. A corollary for the
export archetype: an export gates nothing, so a dry run of it always trips
the `intercepted 0 writes` warning (§10) — that one is expected.

## 12. Anti-patterns

- **Do not** build a plan that does work: no queries, no file reads, no
  `require` on live data directly in the `migration { }` body. Those belong
  in `validate { }` or in `items { }`; the builder only registers nodes.
- **Do not** call a repository, `@KafkaPublisher`, `@HttpClient` or any
  other client of your own without `write { }` / `publish { }` if the
  migration is ever going to be dry-run. Those calls are invisible to the
  gate and will hit production during the rehearsal.
- **Do not** use `writeRows` for something without a row count (Cassandra
  insert, delete+insert, publish, HTTP). Use `write` and decide the
  `WriteOutcome` yourself.
- **Do not** treat `Rejected` as a failure or an exception as a rejection.
  `Rejected` is an expected negative outcome that keeps the item successful;
  a thrown exception is what goes through `ItemError`.
- **Do not** fire `sendAsync` / `publishAsync` outside `publish { }`. It is
  fire-and-forget: the barrier does not see it, `errors.csv` does not get
  it, and the exit code stays 0 (§8.2).
- **Do not** smuggle a write through `jdbc().query(...)` — including
  `insert ... returning`. It throws; use `executeReturning`.
- **Do not** try to use `jdbc().stream` as a stage source. Its sequence is
  dead outside `consume`; use `pages(...)`.
- **Do not** start a parallel stage inside `transactional { }`. The
  tx-bound `SqlOps` is pinned to the opening thread and throws. Nest the
  other way: handler → `transactional { }`.
- **Do not** hand-roll `do { … } while (cursor != null)` inside `items`.
  `pages(...)` gives you the same loop plus the stuck-cursor guard and the
  raw-row accounting.
- **Do not** call `producer.close()` from a migration — the producer
  belongs to the Kora graph. The `topic(...)` handle's `close()` (called by
  the runner) flushes without closing the producer.
- **Do not** instantiate `RunContext` directly in production code. Use the
  runner; for tests use `RunContext.test()`.
- **Do not** rely on the DSL for retry, and do not simulate it with an
  ad-hoc `try/catch` loop around the whole handler — use Kora `@Retry` on
  the typed client method, which classifies exceptions and applies backoff
  at the right scope.
- **Do not** put per-row `flush()` on a CSV write in a hot loop — the
  close-time flush is the design. Call `flush()` only for checkpoints.
- **Do not** mutate `report` fields directly. Use the increment methods on
  `ReportBuilder` if you need ad-hoc counters in a custom op.

## 13. Patterns / archetypes

### 13.1 Export — DB to CSV

```kotlin
@Component
class ExportOrders(private val db: JdbcConnectionFactory) : MigrationDefinition {

    override val name = "EXPORT-ORDERS-001"

    override fun plan() = migration(name = name, author = "agent") {
        val out = output("orders.csv", "id", "customer_id", "total")

        source(
            progress = Progress.Every(10_000),
            items = {
                pages(
                    first = { page(after = 0L) },
                    next = { after: Long -> page(after) },
                    nextCursor = { rows -> rows.last().id },
                    continueWhen = { rows -> rows.size >= PAGE },
                )
            },
        ) { order -> out.row(order.id, order.customerId, order.total) }
    }

    private fun SourceScope.page(after: Long): List<Order> = jdbc(db).query(
        "select id, customer_id, total from orders where id > :after order by id limit :n",
        "after" to after, "n" to PAGE,
    ) { Order(it.getLong(1), it.getString(2), it.getBigDecimal(3)) }

    private companion object { const val PAGE = 5000 }
}
```

An export gates no writes, so its dry run always trips the
`intercepted 0 writes` warning — expected here (§11.2). `rawRows` in the
report tells you how many rows were read, which is the number to compare
against the CSV's line count.

### 13.2 Correction — CSV to DB UPDATE

```kotlin
data class Fix(val id: Long, val newStatus: String)

override fun plan() = migration(name = name, author = "agent") {
    validate {
        errors.includeItem<Fix> { "id=${it.id}, target=${it.newStatus}" }
        errors.includeItem<Map<String, String>> { "id=${it["id"]}" }   // unparsable rows
    }

    val rejected = output("rejected.csv", "id", "reason")

    source(
        parallel = 4,
        onItemError = ItemError.Skip,
        // onRowError is required for bad rows to be skipped — the default is Fail
        // and would abort the run on the first unparsable line.
        items = {
            readCsv("input.csv", classpath = true, onRowError = ItemError.Skip) {
                Fix(it.getValue("id").toLong(), it.getValue("status"))
            }
        },
    ) { fix ->
        val result = writeRows("orders.status", args = mapOf("id" to fix.id)) {
            jdbc(db).execute(
                "update orders set status = :s where id = :id",
                "s" to fix.newStatus, "id" to fix.id,
            )
        }
        if (result is WriteResult.Rejected) rejected.row(fix.id, result.reason)
    }
}
```

`writeRows` turns "no row matched" into a `Rejected` outcome instead of a
silent success, and the run keeps a CSV of everything that did not apply.

### 13.3 Resend — per-strategy scope with a delivery barrier

The archetype `scoped` exists for: work partitioned by a parent, where the
next partition must not start until everything published for the previous
one has been acknowledged.

```kotlin
@Component
class ResendOrders(
    private val db: JdbcConnectionFactory,
    private val publisher: OrdersPublisher,     // @KafkaPublisher, CompletionStage signature
) : MigrationDefinition {

    override val name = "RESEND-001"

    override fun plan() = migration(name = name, author = "agent") {
        val strategies = input("strategies") {
            jdbc(db).query("select id from strategies where active") { it.getLong("id") }
        }

        validate { errors.includeItem<Order> { "orderId=${it.id}" } }

        scoped(
            parents = { resolve(strategies).asSequence() },
            completionTimeout = Duration.ofMinutes(10),
            parallel = 8,
            onItemError = ItemError.Skip,
            errorThreshold = 100,                       // per strategy, reset at each boundary
            items = { strategyId ->
                pages(
                    name = "orders-of-$strategyId",
                    first = { ordersAfter(strategyId, null) },
                    next = { cursor: Instant -> ordersAfter(strategyId, cursor) },
                    nextCursor = { page -> page.last().createdAt },
                    continueWhen = { page -> page.size >= PAGE },
                ).filter { it.dirty }
            },
        ) { order ->
            publish("orders.resend", args = mapOf("orderId" to order.id)) {
                publisher.publishResyncAsync(order.id.toString(), OrderEvent.from(order))
            }
        }
    }
}
```

What the engine guarantees here: strategy *N+1* is not read, let alone
published, until every `publish` of strategy *N* has been acknowledged; a
broker failure in strategy *N* fails the stage with `ScopeEffectsFailed`
and strategy *N+1* never opens; ten minutes without acknowledgements ends
the run with `ScopeCompletionTimeout` and the unfinished effects are
reported as `abandoned` instead of vanishing.

Retry belongs on the publisher method (Kora `@Retry`), not around the
handler — item-level retry would also re-run the enrichment.

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
// The response type is HttpClientResponse (code(), headers(), body(), close()).

// HOCON: resilient.retry.clients.backfill {
//   delay = "1s", attempts = 4, delayStep = "1s"   # 4 retries → 5 calls total
// }

data class Row(val id: Long, val payload: String)

override fun plan() = migration(name = name, author = "agent") {
    source(
        parallel = 4,
        onItemError = ItemError.Handle<Row> { e, _ ->
            if (e is HttpStatusException && e.status == 409) ItemError.Decision.Skip
            else ItemError.Decision.Fail
        },
        items = {
            jdbc(db).query("select id, payload from outbox") {
                Row(it.getLong(1), it.getString(2))
            }.asSequence()
        },
    ) { row ->
        write("backfill.post", args = mapOf("id" to row.id)) {
            val code = client.backfill(row.id, row.payload.toByteArray()).use { it.code() }
            if (code in 200..299) WriteOutcome.Applied
            else WriteOutcome.Rejected("HTTP $code")
        }
    }
}
```

For ad-hoc HTTP without a typed client, `http(httpCall).post` is already
dry-run gated and already raises `HttpStatusException` on a non-2xx, so it
needs no `write` wrapper — what you give up is Kora's `@Retry`.

### 13.5 Two-source comparison — diff to CSV

```kotlin
override fun plan() = migration(name = name, author = "agent") {
    val diff = output("diff.csv", "id", "pg_status", "cass_status")

    val pgRows = input("pg-snapshot") {
        jdbc(db).query("select id, status from orders") {
            it.getLong(1) to it.getString(2)
        }.toMap()
    }

    source(
        items = {
            val pg = resolve(pgRows)
            cassandra(session)
                .stream("select id, status from orders") { it.getLong("id") to it.getString("status") }
                .map { (id, cass) -> Triple(id, pg[id], cass) }
        },
    ) { (id, pg, cass) ->
        if (pg != cass) diff.row(id, pg ?: "MISSING", cass)
    }
}
```

Batching, if the comparison is cheaper per batch (an `IN (:ids)` lookup on
the other side), is a plain `.chunked(500)` on the source sequence — there
is no `chunk` parameter. Remember that the stage element then becomes the
batch, and so do `processed` and `Skip`.

### 13.6 Multi-stage: prepare, then act

Stages are the way to express "do all of A, then all of B". They run in
declaration order, each with its own scope and barrier, and a failure in
the first prevents the second from starting. Once you declare more than one
stage, **every stage needs a name**:

```kotlin
override fun plan() = migration(name = name, author = "agent") {
    val staged = output("staged.csv", "id")

    source(name = "stage-rows", items = { … }) { row ->
        writeRows("orders.stage", args = mapOf("id" to row.id)) {
            jdbc(db).execute("update orders set staged = true where id = :id", "id" to row.id)
        }
        staged.row(row.id)                       // artifact for a human, not for stage 2
    }

    source(
        name = "publish-staged",
        items = { jdbc(db).query("select id from orders where staged") { it.getLong(1) }.asSequence() },
    ) { id ->
        publish("orders.staged", args = mapOf("id" to id)) { publisher.sendAsync(id.toString(), …) }
    }
}
```

The handoff between stages goes through the source of truth (or through a
`shared` in-memory holder), **not** through the CSV: an `output` is
buffered and the interpreter closes it only at the very end of the run, so
a second stage reading that file would see a truncated one. `staged.csv`
here is a diagnostic artifact for whoever reviews the run.

## 14. Testing migrations

Three levels, cheapest first.

**Plan shape — no context at all.** `plan()` performs no I/O, so building
it in a test is free and catches structural mistakes (missing stage names,
duplicate inputs, a stage accidentally dropped):

```kotlin
@Test fun `plan declares both stages`() {
    val plan = FixOrderStatuses(db = mockk()).plan()
    assertThat(plan.stages.map { it.name }).containsExactly("stage-rows", "publish-staged")
    assertThat(plan.outputs.map { it.filename }).containsExactly("staged.csv")
}
```

**Execution — a test run context plus the interpreter.**

```kotlin
class FixOrderStatusesTest {
    @Test fun `dry run does not touch the db`(@TempDir tmp: Path) {
        val ctx = RunContext.test(dryRun = true, outputFolder = tmp)
        val migration = FixOrderStatuses(mockJdbc(/* fixtures */))

        PlanInterpreter(ctx).execute(migration.plan())
        ctx.closeRegistered()

        val report = ctx.report.build()
        assertThat(report.dryRunSkipped).containsKey("orders.fix")
        assertThat(report.failed).isZero()
    }
}
```

```kotlin
// RunContext.Companion  (io.github.dsudomoin.migration.internal)
fun test(
    dryRun: Boolean = false,
    name: String = "test",
    author: String = "test",
    outputFolder: Path? = null,        // null → fresh Files.createTempDirectory("migration-test-")
    defaultProgressEvery: Int = 1000,
    errorThreshold: Long = 0,
    defaultParallel: Int = 1,
): RunContext

// PlanInterpreter(ctx, progressSink = { line -> … }) — the sink is injectable, so a test
// can assert on progress lines without a log appender.
```

Call `ctx.closeRegistered()` when the assertions depend on files being
flushed — in production the runner does it. The test context uses
`ForkJoinPool.commonPool()` as its executor.

For integration tests against a real Postgres / Cassandra / Kafka, use
Testcontainers as you would for any Kora component: build a real
`JdbcConnectionFactory` / `CqlSession` against the container, instantiate
the definition directly and interpret its plan with
`RunContext.test(outputFolder = tmp)`.
`kora/src/test/kotlin/.../pilot/ComparisonPilotTest.kt` is exactly that
shape.

**Wire-up on a real graph.** A context-level test never proves the
component reached the graph or that the config resolved. To test that,
build the actual `@KoraApp` graph — but first put a `MigrationExit`
component in it, otherwise the runner calls `exitProcess` and kills the
test JVM:

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
graph assembly, real parallelism (a `CyclicBarrier(4)` that only completes
if four workers really run at once), two parallel stages in a row without a
stall, dry-run propagation, the `intercepted 0 writes` warning, and exit
code 2 on an unknown name.

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

Keep it unbounded: a stage bounds itself with `parallel`, so a fixed-size
pool only caps the concurrency the stage asked for — silently, since the
report has no field for "how many workers actually ran". The runner never
shuts down an executor it did not create.

### 15.2 Custom `ErrorReporter`

`RunScope.errors` is typed as the concrete `CsvFileErrorReporter` (open
class), **not** the `ErrorReporter` interface. The shipped
`MigrationRunner` always instantiates `CsvFileErrorReporter` — there is no
`@Tag`-based replacement hook out of the box. This is intentional: keep the
API honest about what the runner actually does. The `ErrorReporter`
interface is still exported for fork-and-customize use cases and for the
`includeItem<T>` extension; the concrete class is `open` and `report(...)`
is overridable, so you can subclass without forking the file:

```kotlin
class JsonLinesReporter(
    name: String, author: String,
    errorsFile: Path, tracesFile: Path,
    private val jsonOut: BufferedWriter,
) : CsvFileErrorReporter(name, author, errorsFile, tracesFile) {
    override fun report(e: Throwable, item: Any?) {
        // write a JSON line per error; ignore the inherited CSV path.
    }
}
```

To wire it in you still bypass the shipped runner: write your own
`Lifecycle` component that calls `RunContext.internalCreate(...)` with your
subclass and hands the plan to `PlanInterpreter`. There is no pluggable
factory bean.

### 15.3 Custom op extension

```kotlin
class RedisOps(private val ctx: RunScope, private val client: RedisClient) : AutoCloseable {
    fun get(k: String): String? = client.get(k)                  // read — not gated

    fun set(k: String, v: String) =
        ctx.guardWrite("redis.set", mapOf("key" to k)) { client.set(k, v) }

    override fun close() { /* flush your own buffers; the client belongs to the graph */ }
}

// memoized per client, registered for close — safe to call per item
fun RunScope.redis(client: RedisClient): RedisOps =
    shared(client) { RedisOps(this, client) }
```

Declare the extension on `RunScope` so it is usable in `validate`, in
`items` and in the handler alike. Pass every write through `guardWrite` so
dry-run, logging and report aggregation work out of the box; leave reads
ungated. Build the handle with `shared(key) { … }` (§7.12) if a script would
naturally call it per item — that gets you memoization plus the automatic
close. For a one-off holder, `register(AutoCloseable { … })` is enough; for
something that must die at a `scoped` boundary, `scopedResource { … }`.

If the op is asynchronous, return the `CompletionStage` and let the caller
hand it to `publish { }` — do not swallow it, or the barrier will not know
about the effect.

## 16. File map (for studying the source)

```
core/src/main/kotlin/io/github/dsudomoin/migration/
  MigrationDefinition.kt         MigrationDefinition + ScriptPolicy
  Plan.kt                        Input, Stage.Flat/Scoped, OutputHandle, MigrationPlan,
                                 MigrationBuilder (input/output/validate/source/scoped), migration()
  Scopes.kt                      @MigrationDsl marker; RunScope, InputScope, SourceScope, HandlerScope
  Write.kt                       WriteOutcome, WriteResult
  Effects.kt                     EffectRef, EffectFailure, ScopeEffectsFailed, ScopeCompletionTimeout
  Cursor.kt                      CursorNotAdvancing
  ItemError.kt                   Fail / Skip / Handle<T> + Decision
  Progress.kt                    Default / Off / Every / Custom
  ResourceRegistry.kt            register(AutoCloseable) contract
  csv/CsvRead.kt                 readCsv → Sequence<T>, per-row ItemError policy
  csv/CsvWrite.kt                openCsv → CsvOutput
  csv/CsvEscape.kt               shared RFC 4180 quoting
  error/ErrorReporter.kt         interface + includeItem<T> reified ext
  error/CsvFileErrorReporter.kt  default impl (errors.csv / errors.log)
  report/MigrationReport.kt      immutable snapshot
  report/ReportBuilder.kt        mutable counters during the run
  report/ReportFormatter.kt      text rendering (ascii / unicode)
  internal/
    RunContext.kt                the only RunScope impl (+ shared/register/guardWrite,
                                 test() and internalCreate() factories)
    PlanInterpreter.kt           stage execution, scope units, barrier ordering, StageScope
    CompletionTracker.kt         effect accounting + sealAndAwait barrier
    PagedSequence.kt             the generator behind SourceScope.pages
    ProgressTicker.kt            progress tick logic
    ErrorThresholdExceeded.kt    sentinel for the per-stage threshold abort
    Time.kt                      humanizeDuration for the report

kora/src/main/kotlin/io/github/dsudomoin/migration/kora/
  MigrationModule.kt             @Module: config factory + @Root runner, All<MigrationDefinition>
  MigrationRunner.kt             Lifecycle, owns init() flow and exit codes
  MigrationConfig.kt             @ConfigValueExtractor interface for the `migration`
                                 section + *Values data classes
  MigrationExecutor.kt           @Tag marker for a custom Executor
  MigrationExit.kt               fun interface intercepting the exit code
  ops/SqlOps.kt                  jdbc + transactional
  ops/KafkaOps.kt                kafka + topic
  ops/HttpOps.kt                 http(call) wrapper + HttpStatusException
  ops/CassandraOps.kt            cassandra

example/src/main/kotlin/io/github/dsudomoin/migration/example/
  ExampleApp.kt                  @KoraApp + main() + a plain graph component
  BackfillCustomerTier.kt        CUSTOMER-TIER-001, the reference migration
example/src/main/resources/      application.conf, customers.csv
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
4. `@KoraApp` mixes in `MigrationModule` plus whatever infrastructure
   modules the migration touches. No `KafkaProducerModule` (it does not
   exist) and no extra config module.
5. `application.conf` has `migration.run = ${?MIGRATION_RUN}` **and**
   `migration.dryRun = ${?MIGRATION_DRY_RUN}` — the second line is what
   makes the dry-run flag work at all.
6. The migration class is `@Component`, implements `MigrationDefinition`,
   its `name` is a constant unique across the graph, and `plan()` performs
   no I/O.
7. Every call into a client the library does not own is wrapped in
   `write("label") { … }`; every asynchronous send goes through
   `publish("label") { … }` so the barrier can wait for it.
8. Stages that must not interleave across a partition use `scoped`, with a
   `completionTimeout` that is generous compared to the broker's ack
   latency.
9. Large sources go through `pages(...)`, with a cursor that provably
   advances (composite if the key can repeat).
10. The consumer has run one **dry-run** end-to-end, inspected
    `logs/<name>/migration.log` + the stdout report, and confirmed the
    report does **not** contain `intercepted 0 writes`.
11. The consumer has decided on `defaults.errorThreshold` — `0` means
    "tolerate any number of skips" — and knows it is counted per stage
    (per parent in `scoped`).
12. If the runner is embedded in a process that must survive the run (or in
    a test), a `MigrationExit` component is in the graph; otherwise the
    runner calls `exitProcess`.
