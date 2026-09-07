package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.csv.CsvOutput
import java.nio.file.InvalidPathException
import java.nio.file.Path
import java.time.Duration

/**
 * Ленивая ссылка на значение, общее для прогона. Идентичность — сам объект: значение живёт в
 * контексте прогона, а не здесь, поэтому один и тот же план можно исполнить повторно, не получив
 * кэш предыдущего запуска.
 */
class Input<I> internal constructor(val name: String, internal val load: InputScope.() -> I)

/** Узел плана: именованная стадия со своим источником и обработчиком. */
sealed class Stage<P, T> {
    abstract val name: String?

    /** Плоская стадия: один scope на всю стадию. */
    class Flat<T> internal constructor(
        override val name: String?,
        internal val items: SourceScope.() -> Sequence<T>,
        internal val handle: HandlerScope.(T) -> Unit,
        internal val onItemError: ItemError<T>,
        internal val completionTimeout: Duration?,
        internal val parallel: Int?,
        internal val progress: Progress,
        internal val errorThreshold: Long?,
    ) : Stage<Unit, T>()

    /**
     * Стадия с границей на каждого родителя: родители идут последовательно, и переход к
     * следующему разрешён только после подтверждения всех его асинхронных эффектов и закрытия
     * его ресурсов.
     */
    class Scoped<P, T> internal constructor(
        override val name: String?,
        internal val parents: SourceScope.() -> Sequence<P>,
        internal val items: SourceScope.(P) -> Sequence<T>,
        internal val handle: HandlerScope.(T) -> Unit,
        internal val onItemError: ItemError<T>,
        internal val completionTimeout: Duration?,
        internal val parallel: Int?,
        internal val progress: Progress,
        internal val errorThreshold: Long?,
    ) : Stage<P, T>()
}

/**
 * Неизменяемое описание миграции. Построение плана не выполняет ни одного объявленного в нём
 * действия: [Input.load], источники и обработчики остаются невызванными лямбдами до тех пор, пока
 * runner не выберет эту миграцию.
 */
/**
 * Декларация CSV-выхода, живущего весь прогон.
 *
 * Заголовки задаются один раз при объявлении, а файл открывается один раз на прогон — именно поэтому
 * выход объявляется в билдере, а не внутри стадии: в `scoped`-стадии открытие на каждого родителя
 * затирало бы строки предыдущего.
 *
 * Привязка к файлу существует только во время прогона и снимается в `finally`: вне прогона
 * [row] бросает, а не пишет в чужой файл.
 */
class OutputHandle internal constructor(
    val filename: String,
    internal val headers: List<String>,
) {
    @Volatile
    private var bound: CsvOutput? = null

    internal fun bind(output: CsvOutput) {
        bound = output
    }

    internal fun unbind() {
        bound = null
    }

    /** Записать строку. Работает и под dry-run: выход не меняет целевую систему. */
    fun row(vararg cells: Any?) {
        val target = bound
            ?: error("output '$filename' is not bound to a run: it can only be written from a running migration")
        target.row(*cells)
    }
}

/**
 * Неизменяемое описание миграции: что и в каком порядке делать.
 *
 * План не хранит ничего от прогона: счётчики, кэш input'ов и трекеры живут в контексте,
 * который создаёт runner. Единственное исключение — [OutputHandle]: на время прогона он связан с
 * файлом и отвязывается в `finally`.
 *
 * Создаётся только через [migration]; конструктор закрыт.
 */
class MigrationPlan internal constructor(
    val name: String,
    val author: String,
    val onUnhandled: ScriptPolicy?,
    val stages: List<Stage<*, *>>,
    val inputs: List<Input<*>>,
    val outputs: List<OutputHandle>,
    internal val validation: (InputScope.() -> Unit)?,
)

/**
 * Имена, которые движок открывает в `outputFolder` сам.
 *
 * Пользовательский `output` с таким именем писал бы в тот же файл параллельно с аудитором или
 * файловым логгером, и оба открывают его с `TRUNCATE_EXISTING` — то есть аудит прогона молча
 * уничтожался бы ровно там, где он нужнее всего.
 */
private val RESERVED_ARTIFACTS = setOf("errors.csv", "errors.log", "migration.log")

/** Построитель плана. Экземпляр живёт только внутри вызова [migration]. */
@MigrationDsl
class MigrationBuilder internal constructor() {

    private val stages = mutableListOf<Stage<*, *>>()
    private val inputs = mutableListOf<Input<*>>()
    private val outputs = mutableListOf<OutputHandle>()
    private var validation: (InputScope.() -> Unit)? = null

    fun <I> input(name: String, load: InputScope.() -> I): Input<I> {
        require(inputs.none { it.name == name }) { "duplicate input name '$name'" }
        return Input(name, load).also { inputs += it }
    }

    /**
     * CSV-выход прогона: отчёты, экспорты, диагностика. Файл открывается в `outputFolder`
     * один раз за прогон и закрывается движком.
     */
    fun output(filename: String, vararg headers: String): OutputHandle {
        require(outputs.none { it.filename == filename }) { "duplicate output '$filename'" }
        val normalized = try {
            Path.of(filename).normalize()
        } catch (e: InvalidPathException) {
            throw IllegalArgumentException("output '$filename' is not a valid path", e)
        }
        require(!normalized.isAbsolute && !normalized.startsWith("..")) {
            "output '$filename' must stay inside the migration outputFolder"
        }
        require(normalized.toString() !in RESERVED_ARTIFACTS) {
            "output '$filename' uses a reserved engine artifact name: $RESERVED_ARTIFACTS"
        }
        return OutputHandle(filename, headers.toList()).also { outputs += it }
    }

    /**
     * Проверка конфига выбранного прогона. Выполняется один раз, до первой стадии и до любого
     * эффекта; её ошибка завершает прогон.
     *
     * Отдельный узел, а не «первый input»: порядок разрешения input'ов задаёт первый `resolve`,
     * а не порядок объявления, и стадия могла бы выполнить эффекты с непроверенным конфигом.
     */
    fun validate(check: InputScope.() -> Unit) {
        require(validation == null) { "validate { } is declared more than once" }
        validation = check
    }

    fun <T> source(
        name: String? = null,
        onItemError: ItemError<T> = ItemError.Fail,
        completionTimeout: Duration? = null,
        parallel: Int? = null,
        progress: Progress = Progress.Default,
        errorThreshold: Long? = null,
        items: SourceScope.() -> Sequence<T>,
        handle: HandlerScope.(T) -> Unit,
    ) {
        require(parallel == null || parallel > 0) { "parallel must be > 0, got $parallel" }
        stages += Stage.Flat(name, items, handle, onItemError, completionTimeout, parallel, progress, errorThreshold)
    }

    /**
     * Стадия, разбитая на scope'ы по родителям.
     *
     * Граница родителя — это barrier: все его `publish` должны подтвердиться, а ресурсы —
     * закрыться, прежде чем откроется следующий родитель. [completionTimeout] ограничивает
     * именно ожидание подтверждений после исчерпания источника родителя, а не время его чтения
     * и отправки.
     */
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
    ) {
        require(parallel == null || parallel > 0) { "parallel must be > 0, got $parallel" }
        stages += Stage.Scoped(
            name, parents, items, handle, onItemError, completionTimeout, parallel, progress, errorThreshold,
        )
    }

    /**
     * Стадия, объявляющая свою зависимость от [input] в сигнатуре узла: значение приезжает
     * параметром, а не достаётся вызовом `resolve` из тела.
     *
     * Тонкая надстройка над основной перегрузкой — план и интерпретатор о ней не знают.
     */
    fun <I, T> source(
        input: Input<I>,
        name: String? = null,
        onItemError: ItemError<T> = ItemError.Fail,
        completionTimeout: Duration? = null,
        parallel: Int? = null,
        progress: Progress = Progress.Default,
        errorThreshold: Long? = null,
        items: SourceScope.(I) -> Sequence<T>,
        handle: HandlerScope.(T) -> Unit,
    ) = source(
        name, onItemError, completionTimeout, parallel, progress, errorThreshold,
        items = { items(resolve(input)) },
        handle = handle,
    )

    /** Та же форма для двух зависимостей. */
    fun <I1, I2, T> source(
        input1: Input<I1>,
        input2: Input<I2>,
        name: String? = null,
        onItemError: ItemError<T> = ItemError.Fail,
        completionTimeout: Duration? = null,
        parallel: Int? = null,
        progress: Progress = Progress.Default,
        errorThreshold: Long? = null,
        items: SourceScope.(I1, I2) -> Sequence<T>,
        handle: HandlerScope.(T) -> Unit,
    ) = source(
        name, onItemError, completionTimeout, parallel, progress, errorThreshold,
        items = { items(resolve(input1), resolve(input2)) },
        handle = handle,
    )

    /**
     * `scoped`, объявляющая зависимость родителей от [input]: значение приезжает в [parents]
     * параметром.
     *
     * В [items] оно не передаётся намеренно — в подавляющем большинстве стадий родители берут из
     * input'а ключи, а элементы читаются уже по родителю. Редкому случаю, где значение нужно и
     * элементам, остаётся `resolve(input)` в теле: он вернёт тот же закэшированный объект.
     */
    fun <I, P, T> scoped(
        input: Input<I>,
        parents: SourceScope.(I) -> Sequence<P>,
        name: String? = null,
        completionTimeout: Duration? = null,
        onItemError: ItemError<T> = ItemError.Fail,
        parallel: Int? = null,
        progress: Progress = Progress.Default,
        errorThreshold: Long? = null,
        items: SourceScope.(P) -> Sequence<T>,
        handle: HandlerScope.(T) -> Unit,
    ) = scoped(
        name = name,
        parents = { parents(resolve(input)) },
        completionTimeout = completionTimeout,
        onItemError = onItemError,
        parallel = parallel,
        progress = progress,
        errorThreshold = errorThreshold,
        items = items,
        handle = handle,
    )

    internal fun build(migrationName: String, author: String, onUnhandled: ScriptPolicy?): MigrationPlan {
        require(stages.isNotEmpty()) { "migration '$migrationName' declares no stage" }
        if (stages.size > 1) {
            require(stages.all { it.name != null }) {
                "migration '$migrationName' has ${stages.size} stages, so every stage needs a name"
            }
            val duplicate = stages.mapNotNull { it.name }.groupingBy { it }.eachCount()
                .filterValues { it > 1 }.keys
            require(duplicate.isEmpty()) { "duplicate stage names: $duplicate" }
        }
        return MigrationPlan(
            migrationName,
            author,
            onUnhandled,
            stages.toList(),
            inputs.toList(),
            outputs.toList(),
            validation,
        )
    }
}

/**
 * Собирает [MigrationPlan]. Тело [build] выполняется немедленно, но только регистрирует узлы —
 * ни одна пользовательская лямбда при этом не вызывается.
 */
fun migration(
    name: String,
    author: String,
    onUnhandled: ScriptPolicy? = null,
    build: MigrationBuilder.() -> Unit,
): MigrationPlan = MigrationBuilder().apply(build).build(name, author, onUnhandled)
