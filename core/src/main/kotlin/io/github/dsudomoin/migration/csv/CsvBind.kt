package io.github.dsudomoin.migration.csv

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.github.dsudomoin.migration.ItemError
import io.github.dsudomoin.migration.MigrationScope
import java.nio.charset.Charset
import kotlin.reflect.KClass
import kotlin.reflect.KParameter
import kotlin.reflect.full.primaryConstructor

/**
 * То же, что [readCsv], но строка сразу приезжает разобранной в DTO: `readCsvAs<Customer>(path)`.
 *
 * DTO — обычный `data class`; колонка ищется по имени свойства без учёта регистра и способа
 * записи (`spend_amount`, `SPEND_AMOUNT`, `Spend Amount` → `spendAmount`). Лишние колонки файла
 * игнорируются: в выгрузке их обычно вчетверо больше, чем нужно миграции.
 *
 * Правила для полей, которых в строке нет или чья ячейка пуста:
 * - поле со значением по умолчанию — берётся умолчание;
 * - nullable-поле — `null`;
 * - non-null `String` — пустая строка (это законное значение строки);
 * - остальные non-null поля — ошибка. Без этого пустая ячейка под `Long` молча превратилась
 *   бы в `0` и уехала в целевую систему как настоящее число.
 *
 * Отсутствие колонки под обязательное поле проверяется один раз, до первой записи, и валит
 * прогон списком всех недостающих сразу — чинить файл по одной колонке за прогон незачем.
 *
 * @see readCsv параметры [path], [classpath], [delimiter], [quote], [charset] и [onRowError]
 *              означают то же самое.
 */
inline fun <reified T : Any> MigrationScope.readCsvAs(
    path: String,
    classpath: Boolean = false,
    delimiter: Char = ',',
    quote: Char = '"',
    charset: Charset = Charsets.UTF_8,
    onRowError: ItemError<CsvRow> = ItemError.Fail,
): Sequence<T> {
    val binder = csvBinder(T::class)
    return readCsv(path, classpath, delimiter, quote, charset, onRowError) { binder.bind(it) }
}

/** Точка входа для инлайна [readCsvAs]; прикладной код зовёт `readCsvAs<T>(...)`. */
@PublishedApi
internal fun <T : Any> csvBinder(type: KClass<T>): CsvBinder<T> = CsvBinder(type)

/**
 * Сборка DTO из [CsvRow]: сначала своя проверка обязательных полей, потом Jackson.
 *
 * Своя проверка не косметика. Jackson на отсутствующей колонке и на пустой ячейке отдаёт под
 * `Long` ноль, под `Boolean` — `false`: тихая подмена, которую в отчёте не видно и в целевой
 * системе не отличить от настоящего значения.
 *
 * Не thread-safe и не должен быть: последовательность читается тем же потоком, который крутит
 * цикл `each`, — воркеры получают уже собранные объекты.
 */
@PublishedApi
internal class CsvBinder<T : Any>(private val type: KClass<T>) {

    private class Field(
        val name: String,
        /** Есть значение по умолчанию — колонку можно не давать вовсе. */
        val optional: Boolean,
        val nullable: Boolean,
        /** Для `String` пустая ячейка — законное значение, а не отсутствие данных. */
        val textual: Boolean,
        val typeName: String,
    )

    private val fields: List<Field> = describe(type)

    private var headerChecked = false

    fun bind(row: CsvRow): T {
        if (!headerChecked) {
            checkHeader(row)
            headerChecked = true
        }
        val values = HashMap<String, Any?>(fields.size)
        for (f in fields) {
            val raw = row.getOrNull(f.name) ?: continue
            if (raw.isNotBlank()) {
                values[f.name] = raw
                continue
            }
            when {
                f.optional -> Unit
                f.nullable -> values[f.name] = null
                f.textual -> values[f.name] = raw
                else -> throw IllegalArgumentException(
                    "строка ${row.lineNumber}: колонка '${row.columnName(f.name) ?: f.name}' пуста, " +
                        "а поле '${f.name}' типа ${f.typeName} обязательно — заполните ячейку, " +
                        "сделайте поле nullable или задайте значение по умолчанию",
                )
            }
        }
        return BIND_MAPPER.convertValue(values, type.java)
    }

    private fun checkHeader(row: CsvRow) {
        val missing = fields.filter { !it.optional && !it.nullable && row.getOrNull(it.name) == null }
        if (missing.isEmpty()) return
        throw CsvStructureException(
            "в CSV нет колонок под обязательные поля ${type.simpleName}: " +
                missing.joinToString(", ") { "'${it.name}' (${it.typeName})" } +
                "; колонки файла: ${row.columns.joinToString(", ")}",
        )
    }

    private companion object {
        // ObjectMapper потокобезопасен после настройки и дорог в создании — один на процесс.
        private val BIND_MAPPER: ObjectMapper = jacksonObjectMapper().registerModule(JavaTimeModule())

        fun describe(type: KClass<*>): List<Field> {
            val ctor = type.primaryConstructor
            val params = ctor?.parameters?.filter { it.kind == KParameter.Kind.VALUE }.orEmpty()
            require(params.isNotEmpty()) {
                "readCsvAs требует Kotlin-класс с первичным конструктором и хотя бы одним " +
                    "параметром, а ${type.simpleName} такого не имеет — опишите DTO как data class"
            }
            return params.map { p ->
                val classifier = p.type.classifier as? KClass<*>
                Field(
                    name = requireNotNull(p.name) { "параметр конструктора ${type.simpleName} без имени" },
                    optional = p.isOptional,
                    nullable = p.type.isMarkedNullable,
                    textual = classifier == String::class,
                    typeName = classifier?.simpleName ?: p.type.toString(),
                )
            }
        }
    }
}
