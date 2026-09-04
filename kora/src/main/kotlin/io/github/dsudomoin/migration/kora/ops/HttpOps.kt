package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.RunScope

/**
 * Минимальный функциональный контракт для HTTP-вызова. Намеренно untyped — позволяет адаптировать
 * любой HTTP-клиент (Kora `HttpClient`, OkHttp, raw HTTP) под DSL.
 *
 * @return HTTP status code от внешнего сервиса.
 */
typealias HttpCall = (method: String, path: String, body: ByteArray?, headers: Map<String, String>) -> Int

/**
 * Внешний сервис ответил кодом вне диапазона 2xx. Бросается всеми методами [HttpOps], чтобы
 * неуспешный вызов доходил до политики ошибок стадии и до `errors.csv`, а не засчитывался как успешный.
 *
 * Ловится в `ItemError.Handle { e, item -> ... }` для политики по коду ответа:
 * ```
 * onItemError = ItemError.Handle { e, _: Order ->
 *     if (e is HttpStatusException && e.status == 409) ItemError.Decision.Skip else ItemError.Decision.Fail
 * }
 * ```
 */
class HttpStatusException(
    val method: String,
    val path: String,
    val status: Int,
) : RuntimeException("$method $path -> HTTP $status")

/**
 * Ad-hoc HTTP-операции — wrapper поверх функционального [HttpCall]. Полезно когда типизированного
 * Kora `@HttpClient` нет или его настройка — overkill для одного скрипта.
 *
 * Write-методы (`post`, `patch`, `put`, `delete`) идут через `guardWrite` — dry-run пропускает.
 * Read-методы (`get`) выполняются всегда.
 *
 * **Код ответа проверяется всегда.** Любой статус вне 2xx поднимает [HttpStatusException]: без
 * этого мёртвый бэкенд, отвечающий 500 на каждый запрос, давал бы отчёт «100 000 successful»
 * при нулевом эффекте бэкфилла.
 *
 * Для типизированного Kora `@HttpClient` — оборачивай write-вызов в `write("label") { ... }`,
 * а read-вызов делай напрямую без обёртки.
 */
class HttpOps internal constructor(
    private val ctx: RunScope,
    private val call: HttpCall,
) {
    /** GET — read-only, без dry-run gate. Не-2xx поднимает [HttpStatusException]. */
    fun get(path: String, headers: Map<String, String> = emptyMap()): Int =
        checked("GET", path, null, headers)

    /** POST с dry-run gate (`label = "http.post"`). */
    fun post(path: String, body: ByteArray = EMPTY_BODY, headers: Map<String, String> = emptyMap()): Int =
        ctx.guardWrite("http.post", mapOf("path" to path), dryRunDefault = DRY_RUN_STATUS) {
            checked("POST", path, body, headers)
        }

    /** PATCH с dry-run gate. */
    fun patch(path: String, body: ByteArray = EMPTY_BODY, headers: Map<String, String> = emptyMap()): Int =
        ctx.guardWrite("http.patch", mapOf("path" to path), dryRunDefault = DRY_RUN_STATUS) {
            checked("PATCH", path, body, headers)
        }

    /** PUT с dry-run gate. */
    fun put(path: String, body: ByteArray = EMPTY_BODY, headers: Map<String, String> = emptyMap()): Int =
        ctx.guardWrite("http.put", mapOf("path" to path), dryRunDefault = DRY_RUN_STATUS) {
            checked("PUT", path, body, headers)
        }

    /** DELETE с dry-run gate. */
    fun delete(path: String, headers: Map<String, String> = emptyMap()): Int =
        ctx.guardWrite("http.delete", mapOf("path" to path), dryRunDefault = DRY_RUN_STATUS) {
            checked("DELETE", path, null, headers)
        }

    private fun checked(method: String, path: String, body: ByteArray?, headers: Map<String, String>): Int {
        val status = call(method, path, body, headers)
        if (status !in 200..299) throw HttpStatusException(method, path, status)
        return status
    }

    private companion object {
        // Дефолтное «пустое тело» — shared. `ByteArray(0)` как default-value создавал бы новый
        // массив на каждый вызов; на цикле в миллион items это миллион короткоживущих аллокаций.
        private val EMPTY_BODY = ByteArray(0)

        // Под dry-run возвращаем 200, а не 0: вызывающий код почти всегда проверяет статус, и
        // ноль отправил бы репетицию в ветку ошибки — dry-run обязан идти тем же путём, что и бой.
        private const val DRY_RUN_STATUS = 200
    }
}

/**
 * Фабрика [HttpOps]. [call] — твой адаптер: функция, которая знает как выполнить запрос
 * через конкретный HTTP-клиент.
 */
fun RunScope.http(call: HttpCall): HttpOps = HttpOps(this, call)
