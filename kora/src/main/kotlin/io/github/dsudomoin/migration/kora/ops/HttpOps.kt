package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.MigrationContext

/**
 * Минимальный функциональный контракт для HTTP-вызова. Намеренно untyped — позволяет адаптировать
 * любой HTTP-клиент (Kora `HttpClient`, OkHttp, raw HTTP) под DSL.
 *
 * @return HTTP status code от внешнего сервиса.
 */
typealias HttpCall = (method: String, path: String, body: ByteArray?, headers: Map<String, String>) -> Int

/**
 * Ad-hoc HTTP-операции — wrapper поверх функционального [HttpCall]. Полезно когда типизированного
 * Kora `@HttpClient` нет или его настройка — overkill для одного скрипта.
 *
 * Write-методы (`post`, `patch`, `put`, `delete`) идут через `guardWrite` — dry-run пропускает.
 * Read-методы (`get`) выполняются всегда.
 *
 * Для типизированного Kora `@HttpClient` — оборачивай write-вызов в `mutation("label") { ... }`,
 * а read-вызов делай напрямую без обёртки.
 */
class HttpOps internal constructor(
    private val ctx: MigrationContext,
    private val call: HttpCall,
) {
    /** GET — read-only, без dry-run gate. */
    fun get(path: String, headers: Map<String, String> = emptyMap()): Int =
        call("GET", path, null, headers)

    /** POST с dry-run gate (`label = "http.post"`). */
    fun post(path: String, body: ByteArray = EMPTY_BODY, headers: Map<String, String> = emptyMap()): Int =
        ctx.guardWrite("http.post", mapOf("path" to path), dryRunDefault = 0) {
            call("POST", path, body, headers)
        }

    /** PATCH с dry-run gate. */
    fun patch(path: String, body: ByteArray = EMPTY_BODY, headers: Map<String, String> = emptyMap()): Int =
        ctx.guardWrite("http.patch", mapOf("path" to path), dryRunDefault = 0) {
            call("PATCH", path, body, headers)
        }

    /** PUT с dry-run gate. */
    fun put(path: String, body: ByteArray = EMPTY_BODY, headers: Map<String, String> = emptyMap()): Int =
        ctx.guardWrite("http.put", mapOf("path" to path), dryRunDefault = 0) {
            call("PUT", path, body, headers)
        }

    /** DELETE с dry-run gate. */
    fun delete(path: String, headers: Map<String, String> = emptyMap()): Int =
        ctx.guardWrite("http.delete", mapOf("path" to path), dryRunDefault = 0) {
            call("DELETE", path, null, headers)
        }

    private companion object {
        // Дефолтное «пустое тело» — shared. `ByteArray(0)` как default-value создавал бы новый
        // массив на каждый вызов; на цикле в миллион items это миллион короткоживущих аллокаций.
        private val EMPTY_BODY = ByteArray(0)
    }
}

/**
 * Фабрика [HttpOps]. [call] — твой адаптер: функция, которая знает как выполнить запрос
 * через конкретный HTTP-клиент.
 */
fun MigrationContext.http(call: HttpCall): HttpOps = HttpOps(this, call)
