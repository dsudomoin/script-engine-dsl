package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.internal.RunContext
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test

class HttpOpsTest {

    @Test
    fun `get выполняется и возвращает статус`() {
        val ctx = RunContext.test()
        val calls = mutableListOf<String>()
        val stub: HttpCall = { m, p, _, _ -> calls += "$m $p"; 200 }

        val status = with(handler(ctx)) { http(stub).get("/orders/42") }

        assertThat(status).isEqualTo(200)
        assertThat(calls).containsExactly("GET /orders/42")
    }

    @Test
    fun `post под dry-run не вызывает client и возвращает 200`() {
        val ctx = RunContext.test(dryRun = true)
        var called = false
        val stub: HttpCall = { _, _, _, _ -> called = true; 201 }

        val status = with(handler(ctx)) { http(stub).post("/hooks", "body".toByteArray()) }

        assertThat(called).isFalse()
        // Не 0: вызывающий код почти всегда смотрит на статус, и ноль отправил бы репетицию
        // в ветку ошибки — dry-run обязан идти тем же путём, что и бой.
        assertThat(status).isEqualTo(200)
        assertThat(ctx.report.build().dryRunSkipped).containsEntry("http.post", 1L)
    }

    @Test
    fun `post без dry-run вызывает client`() {
        val ctx = RunContext.test()
        val stub: HttpCall = { _, _, _, _ -> 204 }

        val status = with(handler(ctx)) { http(stub).post("/hooks", ByteArray(0)) }

        assertThat(status).isEqualTo(204)
    }

    @Test
    fun `HttpCall throws - исключение пробрасывается через post`() {
        val ctx = RunContext.test()
        val stub: HttpCall = { _, _, _, _ -> throw java.net.SocketTimeoutException("timeout") }

        val ex = runCatching {
            with(handler(ctx)) { http(stub).post("/hooks", ByteArray(0)) }
        }.exceptionOrNull()

        assertThat(ex).isInstanceOf(java.net.SocketTimeoutException::class.java)
        assertThat(ex!!.message).isEqualTo("timeout")
    }

    @Test
    fun `HttpCall throws - get тоже пробрасывает (нет dry-run gate)`() {
        val ctx = RunContext.test()
        val stub: HttpCall = { _, _, _, _ -> error("network down") }

        val ex = runCatching {
            with(handler(ctx)) { http(stub).get("/health") }
        }.exceptionOrNull()

        assertThat(ex).isNotNull()
        assertThat(ex!!.message).isEqualTo("network down")
    }

    @Test
    fun `patch put delete - все под dry-run`() {
        val ctx = RunContext.test(dryRun = true)
        val stub: HttpCall = { _, _, _, _ -> 200 }

        with(handler(ctx)) {
            http(stub).patch("/a")
            http(stub).put("/b")
            http(stub).delete("/c")
        }

        val skipped = ctx.report.build().dryRunSkipped
        assertThat(skipped).containsKeys("http.patch", "http.put", "http.delete")
    }
}

/**
 * Код ответа проверяется всегда: без этого мёртвый бэкенд, отвечающий 500 на каждый запрос,
 * давал бы отчёт «100 000 successful» при нулевом эффекте бэкфилла.
 */
class HttpOpsStatusTest {

    @Test
    fun `не-2xx поднимает HttpStatusException и не считается успехом`() {
        val ctx = RunContext.test()
        val stub: HttpCall = { _, _, _, _ -> 500 }

        val thrown = catchThrowable { with(handler(ctx)) { http(stub).post("/hooks") } }

        assertThat(thrown)
            .isInstanceOf(HttpStatusException::class.java)
            .hasMessageContaining("POST /hooks")
            .hasMessageContaining("500")
        assertThat((thrown as HttpStatusException).status).isEqualTo(500)
    }

    @Test
    fun `не-2xx на чтении тоже поднимает исключение`() {
        val ctx = RunContext.test()
        val stub: HttpCall = { _, _, _, _ -> 404 }

        assertThat(catchThrowable { with(handler(ctx)) { http(stub).get("/orders/42") } })
            .isInstanceOf(HttpStatusException::class.java)
    }

    @Test
    fun `2xx проходит`() {
        val ctx = RunContext.test()
        val stub: HttpCall = { _, _, _, _ -> 299 }

        assertThat(with(handler(ctx)) { http(stub).put("/orders/42") }).isEqualTo(299)
    }
}
