package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.internal.DefaultMigrationContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class HttpOpsTest {

    @Test
    fun `get выполняется и возвращает статус`() {
        val ctx = DefaultMigrationContext.test()
        val calls = mutableListOf<String>()
        val stub: HttpCall = { m, p, _, _ -> calls += "$m $p"; 200 }

        val status = with(ctx) { http(stub).get("/orders/42") }

        assertThat(status).isEqualTo(200)
        assertThat(calls).containsExactly("GET /orders/42")
    }

    @Test
    fun `post под dry-run не вызывает client и возвращает дефолт 0`() {
        val ctx = DefaultMigrationContext.test(dryRun = true)
        var called = false
        val stub: HttpCall = { _, _, _, _ -> called = true; 201 }

        val status = with(ctx) { http(stub).post("/hooks", "body".toByteArray()) }

        assertThat(called).isFalse()
        assertThat(status).isEqualTo(0)
        assertThat(ctx.report.build().dryRunSkipped).containsEntry("http.post", 1L)
    }

    @Test
    fun `post без dry-run вызывает client`() {
        val ctx = DefaultMigrationContext.test()
        val stub: HttpCall = { _, _, _, _ -> 204 }

        val status = with(ctx) { http(stub).post("/hooks", ByteArray(0)) }

        assertThat(status).isEqualTo(204)
    }

    @Test
    fun `HttpCall throws - исключение пробрасывается через post`() {
        val ctx = DefaultMigrationContext.test()
        val stub: HttpCall = { _, _, _, _ -> throw java.net.SocketTimeoutException("timeout") }

        val ex = runCatching {
            with(ctx) { http(stub).post("/hooks", ByteArray(0)) }
        }.exceptionOrNull()

        assertThat(ex).isInstanceOf(java.net.SocketTimeoutException::class.java)
        assertThat(ex!!.message).isEqualTo("timeout")
    }

    @Test
    fun `HttpCall throws - get тоже пробрасывает (нет dry-run gate)`() {
        val ctx = DefaultMigrationContext.test()
        val stub: HttpCall = { _, _, _, _ -> error("network down") }

        val ex = runCatching {
            with(ctx) { http(stub).get("/health") }
        }.exceptionOrNull()

        assertThat(ex).isNotNull()
        assertThat(ex!!.message).isEqualTo("network down")
    }

    @Test
    fun `patch put delete - все под dry-run`() {
        val ctx = DefaultMigrationContext.test(dryRun = true)
        val stub: HttpCall = { _, _, _, _ -> 200 }

        with(ctx) {
            http(stub).patch("/a")
            http(stub).put("/b")
            http(stub).delete("/c")
        }

        val skipped = ctx.report.build().dryRunSkipped
        assertThat(skipped).containsKeys("http.patch", "http.put", "http.delete")
    }
}
