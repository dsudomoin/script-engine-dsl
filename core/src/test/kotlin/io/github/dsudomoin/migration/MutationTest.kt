package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.internal.DefaultMigrationContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class MutationTest {
    @Test
    fun `mutation выполняет action при dryRun=false`() {
        val ctx = DefaultMigrationContext.test(dryRun = false)
        var called = false
        with(ctx) { mutation("cancel order 42") { called = true } }
        assertThat(called).isTrue()
    }

    @Test
    fun `mutation пропускает action при dryRun=true`() {
        val ctx = DefaultMigrationContext.test(dryRun = true)
        var called = false
        with(ctx) { mutation("cancel order 42") { called = true } }
        assertThat(called).isFalse()
        assertThat(ctx.report.build().dryRunSkipped).containsEntry("mutation:cancel order 42", 1L)
    }

    @Test
    fun `mutation generic возвращает значение action при dryRun=false`() {
        val ctx = DefaultMigrationContext.test(dryRun = false)
        val result = with(ctx) {
            mutation("billing.refund", args = mapOf("id" to 42), dryRunDefault = -1) { 202 }
        }
        assertThat(result).isEqualTo(202)
        assertThat(ctx.report.build().dryRunSkipped).isEmpty()
    }

    @Test
    fun `mutation generic возвращает dryRunDefault при dryRun=true и не вызывает action`() {
        val ctx = DefaultMigrationContext.test(dryRun = true)
        var called = false
        val result = with(ctx) {
            mutation("billing.refund", args = mapOf("id" to 42), dryRunDefault = -1) {
                called = true
                202
            }
        }
        assertThat(called).isFalse()
        assertThat(result).isEqualTo(-1)
        assertThat(ctx.report.build().dryRunSkipped).containsEntry("mutation:billing.refund", 1L)
    }

    @Test
    fun `mutation generic работает с nullable R`() {
        val ctx = DefaultMigrationContext.test(dryRun = true)
        val result: String? = with(ctx) {
            mutation("api.create", dryRunDefault = null as String?) { "real-id" }
        }
        assertThat(result).isNull()
    }

    @Test
    fun `mutation без dryRunDefault и с lambda возвращающим Int всё равно резолвится в Unit-перегрузку`() {
        // Защита от регрессии: новый overload не должен ломать существующий код,
        // в котором лямбда возвращает значение, но `dryRunDefault` не передан.
        val ctx = DefaultMigrationContext.test(dryRun = false)
        var sideEffect = 0
        with(ctx) {
            mutation("legacy") {
                sideEffect = 42
                "ignored"   // возвращаемое значение дискардится — старый контракт
            }
        }
        assertThat(sideEffect).isEqualTo(42)
    }
}
