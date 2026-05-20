package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.internal.DefaultMigrationContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class MigrationContextGuardWriteTest {

    @Test
    fun `guardWrite выполняет action при dryRun=false`() {
        val ctx = DefaultMigrationContext.test(dryRun = false)
        var called = false

        val result = ctx.guardWrite(label = "op", dryRunDefault = -1) {
            called = true
            42
        }

        assertThat(called).isTrue()
        assertThat(result).isEqualTo(42)
    }

    @Test
    fun `guardWrite НЕ выполняет action при dryRun=true и возвращает default`() {
        val ctx = DefaultMigrationContext.test(dryRun = true)
        var called = false

        val result = ctx.guardWrite(label = "op", dryRunDefault = -1) {
            called = true
            42
        }

        assertThat(called).isFalse()
        assertThat(result).isEqualTo(-1)
        assertThat(ctx.report.build().dryRunSkipped).containsEntry("op", 1L)
    }

    @Test
    fun `guardWrite Unit перегрузка - не выполняет action при dryRun=true`() {
        val ctx = DefaultMigrationContext.test(dryRun = true)
        var called = false

        ctx.guardWrite(label = "voidop") {
            called = true
        }

        assertThat(called).isFalse()
        assertThat(ctx.report.build().dryRunSkipped).containsEntry("voidop", 1L)
    }
}
