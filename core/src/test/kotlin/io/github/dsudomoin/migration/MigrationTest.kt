package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.internal.DefaultMigrationContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class MigrationTest {

    private class Sample : Migration(name = "SAMPLE-001", author = "team") {
        var ran = false
        override fun MigrationContext.migrate() { ran = true }
    }

    @Test
    fun `migrate вызывается через контекст, receiver виден как this`() {
        val mig = Sample()
        val ctx = DefaultMigrationContext.test()

        with(ctx) { mig.run { migrate() } }

        assertThat(mig.ran).isTrue()
    }

    @Test
    fun `по дефолту policy null - делегирует в config defaults onUnhandled`() {
        val mig = Sample()
        assertThat(mig.onUnhandled).isNull()
    }
}
