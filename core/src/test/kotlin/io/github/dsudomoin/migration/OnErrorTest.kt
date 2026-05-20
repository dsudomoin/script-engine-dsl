package io.github.dsudomoin.migration

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class OnErrorTest {
    @Test
    fun `handle factory оборачивает lambda и возвращает Decision`() {
        val policy = OnError.handle { _, _ -> OnError.Decision.Skip }
        assertThat(policy.decide(RuntimeException(), Any())).isEqualTo(OnError.Decision.Skip)
    }

    @Test
    fun `handle factory может классифицировать по типу exception`() {
        val policy = OnError.handle { e, _ ->
            when (e) {
                is IllegalArgumentException -> OnError.Decision.Skip
                else                         -> OnError.Decision.Fail
            }
        }
        assertThat(policy.decide(IllegalArgumentException(), Any())).isEqualTo(OnError.Decision.Skip)
        assertThat(policy.decide(RuntimeException(), Any())).isEqualTo(OnError.Decision.Fail)
    }

    @Test
    fun `Fail и Skip — singletons`() {
        // sanity check для sealed-interface object'ов
        assertThat(OnError.Fail).isSameAs(OnError.Fail)
        assertThat(OnError.Skip).isSameAs(OnError.Skip)
    }
}
