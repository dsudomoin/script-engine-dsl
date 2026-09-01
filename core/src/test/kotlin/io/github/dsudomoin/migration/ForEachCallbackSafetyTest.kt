package io.github.dsudomoin.migration

import io.github.dsudomoin.migration.internal.DefaultMigrationContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Диагностический колбэк не имеет права изменить судьбу item'а. Раньше исключение из `logEach`
 * летело из учётного блока и трактовалось как отказ обработки: item попадал и в `successful`, и
 * в `failed`, а под дефолтным `OnError.Fail` миграция обрывалась уже ПОСЛЕ выполненной записи.
 */
class ForEachCallbackSafetyTest {

    @Test
    fun `падающий logEach не превращает успешный item в отказ`() {
        val ctx = DefaultMigrationContext.test()
        val done = mutableListOf<Int>()

        with(ctx) {
            forEach(listOf(1, 2, 3), logEach = { error("логгер сломан") }) { n -> done += n }
        }

        val report = ctx.report.build()
        assertThat(done).containsExactly(1, 2, 3)
        assertThat(report.successful).isEqualTo(3)
        assertThat(report.failed).isEqualTo(0)
        assertThat(report.processed).isEqualTo(3)
        assertThat(report.warnings)
            .describedAs("о сбое сообщаем один раз, иначе на миллионе item'ов список warnings сам станет утечкой")
            .hasSize(1)
        assertThat(report.warnings.single()).contains("logEach")
    }

    @Test
    fun `падающий onErrorLog не мешает Skip-политике`() {
        val ctx = DefaultMigrationContext.test()

        with(ctx) {
            forEach(
                listOf(1, 2),
                onError = OnError.Skip,
                onErrorLog = { _, _ -> error("логгер сломан") },
            ) { error("бизнес-ошибка") }
        }

        val report = ctx.report.build()
        assertThat(report.skipped).isEqualTo(2)
        assertThat(report.failed).isEqualTo(0)
    }
}
