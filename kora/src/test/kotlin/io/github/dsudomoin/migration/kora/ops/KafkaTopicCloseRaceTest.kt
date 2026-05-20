package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.internal.DefaultMigrationContext
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class KafkaTopicCloseRaceTest {

    private class FlushCountingMockProducer
        : MockProducer<String, String>(true, StringSerializer(), StringSerializer()) {
        val flushCount = AtomicInteger()
        override fun flush() {
            flushCount.incrementAndGet()
            super.flush()
        }
    }

    @Test
    fun `close из N потоков — flush вызван ровно один раз`(@TempDir tmp: Path) {
        val producer = FlushCountingMockProducer()
        val ctx = DefaultMigrationContext.test(outputFolder = tmp)
        val t = with(ctx) { topic(producer, "test-topic") }

        val workers = 32
        val pool = Executors.newFixedThreadPool(workers)
        val start = CountDownLatch(1)
        val done = CountDownLatch(workers)

        repeat(workers) {
            pool.submit {
                start.await()
                t.close()
                done.countDown()
            }
        }
        start.countDown()
        check(done.await(5, TimeUnit.SECONDS)) { "concurrent close timed out" }
        pool.shutdown()

        assertThat(producer.flushCount.get()).isEqualTo(1)
    }

    @Test
    fun `close под dry-run — flush не вызывается ни разу`(@TempDir tmp: Path) {
        val producer = FlushCountingMockProducer()
        val ctx = DefaultMigrationContext.test(dryRun = true, outputFolder = tmp)
        val t = with(ctx) { topic(producer, "test-topic") }

        t.close()
        t.close()

        assertThat(producer.flushCount.get()).isEqualTo(0)
    }
}
