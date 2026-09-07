package io.github.dsudomoin.migration.kora.ops

import io.github.dsudomoin.migration.HandlerScope
import io.github.dsudomoin.migration.RunScope
import io.github.dsudomoin.migration.WriteOutcome
import io.github.dsudomoin.migration.WriteResult
import io.github.dsudomoin.migration.internal.RunContext
import java.util.concurrent.CompletionStage

/**
 * [HandlerScope] поверх [RunContext] — для юнит-тестов ops без построения плана.
 *
 * Нужен потому, что пишущие фабрики (`jdbc`, `cassandra`, `http`, `kafka`, `topic`) объявлены
 * расширениями [HandlerScope], а вне исполняемого плана такого scope'а взять неоткуда. Живёт в
 * тестовом sourceSet'е намеренно: в production-API этого шва нет, иначе разделение read/write
 * обходилось бы одной строкой.
 *
 * [publish] не поддержан: у него нет барьера scope'а, и эффект, зарегистрированный здесь, никто
 * бы не дождался. Публикацию проверяй через `PlanInterpreter` на настоящем плане.
 */
internal class TestHandlerScope(private val base: RunContext) : HandlerScope, RunScope by base {

    override fun write(name: String, args: Map<String, Any?>, action: () -> WriteOutcome): WriteResult {
        if (base.dryRun) {
            base.report.incDryRunSkipped(name)
            return WriteResult.DryRunSkipped
        }
        return when (val outcome = action()) {
            is WriteOutcome.Applied -> {
                base.report.incAppliedWrite(name)
                WriteResult.Applied
            }

            is WriteOutcome.Rejected -> {
                base.report.incRejectedWrite(name)
                WriteResult.Rejected(outcome.reason)
            }
        }
    }

    override fun writeRows(name: String, args: Map<String, Any?>, action: () -> Int): WriteResult =
        write(name, args) {
            val rows = action()
            if (rows > 0) WriteOutcome.Applied else WriteOutcome.Rejected("no rows matched")
        }

    override fun publish(name: String, args: Map<String, Any?>, send: () -> CompletionStage<*>): Unit =
        error("publish is not supported outside a running plan: use PlanInterpreter with a real migration")
}

/** Обработчик-scope над контекстом прогона: `with(handler(ctx)) { jdbc(db).execute(...) }`. */
internal fun handler(ctx: RunContext): HandlerScope = TestHandlerScope(ctx)
