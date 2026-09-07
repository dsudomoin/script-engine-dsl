package io.github.dsudomoin.migration.report

import java.nio.file.Path
import java.time.Duration
import java.time.Instant

/**
 * Итоги одной фазы прогона.
 *
 * Фаза — это один узел плана (`source` или `scoped`) со своим источником, обработчиком и политикой
 * ошибок. В `scoped` все родители принадлежат одной фазе: границей фазы служит узел плана, а не
 * отдельный родитель.
 *
 * Заполняется только у миграции с несколькими фазами: у единственной неявной фазы эти числа
 * совпали бы с итогами прогона.
 */
data class PhaseReport(
    val name: String,
    val processed: Long,
    val successful: Long,
    val skipped: Long,
    val failed: Long,
    val sourceSkipped: Long,
    val appliedWrites: Map<String, Long>,
    val rejectedWrites: Map<String, Long>,
    val dryRunSkipped: Map<String, Long>,
    val acknowledgedPublishes: Long,
    val failedEffects: Long,
    val abandonedPublishes: Long,
    val lateRegistered: Long,
    val rawPages: Long,
    val rawRows: Long,
)

/**
 * Immutable снимок статистики прогона миграции. Создаётся [ReportBuilder.build] в конце прогона,
 * рендерится [ReportFormatter] в текстовый отчёт.
 *
 * @property processed всего обработано элементов стадиями.
 * @property successful сколько прошли успешно.
 * @property skipped сколько ушли в Skip-ветку (item'ы в `errors.csv`).
 * @property failed элементы, отказ которых политика признала терминальным. Держится тождество
 *                  `processed = successful + skipped + failed`.
 * @property unhandledFailures отказы уровня прогона или стадии, у которых обрабатываемого элемента
 *                  могло не быть (ошибка источника, валидации, барьера). В тождество **не входят**:
 *                  такой отказ не является исходом ни одного элемента.
 * @property phases итоги по каждой фазе в порядке исполнения. Пуст у миграции с единственной
 *                  неявной фазой — там эти числа совпадают с итогами прогона.
 * @property failedEffects отказы асинхронных эффектов, обнаруженные на барьере стадии. Не входят
 *                         в [failed]: item к тому моменту уже посчитан.
 * @property abandonedPublishes отправленные эффекты, не подтвердившиеся за таймаут барьера.
 * @property lateRegistered эффекты, зарегистрированные уже после закрытия барьера.
 * @property sourceSkipped строки, отброшенные при чтении источника (битый CSV). Не входят в
 *                         [processed] и [skipped]: до стадии они не дошли.
 * @property rawPages сырые страницы, прочитанные `pages(...)` — до пользовательских фильтров.
 * @property dryRunSkipped разбивка `label → count` для skipped writes под dry-run. Ключи —
 *                         human-readable метки операций (`"jdbc.execute"`, `"kafka.publish"`,
 *                         `"customer.tier"`).
 * @property errorsFile путь к `errors.csv` (для печати в отчёте; не nullable если миграция запускалась).
 * @property tracesFile путь к `errors.log`.
 * @property warnings нефатальные предупреждения runner'а (например, исключения при закрытии
 *                    registered-ресурсов). Не влияют на exit-code.
 */
data class MigrationReport(
    val name: String,
    val author: String,
    val startedAt: Instant,
    val finishedAt: Instant,
    val duration: Duration,
    val dryRun: Boolean,
    val processed: Long,
    val successful: Long,
    val skipped: Long,
    val failed: Long,
    val dryRunSkipped: Map<String, Long>,
    val appliedWrites: Map<String, Long> = emptyMap(),
    val rejectedWrites: Map<String, Long> = emptyMap(),
    val acknowledgedPublishes: Long = 0,
    val failedEffects: Long = 0,
    val abandonedPublishes: Long = 0,
    val lateRegistered: Long = 0,
    val sourceSkipped: Long = 0,
    val rawPages: Long = 0,
    val rawRows: Long = 0,
    val unhandledFailures: Long = 0,
    val phases: List<PhaseReport> = emptyList(),
    val errorsFile: Path?,
    val tracesFile: Path?,
    val warnings: List<String> = emptyList(),
)
