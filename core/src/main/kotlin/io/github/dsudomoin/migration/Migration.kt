package io.github.dsudomoin.migration

/**
 * Базовый класс one-shot миграционного скрипта.
 *
 * Каждый скрипт — наследник [Migration] с реализованным [migrate]. Регистрируется в Kora-графе
 * как `@Component`. Runner находит все [Migration] через `All<Migration>` и запускает ту, имя
 * которой совпадает с `migration.run` в HOCON.
 *
 * @param name уникальное имя миграции, по которому runner её находит (например, `"SAMPLE-001"`).
 *             Дубликаты имён в одном Kora-графе → exit-code 2 на старте.
 * @param author человекочитаемая метка автора. Идёт в [io.github.dsudomoin.migration.report.MigrationReport] и
 *               `errors.csv`.
 * @param onUnhandled политика при unhandled-исключении в [migrate]. `null` (дефолт) →
 *                    делегировать в `migration.defaults.onUnhandled` HOCON (который сам по
 *                    умолчанию [ScriptPolicy.FAIL_FAST]). Явное значение перекрывает HOCON.
 */
abstract class Migration(
    val name: String,
    val author: String,
    val onUnhandled: ScriptPolicy? = null,
) {
    /**
     * Тело миграции. Объявлено как extension-receiver-метод на [MigrationContext] —
     * вызовы `forEach`, `openCsv`, `jdbc(db)`, `mutation { }` доступны без префикса.
     *
     * Реализация обычно:
     * 1. Объявляет долгоживущие ресурсы сверху (`val csv = openCsv(...)`, `val t = topic(...)`).
     * 2. Запускает основной цикл через `forEach(items, ...) { ... }`.
     *
     * Runner оборачивает вызов в `try/finally` — все зарегистрированные через
     * [MigrationContext.register] ресурсы закроются после возврата, даже при exception.
     * Поведение при exception определяется [onUnhandled].
     */
    abstract fun MigrationContext.migrate()
}

/**
 * Политика runner'а при unhandled-исключении в `migrate()`.
 *
 * Не влияет на ошибки **внутри** `forEach` — там действует [OnError]. Сюда попадают только
 * исключения, вылетевшие за пределы любого `forEach` или брошенные `OnError.Decision.Fail`.
 */
enum class ScriptPolicy {
    /**
     * Залогировать ошибку, инкрементить `report.failed`, вернуть exit-code 1.
     * Дефолт. Уместен когда непредвиденный сбой = «не пишем дальше, пусть human разберётся».
     */
    FAIL_FAST,

    /**
     * Залогировать ошибку, инкрементить `report.failed`, дойти до конца (закрыть ресурсы,
     * напечатать отчёт), вернуть exit-code 0. Уместен для compliance-сценариев, где даже
     * частичный отчёт нужнее, чем абортированный прогон.
     */
    LOG_AND_COMPLETE,
}
