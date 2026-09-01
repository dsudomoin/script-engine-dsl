package io.github.dsudomoin.migration.kora

/**
 * Как завершить процесс после прогона миграции.
 *
 * По умолчанию runner зовёт `exitProcess(code)`: одноразовый скрипт запускается отдельным
 * процессом (K8s Job, шаг CI), и код возврата — единственное, что видит оркестратор.
 *
 * Компонент этого типа в графе переопределяет поведение. Нужен в двух случаях:
 * - тесты, поднимающие настоящий граф: `exitProcess` убил бы JVM тест-раннера;
 * - встраивание runner'а в приложение, которое продолжает жить после миграции.
 *
 * ```
 * @Component
 * class RecordingExit : MigrationExit {
 *     @Volatile var code: Int? = null
 *     override fun exit(code: Int) { this.code = code }
 * }
 * ```
 */
fun interface MigrationExit {
    fun exit(code: Int)
}
