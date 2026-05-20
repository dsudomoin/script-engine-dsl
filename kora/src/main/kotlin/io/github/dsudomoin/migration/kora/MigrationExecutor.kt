package io.github.dsudomoin.migration.kora

/**
 * Маркер для `@Tag(MigrationExecutor::class)` на пользовательском `Executor`-бине, чтобы
 * runner использовал его вместо дефолтного `Executors.newFixedThreadPool(parallel)`.
 *
 * Уместно, если хочется кастомного thread pool (с metric'ами, ThreadFactory с MDC, и т.д.):
 * ```
 * @Tag(MigrationExecutor::class)
 * fun customExecutor(): Executor = Executors.newFixedThreadPool(16, ...)
 * ```
 *
 * Без этого тэга runner создаёт свой пул на основе `migration.defaults.parallel` из HOCON.
 */
@Target(AnnotationTarget.FIELD, AnnotationTarget.FUNCTION, AnnotationTarget.VALUE_PARAMETER, AnnotationTarget.PROPERTY)
@Retention(AnnotationRetention.RUNTIME)
annotation class MigrationExecutor
