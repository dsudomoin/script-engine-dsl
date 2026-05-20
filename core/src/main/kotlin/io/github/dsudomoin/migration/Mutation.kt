package io.github.dsudomoin.migration

/**
 * Dry-run gate для одиночной write-операции, которую DSL сам перехватить не может (например,
 * вызов типизированного Kora `@HttpClient`- или `@KafkaPublisher`-метода).
 *
 * Под `dryRun = false` [action] вызывается. Под `dryRun = true` [action] **не** вызывается,
 * в лог идёт `INFO [DRY-RUN] mutation:<label> (args...)`, в `report.dryRunSkipped`
 * инкрементится ключ `"mutation:<label>"`.
 *
 * **Важно про label.** Если вызываешь `mutation` в цикле, используй **константный** [label]
 * (`"orders.publish"`), а контекст item'а пиши в [args] (`mapOf("orderId" to id)`). Тогда в отчёте
 * получишь чистый агрегат `mutation:orders.publish: 8421`, а в лог пойдут отдельные строки с
 * `(orderId=42)`. Если же label = `"orders.publish $id"` — в отчёте будет 8421 уникальная запись.
 *
 * @param label человекочитаемая метка операции (константа — будет ключом в `report.dryRunSkipped`).
 * @param args дополнительные диагностические аргументы, идут в dry-run-лог. Не аффектят аггрегацию.
 */
fun MigrationContext.mutation(label: String, args: Map<String, Any?> = emptyMap(), action: () -> Unit) {
    guardWrite(label = "mutation:$label", args = args, action = action)
}

/**
 * Generic-перегрузка [mutation], возвращающая значение из [action]. Под dry-run [action] не
 * вызывается, возвращается [dryRunDefault], в `report.dryRunSkipped` инкрементится `mutation:<label>`.
 *
 * Используется, когда типизированный Kora-клиент (`@HttpClient`, repository-метод) возвращает
 * значимый результат — статус-код, id из `RETURNING`, доменный объект — а DSL должен решить, что
 * подсунуть под dry-run.
 *
 * ```
 * val status = mutation("billing.refund", args = mapOf("orderId" to id), dryRunDefault = 202) {
 *     billing.refund(id)   // returns Int (HTTP status)
 * }
 * ```
 *
 * Backward-совместимость: `Unit`-перегрузка ([mutation] без [dryRunDefault]) остаётся —
 * существующий код продолжает работать. Kotlin резолвит overload по наличию [dryRunDefault]
 * (отсутствует → старая, есть → новая); тело-лямбда `{ 42 }` без [dryRunDefault] всё ещё
 * попадает в `Unit`-перегрузку (`42` дискардится) — это тот же контракт, что и был.
 *
 * @param label константная метка операции — ключ в `report.dryRunSkipped`.
 * @param args диагностические аргументы для dry-run-лога.
 * @param dryRunDefault значение, возвращаемое под dry-run вместо [action].
 */
fun <R> MigrationContext.mutation(
    label: String,
    args: Map<String, Any?> = emptyMap(),
    dryRunDefault: R,
    action: () -> R,
): R = guardWrite(label = "mutation:$label", args = args, dryRunDefault = dryRunDefault, action = action)
