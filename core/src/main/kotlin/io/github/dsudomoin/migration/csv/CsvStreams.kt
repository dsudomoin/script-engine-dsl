package io.github.dsudomoin.migration.csv

import java.io.InputStream
import java.io.InputStreamReader
import java.io.PushbackInputStream
import java.io.Reader
import java.nio.charset.Charset

/**
 * Открывает текстовый читатель, срезая BOM.
 *
 * Без этого выгрузка из Excel (UTF-8 с BOM) даёт первую колонку с именем `﻿id`,
 * которое глазом не отличить от `id` — самый дорогой сорт ошибки в разборе файла.
 *
 * BOM UTF-16 заодно задаёт кодировку: если файл сам сообщил, в чём он, верить ему надёжнее,
 * чем параметру вызова.
 */
internal fun bomAwareReader(stream: InputStream, charset: Charset): Reader {
    val pushback = PushbackInputStream(stream, BOM_MAX_BYTES)
    val head = ByteArray(BOM_MAX_BYTES)
    // readNBytes, а не read: одиночный read вправе вернуть меньше запрошенного, и тогда
    // трёхбайтовый BOM опознался бы не целиком.
    val read = pushback.readNBytes(head, 0, BOM_MAX_BYTES)
    if (read <= 0) return InputStreamReader(pushback, charset)

    var skip = 0
    var effective = charset
    if (read >= 3 && head[0] == 0xEF.toByte() && head[1] == 0xBB.toByte() && head[2] == 0xBF.toByte()) {
        skip = 3
        effective = Charsets.UTF_8
    } else if (read >= 2 && head[0] == 0xFF.toByte() && head[1] == 0xFE.toByte()) {
        skip = 2
        effective = Charsets.UTF_16LE
    } else if (read >= 2 && head[0] == 0xFE.toByte() && head[1] == 0xFF.toByte()) {
        skip = 2
        effective = Charsets.UTF_16BE
    }
    if (read > skip) pushback.unread(head, skip, read - skip)
    return InputStreamReader(pushback, effective)
}

private const val BOM_MAX_BYTES = 3
