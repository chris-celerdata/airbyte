/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.message.DestinationRecordRaw
import io.airbyte.cdk.load.write.DirectLoader
import io.airbyte.cdk.load.write.DirectLoaderFactory
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import okhttp3.Credentials
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Accumulates records and flushes to StarRocks via a direct OkHttp PUT (Stream Load).
 *
 * OkHttp negotiates HTTP/2 over TLS automatically (ALPN), which avoids the
 * Expect: 100-continue handshake that Apache HttpClient 4.x uses.  On CelerData
 * Cloud this makes a ~10x throughput difference (4.8 MB/s vs 0.5 MB/s).
 *
 * OkHttp also follows 307 redirects (FE → BE) automatically, handles TLS, and
 * reuses connections via its internal pool.
 */
class StarRocksDirectLoader(
    private val config: StarRocksConfiguration,
    private val stream: DestinationStream,
    private val httpClient: OkHttpClient,
    private val streamLoadBaseUrl: String,
) : DirectLoader {

    private val log = KotlinLogging.logger {}
    private val mapper = ObjectMapper()
    private val tableName = stream.mappedDescriptor.name

    private val maxBatchBytes = 100L * 1024 * 1024  // 100 MB per CDK checkpoint batch
    private var currentBytes = 0L
    private val rows = mutableListOf<String>()

    private val columnNames: List<String> = buildColumnNames()

    override suspend fun accept(record: DestinationRecordRaw): DirectLoader.DirectLoadResult {
        rows.add(convertToCsvRow(record))
        currentBytes += record.serializedSizeBytes

        if (currentBytes >= maxBatchBytes) {
            flush()
            return DirectLoader.Complete
        }
        return DirectLoader.Incomplete
    }

    override suspend fun finish() {
        if (rows.isNotEmpty()) flush()
    }

    override fun close() {
        // httpClient is shared; nothing to close here
    }

    private fun flush() {
        if (rows.isEmpty()) return

        val csv = rows.joinToString("\n")
        val label = UUID.randomUUID().toString()

        val request = Request.Builder()
            .url("$streamLoadBaseUrl/api/${config.database}/$tableName/_stream_load")
            .put(csv.toRequestBody("text/plain; charset=utf-8".toMediaType()))
            .header("Authorization", Credentials.basic(config.username, config.password))
            .header("label", label)
            .header("column_separator", ",")
            .header("enclose", "\"")
            .header("escape", "\\")
            .header("columns", columnNames.joinToString(","))
            .build()

        log.info {
            "Stream Load PUT: db=${config.database}, table=$tableName, " +
                "rows=${rows.size}, bytes=${currentBytes}, label=$label"
        }

        httpClient.newCall(request).execute().use { response ->
            val body = response.body?.string() ?: ""
            check(response.isSuccessful) {
                "Stream Load failed (HTTP ${response.code}) for label=$label: $body"
            }
            log.info { "Stream Load committed: label=$label, response=$body" }
        }

        rows.clear()
        currentBytes = 0L
    }

    private fun convertToCsvRow(record: DestinationRecordRaw): String {
        val json = record.asJsonRecord()
        val values: List<Any?> = when (config.loadingMode) {
            StarRocksSpecification.LoadingMode.TYPED -> {
                val row = mutableMapOf<String, Any?>()
                json.fields().forEach { (key, value) ->
                    val colName = sanitize(key)
                    row[colName] = when {
                        value.isNull -> null
                        value.isBoolean -> value.asBoolean()
                        value.isInt -> value.asInt()
                        value.isLong -> value.asLong()
                        value.isDouble -> value.asDouble()
                        value.isTextual -> value.asText()
                        value.isObject || value.isArray -> mapper.writeValueAsString(value)
                        else -> value.toString()
                    }
                }
                row["_airbyte_ab_id"] = record.airbyteRawId.toString()
                row["_airbyte_emitted_at"] = Instant.ofEpochMilli(record.rawData.emittedAtMs).toString()
                columnNames.map { row[it] }
            }
            StarRocksSpecification.LoadingMode.RAW -> listOf(
                record.airbyteRawId.toString(),
                Instant.ofEpochMilli(record.rawData.emittedAtMs).toString(),
                mapper.writeValueAsString(json),
            )
        }
        return values.joinToString(",") { v ->
            when (v) {
                null -> ""
                else -> "\"${v.toString().replace("\\", "\\\\").replace("\"", "\\\"")}\""
            }
        }
    }

    private fun buildColumnNames(): List<String> = when (config.loadingMode) {
        StarRocksSpecification.LoadingMode.TYPED -> {
            val schema = stream.schema
            val schemaColumns = if (schema is ObjectType) schema.properties.keys.map { sanitize(it) } else emptyList()
            schemaColumns + listOf("_airbyte_ab_id", "_airbyte_emitted_at")
        }
        StarRocksSpecification.LoadingMode.RAW ->
            listOf("_airbyte_ab_id", "_airbyte_emitted_at", "_airbyte_data")
    }

    private fun sanitize(name: String) = name.replace("-", "_").replace(" ", "_").replace(".", "_")
}

@Singleton
class StarRocksDirectLoaderFactory(
    private val config: StarRocksConfiguration,
) : DirectLoaderFactory<StarRocksDirectLoader> {

    private val log = KotlinLogging.logger {}

    private val streamLoadBaseUrl: String =
        if (config.ssl) "https://${config.host}:443" else "http://${config.host}:${config.httpPort}"

    /**
     * Shared OkHttp client — reuses TLS sessions and connection pool across all batches.
     * OkHttp negotiates HTTP/2 automatically (ALPN), which avoids the Expect: 100-continue
     * overhead that Apache HttpClient 4.x incurs on CelerData Cloud's HTTPS proxy.
     */
    private val httpClient: OkHttpClient = buildOkHttpClient(config.ssl)
        .newBuilder()
        .connectTimeout(30, TimeUnit.SECONDS)
        .readTimeout(600, TimeUnit.SECONDS)
        .writeTimeout(600, TimeUnit.SECONDS)
        .build()

    override val inputPartitions: Int = 1

    override fun create(streamDescriptor: DestinationStream.Descriptor, part: Int): StarRocksDirectLoader {
        log.info { "Creating StarRocks loader for stream ${streamDescriptor.name}, part=$part" }
        val stream = streamRegistry[streamDescriptor]
            ?: throw IllegalStateException("No stream registered for $streamDescriptor")
        return StarRocksDirectLoader(config, stream, httpClient, streamLoadBaseUrl)
    }

    internal val streamRegistry = mutableMapOf<DestinationStream.Descriptor, DestinationStream>()
}
