/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.data.ArrayType
import io.airbyte.cdk.load.data.BooleanType
import io.airbyte.cdk.load.data.DateType
import io.airbyte.cdk.load.data.IntegerType
import io.airbyte.cdk.load.data.NumberType
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.data.TimestampTypeWithTimezone
import io.airbyte.cdk.load.data.TimestampTypeWithoutTimezone
import io.airbyte.cdk.load.write.StreamLoader
import io.github.oshai.kotlinlogging.KotlinLogging
import java.sql.DriverManager

/**
 * Per-stream lifecycle handler:
 * - Creates the destination table in start()
 * - Registers the stream in the DirectLoaderFactory so loaders can access full schema info
 */
class StarRocksStreamLoader(
    override val stream: DestinationStream,
    private val config: StarRocksConfiguration,
    private val loaderFactory: StarRocksDirectLoaderFactory,
) : StreamLoader {

    private val log = KotlinLogging.logger {}
    private val tableName: String = stream.mappedDescriptor.name

    override suspend fun start() {
        // Make stream metadata available to DirectLoader instances
        loaderFactory.streamRegistry[stream.mappedDescriptor] = stream

        log.info { "Ensuring table `$tableName` exists in `${config.database}`" }
        DriverManager.getConnection(config.jdbcUrl, config.username, config.password).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(buildCreateTableDdl())
            }
        }
    }

    override suspend fun teardown(completedSuccessfully: Boolean) {
        log.info { "Stream `$tableName` teardown (success=$completedSuccessfully)" }
    }

    private fun buildCreateTableDdl(): String = when (config.loadingMode) {
        StarRocksSpecification.LoadingMode.TYPED -> buildTypedTableDdl()
        StarRocksSpecification.LoadingMode.RAW -> buildRawTableDdl()
    }

    private fun buildTypedTableDdl(): String {
        val schema = stream.schema
        val schemaColumns = if (schema is ObjectType) {
            schema.properties.entries.joinToString(",\n    ") { (name, fieldType) ->
                val colName = sanitize(name)
                val colType = toStarRocksType(fieldType.type)
                "`$colName` $colType NULL"
            }
        } else ""

        val allColumns = if (schemaColumns.isNotEmpty()) "$schemaColumns,\n    " else ""
        val keyCol = if (schema is ObjectType && schema.properties.isNotEmpty()) {
            "`${sanitize(schema.properties.keys.first())}`"
        } else "`_airbyte_ab_id`"

        return """
            CREATE TABLE IF NOT EXISTS `${config.database}`.`$tableName` (
                ${allColumns}`_airbyte_ab_id` VARCHAR(36) NULL,
                `_airbyte_emitted_at` DATETIME NULL
            )
            DUPLICATE KEY($keyCol)
            DISTRIBUTED BY HASH($keyCol)
            PROPERTIES ("replication_num" = "1")
        """.trimIndent()
    }

    private fun buildRawTableDdl() = """
        CREATE TABLE IF NOT EXISTS `${config.database}`.`$tableName` (
            `_airbyte_ab_id` VARCHAR(36) NULL,
            `_airbyte_emitted_at` DATETIME NULL,
            `_airbyte_data` JSON NULL
        )
        DUPLICATE KEY(`_airbyte_ab_id`)
        DISTRIBUTED BY HASH(`_airbyte_ab_id`)
        PROPERTIES ("replication_num" = "1")
    """.trimIndent()

    private fun toStarRocksType(type: io.airbyte.cdk.load.data.AirbyteType): String = when (type) {
        is IntegerType -> "BIGINT"
        is NumberType -> "DOUBLE"
        is BooleanType -> "BOOLEAN"
        is DateType -> "DATE"
        is TimestampTypeWithTimezone, is TimestampTypeWithoutTimezone -> "DATETIME"
        is ArrayType, is ObjectType -> "JSON"
        else -> "STRING"
    }

    private fun sanitize(name: String) = name.replace("-", "_").replace(" ", "_").replace(".", "_")
}
