/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import io.airbyte.cdk.command.ConfigurationSpecification
import io.airbyte.protocol.models.v0.DestinationSyncMode
import jakarta.inject.Singleton

@Singleton
@JsonSchemaTitle("StarRocks Destination Spec")
class StarRocksSpecification : ConfigurationSpecification() {

    @JsonProperty("host")
    @JsonPropertyDescription("StarRocks FE hostname")
    @get:JsonSchemaInject(json = """{"order":0}""")
    var host: String = ""

    @JsonProperty("port")
    @JsonPropertyDescription("MySQL protocol port (default: 9030)")
    @get:JsonSchemaInject(json = """{"order":1,"default":9030}""")
    var port: Int = 9030

    @JsonProperty("http_port")
    @JsonPropertyDescription("HTTP port for Stream Load API (default: 8030)")
    @get:JsonSchemaInject(json = """{"order":2,"default":8030}""")
    var httpPort: Int = 8030

    @JsonProperty("username")
    @JsonPropertyDescription("Authentication username")
    @get:JsonSchemaInject(json = """{"order":3}""")
    var username: String = ""

    @JsonProperty("password")
    @JsonPropertyDescription("Authentication password")
    @get:JsonSchemaInject(json = """{"airbyte_secret":true,"always_show":true,"order":4}""")
    var password: String = ""

    @JsonProperty("database")
    @JsonPropertyDescription("Target database name")
    @get:JsonSchemaInject(json = """{"order":5}""")
    var database: String = ""

    @JsonProperty("ssl")
    @JsonPropertyDescription("Use SSL/TLS encryption for connections")
    @get:JsonSchemaInject(json = """{"order":6,"default":false}""")
    var ssl: Boolean = false

    @JsonProperty("loading_mode")
    @JsonPropertyDescription("Choose how to load data into StarRocks")
    @get:JsonSchemaInject(json = """{"order":7}""")
    var loadingMode: LoadingMode = LoadingMode.TYPED

    enum class LoadingMode {
        @JsonProperty("typed")
        TYPED,

        @JsonProperty("raw")
        RAW
    }
}

@Singleton
class StarRocksSpecificationExtension : io.airbyte.cdk.load.spec.DestinationSpecificationExtension {
    override val supportedSyncModes = listOf(
        DestinationSyncMode.OVERWRITE,
        DestinationSyncMode.APPEND
    )
    override val supportsIncremental = true
}
