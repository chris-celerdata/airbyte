/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks

import io.airbyte.cdk.load.command.DestinationConfiguration
import io.airbyte.cdk.load.command.DestinationConfigurationFactory
import io.micronaut.context.annotation.Factory
import jakarta.inject.Singleton

data class StarRocksConfiguration(
    val host: String,
    val port: Int,
    val httpPort: Int,
    val username: String,
    val password: String,
    val database: String,
    val ssl: Boolean,
    val loadingMode: StarRocksSpecification.LoadingMode,
    val flushBatchSize: Int = 50_000,
) : DestinationConfiguration() {
    val jdbcUrl: String
        get() {
            val base = "jdbc:mysql://$host:$port/$database"
            return if (ssl) "$base?useSSL=true&requireSSL=true" else base
        }

    /** HTTPS on 443 for cloud clusters, HTTP on httpPort otherwise. */
    val streamLoadBaseUrl: String
        get() = "https://$host:443"

    val fallbackStreamLoadUrl: String
        get() = "http://$host:$httpPort"
}

@Factory
class StarRocksConfigurationProvider(private val config: DestinationConfiguration) {
    @Singleton
    fun get(): StarRocksConfiguration = config as StarRocksConfiguration
}

@Singleton
class StarRocksConfigurationFactory :
    DestinationConfigurationFactory<StarRocksSpecification, StarRocksConfiguration> {
    override fun makeWithoutExceptionHandling(pojo: StarRocksSpecification) =
        StarRocksConfiguration(
            host = pojo.host,
            port = pojo.port,
            httpPort = pojo.httpPort,
            username = pojo.username,
            password = pojo.password,
            database = pojo.database,
            ssl = pojo.ssl,
            loadingMode = pojo.loadingMode,
        )
}
