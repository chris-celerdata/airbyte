/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.state.DestinationFailure
import io.airbyte.cdk.load.write.DestinationWriter
import io.airbyte.cdk.load.write.StreamLoader
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

@Singleton
class StarRocksWriter(
    private val config: StarRocksConfiguration,
    private val loaderFactory: StarRocksDirectLoaderFactory,
) : DestinationWriter {

    private val log = KotlinLogging.logger {}

    override suspend fun setup() {
        log.info { "StarRocks destination setup (host=${config.host}, database=${config.database})" }
    }

    override fun createStreamLoader(stream: DestinationStream): StreamLoader =
        StarRocksStreamLoader(stream, config, loaderFactory)

    override suspend fun teardown(destinationFailure: DestinationFailure?) {
        log.info { "StarRocks destination teardown (failure=${destinationFailure != null})" }
    }
}
