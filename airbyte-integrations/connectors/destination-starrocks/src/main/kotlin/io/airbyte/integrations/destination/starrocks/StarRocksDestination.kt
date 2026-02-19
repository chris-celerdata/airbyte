/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks

import io.airbyte.cdk.AirbyteDestinationRunner

/**
 * StarRocks destination connector entry point.
 *
 * This connector uses the Stream Load API for high-performance bulk loading
 * and supports both typed (schema-based) and raw (JSON blob) modes.
 */
class StarRocksDestination {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            AirbyteDestinationRunner.run(*args)
        }
    }
}
