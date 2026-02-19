/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks

import io.airbyte.cdk.load.check.DestinationChecker
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import okhttp3.Credentials
import okhttp3.Request
import java.sql.DriverManager
import java.util.UUID

@Singleton
class StarRocksChecker : DestinationChecker<StarRocksConfiguration> {

    private val log = KotlinLogging.logger {}

    override fun check(config: StarRocksConfiguration) {
        checkMySQLConnection(config)
        checkStreamLoadApi(config)
    }

    private fun checkMySQLConnection(config: StarRocksConfiguration) {
        DriverManager.getConnection(config.jdbcUrl, config.username, config.password).use { conn ->
            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("SELECT 1")
                check(rs.next() && rs.getInt(1) == 1) { "Unexpected result from SELECT 1" }
            }

            conn.createStatement().use { stmt ->
                val rs = stmt.executeQuery("SHOW DATABASES")
                val databases = generateSequence { if (rs.next()) rs.getString(1) else null }.toSet()
                if (config.database !in databases) {
                    stmt.execute("CREATE DATABASE IF NOT EXISTS `${config.database}`")
                    log.info { "Created database ${config.database}" }
                }
            }

            val testTable = "_airbyte_connection_test_${UUID.randomUUID().toString().take(8)}"
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE `${config.database}`.`$testTable` (id INT)")
                stmt.execute("DROP TABLE `${config.database}`.`$testTable`")
            }
            log.info { "Write permissions verified on ${config.database}" }
        }
    }

    private fun checkStreamLoadApi(config: StarRocksConfiguration) {
        val client = buildOkHttpClient(config.ssl)
        val urls = listOf(config.streamLoadBaseUrl, config.fallbackStreamLoadUrl)

        for (url in urls) {
            try {
                val req = Request.Builder()
                    .url(url)
                    .header("Authorization", Credentials.basic(config.username, config.password))
                    .build()
                client.newCall(req).execute().use { resp ->
                    // Any HTTP response means the endpoint is reachable at the network level.
                    // A 4xx (e.g. 401/403) still confirms TCP connectivity — only a thrown
                    // exception means the host is unreachable.
                    log.info { "Stream Load API accessible at $url (status: ${resp.code})" }
                    return
                }
            } catch (_: Exception) {}
        }
        throw RuntimeException(
            "Stream Load API not accessible at $urls. " +
                "Ensure port 443 (HTTPS) or ${config.httpPort} (HTTP) is reachable."
        )
    }
}
