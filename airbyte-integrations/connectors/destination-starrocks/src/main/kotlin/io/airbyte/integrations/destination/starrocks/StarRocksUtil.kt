/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks

import okhttp3.OkHttpClient
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl.SSLContext
import javax.net.ssl.X509TrustManager

/** Accepts all certificates – used for cloud StarRocks clusters with self-signed certs. */
class TrustAllX509Manager : X509TrustManager {
    override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String) {}
    override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String) {}
    override fun getAcceptedIssuers(): Array<X509Certificate> = arrayOf()
}

fun buildOkHttpClient(ssl: Boolean): OkHttpClient {
    if (!ssl) return OkHttpClient()
    val trustAll = TrustAllX509Manager()
    val sslCtx = SSLContext.getInstance("TLS").also { it.init(null, arrayOf(trustAll), SecureRandom()) }
    return OkHttpClient.Builder()
        .sslSocketFactory(sslCtx.socketFactory, trustAll)
        .hostnameVerifier { _, _ -> true }
        .build()
}
