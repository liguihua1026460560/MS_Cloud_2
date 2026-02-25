package com.macrosan.httpserver.request;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.GoAway;
import io.vertx.core.http.Http2Settings;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.net.SocketAddress;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.cert.X509Certificate;

public class InternalConnection implements HttpConnection {
    @Override
    public HttpConnection goAway(long errorCode, int lastStreamId, Buffer debugData) {
        return this;
    }

    @Override
    public HttpConnection goAwayHandler(@Nullable Handler<GoAway> handler) {
        return this;
    }

    @Override
    public HttpConnection shutdownHandler(@Nullable Handler<Void> handler) {
        return this;
    }

    @Override
    public HttpConnection shutdown() {
        return this;
    }

    @Override
    public HttpConnection shutdown(long timeoutMs) {
        return this;
    }

    @Override
    public HttpConnection closeHandler(Handler<Void> handler) {
        return this;
    }

    @Override
    public void close() {

    }

    @Override
    public Http2Settings settings() {
        return null;
    }

    @Override
    public HttpConnection updateSettings(Http2Settings settings) {
        return this;
    }

    @Override
    public HttpConnection updateSettings(Http2Settings settings, Handler<AsyncResult<Void>> completionHandler) {
        return this;
    }

    @Override
    public Http2Settings remoteSettings() {
        return null;
    }

    @Override
    public HttpConnection remoteSettingsHandler(Handler<Http2Settings> handler) {
        return this;
    }

    @Override
    public HttpConnection ping(Buffer data, Handler<AsyncResult<Buffer>> pongHandler) {
        return this;
    }

    @Override
    public HttpConnection pingHandler(@Nullable Handler<Buffer> handler) {
        return this;
    }

    @Override
    public HttpConnection exceptionHandler(Handler<Throwable> handler) {
        return this;
    }

    @Override
    public SocketAddress remoteAddress() {
        return null;
    }

    @Override
    public SocketAddress localAddress() {
        return null;
    }

    @Override
    public boolean isSsl() {
        return false;
    }

    @Override
    public SSLSession sslSession() {
        return null;
    }

    @Override
    public X509Certificate[] peerCertificateChain() throws SSLPeerUnverifiedException {
        return new X509Certificate[0];
    }

    @Override
    public String indicatedServerName() {
        return null;
    }
}
