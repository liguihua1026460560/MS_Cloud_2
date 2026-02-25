package com.macrosan.httpserver.request;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.*;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.cert.X509Certificate;

public abstract class InternalRequest implements HttpServerRequest {
    @Override
    public HttpVersion version() {
        return HttpVersion.HTTP_1_1;
    }

    @Override
    public String rawMethod() {
        return method().toString();
    }

    @Override
    public boolean isSSL() {
        return false;
    }

    @Override
    public HttpServerRequest setExpectMultipart(boolean expect) {
        return this;
    }

    @Override
    public boolean isExpectMultipart() {
        return true;
    }

    @Override
    public @Nullable String scheme() {
        throw new UnsupportedOperationException("scheme");
    }

    @Override
    public @Nullable String query() {
        throw new UnsupportedOperationException("query");
    }

    @Override
    public SocketAddress remoteAddress() {
        return SocketAddress.inetSocketAddress(80,"127.0.0.1");
    }

    @Override
    public SocketAddress localAddress() {
        throw new UnsupportedOperationException("localAddress");
    }

    @Override
    public SSLSession sslSession() {
        throw new UnsupportedOperationException("sslSession");
    }

    @Override
    public X509Certificate[] peerCertificateChain() throws SSLPeerUnverifiedException {
        throw new UnsupportedOperationException("peerCertificateChain");
    }

    @Override
    public NetSocket netSocket() {
        throw new UnsupportedOperationException("netSocket");
    }

    @Override
    public HttpServerRequest uploadHandler(@Nullable Handler<HttpServerFileUpload> uploadHandler) {
        throw new UnsupportedOperationException("uploadHandler");
    }

    @Override
    public MultiMap formAttributes() {
        throw new UnsupportedOperationException("formAttributes");
    }

    @Override
    public @Nullable String getFormAttribute(String attributeName) {
        throw new UnsupportedOperationException("getFormAttribute");
    }

    @Override
    public ServerWebSocket upgrade() {
        throw new UnsupportedOperationException("upgrade");
    }

    @Override
    public HttpServerRequest customFrameHandler(Handler<HttpFrame> handler) {
        throw new UnsupportedOperationException("customFrameHandler");
    }

    @Override
    public HttpConnection connection() {
        return new InternalConnection();
    }

    @Override
    public HttpServerRequest streamPriorityHandler(Handler<StreamPriority> handler) {
        throw new UnsupportedOperationException("streamPriorityHandler");
    }
}
