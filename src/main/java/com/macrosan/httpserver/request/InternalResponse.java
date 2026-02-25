package com.macrosan.httpserver.request;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.impl.headers.VertxHttpHeaders;

public class InternalResponse implements HttpServerResponse {
    protected Handler<Throwable> exceptionHandler;
    protected int statusCode = 200;
    protected String statusMessage;
    protected boolean chunked = false;
    protected VertxHttpHeaders headers = new VertxHttpHeaders();

    boolean end = false;
    Handler<Buffer> responseHandler;
    Handler<Void> responseEndHandler;

    Handler<Void> headersEndHandler;
    Handler<Void> bodyEndHandler;
    Handler<Void> closeHandler;
    Handler<Void> endHandler;
    long bytesWritten = 0L;
    boolean headWritten = false;

    public InternalResponse(Handler<Buffer> responseHandler, Handler<Void> responseEndHandler) {
        this.responseHandler = responseHandler;
        this.responseEndHandler = responseEndHandler;
    }

    @Override
    public HttpServerResponse write(Buffer data, Handler<AsyncResult<Void>> handler) {
        if (!headWritten) {
            if (null != headersEndHandler) {
                headersEndHandler.handle(null);
            }

            headWritten = true;
        }
        responseHandler.handle(data);
        bytesWritten += data.length();
        if (null != handler) {
            handler.handle(null);
        }
        return this;
    }

    @Override
    public HttpServerResponse closeHandler(@Nullable Handler<Void> handler) {
        this.closeHandler = handler;
        return this;
    }

    @Override
    public HttpServerResponse endHandler(@Nullable Handler<Void> handler) {
        this.endHandler = handler;
        return this;
    }

    @Override
    public void end(Buffer chunk, Handler<AsyncResult<Void>> handler) {
        write(chunk);
        end = true;
        if (null != bodyEndHandler) {
            bodyEndHandler.handle(null);
        }

        if (null != endHandler) {
            endHandler.handle(null);
        }
        close();
        if (null != handler) {
            handler.handle(null);
        }
    }

    @Override
    public void close() {
        if (null != closeHandler) {
            closeHandler.handle(null);
        }

        responseEndHandler.handle(null);
    }

    @Override
    public boolean ended() {
        return end;
    }

    @Override
    public boolean closed() {
        return end;
    }

    @Override
    public boolean headWritten() {
        return headWritten;
    }

    @Override
    public HttpServerResponse headersEndHandler(@Nullable Handler<Void> handler) {
        headersEndHandler = handler;
        return this;
    }

    @Override
    public HttpServerResponse bodyEndHandler(@Nullable Handler<Void> handler) {
        bodyEndHandler = handler;
        return this;
    }

    @Override
    public long bytesWritten() {
        return bytesWritten;
    }

    @Override
    public HttpServerResponse exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }

    @Override
    public HttpServerResponse write(Buffer data) {
        return write(data, null);
    }

    @Override
    public int getStatusCode() {
        return statusCode;
    }

    @Override
    public HttpServerResponse setStatusCode(int statusCode) {
        this.statusCode = statusCode;
        return this;
    }

    @Override
    public String getStatusMessage() {
        return statusMessage;
    }

    @Override
    public HttpServerResponse setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
        return this;
    }

    @Override
    public HttpServerResponse setChunked(boolean chunked) {
        this.chunked = chunked;
        return this;
    }

    @Override
    public boolean isChunked() {
        return chunked;
    }

    @Override
    public MultiMap headers() {
        return headers;
    }

    @Override
    public HttpServerResponse putHeader(String name, String value) {
        headers.set(name, value);
        return this;
    }

    @Override
    public HttpServerResponse putHeader(CharSequence name, CharSequence value) {
        headers.set(name, value);
        return this;
    }

    @Override
    public HttpServerResponse putHeader(String name, Iterable<String> values) {
        headers.set(name, values);
        return this;
    }

    @Override
    public HttpServerResponse putHeader(CharSequence name, Iterable<CharSequence> values) {
        headers.set(name, values);
        return this;
    }

    @Override
    public HttpServerResponse putTrailer(String name, String value) {
        trailers().set(name, value);
        return this;
    }

    @Override
    public HttpServerResponse putTrailer(CharSequence name, CharSequence value) {
        trailers().set(name, value);
        return this;
    }

    @Override
    public HttpServerResponse putTrailer(String name, Iterable<String> values) {
        trailers().set(name, values);
        return this;
    }

    @Override
    public HttpServerResponse putTrailer(CharSequence name, Iterable<CharSequence> value) {
        trailers().set(name, value);
        return this;
    }

    @Override
    public HttpServerResponse write(String chunk, String enc) {
        return write(Buffer.buffer(chunk, enc), null);
    }

    @Override
    public HttpServerResponse write(String chunk, String enc, Handler<AsyncResult<Void>> handler) {
        return write(Buffer.buffer(chunk, enc), handler);
    }

    @Override
    public HttpServerResponse write(String chunk) {
        return write(Buffer.buffer(chunk));
    }

    @Override
    public HttpServerResponse write(String chunk, Handler<AsyncResult<Void>> handler) {
        return write(Buffer.buffer(chunk), handler);
    }

    @Override
    public HttpServerResponse writeContinue() {
        write("100 Continue\r\n");
        return this;
    }

    @Override
    public void end(String chunk) {
        end(Buffer.buffer(chunk));
    }

    @Override
    public void end(String chunk, Handler<AsyncResult<Void>> handler) {
        end(Buffer.buffer(chunk), handler);
    }

    @Override
    public void end(String chunk, String enc) {
        end(Buffer.buffer(chunk, enc));
    }

    @Override
    public void end(String chunk, String enc, Handler<AsyncResult<Void>> handler) {
        end(Buffer.buffer(chunk, enc), handler);
    }

    @Override
    public void end(Buffer chunk) {
        end(chunk, null);
    }

    @Override
    public void end() {
        end(Buffer.buffer(0));
    }

    @Override
    public void end(Handler<AsyncResult<Void>> handler) {
        end(Buffer.buffer(0), handler);
    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public HttpServerResponse sendFile(String filename, long offset, long length) {
        throw new UnsupportedOperationException("sendFile");
    }

    @Override
    public HttpServerResponse sendFile(String filename, long offset, long length, Handler<AsyncResult<Void>> resultHandler) {
        throw new UnsupportedOperationException("sendFile");
    }

    @Override
    public int streamId() {
        return -1;
    }

    @Override
    public void reset(long code) {
    }

    @Override
    public HttpServerResponse push(HttpMethod method, String path, MultiMap headers, Handler<AsyncResult<HttpServerResponse>> handler) {
        return push(method, null, path, headers, handler);
    }

    @Override
    public HttpServerResponse push(HttpMethod method, String host, String path, Handler<AsyncResult<HttpServerResponse>> handler) {
        return push(method, path, handler);
    }

    @Override
    public HttpServerResponse push(HttpMethod method, String path, Handler<AsyncResult<HttpServerResponse>> handler) {
        return push(method, path, null, null, handler);
    }

    @Override
    public HttpServerResponse push(HttpMethod method, String host, String path, MultiMap headers, Handler<AsyncResult<HttpServerResponse>> handler) {
        handler.handle(Future.failedFuture("Push promise is only supported with HTTP2"));
        return this;
    }

    @Override
    public HttpServerResponse writeCustomFrame(int type, int flags, Buffer payload) {
        return this;
    }

    @Override
    public HttpServerResponse setWriteQueueMaxSize(int maxSize) {
        throw new UnsupportedOperationException("setWriteQueueMaxSize");
    }

    @Override
    public HttpServerResponse drainHandler(Handler<Void> handler) {
        throw new UnsupportedOperationException("drainHandler");
    }

    @Override
    public MultiMap trailers() {
        throw new UnsupportedOperationException("trailers");
    }
}
