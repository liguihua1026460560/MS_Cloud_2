package com.macrosan.httpserver.request;

import com.macrosan.action.datastream.StreamService;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.RestfulVerticle;
import com.macrosan.httpserver.ServerConfig;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.impl.headers.VertxHttpHeaders;
import lombok.extern.log4j.Log4j2;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.net.URLEncoder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.ServerConstants.NO_SYNCHRONIZATION_KEY;
import static com.macrosan.constants.ServerConstants.NO_SYNCHRONIZATION_VALUE;

@Log4j2
public class InternalPutRequest extends InternalRequest {
    InternalResponse response;
    Handler<Buffer> handler;
    Handler<Void> endHandler;
    boolean requested = false;
    byte[] data;
    VertxHttpHeaders headers = new VertxHttpHeaders();
    boolean end = false;
    VertxHttpHeaders param = new VertxHttpHeaders();
    String uri;
    AtomicBoolean handled = new AtomicBoolean(false);
    public static Mono<Boolean> putObject(String userId, String bucket, String object, byte[] data, String acl) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        InternalPutRequest[] request = new InternalPutRequest[]{null};
        object = URLEncoder.encode(object);

        try {
            request[0] = new InternalPutRequest(bucket, object, data, acl,
                    v -> {
                        try {
                            if (request[0].response.statusCode == 200) {
                                res.onNext(true);
                            } else {
                                res.onNext(false);
                            }
                        } catch (Exception e) {
                            log.error("put object internal fail", e);
                            res.onNext(false);
                        }
                    });
            MsHttpRequest request0 = new MsHttpRequest(request[0]);

            request0.setCodec("utf-8");
            request0.setUserId(userId);
            request0.addMember("requestType","putLog");
            RestfulVerticle.getRequestSign(request0, "");
            StreamService.getInstance().putObject(request0);
        } catch (Exception e) {
            log.error("put object internal fail", e);
            res.onNext(false);
        }

        Disposable disposable = ErasureServer.DISK_SCHEDULER.schedule(() -> {
            try {
                request[0].response.end();
            } catch (Exception e) {
                log.error("timeout internal put object request fail", e);
            }
            res.onNext(false);
        }, 300, TimeUnit.SECONDS);


        return res.doOnNext(b -> {
            disposable.dispose();
        });
    }

    private InternalPutRequest(String bucket, String object, byte[] data, String acl, Handler<Void> endHandler) {
        uri = "/" + bucket + "/" + object;
        this.data = data;
        headers.set("Content-Length", "" + data.length);
        headers.set("x-amz-acl",acl);
        headers.set("Content-Type","binary/octet-stream");
        headers.set(NO_SYNCHRONIZATION_KEY, NO_SYNCHRONIZATION_VALUE);
        response = new InternalResponse(b -> {
        }, endHandler);
    }

    private void run() {
        if (handler != null && endHandler != null && requested) {
            // 一次性就把所有数据下发给下游了，确保只执行一次
            if (handled.compareAndSet(false, true)) {
                ServerConfig.getInstance().getVertx().runOnContext(v -> {
                    handler.handle(Buffer.buffer(data));
                    end = true;
                    endHandler.handle(null);
                });
            }
        }
    }

    @Override
    public HttpServerRequest exceptionHandler(Handler<Throwable> handler) {
        return this;
    }

    @Override
    public HttpServerRequest handler(Handler<Buffer> handler) {
        this.handler = handler;
        run();
        return this;
    }

    @Override
    public HttpServerRequest pause() {
        requested = true;
        return this;
    }

    @Override
    public HttpServerRequest resume() {
        requested = true;
        run();
        return this;
    }

    @Override
    public HttpServerRequest fetch(long amount) {
        requested = true;
        run();
        return this;
    }

    @Override
    public HttpServerRequest endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        run();
        return this;
    }


    @Override
    public HttpMethod method() {
        return HttpMethod.PUT;
    }

    @Override
    public String uri() {
        return uri;
    }

    @Override
    public @Nullable String path() {
        return uri;
    }

    @Override
    public @Nullable String host() {
        return "127.0.0.1";
    }

    @Override
    public long bytesRead() {
        return end ? data.length : 0;
    }

    @Override
    public boolean isEnded() {
        return end;
    }

    @Override
    public HttpServerResponse response() {
        return response;
    }

    @Override
    public MultiMap headers() {
        return headers;
    }

    @Override
    public @Nullable String getHeader(String headerName) {
        return headers.get(headerName);
    }

    @Override
    public String getHeader(CharSequence headerName) {
        return headers.get(headerName);
    }

    @Override
    public MultiMap params() {
        return param;
    }

    @Override
    public @Nullable String getParam(String paramName) {
        return param.get(paramName);
    }

    @Override
    public String absoluteURI() {
        return "http://127.0.0.1" + uri();
    }

}
