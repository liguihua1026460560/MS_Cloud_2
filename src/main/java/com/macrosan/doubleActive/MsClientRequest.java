package com.macrosan.doubleActive;

import io.vertx.core.Handler;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.reactivex.core.http.HttpClientRequest;
import lombok.extern.log4j.Log4j2;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
public class MsClientRequest extends HttpClientRequest {
    private HttpClientRequest httpClientRequest;

    public MsClientRequest(HttpClientRequest httpClientRequest) {
        super(httpClientRequest.getDelegate());
        this.httpClientRequest = httpClientRequest;
    }

    private final Set<Handler<Void>> responseCloseHandler = new ConcurrentHashSet<>();
    private AtomicBoolean setCloseHandler = new AtomicBoolean(false);

    AtomicBoolean isCLosed = new AtomicBoolean(false);

    private Handler<Void> closeHandler = v -> {
        isCLosed.compareAndSet(false, true);
        for (Handler<Void> handler : responseCloseHandler) {
            try {
                handler.handle(v);
            } catch (Exception e) {
                log.error("failed to exec response close handler, ", e);
            }
        }

    };

    public void addResponseCloseHandler(Handler<Void> handler) {
        try {
            if (httpClientRequest.getDelegate().connection() == null || isCLosed.get()) {
                handler.handle(null);
                return;
            }
            responseCloseHandler.add(handler);

            if (setCloseHandler.compareAndSet(false, true)) {
                if (httpClientRequest.getDelegate().connection() != null || !isCLosed.get()) {
                    httpClientRequest.getDelegate().connection().closeHandler(closeHandler);
                }
            }
        } catch (Exception e) {
            log.error("failed to add response close handler, ", e);
            handler.handle(null);
        }
    }

    public void removeResponseCloseHandler(Handler<Void> handler) {
        try {
            if (null == handler) {
                return;
            }

            if (httpClientRequest.getDelegate().connection() == null || isCLosed.get()) {
                return;
            }


            responseCloseHandler.remove(handler);
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
