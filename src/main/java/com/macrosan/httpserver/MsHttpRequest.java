package com.macrosan.httpserver;

import com.macrosan.message.consturct.Param;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.cert.X509Certificate;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MsHttpRequest
 * <p>
 * 对httpRequest的数据结构进行一定的优化，将一定会出现的变量改为成员变量，将可能会出现的变量放在Map中
 *
 * @author liyixin
 * @date 2019/5/10
 */
@Accessors(chain = true)
public class MsHttpRequest implements HttpServerRequest {

    private static final Logger logger = LogManager.getLogger(MsHttpRequest.class);

    @Getter
    @Setter
    private boolean tempUrlAccess = true;

    @Getter
    @Setter
    private HttpServerRequest delegate;

    @Getter
    @Setter
    private Context context;

    @Getter
    @Setter
    @Param(key = "_bucketname")
    private String bucketName;

    @Getter
    @Setter
    @Param(key = "_objectname")
    private String objectName;

    @Getter
    @Setter
    @Param(key = "_codec")
    private String codec;

    @Getter
    @Setter
    @Param(key = "_id")
    private String userId;

    @Getter
    @Setter
    @Param(key = "_uri")
    private String uri;

    @Getter
    @Setter
    @Param(key = "ak")
    private String accessKey;

    @Getter
    @Setter
    @Param(key = "userName")
    private String userName;

    @Getter
    @Setter
    @Param(key = "isSwift")
    private String isSwift;

    @Getter
    @Setter
    private int quickReturn = ServerConfig.getInstance().getQuickReturn();

    @Getter
    @Setter
    private String syncTag;

    private Map<String, String> members;

    @Getter
    @Setter
    private boolean subRequest;

    @Getter
    private final AtomicBoolean dataHandlerCompleted = new AtomicBoolean( false);

    public MsHttpRequest(HttpServerRequest delegate) {
        this.delegate = delegate;
    }

    public MsHttpRequest(HttpServerRequest delegate, boolean subRequest) {
        this.delegate = delegate;
        this.subRequest = subRequest;
    }

    public MsHttpRequest addMember(String key, String value) {
        if (members == null) {
            members = new UnifiedMap<>();
        }
        members.put(key, value);
        return this;
    }

    public String getMember(String key) {
        try {
            return members.get(key);
        } catch (NullPointerException e) {
            return null;
        }
    }

    public Map<String, String> getMembers() {
        return members;
    }

    public String getMember(String key, String def) {
        return members.getOrDefault(key, def);
    }

    @Override
    public HttpServerRequest exceptionHandler(Handler<Throwable> handler) {
        return delegate.exceptionHandler(handler);
    }

    @Override
    public HttpServerRequest handler(Handler<Buffer> handler) {
        return delegate.handler(handler);
    }

    @Override
    public HttpServerRequest pause() {
        return delegate.pause();
    }

    @Override
    public HttpServerRequest resume() {
        return delegate.resume();
    }

    @Override
    public HttpServerRequest fetch(long amount) {
        return delegate.fetch(amount);
    }

    @Override
    public HttpServerRequest endHandler(Handler<Void> endHandler) {
        return delegate.endHandler(endHandler);
    }

    @Override
    public HttpVersion version() {
        return delegate.version();
    }

    @Override
    public HttpMethod method() {
        return delegate.method();
    }

    @Override
    public String rawMethod() {
        return delegate.rawMethod();
    }

    @Override
    public boolean isSSL() {
        return delegate.isSSL();
    }

    @Override
    public @Nullable String scheme() {
        return delegate.scheme();
    }

    @Override
    public String uri() {
        return delegate.uri();
    }

    @Override
    public @Nullable String path() {
        return delegate.path();
    }

    @Override
    public @Nullable String query() {
        return delegate.query();
    }

    @Override
    public @Nullable String host() {
        return delegate.host();
    }

    @Override
    public long bytesRead() {
        return delegate.bytesRead();
    }

    @Override
    public HttpServerResponse response() {
        return delegate.response();
    }

    @Override
    public MultiMap headers() {
        return delegate.headers();
    }

    @Override
    public @Nullable String getHeader(String headerName) {
        return delegate.getHeader(headerName);
    }

    @Override
    public String getHeader(CharSequence headerName) {
        return delegate.getHeader(headerName);
    }

    @Override
    public MultiMap params() {
        return delegate.params();
    }

    @Override
    public @Nullable String getParam(String paramName) {
        return delegate.getParam(paramName);
    }

    public String getParam(String paramName, String defaultValue) {
        return delegate.params().contains(paramName) ? delegate.getParam(paramName) : defaultValue;
    }

    @Override
    public SocketAddress remoteAddress() {
        return delegate.remoteAddress();
    }

    @Override
    public SocketAddress localAddress() {
        return delegate.localAddress();
    }

    @Override
    public SSLSession sslSession() {
        return delegate.sslSession();
    }

    @Override
    public X509Certificate[] peerCertificateChain() throws SSLPeerUnverifiedException {
        return delegate.peerCertificateChain();
    }

    @Override
    public String absoluteURI() {
        return delegate.absoluteURI();
    }

    @Override
    public NetSocket netSocket() {
        return delegate.netSocket();
    }

    @Override
    public HttpServerRequest setExpectMultipart(boolean expect) {
        return delegate.setExpectMultipart(expect);
    }

    @Override
    public boolean isExpectMultipart() {
        return delegate.isExpectMultipart();
    }

    @Override
    public HttpServerRequest uploadHandler(@Nullable Handler<HttpServerFileUpload> uploadHandler) {
        return delegate.uploadHandler(uploadHandler);
    }

    @Override
    public MultiMap formAttributes() {
        return delegate.formAttributes();
    }

    @Override
    public @Nullable String getFormAttribute(String attributeName) {
        return delegate.getFormAttribute(attributeName);
    }

    @Override
    public ServerWebSocket upgrade() {
        return delegate.upgrade();
    }

    /**
     * 客户端断开时，该方法返回的是false。
     *
     * @return
     */
    @Override
    public boolean isEnded() {
        return delegate.isEnded();
    }

    @Override
    public HttpServerRequest customFrameHandler(Handler<HttpFrame> handler) {
        return delegate.customFrameHandler(handler);
    }

    @Override
    public HttpConnection connection() {
        return delegate.connection();
    }

    @Override
    public HttpServerRequest streamPriorityHandler(Handler<StreamPriority> handler) {
        return delegate.streamPriorityHandler(handler);
    }

    private final LinkedList<Handler<Void>> responseCloseHandler = new LinkedList<>();
    private AtomicBoolean setCloseHandler = new AtomicBoolean(false);

    private Handler<Void> closeHandler = v -> {
        for (Handler<Void> handler : responseCloseHandler) {
            try {
                handler.handle(v);
            } catch (Exception e) {
                logger.error("failed to exec response close handler, ", e);
            }
        }
    };

    public void shutdown() {
        synchronized (responseCloseHandler) {
            if (!responseCloseHandler.isEmpty()) {
                closeHandler.handle(null);
            }
            responseCloseHandler.clear();
        }
    }

    // subRequest 通常需要通过手动回调函数来释放相关资源
    public void addResponseCloseHandler(Handler<Void> handler) {
        boolean add = false;
        if (subRequest) {
            synchronized (responseCloseHandler) {
                responseCloseHandler.add(handler);
                add = true;
            }
        }

        if (delegate == null || delegate.response() == null) {
            return;
        }

        if (delegate.response().ended() || delegate.response().closed()) {
            handler.handle(null);
            return;
        }

        synchronized (responseCloseHandler) {
            if (!add) {
                responseCloseHandler.add(handler);
            }
        }

        if (setCloseHandler.compareAndSet(false, true)) {
            try {
                if (!delegate.response().ended()) {
                    delegate.response().closeHandler(closeHandler);
                }
            } catch (Exception e) {
                logger.error("failed to add response close handler, ", e);
                handler.handle(null);
                return;
            }
        }
    }

    public void removeResponseCloseHandler(Handler<Void> handler) {
        if (null == handler || delegate == null || delegate.response() == null) {
            return;
        }

        if (delegate.response().ended() || delegate.response().closed()) {
            return;
        }

        synchronized (responseCloseHandler) {
            responseCloseHandler.remove(handler);
        }
    }

    /**
     * 添加response().endHandler。当客户端连接断开，前面的closeHandler会晚于endHandler执行。
     */
    private final LinkedList<Handler<Void>> responseEndHandler = new LinkedList<>();
    private AtomicBoolean setEndHandler = new AtomicBoolean(false);

    private Handler<Void> endHandler = v -> {
        for (Handler<Void> handler : responseEndHandler) {
            try {
//                if (delegate.response().closed()){
//                    return;
//                }
                handler.handle(v);
            } catch (Exception e) {
                logger.error("failed to exec response close handler, ", e);
            }
        }
    };


    public void addResponseEndHandler(Handler<Void> handler) {
        if (delegate == null || delegate.response() == null) {
            return;
        }
        if (delegate.response().ended() || delegate.response().closed()) {
            handler.handle(null);
            return;
        }

        synchronized (responseEndHandler) {
            responseEndHandler.add(handler);
        }

        if (setEndHandler.compareAndSet(false, true)) {
            try {
                if (!delegate.response().ended()) {
                    delegate.response().endHandler(endHandler);
                }
            } catch (Exception e) {
                logger.error("failed to add response close handler, ", e);
                handler.handle(null);
                return;
            }
        }
    }

    @Getter
    @Setter
    private Boolean allowCommit;

    public boolean isAllowCommit() {
        return allowCommit != null && allowCommit;
    }
}
