package com.macrosan.rsocket.client;

import com.macrosan.ServerStart;
import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.MossHttpClient;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.rabbitmq.RabbitMqChannels;
import com.macrosan.rsocket.server.Rsocket;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.netty.channel.Channel;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.transport.netty.TcpDuplexConnection;
import io.rsocket.util.DefaultPayload;
import lombok.extern.log4j.Log4j2;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.channel.ChannelOperations;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.PING;

@Log4j2
public class RSocketProxy implements RSocket {
    private static final Payload HEART = DefaultPayload.create("", PING.name());
    public static MsExecutor printClearExecutor = new MsExecutor(1, 1, new MsThreadFactory("not-print-clear"));
    public static Map<String, Long> notPrintDispose = new ConcurrentHashMap<>();
    public static int RETRY_THRESHOLD = 3;
    public static int RS_TIMEOUT = 9;
    public static int RS_DURATION = 4;
    public static int REACHABLE_TIMEOUT = 2000;

    private final RSocket proxy;

    private AtomicLong num = new AtomicLong();
    private Map<Long, String> logMap = new ConcurrentHashMap<>();
    public String ip;
    public AtomicInteger retry = new AtomicInteger(0);

    public RSocketProxy(RSocket proxy, String ip) {
        this.proxy = proxy;
        this.ip = ip;
        ErasureServer.DISK_SCHEDULER.schedule(this::heart, 0, TimeUnit.SECONDS);
    }

    private void heart() {
        if (!proxy.isDisposed()) {
            try {
                proxy.requestResponse(HEART)
                        .timeout(Duration.ofSeconds(RS_TIMEOUT))
                        .doOnNext(p -> {
                            heartResponse(null, false, false, 1);
                            p.release();
                        })
                        .doOnError(e -> {
                            if (e instanceof TimeoutException) {
                                //不可达
                                boolean isNotReachable = !isReachable();
                                boolean isRetryOver = retry.incrementAndGet() > RETRY_THRESHOLD;
                                if (isNotReachable || isRetryOver) {
                                    heartResponse(e, isNotReachable, isRetryOver, 2);
                                }
                            } else {
                                heartResponse(e, false, false, 3);
                            }
                        })
                        .subscribe(payload -> {
                            if (retry.get() > 0) {
                                retry.set(0);
                            }
                        });
            } catch (Throwable e) {
                heartResponse(e, false, false, 4);
            }

            ErasureServer.DISK_SCHEDULER.schedule(this::heart, RS_DURATION, TimeUnit.SECONDS);
        }
    }

    private void heartResponse(Throwable e, boolean isReachable, boolean isRetryOver, int index) {
        if (e != null) {
            try {
                String message = ip + " " + getInfo(proxy) + " " + e.getMessage();
                notPrintDispose.compute(message, (k, v) -> {
                    if (null == v) {
                        log.info("dispose {} rsocket connection {} {}, reachable: {}, retryOver: {}, retry: {}, index: {}", ip, getInfo(proxy), e.getMessage(), isReachable, isRetryOver, retry.get(), index);
                        v = System.currentTimeMillis();
                    }

                    return v;
                });
            } catch (Exception except) {
                log.error("", except);
            }
            proxy.dispose();
        }
    }

    /**
     * 检查对端节点是否在线，通过对端 eth4 口检查
     **/
    private boolean isReachable() {
        try {
            //本节点跳过检查
            if (ServerConfig.getInstance().getHeartIp1().equals(ip) || MossHttpClient.LOCAL_NODE_IP.equals(ip)) {
                return true;
            }

            InetAddress inet = InetAddress.getByName(ip);
            if (inet.isReachable(REACHABLE_TIMEOUT)) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            log.error("check {} reachable error {}", ip, e);
            return false;
        }
    }

    static Field source;
    static Field connection;
    static Field connSource;
    static Field connections2;
    static Field connections3;
    static Field channel;

    static {
        try {
            source = io.rsocket.util.RSocketProxy.class.getDeclaredField("source");
            source.setAccessible(true);
            connection = Class.forName("io.rsocket.RSocketRequester").getDeclaredField("connection");
            connection.setAccessible(true);
            connSource = Class.forName("io.rsocket.internal.ClientServerInputMultiplexer$InternalDuplexConnection")
                    .getDeclaredField("source");
            connSource.setAccessible(true);
            connections2 = TcpDuplexConnection.class.getDeclaredField("connection");
            connections2.setAccessible(true);
            connections3 = ChannelOperations.class.getDeclaredField("connection");
            connections3.setAccessible(true);
            channel = Class.forName("reactor.netty.ReactorNetty$SimpleConnection")
                    .getDeclaredField("channel");
            channel.setAccessible(true);
            printClearExecutor.schedule(RSocketProxy::clearNotPrint, 0, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static void clearNotPrint() {
        try {
            clearPrintMap(ServerStart.notPrintDrop);
            clearPrintMap(ClientTemplate.notPrintLog);
            clearPrintMap(RabbitMqChannels.notPrintExcept);
            clearPrintMap(RSocketClient.notPrintError);
            clearPrintMap(Rsocket.notPrintErr);
            clearPrintMap(RSocketProxy.notPrintDispose);
        } catch (Exception e) {
            log.error("", e);
        }

        printClearExecutor.schedule(RSocketProxy::clearNotPrint, 60, TimeUnit.SECONDS);
    }

    public static void clearPrintMap(Map<String, Long> map) {
        try {
            if (null != map && null != map.keySet() && !map.keySet().isEmpty()) {
                for (String errMsg : map.keySet()) {
                    map.compute(errMsg, (k, v) -> {
                        if (v != null && System.currentTimeMillis() - v > 60_000L) {
                            return null;
                        }

                        return v;
                    });
                }
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static Channel getChannel(RSocket proxy) {
        try {
            Object s = source.get(proxy);
            Object conn = connection.get(s);
            Object connS = connSource.get(conn);
            Object conn2 = connections2.get(connS);
            Object conn3 = connections3.get(conn2);
            Channel chan = (Channel) channel.get(conn3);
            return chan;
        } catch (Exception e) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "getChannel fail", e);
        }
    }

    private static String getInfo(RSocket proxy) {
        try {
            Channel chan = getChannel(proxy);
            return chan.toString();
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        return proxy.fireAndForget(payload);
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        long n = num.incrementAndGet();
        logMap.put(n, payload.getMetadataUtf8());

        try {
            return proxy.requestResponse(payload)
                    .doFinally(s -> {
                        logMap.remove(n);
                    });
        } catch (Exception e) {
            logMap.remove(n);
            throw e;
        }
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        return proxy.requestStream(payload);
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return proxy.requestChannel(payloads);
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
        return proxy.metadataPush(payload);
    }

    @Override
    public double availability() {
        return proxy.availability();
    }

    @Override
    public Mono<Void> onClose() {
        return proxy.onClose();
    }

    @Override
    public void dispose() {
        proxy.dispose();
    }

    @Override
    public boolean isDisposed() {
        return proxy.isDisposed();
    }
}
