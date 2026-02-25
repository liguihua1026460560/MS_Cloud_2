package com.macrosan.rsocket.client;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.rsocket.VertxLoopResource;
import com.macrosan.rsocket.server.MsPayloadDecoder;
import com.macrosan.utils.msutils.EscapeException;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import lombok.extern.log4j.Log4j2;
import org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;

import java.util.LinkedList;
import java.util.List;

import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.constants.SysConstants.HEART_ETH1;
import static com.macrosan.constants.SysConstants.REDIS_NODEINFO_INDEX;
import static com.macrosan.fs.BlockDevice.INIT_BLOCK_SCHEDULER;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.rsocket.VertxLoopResource.VERTX_LOOP_RESOURCE;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;

/**
 * RSocketClient
 *
 * @author liyixin
 * @date 2019/8/30
 */
@Log4j2
public class RSocketClient {
    private final static ConcurrentHashMap<Integer, MonoProcessor<RSocket>>[] HOLDERS = new ConcurrentHashMap[Math.min(PROC_NUM, 48)];
    public static Map<String, Long> notPrintError = new ConcurrentHashMap<>();

    static {
        for (int i = 0; i < HOLDERS.length; i++) {
            HOLDERS[i] = new ConcurrentHashMap<>();
        }
    }

    /**
     * 不做同步递增，并发情况下允许重复重一个HOLDER中获得
     */
    private static int index = 0;

    private static TcpClient createTcpClient(String ip, int port) {
        return TcpClient
                .create(ConnectionProvider.newConnection())
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10_000)
//                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(256 * 1024, 512 * 1024))
                .runOn(VERTX_LOOP_RESOURCE, true)
                .addressSupplier(() -> new InetSocketAddress(ip, port));
    }

    private static Mono<RSocket> connect(String ip, int port) {
        return RSocketFactory
                .connect()
                .keepAlive(Duration.ofSeconds(5), Duration.ofSeconds(10), 10)
                .errorConsumer(e -> {
                    if (e != null && e.toString() != null && (e instanceof NullPointerException || e.toString().contains("Connection reset by peer") || e.toString().contains("No keep-alive acks for"))) {
                        if (!notPrintError.containsKey(e.toString())) {
                            notPrintError.compute(e.toString(), (k ,v) -> {
                               if (null == v) {
                                   log.error("", e);
                                   v = System.currentTimeMillis();
                               }

                               return v;
                            });
                        }
                    } else {
                        log.error("", e);
                    }
                })
                .frameDecoder(MsPayloadDecoder.DEFAULT)
                .transport(() -> TcpClientTransport.create(createTcpClient(ip, port)))
                .start();
    }

    private static boolean isFirst = true;

    public static void clearErrorRSocket(int key) {
        Arrays.stream(HOLDERS).forEach(holder -> {
            holder.compute(key, (k, v) -> {
                try {
                    if (v != null) {
                        //remove all
                        if (ERROR_RSOCKET == v.peek()) {
                            return null;
                        }
                    }
                } catch (Exception e) {
                }

                return v;
            });
        });
    }

    private static void connect(int i, String ip, int port, MonoProcessor<RSocket> processor) {
        int key = (ip + port).hashCode();
        connect(ip, port)
                .timeout(Duration.ofSeconds(15))
                .subscribe(rSocket -> {
                    if (loops.length == HOLDERS.length) {
                        //强制 channel.eventLoop() 和 loops[i] 相同
                        Channel channel = RSocketProxy.getChannel(rSocket);
                        if (channel.eventLoop() != loops[i]) {
                            rSocket.dispose();
                            connect(i, ip, port, processor);
                            return;
                        }
                    }


                    rSocket.onClose().doFinally(s -> {
                        String errorMsg = "remove on close: " + ip;
                        if (!notPrintError.containsKey(errorMsg)) {
                            notPrintError.compute(errorMsg, (k ,v) -> {
                                if (null == v) {
                                    log.error("{}", errorMsg);
                                    v = System.currentTimeMillis();
                                }

                                return v;
                            });
                        }

                        HOLDERS[i].remove(key);
                    }).subscribe();
                    processor.onNext(new RSocketProxy(rSocket, ip));
                }, cause -> {
                    if (null != cause.getMessage()) {
                        if (!notPrintError.containsKey(cause.getMessage())) {
                            notPrintError.compute(cause.getMessage(), (k ,v) -> {
                                if (null == v) {
                                    log.info("remove {} on exception {}", ip, cause.getMessage());
                                    v = System.currentTimeMillis();
                                }

                                return v;
                            });
                        }
                    } else {
                        log.info("remove {} on exception {}", ip, cause.getMessage());
                    }

                    if (isFirst) {
                        HOLDERS[i].remove(key).subscribe(Disposable::dispose, e -> {
                        });
                    } else {
                        Disposable[] disposables = new Disposable[1];
                        disposables[0] = INIT_BLOCK_SCHEDULER.schedulePeriodically(() -> {
                            connect(ip, port)
                                    .timeout(Duration.ofSeconds(15))
                                    .doOnNext(socket -> {
                                        clearErrorRSocket(key);
                                        disposables[0].dispose();
                                    })
                                    .doOnError(e -> {
                                    })
                                    .subscribe(Disposable::dispose);
                        }, 20, 20, TimeUnit.SECONDS);
                    }

                    processor.onNext(ERROR_RSOCKET);
                });
    }

    private static Mono<RSocket> connect(int i, String ip, int port) {
        int key = (ip + port).hashCode();
        MonoProcessor<RSocket> processor;
        synchronized (HOLDERS[i]) {
            if (HOLDERS[i].get(key) == null) {
                processor = MonoProcessor.create();
                HOLDERS[i].put(key, processor);
            } else {
                return HOLDERS[i].get(key);
            }
        }

        connect(i, ip, port, processor);

        return processor;
    }

    static final RSocket ERROR_RSOCKET = new AbstractRSocket() {
        @Override
        public Mono<Payload> requestResponse(Payload payload) {
            payload.release();
            return Mono.error(new EscapeException("Request-Response not implemented."));
        }

        @Override
        public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            Flux.from(payloads).subscribe(payload -> payload.release());
            return Flux.error(new EscapeException("Request-Channel not implemented."));
        }
    };


    public static void init() {
        RedisConnPool pool = RedisConnPool.getInstance();
        ServerConfig.getInstance().getVertx().setTimer(100L, l -> {
            Flux<RSocket> flux = Flux.empty();
            for (int i = 0; i < HOLDERS.length; i++) {
                int k = i;
                Flux<RSocket> connections = pool.getReactive(REDIS_NODEINFO_INDEX).keys("*")
                        .flatMap(node -> pool.getReactive(REDIS_NODEINFO_INDEX).hget(node, HEART_ETH1))
                        .flatMap(ip -> connect(k, ip, BACK_END_PORT)
                                .doOnError(e -> log.info("connect {} fail", ip))
                                .onErrorReturn(new AbstractRSocket() {
                                })
                        );

                flux = flux.mergeWith(connections);
            }

            flux.subscribe(s -> {
            }, e -> log.error("", e), () -> {
                isFirst = false;
                log.info("init RSocket connections successful");
            });
        });
    }

    static ErasureServer server = new ErasureServer();
    public static EventLoop[] loops;
    public static LongIntHashMap loopThreads = new LongIntHashMap();

    static {
        List<EventLoop> loopList = new LinkedList<>();
        AtomicInteger i = new AtomicInteger();
        VertxLoopResource.VERTX.nettyEventLoopGroup().iterator().forEachRemaining(e -> {
            SingleThreadEventExecutor loop = (SingleThreadEventExecutor) e;
            loopList.add((EventLoop) loop);
            loopThreads.put(loop.threadProperties().id(), i.getAndIncrement());
        });

        loops = loopList.toArray(new EventLoop[loopList.size()]);
    }

    public static Mono<RSocket> getRSocket(String ip, int port) {
        if (CURRENT_IP.equalsIgnoreCase(ip)) {
            return Mono.just(server);
        }

        int i;
        int key = (ip + port).hashCode();
        int loopIndex = loopThreads.getIfAbsent(Thread.currentThread().getId(), -1);
        if (loops.length == HOLDERS.length && loopIndex != -1) {
            i = loopIndex;
        } else {
            i = Math.abs(index++ % HOLDERS.length);
        }

        Mono<RSocket> res = HOLDERS[i].get(key);
        if (null == res) {
            res = connect(i, ip, port);
        }

        return res;
    }
}
