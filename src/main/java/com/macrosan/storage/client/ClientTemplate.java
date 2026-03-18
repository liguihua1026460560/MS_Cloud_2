package com.macrosan.storage.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.doubleActive.DataSynChecker;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.ec.server.ErasureServer.PayloadMetaType;
import com.macrosan.filesystem.utils.timeout.FastMonoTimeOut;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.LocalPayload;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.EscapeException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.eclipse.collections.api.block.function.Function2;
import org.eclipse.collections.api.map.primitive.ImmutableIntObjectMap;
import org.eclipse.collections.impl.map.immutable.primitive.ImmutableIntObjectMapFactoryImpl;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.filesystem.cache.Vnode.INODE_HEART_DOWN_ERROR;
import static com.macrosan.httpserver.MossHttpClient.LOCAL_NODE_IP;
import static com.macrosan.message.jsonmsg.Inode.HEART_DOWN_INODE;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;
import static com.macrosan.rsocket.server.Rsocket.DA_RSOCKET_PORT;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class ClientTemplate {
    public static Map<String, Long> notPrintLog = new ConcurrentHashMap<>();

    public static <T> Tuple3<Integer, PayloadMetaType, T> mapPayloadToTuple(int index, Payload payload, Class<T> responseClass) {
        PayloadMetaType type;
        T resObj;
        boolean local = payload instanceof LocalPayload;
        Tuple3<Integer, PayloadMetaType, T> res;

        if (local) {
            LocalPayload<T> localPayload = (LocalPayload<T>) payload;
            type = localPayload.type;
            resObj = localPayload.data;
            res = new Tuple3<>(index, type, resObj);
        } else {
            type = PayloadMetaType.valueOf(payload.getMetadataUtf8());
            if (ERROR == type) {
                res = new Tuple3<>(index, type, null);
            } else {
                if (responseClass.equals(String.class)) {
                    resObj = (T) payload.getDataUtf8();
                } else if (responseClass.equals(byte[].class)) {
                    ByteBuf byteBuf = payload.sliceData();
                    byte[] bytes = new byte[byteBuf.readableBytes()];
                    byteBuf.getBytes(0, bytes);
                    resObj = (T) bytes;
                } else {
                    resObj = Json.decodeValue(payload.getDataUtf8(), responseClass);
                }
                res = new Tuple3<>(index, type, resObj);

            }
        }

        payload.release();
        return res;
    }

    private static <T> Tuple3<Integer, PayloadMetaType, T> mapPayloadToTuple(int index, Payload payload, TypeReference<T> responseClass) {
        PayloadMetaType type;
        T resObj;
        boolean local = payload instanceof LocalPayload;
        Tuple3<Integer, PayloadMetaType, T> res;

        if (local) {
            LocalPayload<T> localPayload = (LocalPayload<T>) payload;
            type = localPayload.type;
            resObj = localPayload.data;
            res = new Tuple3<>(index, type, resObj);
        } else {
            type = PayloadMetaType.valueOf(payload.getMetadataUtf8());
            if (ERROR == type) {
                res = new Tuple3<>(index, type, null);
            } else {
                resObj = Json.decodeValue(payload.getDataUtf8(), responseClass);
                res = new Tuple3<>(index, type, resObj);
            }
        }

        payload.release();
        return res;
    }

    public static <T> ResponseInfo<T> oneResponse(List<SocketReqMsg> msgs,
                                                  PayloadMetaType msgType, Class<T> responseClass,
                                                  List<Tuple3<String, String, String>> nodeList) {
        Function2<Integer, Payload, Tuple3<Integer, PayloadMetaType, T>> mapFunction =
                (index, payload) -> mapPayloadToTuple(index, payload, responseClass);

        return oneResponse(msgs, msgType, mapFunction, nodeList);
    }

    //兼容没有支持LocalPayload的接口，只把已经支持的接口放入
    private static final ImmutableIntObjectMap<PayloadMetaType> FAST_SOCKET_MAP;

    static {
        IntObjectHashMap<PayloadMetaType> fastSocketMap = new IntObjectHashMap<>();
        fastSocketMap.put(PUT_INODE.ordinal(), PUT_INODE);
        fastSocketMap.put(DELETE_FILE.ordinal(), DELETE_FILE);
        fastSocketMap.put(INODE_CACHE_OPT.ordinal(), INODE_CACHE_OPT);
        fastSocketMap.put(INODE_BUFFER.ordinal(), INODE_BUFFER);
        fastSocketMap.put(PUT_OBJECT_META.ordinal(), PUT_OBJECT_META);
        fastSocketMap.put(WRITE_CACHE.ordinal(), WRITE_CACHE);
        fastSocketMap.put(FLUSH_WRITE_CACHE.ordinal(), FLUSH_WRITE_CACHE);
        fastSocketMap.put(FILE_ASYNC.ordinal(), FILE_ASYNC);
        FAST_SOCKET_MAP = ImmutableIntObjectMapFactoryImpl.INSTANCE.ofAll(fastSocketMap);
    }

    private static <T> ResponseInfo<T> oneResponse(List<SocketReqMsg> msgs,
                                                   PayloadMetaType msgType,
                                                   Function2<Integer, Payload, Tuple3<Integer, PayloadMetaType, T>> mapFunction,
                                                   List<Tuple3<String, String, String>> nodeList) {
        Flux<Tuple3<Integer, PayloadMetaType, T>> responses = Flux.empty();
        Duration timeout = Duration.ofSeconds(30);

        if (LIST_VNODE_META.equals(msgType) || LIST_VNODE_META_MARKER.equals(msgType)
                || LIST_VNODE_OBJ.equals(msgType)) {
            timeout = Duration.ofMinutes(5);
        }
        int port = BACK_END_PORT;
        if (msgType.equals(FILE_ASYNC)) {
            port = DA_RSOCKET_PORT;
        }

        for (int i = 0; i < nodeList.size(); i++) {
            int index = i;
            Tuple3<String, String, String> tuple = nodeList.get(i);
            Tuple3<Integer, PayloadMetaType, T>[] errorRes = new Tuple3[1];
            errorRes[0] = new Tuple3<>();
            errorRes[0].var1 = index;
            errorRes[0].var2 = ERROR;
            errorRes[0].var3 = null;

            Payload payload;
            boolean islocal = CURRENT_IP.equalsIgnoreCase(tuple.var1) || LOCAL_NODE_IP.equalsIgnoreCase(tuple.var1);
            if (islocal && FAST_SOCKET_MAP.containsKey(msgType.ordinal())) {
                payload = new LocalPayload<>(msgType, msgs.get(index));
            } else {
                ByteBuf buf = FAST_SOCKET_MAP.containsKey(msgType.ordinal()) ? msgs.get(i).toBytes() : Unpooled.wrappedBuffer(Json.encode(msgs.get(i)).getBytes());
                payload = DefaultPayload.create(buf, Unpooled.wrappedBuffer(msgType.name().getBytes()));
            }

            Mono<Tuple3<Integer, PayloadMetaType, T>> response = RSocketClient.getRSocket(tuple.var1, port)
                    .flatMap(rSocket -> rSocket.requestResponse(payload))
                    .map(p -> mapFunction.apply(index, p));

            int finalPort = port;
            response = FastMonoTimeOut.fastTimeout(response, timeout)
                    .doOnError(e -> {
                        if (DataSynChecker.isDebug && finalPort == DA_RSOCKET_PORT) {
                            log.error("FILE_ASYNC error, ip {}", tuple.var1, e);
                        }
                        if (!(e instanceof EscapeException)) {
                            if (e instanceof TimeoutException || "closed connection".equals(e.getMessage())
                                    || (e.getMessage() != null && e.getMessage().startsWith("No keep-alive acks for"))) {
                                if (INODE_CACHE_HEART.equals(msgType) || INODE_CACHE_HEART_FAIL.equals(msgType)) {
                                    StringBuilder message = new StringBuilder();
                                    message.append(nodeList.get(index).var1);
                                    message.append(msgType);
                                    if (!notPrintLog.containsKey(message.toString())) {
                                        notPrintLog.compute(nodeList.get(index).var1, (k, v) -> {
                                            if (null == v) {
                                                log.error("requestResponse {} {} {} error {}", tuple, msgs.get(index), msgType, e.getMessage());
                                                v = System.currentTimeMillis();
                                            }

                                            return v;
                                        });
                                    }
                                } else {
                                    boolean filter = "closed connection".equals(e.getMessage()) ||
                                            e instanceof ClosedChannelException;
                                    if (filter) {
                                        StringBuilder message = new StringBuilder();
                                        message.append(nodeList.get(index).var1);
                                        message.append(msgType);
                                        if (INODE_CACHE_OPT.equals(msgType)) {
                                            message.append(msgs.get(0).get("opt"));
                                        }
                                        message.append(e.getClass());
                                        notPrintLog.compute(message.toString(), (k, v) -> {
                                            if (null == v) {
                                                log.error("requestResponse {} {} {} error {}", tuple, msgs.get(index), msgType, e.getMessage());
                                                v = System.currentTimeMillis();
                                            }

                                            return v;
                                        });
                                    } else {
                                        log.error("requestResponse {} {} {} error {}", tuple, msgs.get(index), msgType, e.getMessage());
                                    }
                                }
                            } else {
                                log.error("requestResponse {} {} {} error", tuple, msgs.get(index), msgType, e);
                            }
                            if ("closed connection".equals(e.getMessage()) || e.getMessage().contains("ClosedChannelException")) {
                                if (msgType.equals(INODE_CACHE_HEART)) {
                                    errorRes[0].var3 = (T) INODE_HEART_DOWN_ERROR;
                                }
                                if (msgType.equals(INODE_CACHE_OPT)) {
                                    errorRes[0].var3 = (T) HEART_DOWN_INODE;
                                }
                            }
                        } else {
                            if (msgType.equals(INODE_CACHE_HEART)) {
                                errorRes[0].var3 = (T) INODE_HEART_DOWN_ERROR;
                            }
                            if (msgType.equals(INODE_CACHE_OPT)) {
                                errorRes[0].var3 = (T) HEART_DOWN_INODE;
                            }
                        }
                    })
                    .onErrorReturn(errorRes[0]);

            responses = responses.mergeWith(response);
        }


        return new ResponseInfo<>(responses, nodeList.size());
    }

    public static <T> ResponseInfo<T> oneResponse(List<SocketReqMsg> msgs,
                                                  PayloadMetaType msgType, TypeReference<T> responseClass,
                                                  List<Tuple3<String, String, String>> nodeList) {
        Function2<Integer, Payload, Tuple3<Integer, PayloadMetaType, T>> mapFunction =
                (index, payload) -> mapPayloadToTuple(index, payload, responseClass);

        return oneResponse(msgs, msgType, mapFunction, nodeList);
    }

    public static <T> ResponseInfo<T> multiResponse(List<? extends Flux<Payload>> payloads, Class<T> responseClass,
                                                    List<Tuple3<String, String, String>> nodeList) {
        return multiResponse(payloads, responseClass, nodeList, BACK_END_PORT);
    }

    public static <T> ResponseInfo<T> multiResponse(List<? extends Flux<Payload>> payloads, Class<T> responseClass,
                                                    List<Tuple3<String, String, String>> nodeList, int port) {
        Flux<Tuple3<Integer, PayloadMetaType, T>> responses = Flux.empty();
        for (int i = 0; i < nodeList.size(); i++) {
            int index = i;
            Tuple3<String, String, String> tuple = nodeList.get(i);

            Flux<Tuple3<Integer, PayloadMetaType, T>> response = RSocketClient.getRSocket(tuple.var1, port)
                    .flatMapMany(rSocket -> rSocket.requestChannel(payloads.get(index)))
                    .map(p -> mapPayloadToTuple(index, p, responseClass))
                    .doOnError(e -> {
                        if (port == DA_RSOCKET_PORT && DataSynChecker.isDebug) {
                            log.info("", e);
                        } else {
                            log.debug("", e);
                        }
                    })
                    .onErrorReturn(new Tuple3<>(index, ERROR, null));

            responses = responses.mergeWith(response);
        }

        return new ResponseInfo<>(responses, nodeList.size());
    }

    /**
     * 通过requestChannel发送上传指定数据块索引请求。返回的reponses数量为errorChunksList.size()
     *
     * @param payloads        缺失数据块的payload。注意并非完整k+m个数据块的payload。
     * @param responseClass   期望返回类型
     * @param nodeList        所有node信息
     * @param errorChunksList 缺失数据块索引的list
     * @param <T>             期望返回类型
     * @return response合并返回
     */
    public static <T> ClientTemplate.ResponseInfo<T> multiResponse(List<? extends Flux<Payload>> payloads, Class<T> responseClass,
                                                                   List<Tuple3<String, String, String>> nodeList, List<Integer> errorChunksList,
                                                                   List<Flux<Tuple3<Integer, ErasureServer.PayloadMetaType, T>>> otherList) {
        Flux<Tuple3<Integer, ErasureServer.PayloadMetaType, T>> responses = Flux.empty();
        for (int i = 0; i < errorChunksList.size(); i++) {
            int index = i;
            int errorChunkIndex = errorChunksList.get(i);
            Tuple3<String, String, String> tuple = nodeList.get(errorChunkIndex);

            Flux<Tuple3<Integer, ErasureServer.PayloadMetaType, T>> response = RSocketClient.getRSocket(tuple.var1, BACK_END_PORT)
                    .flatMapMany(rSocket -> rSocket.requestChannel(payloads.get(index)))
                    .map(p -> ClientTemplate.mapPayloadToTuple(errorChunkIndex, p, responseClass))
                    .doOnError(e -> log.debug("", e))
                    .onErrorReturn(new Tuple3<>(errorChunkIndex, ERROR, null));

            responses = responses.mergeWith(response);
        }

        for (Flux<Tuple3<Integer, ErasureServer.PayloadMetaType, T>> response : otherList) {
            responses = responses.mergeWith(response);
        }

        return new ClientTemplate.ResponseInfo<>(responses, nodeList.size());
    }

    public static class ResponseInfo<T> {
        public int successNum;
        public int errorNum;
        public int writedNum;
        public Tuple2<PayloadMetaType, T>[] res;
        //link索引，响应类型，负载payload（类型为T)
        public Flux<Tuple3<Integer, PayloadMetaType, T>> responses;

        public ResponseInfo(Flux<Tuple3<Integer, PayloadMetaType, T>> responses, int num) {
            res = new Tuple2[num];

            this.responses = responses.doOnNext(tuple3 -> {
                if (SUCCESS.equals(tuple3.var2)) {
                    successNum++;
                }

                if (ERROR.equals(tuple3.var2)) {
                    errorNum++;
                }

                if (META_WRITEED.equals(tuple3.var2)) {
                    writedNum++;
                }

                res[tuple3.var1] = new Tuple2<>(tuple3.var2, tuple3.var3);
            });
        }
    }

}
