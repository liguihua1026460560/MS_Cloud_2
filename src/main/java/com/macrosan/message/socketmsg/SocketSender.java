package com.macrosan.message.socketmsg;

import com.macrosan.action.core.MergeMap;
import com.macrosan.constants.SysConstants;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.serialize.JsonUtils;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.annotations.NonNull;
import io.reactivex.annotations.Nullable;
import io.vertx.core.net.NetClientOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.net.NetClient;
import io.vertx.reactivex.core.net.NetSocket;
import io.vertx.reactivex.core.parsetools.RecordParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.ServerConstants.LINE_BREAKER;
import static com.macrosan.constants.SysConstants.HEART_ETH1;
import static com.macrosan.constants.SysConstants.HEART_ETH2;

/**
 * SocketSender
 *
 * @author liyixin
 * @date 2018/12/3
 */
public class SocketSender {

    private static final Logger logger = LogManager.getLogger(SocketSender.class.getName());

    private static SocketSender instance;

    private NetClient client;


    /**
     * 设置socket读取超时时间，单位分钟
     */
    private static final int TIMEOUT = 3;

    private SocketSender(Vertx vertx) {
        client = vertx.createNetClient(new NetClientOptions().setReusePort(true).setUsePooledBuffers(true)
                .setTcpNoDelay(true));
    }

    public static void init(Vertx vertx) {
        if (instance == null) {
            instance = new SocketSender(vertx);
        }
    }

    public static SocketSender getInstance() {
        return instance;
    }

    private Single<NetSocket> getSocket(int port, String ip) {
        return client.rxConnect(port, ip);
    }

    public Single<NetSocket> getStreamSocket(String ip1, String ip2) {
        return ThreadLocalRandom.current().nextBoolean() ?
                getSocket(SysConstants.SOCKET_PORT3, ip1).onErrorResumeNext(e -> {
                    logger.info("Connect to " + ip1 + " fail,switch to :" + ip2 + " message : " + e.getMessage());
                    return getSocket(SysConstants.SOCKET_PORT4, ip2);
                }) :
                getSocket(SysConstants.SOCKET_PORT4, ip2).onErrorResumeNext(e -> {
                    logger.info("Connect to " + ip2 + " fail,switch to :" + ip1 + " message : " + e.getMessage());
                    return getSocket(SysConstants.SOCKET_PORT3, ip1);
                });
    }

    public Single<NetSocket> getHeartSocket(String ip1, String ip2) {
        return ThreadLocalRandom.current().nextBoolean() ?
                client.rxConnect(SysConstants.SOCKET_PORT1, ip1).onErrorResumeNext(e -> {
                    logger.info("Connect to " + ip1 + " fail,switch to :" + ip2 + " message : " + e.getMessage());
                    return getSocket(SysConstants.SOCKET_PORT2, ip2);
                }) :
                client.rxConnect(SysConstants.SOCKET_PORT2, ip2).onErrorResumeNext(e -> {
                    logger.info("Connect to " + ip2 + " fail,switch to :" + ip1 + " message : " + e.getMessage());
                    return getSocket(SysConstants.SOCKET_PORT1, ip1);
                });
    }

    //用于发送iam管理服务器的socket端口
    public Single<NetSocket> getManageSocket(String ip) {
        return ThreadLocalRandom.current().nextBoolean() ?
                client.rxConnect(SysConstants.SOCKET_PORT7, ip).onErrorResumeNext(e -> {
                    logger.info("Connect to " + ip + " fail,switch to :" + ip + " message : " + e.getMessage());
                    return getSocket(SysConstants.SOCKET_PORT8, ip);
                }) :
                client.rxConnect(SysConstants.SOCKET_PORT8, ip).onErrorResumeNext(e -> {
                    logger.info("Connect to " + ip + " fail,switch to :" + ip + " message : " + e.getMessage());
                    return getSocket(SysConstants.SOCKET_PORT7, ip);
                });
    }

    /**
     * 返回响应式流，流中是未解码的字符串，用于和对端有多次通信的操作
     *
     * @param socket 要读取消息的套接字
     * @return 未解码的字符串
     */
    public Flowable<Buffer> getStringReactive(@NonNull NetSocket socket) {
        return RecordParser
                .newDelimited(LINE_BREAKER, socket)
                .toFlowable()
                .onErrorReturn(e -> {
                    logger.error("recv time out", e);
                    return null;
                });
    }

    public Flowable<Buffer> getStringByLengthReactive(@NonNull NetSocket socket) {
        RecordParser parser = RecordParser
                .newFixed(4, socket);

        return Flowable.create(emitter -> {
            parser.handler(buf -> {
                int length = buf.getInt(0);
                parser.fixedSizeMode(length);
                parser.handler(value -> {
                    emitter.onNext(value);
                    emitter.onComplete();
                });
            }).exceptionHandler(emitter::onError);
        }, BackpressureStrategy.BUFFER);
    }


    /**
     * 返回解码后的响应式流，一般用于和后端仅一次通信的操作
     * <p>
     * 注：此方法中未关闭Socket，需要在外面关闭或者对端关闭（不推荐）
     *
     * @param socket      要读取消息的套接字
     * @param cls         需要接受的消息的类型
     * @param timeoutFlag 是否设置超时时间，超时时间为三分钟
     * @return 包含消息的响应式流
     */
    @Nullable
    public <T> Flowable<T> getResponseReactive(@NonNull NetSocket socket, Class<T> cls, boolean timeoutFlag) {
        Flowable<T> flowable = getStringReactive(socket)
                .map(buffer -> {
                    T t = JsonUtils.toObject(cls, buffer.getBytes());
                    return t;
                });
        return timeoutFlag ? flowable.timeout(TIMEOUT, TimeUnit.MINUTES) : flowable;
    }

    /**
     * 返回解码后的响应式流，一般用于和后端仅一次通信的操作(长度编码解码方式)
     * <p>
     * 注：此方法中未关闭Socket，需要在外面关闭或者对端关闭（不推荐）
     *
     * @param socket      要读取消息的套接字
     * @param cls         需要接受的消息的类型
     * @param timeoutFlag 是否设置超时时间，超时时间为三分钟
     * @return 包含消息的响应式流
     */
    @Nullable
    public <T> Flowable<T> getResponseByLengthReactive(@NonNull NetSocket socket, Class<T> cls, Boolean timeoutFlag) {
        Flowable<T> flowable = getStringByLengthReactive(socket)
                .map(buffer -> JsonUtils.toObject(cls, buffer.getBytes()));
        return timeoutFlag ? flowable.timeout(TIMEOUT, TimeUnit.MINUTES) : flowable;
    }

    /**
     * 发送并且等待消息返回
     *
     * @param map         存放目标ip的map
     * @param msg         要发送的消息
     * @param cls         期望收到的消息类型
     * @param timeoutFlag 是否超时
     * @param <T>         期望收到的消息类型
     * @return 收到的消息
     */
    public <T> T sendAndGetResponse(MergeMap<String, String> map, SocketReqMsg msg, Class<T> cls, boolean timeoutFlag) {
        return getHeartSocket(map.get(HEART_ETH1), map.get(HEART_ETH2))
                .flatMapPublisher(socket -> getResponseReactive(socket, cls, timeoutFlag)
                        .doOnSubscribe(s -> socket.write(JsonUtils.toString(msg) + LINE_BREAKER))
                        .doOnCancel(socket::close))
                .blockingFirst();
    }

    /**
     * 发送并且等待消息返回
     *
     * @param map         存放目标ip的map
     * @param msg         要发送的消息
     * @param cls         期望收到的消息类型
     * @param timeoutFlag 是否超时
     * @param <T>         期望收到的消息类型
     * @return 收到的消息
     */
    public <T> T sendAndGetResponse(Map<String, String> map, SocketReqMsg msg, Class<T> cls, boolean timeoutFlag) {
        String heartIp1 = map.get(HEART_ETH1);
        String heartIp2 = map.get(HEART_ETH2);
        //一体化HEART_ETH1，HEART_ETH2，对应的是heartbeat_MS_Cloud
        if(ServerConfig.isUnify()){
            heartIp1 = map.get("heartbeat_eth1");
            heartIp2 = map.get("heartbeat_eth2");
        }
        return getHeartSocket(heartIp1, heartIp2)
                .flatMapPublisher(socket -> getResponseReactive(socket, cls, timeoutFlag)
                        .doOnSubscribe(s -> socket.write(JsonUtils.toString(msg) + LINE_BREAKER))
                        .doOnCancel(socket::close))
                .blockingFirst();
    }

    /**
     * 发送并且等待消息返回
     *
     * @param ip          存放管理服务器的ip
     * @param msg         要发送的消息
     * @param cls         期望收到的消息类型
     * @param timeoutFlag 是否超时
     * @param <T>         期望收到的消息类型
     * @return 收到的消息
     */
    public <T> T sendAndGetResponse(String ip, SocketReqMsg msg, Class<T> cls, boolean timeoutFlag) {
        return getManageSocket(ip)
                .flatMapPublisher(socket -> getResponseByLengthReactive(socket, cls, timeoutFlag)
                        .doOnSubscribe(s -> {
                            byte[] msgStr = JsonUtils.toString(msg).getBytes();
                            Buffer buffer = Buffer.buffer();
                            buffer.appendInt(msgStr.length).appendBytes(msgStr);
                            socket.write(buffer);
                        })
                        .doOnCancel(socket::close))
                .blockingFirst();
    }

    /**
     * 直接连接本端心跳ip发送socket 并且等待消息返回
     *
     * @param msg         要发送的消息
     * @param cls         期望收到的消息类型
     * @param timeoutFlag 是否超时
     * @param <T>         期望收到的消息类型
     * @return 收到的消息
     */
    public <T> T sendAndGetResponse(SocketReqMsg msg, Class<T> cls, boolean timeoutFlag) {
        String heartIp1 = ServerConfig.getInstance().getEth4();
        String heartIp2 = ServerConfig.getInstance().getEth4();

        return getHeartSocket(heartIp1, heartIp2)
                .flatMapPublisher(socket -> getResponseReactive(socket, cls, timeoutFlag)
                        .doOnSubscribe(s -> socket.write(JsonUtils.toString(msg) + LINE_BREAKER))
                        .doOnCancel(socket::close))
                .blockingFirst();
    }

}
