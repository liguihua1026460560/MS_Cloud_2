package com.macrosan.storage.metaserver;

import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.EscapeException;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeoutException;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;

@Log4j2
public class RSocketCommUtil {
    public static Mono<List<Tuple2<String, Boolean>>> broadcastRequest(
            String messageType,
            SocketReqMsg msg,
            Duration timeout
    ) {
        return getListMono(messageType, msg, timeout);
    }

    private static Mono<List<Tuple2<String, Boolean>>> getListMono(String messageType, SocketReqMsg msg, Duration timeout) {
        Flux<Tuple2<String, Boolean>> responses = Flux.empty();
        for (String ip : RabbitMqUtils.HEART_IP_LIST) {
            responses = responses.concatWith(
                    RSocketClient.getRSocket(ip, BACK_END_PORT)
                            .flatMap(r -> r.requestResponse(DefaultPayload.create(Json.encode(msg), messageType)))
                            .map(payload -> new Tuple2<>(ip, SUCCESS.name().equals(payload.getMetadataUtf8())))
                            .timeout(timeout)
                            .doOnError(e -> {
                                if (!(e instanceof EscapeException)) {
                                    if (e instanceof TimeoutException || "closed connection".equals(e.getMessage())
                                            || (e.getMessage() != null && e.getMessage().startsWith("No keep-alive acks for"))) {
                                        log.error("requestResponse {} {} error {}", ip, messageType, e.getMessage());
                                    } else {
                                        log.error("requestResponse {} {} error {}", ip, messageType, e.getMessage());
                                    }
                                }
                            })
                            .onErrorReturn(new Tuple2<>(ip, false)));
        }
        return responses.collectList();
    }

    public static Mono<Boolean> validateResponses(
            String bucketName,
            List<Tuple2<String, Boolean>> results,
            Queue<Tuple2<String, String>> errorQueue
    ) {
        return getBooleanMono(bucketName, results, errorQueue);
    }

    private static Mono<Boolean> getBooleanMono(String bucketName, List<Tuple2<String, Boolean>> results, Queue<Tuple2<String, String>> errorQueue) {
        return Mono.fromCallable(() -> {
            int successNum = 0;
            for (Tuple2<String, Boolean> result : results) {
                if (!result.var2) {
                    errorQueue.offer(new Tuple2<>(result.var1, bucketName));
                    if (result.var1.equals(ServerConfig.getInstance().getHeartIp1())) {
                        return false; // 关键节点失败立即返回
                    }
                } else {
                    successNum++;
                }
            }
            int size = RabbitMqUtils.HEART_IP_LIST.size();
            return successNum >= (size % 2 == 0 ? size / 2 : size / 2 + 1);
        });
    }
}