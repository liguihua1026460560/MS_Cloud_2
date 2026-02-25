package com.macrosan.filesystem.utils;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.message.jsonmsg.NFSIpWhitelist;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.net.InetAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;
import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.UPDATE_NFS_IP_WHITELISTS;
import static com.macrosan.ec.server.ErasureServer.SUCCESS_PAYLOAD;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;

@Log4j2
public class IpWhitelistUtils {
    public static final RedisConnPool pool = RedisConnPool.getInstance();
    public static final MsExecutor FS_WHITELIST_EXECUTOR = new MsExecutor(1, 1, new MsThreadFactory("fs-whitelist"));
    public static final Scheduler FS_WHITELIST_SCHEDULER = Schedulers.fromExecutor(FS_WHITELIST_EXECUTOR);
    public static Map<String, Set<NFSIpWhitelist>> nfsIpWhitelistMap = new HashMap<>();

    public static void init() {

        nfsIpWhitelistMap.clear();
        try {
            ScanArgs scanArgs = new ScanArgs().match("*_nfsIpWhitelist").limit(10);
            KeyScanCursor<String> keyScanCursor = new KeyScanCursor<>();
            keyScanCursor.setCursor("0");
            KeyScanCursor<String> res;

            do {
                res = pool.getCommand(REDIS_BUCKETINFO_INDEX).scan(keyScanCursor, scanArgs);
                List<String> nfsIpWhitelistKeys = res.getKeys();
                for (String key : nfsIpWhitelistKeys) {
                    List<String> nfsIpWhitelists = pool.getCommand(REDIS_BUCKETINFO_INDEX).lrange(key, 0, -1);
                    Set<NFSIpWhitelist> nfsIpWhitelistSet = nfsIpWhitelists.stream()
                            .map(json -> Json.decodeValue(json, NFSIpWhitelist.class))
                            .collect(Collectors.toSet());
                    if (nfsIpWhitelistSet.size() > 0) {
                        nfsIpWhitelistMap.put(key.substring(0, key.indexOf("_")), nfsIpWhitelistSet);
                    }
                }
                keyScanCursor.setCursor(res.getCursor());
            } while (!res.isFinished());

            log.info("init NFSIpWhitelists success");
        } catch (Exception e) {
            log.error("init NFSIpWhitelists error ", e);
        }
    }

    public static void deleteNFSIpWhitelists(String bucket) {
        String key = bucket + "_nfsIpWhitelist";
        Long del = pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).del(key);
        if (del > 0) {
            updateNFSIpWhitelists(bucket).subscribe();
        }
    }

    public static String updateNFSIpWhitelistsCommand(String bucket) {
        int res = Mono.just(1L)
                .publishOn(FS_WHITELIST_SCHEDULER)
                .flatMap(i -> updateNFSIpWhitelists(bucket))
                .block(Duration.ofSeconds(35));
        if (res == 1) {
            return "success";
        } else {
            return "fail";
        }
    }

    public static Mono<Integer> updateNFSIpWhitelists(String bucket) {
        SocketReqMsg msg = new SocketReqMsg("", 0);
        msg.put("bucket", bucket);
        MonoProcessor<Integer> res = MonoProcessor.create();
        AtomicInteger num = new AtomicInteger(0);
        List<String> ipList = RabbitMqUtils.HEART_IP_LIST;
        Flux<Tuple2<String, Boolean>> responses = Flux.empty();
        List<String> failIpList = new ArrayList<>();

        for (int i = 0; i < ipList.size(); i++) {
            String ip = ipList.get(i);
            Mono<Tuple2<String, Boolean>> response = RSocketClient.getRSocket(ip, BACK_END_PORT)
                    .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), UPDATE_NFS_IP_WHITELISTS.name())))
                    .timeout(Duration.ofSeconds(30))
                    .map(payload -> {
                        try {
                            String metaDataPayload = payload.getMetadataUtf8();
                            if (metaDataPayload.equalsIgnoreCase(SUCCESS.name())) {
                                num.incrementAndGet();
                                return new Tuple2<>(ip, true);
                            }
                            log.error("{} update NFSIpWhitelists fail", ip);
                            return new Tuple2<>(ip, false);
                        } finally {
                            payload.release();
                        }
                    })
                    .doOnError(e -> {
                        failIpList.add(ip);
                        log.error("{} update NFSIpWhitelists error ", ip, e);
                    })
                    .onErrorReturn(new Tuple2<>(ip, false));

            responses = responses.mergeWith(response);
        }

        responses.collectList().subscribe(t -> {
            if (num.get() > ipList.size() / 2) {
                res.onNext(1);
            } else {
                res.onNext(-1);
            }
            if (!failIpList.isEmpty()) {
                FS_WHITELIST_EXECUTOR.schedule(() -> updateNFSIpWhitelistsFail(msg, failIpList, 10), 10, TimeUnit.SECONDS);
            }
        });

        return res;

    }

    public static void updateNFSIpWhitelistsFail(SocketReqMsg msg, List<String> ipList, long retryTime) {
        Flux<Tuple2<String, Boolean>> responses = Flux.empty();
        List<String> failIpList = new ArrayList<>();

        for (int i = 0; i < ipList.size(); i++) {
            String ip = ipList.get(i);
            Mono<Tuple2<String, Boolean>> response = RSocketClient.getRSocket(ip, BACK_END_PORT)
                    .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), UPDATE_NFS_IP_WHITELISTS.name())))
                    .timeout(Duration.ofSeconds(30))
                    .map(payload -> {
                        try {
                            String metaDataPayload = payload.getMetadataUtf8();
                            if (metaDataPayload.equalsIgnoreCase(SUCCESS.name())) {
                                return new Tuple2<>(ip, true);
                            }
                            log.error("{} update NFSIpWhitelists retry fail", ip);
                            return new Tuple2<>(ip, false);
                        } finally {
                            payload.release();
                        }
                    })
                    .doOnError(e -> {
                        failIpList.add(ip);
                        log.error("{} update NFSIpWhitelists retry error ", ip, e);
                    })
                    .onErrorReturn(new Tuple2<>(ip, false));

            responses = responses.mergeWith(response);
        }

        responses.collectList().subscribe(t -> {
            if (!failIpList.isEmpty() && retryTime <= 160) {
                FS_WHITELIST_EXECUTOR.schedule(() -> updateNFSIpWhitelistsFail(msg, failIpList, retryTime * 2), retryTime * 2, TimeUnit.SECONDS);
            }
        });
    }

    public static Mono<Payload> updateNFSIpWhitelists(Payload payload) {
        try {
            SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
            String bucket = msg.get("bucket");
            String key = bucket + "_nfsIpWhitelist";
            Set<NFSIpWhitelist> nfsIpWhitelistSet = new HashSet<>();
            // 查询redis主，防止获取到未更新的旧数据
            return pool.getMasterReactive(REDIS_BUCKETINFO_INDEX).lrange(key, 0, -1)
                    .doOnNext(nfsIpWhitelist -> nfsIpWhitelistSet.add(Json.decodeValue(nfsIpWhitelist, NFSIpWhitelist.class)))
                    .collectList()
                    .flatMap(ignore -> {
                        if (nfsIpWhitelistSet.size() > 0) {
                            nfsIpWhitelistMap.put(bucket, nfsIpWhitelistSet);
                        } else {
                            nfsIpWhitelistMap.remove(bucket);
                        }
                        return Mono.just(SUCCESS_PAYLOAD);
                    })
                    .doOnError(e -> {
                        log.error("update NFSIpWhitelist error ", e);
                        FS_WHITELIST_EXECUTOR.schedule(() -> updateNFSIpWhitelistsFail(bucket, 10), 10, TimeUnit.SECONDS);
                    })
                    .onErrorReturn(ERROR_PAYLOAD);
        } catch (Exception e) {
            log.error("update NFSIpWhitelist error ", e);
            return Mono.just(ERROR_PAYLOAD);
        }
    }

    public static void updateNFSIpWhitelistsFail(String bucket, long retryTime) {
        String key = bucket + "_nfsIpWhitelist";
        Set<NFSIpWhitelist> nfsIpWhitelistSet = new HashSet<>();
        pool.getMasterReactive(REDIS_BUCKETINFO_INDEX).lrange(key, 0, -1)
                .doOnNext(nfsIpWhitelist -> nfsIpWhitelistSet.add(Json.decodeValue(nfsIpWhitelist, NFSIpWhitelist.class)))
                .collectList()
                .doOnError(e -> {
                    log.error("update NFSIpWhitelist error ", e);
                    if (retryTime <= 160) {
                        FS_WHITELIST_EXECUTOR.schedule(() -> updateNFSIpWhitelistsFail(bucket, retryTime * 2), retryTime * 2, TimeUnit.SECONDS);
                    }
                })
                .subscribe(ignore -> {
                    if (nfsIpWhitelistSet.size() > 0) {
                        nfsIpWhitelistMap.put(bucket, nfsIpWhitelistSet);
                    } else {
                        nfsIpWhitelistMap.remove(bucket);
                    }
                });
    }

    // 将IP地址转换为整数
    public static long ipToLong(String ip) throws Exception {
        InetAddress inetAddress = InetAddress.getByName(ip);
        byte[] addr = inetAddress.getAddress();
        long result = 0;
        for (byte b : addr) {
            result = (result << 8) + (b & 0xFF);
        }
        return result;
    }

    // 判断目标IP是否在指定的范围内
    public static boolean isIpInRange(String ip, NFSIpWhitelist nfsIpWhitelist) {
        try {
            long ipAddr = ipToLong(ip);
            long networkAddr = ipToLong(nfsIpWhitelist.ip);
            long subnetMask = ipToLong(nfsIpWhitelist.mask);

            // 使用掩码与网络地址与目标IP进行与操作
            return (ipAddr & subnetMask) == (networkAddr & subnetMask);
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean checkNFSWhitelist(String bucket, String ip) {
        Set<NFSIpWhitelist> nfsIpWhitelistSet = nfsIpWhitelistMap.get(bucket);
        if (nfsIpWhitelistSet == null || nfsIpWhitelistSet.size() == 0) {
            return true;
        }
        boolean mountAccess = false;
        for (NFSIpWhitelist nfsIpWhitelist : nfsIpWhitelistSet) {
            if (isIpInRange(ip, nfsIpWhitelist)) {
                mountAccess = true;
            }
        }
        return mountAccess;
    }
}
