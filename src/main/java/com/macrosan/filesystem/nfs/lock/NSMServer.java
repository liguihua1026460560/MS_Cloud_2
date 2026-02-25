package com.macrosan.filesystem.nfs.lock;

import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.ec.Utils;
import com.macrosan.filesystem.nfs.api.NSMProc;
import com.macrosan.filesystem.nfs.types.Sm;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.filesystem.nfs.api.NSMProc.isLocalIp;

@Log4j2
public class NSMServer {
    public static AtomicBoolean readSmBak = new AtomicBoolean(false);
    public static AtomicInteger reLockTime = new AtomicInteger(10); // 等待保活恢复锁
    public static AtomicBoolean reclaim = new AtomicBoolean(true);  // NSM宽限期
    public static AtomicInteger reclaimTime = new AtomicInteger(90);
    public static Map<String, Set<Sm>> smMap = new ConcurrentHashMap<>();
    public static Map<String, Set<Sm>> smMergeMap = new ConcurrentHashMap<>();
    public static Map<String, Set<Sm>> smBakMap = new ConcurrentHashMap<>();

    public static int state = 0;

    public final static String PREFIX_SM = "nsm_sm/";
    public final static String PREFIX_SM_BAK = "nsm_sm_bak/";

    static MsExecutor executorNSM = new MsExecutor(1, 1, new MsThreadFactory("nsm-reLock"));

    private static void checkSmBak() {
        if (smBakMap.isEmpty()) {
            reLockTime.set(0);
        }
        if (reLockTime.getAndDecrement() <= 0) {
            sendNotifyList();
            log.info("NSM reLock end");
            return;
        }
        executorNSM.schedule(NSMServer::checkSmBak, 1, TimeUnit.SECONDS);
    }

    private static void checkNotify() {
        if (readSmBak.get() && smBakMap.isEmpty()) {
            reclaimTime.set(0);
        }
        if (reclaimTime.getAndDecrement() <= 0) {
            reclaim.set(false);
            log.info("NSM normal server");
            return;
        }
        executorNSM.schedule(NSMServer::checkNotify, 1, TimeUnit.SECONDS);
    }

    public static void init() {
        executorNSM.submit(NSMServer::checkNotify);

        try {
            byte[] stateBytes = MSRocksDB.getRocksDB(Utils.getMqRocksKey()).get("nsm_state".getBytes());
            if (stateBytes != null) {
                state = Integer.parseInt(new String(stateBytes));
            }
        } catch (Exception e) {
            log.info("NSM rocks get state fail", e);
        }

        if (state % 2 == 0) {
            state++;
        } else {
            state += 2;
        }

        try {
            MSRocksDB.getRocksDB(Utils.getMqRocksKey()).put("nsm_state".getBytes(), String.valueOf(state).getBytes());
        } catch (Exception e) {
            log.info("NSM rocks set state fail", e);
        }


        readSmListToBak();
        log.info("NSM state:" + state);
    }


    public static void sendNotify(Sm sm) {
        Mono.just(sm.ip)
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(ip -> {
                    MonoProcessor<Boolean> res = MonoProcessor.create();
                    return NSMProc.sendGetPortExec(res, sm.ip, sm.local, 1);
                })
                .flatMap(sendSuccess -> {
                    smBakMap.remove(sm.ip + " " + sm.local);
                    deleteIp(PREFIX_SM_BAK + sm.ip + " " + sm.local + "/");
                    return Mono.just(sendSuccess);
                })
                .doFinally(signalType -> {
                    if (isLocalIp(sm.local)) {
                        log.info("NSM sendNotify end, ip:{} local:{}", sm.ip, sm.local);
                    }
                })
                .subscribe();
    }

    public static void sendNotifyList() {
        Set<String> ipSet = new HashSet<>(smBakMap.keySet());
        if (ipSet.size() > 0) {
            Flux.fromIterable(ipSet)
                    .subscribeOn(Schedulers.boundedElastic())
                    .flatMap(ipAndLocal -> {
                        MonoProcessor<Boolean> res = MonoProcessor.create();
                        return Mono.zip(NSMProc.sendGetPortExec(res, ipAndLocal.split(" ")[0], ipAndLocal.split(" ")[1], 1), Mono.just(ipAndLocal));
                    })
                    .flatMap(tuple2 -> {
                        boolean sendSuccess = tuple2.getT1();
                        String ipAndLocal = tuple2.getT2();
                        smBakMap.remove(ipAndLocal);
                        deleteIp(PREFIX_SM_BAK + ipAndLocal + "/");
                        return Mono.just(sendSuccess);
                    })
                    .doFinally(signalType -> {
                        reclaimTime.set(15);
                        log.info("NSM sendNotifyList end");
                    })
                    .subscribe();
        } else {
            reclaimTime.set(0);
        }
    }

    public static void readSmListToBak() {
        try (MSRocksIterator iterator = MSRocksDB.getRocksDB(Utils.getMqRocksKey()).newIterator()) {
            iterator.seek(PREFIX_SM_BAK.getBytes());
            int smBakNum = 0;
            while (iterator.isValid() && new String(iterator.key()).startsWith(PREFIX_SM_BAK)) {
                String key = new String(iterator.key()).substring(PREFIX_SM_BAK.length());
                String smJson = new String(iterator.value());
                log.info("NSM read key:" + new String(iterator.key()) + ", value:" + smJson);
                iterator.next();
                if (!"{}".equals(smJson)) {
                    Sm sm = Json.decodeValue(smJson, Sm.class);
                    Set<Sm> sms = smBakMap.computeIfAbsent(sm.ip + " " + sm.local, k -> new HashSet<>());
                    sms.add(sm);
                    smBakNum++;
                } else {
                    deleteSync(PREFIX_SM_BAK + key);
                }
            }
            log.info("NSM rocks get smBak to smBakMap " + smBakNum);
        } catch (Exception e) {
            log.info("NSM rocks get smBak to smBakMap fail", e);
        }


        try (MSRocksIterator iterator = MSRocksDB.getRocksDB(Utils.getMqRocksKey()).newIterator()) {
            iterator.seek(PREFIX_SM.getBytes());
            int smNum = 0;
            while (iterator.isValid() && new String(iterator.key()).startsWith(PREFIX_SM)) {
                String key = new String(iterator.key()).substring(PREFIX_SM.length());
                String smJson = new String(iterator.value());
                iterator.next();
                if (!"{}".equals(smJson)) {
                    Sm sm = Json.decodeValue(smJson, Sm.class);
                    Set<Sm> sms = smBakMap.computeIfAbsent(sm.ip + " " + sm.local, k -> new HashSet<>());
                    sms.add(sm);
                    putSync(sm.smBakKey(), sm);
                    smNum++;
                }
                deleteSync(PREFIX_SM + key);
            }
            log.info("NSM rocks get sm to smBakMap " + smNum);
        } catch (Exception e) {
            log.info("NSM rocks get sm to smBakMap fail", e);
        }

        readSmBak.set(true);
        executorNSM.submit(NSMServer::checkSmBak);
    }

    public static Mono<Boolean> monLock(Sm sm) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        smMap.compute(sm.ip + " " + sm.local, (key, sms) -> {
            if (sms == null) {
                sms = ConcurrentHashMap.newKeySet();
            }
            if (!sms.contains(sm)) {
                sms.add(sm);
                smMergeMap.computeIfAbsent(sm.mergeKey(), k -> ConcurrentHashMap.newKeySet())
                        .add(sm);
                if (!smBakMap.isEmpty()) {
                    smBakMap.compute(sm.ip + " " + sm.local, (bakKey, bakSms) -> {
                        if (bakSms == null) {
                            put(sm.smKey(), sm)
                                    .subscribe(bb -> res.onNext(true));
                            return null;
                        }
                        if (bakSms.contains(sm)) {
                            bakSms.remove(sm);
                            delete(sm.smBakKey())
                                    .subscribe(b -> {
                                        put(sm.smKey(), sm)
                                                .subscribe(bb -> res.onNext(true));
                                    });
                        } else {
                            put(sm.smKey(), sm)
                                    .subscribe(bb -> res.onNext(true));
                        }
                        return bakSms.isEmpty() ? null : bakSms;
                    });
                } else {
                    put(sm.smKey(), sm)
                            .subscribe(bb -> res.onNext(true));
                }
            } else {
                res.onNext(true);
            }
            return sms;
        });
        return res;
    }

    public static Mono<Boolean> unMonLock(Sm sm) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        smMap.compute(sm.ip + " " + sm.local, (key, sms) -> {
            if (sms == null) {
                res.onNext(true);
                return null;
            }
            if (sm.offset == 0 && sm.len == 0) {
                Set<Sm> mergeSms = smMergeMap.remove(sm.mergeKey());
                if (mergeSms != null) {
                    sms.removeAll(mergeSms);
                    Flux.fromIterable(mergeSms)
                            .flatMap(sm0 -> delete(sm0.smKey()))
                            .collectList()
                            .subscribe(b -> res.onNext(true));
                } else {
                    res.onNext(true);
                }
            } else {
                if (sms.contains(sm)) {
                    sms.remove(sm);
                    smMergeMap.computeIfPresent(sm.mergeKey(), (mergeKey0, mergeSms) -> {
                        mergeSms.remove(sm);
                        return mergeSms.isEmpty() ? null : mergeSms;
                    });
                    delete(sm.smKey())
                            .subscribe(b -> res.onNext(true));
                } else {
                    res.onNext(true);
                }
            }
            return sms.isEmpty() ? null : sms;
        });
        return res;
    }

    public static boolean lockTimeout(Sm sm) {
        smBakMap.compute(sm.ip + " " + sm.local, (key, sms) -> {
            if (sms == null) {
                sms = ConcurrentHashMap.newKeySet();
            }
            putSync(sm.smBakKey(), sm);
            sms.add(sm);
            sendNotify(sm);
            return sms;
        });
        return true;
    }

    /**
     * @param key:nsm_sm/ip local/ino/svid/offset-len  nsm_sm_bak/ip local/ino/svid/offset-len
     * @param sm
     * @return
     */
    public static Mono<Boolean> put(String key, Sm sm) {
        return Mono.fromCallable(() -> {
            try {
                String jsonSm = Json.encode(sm);
                MSRocksDB.getRocksDB(Utils.getMqRocksKey()).put(key.getBytes(), jsonSm.getBytes());
                return true;
            } catch (Exception e) {
                log.info("NSM rocks put key:" + key + " error:", e);
                return false;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    public static Mono<Boolean> delete(String key) {
        return Mono.fromCallable(() -> {
            try {
                MSRocksDB.getRocksDB(Utils.getMqRocksKey()).delete(key.getBytes());
                return true;
            } catch (Exception e) {
                log.info("NSM rocks delete key:" + key + " error", e);
                return false;
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    public static boolean putSync(String key, Sm sm) {
        try {
            String jsonSm = Json.encode(sm);
            MSRocksDB.getRocksDB(Utils.getMqRocksKey()).put(key.getBytes(), jsonSm.getBytes());
            return true;
        } catch (Exception e) {
            log.info("NSM rocks putSync key:" + key + " error:", e);
            return false;
        }
    }

    public static boolean deleteSync(String key) {
        try {
            MSRocksDB.getRocksDB(Utils.getMqRocksKey()).delete(key.getBytes());
            return true;
        } catch (Exception e) {
            log.info("NSM rocks deleteSync key:" + key + " error", e);
            return false;
        }
    }

    /**
     * @param prefix:nsm_sm/ip local/  nsm_sm_bak/ip local/
     * @return
     */
    public static boolean deleteIp(String prefix) {
        try (MSRocksIterator iterator = MSRocksDB.getRocksDB(Utils.getMqRocksKey()).newIterator()) {
            iterator.seek(prefix.getBytes());
            while (iterator.isValid() && new String(iterator.key()).startsWith(prefix)) {
                String key = new String(iterator.key());
                iterator.next();
                deleteSync(key);
            }
            return true;
        } catch (Exception e) {
            log.info("NSM rocks delete ip " + prefix + " error", e);
            return false;
        }
    }
}
