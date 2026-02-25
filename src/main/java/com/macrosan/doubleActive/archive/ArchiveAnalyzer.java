package com.macrosan.doubleActive.archive;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.redis.SampleConnection;
import com.macrosan.doubleActive.MainNodeSelector;
import com.macrosan.ec.ErasureClient;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.message.xmlmsg.lifecycle.LifecycleConfiguration;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.serialize.JaxbUtils;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanStream;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.http.HttpClientRequest;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.action.managestream.BucketLifecycleService.BUCKET_BACKUP_RULES;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.DoubleActiveUtil.WriteQueueMaxSize;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.SWITCH_ON;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.SWITCH_SUSPEND;
import static com.macrosan.doubleActive.archive.ArchieveUtils.*;
import static com.macrosan.doubleActive.archive.ArchiveHandler.archiveScanning;
import static com.macrosan.httpserver.MossHttpClient.*;

@Log4j2
public class ArchiveAnalyzer {
    public static final String countX = "countX";
    public static final String countY = "countY";
    // 记录最近一次x=y时x的值，如果这个值和countX一直相等，说明没有归档新的对象，无需再进行数量检测
    public static final String countSum = "countSum";
    public static final String startTime = "startTime";
    public static final String endTime = "endTime";


    // 有此标记的record表示需要在处理成功时进行归档统计
    public static final String ARCHIVE_ANALYZER_KEY = "archiveAnalyzerKey";

    protected static RedisConnPool pool = RedisConnPool.getInstance();

    public static final String mark = File.separator + "archive_count";

    private static ArchiveAnalyzer instance;

    private SampleConnection connection;

    private UnicastProcessor<Tuple2<String, Long>> processor;

    static final ConcurrentHashSet<String> kset = new ConcurrentHashSet<>();

    static final AtomicBoolean connectionInUse = new AtomicBoolean();

    private UnicastProcessor<String> processorTrans;

    public static ArchiveAnalyzer getInstance() {
        if (instance == null) {
            instance = new ArchiveAnalyzer();
        }

        return instance;
    }

    public void init() {
        connection = new SampleConnection(RedisConnPool.getInstance().getMainNodes(REDIS_TASKINFO_INDEX));
        processor = UnicastProcessor.create(Queues.<Tuple2<String, Long>>unboundedMultiproducer().get());
        processor.publishOn(SCAN_SCHEDULER)
                .flatMap(tuple2 -> {
                    // 保证同时只启用一个redis事务
                    if (!connectionInUse.compareAndSet(false, true)) {
                        Mono.delay(Duration.ofSeconds(5)).publishOn(SCAN_SCHEDULER).subscribe(s -> processor.onNext(tuple2));
                        return Mono.just(false);
                    }

                    String k = tuple2.var1;
                    Long t1 = tuple2.var2;
                    RedisReactiveCommands<String, String> reactive2 = connection.reactive();
                    String[] countSum0 = new String[1];
                    return reactive2.hget(k, countX).defaultIfEmpty("0").map(Long::parseLong)
                            .filter(x -> x.equals(t1))
                            .doOnNext(x -> reactive2.hget(k, countSum).defaultIfEmpty("0").doOnNext(cs -> countSum0[0] = cs))
                            .flatMap(x -> reactive2.watch(k))
                            .flatMap(s -> reactive2.multi()
                                    .doOnSuccess(s0 -> {
                                        reactive2.hset(k, countSum, String.valueOf(t1)).subscribe();
                                        reactive2.hset(k, countY, String.valueOf(t1)).subscribe();
                                        setEndTime(k.split(mark)[0], reactive2).subscribe();
                                    })
                                    .flatMap(v -> reactive2.exec())
                                    .map(transactionResult -> {
                                        if (transactionResult.wasDiscarded()) {
                                            log.warn("Transaction discarded for key: {}", k);
                                            return false;
                                        }
                                        return true;
                                    })
                            )
                            .timeout(Duration.ofSeconds(10))
                            .flatMap(b -> {
                                if (b) {
                                    log.info("scan rule complete, ruleKey:{}, dealt object amount: {}", k.split(mark)[0], t1);
                                    return Mono.just(true);
                                }
                                return Mono.just(false);
                            })
                            .onErrorResume(e -> {
                                log.error("check err, {}", tuple2, e);
                                return Mono.just(false);
                            })
                            .flatMap(b -> {
                                if (!b) {
                                    // 出现错误，回退countSum，之后将由checkRotation重新发起。
                                    String s = countSum0[0];
                                    if (StringUtils.isNotBlank(s)) {
                                        return reactive2.hset(k, countSum, s)
                                                .doOnError(e -> log.error("reset contSum: {} {}", k, s, e))
                                                .map(l -> true);
                                    }
                                }
                                return Mono.just(true);
                            })
                            .doFinally(s -> {
                                kset.remove(k);
                                connectionInUse.compareAndSet(true, false);
                            });
                })
                .doOnError(e -> log.error("process err, ", e))
                .subscribe();

        // 转化旧key
        processorTrans = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        processorTrans.publishOn(SCAN_SCHEDULER)
                .flatMap(k -> {
                    if (!connectionInUse.compareAndSet(false, true)) {
                        Mono.delay(Duration.ofSeconds(3)).publishOn(SCAN_SCHEDULER).subscribe(s -> processorTrans.onNext(k));
                        return Mono.just(false);
                    }

                    String analyzerKey = transformAnalyzerKey(k);
                    String kold = k + mark;
                    String knew = analyzerKey + mark;
                    RedisReactiveCommands<String, String> reactive2 = connection.reactive();
                    Map<String, String> oldMap = new HashMap<>();
                    return reactive2.watch(kold, knew)
                            .then(reactive2.hgetall(kold).defaultIfEmpty(new HashMap<>()).doOnNext(oldMap::putAll))
                            .then(reactive2.multi())
                            .doOnSuccess(s0 -> {
                                long t1 = oldMap.containsKey(countX) ? Long.parseLong(oldMap.get(countX)) : 0L;
                                long t2 = oldMap.containsKey(countY) ? Long.parseLong(oldMap.get(countY)) : 0L;
                                long sum = oldMap.containsKey(countSum) ? Long.parseLong(oldMap.get(countSum)) : 0L;
                                reactive2.hincrby(knew, countX, t1).subscribe();
                                reactive2.hincrby(knew, countY, t2).subscribe();
                                if (sum > 0) {
                                    reactive2.hset(knew, countSum, String.valueOf(sum)).subscribe();
                                }
                                if (StringUtils.isNotBlank(oldMap.get(startTime))) {
                                    reactive2.hget(knew, startTime)
                                            .defaultIfEmpty(String.valueOf(Long.MAX_VALUE))
                                            .flatMap(startNew -> {
                                                // start取小值
                                                if (oldMap.get(startTime).compareTo(startNew) < 0) {
                                                    return reactive2.hset(knew, startTime, oldMap.get(startTime));
                                                }
                                                return Mono.just(false);
                                            })
                                            .defaultIfEmpty(false)
                                            .subscribe();
                                }
                                // 由于新的analyzerkey不会在存在旧key时结算，endNew一定不存在。
                                // 因此如果存在endOld，表示升级前就已经结束了该轮归档。
                                if (StringUtils.isNotBlank(oldMap.get(endTime))) {
                                    reactive2.hset(knew, endTime, oldMap.get(endTime)).subscribe();
                                }
                                // 删除旧key
                                reactive2.del(kold).subscribe();
                            })
                            .then(reactive2.exec())
                            .timeout(Duration.ofSeconds(10))
                            .map(transactionResult -> {
                                if (transactionResult.wasDiscarded()) {
                                    log.debug("Transaction2 discarded for key: {}", kold);
                                    return false;
                                }
                                return true;
                            })
                            .onErrorResume(e -> {
                                log.error("check2 err, {}", kold, e);
                                return Mono.just(false);
                            })
                            .doFinally(s -> {
                                kset.remove(k);
                                connectionInUse.compareAndSet(true, false);
                            })
                            .onErrorResume(e -> {
                                log.error("check3 err, {}", kold, e);
                                return Mono.just(false);
                            });
                })
                .doOnError(e -> log.error("processTrans err, ", e))
                .subscribe();

        SCAN_SCHEDULER.schedule(() -> {
            checkRotation();
            sendArchiveCountList();
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * countX的add操作，必须在归档生成差异记录成功之后再addX。（归档存在重复扫描的情况，可能会写同一条记录好几次，x会比实际大）。
     * 低概率出现写记录成功但addX失败，此时x的值会偏小。可以接受最终x比y小
     * <br></br>
     * dealrecord时存在差异记录rewrite的情况，本次record的处理将不会addY，在之后重试或者处理rewrite的记录时再add y。
     * <br>
     * hincrby单线程iops在21000左右。
     */
    public Mono<Boolean> incrCount(String analyzerKey, String countType, long amount) {
        return this.connection.reactive().hincrby(analyzerKey + mark, countType, amount)
                .doOnNext(l -> {
                    if (countX.equals(countType) && l == 1L) {
                        Mono.just(true).publishOn(SCAN_SCHEDULER).flatMap(b -> setStartTime(analyzerKey)).subscribe();
                    }
                })
                .map(l -> true)
                .onErrorResume(e -> {
                    log.error("incrCount err, {} {}", analyzerKey, countType, e);
                    return Mono.just(false);
                });
    }

    public Mono<Boolean> incrCount(String analyzerKey, String countType) {
        return incrCount(analyzerKey, countType, 1L);
    }

    public Mono<Boolean> decrCount(String analyzerKey, String countType) {
        return incrCount(analyzerKey, countType, -1L);
    }

    /**
     * k个删除成功即进行countY+1，可能出现y重复计数（节点掉线重启后，在rabbitmq处理删除前，将本地已经同步的记录重新再处理一遍），视作缺陷，后续自动将xy对齐。
     */
    public static Mono<Boolean> deleteAndCount(UnSynchronizedRecord record, String analyzerKey, List<Tuple3<String, String, String>> vnodeList) {
        return ErasureClient.deleteUnsyncRocketsValue(record.bucket, record.rocksKey(), record, vnodeList, ARCHIVE_ANALYZER_KEY, null)
                .publishOn(SCAN_SCHEDULER)
                .flatMap(i0 -> {
                    if (i0 != 0) {
                        return ArchiveAnalyzer.getInstance().incrCount(analyzerKey, countY);
                    }  else {
                        return Mono.just(false);
                    }
                });
    }

    public void checkRotation() {
        if (!MainNodeSelector.checkIfSyncNode()) {
            SCAN_SCHEDULER.schedule(this::checkRotation, 30, TimeUnit.SECONDS);
            return;
        }

        ScanStream.scan(pool.getReactive(REDIS_TASKINFO_INDEX), new ScanArgs().match("*" + mark))
                .publishOn(SCAN_SCHEDULER)
                .flatMap(k -> {
                    String analyzerKey = k.split(mark)[0];
                    String bucketName = analyzerKey.split(File.separator)[0];
                    int clusterIndex = Integer.parseInt(analyzerKey.split(File.separator)[1]);
                    // 旧格式的归档统计不进入setEndTime流程，需要先转换
                    // 还存在旧格式key的归档统计也不进入setEndTime流程
                    return Mono.just(isNewAnalyzerKey(analyzerKey))
                            .filter(b -> b)
                            .flatMap(b -> {
                                String oldKey = StringUtils.substringBeforeLast(analyzerKey, File.separator);
                                return pool.getReactive(REDIS_TASKINFO_INDEX).exists(oldKey).map(l -> l > 0);
                            })
                            .filter(b -> !b)
                            .flatMap(b -> pool.getReactive(REDIS_BUCKETINFO_INDEX).exists(bucketName))
                            .filter(i -> {
                                if (i > 0) {
                                    return true;
                                } else {
                                    Mono.just(true).publishOn(SCAN_SCHEDULER)
                                            .then(this.connection.reactive().del(k))
                                            .subscribe();
                                    return false;
                                }
                            })
                            // archiveScanning表示归档是否正在进行，也可以用来判断是否在复制的同步时间内
                            // 滤掉目标站点为本地站点的归档统计（本地不会有往本地归档的归档策略，即archiveScanning.get(clusterIndex)==null）
                            .map(i -> archiveScanning.get(clusterIndex) != null && archiveScanning.get(clusterIndex).get())
                            .filter(b -> b)
                            // 正在归档扫描的策略不进入结算
                            .map(b -> BACK_ANALYZER_MAP.containsValue(analyzerKey))
                            .filter(b -> !b)
                            .flatMap(i -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, DATA_SYNC_SWITCH))
                            .publishOn(SCAN_SCHEDULER)
                            .filter(SWITCH_ON::equals)
                            .filter(i -> archiveIsEnable(bucketName, clusterIndex))
                            .map(t -> k);
                })
                .flatMap(k -> pool.getReactive(REDIS_TASKINFO_INDEX).hgetall(k).zipWith(Mono.just(k)))
                .doOnNext(tuple2 -> {
                    String k = tuple2.getT2();
                    try {
                        Map<String, String> map = tuple2.getT1();
                        long t1 = 0L;
                        if (map.containsKey(countX)) {
                            t1 = Long.parseLong(map.get(countX));
                        }
                        long t2 = 0L;
                        if (map.containsKey(countY)) {
                            t2 = Long.parseLong(map.get(countY));
                        }
                        long sum = 0L;
                        if (map.containsKey(countSum)) {
                            sum = Long.parseLong(map.get(countSum));
                        }

                        if (t1 != 0 && sum == t1 && t1 != t2) {
                            // y有可能在本轮统计结束后仍增加，强制统一
                            long finalT = t1;
                            Mono.just(true).publishOn(SCAN_SCHEDULER)
                                    .subscribe(b -> this.connection.reactive().hset(k, countY, String.valueOf(finalT)).subscribe());
                        }

                        synchronized (kset) {
                            if (t1 != 0 && sum < t1 && t1 <= t2 && !kset.contains(k)) {
                                Tuple2<String, Long> t = new Tuple2<>(k, t1);
                                kset.add(k);
                                Mono.delay(Duration.ofSeconds(30)).publishOn(SCAN_SCHEDULER).subscribe(s -> processor.onNext(t));
                            }
                        }

                    } catch (Exception e) {
                        log.error("checkRotation err, {}", k, e);
                    }
                })
                .doFinally(s -> SCAN_SCHEDULER.schedule(this::checkRotation, 30, TimeUnit.SECONDS))
                .doOnError(e -> log.error("checkRotation err, ", e))
                .subscribe();
    }

    public Mono<Boolean> setStartTime(String analyzerKey) {
        return this.connection.reactive().hget(analyzerKey + mark, startTime).defaultIfEmpty("")
                .flatMap(start -> {
                    if (StringUtils.isBlank(start)) {
                        log.info("archive setStartTime: {}", analyzerKey);
                        return this.connection.reactive().hset(analyzerKey + mark, startTime, String.valueOf(System.currentTimeMillis()));
                    }
                    return Mono.just(true);
                })
                .map(s -> true)
                .onErrorResume(e -> {
                    log.error("setStartTime err, {}", analyzerKey, e);
                    return Mono.just(false);
                });
    }

    public Mono<Boolean> setEndTime(String analyzerKey, RedisReactiveCommands<String, String> commands) {
        return commands.hget(analyzerKey + mark, endTime).defaultIfEmpty("")
                .flatMap(end -> {
                    long currentTimeMillis = System.currentTimeMillis();
                    if (StringUtils.isBlank(end) || currentTimeMillis > Long.parseLong(end)) {
                        log.info("archive setEndTime: {}", analyzerKey);
                        return commands.hset(analyzerKey + mark, endTime, String.valueOf(currentTimeMillis));
                    }
                    return Mono.just(true);
                })
                .map(s -> true)
                .onErrorResume(e -> {
                    log.error("setEndTime err, {}", analyzerKey, e);
                    return Mono.just(false);
                });
    }

    public void sendArchiveCountList() {
        if (!MainNodeSelector.checkIfSyncNode()) {
            SCAN_SCHEDULER.schedule(this::sendArchiveCountList, 30, TimeUnit.SECONDS);
            return;
        }

        ScanStream.hscan(pool.getReactive(REDIS_SYSINFO_INDEX), BUCKET_BACKUP_RULES, new ScanArgs().match("*"))
                .publishOn(SCAN_SCHEDULER)
                .flatMap(keyValue -> {
                    String bucketName = keyValue.getKey();
                    return pool.getReactive(REDIS_BUCKETINFO_INDEX).exists(bucketName)
                            .filter(i -> i > 0)
                            .flatMap(tuple2 -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, DATA_SYNC_SWITCH).defaultIfEmpty(""))
                            .filter(datasync -> SWITCH_ON.equals(datasync) || SWITCH_SUSPEND.equals(datasync))
                            .map(b -> {
                                String str = keyValue.getValue();
                                return (LifecycleConfiguration) JaxbUtils.toObject(str);
                            })
                            .zipWith(Mono.just(bucketName));
                })
                .map(tuple2 -> {
                    LifecycleConfiguration lifecycleConfiguration = tuple2.getT1();
                    String bucket = tuple2.getT2();
                    return generateAnalyzerKeySet(bucket, lifecycleConfiguration).var1;
                })
                .collectList()
                .map(list -> {
                    HashSet<String> set = new HashSet<>();
                    for (HashSet<String> set0 : list) {
                        set.addAll(set0);
                    }
                    return set;
                })
                .flatMap(archiveSet -> {
                    Map<String, Map<String, String>> kMap = new ConcurrentHashMap<>();
                    ConcurrentHashSet<String> delKSet = new ConcurrentHashSet<>();
                    MonoProcessor<Boolean> res = MonoProcessor.create();
                    // 扫描所有归档统计：hdel去掉key set中不存在的归档统计、将sourceIndex为本地的归档统计发给其他站点
                    ScanStream.scan(pool.getReactive(REDIS_TASKINFO_INDEX), new ScanArgs().match("*" + mark))
                            .publishOn(SCAN_SCHEDULER)
                            .flatMap(k -> {
                                // 可能在升级时，未升级节点仍有incr操作,导致统计在了旧的analyzerKey名下
                                // 旧格式的analyzerKey将由扫描节点启动另外的线程并入新格式，这里只允许检查并删除新格式的analyzerKey
                                String analyzerKey = k.split(mark)[0];
                                Map<String, Map<String, String>> resMap = new HashMap<>();
                                synchronized (kset) {
                                    if (!isNewAnalyzerKey(analyzerKey) && !kset.contains(k)) {
                                        kset.add(k);
                                        processorTrans.onNext(analyzerKey);
                                        return Mono.just(resMap);
                                    }
                                }
                                String sourceIndex = StringUtils.substringAfterLast(analyzerKey, File.separator);
                                // 对新格式的analyzerKey做处理，准备将源站点是本地站点的统计发送给其他站点
                                if (archiveSet.contains(analyzerKey)) {
                                    if (String.valueOf(LOCAL_CLUSTER_INDEX).equals(sourceIndex)) {
                                        return pool.getReactive(REDIS_TASKINFO_INDEX).hgetall(k).defaultIfEmpty(new HashMap<>())
                                                .map(map -> {
                                                    resMap.put(k, map);
                                                    return resMap;
                                                });
                                    }
                                    return Mono.just(resMap);
                                } else {
                                    // 源站点是本地站点的归档统计才可以删除
                                    if (String.valueOf(LOCAL_CLUSTER_INDEX).equals(sourceIndex)) {
                                        delKSet.add(k);
                                    }
                                    return Mono.just(resMap);
                                }
                            })
                            .doOnNext(kMap::putAll)
                            .doOnComplete(() -> {
                                // 扫描期间不删除多余的归档统计
                                if (!delKSet.isEmpty()) {
                                    vertx.runOnContext(v0 -> {
                                        log.info("delete useless analyzerKey: {}", delKSet);
                                        pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).del(delKSet.toArray(new String[0]));
                                    });
                                }

                                if (kMap.isEmpty()) {
                                    res.onNext(true);
                                    return;
                                }

                                AtomicInteger requestCount = new AtomicInteger(DA_INDEX_IPS_ENTIRE_MAP.size() - 1);
                                for (Map.Entry<Integer, String[]> entry : DA_INDEX_IPS_ENTIRE_MAP.entrySet()) {
                                    Integer clusterIndex = entry.getKey();
                                    if (LOCAL_CLUSTER_INDEX == clusterIndex) {
                                        continue;
                                    }
                                    String[] clusterIps = INDEX_IPS_MAP.get(clusterIndex);
                                    int currentIndex = ThreadLocalRandom.current().nextInt(0, clusterIps.length);
                                    String ip = clusterIps[currentIndex];

                                    HttpClientRequest request = getClient().request(HttpMethod.PUT, DA_PORT, ip, "?putArchiveCountList");
                                    Tuple2<Map<String, Map<String, String>>, ConcurrentHashSet<String>> tuple2 = new Tuple2<>(kMap, delKSet);
                                    byte[] bytes = Json.encode(tuple2).getBytes();
                                    request.putHeader(EXPECT, EXPECT_100_CONTINUE)
                                            .putHeader(CONTENT_LENGTH, String.valueOf(bytes.length))
                                            .putHeader(SYNC_AUTH, PASSWORD);
                                    request.setHost(ip + ":" + DA_PORT);
                                    request.putHeader(CLUSTER_ALIVE_HEADER, ip)
                                            .setTimeout(30 * 1000)
                                            .setWriteQueueMaxSize(WriteQueueMaxSize)
                                            .exceptionHandler(e -> {
                                                log.error("send putArchiveCountList error, {} {}", currentIndex, ip, e);
                                                if (requestCount.decrementAndGet() == 0) {
                                                    res.onNext(true);
                                                }
                                            })
                                            .handler(resp -> {
                                                if (resp.statusCode() != SUCCESS) {
                                                    log.error("send putArchiveCountList fail, {} {}: {}, {}", currentIndex, ip, resp.statusCode(), resp.statusMessage());
                                                }
                                                if (requestCount.decrementAndGet() == 0) {
                                                    res.onNext(true);
                                                }
                                            });
                                    request.putHeader(CONTENT_LENGTH, String.valueOf(bytes.length));
                                    request.write(Buffer.buffer(bytes));
                                    request.end();
                                }

                            })
                            .doOnError(e -> {
                                log.error("sendArchiveCountList error1, ", e);
                                res.onNext(false);
                            })
                            .subscribe();
                    return res;
                })
                .doFinally(s -> Mono.delay(Duration.ofSeconds(30)).publishOn(SCAN_SCHEDULER).subscribe(l -> sendArchiveCountList()))
                .doOnError(e -> log.error("sendArchiveCountList error2, ", e))
                .subscribe();

    }
}
