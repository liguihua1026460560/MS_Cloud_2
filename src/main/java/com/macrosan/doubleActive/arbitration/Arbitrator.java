package com.macrosan.doubleActive.arbitration;


import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.redis.SampleConnection;
import com.macrosan.doubleActive.HeartBeatChecker;
import com.macrosan.doubleActive.MainNodeSelector;
import com.macrosan.doubleActive.arbitration.ArbitratorUtils.VoteReply;
import com.macrosan.ec.Utils;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.reactivex.core.http.HttpClientRequest;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.HeartBeatChecker.isMultiAliveStarted;
import static com.macrosan.doubleActive.arbitration.ArbitratorUtils.DA_PORT_HEADER;
import static com.macrosan.doubleActive.arbitration.ArbitratorUtils.IS_EVALUATING_MASTER;
import static com.macrosan.doubleActive.arbitration.DAVersionUtils.initMasterInfo;
import static com.macrosan.doubleActive.arbitration.DAVersionUtils.isMasterCluster;
import static com.macrosan.httpserver.MossHttpClient.*;

/**
 * 仲裁。以桶为单位判断桶所在的索引池有无异常。
 * 在扫描节点运行。
 * "主从切换发生情况：<br>
 * 1、仲裁者和主站点链路正常，此时无论其他链路情况如何均不切换主站点<br>
 * 2、仲裁者和主站点链路异常，主站点和从站点链路正常：从站点和仲裁者链路正常与否均不切换主站点；<br>
 * 3、仲裁者和主站点链路异常，主站点和从站点链路也异常：从站点和仲裁者链路正常则切换原从站点为主站点，从站点和仲裁者链路异常则认为环境损坏，无法切主，无法进行业务操作"<br>
 *
 * @author fanjunxi
 */
@Log4j2
public class Arbitrator {

    private static Arbitrator instance;

    public static Arbitrator getInstance() {
        if (instance == null) {
            instance = new Arbitrator();
        }
        return instance;
    }


    private static RedisConnPool pool = RedisConnPool.getInstance();

    /**
     * 主站点索引，每秒钟更新。初始值为0。
     */
    public static Integer MASTER_INDEX = 0;

    /**
     * 同步主站点的任期。初始值为1。
     */
    public final static AtomicLong TERM = new AtomicLong(1L);

    public static final Map<Long, Integer> INDEX_TERM_MAP = new ConcurrentHashMap<>();

    /**
     * 手动进行IP漂移的次数。
     */
    public final static AtomicLong FLOAT_COUNT = new AtomicLong(1L);

    public static AtomicBoolean isUnderElection = new AtomicBoolean(false);

    /**
     * 保存在表2，map，保存任期和投票的站点索引。一个任期内有投过票就不允许再投票
     */
    public static final String VOTE_FOR_INDEX = "vote_for_index";

    /**
     * 保存已经投票过的最大的任期
     */
    public static final String LATEST_VOTED_TERM = "latest_voted_term";

    public SampleConnection connectDb2;

    public SampleConnection connectDb0;

    /**
     * 不同站点间的投票间隔，单位秒。
     */
    final int ELEC_INTERVAL = 0;

    /**
     * 主站点分析完成前不允许数据流和管理流的操作。
     * 环境损坏也会改为true，只在主站点信息确认后改为false
     */
    public final static AtomicBoolean isEvaluatingMaster = new AtomicBoolean(true);

    static Map<Integer, String[]> ABT_INDEX_IPS_ENTIRE_MAP = new ConcurrentHashMap<>();

    public static String ABT_IP;

    public static int ABT_PORT = 2323;

    public static int ABT_HTTPS_PORT = 2424;

    public static int getAbtPort() {
        if (IS_SSL_SYNC.get()) {
            return ABT_HTTPS_PORT;
        }
        return ABT_PORT;
    }

    /**
     * 双活站点和仲裁者数量之和
     */
    static int ABT_CLUSTERS_AMOUNT = Integer.MAX_VALUE;

    public Mono<Boolean> init() {

        Optional.ofNullable(connectDb2).ifPresent(conn -> conn.getConnection().close());
        Optional.ofNullable(connectDb0).ifPresent(conn -> conn.getConnection().close());

        connectDb2 = new SampleConnection(RedisConnPool.getInstance().getMainNodes(REDIS_SYSINFO_INDEX));
        connectDb0 = new SampleConnection(RedisConnPool.getInstance().getMainNodes(REDIS_ROCK_INDEX));

        ABT_INDEX_IPS_ENTIRE_MAP.clear();
        ABT_INDEX_IPS_ENTIRE_MAP.putAll(DA_INDEX_IPS_ENTIRE_MAP);
        // 仲裁的index默认是比最大的那个双活的index+1
        int abtIndex = -1;
        for (Integer i : DA_INDEX_IPS_ENTIRE_MAP.keySet()) {
            if (i > abtIndex) {
                abtIndex = i;
            }
        }
        abtIndex++;

        String abtIps = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, ArbitratorUtils.ARBITRATOR_IP);
        if (StringUtils.isNotBlank(abtIps) && DA_INDEX_IPS_ENTIRE_MAP.size() % 2 == 0) {
            // 仲裁者的setArbitrator请求通过，且当前非3DC站点，将ARBITRATOR_IP添加进来。
            // 三个DA站点，满足奇数个仲裁模块，添加仲裁者会失效。
            String[] split = abtIps.split(";");
            ABT_INDEX_IPS_ENTIRE_MAP.put(abtIndex, split);
            ABT_IP = abtIps;
            ABT_CLUSTERS_AMOUNT = ABT_INDEX_IPS_ENTIRE_MAP.size();
        }

        // 集群中站点和仲裁者数量之和为偶数时跳过，此时为优化前的双活。需要等到仲裁者部署请求进来后才能初始化仲裁模块。
        if (IS_THREE_SYNC || ABT_INDEX_IPS_ENTIRE_MAP.size() < 3 || ABT_INDEX_IPS_ENTIRE_MAP.size() % 2 == 0) {
            isEvaluatingMaster.set(false);
            if ("master".equals(Utils.getRoleState())) {
                Mono.just(true).publishOn(SCAN_SCHEDULER)
                        .subscribe(s -> RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, IS_EVALUATING_MASTER, "0"));
            }
            return Mono.just(false);
        }
        log.info("start arbitrator module");

        String abtPortStr = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, ArbitratorUtils.ARBITRATOR_PORT);
        if (StringUtils.isNotBlank(abtPortStr)) {
            ABT_PORT = Integer.parseInt(abtPortStr);
        }

        String abtHttpsPortStr = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, ArbitratorUtils.ARBITRATOR_HTTPS_PORT);
        if (StringUtils.isNotBlank(abtHttpsPortStr)) {
            ABT_HTTPS_PORT = Integer.parseInt(abtHttpsPortStr);
        }

        // 未完成firstCheckMaster前设置为1，防止流量进来。
        if (MainNodeSelector.checkIfSyncNode()) {
            ArbitratorUtils.updateEvaluatingMasterStatus(1, true);
        }

        initMasterInfo();
        return informDAPort();
    }

    /**
     * 本地与主站点心跳情况，心跳异常则发起投票。先给自己投一票再发给除原主外其他的站点。
     * <p>
     * 如果一直没有收到半数以上节点的响应将一直执行。
     *
     * @param curTerm 发起选举时所处的任期，可以等同于原主站点的term
     */
    Set<Long> termSet = new ConcurrentHashSet<>();

    public void runElection(long curTerm) {
        // 集群中无复制站点跳过。暂不支持独立的仲裁者。
        if (!DAVersionUtils.isStrictConsis()) {
            return;
        }
        // 同一任期的选举一个站点只能发起一次
        if (termSet.contains(curTerm)) {
            log.info("extra:already has this term. {}", curTerm);
            return;
        }

        if (!HeartBeatChecker.heartBeatIsNormal(LOCAL_CLUSTER_INDEX)) {
            log.info("local heartbeat is abnormal. stop election. ");
            return;
        }
        // 可能选举过程中断电切了扫描节点，redis中的election_status是1。所以新的扫描节点第一次进来这里，不从redis中拿electionStatus。
        if (isUnderElection.get()) {
            // 选举过程中别的站点升主成功又断连了，会有新的选举流程（curTerm更大），等待本次选举完成后再进行
            log.info("underElection already. ");
            Mono.delay(Duration.ofSeconds(ELEC_INTERVAL)).publishOn(SCAN_SCHEDULER).subscribe(s -> runElection(curTerm));
            return;
        }

        // 已经更新过主站点，则本次选举取消
        long termNow = Long.parseLong(pool.getCommand(REDIS_ROCK_INDEX).get(ArbitratorUtils.DA_TERM));
        if (curTerm < termNow) {
            log.info("term updated before election. cancel.");
            return;
        }

        log.info("start arbitrator election. ");
        termSet.add(curTerm);
        ArbitratorUtils.setElectionStatus(1);
        UnicastProcessor<Integer> processor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        UnicastProcessor<Integer> replyProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        AtomicInteger approveCount = new AtomicInteger();
        AtomicInteger replyCount = new AtomicInteger();
        String nextTermStr = String.valueOf(curTerm + 1);
        String localClusterIndexStr = String.valueOf(LOCAL_CLUSTER_INDEX);
        List<Integer> approvedClusterList = new ArrayList<>();

        replyProcessor.publishOn(SCAN_SCHEDULER)
                .doFinally(s -> termSet.remove(curTerm))
                .subscribe(i -> {
                    approveCount.addAndGet(i);
                    // 除了本地站点和原主站点都已经返回过i(连不上的也会返回0)，则complete
                    if (replyCount.incrementAndGet() >= (ABT_CLUSTERS_AMOUNT - 2)) {
                        replyProcessor.onComplete();
                    }
                }, e -> log.error("", e), () -> {
                    if (approveCount.get() >= (ABT_CLUSTERS_AMOUNT / 2 + 1)) {
                        log.info("arbitrator election win. index:{} term: {}", localClusterIndexStr, nextTermStr);
                        informChangeMasterCluster(LOCAL_CLUSTER_INDEX, curTerm + 1, approvedClusterList)
                                .map(b -> {
                                    if (b) {
                                        return ArbitratorUtils.informNode("0");
                                    }
                                    return b;
                                })
                                .subscribe(b -> ArbitratorUtils.setElectionStatus(0));
                    } else {
                        log.info("arbitrator election lose. index:{} term: {}", localClusterIndexStr, nextTermStr);
                        // 存在无票的情况，此时结束选举状态
                        if (approvedClusterList.isEmpty()) {
                            ArbitratorUtils.setElectionStatus(0);
                            return;
                        }
                        MonoProcessor<Boolean> res = MonoProcessor.create();
                        ArbitratorUtils.refund(approvedClusterList, curTerm, res).subscribe(b -> ArbitratorUtils.setElectionStatus(0));
                                /*
                                 因为每个站点只能投一票，三活以上的环境可能会出现一轮选举没有产生赢家的情况。此时需要再开始一轮选举，发起时间随机。
                                 */
                    }
                });

        AtomicInteger retryCount = new AtomicInteger();
        processor.publishOn(SCAN_SCHEDULER).subscribe(i -> {
            //先投票给自己
            ArbitratorUtils.voteReplyMono(nextTermStr, localClusterIndexStr).subscribe(reply -> {
                switch (reply) {
                    case RETRY:
                        log.info("self vote RETRY, retry election. ");
                        processor.onNext(1);
                        break;
                    case DISAPPROVE:
                        log.info("self vote DISAPPROVE, stop election. ");
                        replyProcessor.onComplete();
                        processor.onComplete();
                        break;
                    case APPROVE:
                        log.info("self vote APPROVE");
                        approveCount.incrementAndGet();
                        approvedClusterList.add(LOCAL_CLUSTER_INDEX);

                        for (Entry<Integer, String[]> entry : ABT_INDEX_IPS_ENTIRE_MAP.entrySet()) {
                            Integer clusterIndex = entry.getKey();
                            String[] ips = entry.getValue();

                            // 跳过本地站点和原主节点
                            if (clusterIndex.equals(LOCAL_CLUSTER_INDEX) || clusterIndex.equals(MASTER_INDEX)) {
                                continue;
                            }
                            UnicastProcessor<Integer> httpProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
                            AtomicInteger retryTime = new AtomicInteger();
                            httpProcessor.publishOn(SCAN_SCHEDULER).doOnNext(ipIndex -> {
                                String ip = ips[ipIndex];
                                int port = ip.equals(ABT_IP) ? getAbtPort() : DA_PORT;
                                int nextIpIndex = (ipIndex + 1) % ips.length;
                                HttpClientRequest request = HeartBeatChecker.getClient().request(HttpMethod.GET, port, ip, "?vote");
                                request.setTimeout(3_000)
                                        .setHost(ip + ":" + port)
                                        .putHeader(SYNC_AUTH, PASSWORD)
                                        .putHeader(ArbitratorUtils.DA_TERM_HEADER, nextTermStr)
                                        .putHeader(ArbitratorUtils.MASTER_CLUSTER_INDEX_HEADER, localClusterIndexStr)
                                        .exceptionHandler(e -> {
                                            log.error("runElection request to cluster {} ip {} error, ", clusterIndex, ip, e);
                                            request.reset();
                                            if (retryTime.incrementAndGet() >= ips.length) {
                                                log.error("all ip of cluster {} is down. ", clusterIndex, e);
                                                replyProcessor.onNext(0);
                                                httpProcessor.onComplete();
                                                return;
                                            }
                                            httpProcessor.onNext(nextIpIndex);
                                        })
                                        .handler(resp -> {
                                            VoteReply voteReply = VoteReply.valueOf(resp.getHeader("vote_reply"));
                                            log.info("vote result: {}, cluster:{}, ip:{}", voteReply.name(), clusterIndex, ip);
                                            switch (voteReply) {
                                                case APPROVE:
                                                    replyProcessor.onNext(1);
                                                    approvedClusterList.add(clusterIndex);
                                                    httpProcessor.onComplete();
                                                    break;
                                                case DISAPPROVE:
                                                    replyProcessor.onNext(0);
                                                    httpProcessor.onComplete();
                                                    break;
                                                case RETRY:
                                                    // 不断重试过程中主站点链路恢复，则不需要再升主，中断流程。
                                                    if (HeartBeatChecker.heartBeatIsNormal(MASTER_INDEX)) {
                                                        log.info("stop election. Main Cluster resume.");
                                                        replyProcessor.onComplete();
                                                        return;
                                                    }

                                                    // 重试次数超过3次则此次选举中止，防止一直处于选举状态导致其他站点的evaluate请求一直不成功
                                                    if (retryCount.incrementAndGet() >= 3) {
                                                        replyProcessor.onNext(0);
                                                        httpProcessor.onComplete();
                                                        break;
                                                    }

                                                    Mono.delay(Duration.ofSeconds(1)).subscribe(s -> httpProcessor.onNext(nextIpIndex));
                                                    break;
                                            }
                                        })
                                        .end();

                            }).subscribe();
                            httpProcessor.onNext(ThreadLocalRandom.current().nextInt(ips.length));
                        }
                }
            }, e -> log.error("self vote error, ", e));
        });

        Mono.delay(Duration.ofSeconds(ELEC_INTERVAL)).subscribe(s -> processor.onNext(1));
    }


    /**
     * 获得超半数投票后本站点发给其他站点自己要升主。
     * 半数以上站点返回了成功则修改自身的状态为主，否则将一直重试。
     * <p>
     * 其他站点更改了主站点索引会发来getDAVersion请求，在升主前不回应，保证DAVersion统一——
     * 因为如果先将自己设置成为主站点，则发给自己的流量会由本地生成DAVersion，其他站点可能还是使用旧term的DAVersion。
     *
     * @param newMasterIndex      新的主站点索引
     * @param approvedClusterList 之前投票给自己的站点，包括LOCAL_CLUSTER_INDEX
     */
    Mono<Boolean> informChangeMasterCluster(int newMasterIndex, long newTerm, List<Integer> approvedClusterList) {
        MonoProcessor<Boolean> res = MonoProcessor.create();

        UnicastProcessor<Integer> replyProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        AtomicInteger replyCount = new AtomicInteger(1);
        AtomicInteger successCount = new AtomicInteger(1);
        AtomicBoolean isComplete = new AtomicBoolean();
        replyProcessor.publishOn(SCAN_SCHEDULER).subscribe(i -> {
            successCount.addAndGet(i);
            if (replyCount.incrementAndGet() >= approvedClusterList.size()) {
                replyProcessor.onComplete();
            }
        }, e -> log.error(""), () -> {
            isComplete.set(true);
            if (successCount.get() >= approvedClusterList.size()) {
                log.info("informChangeMasterCluster compelte, approveList:{}", approvedClusterList);
                ArbitratorUtils.upgradeMasterRecursion(newMasterIndex, newTerm, res);
            } else {
                log.error("informChangeMasterCluster failed, some clusters vote to a new term. ");
                res.onNext(false);
            }
        });

        for (Entry<Integer, String[]> entry : ABT_INDEX_IPS_ENTIRE_MAP.entrySet()) {
            Integer clusterIndex = entry.getKey();
            String[] ips = entry.getValue();
            if (clusterIndex.equals(LOCAL_CLUSTER_INDEX) || !approvedClusterList.contains(clusterIndex)) {
                continue;
            }
            UnicastProcessor<Integer> httpProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
            httpProcessor.publishOn(SCAN_SCHEDULER).subscribe(ipIndex -> {
                String ip = ips[ipIndex];
                int port = ip.equals(ABT_IP) ? getAbtPort() : DA_PORT;
                int nextIpIndex = (ipIndex + 1) % ips.length;
                HttpClientRequest request = HeartBeatChecker.getClient().request(HttpMethod.GET, port, ip, "?changeMasterCluster");
                request.setTimeout(5000)
                        .setHost(ip + ":" + port)
                        .putHeader(SYNC_AUTH, PASSWORD)
                        .putHeader(ArbitratorUtils.MASTER_CLUSTER_INDEX_HEADER, String.valueOf(newMasterIndex))
                        .putHeader(ArbitratorUtils.DA_TERM_HEADER, String.valueOf(newTerm))
                        .exceptionHandler(e -> {
                            log.error("changeMasterCluster request to cluster {} ip {} error, ", clusterIndex, ip, e);
                            request.reset();
                            httpProcessor.onNext(nextIpIndex);
                        })
                        .handler(resp -> {
                            log.info("informChangeMaster result: cluster:{}, res:{}", clusterIndex, resp.statusCode());
                            // 已有半数以上站点返回成功，不用再管其他站点的结果
                            if (isComplete.get()) {
                                httpProcessor.onComplete();
                                return;
                            }
                            switch (resp.statusCode()) {
                                case SUCCESS:
                                    replyProcessor.onNext(1);
                                    break;
                                case BAD_REQUEST_REQUEST:
                                    // 已经新投过票了。似乎不会在三站点出现。
                                    log.error("Already vote a new trem.");
                                    replyProcessor.onNext(0);
                                    break;
                                case INTERNAL_SERVER_ERROR:
                                    httpProcessor.onNext(ipIndex);

                            }
                            httpProcessor.onComplete();
                        })
                        .end();
            });
            httpProcessor.onNext(ThreadLocalRandom.current().nextInt(ips.length));
        }
        return res;
    }

    public static AtomicBoolean SERVICE_AVAIL = new AtomicBoolean();

    /**
     * 初始化时确认主站点信息，完成前不允许多站点相关的请求通过
     */
    public void firstCheckMaster() {
        // 集群中无复制站点跳过。暂不支持独立的仲裁者。
        if (!DAVersionUtils.isStrictConsis()) {
            SERVICE_AVAIL.set(true);
            return;
        }

        if (isMultiAliveStarted && MainNodeSelector.checkIfSyncNode()) {
            ArbitratorUtils.evaluateMasterIndex(new AtomicBoolean())
                    .flatMap(b -> {
                        MonoProcessor<Boolean> res = MonoProcessor.create();
                        ArbitratorUtils.upgradeMasterRecursion(MASTER_INDEX, TERM.get(), res);
                        return res;
                    })
                    .doOnNext(b -> ArbitratorUtils.updateEvaluatingMasterStatus(0, true))
                    .doOnNext(b -> {
                        if (LOCAL_CLUSTER_INDEX.equals(MASTER_INDEX)) {
                            isMasterCluster.compareAndSet(false, true);
                        } else {
                            isMasterCluster.compareAndSet(true, false);
                        }
                        SERVICE_AVAIL.set(true);
                    })
                    .doOnNext(b -> log.info("First check Master Index info complete."))
                    .subscribe();
        } else {
            SERVICE_AVAIL.set(true);
        }
    }

    /**
     * moss端口号又或者要更改同步的ssl，都要通知仲裁者。
     */
    public Mono<Boolean> informDAPort() {
        if (!MainNodeSelector.checkIfSyncNode()) {
            return Mono.just(true);
        }

        MonoProcessor<Boolean> res = MonoProcessor.create();
        UnicastProcessor<Integer> httpProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        httpProcessor.publishOn(SCAN_SCHEDULER)
                .subscribe(i -> {
                    AtomicBoolean hasEnd = new AtomicBoolean();
                    HttpClientRequest request = HeartBeatChecker.getClient().request(HttpMethod.GET, getAbtPort(), ABT_IP, "?informDAPort");
                    request.setTimeout(3000)
                            .setHost(ABT_IP + ":" + getAbtPort())
                            .putHeader(DA_PORT_HEADER, String.valueOf(DA_PORT))
                            .putHeader("isSSL", String.valueOf(IS_SSL_SYNC.get()))
                            .putHeader(SYNC_AUTH, PASSWORD)
                            .exceptionHandler(e -> {
                                if (hasEnd.compareAndSet(false, true)) {
                                    log.error("informDAPort error, {}, {}", DA_PORT, IS_SSL_SYNC.get(), e);
                                    request.reset();
                                    Mono.delay(Duration.ofSeconds(5)).publishOn(SCAN_SCHEDULER).subscribe(s -> httpProcessor.onNext(1));
                                }
                            })
                            .handler(resp -> {
                                if (resp.statusCode() != SUCCESS) {
                                    Mono.delay(Duration.ofSeconds(5)).publishOn(SCAN_SCHEDULER).subscribe(s -> httpProcessor.onNext(1));
                                    return;
                                }
                                res.onNext(true);
                                log.info("informDAPort complete.");
                                httpProcessor.onComplete();
                            })
                            .end();
                });
        httpProcessor.onNext(1);
        return res;
    }

}
