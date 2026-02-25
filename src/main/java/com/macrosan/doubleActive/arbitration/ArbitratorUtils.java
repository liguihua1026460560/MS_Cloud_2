package com.macrosan.doubleActive.arbitration;

import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.doubleActive.HeartBeatChecker;
import com.macrosan.doubleActive.MainNodeSelector;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.SshClientUtils;
import io.lettuce.core.ScanStream;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.reactivex.core.http.HttpClientRequest;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.constants.ServerConstants.SUCCESS;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.HeartBeatChecker.siteRoleStateChange;
import static com.macrosan.doubleActive.arbitration.Arbitrator.*;
import static com.macrosan.doubleActive.arbitration.ArbitratorUtils.VoteReply.*;
import static com.macrosan.doubleActive.arbitration.DAVersionUtils.daVersionPrefix;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.FRESH_DA_TERM;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.START_ARBITRATOR;
import static com.macrosan.httpserver.MossHttpClient.*;

/**
 * @author fanjunxi
 */
@Log4j2
public class ArbitratorUtils {
    private static RedisConnPool pool = RedisConnPool.getInstance();

    public static final String DA_TERM = "DA_TERM";
    public static final String MASTER_CLUSTER_INDEX = "MASTER_INDEX";
    public static final String DA_VERSION_PREFIX = "DA_VERSION_PREFIX";
    public static final String ELECTION_STATUS = "election_status";
    public static final String DA_TERM_HEADER = "da_term_header";
    public static final String IP_FLOAT_COUNT_HEADER = "ip_float_count_header";
    public static final String MASTER_CLUSTER_INDEX_HEADER = "master_cluster_index_header";
    public static final String ADD_MIP_MAP_HEADER = "add_mip_map_header";
    public static final String IS_EVALUATING_MASTER = "is_evaluating_master";
    /**
     * 表2，master_cluster，记录仲裁者ip，分号分隔
     */
    public static final String ARBITRATOR_IP = "arbitrator_ip";
    public static final String ARBITRATOR_IP_HEADER = "arbitrator_ip_header";

    public static final String ARBITRATOR_PORT = "arbitrator_port";
    public static final String ARBITRATOR_HTTPS_PORT = "arbitrator_https_port";
    public static final String ARBITRATOR_PORT_HEADER = "arbitrator_port_header";
    public static final String ARBITRATOR_HTTPS_PORT_HEADER = "arbitrator_https_port_header";
    public static final String ARBITRATOR_SPEC_IP = "arbitrator_spec_ip";
    public static final String ARBITRATOR_SPEC_IPS = "arbitrator_spec_ips";
    public static final String ARBITRATOR_SPEC_ETH = "arbitrator_spec_eth";

    public static final String ARBITRATOR_STATUS_EACH = "arbitrator_status_each";

    public static final String ARBITRATOR_STATUS = "arbitrator_status";

    public static final String ARBITRATOR_ADMIT_EACH = "arbitrator_admit_each";

    public static final String ARBITRATOR_ADMIT = "arbitrator_admit";


    public static final String DA_PORT_HEADER = "da_port_header";

    public static final String DA_STATUS_HEADER = "da_status_header";

    public static final String DA_LINK_HEADER = "da_link_header";
    /**
     * 1表示仲裁者发来了更改Ip或port的请求，需要重启仲裁模块
     */
    public static final String ARBITRAOTR_CHANGED = "arbitrator_changed";

    public enum VoteReply {
        APPROVE,
        DISAPPROVE,
        RETRY;
    }

    /**
     * 修改redis中的选举状态。只能由选举站点（扫描节点）调用。
     *
     * @param status 0表示未在选举中；1表示在。
     */
    public static void setElectionStatus(int status) {
        isUnderElection.set(status == 1);
        Mono.just(true).publishOn(SCAN_SCHEDULER)
                .subscribe(s -> RedisConnPool.getInstance().getShortMasterCommand(REDIS_ROCK_INDEX).set(ELECTION_STATUS, String.valueOf(status)));
    }

    /**
     * 使用独立的redis连接主节点。使用redis事务保证同时来的投票请求不会交叉混乱。
     * <p>
     * 1. 自身的任期如果大于等于本次投票请求中的任期则拒绝投票。
     * <p>
     * 2. 一个站点在一次任期内只允许给一个站点投票。
     *
     * @param termVotedStr  要投票的站点所处任期
     * @param indexVotedStr 要投票的站点索引
     * @return 投票结果
     */
    public static Mono<VoteReply> voteReplyMono(String termVotedStr, String indexVotedStr) {
        // 如果本站点和主站点的心跳正常，不会投票通过，而是让发起选举的站点重试。防止切主到非最佳的站点。（如：双活链路断开但主站点健康状态正常，此时从站点无需升主）
        if (HeartBeatChecker.heartBeatIsNormal(MASTER_INDEX)) {
            // 非主站点发起选举时主站点心跳恢复，中断选举。
            log.info("master cluster heatbeat recoverd. {}", MASTER_INDEX);
            if (indexVotedStr.equals(String.valueOf(LOCAL_CLUSTER_INDEX))) {
                return Mono.just(DISAPPROVE);
            }
            return Mono.just(RETRY);
        }

        RedisReactiveCommands<String, String> reactive = Arbitrator.getInstance().connectDb2.reactive();
        return reactive.watch(VOTE_FOR_INDEX, LATEST_VOTED_TERM)
                .publishOn(SCAN_SCHEDULER)
                .flatMap(s -> reactive.hget(VOTE_FOR_INDEX, termVotedStr).defaultIfEmpty("-1")
                        .zipWith(reactive.get(LATEST_VOTED_TERM).defaultIfEmpty("-1")))
                .flatMap(tuple2 -> {
                    String index = tuple2.getT1();
                    String latestTerm = tuple2.getT2();
//                    // 该term下有记录表示已经投过站点
                    if (!index.equals("-1")) {
                        reactive.unwatch();
                        return Mono.just(indexVotedStr.equals(index) ? APPROVE : DISAPPROVE);
                    }
                    // 最近投票的任期大于等于本次请求中的任期则返回disapprove
                    if (Long.parseLong(latestTerm) >= Long.parseLong(termVotedStr)) {
                        reactive.unwatch();
                        return Mono.just(DISAPPROVE);
                    }
                    return reactive.multi()
                            .publishOn(SCAN_SCHEDULER)
                            .doOnSuccess(s -> {
                                // 和后续subscribe里的oncomplete不冲突
                                reactive.hset(VOTE_FOR_INDEX, termVotedStr, indexVotedStr).subscribe();
                                reactive.set(LATEST_VOTED_TERM, termVotedStr).subscribe();
                            })
                            .flatMap(s -> reactive.exec())
                            .map(transactionResult -> {
                                log.info("vote to {}, {} transactionResult:{}", indexVotedStr, termVotedStr, transactionResult.wasDiscarded());
                                if (transactionResult.wasDiscarded()) {
                                    return RETRY;
                                } else {
                                    return APPROVE;
                                }
                            });
                })
                .onErrorResume(e -> {
                    log.error("voteReply error2, term:{}, index{}", termVotedStr, indexVotedStr, e);
                    return Mono.just(RETRY);
                });
    }

    /**
     * 退票给所有投过自己的站点。将投票记录还原成原来的主站点。
     * 双活目前只有自己投自己的票需要退。因为不存在自己没投自己，别人投了自己的情况，而只要有来自外部的一票投了自己则必定会选举胜出。
     */
    public static Mono<Boolean> refund(List<Integer> approvedCluster, long curTerm, MonoProcessor<Boolean> res) {
        String termVotedStr = String.valueOf(curTerm + 1);
        for (int i : approvedCluster) {
            if (i == LOCAL_CLUSTER_INDEX) {
                RedisReactiveCommands<String, String> reactive2 = Arbitrator.getInstance().connectDb2.reactive();
                reactive2.watch(VOTE_FOR_INDEX, LATEST_VOTED_TERM)
                        .publishOn(SCAN_SCHEDULER)
                        .flatMap(s -> reactive2.multi())
                        .doOnSuccess(s -> {
                            reactive2.hdel(VOTE_FOR_INDEX, termVotedStr).subscribe();
                            reactive2.set(LATEST_VOTED_TERM, String.valueOf(curTerm)).subscribe();
                        })
                        .flatMap(s -> reactive2.exec())
                        .map(transactionResult -> !transactionResult.wasDiscarded())
                        .onErrorResume(e -> {
                            log.error("refund error, termVotedStr:{}, curTerm:{} ", termVotedStr, curTerm, e);
                            return Mono.just(false);
                        })
                        .subscribe(b -> {
                            if (b) {
                                log.info("refund success, termVotedStr:{}, curTerm:{} ", termVotedStr, curTerm);
                                res.onNext(true);
                            } else {
                                refund(approvedCluster, curTerm, res);
                            }
                        });


            } else {
                // 其他站点
            }
        }
        return res;
    }

    /**
     * 出现新的主站点时更新本地的主站点信息。
     * 只允许更新为更大的任期。
     */
    public static Mono<Boolean> upgradeMaster(int newMasterIndex, long newTerm) {
        final long[] formerTerm = new long[1];
        final String[] formerMasterIndex = new String[1];
        AtomicBoolean redisHasChanged = new AtomicBoolean();
        return pool.getReactive(REDIS_ROCK_INDEX).get(DA_TERM).defaultIfEmpty("-1")
                .zipWith(pool.getReactive(REDIS_ROCK_INDEX).get(MASTER_CLUSTER_INDEX).defaultIfEmpty("-1"))
                .publishOn(SCAN_SCHEDULER)
                .flatMap(tuple -> {
                    long term = Long.parseLong(tuple.getT1());
                    String masterIndex = tuple.getT2();
                    formerTerm[0] = term;
                    formerMasterIndex[0] = masterIndex;
                    if (newTerm < term ||
                            (newTerm == term && String.valueOf(newMasterIndex).equals(masterIndex))) {
                        // 已有的任期更大或者待更改的信息无变化，认为成功。
                        log.info("no need to upgradeMaster. curTerm:{}, newTerm:{}, newMasterIndex:{}", term, newTerm, newMasterIndex);
                        return Mono.just(true);
                    } else {
                        RedisReactiveCommands<String, String> reactive0 = Arbitrator.getInstance().connectDb0.reactive();
                        return reactive0.watch(DA_TERM, MASTER_CLUSTER_INDEX)
                                .publishOn(SCAN_SCHEDULER)
                                .flatMap(s -> reactive0.multi())
                                .doOnSuccess(s -> {
                                    // 必须同时更新，防止其他站点拿到的TERM是新的但DA_VERSION_PREFIX还没更新，从而导致拼成的DASyncStamp很大
                                    reactive0.set(DA_TERM, String.valueOf(newTerm)).subscribe();
                                    reactive0.set(MASTER_CLUSTER_INDEX, String.valueOf(newMasterIndex)).subscribe();
                                    reactive0.set(DA_VERSION_PREFIX, "0").subscribe();
                                })
                                .flatMap(s -> reactive0.exec())
                                .map(transactionResult -> !transactionResult.wasDiscarded())
                                .publishOn(SCAN_SCHEDULER)
                                .doOnNext(b -> {
                                    if (b) {
                                        redisHasChanged.set(true);
                                        // 更新redis中的master_cluster
                                        Map<String, String> map = new HashMap<>();
                                        map.put(CLUSTER_NAME, INDEX_NAME_MAP.get(newMasterIndex));

                                        StringBuilder stringBuilder = new StringBuilder();
                                        boolean isFirst = true;
                                        for (String ip : INDEX_IPS_ENTIRE_MAP.get(newMasterIndex)) {
                                            if (isFirst) {
                                                isFirst = false;
                                                stringBuilder.append(ip);
                                                continue;
                                            }
                                            stringBuilder.append(",");
                                            stringBuilder.append(ip);
                                        }
                                        map.put(CLUSTER_IPS, stringBuilder.toString());

                                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hmset(MASTER_CLUSTER, map);

                                        // 更新mysql中的role
                                        String host = ServerConfig.getInstance().getMasterVip1();
                                        StringBuilder commandSb = new StringBuilder();
                                        for (Map.Entry<Integer, String> entry : INDEX_NAME_MAP.entrySet()) {
                                            Integer index = entry.getKey();
                                            String clusterName = entry.getValue();
                                            String role = "0";
                                            if (index == newMasterIndex) {
                                                role = "1";
                                            }
                                            String command = String.format("update site_info set role=%s where site_name='%s';", role, clusterName);
                                            commandSb.append(command);
                                        }
                                        String execCommand = String.format("mysql -uroot -p123456 -h%s moss_web -e \"%s\"",
                                                host,
                                                commandSb.toString());
                                        log.info("exec: {}", execCommand);
                                        Tuple2<String, String> execTuple2 = SshClientUtils.exec(execCommand, true);
                                        if (StringUtils.isNotBlank(execTuple2.var2)) {
                                            throw new RuntimeException("exec MySQL command fail. " + execTuple2.var2);
                                        }

                                        synchronized (TERM) {
                                            MASTER_INDEX = newMasterIndex;
                                            TERM.set(newTerm);
                                            INDEX_TERM_MAP.put(TERM.get(), MASTER_INDEX);
                                        }
                                        log.info("upgradeMasterInfo success, newMasterIndex:{}, newTerm:{}", newMasterIndex, newTerm);
                                        siteRoleStateChange();
                                    }
                                });
                    }
                })
                .onErrorResume(e -> {
                    log.error("upgrade master fail. {}", redisHasChanged.get(), e);
                    if (redisHasChanged.get()) {
                        RedisReactiveCommands<String, String> reactive0 = Arbitrator.getInstance().connectDb0.reactive();
                        return reactive0.watch(DA_TERM, MASTER_CLUSTER_INDEX)
                                .publishOn(SCAN_SCHEDULER)
                                .flatMap(s -> reactive0.multi())
                                .doOnSuccess(s -> {
                                    // 必须同时更新，防止其他站点拿到的TERM是新的但DA_VERSION_PREFIX还没更新，从而导致拼成的DASyncStamp很大
                                    reactive0.set(DA_TERM, String.valueOf(formerTerm[0])).subscribe();
                                    reactive0.set(MASTER_CLUSTER_INDEX, String.valueOf(formerMasterIndex[0])).subscribe();
                                })
                                .flatMap(s -> reactive0.exec())
                                .map(transactionResult -> !transactionResult.wasDiscarded())
                                .map(b -> false);
                    }
                    return Mono.just(false);
                });
    }

    public static void upgradeMasterRecursion(int newMasterIndex, long newTerm, MonoProcessor<Boolean> res) {
        ArbitratorUtils.upgradeMaster(newMasterIndex, newTerm).subscribe(b -> {
            if (b) {
                res.onNext(true);
            } else {
                log.info("retry upgradeMasterRecursion. ");
                Mono.delay(Duration.ofSeconds(1)).publishOn(SCAN_SCHEDULER).subscribe(s -> upgradeMasterRecursion(newMasterIndex, newTerm, res));
            }
        });
    }

    /**
     * 更改redis后通知所有节点刷新一次内存里的主站点信息
     */
    public static Mono<Boolean> informNode(String code) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        List<String> ipList = NodeCache.getCache().keySet().stream().sorted().map(NodeCache::getIP).collect(Collectors.toList());
        List<Tuple3<String, String, String>> nodeList = new ArrayList<>(ipList.size());
        for (String ip : ipList) {
            Tuple3<String, String, String> tuple3 = new Tuple3<>(ip, "", "");
            nodeList.add(tuple3);
        }
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(t -> new SocketReqMsg("", 0)
                                .put("code", code)
//                                    .put(DA_TERM_HEADER, newTerm)
                )
                .collect(Collectors.toList());
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, FRESH_DA_TERM, String.class, nodeList);
        Set<String> successIpSet = new HashSet<>();

        responseInfo.responses
                .timeout(Duration.ofSeconds(30))
                .subscribe(s -> {
                    if (s.var2 == ErasureServer.PayloadMetaType.SUCCESS) {
                        successIpSet.add(nodeList.get(s.var1).var1);
                    }
                }, e -> log.error("", e), () -> {
                    if (responseInfo.successNum >= nodeList.size() / 2 + 1) {
                        log.info("freshDAMasterInfo server success, {}， {}", code, responseInfo.res);
                        res.onNext(true);
                    } else {
                        log.info("freshDAMasterInfo server fail, {}， {}", code, responseInfo.res);
                        res.onNext(false);
                    }
                });
        return res;
    }


    /**
     * 站点链路恢复，或者重启时，若已经有了新的主站点，需要更新主站点信息。(三站点只有原主连不上其他两个站点后恢复时才需要降级)
     * 如果集群中至少有一个站点在选举状态，则需要重试此方法。
     * 在本方法结束前不允许数据流和管理流的操作。
     * 最终认为所有响应的站点中，任期最大的MASTER_INDEX记录为主站点索引。一直超时就按本地记录确认主站点。
     * <p>
     * 另：一个非主站点先启动拿不到主站点信息，启动后会给自己投票切主，此时可能主站点若还未上线而复制站点启动，则会切主。
     *
     * @return 主站点信息的更新结果是否可靠。统计了大部分站点(包括自身)则认为可靠。
     */
    public static Mono<Boolean> evaluateMasterIndex(AtomicBoolean limited) {
        log.info("start evaluate Master Index info. ");
        // 如果本地记录自己是主站点，则必须要别的站点返回了至少一个响应才能够结束，避免双主出现。

        MonoProcessor<Boolean> res = MonoProcessor.create();
        //去所有站点拿MasterIndex和term，term最大的为主站点
        UnicastProcessor<Integer> replyProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        Map<Long, Integer> map = new ConcurrentHashMap<>();
        AtomicInteger succCount = new AtomicInteger(1);
        AtomicInteger replyCount = new AtomicInteger(1);
        AtomicBoolean isComplete = new AtomicBoolean();
        replyProcessor.publishOn(SCAN_SCHEDULER).subscribe(i -> {
            succCount.addAndGet(i);
            // 需要每个站点都有结果
            if (replyCount.incrementAndGet() >= ABT_CLUSTERS_AMOUNT) {
                replyProcessor.onComplete();
            }
        }, e -> log.error("", e), () -> {
            isComplete.set(true);
            long maxTerm = TERM.get();
            for (long term : map.keySet()) {
                if (term > maxTerm) {
                    maxTerm = term;
                }
            }
            if (maxTerm > TERM.get()) {
                synchronized (TERM) {
                    MASTER_INDEX = map.get(maxTerm);
                    TERM.set(maxTerm);
                    INDEX_TERM_MAP.put(TERM.get(), MASTER_INDEX);
                }
                log.info("update master info: masterIndex:{}, term:{}", MASTER_INDEX, TERM.get());
            }
            res.onNext(succCount.get() >= (ABT_CLUSTERS_AMOUNT - 1));

        });

        AtomicInteger respCount = new AtomicInteger(1);
        for (Map.Entry<Integer, String[]> entry : ABT_INDEX_IPS_ENTIRE_MAP.entrySet()) {
            Integer clusterIndex = entry.getKey();
            String[] ips = entry.getValue();
            if (clusterIndex.equals(LOCAL_CLUSTER_INDEX)) {
                continue;
            }
            UnicastProcessor<Integer> httpProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
            AtomicInteger retryTime = new AtomicInteger();
            httpProcessor.publishOn(SCAN_SCHEDULER).subscribe(ipIndex -> {
                String ip = ips[ipIndex];
                int port = ip.equals(ABT_IP) ? getAbtPort() : DA_PORT;
                int nextIpIndex = (ipIndex + 1) % ips.length;
                HttpClientRequest request = HeartBeatChecker.getClient().request(HttpMethod.GET, port, ip, "?evaluateMasterIndex");
                request.setTimeout(3000)
                        .setHost(ip + ":" + port)
                        .putHeader(SYNC_AUTH, PASSWORD)
                        .exceptionHandler(e -> {
                            log.error("check masterIndex request to cluster {} ip {} error, {}", clusterIndex, ip, e.getMessage());
                            request.reset();
                            // evaluateMasterIndex时扫描节点由于网口问题可能会变化，本站点如果连不上其他站点可能会一直卡在这里。
                            if (retryTime.incrementAndGet() >= ips.length && (limited.get() || !MainNodeSelector.checkIfSyncNode())) {
                                log.error("evaluate: all ip of cluster {} is down. ", clusterIndex, e);
                                replyProcessor.onNext(0);
                                httpProcessor.onComplete();
                                return;
                            }
                            Mono.delay(Duration.ofSeconds(1)).publishOn(SCAN_SCHEDULER).subscribe(s -> httpProcessor.onNext(nextIpIndex));
                        })
                        .handler(resp -> {
                            if (isComplete.get()) {
                                httpProcessor.onComplete();
                                return;
                            }
                            // 请求是发给在投票期间的站点，重试
                            if (resp.statusCode() != SUCCESS) {
                                log.info("retry evaluate, cluster is under election:{}", clusterIndex);
                                Mono.delay(Duration.ofSeconds(1)).publishOn(SCAN_SCHEDULER).subscribe(s -> httpProcessor.onNext(ipIndex));
                                return;
                            }

                            long newTerm = Long.parseLong(resp.getHeader(DA_TERM_HEADER));
                            int newMasterIndex = Integer.parseInt(resp.getHeader(MASTER_CLUSTER_INDEX_HEADER));
                            log.info("evaluateMasterIndex: get master info from cluster {}, newTerm {}, new MasterIndex {}", clusterIndex, newTerm, newMasterIndex);

                            //todo f 多个站点发起选举时有可能造成相同的term下有不同的masterIndex
                            map.put(newTerm, newMasterIndex);
                            replyProcessor.onNext(1);
                            // 有了超半数响应，其他站点可以不响应。
                            if (respCount.incrementAndGet() >= (ABT_CLUSTERS_AMOUNT / 2 + 1)) {
                                limited.compareAndSet(false, true);
                            }
                            httpProcessor.onComplete();
                        })
                        .end();
            }, e -> log.error("", e));
            httpProcessor.onNext(ThreadLocalRandom.current().nextInt(ips.length));
        }
        return res;
    }

    public static AtomicBoolean isChecking = new AtomicBoolean();

    /**
     * 链路恢复正常时，检查主站点的情况，更新到本地。扫描节点才能使用。
     * 心跳恢复正常的时候调用。因为更新心跳状态为正常的节点必定能与其他站点连通，而更新心跳状态异常的站点在站点心跳恢复后也不一定连通，所以要在心跳的sucessHandler调用。
     * 如果此方法没有正常结束，本站点将一直无法进行业务。
     */
    public static void checkMasterInfo(long originTerm, Integer originMasterIndex, Function<Void, Boolean> checkFunction) {
        if (!DAVersionUtils.isStrictConsis()) {
            return;
        }
        if (!isChecking.compareAndSet(false, true)) {
            log.info("another one is checking master info. return.");
            return;
        }
        updateEvaluatingMasterStatus(1);
        MonoProcessor<Boolean> res = MonoProcessor.create();
        res.map(b -> {
            if (b) {
                return ArbitratorUtils.informNode("0");
            }
            return b;
        }).subscribe(b -> {
            updateEvaluatingMasterStatus(0);
            log.info("checkMaster info succeeded");
        });

        AtomicBoolean limited = new AtomicBoolean(true);
        if (MASTER_INDEX.equals(LOCAL_CLUSTER_INDEX)) {
            limited.set(false);
        }

        UnicastProcessor<Integer> processor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        processor.publishOn(SCAN_SCHEDULER)
                .doFinally(s -> {
                    isChecking.compareAndSet(true, false);
                })
                .delayElements(Duration.ofSeconds(5))
                // 期间切换了扫描节点，将由新的扫描节点重新确认主站点状态
                .filter(b -> {
                    if (checkFunction.apply(null)) {
                        return true;
                    } else {
                        // 不重置evaluating状态，将由新的扫描节点去判断 // updateEvaluatingMasterStatus(0);
                        try {
                            log.info("Main node changed. {}", checkFunction.toString());
                            isChecking.set(false);
                            res.dispose();
                            limited.compareAndSet(false, true);
                            processor.dispose();
                        } catch (Exception e) {
                        }
                        return false;
                    }
                })
                .map(integer -> {
                    // 无法与其他站点通信，开始循环
                    if (HeartBeatChecker.isIsolated()) {
                        processor.onNext(1);
                        return false;
                    } else {
                        return true;
                    }
                })
                .filter(b -> b)
                .flatMap(b -> ArbitratorUtils.evaluateMasterIndex(limited))
                .subscribe(b -> {
                    if (b) {
                        processor.onComplete();
                    } else {
                        // 结果不可靠，回退并重新检查
                        log.info("checkMaster info failed");
                        synchronized (TERM) {
                            MASTER_INDEX = originMasterIndex;
                            TERM.set(originTerm);
                            INDEX_TERM_MAP.put(TERM.get(), MASTER_INDEX);
                        }
                        processor.onNext(1);
                    }
                }, e -> log.error("checkMasterInfo error, ", e), () -> {
                    // 防止切主时本地daVersionPrefix更大
                    if (!LOCAL_CLUSTER_INDEX.equals(MASTER_INDEX)) {
                        char[] temp = new char[19];
                        Arrays.fill(temp, '0');
                        daVersionPrefix = new String(temp);
                    }
                    ArbitratorUtils.upgradeMasterRecursion(MASTER_INDEX, TERM.get(), res);
                });

        processor.onNext(1);
    }

    public static void updateEvaluatingMasterStatus(int status) {
        updateEvaluatingMasterStatus(status, false);
    }

    public static void updateEvaluatingMasterStatus(int status, boolean force) {
        if (!DAVersionUtils.isStrictConsis()) {
            return;
        }
        synchronized (isEvaluatingMaster) {
            boolean b = status == 1;
            isEvaluatingMaster.compareAndSet(!b, b);
            if (force) {
                log.info("updateEvaluatingMasterStatus to {}", status);
                RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, IS_EVALUATING_MASTER, String.valueOf(status));
            } else {
                pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, IS_EVALUATING_MASTER)
                        .publishOn(SCAN_SCHEDULER)
                        .defaultIfEmpty("-1")
                        .doOnNext(isEval -> {
                            if (Integer.parseInt(isEval) != status) {
                                RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, IS_EVALUATING_MASTER, String.valueOf(status));
                                log.info("updateEvaluatingMasterStatus to {}", status);
                            }
                        })
                        .doOnError(e -> log.error("updateEvaluatingMasterStatus error, ", e))
                        .subscribe();
            }

        }
    }


    public static void startEveryArbitrator() {
        Disposable[] disposables = new Disposable[2];
        UnicastProcessor<Integer> retryProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        Set<String> skipIp = new ConcurrentHashSet<>();
        retryProcessor.publishOn(SCAN_SCHEDULER).subscribe(i -> {
            disposables[0] = ScanStream.scan(RedisConnPool.getInstance().getReactive(REDIS_NODEINFO_INDEX))
                    .flatMap(node -> RedisConnPool.getInstance().getReactive(REDIS_NODEINFO_INDEX).hget(node, SysConstants.HEART_IP))
                    .collectList()
                    .map(list -> {
                        List<Tuple3<String, String, String>> nodeList = new ArrayList<>(list.size());
                        for (String ip : list) {
                            Tuple3<String, String, String> tuple3 = new Tuple3<>(ip, "", "");
                            nodeList.add(tuple3);
                        }
                        return nodeList;
                    })
                    .subscribe(nodeList -> {
                        List<SocketReqMsg> msgs = nodeList.stream()
                                .map(t -> {
                                    SocketReqMsg msg = new SocketReqMsg("", 0);
                                    if (skipIp.contains(t.var1)) {
                                        msg.put("init", "0");
                                    } else {
                                        msg.put("init", "1");
                                    }
                                    return msg;
                                })
                                .collect(Collectors.toList());
                        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, START_ARBITRATOR, String.class, nodeList);
                        disposables[1] = responseInfo.responses.subscribe(s -> {
                            if (s.var2 == ErasureServer.PayloadMetaType.SUCCESS) {
                                skipIp.add(nodeList.get(s.var1).var1);
                            }
                        }, e -> log.error("", e), () -> {
                            if (responseInfo.successNum == nodeList.size()) {
                                log.info("startEveryArbitrator complete.");
                                retryProcessor.onComplete();
                            } else {
                                log.info("startEveryArbitrator fail. successIp:{}", skipIp);
                                Mono.delay(Duration.ofSeconds(5)).subscribe(s -> retryProcessor.onNext(1));
                                DoubleActiveUtil.streamDispose(disposables);
                            }
                        });
                    });

        });
        retryProcessor.onNext(1);
    }
}
