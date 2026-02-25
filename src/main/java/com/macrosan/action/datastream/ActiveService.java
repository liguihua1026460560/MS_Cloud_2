package com.macrosan.action.datastream;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.DataSynChecker;
import com.macrosan.doubleActive.HeartBeatChecker;
import com.macrosan.doubleActive.arbitration.Arbitrator;
import com.macrosan.doubleActive.arbitration.ArbitratorUtils;
import com.macrosan.doubleActive.arbitration.DAVersionUtils;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.SshClientUtils;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.http.HttpClientRequest;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.*;
import static com.macrosan.doubleActive.DataSyncSignHandler.BUCKET_SIGN_TYPE;
import static com.macrosan.doubleActive.DoubleActiveUtil.checkConfigNodeSet;
import static com.macrosan.doubleActive.HeartBeatChecker.*;
import static com.macrosan.doubleActive.arbitration.Arbitrator.*;
import static com.macrosan.doubleActive.arbitration.ArbitratorUtils.*;
import static com.macrosan.doubleActive.deployment.AddClusterHandler.index_his_sync;
import static com.macrosan.httpserver.MossHttpClient.*;
import static com.macrosan.httpserver.ResponseUtils.addAllowHeader;
import static com.macrosan.httpserver.ResponseUtils.addPublicHeaders;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;

/**
 * @auther wuhaizhong
 * 双活相关接口
 * @date 2021/4/26
 */
@Log4j2
public class ActiveService extends BaseService {

    private static final Logger logger = LogManager.getLogger(ActiveService.class.getName());

    private static ActiveService instance = null;

    protected static RedisConnPool pool = RedisConnPool.getInstance();

    private ActiveService() {
        super();
    }

    public static ActiveService getInstance() {
        if (instance == null) {
            instance = new ActiveService();
        }
        return instance;
    }

    public static final String SYNC_AUTH = "Sync-Auth";

    public static final String SYNC_AK = "MAKSYNC0000000000000";
    public static final String SYNC_SK = "MSKSYNC000000000000000000000000000000000";
    public static final String SYNC_ID = "MAKIMTH2V7QUH5CMKE8Jfdfgdg";
    public static final String SYNC_USER = "MAKIMTH2V7QUH5CMKE8Jgdgdgd";
    public static final String PASSWORD = "9b61fd9907c674e153934299d630ec9c";

    /**
     * 接收其他节点发来的record。
     * 如果是本站点发来的，record.syncFlag为true，表示要进行异步复制分布式处理；
     * 如果是对面站点发来的，record.syncFlag为false，则要落盘，等待本站点异步复制处理。
     *
     * @param request
     * @return
     */
    public int dealSyncRecord(MsHttpRequest request) {
        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "no auth to sync."));
            return UNKNOWN_ERROR;
        }

        StringBuilder stringBuilder = new StringBuilder();
        request.handler(buffer -> stringBuilder.append(buffer.toString()))
                .endHandler(v -> {
                    try {
                        ArrayList<SyncRquest> recordList = Json.decodeValue(stringBuilder.toString(), new TypeReference<ArrayList<SyncRquest>>() {
                        });
                        HashSet<Integer> unDoneSet = new HashSet<>();
                        AtomicBoolean hasSuccess = new AtomicBoolean();
                        AtomicInteger dealCount = new AtomicInteger();

                        for (int i = 0; i < recordList.size(); i++) {
                            int finalI = i;
                            UnSynchronizedRecord record = recordList.get(i).record;
                            String bucketSwitch = recordList.get(i).bucketSwitch;
                            String deleteSource = recordList.get(i).deleteSource;
                            record.headers.put("Sync-Distributed", "1");
                            if (record.syncFlag) {
                                SyncRquest syncRquest = new SyncRquest();
                                syncRquest.record = record;
                                syncRquest.bucketSwitch = bucketSwitch;
                                syncRquest.deleteSource = deleteSource;
                                syncRquest.res = MonoProcessor.create();
                                Disposable subscribe = syncRquest.res
                                        .publishOn(SCAN_SCHEDULER)
                                        .subscribe(b -> {
                                            if (b) {
                                                hasSuccess.compareAndSet(false, true);
                                            } else {
                                                unDoneSet.add(finalI);
                                            }

                                            if (dealCount.incrementAndGet() == recordList.size()) {
                                                int resCode = hasSuccess.get() ? SUCCESS : INTERNAL_SERVER_ERROR;
                                                try {
                                                    byte[] body = Json.encode(unDoneSet).getBytes();
                                                    addPublicHeaders(request, getRequestId())
                                                            .putHeader(CONTENT_LENGTH, String.valueOf(body.length))
                                                            .setStatusCode(resCode)
                                                            .write(Buffer.buffer(body));
                                                    addAllowHeader(request.response()).end();
                                                } catch (Exception e) {
                                                    //有可能主站点因为超时或负荷高会主动关连接。
                                                }
                                            }

                                        }, e -> {
                                            logger.error("sync record error!", e);
                                            MsException.dealException(request, e);
                                        });
                                request.addResponseCloseHandler(s -> subscribe.dispose());
                                int processorIndex = ThreadLocalRandom.current().nextInt(PROCESSOR_NUM);
                                recordProcessors[processorIndex].onNext(syncRquest);

                            } else {
                                StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(record.bucket);
                                String bucketVnode = storagePool.getBucketVnodeId(record.bucket);
                                // 如果bucket_sync_set没有record相关的桶则需要往里添加
                                DataSynChecker.getInstance().addSyncBucket(record.index, record.bucket);
                                storagePool.mapToNodeInfo(bucketVnode)
                                        .flatMap(nodeList -> {
                                            if (UnSynchronizedRecord.isOldPath(record.rocksKey())) {
                                                record.setRecordKey(null);
                                                record.rocksKey();
                                            }
                                            record.headers.put("Sync-Notify", "1");
                                            return ECUtils.putSynchronizedRecord(storagePool, record.rocksKey(), Json.encode(record), nodeList, false);
                                        })
                                        .subscribe(res -> {
                                            if (res) {
                                                addPublicHeaders(request, getRequestId())
                                                        .setStatusCode(200);
                                                addAllowHeader(request.response()).end();
                                            } else {
                                                MsException.dealException(request, new MsException(ErrorNo.UNKNOWN_ERROR, ""));
                                            }
                                        }, e -> {
                                            logger.error("put sync recodr error", e);
                                            MsException.dealException(request, new MsException(ErrorNo.UNKNOWN_ERROR, ""));
                                        });
                            }
                        }
                    } catch (Exception e) {
                        logger.error(e);
                        MsException.dealException(request, e);
                    }
                })
                .exceptionHandler(e -> {
                    logger.error(e);
                    MsException.dealException(request, e);
                })
                .resume();

        if (request.headers().contains("Expect")) {
            request.response().writeContinue();
        }

        return ErrorNo.SUCCESS_STATUS;
    }

    public int getClusterStatus(MsHttpRequest request) {
//        if (request.getHeader("arbitrator") == null) {
//            return ErrorNo.SUCCESS_STATUS;
//        }

        ScanArgs scanAllArg = new ScanArgs().match("Pool_*");
        JSONObject jsonObject = new JSONObject();
        StringBuilder stringBuilder = new StringBuilder();
        String requestCluster = request.getHeader("request_index");
        String isArrArb = request.getHeader("no_arb");
        Disposable[] disposables = new Disposable[2];
        disposables[0] = ScanStream.scan(pool.getReactive(REDIS_POOL_INDEX), scanAllArg)
                .flatMap(key -> pool.getReactive(REDIS_POOL_INDEX).hget(key, "role").zipWith(Mono.just(key)))
                .filter(tuple2 -> !"es".equals(tuple2.getT1()))
                .flatMap(tuple2 -> Mono.just(tuple2.getT1()).zipWith(pool.getReactive(REDIS_POOL_INDEX).hget(tuple2.getT2(), "state")))
                .collectList()
                .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTERS_STATUS).defaultIfEmpty("")
                        .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hgetall("index_his_sync").defaultIfEmpty(new HashMap<>())
                                .flatMap(map -> {
                                    // 历史数据没有同步完成
                                    if (StringUtils.isEmpty(requestCluster) || !"1".equals(map.getOrDefault(requestCluster, "0"))) {
                                        return Mono.just("");
                                    }
                                    return pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTERS_SYNC_STATE).defaultIfEmpty("");
                                }))
                )
                .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hgetall("active_live_change_ips").defaultIfEmpty(new HashMap<>())
                )
                .doOnNext(tuple2 -> {
                    disposables[1] = ScanStream.scan(pool.getReactive(REDIS_NODEINFO_INDEX), new ScanArgs().match("*"))
                            .flatMap(uuid -> {
                                if (!request.headers().contains("arbitrator-req")) {
                                    return Mono.just(uuid);
                                }
                                if (!request.headers().contains("test") || !uuid.equals(ServerConfig.getInstance().getHostUuid())) {
                                    return Mono.just(uuid);
                                }
                                return pool.getReactive(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SYNC_ETH)
                                        .defaultIfEmpty("0")//防止升级上来的环境缺少该字段
                                        .flatMap(syncEthType -> {
                                            if ("eth2".equals(syncEthType) || "eth3".equals(syncEthType) || "eth18".equals(syncEthType)) {
                                                return pool.getReactive(REDIS_SYSINFO_INDEX).hget(syncEthType + "_status", uuid)
                                                        .doOnNext(status -> {
                                                            if (!status.startsWith("1")) {
                                                                throw new MsException(UNKNOWN_ERROR, "local syncIpState is down");
                                                            }
                                                        })
                                                        .map(s -> uuid);
                                            }
                                            return pool.getReactive(REDIS_NODEINFO_INDEX).hgetall(uuid)
                                                    .doOnNext(uuidInfo -> {
                                                        int syncIpState;
                                                        if ("eth12".equals(syncEthType)) {
                                                            syncIpState = "up".equals(uuidInfo.get(ACTIVE_SYNC_IP_STATE)) ? 1 : 0;
                                                        } else if ("0".equals(syncEthType)) {
                                                            if (StringUtils.isNotBlank(uuidInfo.get(ACTIVE_SYNC_IP_STATE))) {
                                                                syncIpState = "up".equals(uuidInfo.get(ACTIVE_SYNC_IP_STATE)) ? 1 : 0;
                                                            } else {
                                                                syncIpState = "1".equals(uuidInfo.get(NODE_SERVER_STATE)) ? 1 : 0;
                                                            }
                                                        } else {
                                                            syncIpState = "1".equals(uuidInfo.get(NODE_SERVER_STATE)) ? 1 : 0;
                                                        }
                                                        if (syncIpState != 1) {
                                                            throw new MsException(UNKNOWN_ERROR, "local syncIpState is down");
                                                        }
                                                    })
                                                    .map(s -> uuid);
                                        });
                            })
                            .flatMap(uuid -> pool.getReactive(REDIS_NODEINFO_INDEX).hgetall(uuid)
                                    .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hget(AVAILABLE_NODE_LIST, uuid).defaultIfEmpty("0"))
                                    .doOnNext(tuple21 -> {
                                        Map<String, String> uuidInfo = tuple21.getT1();
                                        if (StringUtils.isNotBlank(uuidInfo.get(HEART_ETH1))) {
                                            String syncIp = ETH4_SYNC_IP_MAP.get(uuidInfo.get(HEART_ETH1));
                                            if (tuple21.getT2().equals("0")) {
                                                jsonObject.put(syncIp, "0");
                                            } else {
                                                jsonObject.put(syncIp, uuidInfo.get(NODE_SERVER_STATE));
                                            }
                                        }
                                    }))
                            .flatMap(s -> pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(ETH2_STATUS)
                                    .doOnNext(map -> {
                                        if (!map.isEmpty()) {
                                            request.response()
                                                    .putHeader(ETH2_STATUS, Json.encode(map));
                                        }
                                    }))
                            .flatMap(s -> pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(ETH3_STATUS)
                                    .doOnNext(map -> {
                                        if (!map.isEmpty()) {
                                            request.response()
                                                    .putHeader(ETH3_STATUS, Json.encode(map));
                                        }
                                    }))
                            .flatMap(s -> pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(AVAILABLE_NODE_LIST)
                                    .doOnNext(map -> {
                                        if (!map.isEmpty()) {
                                            request.response().putHeader(AVAILABLE_NODE_LIST, Json.encode(map));
                                        }
                                    }))
                            //例：0-0-{"172.17.0.10":"1","172.17.0.12":"1","172.17.0.11":"1"}-{"0":1,"1":1,"2":1}-{\"0\":\"SYNCED\",\"1\":\"SYNCING\",\"2\":\"SYNCING\"}
                            // 站点的Pool_index池状态，Pool_data池状态，各节点间状态（[syncIp, server_state]），检测到的各站点心跳状态，差异记录同步状态
                            //多存储池环境可能有多个数据池索引池和缓存池
                            //存储池：0正常，1降级，2降级临界状态，3故障 4永久故障  | 节点间状态： 0断开，1正常
                            .doOnComplete(() -> {
                                boolean indexPoolBroken = false;
                                boolean dataPoolBroken = false;
                                for (Tuple2 tuple : tuple2.getT1().getT1()) {
                                    if ("3".equals(tuple.getT2()) || "4".equals(tuple.getT2())) {
                                        if ("meta".equals(tuple.getT1())) {
                                            indexPoolBroken = true;
                                        } else {
                                            dataPoolBroken = true;
                                        }
                                    }
                                }

                                stringBuilder.append(indexPoolBroken ? "3" : "0").append("-");
                                stringBuilder.append(dataPoolBroken ? "3" : "0").append("-");
                                stringBuilder.append(JSONObject.toJSONString(jsonObject));
                                if (StringUtils.isNotEmpty(tuple2.getT1().getT2().getT1())) {
                                    stringBuilder.append("-").append(tuple2.getT1().getT2().getT1());
                                }
                                if (StringUtils.isNotEmpty(tuple2.getT1().getT2().getT2())) {
                                    stringBuilder.append("-").append(tuple2.getT1().getT2().getT2());
                                }
                                log.debug("heartbeat request: {}", stringBuilder.toString());
                                // 将MS_Cloud正常的节点的syncIp放入availSet
                                Set<String> availSyncIpSet = new HashSet<>();
                                AVAIL_BACKEND_IP_ENTIRE_SET.forEach(eth4Ip ->
                                        availSyncIpSet.add(ETH4_SYNC_IP_MAP.get(eth4Ip))
                                );
                                if (StringUtils.isNotBlank(requestCluster) && ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(Integer.parseInt(requestCluster)) && !IS_ASYNC_CLUSTER) {
                                    request.response()
                                            .putHeader(DA_TERM_HEADER, String.valueOf(TERM.get()))
                                            .putHeader(MASTER_CLUSTER_INDEX_HEADER, String.valueOf(MASTER_INDEX))
                                            .putHeader("isEvaluatingMaster", String.valueOf(isEvaluatingMaster.get()));
                                }
                                if (StringUtils.isNotBlank(requestCluster) && StringUtils.isNotBlank(isArrArb) && !ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(Integer.parseInt(requestCluster))) {
                                    request.response()
                                            .putHeader(DA_TERM_HEADER, String.valueOf(TERM.get()))
                                            .putHeader(IP_FLOAT_COUNT_HEADER, String.valueOf(FLOAT_COUNT.get()))
                                            .putHeader(MASTER_CLUSTER_INDEX_HEADER, String.valueOf(MASTER_INDEX))
                                            .putHeader(ADD_MIP_MAP_HEADER, Json.encode(tuple2.getT2()));
                                }
                                request.response()
                                        .setStatusCode(SUCCESS)
                                        .putHeader(CLUSTER_VALUE, stringBuilder.toString())
                                        .putHeader("service_avail_ip", Json.encode(availSyncIpSet))
                                        .putHeader("CONFIG_NODE_SYNCIP_SET", Json.encode(CONFIG_NODE_SYNCIP_SET))
                                        .putHeader("init_finished", String.valueOf(init_finished.get()))
                                        .end();
                            })
                            .doOnError(e -> MsException.dealException(request, e))
                            .subscribe();
                    request.addResponseCloseHandler(v -> {
                        for (Disposable d : disposables) {
                            if (null != d) {
                                d.dispose();
                            }
                        }
                    });
                })
                .doOnError(e -> MsException.dealException(request, e))
                .subscribe();

        return ErrorNo.SUCCESS_STATUS;
    }

    public static final String MOSS_STATUS = "moss-status";

    // 对外接口，支持单站点
    public int getStatus(MsHttpRequest request) {
        if (isMultiAliveStarted) {
            if (!init_finished.get() || Arbitrator.isEvaluatingMaster.get()
                    || (DAVersionUtils.isStrictConsis() && !DAVersionUtils.canGet.get())) {
                addPublicHeaders(request, getRequestId())
                        .setStatusCode(SUCCESS)
                        .putHeader(MOSS_STATUS, "0")
                        .end();
                return ErrorNo.SUCCESS_STATUS;
            }

            pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTERS_STATUS).defaultIfEmpty("")
                    .subscribe(s -> {
                        try {
                            Map<Integer, Integer> map = Json.decodeValue(s, new TypeReference<Map<Integer, Integer>>() {
                            });
                            Integer status = map.get(LOCAL_CLUSTER_INDEX);
                            addPublicHeaders(request, getRequestId())
                                    .setStatusCode(SUCCESS)
                                    .putHeader(MOSS_STATUS, status == 1 ? "1" : "0")
                                    .end();
                        } catch (Exception e) {
                            log.error("getStatus err1, ", e);
                            addPublicHeaders(request, getRequestId())
                                    .setStatusCode(SUCCESS)
                                    .putHeader(MOSS_STATUS, "0")
                                    .end();
                        }
                    }, e -> {
                        log.error("getStatus err2, ", e);
                        addPublicHeaders(request, getRequestId())
                                .setStatusCode(SUCCESS)
                                .putHeader(MOSS_STATUS, "0")
                                .end();
                    });
            return ErrorNo.SUCCESS_STATUS;
        }

        ScanArgs scanAllArg = new ScanArgs().match("Pool_*");
        Disposable[] disposables = new Disposable[2];
        ScanStream.scan(pool.getReactive(REDIS_POOL_INDEX), scanAllArg)
                .flatMap(key -> pool.getReactive(REDIS_POOL_INDEX).hget(key, "role").zipWith(Mono.just(key)))
                .filter(tuple2 -> !"es".equals(tuple2.getT1()))
                .flatMap(tuple2 -> Mono.just(tuple2.getT1()).zipWith(pool.getReactive(REDIS_POOL_INDEX).hget(tuple2.getT2(), "state")))
                .collectList()
                .flatMap(tuple2s -> {
                    boolean indexPoolBroken = false;
                    boolean dataPoolBroken = false;
                    for (Tuple2 tuple : tuple2s) {
                        if ("3".equals(tuple.getT2()) || "4".equals(tuple.getT2())) {
                            if ("meta".equals(tuple.getT1())) {
                                indexPoolBroken = true;
                            } else {
                                dataPoolBroken = true;
                            }
                        }
                    }
                    if (indexPoolBroken || dataPoolBroken) {
                        return Mono.just(false);
                    }
                    // 主备备有一个正常才能说明redis的状态记录准确
                    Set<String> configNodeHeartIpSet = new HashSet<>();
                    for (String uuid : CONFIG_NODE_SET) {
                        configNodeHeartIpSet.add(UUID_ETH4IP_MAP.get(uuid));
                    }
                    if (!checkConfigNodeSet(configNodeHeartIpSet, AVAIL_BACKEND_IP_ENTIRE_SET)) {
                        return Mono.just(false);
                    }

                    return ScanStream.scan(pool.getReactive(REDIS_NODEINFO_INDEX), new ScanArgs().match("*"))
                            .flatMap(uuid ->
                                    pool.getReactive(REDIS_NODEINFO_INDEX).hgetall(uuid)
                                            .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hget(AVAILABLE_NODE_LIST, uuid).defaultIfEmpty("0"))
                            )
                            .collectList()
                            .flatMap(tuple2List -> {
                                Map<String, String> serverStateMap = new HashMap<>();
                                int nodeAmount = 0;
                                for (Tuple2<Map<String, String>, String> tuple2 : tuple2List) {
                                    // 保存heartIp和serverState状态
                                    Map<String, String> uuidInfo = tuple2.getT1();
                                    String heartIp = uuidInfo.get(HEART_ETH1);
                                    if (StringUtils.isNotBlank(heartIp)) {
                                        nodeAmount++;
                                        if (tuple2.getT2().equals("0")) {
                                            serverStateMap.put(heartIp, "0");
                                        } else {
                                            serverStateMap.put(heartIp, uuidInfo.get(NODE_SERVER_STATE));
                                        }
                                    }
                                }
                                int clusterRes = 0;
                                for (Map.Entry<String, String> entry : serverStateMap.entrySet()) {
                                    String eth4Ip = entry.getKey();
                                    int serverStatus = Integer.parseInt(entry.getValue());
                                    if (serverStatus == 1) {
                                        if (AVAIL_BACKEND_IP_ENTIRE_SET.contains(eth4Ip)) {
                                            clusterRes++;
                                        }
                                    }
                                }
                                return Mono.just(clusterRes >= nodeAmount / 2 + 1);
                            })
                            .onErrorResume(e -> {
                                log.error("server_state check err, ", e);
                                return Mono.just(false);
                            });
                })
                .subscribe(b -> {
                    addPublicHeaders(request, getRequestId())
                            .setStatusCode(SUCCESS)
                            .putHeader(MOSS_STATUS, b ? "1" : "0")
                            .end();
                });


        return ErrorNo.SUCCESS_STATUS;
    }

    public int getHisSyncStatus(MsHttpRequest request) {
        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "no auth to sync."));
            return UNKNOWN_ERROR;
        }
        String clusterIndex = request.getHeader(index_his_sync);
        pool.getReactive(REDIS_SYSINFO_INDEX)
                .hget(index_his_sync, clusterIndex)
                .defaultIfEmpty("-1")
                .doOnNext(status ->
                        request.response()
                                .setStatusCode(SUCCESS)
                                .putHeader(index_his_sync, status).end()
                )
                .doOnError(e -> MsException.dealException(request, e))
                .subscribe();
        return ErrorNo.SUCCESS_STATUS;
    }

    public int getMasterSyncStamp(MsHttpRequest request) {
        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "no auth to sync."));
            return UNKNOWN_ERROR;
        }

        // 因为非主站点先更改MASTER_INDEX，此时主站点选举流程可能还未结束，不返回DAVersion
        String version = DAVersionUtils.syncStamp2DaPrefix(VersionUtil.getVersionNumTrue());
        pool.getReactive(REDIS_ROCK_INDEX).get(ArbitratorUtils.ELECTION_STATUS)
                .defaultIfEmpty("0")
                .map(s -> s.equals("0") ? SUCCESS : BAD_REQUEST_REQUEST)
                .subscribe(statusCode -> {
                    request.response()
                            .setStatusCode(statusCode)
                            .putHeader(DA_VERSION_PREFIX, version)
                            .putHeader(DA_TERM, String.valueOf(TERM.get()))
                            .end();
                });
        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 处理其他站点发来的投票请求。
     */
    public int voteMaster(MsHttpRequest request) {
        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "no auth to sync."));
            return UNKNOWN_ERROR;
        }
        String indexVotedStr = request.getHeader(ArbitratorUtils.MASTER_CLUSTER_INDEX_HEADER);
        String termVotedStr = request.getHeader(ArbitratorUtils.DA_TERM_HEADER);
        ArbitratorUtils.voteReplyMono(termVotedStr, indexVotedStr)
                .subscribe(s -> {
                    request.response()
                            .setStatusCode(SUCCESS)
                            .putHeader("vote_reply", s.name())
                            .end();
                }, e -> log.error("vote error", e));
        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 处理升主请求，升主的任期比本地的latestTerm大，才允许升主。
     */
    public int changeMasterCluster(MsHttpRequest request) {
        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "no auth to sync."));
            return UNKNOWN_ERROR;
        }
        String newMasterIndex = request.getHeader(ArbitratorUtils.MASTER_CLUSTER_INDEX_HEADER);
        String newTerm = request.getHeader(ArbitratorUtils.DA_TERM_HEADER);

        pool.getReactive(REDIS_SYSINFO_INDEX).get(LATEST_VOTED_TERM)
                .flatMap(latestTerm -> {
                    long newTermL = Long.parseLong(newTerm);
                    if (Long.parseLong(latestTerm) > newTermL) {
                        // 已经给更新的任期投过票，则拒绝本次切主请求。
                        log.error("expired term {}", newTerm);
                        return Mono.just(BAD_REQUEST_REQUEST);
                    } else {
                        return ArbitratorUtils.upgradeMaster(Integer.parseInt(newMasterIndex), newTermL)
                                .flatMap(b -> {
                                    if (b) {
                                        return ArbitratorUtils.informNode("0")
                                                .map(c -> c ? SUCCESS : INTERNAL_SERVER_ERROR);
                                    }
                                    return Mono.just(INTERNAL_SERVER_ERROR);
                                });
                    }
                })
                .subscribe(statusCode -> {
                    request.response()
                            .setStatusCode(statusCode)
                            .end();
                }, e -> {
                    log.error("", e);
                    MsException.dealException(request, e);
                });
        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 站点开机、链路恢复的时候会发送请求至其他站点的本接口，判断实际的主站点和任期。
     * 升主到一半可能造成LATEST_VOTED_TERM比最终的实际主站点任期大的情况。
     * 当本站点还在选举过程中时，该接口返回400
     */
    public int evaluateMasterIndex(MsHttpRequest request) {
        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "no auth to sync."));
            return UNKNOWN_ERROR;
        }

        pool.getReactive(REDIS_ROCK_INDEX).get(ArbitratorUtils.ELECTION_STATUS)
                .defaultIfEmpty("0")
                .map(s -> s.equals("0") ? SUCCESS : BAD_REQUEST_REQUEST)
                .doOnNext(statusCode -> request.response().setStatusCode(statusCode))
                // 实时获取最新的主站点信息，防止本站点选举成功并修改redis后，其他节点还没有被定时器同步到内存中
                .flatMap(s -> pool.getReactive(REDIS_ROCK_INDEX).get(ArbitratorUtils.MASTER_CLUSTER_INDEX))
                .doOnNext(masterIndex -> request.response().putHeader(ArbitratorUtils.MASTER_CLUSTER_INDEX_HEADER, masterIndex))
                .flatMap(s -> pool.getReactive(REDIS_ROCK_INDEX).get(ArbitratorUtils.DA_TERM))
                .doOnNext(term -> request.response().putHeader(ArbitratorUtils.DA_TERM_HEADER, term))
                .subscribe(statusCode -> request.response().end());

        return ErrorNo.SUCCESS_STATUS;
    }

    public int setArbitrator(MsHttpRequest request) {
        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "no auth to sync."));
            return UNKNOWN_ERROR;
        }

        if (!isMultiAliveStarted) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "this cluster is not multi-alived."));
            return UNKNOWN_ERROR;
        }

        String abtIp = request.getHeader(ARBITRATOR_IP_HEADER);
        String abtPort = request.getHeader(ARBITRATOR_PORT_HEADER);
        String abtHttpsPort = request.getHeader(ARBITRATOR_HTTPS_PORT_HEADER);
        String abtSpecIp = request.getHeader(ARBITRATOR_SPEC_IP);
        pool.getReactive(REDIS_SYSINFO_INDEX)
                .hgetall(MASTER_CLUSTER)
                .defaultIfEmpty(new HashMap<>())
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(map -> {
                    boolean changed = false;
                    String ip = map.get(ARBITRATOR_IP);
                    String port = map.get(ARBITRATOR_PORT);
                    String httpsPort = map.get(ARBITRATOR_HTTPS_PORT);
                    String specIp = map.get(ARBITRATOR_SPEC_IP);
                    if (!abtIp.equals(ip)) {
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, ARBITRATOR_IP, abtIp);
                        log.info("set arbitrator ip success. ip: {}", abtIp);
                        changed = true;
                    }
                    if (!abtPort.equals(port)) {
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, ARBITRATOR_PORT, abtPort);
                        log.info("set arbitrator port success. port: {}", abtPort);
                        changed = true;
                    }
                    if (!abtHttpsPort.equals(httpsPort)) {
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, ARBITRATOR_HTTPS_PORT, abtHttpsPort);
                        log.info("set arbitrator https port success. port: {}", abtHttpsPort);
                        changed = true;
                    }

                    if (StringUtils.isNotEmpty(abtSpecIp) && !abtSpecIp.equals(specIp) && StringUtils.isNotEmpty(abtSpecIp)) {
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, ARBITRATOR_SPEC_IP, abtSpecIp);
                        String specEth = getSpecEthFromMySql(abtSpecIp);
                        String ipStr = getIpBySpecEthFromMySql(specEth);
                        if (StringUtils.isBlank(ipStr)) {
                            throw new MsException(UNKNOWN_ERROR, "setArbitrator: no ip under this eth ");
                        }
                        String[] ips = ipStr.split("\n");
                        if (ips.length != INDEX_SNUM_MAP.get(LOCAL_CLUSTER_INDEX)) {
                            throw new MsException(UNKNOWN_ERROR, "setArbitrator: ip amount does not match");
                        }
                        log.info("eth {}, str {}", specEth, ips);
                        for (String specIP : ips) {
                            String node = getNodeBySpecEthFromMySql(specEth, specIP);
                            if (StringUtils.isBlank(node)) {
                                throw new MsException(UNKNOWN_ERROR, "setArbitrator: no node under this eth and ip ");
                            }
                            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(ARBITRATOR_SPEC_IPS, node, specIP);
                            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(ARBITRATOR_STATUS_EACH, node, "1");
                        }
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, ARBITRATOR_SPEC_ETH, specEth);
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, ARBITRATOR_STATUS, "1");
                        log.info("set arbitrator spec ip success. ips: {}", (Object) ips);
                        changed = true;
                    }
                    if (changed) {
                        startEveryArbitrator();
                        siteStateChange();
                    }
                })
                .subscribe(statusCode -> {
                    request.response()
                            .setStatusCode(SUCCESS)
                            .end();
                }, e -> {
                    log.error("setArbitrator error, ", e);
                    request.response()
                            .setStatusCode(500)
                            .end();
                });
        return ErrorNo.SUCCESS_STATUS;
    }

    public int getHeartBeat(MsHttpRequest request) {
        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "no auth to sync."));
            return UNKNOWN_ERROR;
        }

        String cluster_index = request.getHeader("cluster_index");
        if (cluster_index.equals("checkNode")) {
            // 同站点的节点检查其他节点是否正常，走eth4
            // 如果有仲裁，需要判断是否firstCheck已完成。
            request.response()
                    .putHeader("service_avail", String.valueOf(SERVICE_AVAIL.get()))
                    .setStatusCode(200)
                    .end();
            return ErrorNo.SUCCESS_STATUS;
        }

        if (!isMultiAliveStarted) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "this cluster is not multi-alived."));
            return UNKNOWN_ERROR;
        }

        if (!cluster_index.equals(String.valueOf(LOCAL_CLUSTER_INDEX))) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "getHeartBeat error cluster. " + cluster_index));
            return UNKNOWN_ERROR;
        }
        // 只有eth12异常导致心跳检测结果异常，但可与仲裁者连通且其余指标皆正常的主站点，仲裁者会认为该主站点正常，不会投给其他站点赞成票
        // 此时需要检测除eth12外有无其他故障
        request.response().putHeader("unitCountArb", String.valueOf(unitCountArb));
        request.params().add("arbitrator-req", "true");
        getClusterStatus(request);


        return ErrorNo.SUCCESS_STATUS;
    }

    public int getAbtIps(MsHttpRequest request) {
        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "no auth to sync."));
            return UNKNOWN_ERROR;
        }

        String specIp = request.getHeader("specIp");
        String localUuid = ServerConfig.getInstance().getHostUuid();

        AtomicInteger succCount = new AtomicInteger(0);
        UnicastProcessor<Integer> replyProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        Map<Integer, String> indexIpsMap = new HashMap<>();
        replyProcessor.publishOn(SCAN_SCHEDULER)
                .subscribe(i -> {
                    if (succCount.incrementAndGet() == NODE_AMOUNT) {
                        replyProcessor.onComplete();
                    }
                }, e -> {
                    log.error("getAbtIps error, ", e);
                    MsException.dealException(request, e);
                }, () -> {
                    request.response()
                            .putHeader("ips", Json.encode(indexIpsMap))
                            .setStatusCode(200)
                            .end();
                    log.info("getAbtIps complete. ips: {}", indexIpsMap);
                });

        Mono.just(1).publishOn(SCAN_SCHEDULER)
                .map(v -> getSpecEthFromMySql(specIp))
                .doOnNext(eth -> {
                    if (StringUtils.isBlank(eth)) {
                        log.error("getAbtIps: no such ip, {}", specIp);
                        throw new MsException(UNKNOWN_ERROR, "getAbtIps: no such ip, " + specIp);
                    }
                })
                .subscribe(eth -> {
                    for (Map.Entry<Integer, String[]> entry : DA_INDEX_IPS_MAP.entrySet()) {
                        int clusterIndex = entry.getKey();
                        String[] ips = entry.getValue();
                        UnicastProcessor<Integer> httpProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
                        httpProcessor.publishOn(SCAN_SCHEDULER).subscribe(ipIndex -> {
                            String ip = ips[ipIndex];
                            int nextIpIndex = (ipIndex + 1) % ips.length;
                            HttpClientRequest request2 = HeartBeatChecker.getClient().request(HttpMethod.GET, DA_PORT, ip, "?getOtherClusterIps");
                            request2.putHeader("specEth", eth)
                                    .setTimeout(10_000)
                                    .setHost(ip + ":" + DA_PORT)
                                    .putHeader(SYNC_AUTH, PASSWORD)
                                    .exceptionHandler(e -> {
                                        log.error("getIps failed, ip {}", request2.getHost(), e);
                                        request2.reset();
                                        Mono.delay(Duration.ofSeconds(1)).publishOn(SCAN_SCHEDULER).subscribe(c -> httpProcessor.onNext(nextIpIndex));
                                    })
                                    .handler(resp -> {
                                        if (resp.statusCode() != SUCCESS) {
                                            log.error("getOtherClusterIps error, {}", resp.statusMessage());
                                            replyProcessor.onError(new RuntimeException(resp.statusMessage()));
                                            return;
                                        }
                                        String ipsStr = resp.getHeader("ips");
                                        log.info("getAbtIps, clusterIndex: {}, eth: {}, ips: {}", clusterIndex, eth, ipsStr);
                                        indexIpsMap.put(clusterIndex, ipsStr);
                                        replyProcessor.onNext(1);
                                        httpProcessor.onComplete();
                                    })
                                    .end();
                        });
                        httpProcessor.onNext(0);
                    }
                }, e -> MsException.dealException(request, e));
        return ErrorNo.SUCCESS_STATUS;
    }

    public int getOtherClusterIps(MsHttpRequest request) {
        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "no auth to sync."));
            return UNKNOWN_ERROR;
        }

        String specEth = request.getHeader("specEth");
        Mono.just(1).publishOn(SCAN_SCHEDULER)
                .map(v -> getIpBySpecEthFromMySql(specEth))
                .map(str -> {
                    if (StringUtils.isBlank(str)) {
                        log.error("getOtherClusterIps: no ip under this eth, {}", specEth);
                        throw new MsException(UNKNOWN_ERROR, "getOtherClusterIps: no ip under this eth ");
                    }
                    String[] ips = str.split("\n");
                    if (ips.length != INDEX_SNUM_MAP.get(LOCAL_CLUSTER_INDEX)) {
                        log.error("getOtherClusterIps: ip amount does not match, {}, {}", specEth, str);
                        throw new MsException(UNKNOWN_ERROR, "getOtherClusterIps: ip amount does not match");
                    }
                    return ips;
                })
                .subscribe(localIpsList -> {
                    String join = StringUtils.join(localIpsList, ",");
                    log.info("getOtherClusterIps, clusterIndex: {}, eth: {}, ips: {}", LOCAL_CLUSTER_INDEX, specEth, join);
                    request.response()
                            .putHeader("ips", join)
                            .setStatusCode(200)
                            .end();
                }, e -> MsException.dealException(request, e));

        return ErrorNo.SUCCESS_STATUS;
    }

    public int checkBucketSync(MsHttpRequest request) {
        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "no auth to sync."));
            return UNKNOWN_ERROR;
        }
        String bucketName = request.getHeader("bucketName");
        String index = request.getHeader("index");
        pool.getReactive(REDIS_SYSINFO_INDEX).sismember(NEED_SYNC_BUCKETS, bucketName)
                .defaultIfEmpty(false)
                .zipWith(pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, LIFE_EXPIRATION_FLAG).defaultIfEmpty("0"))
                .subscribe(tuple2 -> {
                    boolean checkSync = tuple2.getT1() || "1".equals(tuple2.getT2());
                    if (!checkSync) {
                        checkSync = checkBucSyncState(bucketName, Integer.valueOf(index));
                    }
                    request.response().putHeader("checkSync", checkSync ? "1" : "0")
                            .setStatusCode(200)
                            .end();
                }, e -> MsException.dealException(request, e));

        return ErrorNo.SUCCESS_STATUS;
    }

    public int checkBucketCapacity(MsHttpRequest request) {
        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "no auth to sync."));
        }
        String bucketName = request.getHeader("bucketName");
        if (bucketName == null || bucketName.isEmpty()) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "bucketName is missing."));
        }
        ECUtils.hadObject(bucketName)
                .flatMap(b -> {
                    if (b) {
                        String uploadNr = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "upload_nr");
                        return Mono.just(DELETE_BUCKET_FLAG.equals(uploadNr));
                    }
                    return Mono.just(false);
                })
                .subscribe(
                        b -> request.response()
                                .putHeader("checkRes", b ? "1" : "0")
                                .setStatusCode(200)
                                .end(),
                        e -> MsException.dealException(request, e)
                );
        return ErrorNo.SUCCESS_STATUS;
    }

    public int checkOtherClusterBucket(MsHttpRequest request) {
        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "no auth to sync."));
            return UNKNOWN_ERROR;
        }

        String bucketName = request.getHeader("bucketName");
        boolean hasObject = ECUtils.hadObject(bucketName).block();
        return hasObject ? ErrorNo.OBJ_EXIST : ErrorNo.SUCCESS_STATUS;
    }

    private String getSpecEthFromMySql(String ip) {
        String getEthCommand = String.format("select name from port_info where ip='%s'", ip);
        return getStringFromMySql(getEthCommand);
    }

    private String getIpBySpecEthFromMySql(String specEth) {
        String getEthCommand = String.format("select ip from port_info where name='%s'", specEth);
        return getStringFromMySql(getEthCommand);
    }

    private String getNodeBySpecEthFromMySql(String specEth, String ip) {
        String getEthCommand = String.format("select node_name from port_info where name='%s' and ip='%s'", specEth, ip);
        return getStringFromMySql(getEthCommand);
    }

    private String getStringFromMySql(String getEthCommand) {
        String execCommand = String.format("mysql -uroot -p123456 -h%s moss_web -N -s -e \"%s\"",
                ServerConfig.getInstance().getMasterVip1(),
                getEthCommand);

        com.macrosan.utils.functional.Tuple2<String, String> execTuple2 = SshClientUtils.exec(execCommand, true, false);
        if (StringUtils.isNotBlank(execTuple2.var2)) {
            log.info("exec MySQL command fail. " + execTuple2.var2);
            return "";
        }
        return execTuple2.var1;
    }

    private Mono<Map<String, Integer>> getBucketSignsMap(String bucket) {
        return pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucket, BUCKET_SIGN_TYPE)
                .defaultIfEmpty("")
                .flatMap(s -> {
                    Map<String, Integer> map = new LinkedHashMap<>();
                    if (StringUtils.isNotBlank(s)) {
                        HashMap<String, Integer> hashMap = Json.decodeValue(s, new TypeReference<HashMap<String, Integer>>() {
                        });
                        for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
                            map.put(entry.getKey(), entry.getValue());
                        }
                    }
                    return Mono.just(map);
                });
    }

    public int getAWSSigns(MsHttpRequest request) {
        String bucket = request.getBucketName();
        getBucketSignsMap(bucket)
                .subscribe(map -> {
                    request.response()
                            .putHeader("bucket_sign_type", Json.encode(map))
                            .setStatusCode(200)
                            .end();
                }, e -> MsException.dealException(request, e));
        return ErrorNo.SUCCESS_STATUS;
    }

    public int setAWSSign(MsHttpRequest request) {
        String bucket = request.getBucketName();
        String srcIndex = request.getHeader("src_index");
        String dstIndex = request.getHeader("dst_index");
        String signType = request.getHeader("sign_type");

        getBucketSignsMap(bucket)
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(map -> {
                    map.put(srcIndex + "-" + dstIndex, Integer.parseInt(signType));
                    pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket, BUCKET_SIGN_TYPE, Json.encode(map));
                })
                .subscribe(s -> {
                    request.response()
                            .setStatusCode(200)
                            .end();
                }, e -> MsException.dealException(request, e));
        return ErrorNo.SUCCESS_STATUS;
    }

    public int deleteAWSSign(MsHttpRequest request) {
        String bucket = request.getBucketName();
        String srcIndex = request.getHeader("src_index");
        String dstIndex = request.getHeader("dst_index");

        getBucketSignsMap(bucket)
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(map -> {
                    map.remove(srcIndex + "-" + dstIndex);
                    pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket, BUCKET_SIGN_TYPE, Json.encode(map));
                })
                .subscribe(s -> {
                    request.response()
                            .setStatusCode(200)
                            .end();
                }, e -> MsException.dealException(request, e));
        return ErrorNo.SUCCESS_STATUS;
    }

    public int putArchiveCountList(MsHttpRequest request) {
        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            MsException.dealException(request, new MsException(UNKNOWN_ERROR, "no auth to sync."));
            return UNKNOWN_ERROR;
        }
        Buffer buffer = Buffer.buffer();
        request.handler(buffer::appendBuffer)
                .endHandler(vo -> {
                    com.macrosan.utils.functional.Tuple2<Map<String, Map<String, String>>, ConcurrentHashSet<String>> map = Json.decodeValue(buffer, new TypeReference<com.macrosan.utils.functional.Tuple2<Map<String, Map<String, String>>, ConcurrentHashSet<String>>>() {
                    });
                    Map<String, Map<String, String>> kmap = map.var1;
                    ConcurrentHashSet<String> delKSet = map.var2;
                    vertx.runOnContext(v2 -> {
                        try {
                            kmap.forEach((k, v) -> pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hmset(k, v));

                            if (!delKSet.isEmpty()) {
                                log.info("delete useless analyzerKey: {}", delKSet);
                                pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).del(delKSet.toArray(new String[0]));
                            }

                            addPublicHeaders(request, getRequestId())
                                    .setStatusCode(200);
                            addAllowHeader(request.response()).end();
                        } catch (Exception e) {
                            log.error("getArchiveCountList err, ", e);
                            MsException.dealException(request, e);
                        }
                    });
                })
                .exceptionHandler(e -> {
                    logger.error(e);
                    MsException.dealException(request, e);
                })
                .resume();

        if (request.headers().contains(EXPECT)) {
            request.response().writeContinue();
        }
        return ErrorNo.SUCCESS_STATUS;
    }
}