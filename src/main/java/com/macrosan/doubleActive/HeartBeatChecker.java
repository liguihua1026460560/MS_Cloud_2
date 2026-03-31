package com.macrosan.doubleActive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.ReadWriteLock;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.redis.SampleConnection;
import com.macrosan.doubleActive.arbitration.Arbitrator;
import com.macrosan.doubleActive.arbitration.ArbitratorUtils;
import com.macrosan.doubleActive.arbitration.DAVersionUtils;
import com.macrosan.doubleActive.archive.ArchiveHandler;
import com.macrosan.doubleActive.deployment.AddClusterHandler;
import com.macrosan.doubleActive.deployment.BucketSyncChecker;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.httpserver.MossHttpClient;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.BucketInfo;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.socketmsg.SocketSender;
import com.macrosan.message.socketmsg.StringResMsg;
import com.macrosan.message.xmlmsg.BucketCapInfoResult;
import com.macrosan.message.xmlmsg.BucketUsedObjectsResult;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.codec.UrlEncoder;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.SshClientUtils;
import com.macrosan.utils.property.PropertyReader;
import com.macrosan.utils.serialize.JaxbUtils;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanStream;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.http.HttpClient;
import io.vertx.reactivex.core.http.HttpClientRequest;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.function.Tuple2;

import java.io.File;
import java.net.InetAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.*;
import static com.macrosan.doubleActive.DataSynChecker.DATA_SYNC_STATE.SYNCED;
import static com.macrosan.doubleActive.DoubleActiveUtil.*;
import static com.macrosan.doubleActive.arbitration.Arbitrator.*;
import static com.macrosan.doubleActive.arbitration.ArbitratorUtils.*;
import static com.macrosan.httpserver.MossHttpClient.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.utils.regex.PatternConst.BUCKET_NAME_PATTERN;

/**
 * 启动心跳模块，随主节点切换启停。判断站点心跳是否正常有以下标准：Pool_index池状态，Pool_data池状态，各节点间状态（后端网络是否正常）
 * <br/>
 * 除心跳外，还会根据节点的双活网络状态、后端网络状态，动态更新可访问的节点。
 *
 * @author fanjunxi
 */
@Log4j2
public class HeartBeatChecker {
    private static RedisConnPool pool = RedisConnPool.getInstance();

    private static final String UUID = ServerConfig.getInstance().getHostUuid();

    private static Vertx vertx;

    private static HttpClient httpClient;

    private static HttpClient httpsClient;

    private static SocketSender sender = SocketSender.getInstance();

    /**
     * 心跳间隔
     */
    public static final int HEARTBEAT_INTERVAL_SECONDS = 3;

    /**
     * 多少次心跳失败后视作站点不可用
     */
    private static final int HEARTBEAT_INTERVAL_TIMES = 3;

    /**
     * 要等待其他业务模块启动，心跳定时器启动需要一个延时
     */
    public static final int INIT_DELAY_SECONDS = 9;

    /**
     * 每个站点下各自有多少个节点
     */
    public static final Map<Integer, Integer> INDEX_SNUM_MAP = new HashMap<>();

    /**
     * 统计各站点状态，出现1次心跳异常则+1，大于等于HEARTBEAT_INTERVAL_TIMES则认为心跳异常
     */
    public static final Map<Integer, AtomicLong> INDEX_STATUS_COUNT_MAP = new ConcurrentHashMap<>();

    /**
     * 保存各站点link_state状态，0表示异常，1表示正常。
     */
    public static final Map<Integer, Integer> INDEX_STATUS_MAP = new ConcurrentHashMap<>();

    /**
     * 保存各站点下所有节点的状态。注意，记录的状态并非指能否与主站点联通，而是该站点各个节点的后端网络的状态及MS_Cloud是否能通信。0异常，1正常。
     * [syncIp, status]
     */
    public static final Map<String, Integer> IP_STATUS_BACKEND_MAP = new ConcurrentHashMap<>();

    /**
     * 各节点与主站点的双活网络是否联通。0异常，1正常。
     */
    public static final Map<String, Integer> IP_STATUS_MAP = new ConcurrentHashMap<>();

    /**
     * 统计其他站点下的各link_state
     */
    public static final Map<Integer, Map<Integer, Integer>> OTHER_INDEXES_STATUS_MAP = new ConcurrentHashMap<>();

    public static final Map<Integer, Map<Integer, String>> OTHER_INDEXES_SYNC_STATE_MAP = new ConcurrentHashMap<>();

    public static HeartBeatChecker instance;

    public static HeartBeatChecker getInstance() {
        if (instance == null) {
            instance = new HeartBeatChecker();
        }
        return instance;
    }

    public static HttpClient getClient() {
        if (IS_SSL_SYNC.get()) {
            return httpsClient;
        }
        return httpClient;
    }

    public static HttpClient getHttpClient() {
        return httpClient;
    }

    private static boolean hasHeartBeatStarted = false;

    public static boolean isMultiAliveStarted = false;

    public static boolean isNotDeleteEs = true;

    public static String syncPolicy = "";

    private static AtomicBoolean IS_OBJ_NUM_CHECKING = new AtomicBoolean();

    private static AtomicLong HEART_BEAT_NUM = new AtomicLong(0);

    private static AtomicBoolean HEART_BEAT_RESET = new AtomicBoolean();

    private static AtomicBoolean CHECK_TIME_OUT_IPS = new AtomicBoolean();

    // 保存所有本地站点的eth4 ip
    public final static Map<String, String> UUID_ETH4IP_MAP = new ConcurrentHashMap<>();

    // 保存所有主备站点的uuid，0001、0002
    public final static Set<String> CONFIG_NODE_SET = new ConcurrentSkipListSet<>();

    public static void multiLiveCheck() {
        startMultiLive();
        SCAN_TIMER.scheduleWithFixedDelay(() -> {
            try {
                String syncIpList = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_IPS);
                if (StringUtils.isBlank(syncIpList)) {
                    // 启动单站点的前端包检测
                    ScanStream.scan(pool.getReactive(REDIS_NODEINFO_INDEX), new ScanArgs().match("*"))
                            .flatMap(uuid -> pool.getReactive(REDIS_NODEINFO_INDEX).hget(uuid, HEART_ETH1).defaultIfEmpty("")
                                    .map(heartIp -> {
                                        if (uuid.equals(ServerConfig.getInstance().getHostUuid())) {
                                            LOCAL_BACKEND_IP = heartIp;
                                        }
                                        UUID_ETH4IP_MAP.put(uuid, heartIp);
                                        return uuid;
                                    }))
                            .collectList()
                            .subscribeOn(ErasureServer.DISK_SCHEDULER)
                            .doOnNext(currentUuids -> {
                                // 删除不存在的旧节点(缩减节点)
                                UUID_ETH4IP_MAP.keySet().retainAll(currentUuids);
                                pingLocalBackendIp(UUID_ETH4IP_MAP);
                            })
                            // 更新CONFIG_NODE_SET
                            .flatMap(l -> pool.getReactive(REDIS_SYSINFO_INDEX).get("config_node_list").defaultIfEmpty(""))
                            .doOnNext(nodeStr -> {
                                Set<String> set = Json.decodeValue(nodeStr, new TypeReference<Set<String>>() {
                                });
                                CONFIG_NODE_SET.removeIf(s -> !set.contains(s));
                                CONFIG_NODE_SET.addAll(set);
                            })
                            .doOnError(e -> log.error("single pingLocalBackendIp error, ", e))
                            .subscribe();
                    return;
                }
                //local_cluster的ip_list发生变化说明添加了站点，需要重新初始化双活模块
                String[] nodesIps = syncIpList.split(";");
                if (nodesIps.length < INDEX_IPS_ENTIRE_MAP.size() && isMultiAliveStarted) {
                    isMultiAliveStarted = false;
                    hasHeartBeatStarted = false;
                    HeartBeatChecker.getInstance().close();
                    DataSynChecker.getInstance().close();
                    MossHttpClient.getInstance().close();
                } else if (nodesIps.length > INDEX_IPS_ENTIRE_MAP.size() && isMultiAliveStarted) {
                    //站点数量增加，重新初始化部分参数
                    log.info("Add cluster. curIps:{}, change to: {}", Json.encode(INDEX_IPS_ENTIRE_MAP), syncIpList);
                    Set<Integer> indexSet = new HashSet<>();
                    for (int i = INDEX_IPS_ENTIRE_MAP.size(); i < nodesIps.length; i++) {
                        indexSet.add(i);
                    }
                    refreshPublicFields(indexSet);
                } else if (hasIplistChanged(nodesIps) && isMultiAliveStarted) {
                    //节点数量增加，重新初始化部分参数
                    log.info("Add node. curIps:{}, change to: {}", Json.encode(INDEX_IPS_ENTIRE_MAP), syncIpList);
                    refreshPublicFields(null);
                }
                startMultiLive();
            } catch (Exception e) {
                log.error("scheduleAtFixedRate0, ", e);
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    private static boolean hasIplistChanged(String[] nodesIps) {
        if (nodesIps.length != INDEX_IPS_ENTIRE_MAP.size()) {
            return true;
        }

        for (int i = 0; i < nodesIps.length; i++) {
            String[] split = nodesIps[i].split(",");
            if (!Arrays.equals(split, INDEX_IPS_ENTIRE_MAP.get(i))) {
                // 本站点eth12发生变化，移除eth12对应的转发eth4映射
                if (LOCAL_CLUSTER_INDEX == i) {
                    Set<String> entireIps = new HashSet<>(Arrays.asList(INDEX_IPS_ENTIRE_MAP.get(i)));
                    for (String ip : split) {
                        entireIps.remove(ip);
                    }
                    if (entireIps.size() > 0) {
                        entireIps.forEach(SYNC_ETH4_IP_MAP::remove);
                    }
                }
                return true;
            }
        }
        return false;
    }

    /**
     * 检测redis中有无master_cluster，据此判断是否要启动双活功能
     */
    private static void startMultiLive() {
        try {
            // 尚未启动双活且存在MSTER_CLUSTER字段则开启双活模块，开始同步已有数据。
            Map<String, String> masterClusterMap = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(MASTER_CLUSTER);
            isNotDeleteEs = "false".equals(masterClusterMap.getOrDefault("deleteEs", "false"));
            if (!isMultiAliveStarted && !masterClusterMap.isEmpty()) {
                log.info("sync data modules start.");
                ACLUtils.init(false);
                MossHttpClient.getInstance().init();
                MainNodeSelector.getInstance().init();
                DataSynChecker.getInstance().init();
                HeartBeatChecker.getInstance().init();
                Arbitrator.getInstance().init();
                DAVersionUtils.getInstance().init();
                // 若初次启动，isMultiAliveStarted设为true表示双活异步复制初始化完毕，可以开始记录record。
                // 初始化前存在的数据均视为历史数据。
                isMultiAliveStarted = true;
                AddClusterHandler.init();
                BucketSyncChecker.init();
                BucketCloseChecker.getInstance().init();
                BadDataChecker.getInstance().init();
                ArchiveHandler.getInstance().init();
                // 通知后端包定时扫描复制链路更改情况
                SocketReqMsg dmSocket = new SocketReqMsg("startModifyLinkScan", 0);
                sender.sendAndGetResponse(dmSocket, StringResMsg.class, true);
                log.info("send msg to ms_cloud scan sync link modify !!!");
            }

            if (isMultiAliveStarted && masterClusterMap.isEmpty()) {
                log.info("sync data modules end.");
                isMultiAliveStarted = false;
                HeartBeatChecker.getInstance().close();
                DataSynChecker.getInstance().close();
                MossHttpClient.getInstance().close();
                AddClusterHandler.close();
            }
            // 多站点未部署仲裁的情况下，检测term是否发送变化
            if (isMultiAliveStarted && !masterClusterMap.isEmpty() && !DAVersionUtils.isStrictConsis()) {
                // 没有开仲裁，默认是第一个term。（生成DASyncStamp用）
                String term = pool.getCommand(REDIS_ROCK_INDEX).get(DA_TERM);
                String count = pool.getCommand(REDIS_ROCK_INDEX).get("FLOAT_COUNT");
                if (StringUtils.isNotBlank(term)) {
                    long newTerm = Long.parseLong(term);
                    synchronized (TERM) {
                        if (TERM.get() != newTerm) {
                            String masterIndex = pool.getCommand(REDIS_ROCK_INDEX).get(MASTER_CLUSTER_INDEX);
                            TERM.set(newTerm);
                            MASTER_INDEX = Integer.parseInt(masterIndex);
                            INDEX_TERM_MAP.put(TERM.get(), MASTER_INDEX);
                            log.info("set new term: {}, master index: {}", TERM.get(), MASTER_INDEX);
                        }
                    }
                }
                if (StringUtils.isNotBlank(count)) {
                    long newCount = Long.parseLong(count);
                    if (FLOAT_COUNT.get() != newCount) {
                        FLOAT_COUNT.set(newCount);
                        log.info("set new float count: {}", FLOAT_COUNT.get());
                    }
                }
            }
            if (masterClusterMap.containsKey(SYNC_POLICY)) {
                syncPolicy = masterClusterMap.get(SYNC_POLICY);
            }
            boolean needToInformArbitrator = false;
            String syncMethod = pool.getCommand(REDIS_SYSINFO_INDEX).get("sync_method");
            boolean syncMethodRedis = StringUtils.isNotEmpty(syncMethod) && !"http".equals(syncMethod);
            if (IS_SSL_SYNC.get() != syncMethodRedis) {
                needToInformArbitrator = true;
                IS_SSL_SYNC.set(syncMethodRedis);
                siteStateChange();
            }

            DA_HTTP_PORT = Integer.parseInt(ServerConfig.getInstance().getHttpPort());
            String port = !IS_SSL_SYNC.get() ? ServerConfig.getInstance().getHttpPort()
                    : ServerConfig.getInstance().getHttpsPort();
            if (DA_PORT != Integer.parseInt(port)) {
                needToInformArbitrator = true;
                DA_PORT = Integer.parseInt(port);
            }
            if (needToInformArbitrator && DAVersionUtils.isStrictConsis()) {
                Arbitrator.getInstance().informDAPort().subscribe();
            }
        } catch (Exception e) {
            log.error("startMultiLive err, ", e);
            isMultiAliveStarted = false;
        }
    }

    private static void refreshPublicFields(Set<Integer> indexSet) {
        MossHttpClient.initClustersCollections();
        DataSynChecker.getInstance().init();
        HeartBeatChecker.initClustersCollections();
        BucketSyncChecker.updateIndexSet(indexSet);
        HeartBeatChecker.initSiteIpSet();

        if (indexSet != null) {
            for (Integer index : indexSet) {
//            RocksDBCheckPoint.init(index);
                AddClusterHandler.checkAddClusterTimeStamp(index);
            }
        }
        log.info("Clusters' status has updated.");
    }

    private static ScheduledFuture scheduledFuture1;

    private static ScheduledFuture scheduledFuture2;

    private static ScheduledFuture scheduledFuture3;

    public static AtomicBoolean init_finished = new AtomicBoolean();

    public static SampleConnection connectDb2;

    static {
        vertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(PROC_NUM)
                .setPreferNativeTransport(true));
        HttpClientOptions options = new HttpClientOptions()
                .setKeepAlive(true)
                .setKeepAliveTimeout(60)
                .setMaxPoolSize(100)
                .setConnectTimeout(3000)
                .setIdleTimeout(30);
        HttpClientOptions httpsOptions = new HttpClientOptions()
                .setKeepAlive(true)
                .setKeepAliveTimeout(60)
                .setMaxPoolSize(100)
                .setConnectTimeout(3000)
                .setIdleTimeout(30)
                .setSsl(true)
                .setVerifyHost(false)
                .setTrustAll(true);
        httpClient = vertx.createHttpClient(options);
        httpsClient = vertx.createHttpClient(httpsOptions);
    }

    public void init() {
//        AVAIL_BACKEND_IP_ENTIRE_SET.add(ServerConfig.getInstance().getHeartIp1());
        initClustersCollections();
        AVAIL_BACKEND_IP_ENTIRE_SET.addAll(SYNC_ETH4_IP_MAP.values());
        pingLocalBackendIp();

        resetAvailIps();

        Optional.ofNullable(connectDb2).ifPresent(conn -> conn.getConnection().close());
        connectDb2 = new SampleConnection(RedisConnPool.getInstance().getMainNodes(REDIS_SYSINFO_INDEX));

        FRONT_ETH_NAME.add("eth2");
        FRONT_ETH_NAME.add("eth3");

        Optional.ofNullable(scheduledFuture1).ifPresent(scheduledFuture -> scheduledFuture.cancel(true));
        scheduledFuture1 = SCAN_TIMER.scheduleAtFixedRate(() -> {
            try {
                log.debug("start scheduledFuture1");
                getAvailClusters();
                setRedirectableBackendIps();
                checkAbtIpConnected();
                getNodeAmountPerRound();
                recheckTimeoutIp();
                //节点为站点的主节点且启动了双活，才会启动心跳流
                //非心跳节点，如果有心跳流，则中止
                if (MainNodeSelector.checkIfSyncNode()) {
                    if (!hasHeartBeatStarted) {
                        hasHeartBeatStarted = true;
                        startHeartBeat();
                        // 切换扫描节点的时候重置选举状态，更新clusterStatus
                        ArbitratorUtils.setElectionStatus(0);
                        getClustersStatus();
                    }
                    pingEveryNode();
                    refreshNodeStatus();
                    flushClustersStatus();
                    readMIps();
                    if (HEART_BEAT_NUM.get() > 3 && HEART_BEAT_RESET.compareAndSet(false, true)) {
                        log.info("reset link state and sync state!");
                        siteLinkStateChange();
                        siteSyncStateChange();
                    }
                    if (!EXTRA_INDEX_IPS_ENTIRE_MAP.isEmpty()) {
                        checkExtraLinkState();
                    }
                } else {
                    Optional.ofNullable(scheduledFuture2).ifPresent(scheduledFuture -> scheduledFuture.cancel(true));
                    hasHeartBeatStarted = false;
                    getAvailIpList();
                    getClustersStatus();
                    getOtherClustersStatus();
                }
                checkFirstAbtNormalNode();
                checkAbtStatus();
                dealDns();
                refreshConfigNodeList();
                if (!EXTRA_INDEX_IPS_ENTIRE_MAP.isEmpty()) {
                    pingExtraNode();
                }
                init_finished.compareAndSet(false, true);
            } catch (Exception e) {
                log.error("scheduleAtFixedRate1, ", e);
            }
        }, 9, 9, TimeUnit.SECONDS);

        Optional.ofNullable(scheduledFuture3).ifPresent(scheduledFuture -> scheduledFuture.cancel(true));
        scheduledFuture3 = SCAN_TIMER.scheduleAtFixedRate(() -> {
            log.debug("pinglocalbackend");
            pingLocalBackendIp();
        }, 3, 3, TimeUnit.SECONDS);

        // 所有站点前端包一起重启有可能出现第一次firstCheckMaster查到的结果不及时的问题（比如对端站点正好在选举过程中）
        SCAN_TIMER.schedule(() -> Arbitrator.getInstance().firstCheckMaster(), 60, TimeUnit.SECONDS);
    }

    /**
     * 保存本站点各个节点的eth4，包括本地节点。[sync ip, eth4 ip]，注意只有本站点的信息，没有对站点
     */
    public static final Map<String, String> SYNC_ETH4_IP_MAP = new ConcurrentHashMap<>();

    /**
     * 保存本站点各个节点的eth4，包括本地节点。[eth4 ip, sync ip, ]，注意只有本站点的信息，没有对站点
     */
    public static final Map<String, String> ETH4_SYNC_IP_MAP = new ConcurrentHashMap<>();

    public static final Map<String, String> UUID_BACKEND_IP_MAP = new ConcurrentHashMap<>();

    /**
     * 保存各站点在心跳异常后，是否有心跳请求轮询全部失败。如果有则需要在下一次恢复上线时检查主站点信息
     */
    public static final Map<Integer, AtomicBoolean> INDEX_REQ_FAIL_COUNT_MAP = new ConcurrentHashMap<>();

    /**
     * 通过eth4访问本站点所有节点的MS_Cloud，包括本地节点，保存前端包已经可用的eth4 ip。
     * 对于前端包来说，可用的标志是可以访问接口。如果有仲裁则还需要canGet为true。
     */
    public static final Set<String> AVAIL_BACKEND_IP_ENTIRE_SET = new ConcurrentHashSet<>();

    /**
     * 除本地eth4外的,eth4和eth12均正常节点的eth4ip，用于转发
     */
    public static final Set<String> REDIRECTABLE_BACKEND_IP_SET = new ConcurrentHashSet<>();

    public static final Map<String, String> BUSINESS_ETH_MAP = new ConcurrentSkipListMap<>();

    /**
     * 保存主备备的syncIp
     */
    public static final Set<String> CONFIG_NODE_SYNCIP_SET = new ConcurrentHashSet<>();

    public static final Map<String, String> CONFIG_NODE_SYNCIP_MAP = new ConcurrentHashMap();

    public static String LOCAL_BACKEND_IP;

    /**
     * 保存站点索引和mip。
     */
    public static final Map<Integer, String[]> M_IPS_ENTIRE_MAP = new HashMap<>();

    /**
     * 保存其他站点的eth2状态
     */
    public static final Map<Integer, Map<String, String>> INDEX_ETH2_MAP = new ConcurrentHashMap<>();

    public static final Map<Integer, Map<String, String>> INDEX_ETH3_MAP = new ConcurrentHashMap<>();

    public static final Map<Integer, Map<String, String>> INDEX_AVAIL_MAP = new ConcurrentHashMap<>();

    private static void initClustersCollections() {
        INDEX_IPS_ENTIRE_MAP.forEach((clusterIndex, ips) -> {
            INDEX_STATUS_COUNT_MAP.put(clusterIndex, new AtomicLong());
            INDEX_STATUS_MAP.put(clusterIndex, 1);
            INDEX_SNUM_MAP.put(clusterIndex, ips.length);
            INDEX_REQ_FAIL_COUNT_MAP.put(clusterIndex, new AtomicBoolean());
        });
        IP_STATUS_MAP.clear();
        IP_INDEX_MAP.forEach((ip, clusterIndex) -> {
            IP_STATUS_BACKEND_MAP.put(ip, 1);
            IP_STATUS_MAP.put(ip, 1);
        });

        String nodeListStr = pool.getCommand(REDIS_SYSINFO_INDEX).get("node_list");
        for (String uuid : JSON.parseArray(nodeListStr, String.class)) {
            String eth4Ip = pool.getCommand(REDIS_NODEINFO_INDEX).hget(uuid, HEART_ETH1);
            UUID_BACKEND_IP_MAP.put(uuid, eth4Ip);

            String syncNodeIp = "";
            SYNC_ETH_NAME = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SYNC_ETH);
            String ethType = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, ETH_TYPE);
            if (StringUtils.isNotEmpty(ethType)) {
                SYNC_ETH_TYPE = ethType;
            }
            if (StringUtils.isNotEmpty(SYNC_ETH_NAME)) {
                String syncEthToType = syncEthToType(SYNC_ETH_NAME, ethType);
                syncNodeIp = pool.getCommand(REDIS_NODEINFO_INDEX).hget(uuid, syncEthToType);
            } else {
                syncNodeIp = pool.getCommand(REDIS_NODEINFO_INDEX).hget(uuid, ACTIVE_SYNC_IP);
                if (StringUtils.isNotEmpty(syncNodeIp)) {
                    SYNC_ETH_NAME = "eth12";
                } else {
                    SYNC_ETH_NAME = "eth4";
                }
            }
            String syncIp = !StringUtils.isEmpty(syncNodeIp) ? syncNodeIp : eth4Ip;

            SYNC_ETH4_IP_MAP.put(syncIp, eth4Ip);
            ETH4_SYNC_IP_MAP.put(eth4Ip, syncIp);
            if (uuid.equals(ServerConfig.getInstance().getHostUuid())) {
                LOCAL_BACKEND_IP = eth4Ip;
            }
        }

        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : SYNC_ETH4_IP_MAP.entrySet()) {
            stringBuilder.append(entry.getKey() + ", ");
        }
        log.info("SYNC_ETH4_IP_MAP order: {}", stringBuilder.toString());

        String businessEthNumStr = pool.getCommand(REDIS_SYSINFO_INDEX).get("businessEthNum");
        ReadWriteLock.readLock(true);
        PropertyReader reader = new PropertyReader(PUBLIC_CONF_FILE);
        for (int i = 1; i < Integer.parseInt(businessEthNumStr) + 1; i++) {
            String ethName = "business_eth" + i;
            BUSINESS_ETH_MAP.put(ethName, reader.getPropertyAsString(ethName));
        }
        ReadWriteLock.unLock(true);
        String config_node_list = pool.getCommand(REDIS_SYSINFO_INDEX).get("config_node_list");
        Set<String> set = Json.decodeValue(config_node_list, new TypeReference<Set<String>>() {
        });
        for (String uuid : set) {
            String eth4Ip = UUID_BACKEND_IP_MAP.get(uuid);
            String syncIp = ETH4_SYNC_IP_MAP.get(eth4Ip);
            CONFIG_NODE_SYNCIP_SET.add(syncIp);
            CONFIG_NODE_SYNCIP_MAP.put(uuid, syncIp);
        }
    }

    /**
     * 更新config_node_list
     */
    private static void refreshConfigNodeList() {
        pool.getReactive(REDIS_SYSINFO_INDEX).get("config_node_list")
                .subscribe(nodeStr -> {
                    try {
                        Set<String> set = Json.decodeValue(nodeStr, new TypeReference<Set<String>>() {
                        });
                        if (!CONFIG_NODE_SYNCIP_MAP.keySet().containsAll(set)) {
                            for (String uuid : set) {
                                if (!UUID_BACKEND_IP_MAP.containsKey(uuid)) {
                                    continue;
                                }
                                String eth4Ip = UUID_BACKEND_IP_MAP.get(uuid);
                                String syncIp = ETH4_SYNC_IP_MAP.get(eth4Ip);
                                CONFIG_NODE_SYNCIP_SET.add(syncIp);
                                CONFIG_NODE_SYNCIP_MAP.put(uuid, syncIp);
                            }
                            for (String uuid : CONFIG_NODE_SYNCIP_MAP.keySet()) {
                                if (!set.contains(uuid)) {
                                    CONFIG_NODE_SYNCIP_SET.remove(CONFIG_NODE_SYNCIP_MAP.get(uuid));
                                    CONFIG_NODE_SYNCIP_MAP.remove(uuid);
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("refresh config node list error.", e);
                    }
                }, e -> log.error("", e));

    }

    public void close() {
        if (vertx != null) {
            vertx.close();
            vertx = null;
        }
        if (httpClient != null) {
            httpClient.close();
            httpClient = null;
        }
        if (httpsClient != null) {
            httpsClient.close();
            httpsClient = null;
        }
        hasHeartBeatStarted = false;
        OTHER_INDEXES_SYNC_STATE_MAP.clear();
        Optional.ofNullable(scheduledFuture1).ifPresent(scheduledFuture -> scheduledFuture.cancel(true));
        Optional.ofNullable(scheduledFuture2).ifPresent(scheduledFuture -> scheduledFuture.cancel(true));
        Optional.ofNullable(scheduledFuture3).ifPresent(scheduledFuture -> scheduledFuture.cancel(true));
    }

    private void log() {
        //---------------
        StringBuilder availIpSb = new StringBuilder();
        StringBuilder heartBeatCountSb = new StringBuilder();
        StringBuilder availClusterSb = new StringBuilder();

        INDEX_IPS_MAP.forEach((index, ips) -> {
            for (String ip : ips) {
                availIpSb.append(ip).append(", ");
            }
            availIpSb.append(" | ");
        });
        INDEX_STATUS_COUNT_MAP.forEach((index, count) -> heartBeatCountSb.append(index.toString()).append(":").append(count).append(", "));
        INDEX_STATUS_MAP.forEach((index, status) -> availClusterSb.append(index.toString()).append(":").append(status).append(", "));

        log.info(" available ips:{}", availIpSb.toString());
        log.info("heartbeat count: {}", heartBeatCountSb.toString());
        log.info("available cluster: {}", availClusterSb.toString());
//-------------------
    }

    private void startHeartBeat() {
        try {
            //启动心跳时初始化各map
            resetAvailIps();
            //防止特殊情况下有多个心跳流
            Optional.ofNullable(scheduledFuture2).ifPresent(scheduledFuture -> scheduledFuture.cancel(true));
            scheduledFuture2 = SCAN_TIMER.scheduleAtFixedRate(() -> {
                try {
                    startChecker();
                    objNumCheckStart();
                    if (HEART_BEAT_NUM.get() < 4) {
                        HEART_BEAT_NUM.incrementAndGet();
                    }
                } catch (Exception e) {
                    log.error("scheduleAtFixedRate2, ", e);
                }
            }, HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("start heart beat failed, ", e);
        }
    }

    public static final ConcurrentHashSet<String> TIMEOUT_RECHECK_IP_SET = new ConcurrentHashSet<>();

    /**
     * 发起请求获取各站点双活状态
     */
    private static void startChecker() {
        String requestId = RandomStringUtils.randomAlphanumeric(10);
        for (Map.Entry<Integer, String[]> mapEntry : INDEX_IPS_ENTIRE_MAP.entrySet()) {
            Integer clusterIndex = mapEntry.getKey();
            String[] ips = mapEntry.getValue();
            if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex)) {
                continue;
            }
            String[] availIps;
            List<String> ipList = new ArrayList<>();
            for (String ip : ips) {
                if (!TIMEOUT_RECHECK_IP_SET.contains(ip)) {
                    ipList.add(ip);
                }
            }

            // 所有节点在双倍时间都没能恢复, 访问第一个节点发送请求
            if (ipList.size() == 0) {
                ipList.add(ips[0]);
            }

            //遍历目标站点ip
            availIps = ipList.toArray(new String[0]);
            PoolingRequest poolingRequest = new PoolingRequest(HttpMethod.GET, clusterIndex, "?clusterstatus", getClient());
            poolingRequest.setAuthString(INNER_AUTH);
            Disposable subscribe = poolingRequest.requestHeartBeat(availIps, HEARTBEAT_INTERVAL_SECONDS * 1000, unitCount)
                    .subscribe(request -> {
                        AtomicBoolean hasExc = new AtomicBoolean(false);
//                        request.setTimeout(HEARTBEAT_INTERVAL_SECONDS * 1000 / INDEX_SNUM_MAP.get(clusterIndex));
                        request.putHeader("test", CURRENT_IP);
                        request.putHeader("request_index", String.valueOf(LOCAL_CLUSTER_INDEX));
                        if (!DAVersionUtils.isStrictConsis()) {
                            request.putHeader("no_arb", "1");
                        }
//                        request.putHeader(SYNC_AUTH, PASSWORD);
                        long cur1 = System.currentTimeMillis();
                        request.exceptionHandler(e -> {
                            request.reset();
                            if (!clusterIndex.equals(LOCAL_CLUSTER_INDEX)) {
                                OTHER_INDEXES_STATUS_MAP.remove(clusterIndex);
                            }
                            if (!hasExc.compareAndSet(false, true)) {
                                return;
                            }
                            if (e instanceof TimeoutException) {
                                String finalIp = request.getHost();
                                int idx = finalIp.lastIndexOf(":");
                                if (idx != -1) {
                                    finalIp = finalIp.substring(0, idx);
                                }
                                TIMEOUT_RECHECK_IP_SET.add(finalIp);
                            }
                            log.error("heartbeat request error, {}, {}, requestId:{}", e.getClass().getTypeName(), e.getMessage(), requestId);
                            if (poolingRequest.needRetry()) {
                                poolingRequest.retryProcessor.onNext(1);
                            } else {
                                // 轮询皆失败，此次心跳失败
                                failHandler(clusterIndex);
                                if (!heartBeatIsNormal(clusterIndex)) {
                                    INDEX_REQ_FAIL_COUNT_MAP.get(clusterIndex).compareAndSet(false, true);
                                }
                                poolingRequest.retryProcessor.onComplete();
                            }
                        }).handler(resp -> {
                            // 新版本的心跳代码响应头加入了两个字段。为兼容旧版本的升级在判断时需要做区分。
                            long cur3 = System.currentTimeMillis();
                            log.debug("heartbeat request ionssfd, {}, requestId:{} {}", request.getHost(), requestId, cur3 - cur1);
                            boolean newVersion = false;
                            Set<String> availSyncIpSet = null;
                            Set<String> configNodeSyncIpSet = null;
                            if (StringUtils.isNotEmpty(resp.getHeader("service_avail_ip"))) {
                                // 后端网络正常且前端包端口已监听的syncIp集合
                                availSyncIpSet = Json.decodeValue(resp.getHeader("service_avail_ip"), new TypeReference<Set<String>>() {
                                });
                                // 存主备备的syncIp
                                configNodeSyncIpSet = Json.decodeValue(resp.getHeader("CONFIG_NODE_SYNCIP_SET"), new TypeReference<Set<String>>() {
                                });
                                newVersion = true;
                            }
                            boolean initFinished = false;
                            String initFinishedStr = resp.getHeader("init_finished");
                            if (StringUtils.isNotEmpty(initFinishedStr)) {
                                initFinished = Boolean.parseBoolean(initFinishedStr);
                            }

                            // 请求没有返200，继续轮询
                            // 或者，返回响应节点的所有的前端包检测都不通过，可能是该访问节点的eth4断了，也需要轮询
                            if (SUCCESS != resp.statusCode() || (newVersion && availSyncIpSet.isEmpty() && initFinished)) {
                                if (poolingRequest.needRetry()) {
                                    poolingRequest.retryProcessor.onNext(1);
                                } else {
                                    // 轮询皆失败，此次心跳失败
                                    log.error("every eth4 is abnormal, clusterIndex:{}, {}, {}", clusterIndex, resp.statusCode(), availSyncIpSet);
                                    failHandler(clusterIndex);
                                    if (!heartBeatIsNormal(clusterIndex)) {
                                        INDEX_REQ_FAIL_COUNT_MAP.get(clusterIndex).compareAndSet(false, true);
                                    }
                                    poolingRequest.retryProcessor.onComplete();
                                }
                                return;
                            }

                            // 所有主备节点前端包都不正常，则认为站点异常
                            if (newVersion && !checkConfigNodeSet(configNodeSyncIpSet, availSyncIpSet) && initFinished) {
                                log.error("checkConfigNodeSet error, clusterIndex:{}, {}, {}", clusterIndex, configNodeSyncIpSet, availSyncIpSet);
                                failHandler(clusterIndex);
                                if (!heartBeatIsNormal(clusterIndex)) {
                                    INDEX_REQ_FAIL_COUNT_MAP.get(clusterIndex).compareAndSet(false, true);
                                }
                                poolingRequest.retryProcessor.onComplete();
                                return;
                            }

                            //例：0-0-{"172.17.0.10":"1","172.17.0.12":"1","172.17.0.11":"1"}-{"0":1,"1":1,"2":1}-{\"0\":\"SYNCED\",\"1\":\"SYNCING\",\"2\":\"SYNCING\"}
                            // 站点的Pool_index池状态，Pool_data池状态，各节点间状态（[syncIp, server_state]），检测到的各站点心跳状态，差异记录同步状态
                            //多存储池环境可能有多个数据池索引池和缓存池
                            //存储池：0正常，1降级，2降级临界状态，3故障 4永久故障  | 节点间状态： 0断开，1正常
                            String ipsStatus = resp.headers().get(CLUSTER_VALUE);
                            String[] split = ipsStatus.split("-");
                            int poolIndexStatus = Integer.parseInt(split[0]);
                            int poolDataStatus = Integer.parseInt(split[1]);
                            JSONObject syncIpServerStateJsObj = JSON.parseObject(split[2]);
                            if (!clusterIndex.equals(LOCAL_CLUSTER_INDEX) && split.length > 3 && StringUtils.isNotEmpty(split[3])) {
                                Map<Integer, Integer> otherClusterStatusMap = Json.decodeValue(split[3], new TypeReference<Map<Integer, Integer>>() {
                                });
                                OTHER_INDEXES_STATUS_MAP.put(clusterIndex, otherClusterStatusMap);
                                log.debug("other_clusters_status in cluster {} is {}", clusterIndex, OTHER_INDEXES_STATUS_MAP);
                            }
                            if (split.length > 4 && StringUtils.isNotEmpty(split[4])) {
                                Map<Integer, String> otherClusterSyncStateMap = Json.decodeValue(split[4], new TypeReference<Map<Integer, String>>() {
                                });
                                OTHER_INDEXES_SYNC_STATE_MAP.put(clusterIndex, otherClusterSyncStateMap);
                                log.debug("other_clusters_status in cluster {} is {}", clusterIndex, OTHER_INDEXES_SYNC_STATE_MAP);
                            } else {
                                OTHER_INDEXES_SYNC_STATE_MAP.remove(clusterIndex);
                            }

                            // eth4断开的节点无法将本地redis表8中的server_state改为0，所以在该节点eth12正常时仍会向该节点发心跳，此时返回的server_state为1
                            int clusterRes = 0;
                            for (Map.Entry<String, Object> entry : syncIpServerStateJsObj.entrySet()) {
                                String syncIp = entry.getKey();
                                int serverStatus = Integer.parseInt(entry.getValue().toString());
                                if (serverStatus == 1) {
                                    if (newVersion && initFinished) {
                                        if (availSyncIpSet.contains(syncIp)) {
                                            clusterRes++;
                                            IP_STATUS_BACKEND_MAP.put(syncIp, 1);
                                        } else {
                                            IP_STATUS_BACKEND_MAP.put(syncIp, 0);
                                        }
                                    } else {
                                        clusterRes++;
                                        IP_STATUS_BACKEND_MAP.put(syncIp, 1);
                                    }
                                } else {
                                    IP_STATUS_BACKEND_MAP.put(syncIp, 0);
                                }
                            }

                            if (StringUtils.isNotEmpty(resp.getHeader(ETH2_STATUS))) {
                                Map<String, String> map = Json.decodeValue(resp.getHeader(ETH2_STATUS), new TypeReference<Map<String, String>>() {
                                });
                                INDEX_ETH2_MAP.computeIfAbsent(clusterIndex, k -> new HashMap<>()).putAll(map);
                            }

                            if (StringUtils.isNotEmpty(resp.getHeader(ETH3_STATUS))) {
                                Map<String, String> map = Json.decodeValue(resp.getHeader(ETH3_STATUS), new TypeReference<Map<String, String>>() {
                                });
                                INDEX_ETH3_MAP.computeIfAbsent(clusterIndex, k -> new HashMap<>()).putAll(map);
                            }

                            if (StringUtils.isNotEmpty(resp.getHeader(AVAILABLE_NODE_LIST))) {
                                Map<String, String> map = Json.decodeValue(resp.getHeader(AVAILABLE_NODE_LIST), new TypeReference<Map<String, String>>() {
                                });
                                INDEX_AVAIL_MAP.computeIfAbsent(clusterIndex, k -> new HashMap<>()).putAll(map);
                            }

                            //站点的后端网路正常（返回server_state为1的节点超过半数）且存储池无异常，视为所在站点在线
                            if ((clusterRes >= (INDEX_SNUM_MAP.get(clusterIndex) / 2 + 1)
                                    && poolIndexStatus < 3 && poolDataStatus < 3)) {
                                successHandler(clusterIndex);
                                // 没有部署仲裁的站点，如果检测到其他站点的任期变大，且主站点改变，本地需要主从站点切换
                                if (!IS_ASYNC_CLUSTER && !DAVersionUtils.isStrictConsis()) {
                                    long ecTerm = Long.parseLong(resp.getHeader(DA_TERM_HEADER));
                                    long floatCount = Long.parseLong(resp.getHeader(IP_FLOAT_COUNT_HEADER));
                                    int masterIndex = Integer.parseInt(resp.getHeader(MASTER_CLUSTER_INDEX_HEADER));
                                    String mipInfo = resp.getHeader(ADD_MIP_MAP_HEADER);
                                    log.debug("ecTerm: {}, masterIndex: {}, mipInfo:{}, TERM:{}, M_IP:{}", ecTerm, masterIndex, mipInfo, TERM.get(), M_IPS_ENTIRE_MAP);
                                    AtomicBoolean changeTerm = new AtomicBoolean();
                                    if (TERM.get() < ecTerm) {
                                        // 本地检测到和其他站点的连接状态恢复，任期小于其他站点
                                        changeTerm.compareAndSet(false, true);
                                    }
                                    if (FLOAT_COUNT.get() < floatCount) {
                                        // 检测到其他站点有手动切换vip的操作，float_count次数 + 1
                                        Mono.just(1).publishOn(SCAN_SCHEDULER).subscribe(s -> pool.getShortMasterCommand(REDIS_ROCK_INDEX).set("FLOAT_COUNT", String.valueOf(floatCount)));
                                    }
                                    if (!LOCAL_CLUSTER_INDEX.equals(clusterIndex) && changeTerm.get() && LOCAL_CLUSTER_INDEX != masterIndex && asyncUpgradeMaster.compareAndSet(false, true)) {
                                        MonoProcessor<Boolean> res = MonoProcessor.create();
                                        upgradeMasterRecursion(masterIndex, ecTerm, res);
                                        res.map(b -> {
                                            if (b) {
                                                return informNode("0");
                                            }
                                            return false;
                                        }).subscribe(s -> asyncUpgradeMaster.set(false));
                                    }

                                    // 没有部署仲裁的情况下，根据情况判断减去自身mip，本站点mip漂移到其他站点是靠脚本完成
                                    if (StringUtils.isNotBlank(mipInfo)) {
                                        if (!clusterIndex.equals(LOCAL_CLUSTER_INDEX)) {
                                            removeLocalMip(mipInfo);
                                        }
                                    }
                                }
                            } else {
                                log.error("getcluster status error, {}, clusterIndex:{}, {}, {}", request.getHost(), clusterIndex, ipsStatus, availSyncIpSet);
                                failHandler(clusterIndex);
                            }
                            poolingRequest.retryProcessor.onComplete();

                            if (IS_ASYNC_CLUSTER && !ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex)) {
                                int indexGet = Integer.parseInt(resp.getHeader(MASTER_CLUSTER_INDEX_HEADER));
                                long termGet = Long.parseLong(resp.getHeader(DA_TERM_HEADER));
                                boolean isEvalGet = Boolean.parseBoolean(resp.getHeader("isEvaluatingMaster"));
                                if (isEvalGet) {
                                    return;
                                }
                                if (TERM.get() < termGet && asyncUpgradeMaster.compareAndSet(false, true)) {
                                    MonoProcessor<Boolean> res = MonoProcessor.create();
                                    upgradeMasterRecursion(indexGet, termGet, res);
                                    res.map(b -> {
                                        if (b) {
                                            return informNode("0");
                                        }
                                        return b;
                                    }).subscribe(s -> {
                                        asyncUpgradeMaster.set(false);
                                    });
                                }
                            }
                        });
                        request.end();
                    }, e -> {
                        if (!clusterIndex.equals(LOCAL_CLUSTER_INDEX)) {
                            OTHER_INDEXES_STATUS_MAP.remove(clusterIndex);
                            OTHER_INDEXES_SYNC_STATE_MAP.remove(clusterIndex);
                        }
                        log.error("", e);
                        poolingRequest.retryProcessor.onComplete();
                    });
        }
    }

    private static void removeLocalMip(String mipInfo) {
        try {
            Map<String, String> mipMap = Json.decodeValue(mipInfo, new TypeReference<Map<String, String>>() {
            });
            if (mipMap.size() > 0) {
                String[] localMipStr = M_IPS_ENTIRE_MAP.getOrDefault(LOCAL_CLUSTER_INDEX, new String[]{});
                if (localMipStr.length == 0) {
                    return;
                }
                List<String> list = Arrays.asList(localMipStr);
                for (String index : mipMap.keySet()) {
                    // 判断其他站点是否手动添加了本站点的 mip，有则进行下掉本地的 mip
                    String[] mIps = mipMap.get(index).split("_");
                    int otherAddCount = Integer.parseInt(mIps[0]);
                    String[] otherAddMips = mIps[1].split(",");
                    Set<String> removeIps = new ConcurrentHashSet<>();
                    for (String addMip : otherAddMips) {
                        // 本地mip切换到其他站点
                        if (list.contains(addMip)) {
                            removeIps.add(addMip);
                        }
                    }
                    if (removeIps.size() > 0) {
                        String changeIps = pool.getCommand(REDIS_SYSINFO_INDEX).hget("active_live_change_ips", index);
                        if (StringUtils.isNotBlank(changeIps)) {
                            String[] info = changeIps.split("_");
                            int localAddCount = Integer.parseInt(info[0]);
                            List<String> mips = Arrays.asList(info[1].split(","));
                            if (localAddCount >= otherAddCount) {
                                removeIps.removeIf(mips::contains);
                            }
                        }
                        if (removeIps.size() > 0) {
                            String[] removeStr = removeIps.toArray(new String[0]);
                            removeMultiClusterIp(LOCAL_CLUSTER_INDEX, removeStr, "all");
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private static AtomicBoolean asyncUpgradeMaster = new AtomicBoolean(false);

    private static void objNumCheckStart() {
        pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(MASTER_CLUSTER)
                .flatMap(clusterMap -> {
                    if (clusterMap.isEmpty()) {
                        return Mono.just(-1);
                    }
                    String[] masterIps = clusterMap.get(CLUSTER_IPS).split(",");
                    if (Arrays.asList(masterIps).contains(LOCAL_NODE_IP)) {
                        return Mono.just(-1);
                    }
                    AtomicInteger index = new AtomicInteger(-1);
                    INDEX_IPS_ENTIRE_MAP.forEach((clusterIndex, ips) -> {
                        if (Arrays.asList(ips).contains(masterIps[0])) {
                            index.set(clusterIndex);
                        }
                    });
                    if (index.get() == -1 || INDEX_STATUS_MAP.get(index.get()) == 0 || !OTHER_INDEXES_SYNC_STATE_MAP.containsKey(index.get())) {
                        return Mono.just(-1);
                    }
                    if (IS_ASYNC_CLUSTER) {
                        for (Integer clusterIndex : DA_INDEX_IPS_ENTIRE_MAP.keySet()) {
                            if (currClusterSyncMap.get(index.get()) == DATA_SYNC_STATE.SYNCING || currClusterSyncMap.get(index.get()) == DATA_SYNC_STATE.INTERRUPTED
                                    || "SYNCING".equals(OTHER_INDEXES_SYNC_STATE_MAP.computeIfAbsent(clusterIndex, v -> new HashMap<>())
                                    .getOrDefault(LOCAL_CLUSTER_INDEX, "SYNCING")) ||
                                    "INTERRUPTED".equals(OTHER_INDEXES_SYNC_STATE_MAP.get(clusterIndex).getOrDefault(LOCAL_CLUSTER_INDEX, "SYNCING"))) {
                                return Mono.just(-1);
                            }
                        }
                    } else if (IS_THREE_SYNC) {
                        for (Integer clusterIndex : DA_INDEX_IPS_ENTIRE_MAP.keySet()) {
                            if (currClusterSyncMap.get(index.get()) == DATA_SYNC_STATE.SYNCING || currClusterSyncMap.get(index.get()) == DATA_SYNC_STATE.INTERRUPTED ||
                                    "SYNCING".equals(OTHER_INDEXES_SYNC_STATE_MAP.computeIfAbsent(clusterIndex, v -> new HashMap<>()).getOrDefault(LOCAL_CLUSTER_INDEX, "SYNCING")) ||
                                    "INTERRUPTED".equals(OTHER_INDEXES_SYNC_STATE_MAP.get(clusterIndex).getOrDefault(LOCAL_CLUSTER_INDEX, "SYNCING")) ||
                                    "SYNCING".equals(OTHER_INDEXES_SYNC_STATE_MAP.get(clusterIndex).getOrDefault(index.get(), "SYNCING")) ||
                                    "INTERRUPTED".equals(OTHER_INDEXES_SYNC_STATE_MAP.get(clusterIndex).getOrDefault(index.get(), "SYNCING"))) {
                                return Mono.just(-1);
                            }
                        }
                    } else {
                        if (currClusterSyncMap.get(index.get()) == DATA_SYNC_STATE.SYNCING || currClusterSyncMap.get(index.get()) == DATA_SYNC_STATE.INTERRUPTED
                                || "SYNCING".equals(OTHER_INDEXES_SYNC_STATE_MAP.computeIfAbsent(index.get(), v -> new HashMap<>())
                                .getOrDefault(LOCAL_CLUSTER_INDEX, "SYNCING")) ||
                                "INTERRUPTED".equals(OTHER_INDEXES_SYNC_STATE_MAP.get(index.get()).getOrDefault(LOCAL_CLUSTER_INDEX, "SYNCING"))) {
                            return Mono.just(-1);
                        }
                    }
                    return Mono.just(index.get());

                })
                .filter(index -> index != -1)
                .filter(index -> IS_OBJ_NUM_CHECKING.compareAndSet(false, true))
                .flatMap(index -> ScanStream.scan(pool.getReactive(REDIS_BUCKETINFO_INDEX))
                        .filter(bucketName -> BUCKET_NAME_PATTERN.matcher(bucketName).matches())
                        .flatMap(bucketName -> DoubleActiveUtil.checkBucObjNumIsEnable(bucketName).filter(b -> b).map(b -> bucketName))
                        .flatMap(bucketName -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hexists(bucketName, "address").filter(b -> !b).map(b -> bucketName))
                        .collectList().zipWith(Mono.just(index)))
                .doOnNext(tuple2 -> {
                    if (tuple2.getT1().isEmpty()) {
                        IS_OBJ_NUM_CHECKING.set(false);
                    } else {
                        objNumChecking(tuple2.getT1(), tuple2.getT2());
                    }
                })
                .doOnError(e -> {
                    IS_OBJ_NUM_CHECKING.set(false);
                    log.error("compare obj num with master cluster error", e);
                })
                .subscribe(b -> IS_OBJ_NUM_CHECKING.set(false));
    }

    private static void objNumChecking(List<String> bucketList, int clusterIndex) {
        UnicastProcessor<Integer> bucketIndexSignal = UnicastProcessor.create();
        bucketIndexSignal
                .doOnComplete(() -> IS_OBJ_NUM_CHECKING.set(false))
                .subscribe(index -> {
                    if (index >= bucketList.size()) {
                        bucketIndexSignal.onComplete();
                        return;
                    }
                    String bucketName = bucketList.get(index);
                    pool.getReactive(REDIS_BUCKETINFO_INDEX)
                            .hget(bucketName, BUCKET_USER_ID).defaultIfEmpty("")
                            .filter(StringUtils::isNotEmpty)
                            .flatMap(userId -> pool.getReactive(REDIS_USERINFO_INDEX).hgetall(userId))
                            .flatMap(idMap -> {
                                String ak1 = idMap.get(USER_DATABASE_ID_AK1);
                                String ak = StringUtils.isEmpty(ak1) || "null".equals(ak1) ? idMap.get(USER_DATABASE_ID_AK2) : ak1;
                                if (StringUtils.isEmpty(ak) || "null".equals(ak)) {
                                    return Mono.just("");
                                } else {
                                    return Mono.just(ak);
                                }
                            })
                            .filter(StringUtils::isNotEmpty)
                            .zipWith(pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, CLUSTER_NAME))
                            .flatMap(tup2 -> Mono.just(tup2.getT1()).zipWith(getBucketSyncCheck(bucketName, tup2.getT2(), clusterIndex)))
                            .filter(Tuple2::getT2)
                            .map(Tuple2::getT1)
                            .flatMap(ak -> getBucketObjNum(clusterIndex, ak, bucketName).zipWith(getBucketCap(clusterIndex, ak, bucketName)))
                            .filter(tuple2 -> StringUtils.isNotEmpty(tuple2.getT1()) && StringUtils.isNotEmpty(tuple2.getT2()))
                            .flatMap(tuple2 -> ErasureClient.reduceBucketInfo(bucketName).zipWith(pool.getReactive(REDIS_SYSINFO_INDEX)
                                    .sismember(NEED_SYNC_BUCKETS, bucketName).defaultIfEmpty(false))
                                    .flatMap(tuple21 -> {
                                        BucketInfo bucketInfo = tuple21.getT1();
                                        if (BucketInfo.ERROR_BUCKET_INFO.equals(bucketInfo)) {
                                            return Mono.just(false);
                                        }
                                        String masterObjNum = tuple2.getT1();
                                        String masterBucketCap = tuple2.getT2();
                                        String objNum = bucketInfo.getObjectNum();
                                        String bucketCap = bucketInfo.getBucketStorage();
                                        if (!tuple21.getT2() && (IS_ASYNC_CLUSTER || SYNCED.equals(SYNC_BUCKET_STATE_MAP.get(clusterIndex).getOrDefault(bucketName, SYNCED))) &&
                                                (!objNum.equals(masterObjNum) || !bucketCap.equals(masterBucketCap))) {
                                            log.info("{} cap is diff from {} {} {} {} {}", bucketName, clusterIndex,
                                                    objNum, masterObjNum, bucketCap, masterBucketCap);
                                            String num = StringUtils.isEmpty(masterObjNum) ? "0"
                                                    : String.valueOf(Long.parseLong(masterObjNum) - Long.parseLong(objNum));
                                            String cap = StringUtils.isEmpty(masterBucketCap) ? "0"
                                                    : String.valueOf(Long.parseLong(masterBucketCap) - Long.parseLong(bucketCap));
                                            StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucketName);
                                            return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(bucketName))
                                                    .flatMap(nodeList -> ErasureClient.updateBucketCapacity(bucketName, num, cap, nodeList));
                                        } else {
                                            log.debug("{} cap is same with {} {} {} {} {}", bucketName, clusterIndex,
                                                    objNum, masterObjNum, bucketCap, masterBucketCap);
                                        }
                                        return Mono.just(true);
                                    }))
                            .doFinally(b -> bucketIndexSignal.onNext(index + 1))
                            .doOnError(e -> log.error("compare cap with cluster {} error", clusterIndex, e))
                            .subscribe();
                });
        bucketIndexSignal.onNext(0);
    }

    private static Mono<Boolean> getBucketSyncCheck(String bucketName, String siteName, Integer masterIndex) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        Integer clusterIndex = null;
        for (Integer index : INDEX_NAME_MAP.keySet()) {
            if (INDEX_NAME_MAP.get(index).equals(siteName)) {
                clusterIndex = index;
            }
        }
        if (clusterIndex != null) {
            if (clusterIndex.equals(LOCAL_CLUSTER_INDEX)) {
                pool.getReactive(REDIS_SYSINFO_INDEX).sismember(NEED_SYNC_BUCKETS, bucketName)
                        .defaultIfEmpty(false)
                        .subscribe(b -> {
                            boolean checkSync = b;
                            if (!b) {
                                checkSync = checkBucSyncState(bucketName, masterIndex);
                            }
                            res.onNext(!checkSync);
                        }, e -> {
                            log.error("check bucket {} sync fail from local cluster, error", bucketName, e);
                            res.onNext(false);
                        });
            } else {
                PoolingRequest poolingRequest = new PoolingRequest(HttpMethod.GET, clusterIndex, "?checkBucketSync", getClient());
                poolingRequest.setAuthString(INNER_AUTH);
                Integer finalClusterIndex = clusterIndex;
                Disposable subscribe = poolingRequest.request()
                        .subscribe(request -> {
                            request.putHeader(IS_SYNCING, "1");
                            request.putHeader("bucketName", bucketName);
                            request.putHeader("index", LOCAL_CLUSTER_INDEX.toString());
                            request.exceptionHandler(e -> {
                                res.onNext(false);
                                log.error("check bucket {} sync, {}, {}", bucketName, e.getClass().getTypeName(), e.getMessage());
                            }).handler(resp -> {
                                if (SUCCESS == resp.statusCode()) {
                                    String checkSync = resp.getHeader("checkSync");
                                    if ("1".equals(checkSync)) {
                                        log.debug("check error: bucket {} no sync from cluster {}", bucketName, finalClusterIndex);
                                        res.onNext(false);
                                    } else {
                                        res.onNext(true);
                                    }
                                } else {
                                    log.debug("check error: bucket {} no sync from cluster {}", bucketName, finalClusterIndex);
                                    res.onNext(false);
                                }
                            });
                            request.end();
                        }, e -> {
                            log.error("check bucket {} sync fail from cluster {}, error", bucketName, finalClusterIndex, e);
                            res.onNext(false);
                        });
            }
        }
        return res;
    }

    private static Mono<String> getBucketObjNum(int clusterIndex, String ak, String bucketName) {
        MonoProcessor<String> res = MonoProcessor.create();
        UnSynchronizedRecord record = new UnSynchronizedRecord().setHeaders(new HashMap<>());
        record.headers.put(AUTHORIZATION, "AWS " + ak + ":0000");
        record.setIndex(clusterIndex);
        String uri = UrlEncoder.encode(File.separator + bucketName, "UTF-8") + "?objectsInfo";
        PoolingRequest poolingRequest = new PoolingRequest(HttpMethod.GET, clusterIndex, uri, getClient());
        poolingRequest.setAuthString(INNER_AUTH);
        Disposable subscribe = poolingRequest.request(record)
                .subscribe(request -> {
                    request.setTimeout(HEARTBEAT_INTERVAL_SECONDS * 1000);
                    request.putHeader(IS_SYNCING, "1");
                    request.exceptionHandler(e -> {
                        res.onNext("");
                        log.error("get {} cap request error, {}, {}", bucketName, e.getClass().getTypeName(), e.getMessage());
                    }).handler(resp -> {
                        if (SUCCESS != resp.statusCode()) {
                            res.onNext("");
                        } else {
                            resp.bodyHandler(body -> {
                                BucketUsedObjectsResult objectsResult;
                                if (StringUtils.isNotEmpty(new String(body.getBytes()))) {
                                    objectsResult = (BucketUsedObjectsResult) JaxbUtils.toObject(new String(body.getBytes()));
                                    if (objectsResult != null) {
                                        res.onNext(objectsResult.getUsedObjects());
                                    } else {
                                        res.onNext("");
                                    }
                                } else {
                                    res.onNext("");
                                }
                            });
                        }
                    });
                    request.end();
                }, e -> {
                    log.error("get {} obj num from {} error", bucketName, clusterIndex, e);
                    res.onNext("");
                });
        return res;
    }

    private static Mono<String> getBucketCap(int clusterIndex, String ak, String bucketName) {
        MonoProcessor<String> res = MonoProcessor.create();
        UnSynchronizedRecord record = new UnSynchronizedRecord().setHeaders(new HashMap<>());
        record.setIndex(clusterIndex);
        record.headers.put(AUTHORIZATION, "AWS " + ak + ":0000");
        String uri = UrlEncoder.encode(File.separator + bucketName, "UTF-8") + "?capinfo";
        PoolingRequest poolingRequest = new PoolingRequest(HttpMethod.GET, clusterIndex, uri, getClient());
        poolingRequest.setAuthString(INNER_AUTH);
        Disposable subscribe = poolingRequest.request(record)
                .subscribe(request -> {
                    request.setTimeout(HEARTBEAT_INTERVAL_SECONDS * 1000);
                    request.putHeader(IS_SYNCING, "1");
                    request.exceptionHandler(e -> {
                        res.onNext("");
                        log.error("get {} cap request error, {}, {}", bucketName, e.getClass().getTypeName(), e.getMessage());
                    }).handler(resp -> {
                        if (SUCCESS != resp.statusCode()) {
                            res.onNext("");
                        } else {
                            resp.bodyHandler(body -> {
                                BucketCapInfoResult bucketCapResult;
                                if (StringUtils.isNotEmpty(new String(body.getBytes()))) {
                                    bucketCapResult = (BucketCapInfoResult) JaxbUtils.toObject(new String(body.getBytes()));
                                    if (bucketCapResult != null) {
                                        res.onNext(bucketCapResult.getUsedCapacity());
                                    } else {
                                        res.onNext("");
                                    }
                                } else {
                                    res.onNext("");
                                }
                            });
                        }
                    });
                    request.end();
                }, e -> {
                    log.error("get {} cap info from {} error", bucketName, clusterIndex, e);
                    res.onNext("");
                });
        return res;
    }

    /**
     * 心跳检测通过，如果该站点当前状态为心跳异常，清空异常计数，更改站点状态并写redis
     */
    private static void successHandler(int clusterIndex) {
        Integer formerState = INDEX_STATUS_MAP.put(clusterIndex, 1);
        // 站点心跳正常且和半数连通，且当前环境状态为不可操作，且非复制站点，则可以重新check主站点信息
        // 如果本地站点出现了心跳请求全部失败的情况，需要在恢复时checkMasterInfo
        if ((!isIsolated() && isEvaluatingMaster.get() && !IS_ASYNC_CLUSTER)
                || (INDEX_REQ_FAIL_COUNT_MAP.get(clusterIndex).get() && clusterIndex == LOCAL_CLUSTER_INDEX)) {
            // 可能存在主站点察觉自身心跳异常前就恢复，期间已经选出新主的情况。此时走不到这里，即不会更新为新主。
            log.info("cluster {} is resume. start checkMasterInfo", clusterIndex);
            ArbitratorUtils.checkMasterInfo(TERM.get(), MASTER_INDEX, v -> MainNodeSelector.checkIfSyncNode());
        }

        if (INDEX_STATUS_COUNT_MAP.get(clusterIndex).get() > 0) {
            INDEX_STATUS_COUNT_MAP.put(clusterIndex, new AtomicLong());
        }
        INDEX_REQ_FAIL_COUNT_MAP.get(clusterIndex).compareAndSet(true, false);

        final Integer[] stateInMem = {1};
        // 有1个非DA站点状态不正常，就不设置link_state。
        INDEX_STATUS_MAP.forEach((i, s) -> {
            if (s == 0 && (ASYNC_INDEX_IPS_ENTIRE_MAP.isEmpty() || !ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(i))) {
                stateInMem[0] = 0;
            }
        });

        checkMipInSuccess(clusterIndex);

        getStateInRedis(clusterIndex, "1")
                .publishOn(SCAN_SCHEDULER)
                .map(state -> {
                    if (ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex)) {
                        // 复制站点的状态改变不需要考虑双活站点是否有链路异常，只考虑当前状态是否需要改成1
                        return Integer.parseInt(state) == 0;
                    } else {
                        // redis中state为1不需要再改成1；内存中的所有站点状态有不为1的，也不改redis中的state为1。
                        return Integer.parseInt(state) == 0;
                    }
                })
                .filter(b -> b)
                .doOnNext(b -> {
                    // syncDataCheck会一直运行，无需在心跳恢复的时候再运行，可能出现重复扫描同一bucket
//            DataSynChecker.syncDataCheck();
                    log.info("cluster {} is resume. ", clusterIndex);
                    setLinkState(clusterIndex, stateInMem[0]);
                    siteLinkStateChange();
                })
                .subscribe();
    }

    /**
     * 心跳检测失败。异常次数+1。当异常次数超过阈值且该站点当前状态为心跳正常，更改站点状态并写redis
     */
    private static void failHandler(int clusterIndex) {
        getEth12State()
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(b -> {
                    if (!b) {
                        INDEX_STATUS_COUNT_MAP.put(clusterIndex, new AtomicLong());
//                        INDEX_STATUS_MAP.put(clusterIndex, 1);
                    }
                })
                .filter(b -> b)
                .flatMap(status -> Mono.just(INDEX_STATUS_COUNT_MAP.get(clusterIndex).incrementAndGet()))
                .filter(failCount -> failCount >= HEARTBEAT_INTERVAL_TIMES)
                .flatMap(b -> {
                    log.info("cluster {} is down. ", clusterIndex);
                    INDEX_STATUS_MAP.put(clusterIndex, 0);

                    // 复制站点不需要这一步
                    if (DAVersionUtils.isStrictConsis()
                            && isIsolated()
                            && !ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(LOCAL_CLUSTER_INDEX)) {
                        ArbitratorUtils.updateEvaluatingMasterStatus(1);
                    }

                    // 主站点心跳异常，从站点在能与其他站点连通的情况下发起选举。
                    // 复制站点不会发起选举。
                    if (DAVersionUtils.isStrictConsis()
                            && clusterIndex == MASTER_INDEX
                            && !LOCAL_CLUSTER_INDEX.equals(MASTER_INDEX)
                            && !isIsolated()
                            && !ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(LOCAL_CLUSTER_INDEX)) {
                        Arbitrator.getInstance().runElection(TERM.get());
                    }
                    DoubleActiveUtil.checkMipInFail(clusterIndex);
                    return getStateInRedis(clusterIndex, "0");
                })
                .filter(state -> !"0".equals(state))
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(state -> {
                    setLinkState(clusterIndex, "0");
                    siteLinkStateChange();
                })
                .subscribe();
    }

    /**
     * 本站点心跳检测不通过，或除本站点外没有一个站点正常，返回true。
     * 本站点心跳检测通过且至少有一个其他站点心跳正常，返回false。
     */
    public static boolean isInterrupt() {
        if (INDEX_STATUS_MAP.get(LOCAL_CLUSTER_INDEX) == 0) {
            return true;
        }
        long normalNum = INDEX_STATUS_MAP.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(LOCAL_CLUSTER_INDEX))
                .filter(entry -> !entry.getValue().equals(0)).count();
        return normalNum == 0;
    }

    /**
     * 自身心跳不正常且和仲裁者不连通则认为本站点不正常，返回true;
     * <p>
     * 双活心跳正常的站点数量（包括自身） + 仲裁者联通的数量（1）小于半数，则认为该站点不正常，返回true
     */
    public static boolean isIsolated() {
        if (!heartBeatIsNormal(LOCAL_CLUSTER_INDEX) && !abtAdmit.get()) {
            return true;
        }

        // 主站点只要和仲裁者能通信就返false
        boolean localEth4IsNormal = AVAIL_BACKEND_IP_ENTIRE_SET.contains(LOCAL_BACKEND_IP);
        if (DAVersionUtils.isMasterCluster.get() && abtAdmit.get() && localEth4IsNormal) {
            return false;
        }

        long normalNum = INDEX_STATUS_MAP.entrySet().stream()
                .filter(entry -> DA_INDEX_IPS_ENTIRE_MAP.containsKey(entry.getKey()))
                .filter(entry -> !entry.getValue().equals(0)).count();

        if (abtAdmit.get() && localEth4IsNormal) {
            normalNum += 1;
        }
        return normalNum <= (NODE_AMOUNT + 1) / 2;
    }

    public static boolean asyncIsInterrupt(int asyncClusterIndex) {
        return INDEX_STATUS_MAP.get(asyncClusterIndex) == 0;
    }

    /**
     * 使用了复制网卡，且本节点eth12正常，表示心跳检测结果正确。
     * 本节点eth12异常但其他节点的eth12有正常的，则需要其他节点进行心跳检测，本次心跳检测结果作废。
     *
     * @return true表示本地eth12正常或者所有eth12都不正常，可以确认心跳有问题，需要进一步处理。false表示有其他节点eth12正常，本节点不处理，由下一个扫描节点处理
     */
    private static Mono<Boolean> getEth12State() {
        if (USE_ETH4) {
            return Mono.just(true);
        }
        return Mono.just("eth12".equals(SYNC_ETH_NAME))
                .flatMap(eth12 -> {
                    if (eth12) {
                        return pool.getReactive(REDIS_NODEINFO_INDEX).hget(UUID, ACTIVE_SYNC_IP_STATE)
                                .flatMap(status -> {
                                    if ("up".equals(status)) {
                                        return Mono.just(true);
                                    }
                                    return ScanStream.scan(pool.getReactive(REDIS_NODEINFO_INDEX), new ScanArgs().match("*"))
                                            .flatMap(uuid -> pool.getReactive(REDIS_NODEINFO_INDEX).hget(uuid, ACTIVE_SYNC_IP_STATE))
                                            .collectList()
                                            .flatMap(list -> {
                                                if (list.contains("up")) {
                                                    return Mono.just(false);
                                                }
                                                return Mono.just(true);
                                            });
                                });
                    } else {
                        return pool.getReactive(REDIS_SYSINFO_INDEX).hget(SYNC_ETH_NAME + "_status", UUID)
                                .flatMap(status -> {
                                    if (status.startsWith("1")) {
                                        return Mono.just(true);
                                    }
                                    return ScanStream.scan(pool.getReactive(REDIS_NODEINFO_INDEX), new ScanArgs().match("*"))
                                            .flatMap(uuid -> pool.getReactive(REDIS_SYSINFO_INDEX).hget(SYNC_ETH_NAME + "_status", uuid)
                                                    .map(s -> s.startsWith("1")))
                                            .collectList()
                                            .flatMap(list -> {
                                                if (list.contains(true)) {
                                                    return Mono.just(false);
                                                }
                                                return Mono.just(true);
                                            });
                                });
                    }
                });
    }

    public static void siteStateChange() {
        if (!ASYNC_INDEX_IPS_ENTIRE_MAP.isEmpty() && ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(LOCAL_CLUSTER_INDEX)) {
            return;//第三站点无需心跳检测
        }
        SocketReqMsg dmSocket = new SocketReqMsg("arbitratorChange", 0);
        sender.sendAndGetResponse(dmSocket, StringResMsg.class, true);
        log.info("send msg to ms_cloud change arbitrator state !!!");
    }

    private static void siteLinkStateChange() {
        if (!ASYNC_INDEX_IPS_ENTIRE_MAP.isEmpty() && ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(LOCAL_CLUSTER_INDEX)) {
            return;//第三站点无需心跳检测
        }
        SocketReqMsg dmSocket = new SocketReqMsg("siteLinkStateChange", 0);
        sender.sendAndGetResponse(dmSocket, StringResMsg.class, true);
        log.info("send msg to ms_cloud change site link state !!!");
    }

    static void siteSyncStateChange() {
        if (!ASYNC_INDEX_IPS_ENTIRE_MAP.isEmpty() && ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(LOCAL_CLUSTER_INDEX)) {
            return;//第三站点无需心跳检测
        }
        SocketReqMsg dmSocket = new SocketReqMsg("siteSyncStateChange", 0);
        sender.sendAndGetResponse(dmSocket, StringResMsg.class, true);
        log.info("send msg to ms_cloud change site sync state !!!");
    }

    public static void siteRoleStateChange() {
        SocketReqMsg dmSocket = new SocketReqMsg("siteRoleStateChange", 0);
        sender.sendAndGetResponse(dmSocket, StringResMsg.class, true);
        log.info("send msg to ms_cloud change site role !!!");
    }

    /**
     * 主节点和每个节点双活网络是否能连通
     */
    private static void pingEveryNode() {
        IP_INDEX_MAP.forEach((ip, index) -> {
            try {
                boolean reachable = InetAddress.getByName(ip).isReachable(1_000);
                int nodeState = reachable ? 1 : 0;
                if (nodeState != IP_STATUS_MAP.get(ip)) {
                    IP_STATUS_MAP.put(ip, nodeState);
                }
            } catch (Exception e) {
                log.error("", e);
            }
        });
    }

    private static final String AVAIL_IP_LIST = "avail_ip_list";

    /**
     * 更新可访问节点。INDEX_IPS_MAP动态改变
     */
    private static void refreshNodeStatus() {
        INDEX_IPS_ENTIRE_MAP.forEach((clusterIndex, ips) -> {
            List<String> ipList = new ArrayList<>();
            for (String ip : ips) {
                //节点所在站点心跳正常，节点后端网络无异常、和本节点双活链路可以连通的节点视为可访问节点。
//                log.info("heart-{}, back-{}, front-{}", heartBeatIsNormal(clusterIndex), IP_STATUS_BACKEND_MAP.get(ip) == 1
//                        , IP_STATUS_MAP.get(ip) == 1);
                if (heartBeatIsNormal(clusterIndex) && IP_STATUS_BACKEND_MAP.get(ip) == 1
                        && IP_STATUS_MAP.get(ip) == 1) {
                    ipList.add(ip);
                }
            }
            //如果某一个站点不可用（存储池异常、网络不通），则保留头一个ip（防止发起双活请求时拿不到ip报空指针），用来写入record，触发异常修复。
            if (ipList.size() == 0) {
                ipList.add(INDEX_IPS_ENTIRE_MAP.get(clusterIndex)[0]);
            }
            INDEX_IPS_MAP.put(clusterIndex, ipList.toArray(new String[0]));
            if (DA_INDEX_IPS_MAP.containsKey(clusterIndex)) {
                DA_INDEX_IPS_MAP.put(clusterIndex, ipList.toArray(new String[0]));
            }
        });

        Mono.just("1").publishOn(SCAN_SCHEDULER)
                .subscribe(s -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, AVAIL_IP_LIST, Json.encode(INDEX_IPS_MAP)));
    }

    /**
     * 从redis读取可用ip及本站点的link_state状态
     */
    private static void getAvailIpList() {
        pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(LOCAL_CLUSTER)
                .subscribe(map -> {
                    Map<Integer, String[]> availIpListMap = Json.decodeValue(map.get(AVAIL_IP_LIST), new TypeReference<Map<Integer, String[]>>() {
                    });
                    INDEX_IPS_ENTIRE_MAP.keySet().forEach(clusterIndex -> {
                        if (DA_INDEX_IPS_MAP.containsKey(clusterIndex)) {
                            DA_INDEX_IPS_MAP.put(clusterIndex, availIpListMap.get(clusterIndex));
                        }
                    });
                    INDEX_IPS_MAP.putAll(availIpListMap);
                });

    }

    /**
     * 根据索引判断站点心跳是否正常。
     * 因为是主节点下刷的，从节点会有一定延迟。
     */
    public static boolean heartBeatIsNormal(int clusterIndex) {
//        return hasHeartBeatStarted ?
//                INDEX_STATUS_MAP.getOrDefault(clusterIndex, 1) == 1 :
//                INDEX_IPS_MAP.get(clusterIndex).length >= (INDEX_SNUM_MAP.get(clusterIndex) / 2 + 1);
        return INDEX_STATUS_MAP.getOrDefault(clusterIndex, 1) == 1;
    }

    private static Mono<String> getStateInRedis() {
        return pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, LINK_STATE)
                .switchIfEmpty(Mono.just("1"));
    }

    private static Mono<String> getStateInRedis(int clusterIndex, String expect) {
        return pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTERS_STATUS)
                .switchIfEmpty(Mono.just(Json.encode(new HashMap<>())))
                .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, LINK_STATE).switchIfEmpty(Mono.just("1")))
                .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, EXTRA_LINK_STATE).switchIfEmpty(Mono.just("1")))
                .flatMap(tuple2 -> {
                    String map = tuple2.getT1().getT1();
                    String linkState = tuple2.getT1().getT2();
                    String extraLinkState = tuple2.getT2();

                    if (map.isEmpty()) {
                        Mono.just(1).publishOn(SCAN_SCHEDULER)
                                .subscribe(s -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, CLUSTERS_STATUS, Json.encode(INDEX_STATUS_MAP)));
                        return Mono.just(linkState);
                    }
                    Map<Integer, String> clusterStatusMap = Json.decodeValue(map, new TypeReference<Map<Integer, String>>() {
                    });
                    if (clusterStatusMap.size() != INDEX_IPS_ENTIRE_MAP.size()) {
                        Mono.just(1).publishOn(SCAN_SCHEDULER)
                                .subscribe(s -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, CLUSTERS_STATUS, Json.encode(INDEX_STATUS_MAP)));
                    }

                    // 3DC的双活站点如果cluster_status里的状态和link_state不同，需要更新统一。
//                        if (!ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex)) {
//                            if (!clusterStatusMap.getOrDefault(clusterIndex, "1").equals(linkState)) {
//                                return Mono.just("0");
//                            }
//                        }
                    // 如果是successHandler，需要保证map和link_state均为1，其中任一为0则返回0；
                    // 如果是failHandler，需要保证map和link_state均为0，其中任一为1则返回1；
                    String clusterStatus = clusterStatusMap.getOrDefault(clusterIndex, "1");
                    String res = clusterStatus;
                    // async站点的连接状态变动不会修改link_state，而是extra_link_state
                    if (ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex)) {
                        linkState = extraLinkState;
                    }
                    if (expect.equals(clusterStatus) && expect.equals(linkState)) {
                        res = expect;
                    } else {
                        res = expect == "1" ? "0" : "1";
                    }
                    // 如果返回的res和expect的值相同则表示无需进行后续的修改redis里的状态
                    return Mono.just(res);
                });
    }

    private static void setLinkState(int clusterIndex, String state) {
        try {
            synchronized (INDEX_STATUS_MAP) {
                log.info("set link state, {}, {}, {}", clusterIndex, state, INDEX_STATUS_MAP);
                if (!ASYNC_INDEX_IPS_ENTIRE_MAP.isEmpty() && ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex)) {
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, EXTRA_LINK_STATE, state);
                } else {
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, LINK_STATE, state);
                }
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, CLUSTERS_STATUS, Json.encode(INDEX_STATUS_MAP));
            }
        } catch (Exception e) {
            log.error("set link state error", e);
        }
    }

    private static void setLinkState(int clusterIndex, Integer state) {
        try {
            synchronized (INDEX_STATUS_MAP) {
                log.info("successHandler set link state, {}, {}, {}", clusterIndex, state, INDEX_STATUS_MAP);
                if (state == 1) {
                    if (!ASYNC_INDEX_IPS_ENTIRE_MAP.isEmpty() && ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex)) {
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, EXTRA_LINK_STATE, state + "");
                    } else {
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, LINK_STATE, state + "");
                    }
                }
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, CLUSTERS_STATUS, Json.encode(INDEX_STATUS_MAP));
            }
        } catch (Exception e) {
            log.error("set link state error", e);
        }
    }

    /**
     * 每次启动双活服务时重置内存和redis中的可用ip
     */
    private static void resetAvailIps() {
        INDEX_IPS_MAP.putAll(INDEX_IPS_ENTIRE_MAP);
        Mono.just("1").publishOn(SCAN_SCHEDULER)
                .subscribe(s -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, AVAIL_IP_LIST, Json.encode(INDEX_IPS_ENTIRE_MAP)));
    }

    /**
     * 主节点定期下刷各站点心跳状态
     */
    private static void flushClustersStatus() {
        Mono.just("1").publishOn(SCAN_SCHEDULER)
                .subscribe(s -> {
//                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, CLUSTERS_STATUS, Json.encode(INDEX_STATUS_MAP));
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, OTHER_CLUSTERS_STATUS, Json.encode(OTHER_INDEXES_STATUS_MAP));
                });
    }

    /**
     * 从节点定时获取每个站点的心跳状态
     */
    private static void getClustersStatus() {
        pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTERS_STATUS)
                .subscribe(map -> {
                    if (map == null) {
                        return;
                    }
                    Map<Integer, Integer> clusterStatusMap = Json.decodeValue(map, new TypeReference<Map<Integer, Integer>>() {
                    });
                    INDEX_STATUS_MAP.putAll(clusterStatusMap);
                });
    }

    /**
     * 从节点定时获取其他站点记录的每个站点的心跳状态
     */
    private static void getOtherClustersStatus() {
        pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, OTHER_CLUSTERS_STATUS)
                .subscribe(map -> {
                    if (map == null) {
                        return;
                    }
                    Map<Integer, Map<Integer, Integer>> clusterStatusMap = Json.decodeValue(map, new TypeReference<Map<Integer, Map<Integer, Integer>>>() {
                    });
                    OTHER_INDEXES_STATUS_MAP.putAll(clusterStatusMap);
                });
    }

    /**
     * 3DC下AC的差异数据，当前站点A与C不通时，判断BC的链路状态，true则将差异记录发给B，由B同步给C
     */
    public static boolean recordTransfer(int index) {
        if (ASYNC_INDEX_IPS_ENTIRE_MAP.isEmpty() || !ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(index)) {
            return false;
        }
        int otherIndex = getOtherSiteIndex();
        return INDEX_STATUS_MAP.get(index) != 1 && INDEX_STATUS_MAP.get(otherIndex) == 1
                && OTHER_INDEXES_STATUS_MAP.containsKey(otherIndex) && OTHER_INDEXES_STATUS_MAP.get(otherIndex).get(index) == 1;
    }

    public static boolean otherIsInterrupt(int index) {
        if (ASYNC_INDEX_IPS_ENTIRE_MAP.isEmpty() || !ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(index)) {
            return true;
        }
        int otherIndex = getOtherSiteIndex();
        if (!OTHER_INDEXES_STATUS_MAP.containsKey(otherIndex)) {
            return true;
        }
        return OTHER_INDEXES_STATUS_MAP.get(otherIndex).get(index) == 0;
    }

    /**
     * 存放除本站点外可用的站点索引。
     */
    private static Integer[] otherAvailClusters;

    private void getAvailClusters() {
        List<Integer> indexList = new ArrayList<>();
        INDEX_STATUS_MAP.forEach((index, status) -> {
            if (status == 1 && !index.equals(LOCAL_CLUSTER_INDEX) && !ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(index)) {
                indexList.add(index);
            }
        });
        otherAvailClusters = indexList.toArray(new Integer[0]);
    }

    public static int getAnAvailClusterIndex() {
        int length = otherAvailClusters.length;
        if (length == 0) {
            return -1;
        } else {
            int i = ThreadLocalRandom.current().nextInt(length);
            return otherAvailClusters[i];
        }
    }


    private static final Map<String, AtomicLong> ETH4_PINGCOUNT_MAP = new ConcurrentHashMap<>();

    public static final ConcurrentHashSet<String> AVAIL_UUID_MAP = new ConcurrentHashSet<>();

    private static void pingLocalBackendIp() {
        pingLocalBackendIp(UUID_BACKEND_IP_MAP);
    }

    private static void pingLocalBackendIp(Map<String, String> uuidEth4Map) {
        for (Map.Entry<String, String> entry : uuidEth4Map.entrySet()) {
            try {
                String uuid = entry.getKey();
                String eth4Ip = entry.getValue();
                AtomicLong pingCount = ETH4_PINGCOUNT_MAP.computeIfAbsent(eth4Ip, k -> new AtomicLong(3));
                HttpClientRequest request = getHttpClient().request(HttpMethod.GET, DA_HTTP_PORT, eth4Ip, "?getHeartBeat");
                request.setTimeout(1000)
                        .setHost(eth4Ip + ":" + DA_HTTP_PORT)
                        .putHeader(SYNC_AUTH, PASSWORD)
                        .putHeader("cluster_index", "checkNode")
                        // 不需要知道差异记录同步状态
                        .exceptionHandler(e -> {
                            log.error("eth4 check is unavailable, node:{}", eth4Ip, e);
                            if (pingCount.incrementAndGet() >= 3) {
                                AVAIL_BACKEND_IP_ENTIRE_SET.remove(eth4Ip);
                                AVAIL_UUID_MAP.remove(uuid);
                            }
                        })
                        .handler(resp -> {
                            if (500 == resp.statusCode()) {
                                if (pingCount.incrementAndGet() >= 3) {
                                    AVAIL_BACKEND_IP_ENTIRE_SET.remove(eth4Ip);
                                    AVAIL_UUID_MAP.remove(uuid);
                                }
                                return;
                            }
                            boolean serviceAvail = true;
                            if (StringUtils.isNotBlank(resp.getHeader("service_avail"))) {
                                serviceAvail = Boolean.parseBoolean(resp.getHeader("service_avail"));
                            }
                            Mono.just(serviceAvail)
                                    .publishOn(SCAN_SCHEDULER)
                                    .subscribe(b -> {
                                        if (b) {
                                            // 本地的请求还要确认自身的网口状态，因为拔了网线依然可以和自己的网口通信
                                            if (LOCAL_BACKEND_IP.equals(eth4Ip)) {
                                                com.macrosan.utils.functional.Tuple2<String, String> exec = SshClientUtils.exec("ip a|grep 'eth4:'", true, false);
                                                if (StringUtils.isNotBlank(exec.var2) || exec.var1.contains("DOWN")) {
                                                    if (pingCount.incrementAndGet() >= 3) {
                                                        AVAIL_BACKEND_IP_ENTIRE_SET.remove(eth4Ip);
                                                        AVAIL_UUID_MAP.remove(uuid);
                                                    }
                                                    return;
                                                }
                                            }
                                            pingCount.set(0);
                                            AVAIL_BACKEND_IP_ENTIRE_SET.add(eth4Ip);
                                            AVAIL_UUID_MAP.add(uuid);
                                        } else {
                                            if (pingCount.incrementAndGet() >= 3) {
                                                AVAIL_BACKEND_IP_ENTIRE_SET.remove(eth4Ip);
                                                AVAIL_UUID_MAP.remove(uuid);
                                            }
                                        }
                                        log.debug("AVAIL_BACKEND_IP_ENTIRE_SET: {} {}", AVAIL_BACKEND_IP_ENTIRE_SET, AVAIL_UUID_MAP);
                                    });
                        })
                        .end();
            } catch (Exception e) {
                log.error("", e);
            }
        }
    }

    /**
     * 设置eth4和eth12均正常的节点，用来转发双活请求
     */
    private void setRedirectableBackendIps() {
        for (Map.Entry<String, String> entry : SYNC_ETH4_IP_MAP.entrySet()) {
            String syncIp = entry.getKey();
            String eth4Ip = entry.getValue();
            if (Arrays.asList(INDEX_IPS_MAP.get(LOCAL_CLUSTER_INDEX)).contains(syncIp)
                    && AVAIL_BACKEND_IP_ENTIRE_SET.contains(eth4Ip)
                    && !eth4Ip.equals(ServerConfig.getInstance().getHeartIp1())) {
                REDIRECTABLE_BACKEND_IP_SET.add(eth4Ip);
            } else {
                REDIRECTABLE_BACKEND_IP_SET.remove(eth4Ip);
            }
        }
    }

    public static AtomicBoolean abtAdmit = new AtomicBoolean();

    /**
     * 发送请求确认仲裁者状态是否正常
     */
    private void checkAbtIpConnected() {
        if (!DAVersionUtils.isStrictConsis()) {
            return;
        }
        AtomicBoolean updateSpec = new AtomicBoolean(false);
        String oldSpecIp = pool.getCommand(REDIS_SYSINFO_INDEX).hget(ARBITRATOR_SPEC_IPS, UUID);
        String eth = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, ARBITRATOR_SPEC_ETH);
        String ethType = arbEthToType(eth);
        String specIp = pool.getCommand(REDIS_NODEINFO_INDEX).hget(UUID, ethType);

        HttpClientRequest request = HeartBeatChecker.getClient().request(HttpMethod.GET, getAbtPort(), ABT_IP, "?evaluateMasterIndex");
        if (StringUtils.isNotBlank(eth) && StringUtils.isNotBlank(specIp) && !specIp.equals(oldSpecIp)) {
            request.putHeader("oldSpecIp", StringUtils.isBlank(oldSpecIp) ? "" : oldSpecIp);
            request.putHeader("specIp", specIp);
            updateSpec.set(true);
        } else {
            request.putHeader("specIp", oldSpecIp);
        }
        request.setTimeout(3000)
                .setHost(ABT_IP + ":" + ABT_PORT)
                .putHeader(SYNC_AUTH, PASSWORD)
                .putHeader("index", String.valueOf(LOCAL_CLUSTER_INDEX))
                .exceptionHandler(e -> {
                    request.reset();
                    log.error("arbitrator conn error, ", e);
                    setAbtAdmitEach("0");
                    setAbtStatusEach("0");
                })
                .handler(resp -> {
                    boolean b = Boolean.parseBoolean(resp.getHeader(ArbitratorUtils.DA_STATUS_HEADER));
                    String daLink = resp.getHeader(DA_LINK_HEADER);
                    boolean f = StringUtils.isBlank(daLink) || daLink.equalsIgnoreCase("true");
                    if (resp.statusCode() == SUCCESS) {
                        if (updateSpec.get()) {
                            Mono.just(1).publishOn(SCAN_SCHEDULER)
                                    .doOnNext(s -> {
                                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(ARBITRATOR_SPEC_IPS, UUID, specIp);
                                    })
                                    .doOnError(e -> log.error("setSpecIp {} {} error, ", UUID, specIp, e))
                                    .subscribe();
                        }
                        if (b) {
                            setAbtAdmitEach("1");
                        } else {
                            setAbtAdmitEach("0");
                        }
                        if (f) {
                            setAbtStatusEach("1");
                        } else {
                            setAbtStatusEach("0");
                        }
                    } else {
                        log.error("checkAbtIpConnected return bad resp, {}, {}", resp.statusCode(), resp.statusMessage());
                        setAbtAdmitEach("0");
                        setAbtStatusEach("0");
                    }
                })
                .end();
    }

    private String arbEthToType(String syncEth) {
        String eth;
        if ("eth2".equals(syncEth)) {
            eth = BUSINESS_ETH1;
        } else if ("eth3".equals(syncEth)) {
            eth = BUSINESS_ETH2;
        } else if ("eth4".equals(syncEth)) {
            eth = HEART_ETH1;
        } else if ("eth0".equals(syncEth)) {
            eth = OUTER_MANAGER_IP;
        } else {
            eth = SYNC_IP;
        }
        return eth;
    }

    /**
     * 本地节点和仲裁间的状态是否正常。（可联通且仲裁判断本地节点正常）
     */
    private void setAbtAdmitEach(String status) {
        setAbtEach(ARBITRATOR_ADMIT_EACH, status);
    }

    /**
     * 是否可与仲裁通信
     */
    private void setAbtStatusEach(String status) {
        setAbtEach(ARBITRATOR_STATUS_EACH, status);
    }

    private void setAbtEach(String redisKey, String status) {
        pool.getReactive(REDIS_SYSINFO_INDEX).hget(redisKey, UUID).defaultIfEmpty("-1")
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(curStatus -> {
                    if (!status.equals(curStatus)) {
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(redisKey, UUID, status);
                        log.info("setAbt {} to {}", redisKey, status);
                    }
                })
                .doOnError(e -> log.error("setAbt {} error, ", redisKey, e))
                .subscribe();
    }

    /**
     * 第一个与仲裁者可通且eth4正常的节点。
     */
    public static volatile String FIRST_ABT_NORMAL_NODE = "";

    final AtomicBoolean isCheckAbtAdmit = new AtomicBoolean();

    /**
     * 获取按字典排序第一个与仲裁者admit状态正常且eth4正常的节点。(eth4不正常可能更改不了自己的arbitrator_status)
     * 如果自己就是这个节点，进行checkMasterInfo和更改站点与仲裁者admit状态.
     * 如果没有节点与仲裁者可通，由扫描节点完成上述工作
     */
    private void checkFirstAbtNormalNode() {
        if (!DAVersionUtils.isStrictConsis()) {
            return;
        }

        if (!isCheckAbtAdmit.compareAndSet(false, true)) {
            return;
        }
        pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(ARBITRATOR_ADMIT_EACH)
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(statusMap -> {
                    TreeMap<String, String> map = new TreeMap<>(statusMap);
                    boolean found = false;
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        String uuid = entry.getKey();
                        String s = entry.getValue();
                        if (s.equals("1") && AVAIL_BACKEND_IP_ENTIRE_SET.contains(UUID_BACKEND_IP_MAP.get(uuid))) {
                            FIRST_ABT_NORMAL_NODE = entry.getKey();
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        FIRST_ABT_NORMAL_NODE = "";
                    }
                })
                .filter(b -> isFirstAbtNormalNode())
                .flatMap(s -> pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, ARBITRATOR_ADMIT).defaultIfEmpty("-1"))
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(curAdmit -> {
                    String status = StringUtils.isBlank(FIRST_ABT_NORMAL_NODE) ? "0" : "1";

                    if (status.equals("1")) {
                        if (!isIsolated() && isEvaluatingMaster.get()
                                && !IS_ASYNC_CLUSTER) {
                            // 可能存在主站点察觉自身心跳异常前就恢复，期间已经选出新主的情况。此时走不到这里，即不会更新为新主。
                            log.info("checkAbtIpConnected is resume. start checkMasterInfo");
                            ArbitratorUtils.checkMasterInfo(TERM.get(), MASTER_INDEX, v -> isFirstAbtNormalNode());
                        }
                    }

                    if (!status.equals(curAdmit)) {
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, ARBITRATOR_ADMIT, status);
                    }
                })
                .timeout(Duration.ofSeconds(5))
                .doOnError(e -> log.error("checkFirstAbtNormalNode error, ", e))
                .doFinally(s -> isCheckAbtAdmit.set(false))
                .subscribe();
    }

    final AtomicBoolean isCheckAbtStatus = new AtomicBoolean();

    /**
     * 更新页面上的仲裁者是否可通的状态。
     */
    private void checkAbtStatus() {
        if (!DAVersionUtils.isStrictConsis()) {
            return;
        }

        if (!isFirstAbtNormalNode()) {
            return;
        }

        if (!isCheckAbtStatus.compareAndSet(false, true)) {
            return;
        }

        pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, ARBITRATOR_STATUS).defaultIfEmpty("-1")
                .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(ARBITRATOR_STATUS_EACH))
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(tuple2 -> {
                    Map<String, String> statusMap = tuple2.getT2();
                    String status = "0";
                    TreeMap<String, String> map = new TreeMap<>(statusMap);
                    // 有一个节点与仲裁可通则设置页面仲裁状态为可达
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        String s = entry.getValue();
                        if (s.equals("1")) {
                            status = "1";
                            break;
                        }
                    }

                    String curStatus = tuple2.getT1();
                    if (!status.equals(curStatus)) {
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, ARBITRATOR_STATUS, status);
                        log.info("setAbtStatusForSite to {}", status);
                        siteStateChange();
                    }
                })
                .timeout(Duration.ofSeconds(5))
                .doOnError(e -> log.error("checkAbtStatus error, ", e))
                .doFinally(s -> isCheckAbtStatus.compareAndSet(true, false))
                .subscribe();
    }

    public static boolean isFirstAbtNormalNode() {
        return StringUtils.isBlank(FIRST_ABT_NORMAL_NODE) ?
                MainNodeSelector.checkIfSyncNode() :
                FIRST_ABT_NORMAL_NODE.equals(UUID);
    }

    private static void dealDns() {
        pool.getReactive(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SYNC_POLICY)
                .filter("0"::equals)
                .flatMap(b -> pool.getReactive(REDIS_SYSINFO_INDEX).hexists(LOCAL_CLUSTER, ARBITRATOR_ADMIT))
                .filter(b -> b)
                .flatMap(b -> pool.getReactive(REDIS_SYSINFO_INDEX).get("masterDnsuuid").zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).get("slaveDnsuuid")))
                .flatMap(tuple -> Mono.just(UUID.equals(tuple.getT1())).zipWith(Mono.just(UUID.equals(tuple.getT2()))))
                .filter(tuple -> tuple.getT1() || tuple.getT2())
                // 存在dnsIp的节点才会往下走
                .map(Tuple2::getT1)
                .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hgetall("dns_ip"))
//                .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, ARBITRATOR_STATUS))
                .flatMap(tuple -> {
                    //var1是当前节点是否为主dns所在节点
                    if (!heartBeatIsNormal(LOCAL_CLUSTER_INDEX)) {
                        if (abtAdmit.get() && DAVersionUtils.isMasterCluster.get() && !isEvaluatingMaster.get()) {
                            // 主站点自己链路断开且和仲裁者通 dns升
                            return Mono.just(new Tuple3<>(tuple.getT1(), tuple.getT2(), "1"));
                        } else {
                            return Mono.just(new Tuple3<>(tuple.getT1(), tuple.getT2(), "0"));
                        }
                    } else if (!heartBeatIsNormal(getOtherSiteIndex())) {
                        if (!abtAdmit.get()) {
                            //自己正常，但与另一个站点及仲裁都断开 dns降
                            return Mono.just(new Tuple3<>(tuple.getT1(), tuple.getT2(), "0"));
                        } else {
                            if (DAVersionUtils.isMasterCluster.get() && !isEvaluatingMaster.get()) {
                                //升
                                return Mono.just(new Tuple3<>(tuple.getT1(), tuple.getT2(), "1"));
                            } else {
                                //自己正常但与另一个站点断开且自己是从站点 DNS降
                                return Mono.just(new Tuple3<>(tuple.getT1(), tuple.getT2(), "0"));
                            }
                        }
                    } else {
                        //升
                        return Mono.just(new Tuple3<>(tuple.getT1(), tuple.getT2(), "1"));
                    }
                })
                .flatMap(tuple3 -> {
                    String key = tuple3.var1 ? "master@" + UUID : "slave@" + UUID;

                    Map<String, String> dnsMap = tuple3.var2;
                    String stateNow = tuple3.var3;

                    return pool.getReactive(REDIS_SYSINFO_INDEX).hget("clusterDnsMap", key)
                            .defaultIfEmpty("")
                            .flatMap(state -> {
                                boolean needNoChange = stateNow.equals(state);
                                // stateNow=0，但当前的前端网口如果还包含了dnsIp，则仍需要走setDns方法
                                if (stateNow.equals("0") && needNoChange) {
                                    for (String eth : BUSINESS_ETH_MAP.values()) {
                                        com.macrosan.utils.functional.Tuple2<String, String> showEth2 = SshClientUtils.exec("ip a show " + eth, true, false);
                                        if (StringUtils.isNotBlank(showEth2.var2)) {
                                            log.error("get {} error, {}", eth, showEth2.var2);
                                        }
                                        for (String value : dnsMap.values()) {
                                            if (showEth2.var1.contains(value)) {
                                                needNoChange = false;
                                                break;
                                            }
                                        }
                                    }
                                }
                                return Mono.just(tuple3).zipWith(Mono.just(needNoChange));
                            });
                })
                .filter(tuple2 -> !tuple2.getT2())
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(tuple2 -> setDns(tuple2.getT1().var1, tuple2.getT1().var2, Integer.parseInt(tuple2.getT1().var3)))
                .doOnError(e -> log.error("deal dns error", e))
                .subscribe();
    }

    private static void setDns(boolean masterUuid, Map<String, String> dnsIpMap, int status) {
        log.info("deal dns status {}", status);
        Map<String, Map<String, String>> dnsMap = new HashMap<>();
        for (int i = 1; i < BUSINESS_ETH_MAP.size() + 1; i++) {
            Map<String, String> ipMap = new HashMap<>();
            String eth = BUSINESS_ETH_MAP.get("business_eth" + i);
            if (masterUuid) {
                String ipv4 = dnsIpMap.get("master_dns" + i);
                if (StringUtils.isNotEmpty(ipv4)) {
                    ipMap.put("ipv4", ipv4);
                }
                String ipv6 = dnsIpMap.get("master_dns" + i + "_ipv6");
                if (StringUtils.isNotEmpty(ipv6)) {
                    ipMap.put("ipv6", ipv6);
                }
            } else {
                String ipv4 = dnsIpMap.get("slave_dns" + i);
                if (StringUtils.isNotEmpty(ipv4)) {
                    ipMap.put("ipv4", ipv4);
                }
                String ipv6 = dnsIpMap.get("slave_dns" + i + "_ipv6");
                if (StringUtils.isNotEmpty(ipv6)) {
                    ipMap.put("ipv6", ipv6);
                }
            }
            dnsMap.put(eth, ipMap);
        }
        Map<String, String> clusterDnsMap = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall("clusterDnsMap");
        for (String key : clusterDnsMap.keySet()) {
            if (masterUuid && key.startsWith("master")) {
                log.info("deal dns delete key:  {}", key);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel("clusterDnsMap", key);
            } else if (!masterUuid && key.startsWith("slave")) {
                log.info("deal dns delete key:  {}", key);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel("clusterDnsMap", key);
            }
        }
        for (String ethName : dnsMap.keySet()) {
            Map<String, String> ipMap = dnsMap.get(ethName);
            for (String ethType : ipMap.keySet()) {
                String cmd;
                String prefix = "64";
                if ("ipv4".equals(ethType)) {
                    String res = SshClientUtils.exec("cat /etc/sysconfig/network-scripts/ifcfg-" + ethName + " |grep PREFIX");
                    if (StringUtils.isBlank(res)) {
                        continue;
                    }
                    prefix = res.split("=")[1];
                }
                if (status == 1) {
                    cmd = "ip addr add dev " + ethName + " " + ipMap.get(ethType) + "/" + prefix;
                } else {
                    cmd = "ip addr del dev " + ethName + " " + ipMap.get(ethType) + "/" + prefix;
                }
                log.info("deal dns cmd:  {}", cmd);
                SshClientUtils.exec(cmd);
            }
        }
        if (masterUuid) {
            if (status == 0) {
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset("clusterDnsMap", "master@" + UUID, "0");
            } else {
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset("clusterDnsMap", "master@" + UUID, "1");
            }
        } else {
            if (status == 0) {
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset("clusterDnsMap", "slave@" + UUID, "0");
            } else {
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset("clusterDnsMap", "slave@" + UUID, "1");
            }
        }
    }

    // 保存每个moss节点和各个extra站点的不可连通ip列表。<uuid, JsonUreachIpList<extraClusterIndex, unreachIpList>>
    public static final String EXTRA_ASYNC_STATUS = "extra_async_status";

    /**
     * 每个moss节点和各个minio节点是否能连通
     */
    private static void pingExtraNode() {
        Map<Integer, List<String>> unReachableIpList = new TreeMap<>();
        EXTRA_INDEX_IPS_ENTIRE_MAP.forEach((index, ips) -> {
            for (String ip : ips) {
                try {
                    boolean reachable = InetAddress.getByName(ip).isReachable(3_000);
                    if (!reachable) {
                        unReachableIpList.computeIfAbsent(index, i -> new ArrayList<>()).add(ip);
                    }
                } catch (Exception e) {
                    log.error("pingExtraNode error, {}", e.getMessage());
                    unReachableIpList.computeIfAbsent(index, i -> new ArrayList<>()).add(ip);
                }
            }
        });
        Mono.just("1").publishOn(SCAN_SCHEDULER)
                .subscribe(s -> {
                    String state = pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hget(EXTRA_ASYNC_STATUS, UUID);
                    if (StringUtils.isNotEmpty(state) && state.equals(Json.encode(unReachableIpList))) {
                        return;
                    }
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(EXTRA_ASYNC_STATUS, UUID, Json.encode(unReachableIpList));

                });
    }

    public void checkExtraLinkState() {
        pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(EXTRA_ASYNC_STATUS)
                .publishOn(SCAN_SCHEDULER)
                .filter(map -> !map.isEmpty())
                .map(map -> {
                    // 有一个moss节点和一个minio节点能ping通就认为异构复制的连接状态正常
                    Map<Integer, String> res = new HashMap<>();
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        String v = entry.getValue();
                        TreeMap<Integer, List<String>> uuidUnreachListMap = Json.decodeValue(v, new TypeReference<TreeMap<Integer, List<String>>>() {
                        });
                        if (uuidUnreachListMap.isEmpty()) {
                            // 有一个节点什么unreachip都不存在，表示和所有异构站点的所有节点的连接均正常。
                            for (Integer extraClusterIndex : EXTRA_INDEX_IPS_ENTIRE_MAP.keySet()) {
                                res.put(extraClusterIndex, "1");
                            }
                            return res;
                        }
                        for (Map.Entry<Integer, List<String>> e : uuidUnreachListMap.entrySet()) {
                            Integer extraClusterIndex = e.getKey();
                            List<String> unreachableList = e.getValue();
                            if (unreachableList.isEmpty() || unreachableList.size() < EXTRA_INDEX_IPS_ENTIRE_MAP.get(extraClusterIndex).length) {
                                res.put(extraClusterIndex, "1");
                            } else {
                                res.put(extraClusterIndex, "0");
                            }
                        }
                    }
                    return res;
                })
                .subscribe(map -> {
                    for (Map.Entry<Integer, String> entry : map.entrySet()) {
                        Integer extraClusterIndex = entry.getKey();
                        String curState = entry.getValue();
                        INDEX_STATUS_MAP.put(extraClusterIndex, Integer.parseInt(curState));
                        getStateInRedis(extraClusterIndex, curState)
                                .publishOn(SCAN_SCHEDULER)
                                .filter(state -> !curState.equals(state))
                                .subscribe(a -> {
                                    setLinkState(extraClusterIndex, curState);
                                    siteLinkStateChange();
                                });
                    }
                });

    }

    public void checkExtraCopyState() {
        pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(EXTRA_ASYNC_STATUS)
                .publishOn(SCAN_SCHEDULER)
                .filter(map -> !map.isEmpty())
                .flatMap(map -> {
                    boolean res = false;
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        String v = entry.getValue();
                        TreeMap<Integer, List<String>> reachMap = Json.decodeValue(v, new TypeReference<TreeMap<Integer, List<String>>>() {
                        });
                        if (reachMap.isEmpty()) {
                            res = true;
                        }
                        for (Map.Entry<Integer, List<String>> e : reachMap.entrySet()) {
                            List<String> list = e.getValue();
                            if (list.isEmpty() || list.size() < EXTRA_INDEX_IPS_ENTIRE_MAP.get(1).length) {
                                res = true;
                            }
                        }
                    }
                    String curState = "1";
                    if (!res) {//断开
                        INDEX_STATUS_MAP.put(1, 0);
                        curState = "0";
                    } else {
                        INDEX_STATUS_MAP.put(1, 1);
                    }
                    return Mono.just(curState);
                })
                .flatMap(curState -> getStateInRedis(1, curState).filter(state -> !curState.equals(state)).map(state -> curState))
                .subscribe(state -> Mono.just("1").publishOn(SCAN_SCHEDULER).subscribe(s -> {
                    setLinkState(1, state);
                    siteLinkStateChange();
                }));

    }

    // 单位超时时间内轮询的ip个数
    public static int unitCount = HEARTBEAT_INTERVAL_SECONDS;

    public static int unitCountArb = HEARTBEAT_INTERVAL_SECONDS;

    public void getNodeAmountPerRound() {
        pool.getReactive(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, "unitCount")
                .defaultIfEmpty(String.valueOf(HEARTBEAT_INTERVAL_SECONDS))
                .doOnNext(s -> unitCount = Integer.parseInt(s))
                .flatMap(s -> pool.getReactive(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, "unitCountArb"))
                .subscribe(s -> unitCountArb = Integer.parseInt(s));
    }

    // 如果链路延迟接近心跳超时时间，会概率出现超时报错，且需要双倍的超时时间才能恢复。原因怀疑与http请求的占用相关，需要进行规避。
    public void recheckTimeoutIp() {
        try {
            if (CHECK_TIME_OUT_IPS.compareAndSet(false, true)) {
                HashSet<String> set = new HashSet<>(TIMEOUT_RECHECK_IP_SET);
                for (String ip : set) {
                    // 防止有旧Ip在集合中无法清除
                    Set<String> allIps = INDEX_IPS_ENTIRE_MAP.values()
                            .stream()
                            .flatMap(Arrays::stream)
                            .collect(Collectors.toSet());
                    if (!allIps.contains(ip)) {
                        log.info("recheck remove old ip {}", ip);
                        TIMEOUT_RECHECK_IP_SET.remove(ip);
                        continue;
                    }
                    HttpClientRequest request = getClient().request(HttpMethod.GET, DA_PORT, ip, "?clusterstatus");
                    request.setTimeout(HEARTBEAT_INTERVAL_SECONDS * 1000 * 2 / unitCount)
                            .setHost(ip + ":" + DA_PORT)
                            .putHeader("test", CURRENT_IP)
                            .putHeader("request_index", String.valueOf(LOCAL_CLUSTER_INDEX))
                            .exceptionHandler(e -> {
                                request.reset();
                                log.error("recheck ip {} err, ", ip, e);
                            })
                            .handler(resp -> {
                                log.debug("recheck ip {} success, ", ip);
                                TIMEOUT_RECHECK_IP_SET.remove(ip);
                            })
                            .end();
                }
            }
        } finally {
            CHECK_TIME_OUT_IPS.compareAndSet(true, false);
        }
    }

    public static void initSiteIpSet() {
        Mono.delay(Duration.ofSeconds(5))
                .publishOn(SCAN_SCHEDULER).subscribe(l -> TIMEOUT_RECHECK_IP_SET.clear());
    }


}
