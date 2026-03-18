package com.macrosan.httpserver;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.*;
import com.macrosan.doubleActive.arbitration.Arbitrator;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.part.PartClient;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.message.xmlmsg.Error;
import com.macrosan.message.xmlmsg.Uploads;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.rsocket.server.Rsocket;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.authorize.AuthorizeFactory;
import com.macrosan.utils.authorize.AuthorizeV2;
import com.macrosan.utils.authorize.AuthorizeV4;
import com.macrosan.utils.cache.Md5DigestPool;
import com.macrosan.utils.codec.MixedStringConverter;
import com.macrosan.utils.codec.UrlEncoder;
import com.macrosan.utils.functional.LongLongTuple;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.serialize.JaxbUtils;
import com.macrosan.utils.worm.WormUtils;
import io.netty.buffer.ByteBuf;
import io.vertx.core.Handler;
import io.vertx.core.VertxOptions;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.MultiMap;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.AsyncFile;
import io.vertx.reactivex.core.file.FileSystem;
import io.vertx.reactivex.core.http.HttpClient;
import io.vertx.reactivex.core.http.HttpClientRequest;
import io.vertx.reactivex.core.http.HttpClientResponse;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.constants.ErrorNo.CONTENT_LENGTH_NOT_ALLOWED;
import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.DataSynChecker.isDebug;
import static com.macrosan.doubleActive.DoubleActiveUtil.*;
import static com.macrosan.doubleActive.PoolingRequest.escapeHeaders;
import static com.macrosan.ec.ECUtils.dealCopyRange;
import static com.macrosan.httpserver.ResponseUtils.responseError;
import static com.macrosan.httpserver.RestfulVerticle.getRequestSign;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.Type.*;
import static com.macrosan.utils.authorize.AuthorizeFactory.getAuthorizer;
import static com.macrosan.utils.authorize.AuthorizeV4.EMPTY_SHA256;
import static com.macrosan.utils.authorize.AuthorizeV4.X_AMZ_DECODED_CONTENT_LENGTH;
import static com.macrosan.utils.msutils.MsDateUtils.nowToGMT;
import static com.macrosan.utils.msutils.MsException.dealException;
import static com.macrosan.utils.store.StoreUtils.bytesToHex;
import static com.macrosan.utils.sts.RoleUtils.*;
import static com.macrosan.utils.trash.TrashUtils.bucketTrash;
import static com.macrosan.utils.worm.WormUtils.WORM_STAMP;

@Log4j2
public class MossHttpClient {

    /**
     * 该多活环境中站点的数量。不包括async站点
     */
    public static int NODE_AMOUNT = 2;

    /**
     * 该多活环境中站点的数量。包括async站点
     */
    public static Integer CLUSTERS_AMOUNT = 2;

    private static RedisConnPool pool = RedisConnPool.getInstance();

    /**
     * 本地复制链路的网口ip，eth4或者eth12
     */
    public static String LOCAL_NODE_IP = ServerConfig.getInstance().getHeartIp1();

    public static String SYNC_ETH_NAME = "";

    public static String SYNC_ETH_TYPE = "ipv4";

    /**
     * 保存所有节点的ip和所属第几个双活站点。<ip, 站点索引>
     */
    public static final Map<String, Integer> IP_INDEX_MAP = new ConcurrentHashMap<>();

    /**
     * 当前节点属于第几个双活站点。
     */
    public static Integer LOCAL_CLUSTER_INDEX;

    /**
     * 当前站点有多少个节点。由iplist长度决定。
     */
    public static Integer LOCAL_NODE_AMOUNT;

    /**
     * 保存各个站点下的所有“可用”ip，用于发送双活相关请求时根据站点索引获取可用ip。<站点索引, ip数组>
     * 注意该集合内容会随心跳检测的进行而改变。
     */
    public static final Map<Integer, String[]> INDEX_IPS_MAP = new ConcurrentHashMap<>();

    public static final Map<String, Map<String, String>> ETH12_INDEX_MAP = new ConcurrentHashMap<>();

    /**
     * 完整保存各个站点下所有可用ip
     */
    public static final Map<Integer, String[]> INDEX_IPS_ENTIRE_MAP = new ConcurrentHashMap<>();

    private static MossHttpClient instance;

    public static MossHttpClient getInstance() {
        if (instance == null) {
            instance = new MossHttpClient();
        }
        return instance;
    }

    public static HttpClient getClient() {
        if (IS_SSL_SYNC.get()) {
            return httpsClient;
        }
        return httpClient;
    }

    private static HttpClient httpClient;

    private static HttpClient httpsClient;

    private HashMap<Boolean, IntObjectHashMap<HttpClient>> frontClientMap = new HashMap<>();

    public static Vertx vertx;

    public static final long DEFAULT_FETCH_AMOUNT = 4 * 1024L;

    public static final long BACKUP_FETCH_AMOUNT = 4 * 16L;

    /**
     * 不再使用默认的metaPool作为record的存储池
     */
    @Deprecated
    public static StoragePool SYNC_RECORD_STORAGE_POOL;

    // 是否使用的是非eth4网口作为复制链路网口。
    public static boolean USE_ETH4 = true;

    /**
     * 本站点是否为多活站点的async站点
     */
    public static boolean IS_ASYNC_CLUSTER = false;

    /**
     * 多活站点的async站点索引，逗号分隔
     */
    public static String ASYNC_CLUSTERS;

    /**
     * 本站点是双活站点且有async站点，则此为true，表示需要预提交async站点的差异记录
     */
    public static Boolean WRITE_ASYNC_RECORD = false;

    /**
     * 本站点是三复制站点，需要写两份差异记录
     */
    public static Boolean IS_THREE_SYNC = false;

    /**
     * 若存在双活站点以外的异步复制站点，保存站点索引和ips
     */
    public static final Map<Integer, String[]> ASYNC_INDEX_IPS_MAP = new ConcurrentHashMap<>();

    public static final Map<Integer, String[]> ASYNC_INDEX_IPS_ENTIRE_MAP = new ConcurrentHashMap<>();


    /**
     * 若存在双活站点以外的异步复制站点，保存站点索引和ips
     */
    public static final Integer THREE_SYNC_INDEX = 2;


    /**
     * 保存非async站点的ip。当没有async站点时和INDEX_IPS_MAP一致
     */
    public static final Map<Integer, String[]> DA_INDEX_IPS_MAP = new ConcurrentHashMap<>();

    public static final Map<Integer, String[]> DA_INDEX_IPS_ENTIRE_MAP = new ConcurrentHashMap<>();

    public static int DA_PORT;

    public static int DA_HTTP_PORT;

    /**
     * 保存站点索引和站点名称
     */
    public static final Map<Integer, String> INDEX_NAME_MAP = new ConcurrentHashMap<>();

    public static final Map<String, Integer> NAME_INDEX_MAP = new ConcurrentHashMap<String, Integer>();

    public static final Map<String, Integer> NAME_INDEX_FLAT_MAP = new ConcurrentHashMap<>();

    public static final AtomicBoolean hasExtraAsync = new AtomicBoolean();

    public static final AtomicBoolean IS_SSL_SYNC = new AtomicBoolean();

    public static final Map<Integer, String[]> EXTRA_INDEX_IPS_ENTIRE_MAP = new ConcurrentHashMap<>();

    public static int EXTRA_PORT;

    public static boolean EXTRA_CLUSTER = false;

    public static final Map<String, Tuple2<String, String>> EXTRA_AK_SK_MAP = new ConcurrentHashMap<>();

    int poolNum;

    int maxPoolSize;

    public void init() {
        try {
            String syncMethod = pool.getCommand(REDIS_SYSINFO_INDEX).get("sync_method");
            IS_SSL_SYNC.set((StringUtils.isNotEmpty(syncMethod) && !"http".equals(syncMethod)));
            vertx = Vertx.vertx(new VertxOptions()
                    .setEventLoopPoolSize(PROC_NUM)
                    .setPreferNativeTransport(true));
            HttpClientOptions options = new HttpClientOptions()
                    .setKeepAlive(true)
                    .setKeepAliveTimeout(60)
                    .setMaxPoolSize(500)
                    .setMaxWaitQueueSize(10000)
                    .setConnectTimeout(3000)
                    .setIdleTimeout(30);

            HttpClientOptions httpsOptions = new HttpClientOptions()
                    .setKeepAlive(true)
                    .setKeepAliveTimeout(60)
                    .setMaxPoolSize(500)
                    .setMaxWaitQueueSize(10000)
                    .setConnectTimeout(3000)
                    .setIdleTimeout(30)
                    .setSsl(true)
                    .setVerifyHost(false)
                    .setTrustAll(true);

            httpClient = vertx.createHttpClient(options);
            httpsClient = vertx.createHttpClient(httpsOptions);

            log.info("PROC_NUM: {}", PROC_NUM);
            poolNum = 16;
            maxPoolSize = 1600;
            HttpClientOptions options2 = new HttpClientOptions()
                    .setKeepAlive(true)
                    .setKeepAliveTimeout(60)
                    .setMaxPoolSize(maxPoolSize / poolNum)
                    .setMaxWaitQueueSize(10000)
                    .setConnectTimeout(3000)
                    .setIdleTimeout(30);
            HttpClientOptions httpsOptions2 = new HttpClientOptions()
                    .setKeepAlive(true)
                    .setKeepAliveTimeout(60)
                    .setMaxPoolSize(maxPoolSize / poolNum)
                    // ssl处理速度慢，设10000容易oom
                    .setMaxWaitQueueSize(1000)
                    .setConnectTimeout(3000)
                    .setIdleTimeout(30)
                    .setSsl(true)
                    .setVerifyHost(false)
                    .setTrustAll(true);

            IntObjectHashMap<HttpClient> httpClientMap = new IntObjectHashMap<>();
            IntStream.range(0, poolNum).forEach(n -> httpClientMap.put(n, vertx.createHttpClient(options2)));
            frontClientMap.put(false, httpClientMap);

            IntObjectHashMap<HttpClient> httpsClientMap = new IntObjectHashMap<>();
            IntStream.range(0, poolNum).forEach(n -> httpsClientMap.put(n, vertx.createHttpClient(httpsOptions2)));
            frontClientMap.put(true, httpsClientMap);

            initClustersCollections();
            SYNC_RECORD_STORAGE_POOL = StoragePoolFactory.getStoragePool(StorageOperate.META);

            Rsocket.initAsync();
            RSocketClient.initAsync();

            VersionUtil.isMultiCluster = true;
        } catch (Exception e) {
            log.error("init MossHttpClient failed, ", e);
        }
    }

    private HttpClient getFrontClient() {
        int i = ThreadLocalRandom.current().nextInt(poolNum);
        if (IS_SSL_SYNC.get()) {
            return frontClientMap.get(true).get(i);
        }
        return frontClientMap.get(false).get(i);
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
    }

    public static void initClustersCollections() {
        //根据redis记录初始化各个当前节点
        Map<String, String> localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(LOCAL_CLUSTER);
        String[] ipList = localCluster.get(CLUSTER_IPS).split(";");
        String localNodeIp = null;
        SYNC_ETH_NAME = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SYNC_ETH);
        String ethType = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, ETH_TYPE);
        if (StringUtils.isNotEmpty(ethType)) {
            SYNC_ETH_TYPE = ethType;
        }
        String syncEthToType = ACTIVE_SYNC_IP;
        if (StringUtils.isNotEmpty(SYNC_ETH_NAME)) {
            syncEthToType = syncEthToType(SYNC_ETH_NAME, ethType);
            localNodeIp = pool.getCommand(REDIS_NODEINFO_INDEX).hget(ServerConfig.getInstance().getHostUuid(), syncEthToType);
        } else {
            localNodeIp = pool.getCommand(REDIS_NODEINFO_INDEX).hget(ServerConfig.getInstance().getHostUuid(), ACTIVE_SYNC_IP);
            if (StringUtils.isNotEmpty(localNodeIp)) {
                SYNC_ETH_NAME = "eth12";
            } else {
                SYNC_ETH_NAME = "eth4";
            }
        }
        USE_ETH4 = "eth4".equals(SYNC_ETH_NAME);
        String syncIp = USE_ETH4 ? LOCAL_NODE_IP : localNodeIp;
        LOCAL_NODE_IP = syncIp;
        Set<String> other_clusters = pool.getCommand(REDIS_SYSINFO_INDEX).smembers("other_clusters");

        for (int i = 0; i < ipList.length; i++) {
            String[] oneNodeIps = ipList[i].split(",");
            INDEX_IPS_MAP.put(i, oneNodeIps);
            INDEX_IPS_ENTIRE_MAP.put(i, oneNodeIps);

            for (int j = 0; j < oneNodeIps.length; j++) {
                String oneNodeIp = oneNodeIps[j];
                IP_INDEX_MAP.put(oneNodeIp, i);
                if (oneNodeIp.equals(syncIp)) {
                    LOCAL_CLUSTER_INDEX = i;
                    INDEX_NAME_MAP.put(i, localCluster.get(CLUSTER_NAME));
                }
            }
            for (String s : other_clusters) {
                JSONObject jsonObject = JSON.parseObject(s);
                if (jsonObject.getString(CLUSTER_IPS).equals(ipList[i])) {
                    INDEX_NAME_MAP.put(i, jsonObject.getString(CLUSTER_NAME));
                    break;
                }
            }
        }

        // 新加的节点尚未配置syncIp时，LOCAL_CLUSTER_INDEX无法初始化。
        if (LOCAL_CLUSTER_INDEX == null) {
            Map<String, String> indexClusterMap = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall("index_cluster_map");
            indexClusterMap.forEach((clusterName, index) -> {
                if (clusterName.equals(localCluster.get(CLUSTER_NAME))) {
                    LOCAL_CLUSTER_INDEX = Integer.parseInt(index);
                    INDEX_NAME_MAP.put(Integer.parseInt(index), clusterName);
                }
            });
        }

        INDEX_NAME_MAP.forEach((index, name) -> {
            NAME_INDEX_MAP.put(name, index);
            String stringSimple = MixedStringConverter.convertMixedStringSimple(name);
            NAME_INDEX_FLAT_MAP.put(stringSimple, index);
        });

        if ("master".equals(Utils.getRoleState())) {
            for (Entry<Integer, String> entry : INDEX_NAME_MAP.entrySet()) {
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset("index_cluster_map", entry.getValue(), String.valueOf(entry.getKey()));
            }
        }

        String syncPolicyAsync = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SYNC_POLICY_ASYNC);
        ASYNC_CLUSTERS = syncPolicyAsync;
        if (StringUtils.isNotBlank(syncPolicyAsync)) {
            String[] asyncIndexes = syncPolicyAsync.split(",");
            for (String index : asyncIndexes) {
                int asyncIndex = Integer.parseInt(index);
                if (INDEX_IPS_ENTIRE_MAP.get(asyncIndex) == null) {
                    log.info("no suitable async cluster index in iplist");
                    continue;
                }
                ASYNC_INDEX_IPS_MAP.put(asyncIndex, INDEX_IPS_ENTIRE_MAP.get(asyncIndex));
                ASYNC_INDEX_IPS_ENTIRE_MAP.put(asyncIndex, INDEX_IPS_ENTIRE_MAP.get(asyncIndex));
                if (asyncIndex == LOCAL_CLUSTER_INDEX) {
                    IS_ASYNC_CLUSTER = true;
                }
            }
        }

        initOptionalAsyncClusters();

        // 判断目前多站点策略，0为三活，1为三复制
        String clusterSyncWay = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTERS_SYNC_WAY);
        if (StringUtils.isNotBlank(clusterSyncWay)) {
            if ("1".equals(clusterSyncWay)) {
                IS_THREE_SYNC = true;
            }
        }

        INDEX_IPS_ENTIRE_MAP.forEach((index, ips) -> {
            if (!ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(index)) {
                DA_INDEX_IPS_MAP.put(index, ips);
                DA_INDEX_IPS_ENTIRE_MAP.put(index, ips);
            }
        });

        // 先部署复制再部署的情况下，原先的从站点会变成复制站点
        ASYNC_INDEX_IPS_ENTIRE_MAP.forEach((index, ip) -> {
            DA_INDEX_IPS_MAP.remove(index);
            DA_INDEX_IPS_ENTIRE_MAP.remove(index);
        });

        LOCAL_NODE_AMOUNT = INDEX_IPS_ENTIRE_MAP.get(LOCAL_CLUSTER_INDEX).length;
        NODE_AMOUNT = INDEX_IPS_ENTIRE_MAP.size() - ASYNC_INDEX_IPS_ENTIRE_MAP.size();
        CLUSTERS_AMOUNT = INDEX_IPS_ENTIRE_MAP.size();

        if (IS_THREE_SYNC || (!IS_ASYNC_CLUSTER && !ASYNC_INDEX_IPS_ENTIRE_MAP.isEmpty())) {
            WRITE_ASYNC_RECORD = true;
        }

        String port = !IS_SSL_SYNC.get() ? ServerConfig.getInstance().getHttpPort()
                : ServerConfig.getInstance().getHttpsPort();
        DA_PORT = Integer.parseInt(port);
        DA_HTTP_PORT = Integer.parseInt(ServerConfig.getInstance().getHttpPort());
        if (!USE_ETH4) {
            for (String uuid : pool.getCommand(REDIS_NODEINFO_INDEX).keys("*")) {
                Map<String, String> uuidMap = pool.getCommand(REDIS_NODEINFO_INDEX).hgetall(uuid);
                String curSyncIp = uuidMap.getOrDefault(syncEthToType, "null");
                ETH12_INDEX_MAP.put(curSyncIp, uuidMap);
            }
        }

//        MAX_DEAL_AMOUNT = MAX_DEAL_AMOUNT / (NODE_AMOUNT + EXTRA_INDEX_IPS_ENTIRE_MAP.size());
        SyncRecordLimiter.start();

        initFormerAsyncCluster();
        updateDnsList();
    }

    public static void updateDnsList() {
        for (Integer index : DA_INDEX_IPS_MAP.keySet()) {
            if (LOCAL_CLUSTER_INDEX.equals(index)) {
                continue;
            }
            String siteName = INDEX_NAME_MAP.get(index);
            if (ONLY_HOST_LIST.size() > 0) {
                for (String host : ONLY_HOST_LIST) {
                    String[] split = host.split("\\.", 2);
                    String clusterHost = split[0] + "." + siteName + "." + ServerConfig.getInstance().getRegion() + "." + split[1];
                    HOST_LIST.add(clusterHost);
                }
            }
        }
    }

    /**
     * 3DC或三复制的站点B历史差异记录同步状态，只有站点B有用。
     * key: 新加入的站点索引
     * value: -1为默认值，不做任何处理；0表示有差异记录需要同步；1表示差异记录已同步完毕
     */
    public static final Map<Integer, AtomicInteger> ASYNC_SYNC_COMPLETE_MAP = new ConcurrentHashMap<>();

    /**
     * 先部署了复制站点后部署3DC或者再加入新的非async站点，站点B变为async站点，需要对变为async站点的站点B做额外处理：
     * 开启DataSynChecker；扫描历史数据（差异记录）
     */
    static void initFormerAsyncCluster() {
        // 站点A和最后部署的站点不需要处理
        if (LOCAL_CLUSTER_INDEX == 0 || LOCAL_CLUSTER_INDEX == INDEX_IPS_ENTIRE_MAP.size() - 1) {
            return;
        }
        // 非async站点不需要处理
        if (!IS_ASYNC_CLUSTER) {
            return;
        }

        Map<String, String> map = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(ASYNC_SYNC_COMPLETE_STR);
        // 新加站点的索引
        int lastIndex = CLUSTERS_AMOUNT - 1;
        // 如果async站点的下一个站点已经同步完了async站点的数据，则表示该async站点的数据已经不是只有自己有了，可以跳过历史数据同步
        String s = map.get(LOCAL_CLUSTER_INDEX + 1);
        if ("1".equals(s)) {
            return;
        }

        map.forEach((k, v) -> ASYNC_SYNC_COMPLETE_MAP.put(Integer.parseInt(k), new AtomicInteger(Integer.parseInt(v))));

        ASYNC_SYNC_COMPLETE_MAP.computeIfAbsent(lastIndex, k -> new AtomicInteger(-1));
        // 新加站点已经进入历史数据同步流程或者已经完成流程，则跳出。
        if (ASYNC_SYNC_COMPLETE_MAP.get(lastIndex).get() == 0
                || ASYNC_SYNC_COMPLETE_MAP.get(lastIndex).get() == 1) {
            return;
        }
        ASYNC_SYNC_COMPLETE_MAP.get(lastIndex).set(0);
        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(ASYNC_SYNC_COMPLETE_STR, String.valueOf(lastIndex), "0");

    }


    /**
     * 初始化额外的复制站点，需要ipList记录复制集群ip，以及index记录（在表2，master_cluster，extra_async_cluster）。
     * 单站点sync_policy为空，需要设置为“1”。单站点也需要加上自己用以同步的ip至ipList，以及hset master_cluster cluster_name。
     * port等信息记录在表2，hgetall extra_async
     */
    static void initOptionalAsyncClusters() {
        String hget = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, EXTRA_ASYNC_CLUSTER);
        if (StringUtils.isBlank(hget)) {
            EXTRA_CLUSTER = false;
            return;
        }
        EXTRA_CLUSTER = true;
        ASYNC_CLUSTERS = StringUtils.isBlank(ASYNC_CLUSTERS) ? hget : ASYNC_CLUSTERS + "," + hget;
        hasExtraAsync.set(true);
        String syncPolicy = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SYNC_POLICY);
        if (StringUtils.isBlank(syncPolicy)) {
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, SYNC_POLICY, "1");
        }

        String[] extraIndexes = hget.split(",");
        for (String extraIndex : extraIndexes) {
            int index = Integer.parseInt(extraIndex);
            if (!INDEX_IPS_ENTIRE_MAP.containsKey(index)) {
                throw new RuntimeException("no such extra-async index in iplist.");
            }

            EXTRA_INDEX_IPS_ENTIRE_MAP.put(index, INDEX_IPS_ENTIRE_MAP.get(index));
            ASYNC_INDEX_IPS_MAP.put(index, INDEX_IPS_ENTIRE_MAP.get(index));
            ASYNC_INDEX_IPS_ENTIRE_MAP.put(index, INDEX_IPS_ENTIRE_MAP.get(index));
        }

        Map<String, String> hgetall = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(EXTRA_ASYNC);
        EXTRA_PORT = Integer.parseInt(hgetall.get("port"));
    }

    public void manageSend(MsHttpRequest r) {
        //<ip, HttpClientRequest>, 保存待数据传输的双活连接
        Map<String, HttpClientRequest> reqs = new HashMap<>(NODE_AMOUNT);
        // 保存包括失败的请求在内的所有连接，用于后续手动关闭。
        Map<String, HttpClientRequest> totalReqs = new HashMap<>();

        //存放双活响应的map
        UnicastProcessor<Tuple2<String, HttpClientResponse>> processor = UnicastProcessor.create(
                Queues.<Tuple2<String, HttpClientResponse>>unboundedMultiproducer().get());

        r.exceptionHandler(e -> {
            try {
                log.debug("origin request exception, {}", r.uri(), e);
                for (HttpClientRequest request : totalReqs.values()) {
                    try {
                        if (request.getDelegate().connection() != null) {
                            synchronized (request.getDelegate().connection()) {
                                request.end();
                            }
                        }
                    } catch (Exception exception) {
                        log.debug("", e);
                    }
                }

                totalReqs.forEach((ip, req) -> req = null);
                totalReqs.clear();
                reqs.forEach((ip, req) -> req = null);
                reqs.clear();

                r.connection().close();
                processor.onComplete();
            } catch (Exception exception) {
                log.error("", e);
            }
        });

        AtomicInteger reqsAmount = new AtomicInteger(NODE_AMOUNT);

        //向每个站点发送http请求。本站点的请求发送往本节点。其他站点的请求随机发送到一个下属节点。
        HttpResponseSummary responseSummary = new HttpResponseSummary(processor);
        redirectManageRequest(r, reqs, processor, reqsAmount, totalReqs, responseSummary);

        AtomicLong i = new AtomicLong();
        Disposable subscribe1 = responseSummary.responses.subscribe(tuple2 -> {
                    if (i.incrementAndGet() == NODE_AMOUNT) {
                        processor.onComplete();
                    }
                }, e -> log.error("Active-Active sync request error", e),
                () -> {
                    //r已经异常关闭。
                    if (r.response().ended() || r.response().closed()) {
                        log.error("close every requests");
                        totalReqs.forEach((ip, request) -> closeConn(request));
//                                streamDispose(disposables);
                        return;
                    }

                    //至少成功一个集群时，给用户返回成功；
                    //全部失败，返回本端的错误消息。
                    try {
                        if (responseSummary.successAmount >= 1) {
                            //存在站点写入失败，且站点连接未显示断开，写入同步record（站点断开在请求开始前写入）
                            if (responseSummary.successAmount != NODE_AMOUNT) {
                                succOneCount.incrementAndGet();
                                // 两个站点都成功才会返回，否则挂起

                            } else {
                                succTwoCount.incrementAndGet();
                                Buffer b1 = responseSummary.respBuffers.get(responseSummary.successResponse);
                                if (b1 != null) {
                                    dealResponse(r, responseSummary.successResponse, b1.slice(xmlHeader.length, b1.length()));
                                    return;
                                }
                                responseSummary.successResponse.bodyHandler(buffer -> {
                                    dealResponse(r, responseSummary.successResponse, buffer);
                                }).resume();
                            }

                            totalReqs.forEach((ip, req) -> req = null);
                            totalReqs.clear();
                            reqs.forEach((ip, req) -> req = null);

                        } else {
//                                    log.info("{} fail, {}", r.uri(), responseSummary)failTwoCount.incrementAndGet();
                            if (responseSummary.localResponse == null) {
                                dealException(r, new MsException(UNKNOWN_ERROR, "no sync req can be sent. " + r.uri()));
                            } else {
                                dealErrResponse(r, responseSummary, new AtomicBoolean());
                            }
                            totalReqs.forEach((ip, req) -> req = null);
                            totalReqs.clear();
                            reqs.forEach((ip, req) -> req = null);
                        }
                        reqs.clear();

                    } catch (Exception e) {
                        log.error("", e);
                        dealException(r, e);
                    }
                });
        r.addResponseCloseHandler(v -> subscribe1.dispose());

    }

    private void redirectManageRequest(MsHttpRequest r, Map<String, HttpClientRequest> reqs, UnicastProcessor<Tuple2<String, HttpClientResponse>> processor, AtomicInteger reqsAmount, Map<String,
            HttpClientRequest> totalReqs, HttpResponseSummary responseSummary) {
        // 没有连接问题
        AtomicInteger responseCount = new AtomicInteger();
        // 连接中断出错或者返回不为200的请求个数
        AtomicInteger exceptionCount = new AtomicInteger();
        // 有站点A的put请求已经返回，站点B还未响应continue，此时应由站点B request的continueHandler触发resume。
        AtomicBoolean hasContinue = new AtomicBoolean();

        Map<Integer, String[]> map = new HashMap<>(NODE_AMOUNT);
        DA_INDEX_IPS_MAP.forEach(map::put);

        // 保证一次客户端请求只调用一次resumeR
        AtomicBoolean startResume = new AtomicBoolean();
        AtomicBoolean isFirst = new AtomicBoolean(true);

        r.headers().remove(MULTI_SYNC);
        String accessKey = getNewAccessKey();
        String secretKey = getNewSecretKey();
        String policyId = POLICY_ID_PREFIX + getNewId();
        String assumeId = ASSUME_PREFIX + getNewId();
        String roleMrn = r.getMember("Iam_" + ROLE_ARN);
        String durationSecondsStr = r.getMember("Iam_" + DURATION_SECONDS);
        long durationSeconds = checkAndGetDurationSeconds(roleMrn, durationSecondsStr);
        long deadline = System.currentTimeMillis() / 1000 + durationSeconds;//所有转发的请求共用deadline

        //向每个站点发送http请求。本站点的请求发送往本节点。其他站点的请求随机发送到一个下属节点。
        map.forEach((index, ips) -> {
            int[] ipIndex = new int[]{ThreadLocalRandom.current().nextInt(ips.length)};
            UnicastProcessor<Integer> retryProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
            // 已取过的ip在ips的索引
            Set<Integer> indexSet = new ConcurrentHashSet<>();

            AtomicInteger tryNum = new AtomicInteger();
            String[] tempIp = new String[1];
            // 加pulishon会造成很多连接关闭。
            Disposable subscribe = retryProcessor.subscribe(i -> {
                indexSet.add(ipIndex[0]);
                if (tryNum.get() > 0) {
                    // 在剩下的ip中随机获取一个
                    int length = ips.length - tryNum.get() + 1;
                    int[] restIndexes = new int[length];
                    int b = 0;
                    for (int a = 0; a < ips.length; a++) {
                        if (indexSet.contains(a)) {
                            continue;
                        }
                        restIndexes[b++] = a;
                    }
                    int nexInt = ThreadLocalRandom.current().nextInt(length);
                    ipIndex[0] = restIndexes[nexInt];
                }
                String ip = LOCAL_CLUSTER_INDEX.equals(index) ? LOCAL_IP_ADDRESS : ips[ipIndex[0]];
                tempIp[0] = ip;
                if (r.response().ended() || r.response().closed()) {
                    exceptionCount.incrementAndGet();
                    processor.onNext(new Tuple2<>(ip, null));
                    if (responseCount.incrementAndGet() >= NODE_AMOUNT) {
                        //向各个节点分发的请求都有了返回或出错，如果此时并非所有节点都出错，则开始数据流的传输
                        if (exceptionCount.get() < NODE_AMOUNT && startResume.compareAndSet(false, true)) {
                            reqsAmount.set(NODE_AMOUNT - exceptionCount.get());
                            resumeR(r, reqsAmount.get() > 0);
                        }
                    }
                    retryProcessor.onComplete();
                    return;
                }

                AtomicBoolean hasExc = new AtomicBoolean(false);
                HttpClientRequest request = getFrontClient().request(r.method(), DA_PORT, ip, r.uri());
                request.setWriteQueueMaxSize(WriteQueueMaxSize)
                        .setTimeout(10_000);
                reqs.put(ip, request);
                totalReqs.put(ip + "-" + i, request);
                // 该请求有没有进入continueHandler
                AtomicBoolean singleContinue = new AtomicBoolean();
                try {
                    request.setHost(ip + ":" + DA_PORT).exceptionHandler(e -> {
                        if (!hasExc.compareAndSet(false, true)) {
                            return;
                        }
                        reqs.remove(ip);
                        totalReqs.remove(ip + "-" + i);
                        log.debug("send {} request to {} error1! {}, {}, {}", request.method().name(), request.getHost() + request.uri(), e.getClass().getName(),
                                e.getMessage(),
                                startResume.get());
                        // 似乎加上后出现大量异常request的情况下httpstream回收变快
                        request.reset();
                        log.debug("{}, {}, {}, {}, {}, {}, {}",
                                request.method().name() + request.getHost() + request.uri(),
                                tryNum.get(),
                                reqsAmount,
                                exceptionCount.get(),
                                startResume.get(),
                                reqs.size(),
                                responseCount.get());

                        // 如果已经开始数据传输途中出现了连接问题，也不必重试了。
                        if (r.response().ended() || r.response().closed() || startResume.get()) {
                            exceptionCount.incrementAndGet();
                            // 双活连接正常数量大于0，需要客户端继续传数据。
                            if (reqsAmount.decrementAndGet() > 0) {
                                if (startResume.get()) {
                                    r.fetch(1L);
                                }
                            }
                            processor.onNext(new Tuple2<>(ip, null));
                            retryProcessor.onComplete();
                            return;
                        }


                        if (tryNum.incrementAndGet() == ips.length) {
                            exceptionCount.incrementAndGet();
                            processor.onNext(new Tuple2<>(ip, null));
                            if (responseCount.incrementAndGet() >= NODE_AMOUNT) {
                                //向各个节点分发的请求都有了返回或出错，如果此时并非所有节点都出错，则开始数据流的传输
                                if (exceptionCount.get() < NODE_AMOUNT && startResume.compareAndSet(false, true)) {
                                    reqsAmount.set(NODE_AMOUNT - exceptionCount.get());
                                    resumeR(r, reqsAmount.get() > 0);
                                }
                            }
                            retryProcessor.onComplete();
                        } else {
                            retryProcessor.onNext(tryNum.get());
                        }
                    }).handler(response -> {
                        boolean respSuc = response.statusCode() == SUCCESS;
                        if (respSuc) {
                            response.handler(b -> {
                                responseSummary.respBuffers.computeIfAbsent(response, k -> Buffer.buffer()).appendBuffer(b);
                                if (isFirst.compareAndSet(true, false)) {
                                    r.response()
                                            .putHeader(CONTENT_TYPE, "application/xml")
                                            .putHeader(TRANSFER_ENCODING, "chunked")
                                            .putHeader(SERVER, "MOSS")
                                            .putHeader(ACCESS_CONTROL_HEADERS, "*")
                                            .putHeader(ACCESS_CONTROL_ORIGIN, "*")
                                            .putHeader(DATE, nowToGMT())
                                            .write(io.vertx.core.buffer.Buffer.buffer(xmlHeader));
                                }

                                r.response().write("\n");
                            }).exceptionHandler(e -> {
                                log.debug("response err, host {} response status {} respCount {} excCount {} reqsAmout {} auth {}",
                                        request.getHost() + request.uri(), response.statusCode(), responseCount.get(), exceptionCount.get(), reqsAmount.get(), r.getHeader(AUTHORIZATION));
                                exceptionCount.incrementAndGet();
                                processor.onNext(new Tuple2<>(ip, null));
                                if (responseCount.incrementAndGet() >= NODE_AMOUNT) {
                                    //向各个节点分发的请求都有了返回或出错，如果此时并非所有节点都出错，则开始数据流的传输
                                    if (exceptionCount.get() < NODE_AMOUNT && startResume.compareAndSet(false, true)) {
                                        reqsAmount.set(NODE_AMOUNT - exceptionCount.get());
                                        resumeR(r, reqsAmount.get() > 0);
                                    }
                                }
                                retryProcessor.onComplete();
                            }).endHandler(v -> {
                                // A站点返回如404、500的响应，B站点可能可以正常。此时需要这个handler能够触发resume。
                                exceptionCount.incrementAndGet();
                                reqs.remove(ip);
                                if (reqsAmount.decrementAndGet() > 0) {
                                    if (startResume.get()) {
                                        r.fetch(1L);
                                    }
                                }
                                hasContinue.compareAndSet(true, false);
                                // 本次请求走过了continueHandler, 会记两次responseCount，需要-1。
                                if (singleContinue.get()) {
                                    responseCount.decrementAndGet();
                                }

                                // 有可能一个请求走了continue和handler，就会开始resume，此时有可能另一个请求还没到continue
                                if (responseCount.incrementAndGet() >= NODE_AMOUNT && !hasContinue.get() && startResume.compareAndSet(false, true)) {
                                    reqsAmount.set(NODE_AMOUNT - exceptionCount.get());
                                    resumeR(r, reqsAmount.get() > 0);
                                }
                                log.debug("get response1 {} {} response status {} respCount {} excCount {} reqsAmout {} auth {}", request.method().name(),
                                        request.getHost() + request.uri(), response.statusCode(), responseCount.get(), exceptionCount.get(), reqsAmount.get(), r.getHeader(AUTHORIZATION));
                                processor.onNext(new Tuple2<>(ip, response));
                                retryProcessor.onComplete();
                            });
                            return;
                        }

                        response.pause();
                        // A站点返回如404、500的响应，B站点可能可以正常。此时需要这个handler能够触发resume。
                        if (singleContinue.get()) {
                            responseCount.decrementAndGet();
                        }

                        // 有可能一个请求走了continue和handler，就会开始resume，此时有可能另一个请求还没到continue
                        if (responseCount.incrementAndGet() >= NODE_AMOUNT && !hasContinue.get() && startResume.compareAndSet(false, true)) {
                            reqsAmount.set(NODE_AMOUNT - exceptionCount.get());
                            resumeR(r, reqsAmount.get() > 0);
                        }
                        log.debug("get response2 {} {} response status {} respCount {} excCount {} reqsAmout {} auth {}", request.method().name(),
                                request.getHost() + request.uri(), response.statusCode(), responseCount.get(), exceptionCount.get(), reqsAmount.get(), r.getHeader(AUTHORIZATION));
                        processor.onNext(new Tuple2<>(ip, response));
                        retryProcessor.onComplete();
                    });

                    r.headers().forEach(header -> request.putHeader(header.getKey(), header.getValue()));
                    request.headers().add("AccessKey", accessKey);
                    request.headers().add("SecretKey", secretKey);
                    request.headers().add("PolicyId", policyId);
                    request.headers().add("AssumeId", assumeId);
                    request.headers().add("Deadline", String.valueOf(deadline));
                    request.headers().add(DOUBLE_FLAG, "1");
                    if (r.headers().contains(X_AMZ_COPY_SOURCE)) {
                        request.putHeader(CONTENT_LENGTH, "0");
                    }
                    encodeAmzHeaders(request);
                    request.putHeader(CLUSTER_ALIVE_HEADER, ip);
                    request.end(r.getMember("body"));
                } catch (Exception e) {
                    log.error("send {} request to {} error2! {}, {}", request.method().name(), request.getHost(), e.getClass().getName(), e.getMessage());
                    dealException(r, e);
                }
            }, e -> {
                log.error("redirect error, ip: {}", tempIp[0], e);
//                dealException(r, e);
                processor.onNext(new Tuple2<>(tempIp[0], null));
                retryProcessor.onComplete();

            });
            r.addResponseCloseHandler(s -> Optional.ofNullable(subscribe).ifPresent(Disposable::dispose));
            retryProcessor.onNext(1);
        });

    }

    public void send(MsHttpRequest r) {
        try {
            //<ip, HttpClientRequest>, 保存待数据传输的双活连接
            Map<String, HttpClientRequest> reqs = new HashMap<>(NODE_AMOUNT);
            // 保存包括失败的请求在内的所有连接，用于后续手动关闭。
            Map<String, HttpClientRequest> totalReqs = new HashMap<>();

            //存放双活响应的map
            UnicastProcessor<Tuple2<String, HttpClientResponse>> processor = UnicastProcessor.create(
                    Queues.<Tuple2<String, HttpClientResponse>>unboundedMultiproducer().get());
            boolean formUpload = HttpMethod.POST == r.method() && r.headers().contains(CONTENT_TYPE) && r.getHeader(CONTENT_TYPE).contains("multipart/form-data");
            UnSynchronizedRecord.Type type = r.headers().contains(MULTI_SYNC) && "1".equals(r.headers().get(MULTI_SYNC)) ? ERROR_PUT_BUCKET : UnSynchronizedRecord.type(r.uri(), formUpload ? "PUT" :
                    r.method().toString());
//            // 异步修复put时判断是否需要中断的标志
//            if (type == ERROR_PUT_OBJECT) {
//                unSynchronizedRecord.getHeaders().put(CLUSTER_VALUE, "interrupt");
//            }

            AtomicBoolean waitForFetch = new AtomicBoolean(false);
            //每个双活请求实际写入元素的次数的总和。总增加量 = 原请求fetch数 * 双活个数
            AtomicLong writeTotal = new AtomicLong();
            AtomicLong fetchTotal = new AtomicLong(DEFAULT_FETCH_AMOUNT);
            AtomicInteger reqsAmount = new AtomicInteger(NODE_AMOUNT);
            AtomicBoolean resHasEnd = new AtomicBoolean();

            if ((r.headers().contains(CONTENT_LENGTH) || r.headers().contains(X_AMZ_DECODED_CONTENT_LENGTH)) && r.headers().contains(TRANSFER_ENCODING)) {
                dealException(r, new MsException(CONTENT_LENGTH_NOT_ALLOWED, "content-length is not allowed when using Chunked transfer encoding"));
                return;
            }

            boolean useSSL = IS_SSL_SYNC.get();
            if ((r.headers().contains(CONTENT_LENGTH)
                    && Long.parseLong(r.headers().get(CONTENT_LENGTH)) > 0)
                    || (r.headers().contains(TRANSFER_ENCODING) && "chunked".equals(r.headers().get(TRANSFER_ENCODING)))) {
                r.handler(b -> {
//                    log.info("{}, reqAmount:{}, blength:{} ", r.uri(), reqsAmount.get(), b.length());
                    if (resHasEnd.get()) {
                        return;
                    }
                    AtomicBoolean useCopy = new AtomicBoolean();
                    ByteBuf copyB = null;
                    if (useSSL) {
                        copyB = b.getByteBuf().copy();
                    }
                    ByteBuf finalCopyB = copyB;
                    reqs.forEach((ip, request) -> {
                        try {
                            Buffer buffer;
                            if (useSSL && useCopy.compareAndSet(false, true)) {
                                buffer = Buffer.buffer(finalCopyB);
                            } else {
                                buffer = Buffer.newInstance(b);
                            }

                            // 可能会有req异常被移除，但在移除前依然有数据进来，因此需要将writeTotal放在写请求体前面计数。
                            request.write(buffer, v -> {
                                // 用fetch实现限流。处理完当前fetch到的才会去fetch下一轮数据。
//                            log.info("ip{}, write:{}, fetch:{}, {}", ip, writeTotal.get(), fetchTotal.get(), waitForFetch.get());
                                if (!waitForFetch.get() && writeTotal.incrementAndGet() >= fetchTotal.get()) {
                                    r.fetch(DEFAULT_FETCH_AMOUNT);
                                    fetchTotal.addAndGet(DEFAULT_FETCH_AMOUNT * reqsAmount.get());
                                }
                            });

                            if (!waitForFetch.get() && request.writeQueueFull()) {
                                waitForFetch.set(true);
                                request.drainHandler(done -> {
                                    waitForFetch.set(false);
                                    r.fetch(DEFAULT_FETCH_AMOUNT);
                                    fetchTotal.addAndGet(DEFAULT_FETCH_AMOUNT * reqsAmount.get());
                                });
                            }
                        } catch (Exception e) {
                            log.error("{}", r.method().name() + r.uri(), e);
                        }
                    });
                });

                r.endHandler(a -> reqs.forEach((k, v) -> {
                    try {
                        if (v.getDelegate().connection() != null) {
                            synchronized (v.getDelegate().connection()) {
                                v.end();
                            }
                        }
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }));
            }

            r.exceptionHandler(e -> {
                try {
                    log.debug("origin request exception, {}", r.uri(), e);
                    for (HttpClientRequest request : totalReqs.values()) {
                        try {
                            if (request.getDelegate().connection() != null) {
                                synchronized (request.getDelegate().connection()) {
                                    request.end();
                                }
                            }
                        } catch (Exception exception) {
                            log.debug("", e);
                        }
                    }
//                    totalReqs.forEach((k, v) -> DoubleActiveUtil.closeConn(v));
                    totalReqs.forEach((ip, req) -> req = null);
                    totalReqs.clear();
                    reqs.forEach((ip, req) -> req = null);
                    reqs.clear();

                    r.connection().close();
//                    dealException(r, e);
                    processor.onComplete();
//                    streamDispose(disposables);
                } catch (Exception exception) {
                    log.error("", e);
                }
            });

            if (AuthorizeV4.CONTENT_SHA256_MULTIPLE_CHUNKS_PAYLOAD.equals(r.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256))) {
                if (r.getHeader(TRANSFER_ENCODING) == null || !r.getHeader(TRANSFER_ENCODING).contains("chunked")) {
                    String decodedContentLength = r.getHeader(X_AMZ_DECODED_CONTENT_LENGTH);
                    if (decodedContentLength == null) {
                        dealException(r, new MsException(ErrorNo.MISSING_DECODED_CONTENT_LENGTH, "You must provide the Content-Length HTTP header."));
                        return;
                    }
                    try {
                        Long.parseLong(decodedContentLength);
                    } catch (NumberFormatException e) {
                        dealException(r, new MsException(ErrorNo.MISSING_DECODED_CONTENT_LENGTH, e.getMessage()));
                        return;
                    }
                    //原来的Content-Length缓存下来
                    r.addMember(CONTENT_LENGTH, r.getHeader(CONTENT_LENGTH));
                    r.headers().set(CONTENT_LENGTH, decodedContentLength);
                }
            }
//            UnSynchronizedRecord unSynchronizedRecord = buildSyncRecord(r);

            AtomicBoolean needBuffer = new AtomicBoolean();
            Mono<UnSynchronizedRecord> recordMono = pool.getReactive(REDIS_BUCKETINFO_INDEX)
                    .hgetall(r.getBucketName())
                    .defaultIfEmpty(new HashMap<>())
                    .doOnNext(bucketInfo -> {
                        r.headers().add(ORIGIN_INDEX_CRYPTO, bucketInfo.get("crypto") == null ? "" : bucketInfo.get("crypto"));
                    })
                    .map(map -> StringUtils.isNotBlank(map.get(BUCKET_VERSION_STATUS)) ? map.get(BUCKET_VERSION_STATUS) : "NULL")
                    .zipWith(VersionUtil.getVersionNum(r.getBucketName(), r.getObjectName()))
                    .map(tuple2 -> buildSyncRecord(r, tuple2.getT1(), tuple2.getT2()));

            // 保证不同站点间分段任务初始化时间一致
            if (ERROR_INIT_PART_UPLOAD.equals(type)) {
                r.headers().add("Initiated", String.valueOf(System.currentTimeMillis()));
            }

            if (ERROR_COMPLETE_PART.equals(type) || ERROR_PART_UPLOAD.equals(type)) {
                if (r.headers().contains(X_AMZ_COPY_SOURCE)) {
                    needBuffer.set(true);
                }
                io.vertx.core.MultiMap params = RestfulVerticle.params(r.uri());
                String uploadId = params.get(URL_PARAM_UPLOADID);
                StoragePool storagePool = StoragePoolFactory.getStoragePool(StorageOperate.META, r.getBucketName());
                recordMono = recordMono.flatMap(unSynchronizedRecord ->
                        storagePool.mapToNodeInfo(storagePool.getBucketVnodeId(unSynchronizedRecord.bucket, unSynchronizedRecord.object))
                                .flatMap(nodeList -> PartClient.getInitPartInfo(unSynchronizedRecord.bucket, unSynchronizedRecord.object, uploadId, nodeList, null)
                                        .doOnNext(initPartInfo -> {
                                            if (initPartInfo.equals(InitPartInfo.ERROR_INIT_PART_INFO) || initPartInfo.equals(InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO) || initPartInfo.delete) {
                                                unSynchronizedRecord.setVersionId(null).headers.remove(VERSIONID);
                                            } else {
                                                unSynchronizedRecord.setVersionId(initPartInfo.metaData.getVersionId()).headers.put(VERSIONID, initPartInfo.metaData.getVersionId());
                                            }
                                        })
                                        .map(initPartInfo -> unSynchronizedRecord)))
                        .flatMap(record -> generateSourceSize(record, r, false));
            } else if (ERROR_PUT_BUCKET.equals(type)) {
                recordMono = recordMono.doOnNext(record -> {
                    try {
                        record.headers.put("version_num", VersionUtil.getVersionNum());
                        record.headers.put(DOUBLE_FLAG, "1");
                        record.headers.put(BUCKET_NAME, r.getBucketName());
                        r.addMember("service", AuthorizeV4.SERVICE_S3);
                        String accessKey = "";
                        String signature = "";
                        String authType = "";
                        String sign = "";

                        sign = getRequestSign(r, authType);
                        if (r.headers().contains(AUTHORIZATION)) {
                            String[] array = r.getHeader(AUTHORIZATION).split(" ");
                            authType = array[0];
                            if (authType.equals(AuthorizeFactory.AWS)) {
                                array = array[1].split(":");
                                accessKey = array[0];
                                signature = StringUtils.isBlank(accessKey) ? signature : array[1];
                            } else if (authType.equals(AuthorizeFactory.SHA256)) {
                                AuthorizeV4.V4Authorization v4Authorization = AuthorizeV4.V4Authorization.from(r);
                                signature = v4Authorization.get(AuthorizeV4.V4Authorization.SIGNATURE);
                                accessKey = v4Authorization.get(AuthorizeV4.V4Authorization.CREDENTIAL).split("/")[0];
                            } else {
                                responseError(r, ErrorNo.UNSUPPORTED_AUTHORIZATION_TYPE);
                                return;
                            }
                        } else if (r.headers().contains(X_AUTH_TOKEN)) {
                            String token = r.getHeader(X_AUTH_TOKEN);
                            if (StringUtils.isNotEmpty(token)) {
                                authType = "token";
                                signature = token;
                            } else {
                                responseError(r, ErrorNo.MISSING_SECURITY_HEADER);
                                return;
                            }
                        }

                        getAuthorizer(authType).apply(r, sign, accessKey, signature)
                                .subscribe(flag -> {
                                    record.headers.put(USER_ID, r.getUserId());
                                    record.headers.put("ak", r.getAccessKey());
                                    pool.getReactive(REDIS_USERINFO_INDEX)
                                            .hget(r.getAccessKey(), "secret_key")
                                            .subscribe(sk -> {
                                                record.headers.put("sk", sk);
                                                insertSharedLog(record.headers, "deleteBucket");
                                            }, e -> dealException(r, e));
                                }, e -> dealException(r, e));
                    } catch (MsException e) {
                        responseError(r, e.getErrCode());
                    } catch (Exception e) {
                        responseError(r, UNKNOWN_ERROR);
                    }
                });
            } else if (!type.useCurrStamp) {
//                recordMono = Mono.just(unSynchronizedRecord);
            } else if (ERROR_PUT_OBJECT.equals(type) && !r.headers().contains(X_AMZ_COPY_SOURCE)) {
//                recordMono = Mono.just(unSynchronizedRecord);
            } else if (ERROR_PUT_OBJECT.equals(type) && r.headers().contains(X_AMZ_COPY_SOURCE)) {
                needBuffer.set(true);
                recordMono = recordMono.flatMap(record -> generateSourceSize(record, r, false)
                        .doOnNext(record1 -> record.headers.put(SYNC_COPY_PART_UPLOADID, RandomStringUtils.randomAlphabetic(32))));
            } else {
                StoragePool storagePool = StoragePoolFactory.getStoragePool(StorageOperate.META, r.getBucketName());
                recordMono = recordMono.flatMap(unSynchronizedRecord ->
                        storagePool.mapToNodeInfo(storagePool.getBucketVnodeId(r.getBucketName(), r.getObjectName()))
                                .flatMap(bucketVnodeList -> ErasureClient.getObjectMetaVersion(r.getBucketName(), r.getObjectName(), unSynchronizedRecord.versionId, bucketVnodeList, r))
                                .map(metaData -> {
                                    if (metaData.isAvailable()) {
                                        String syncStamp = unSynchronizedRecord.headers.get(SYNC_STAMP);
                                        if (type != ERROR_DELETE_OBJECT) {
                                            unSynchronizedRecord.headers.put(VERSIONID, metaData.versionId);
                                        }
                                        if (ERROR_PUT_OBJECT.equals(type)) {
                                            unSynchronizedRecord.setLastStamp(metaData.syncStamp);
                                        } else {
                                            unSynchronizedRecord.setSyncStamp(metaData.syncStamp);
                                            unSynchronizedRecord.headers.put(SYNC_STAMP, metaData.syncStamp);
                                        }
                                        if (ERROR_DELETE_OBJECT.equals(type) && StringUtils.isNotEmpty(bucketTrash.get(r.getBucketName()))) {
                                            unSynchronizedRecord.headers.put(CONTENT_LENGTH, String.valueOf(metaData.endIndex - metaData.startIndex + 1));
                                            unSynchronizedRecord.setLastStamp(metaData.syncStamp);
                                            if (unSynchronizedRecord.headers.containsKey("recoverObject")) {
                                                unSynchronizedRecord.setSyncStamp(syncStamp);
                                                unSynchronizedRecord.headers.put(SYNC_STAMP, syncStamp);
                                            }
                                        }
                                    }
                                    return unSynchronizedRecord;
                                }));
            }

            final boolean noRecord = ERROR_PUT_BUCKET.equals(type);
            final String[] preRecordKey = new String[1];
            //预提交同步Log
            Mono<UnSynchronizedRecord> finalRecordMono = recordMono;
            Disposable subscribe = WormUtils.currentWormTimeMillis().flatMap(wormTime -> {
                r.headers().add(WORM_STAMP, String.valueOf(wormTime));
                return finalRecordMono;
            }).flatMap(record -> {
                preRecordKey[0] = record.rocksKey();
                r.headers().add(SYNC_RECORD_HEADER, record.rocksKey());
                if (noRecord) {
                    return Mono.just(true).zipWith(Mono.just(record));
                }
                StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(record.bucket);
                String bucketVnode = storagePool.getBucketVnodeId(record.bucket);
                return storagePool.mapToNodeInfo(bucketVnode)
                        // 避免过早拒绝客户端请求，去掉timeout,暂无问题
                        .flatMap(nodeList -> ECUtils.putSynchronizedRecord(storagePool, preRecordKey[0], Json.encode(record), nodeList, WRITE_ASYNC_RECORD, r))
                        .zipWith(Mono.just(record));
            }).subscribe(btuple2 -> {
                Boolean b = btuple2.getT1();
                UnSynchronizedRecord unSynchronizedRecord = btuple2.getT2();

                if (!b) {
                    dealException(r, new MsException(UNKNOWN_ERROR, "write pre sync record fail!"));
                    return;
                }
                //向每个站点发送http请求。本站点的请求发送往本节点。其他站点的请求随机发送到一个下属节点。
                HttpResponseSummary responseSummary = new HttpResponseSummary(processor);
                redirectRequest(r, reqs, processor, unSynchronizedRecord.syncStamp, unSynchronizedRecord.headers, reqsAmount, totalReqs, fetchTotal, writeTotal, waitForFetch, responseSummary,
                        needBuffer);

                AtomicLong i = new AtomicLong();
                Disposable subscribe1 = responseSummary.responses.subscribe(tuple2 -> {
                            if (i.incrementAndGet() == NODE_AMOUNT) {
                                processor.onComplete();
                            }
                        }, e -> log.error("Active-Active sync request error", e),
                        () -> {
                            resHasEnd.set(true);
                            //r已经异常关闭。
                            if (r.response().ended() || r.response().closed()) {
                                log.info("close every requests");
                                totalReqs.forEach((ip, request) -> closeConn(request));
//                                streamDispose(disposables);
                                return;
                            }

                            //至少成功一个集群时，给用户返回成功；
                            //全部失败，返回本端的错误消息。
                            try {
//                                HttpClientResponse aliveResponse = responseSummary.successResponse;
//                                log.info("success, {} ,statuscode: {}, {}", aliveResponse.request().getHost() + aliveResponse.request().uri(), aliveResponse.statusCode(),
//                                 re       aliveResponse.statusMessage());
                                if (responseSummary.successAmount >= 1) {
                                    //存在站点写入失败，且站点连接未显示断开，写入同步record（站点断开在请求开始前写入）
                                    if (responseSummary.successAmount != NODE_AMOUNT) {
                                        succOneCount.incrementAndGet();
                                        //修改log状态
                                        unSynchronizedRecord
                                                .setIndex(getErrorIp(responseSummary))
                                                .setSuccessIndex(getSuccessIp(responseSummary))
                                                .setCommited(true);

                                        if (!ASYNC_INDEX_IPS_ENTIRE_MAP.isEmpty()) {
                                            Set<Integer> successSet = new HashSet<>();
                                            successSet.add(unSynchronizedRecord.successIndex);
                                            unSynchronizedRecord.setSuccessIndexSet(successSet);
                                        }
                                        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(unSynchronizedRecord.bucket);
                                        String bucketVnode = storagePool.getBucketVnodeId(unSynchronizedRecord.bucket);
                                        Disposable subscribe2 = storagePool.mapToNodeInfo(bucketVnode)
//                                                    .publishOn(SCAN_SCHEDULER)
                                                .flatMap(nodeList -> noRecord ? Mono.just(1) : ECUtils.updateSyncRecord(unSynchronizedRecord, nodeList, WRITE_ASYNC_RECORD, r))
                                                .map(resInt -> resInt != 0)
                                                .timeout(Duration.ofSeconds(10))
                                                .subscribe(res -> {
                                                    if (!res) {
                                                        log.error("commit sync record error!" + unSynchronizedRecord);
                                                    }
                                                    if (noRecord) {
                                                        deleteBySyncAction(unSynchronizedRecord.headers);
                                                    }
                                                    // 两个站点都成功才会返回，否则挂起
                                                    if (responseSummary.localResponse != null && responseSummary.localResponse.headers().contains(IFF_TAG)) {
                                                        Buffer b1 = responseSummary.respBuffers.get(responseSummary.localResponse);
                                                        if (needBuffer.get() && b1 != null) {
                                                            dealResponse(r, responseSummary.localResponse, b1.slice(xmlHeader.length, b1.length()));
                                                            return;
                                                        }
                                                        responseSummary.localResponse.bodyHandler(buffer -> dealResponse(r, responseSummary.localResponse, buffer)).resume();
                                                    } else {
                                                        if (getSuccessIp(responseSummary).equals(Arbitrator.MASTER_INDEX)) {
//                                                            responseSummary.successResponse.bodyHandler(buffer -> dealResponse(r, responseSummary.successResponse, buffer)).resume();
                                                        } else {
                                                            if (responseSummary.successResponse.headers().contains(IFF_TAG)) {
                                                                Buffer b1 = responseSummary.respBuffers.get(responseSummary.successResponse);
                                                                if (needBuffer.get() && b1 != null) {
                                                                    dealResponse(r, responseSummary.successResponse, b1.slice(xmlHeader.length, b1.length()));
                                                                    return;
                                                                }
                                                                responseSummary.successResponse.bodyHandler(buffer -> dealResponse(r, responseSummary.successResponse, buffer)).resume();
                                                            } else {
//                                                                dealException(r, new MsException(UNKNOWN_ERROR, "master index has not return success. "));
                                                            }
                                                        }
                                                    }
                                                }, e -> {
                                                    log.error("commit sync record error", e);
                                                    // 更新预提交记录未成功，仍返回响应
                                                    if (getSuccessIp(responseSummary).equals(Arbitrator.MASTER_INDEX)) {
//                                                        responseSummary.successResponse.bodyHandler(buffer -> {
//                                                            dealResponse(r, responseSummary.successResponse, buffer);
//                                                        }).resume();
                                                    } else {
                                                        if (responseSummary.successResponse.headers().contains(IFF_TAG)) {
                                                            Buffer b1 = responseSummary.respBuffers.get(responseSummary.successResponse);
                                                            if (needBuffer.get() && b1 != null) {
                                                                dealResponse(r, responseSummary.successResponse, b1.slice(xmlHeader.length, b1.length()));
                                                                return;
                                                            }
                                                            responseSummary.successResponse.bodyHandler(buffer -> dealResponse(r, responseSummary.successResponse, buffer)).resume();
                                                        } else {
//                                                            dealException(r, new MsException(UNKNOWN_ERROR, "master index has not return success. "));
                                                        }
                                                    }
                                                });
                                        r.addResponseCloseHandler(v -> subscribe2.dispose());
                                    } else {
                                        succTwoCount.incrementAndGet();
                                        unSynchronizedRecord
                                                .setSuccessIndex(getSuccessIp(responseSummary))
                                                .setCommited(true);
                                        if (!ASYNC_INDEX_IPS_ENTIRE_MAP.isEmpty()) {
                                            Set<Integer> successSet = new HashSet<>();
                                            successSet.add(getOtherSiteIndex());
                                            successSet.add(LOCAL_CLUSTER_INDEX);
                                            unSynchronizedRecord.setSuccessIndexSet(successSet);
                                        }
                                        if (noRecord) {
                                            deleteShareLog(unSynchronizedRecord.headers, "deleteBucket")
                                                    .subscribe(d -> {
                                                        if (d) {
                                                            responseSummary.successResponse.bodyHandler(buffer -> {
                                                                dealResponse(r, responseSummary.successResponse, buffer);
                                                            }).resume();
                                                        }
                                                    });
                                        } else {
                                            // 如果r先结束，释放了rsocket流，服务端的rocksDB操作可能不被进行
                                            Disposable subscribe2 = deleteUnsyncRecord(r.getBucketName(), preRecordKey[0], unSynchronizedRecord, UPDATE_ASYNC_RECORD, r, noRecord)
                                                    .subscribe(res -> {
                                                        Buffer b1 = responseSummary.respBuffers.get(responseSummary.successResponse);
                                                        if (needBuffer.get() && b1 != null) {
                                                            dealResponse(r, responseSummary.successResponse, b1.slice(xmlHeader.length, b1.length()));
                                                            return;
                                                        }
                                                        responseSummary.successResponse.bodyHandler(buffer -> {
                                                            dealResponse(r, responseSummary.successResponse, buffer);
                                                        }).resume();
                                                    }, e -> {
                                                        log.error("del sync record error", e);
                                                        Buffer b1 = responseSummary.respBuffers.get(responseSummary.successResponse);
                                                        if (needBuffer.get() && b1 != null) {
                                                            dealResponse(r, responseSummary.successResponse, b1.slice(xmlHeader.length, b1.length()));
                                                            return;
                                                        }
                                                        responseSummary.successResponse.bodyHandler(buffer -> {
                                                            dealResponse(r, responseSummary.successResponse, buffer);
                                                        }).resume();
                                                    });
                                            r.addResponseCloseHandler(v -> subscribe2.dispose());
                                        }
                                    }

                                    totalReqs.forEach((ip, req) -> req = null);
                                    totalReqs.clear();
                                    reqs.forEach((ip, req) -> req = null);

                                } else {
//                                    log.info("{} fail, {}", r.uri(), responseSummary);
                                    failTwoCount.incrementAndGet();
                                    if (noRecord) {
                                        deleteShareLog(unSynchronizedRecord.headers, "deleteBucket")
                                                .subscribe(d -> {
                                                    if (d) {
                                                        if (responseSummary.localResponse == null) {
                                                            dealException(r, new MsException(UNKNOWN_ERROR, "no sync req can be sent. " + r.uri()));
                                                        } else {
                                                            dealErrResponse(r, responseSummary, needBuffer);
                                                        }
                                                    }
                                                });
                                    }
                                    if (responseSummary.localResponse == null) {
                                        dealException(r, new MsException(UNKNOWN_ERROR, "no sync req can be sent. " + r.uri()));
                                    } else {
                                        if (responseSummary.errorNum.get() == 0) {
//                                            Disposable subscribe2 = deleteUnsyncRecord(r.getBucketName(), preRecordKey[0], null, DELETE_ASYNC_RECORD, r, noRecord)
//                                                    .subscribe(res -> dealErrResponse(r, responseSummary, needBuffer), e -> log.error("", e));
//                                            r.addResponseCloseHandler(v -> subscribe2.dispose());
                                            dealErrResponse(r, responseSummary, needBuffer);
                                        } else {
                                            dealErrResponse(r, responseSummary, needBuffer);
                                        }
                                    }
                                    totalReqs.forEach((ip, req) -> req = null);
                                    totalReqs.clear();
                                    reqs.forEach((ip, req) -> req = null);
                                }
                                reqs.clear();

                            } catch (Exception e) {
                                log.error("", e);
                                dealException(r, e);
                            }
                        });
                r.addResponseCloseHandler(v -> subscribe1.dispose());
            }, e -> {
                dealException(r, e);
//                streamDispose(disposables);
            });
            r.addResponseCloseHandler(v -> subscribe.dispose());

//            r.addResponseCloseHandler(e -> {
////                Optional.ofNullable(processor).ifPresent(FluxProcessor::dispose);
////                streamDispose(disposables);
//                totalReqs.forEach((ip, req) -> req = null);
//                totalReqs.clear();
//                reqs.forEach((ip, req) -> req = null);
//                reqs.clear();
//            });


        } catch (Exception e) {
            log.error("send error", e);
            dealException(r, e);
        }
    }

    /**
     * 解析copy-source
     * 带versionId时：x-amz-copy-source:/bucket/key?versionId=v
     * 不带versionId时：x-amz-copy-source:/bucket/key
     *
     * @param sourceKey copy-source
     * @return bucket/key/versionId
     */
    private static String[] parseSourceKey(String sourceKey) {
        String xAmzCopySource;
        try {
            xAmzCopySource = URLDecoder.decode(sourceKey, "UTF-8");
            if (xAmzCopySource.startsWith(SLASH)) {
                xAmzCopySource = xAmzCopySource.substring(1);
            }
        } catch (UnsupportedEncodingException e) {
            log.error("", e);
            return new String[]{};
        }
        String[] array = xAmzCopySource.split("/", 2);
        if (array.length < 2) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "The format of x-amz-copy-source is incorrect!");
        }
        String[] arr2 = array[1].split("\\?", 2);
        String sourceBucket = array[0];
        String sourceObject = arr2[0];
        String sourceVersionId = arr2.length == 1 ? "" : arr2[1].substring(arr2[1].indexOf("=") + 1);
        return new String[]{sourceBucket, sourceObject, sourceVersionId};

    }

    /**
     * 设置copy对象record同步时的Content-length，
     * 若当前站点源对象存在，则当前对象的大小即为Content-length
     * 若当前站点源对象不存在，则获取另一个双活站点对象的大小为Content-length，不存在时设为0，其他情况下Content-length按最大值5G
     *
     * @param record       差异记录
     * @param r            请求
     * @param asyncRequest 请求类型 双活或者复制，复制则仅获取当前站点的对象大小为Content-length
     * @return 生成新的record
     */
    static Mono<UnSynchronizedRecord> generateSourceSize(UnSynchronizedRecord record, MsHttpRequest r, boolean asyncRequest) {
        if (!record.headers.containsKey(COPY_SOURCE_KEY)) {
            return Mono.just(record);
        }
        String[] array = parseSourceKey(r.headers().get(X_AMZ_COPY_SOURCE));
        String sourceBucket = ServerConfig.isBucketUpper() ? array[0].toLowerCase() : array[0];
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(sourceBucket);
        String sourceObject = array[1];
        String sourceVersionId = array[2];
        return storagePool.mapToNodeInfo(storagePool.getBucketVnodeId(sourceBucket, sourceObject))
                .flatMap(bucketVnodeList -> ErasureClient.getObjectMetaVersion(sourceBucket, sourceObject, sourceVersionId, bucketVnodeList, r))
                .flatMap(metaData -> {
                    long contentLength = MAX_COPY_LIMIT_SIZE;
                    if (r.headers().contains(X_AMZ_COPY_SOURCE_RANGE)) {
                        String range = r.getHeader(X_AMZ_COPY_SOURCE_RANGE);
                        String[] rangeArray = range.split("=");
                        if (!"bytes".equals(rangeArray[0]) || rangeArray.length != 2) {
                            throw new MsException(ErrorNo.INVALID_ARGUMENT, "The x-amz-copy-source-range value must be of the " +
                                    "form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy");
                        }
                        rangeArray = rangeArray[1].split("-");
                        if (rangeArray.length != 2 || StringUtils.isEmpty(rangeArray[0])) {
                            throw new MsException(ErrorNo.INVALID_ARGUMENT, "The x-amz-copy-source-range value must be of the " +
                                    "form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy");
                        }
                        long startIndex = Long.parseLong(rangeArray[0]);
                        long endIndex = Long.parseLong(rangeArray[1]);
                        contentLength = endIndex - startIndex + 1;
                    }
                    if (metaData.isAvailable()) {
                        record.setLastStamp(metaData.syncStamp);

                        long total = metaData.endIndex - metaData.startIndex + 1;
                        contentLength = metaData.endIndex + 1;
                        if (r.headers().contains(X_AMZ_COPY_SOURCE_RANGE)) {
                            String range = r.getHeader(X_AMZ_COPY_SOURCE_RANGE);
                            LongLongTuple t = dealCopyRange(range, total);
                            contentLength = t.var2 - t.var1 + 1;
                        }
                        if (StringUtils.isEmpty(sourceVersionId)) {
                            r.headers().set(SYNC_COPY_SOURCE, record.headers.get(X_AMZ_COPY_SOURCE) + "?versionId=" + metaData.versionId);
                        }
                        return Mono.just(contentLength);
                    } else {
                        if (asyncRequest || MetaData.ERROR_META.equals(metaData)) {
                            return Mono.just(contentLength);
                        }

                        String uri = UrlEncoder.encode(File.separator + sourceBucket + File.separator + sourceObject, "UTF-8");
                        if (StringUtils.isNotEmpty(sourceVersionId)) {
                            uri = uri + "?versionId=" + StringUtils.isNotEmpty(sourceVersionId);
                        }
                        Map<String, String> headerMap = new HashMap<>();
                        headerMap.put(IS_SYNCING, "1");
                        headerMap.put(AUTHORIZATION, record.headers.get(AUTHORIZATION));
                        long finalContentLength = contentLength;
                        return MossHttpClient.getInstance().sendRequest(getOtherSiteIndex(), sourceBucket, sourceObject, uri, HttpMethod.HEAD, headerMap, null)
                                .flatMap(tuple3 -> {
                                    if (tuple3.var1 == SUCCESS) {
                                        r.headers().set(SYNC_COPY_SOURCE, record.headers.get(X_AMZ_COPY_SOURCE) + "?versionId=" + tuple3.var3.get(X_AMX_VERSION_ID));
                                        if (r.headers().contains(X_AMZ_COPY_SOURCE_RANGE)) {
                                            String range = r.getHeader(X_AMZ_COPY_SOURCE_RANGE);
                                            LongLongTuple t = dealCopyRange(range, Long.valueOf(tuple3.var3.get(CONTENT_LENGTH)));
                                            return Mono.just(t.var2 - t.var1 + 1);
                                        }
                                        return Mono.just(Long.valueOf(tuple3.var3.get(CONTENT_LENGTH)));
                                    } else if (tuple3.var1 == NOT_FOUND || tuple3.var1 == NOT_ALLOWED) {
                                        return Mono.just(0L);
                                    }
                                    return Mono.just(finalContentLength);
                                });
                    }
                }).map(contentLength -> {
                    record.headers.put(CONTENT_LENGTH, String.valueOf(contentLength));
                    return record;
                });
    }

    static Mono<UnSynchronizedRecord> addRecordObjectSize(UnSynchronizedRecord record, MsHttpRequest request) {
        String bucketName = record.bucket;
        String objectName = record.object;
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);

        return storagePool.mapToNodeInfo(storagePool.getBucketVnodeId(bucketName, objectName))
                .flatMap(bucketVnodeList -> ErasureClient.getObjectMetaVersion(bucketName, objectName, record.versionId, bucketVnodeList, request))
                .flatMap(metaData -> {
                    long size = metaData.endIndex - metaData.startIndex + 1;
                    record.headers.put(CONTENT_LENGTH, String.valueOf(size));
                    return Mono.just(record);
                });
    }

    public static AtomicLong succTwoCount = new AtomicLong();

    public static AtomicLong succOneCount = new AtomicLong();

    public static AtomicLong failTwoCount = new AtomicLong();

    /**
     * 根据response获取失败站点的索引
     */
    private static Integer getErrorIp(HttpResponseSummary responseSummary) {
        if (StringUtils.isNotEmpty(responseSummary.errorIp)) {
            if (LOCAL_IP_ADDRESS.equals(responseSummary.errorIp)) {
                return LOCAL_CLUSTER_INDEX;
            } else {
                return IP_INDEX_MAP.get(responseSummary.errorIp);
            }
        } else {
            if (LOCAL_IP_ADDRESS.equals(responseSummary.successIp)
                    || LOCAL_CLUSTER_INDEX.equals(IP_INDEX_MAP.get(responseSummary.successIp))) {
                return getOtherSiteIndex();
            } else {
                return LOCAL_CLUSTER_INDEX;
            }
        }
    }

    final byte[] xmlHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>".getBytes();

    /**
     * 根据response获取成功站点的索引
     */
    private static Integer getSuccessIp(HttpResponseSummary responseSummary) {
        if (LOCAL_IP_ADDRESS.equals(responseSummary.successIp)) {
            return LOCAL_CLUSTER_INDEX;
        } else {
            return IP_INDEX_MAP.get(responseSummary.successIp);
        }
    }

    /**
     * 将请求向主从站点同时转发，连接到链路不通节点，转换节点进行重试发送
     */
    private void redirectRequest(MsHttpRequest r, Map<String, HttpClientRequest> reqs,
                                 UnicastProcessor<Tuple2<String, HttpClientResponse>> processor,
                                 String syncStamp, Map<String, String> specialHeaders, AtomicInteger reqsAmount,
                                 Map<String, HttpClientRequest> totalReqs, AtomicLong fetchTotal, AtomicLong writeTotal,
                                 AtomicBoolean waitForFetch, HttpResponseSummary responseSummary,
                                 AtomicBoolean needBuffer) {
        // 没有连接问题
        AtomicInteger responseCount = new AtomicInteger();
        // 连接中断出错或者返回不为200的请求个数
        AtomicInteger exceptionCount = new AtomicInteger();
        // 有站点A的put请求已经返回，站点B还未响应continue，此时应由站点B request的continueHandler触发resume。
        AtomicBoolean hasContinue = new AtomicBoolean();

        Map<Integer, String[]> map = new HashMap<>(NODE_AMOUNT);
        DA_INDEX_IPS_MAP.forEach(map::put);

        // 保证一次客户端请求只调用一次resumeR
        AtomicBoolean startResume = new AtomicBoolean();
        AtomicBoolean isFirst = new AtomicBoolean(true);
        r.headers().remove(MULTI_SYNC);
        //向每个站点发送http请求。本站点的请求发送往本节点。其他站点的请求随机发送到一个下属节点。
        map.forEach((index, ips) -> {
            int[] ipIndex = new int[]{ThreadLocalRandom.current().nextInt(ips.length)};
            UnicastProcessor<Integer> retryProcessor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
            // 已取过的ip在ips的索引
            Set<Integer> indexSet = new ConcurrentHashSet<>();

            AtomicInteger tryNum = new AtomicInteger();
            String[] tempIp = new String[1];
            // 加pulishon会造成很多连接关闭。
            Disposable subscribe = retryProcessor.subscribe(i -> {
                indexSet.add(ipIndex[0]);
                if (tryNum.get() > 0) {
                    // 在剩下的ip中随机获取一个
                    int length = ips.length - tryNum.get() + 1;
                    int[] restIndexes = new int[length];
                    int b = 0;
                    for (int a = 0; a < ips.length; a++) {
                        if (indexSet.contains(a)) {
                            continue;
                        }
                        restIndexes[b++] = a;
                    }
                    int nexInt = ThreadLocalRandom.current().nextInt(length);
                    ipIndex[0] = restIndexes[nexInt];
                }
                String ip = LOCAL_CLUSTER_INDEX.equals(index) ? LOCAL_IP_ADDRESS : ips[ipIndex[0]];
                tempIp[0] = ip;
                if (r.response().ended() || r.response().closed()) {
                    exceptionCount.incrementAndGet();
                    processor.onNext(new Tuple2<>(ip, null));
                    if (responseCount.incrementAndGet() >= NODE_AMOUNT) {
                        //向各个节点分发的请求都有了返回或出错，如果此时并非所有节点都出错，则开始数据流的传输
                        if (exceptionCount.get() < NODE_AMOUNT && startResume.compareAndSet(false, true)) {
                            reqsAmount.set(NODE_AMOUNT - exceptionCount.get());
                            resumeR(r, reqsAmount.get() > 0);
                        }
                    }
                    retryProcessor.onComplete();
                    return;
                }

                AtomicBoolean hasExc = new AtomicBoolean(false);
                HttpClientRequest request = getFrontClient().request(r.method(), DA_PORT, ip, r.uri());
                request.setWriteQueueMaxSize(WriteQueueMaxSize)
                        .setTimeout(10_000);
                reqs.put(ip, request);
                totalReqs.put(ip + "-" + i, request);
                // 该请求有没有进入continueHandler
                AtomicBoolean singleContinue = new AtomicBoolean();
                try {
                    request.setHost(ip + ":" + DA_PORT).exceptionHandler(e -> {
                        if (!hasExc.compareAndSet(false, true)) {
                            return;
                        }
                        reqs.remove(ip);
                        totalReqs.remove(ip + "-" + i);
                        log.debug("send {} request to {} error1! {}, {}, {}", request.method().name(), request.getHost() + request.uri(), e.getClass().getName(),
                                e.getMessage(),
                                startResume.get());
                        // 似乎加上后出现大量异常request的情况下httpstream回收变快
                        request.reset();
                        log.debug("{}, {}, {}, {}, {}, {}, {}, {}, {}",
                                request.method().name() + request.getHost() + request.uri(),
                                tryNum.get(),
                                reqsAmount,
                                exceptionCount.get(),
                                startResume.get(),
                                reqs.size(),
                                responseCount.get(),
                                writeTotal.get(),
                                fetchTotal.get());

                        // 如果已经开始数据传输途中出现了连接问题，也不必重试了。
                        if (r.response().ended() || r.response().closed() || startResume.get()) {
                            exceptionCount.incrementAndGet();
                            // 双活连接正常数量大于0，需要客户端继续传数据。
                            if (reqsAmount.decrementAndGet() > 0) {
                                // 该连接关闭前可能fetchTotal已经加上了它这份DEFAULT_FETCH_AMOUNT，需要减去。
                                fetchTotal.addAndGet(-DEFAULT_FETCH_AMOUNT);
                                // 传输过程中的连接异常会导致该连接的writeQueue被写满。正常的连接因为waitForFetch也无法走到fetch
                                waitForFetch.set(false);
                                if (startResume.get()) {
                                    r.fetch(1L);
                                }
                            }
                            processor.onNext(new Tuple2<>(ip, null));
                            retryProcessor.onComplete();
                            return;
                        }


                        if (tryNum.incrementAndGet() == ips.length) {
                            exceptionCount.incrementAndGet();
                            processor.onNext(new Tuple2<>(ip, null));
                            if (responseCount.incrementAndGet() >= NODE_AMOUNT) {
                                //向各个节点分发的请求都有了返回或出错，如果此时并非所有节点都出错，则开始数据流的传输
                                if (exceptionCount.get() < NODE_AMOUNT && startResume.compareAndSet(false, true)) {
                                    reqsAmount.set(NODE_AMOUNT - exceptionCount.get());
                                    resumeR(r, reqsAmount.get() > 0);
                                }
                            }
                            retryProcessor.onComplete();
                        } else {
                            retryProcessor.onNext(tryNum.get());
                        }
                    }).continueHandler(v -> {
                        hasContinue.compareAndSet(false, true);
                        singleContinue.compareAndSet(false, true);
//                        log.info("continuehandler {} count {}", request.getHost() + request.uri(), responseCount.get());
                        if (responseCount.incrementAndGet() >= NODE_AMOUNT && startResume.compareAndSet(false, true)) {
                            reqsAmount.set(NODE_AMOUNT - exceptionCount.get());
                            resumeR(r, reqsAmount.get() > 0);
                        }
                    }).handler(response -> {
                        boolean respSucc = response.statusCode() == SUCCESS || response.statusCode() == DEL_SUCCESS;
                        if (needBuffer.get() && respSucc) {
                            response.handler(b -> {
                                try {
                                    responseSummary.respBuffers.computeIfAbsent(response, k -> Buffer.buffer()).appendBuffer(b);
                                    if (isFirst.compareAndSet(true, false)) {
                                        r.response()
                                                .putHeader(CONTENT_TYPE, "application/xml")
                                                .putHeader(TRANSFER_ENCODING, "chunked")
                                                .putHeader(SERVER, "MOSS")
                                                .putHeader(ACCESS_CONTROL_HEADERS, "*")
                                                .putHeader(ACCESS_CONTROL_ORIGIN, "*")
                                                .putHeader(DATE, nowToGMT())
                                                .write(io.vertx.core.buffer.Buffer.buffer(xmlHeader));
                                    }
                                    if (r.response().ended() || r.response().closed()) {
                                        exceptionCount.incrementAndGet();
                                        processor.onNext(new Tuple2<>(ip, null));
                                        if (responseCount.incrementAndGet() >= NODE_AMOUNT) {
                                            //向各个节点分发的请求都有了返回或出错，如果此时并非所有节点都出错，则开始数据流的传输
                                            if (exceptionCount.get() < NODE_AMOUNT && startResume.compareAndSet(false, true)) {
                                                reqsAmount.set(NODE_AMOUNT - exceptionCount.get());
                                                resumeR(r, reqsAmount.get() > 0);
                                            }
                                        }
                                        retryProcessor.onComplete();
                                        return;
                                    }
                                    r.response().write("\n");
                                } catch (Exception e) {
                                    log.error("response handler err, {}", request.uri(), e);
                                }
                            }).exceptionHandler(e -> {
                                log.debug("response err, host {} response status {} respCount {} excCount {} reqsAmout {} auth {}",
                                        request.getHost() + request.uri(), response.statusCode(), responseCount.get(), exceptionCount.get(), reqsAmount.get(), r.getHeader(AUTHORIZATION));
                                exceptionCount.incrementAndGet();
                                processor.onNext(new Tuple2<>(ip, null));
                                if (responseCount.incrementAndGet() >= NODE_AMOUNT) {
                                    //向各个节点分发的请求都有了返回或出错，如果此时并非所有节点都出错，则开始数据流的传输
                                    if (exceptionCount.get() < NODE_AMOUNT && startResume.compareAndSet(false, true)) {
                                        reqsAmount.set(NODE_AMOUNT - exceptionCount.get());
                                        resumeR(r, reqsAmount.get() > 0);
                                    }
                                }
                                retryProcessor.onComplete();
                            }).endHandler(v -> {
                                // A站点返回如404、500的响应，B站点可能可以正常。此时需要这个handler能够触发resume。
                                if (respSucc) {
                                    exceptionCount.incrementAndGet();
                                    reqs.remove(ip);
                                    if (reqsAmount.decrementAndGet() > 0) {
                                        fetchTotal.addAndGet(-DEFAULT_FETCH_AMOUNT);
                                        waitForFetch.set(false);
                                        if (startResume.get()) {
                                            r.fetch(1L);
                                        }
                                    }
                                    hasContinue.compareAndSet(true, false);
                                }
                                // 本次请求走过了continueHandler, 会记两次responseCount，需要-1。
                                if (singleContinue.get()) {
                                    responseCount.decrementAndGet();
                                }

                                // 有可能一个请求走了continue和handler，就会开始resume，此时有可能另一个请求还没到continue
                                if (responseCount.incrementAndGet() >= NODE_AMOUNT && !hasContinue.get() && startResume.compareAndSet(false, true)) {
                                    reqsAmount.set(NODE_AMOUNT - exceptionCount.get());
                                    resumeR(r, reqsAmount.get() > 0);
                                }
                                log.debug("get response1 {} {} response status {} respCount {} excCount {} reqsAmout {} auth {}", request.method().name(),
                                        request.getHost() + request.uri(), response.statusCode(), responseCount.get(), exceptionCount.get(), reqsAmount.get(), r.getHeader(AUTHORIZATION));
                                processor.onNext(new Tuple2<>(ip, response));
                                retryProcessor.onComplete();
                            });
                            return;
                        }

                        response.pause();
                        // A站点返回如404、500的响应，B站点可能可以正常。此时需要这个handler能够触发resume。
                        if (respSucc) {
                            exceptionCount.incrementAndGet();
                            reqs.remove(ip);
                            if (reqsAmount.decrementAndGet() > 0) {
                                fetchTotal.addAndGet(-DEFAULT_FETCH_AMOUNT);
                                waitForFetch.set(false);
                                if (startResume.get()) {
                                    r.fetch(1L);
                                }
                            }
                            hasContinue.compareAndSet(true, false);
                        }
                        // 本次请求走过了continueHandler, 会记两次responseCount，需要-1。
                        if (singleContinue.get()) {
                            responseCount.decrementAndGet();
                        }

                        // 有可能一个请求走了continue和handler，就会开始resume，此时有可能另一个请求还没到continue
                        if (responseCount.incrementAndGet() >= NODE_AMOUNT && !hasContinue.get() && startResume.compareAndSet(false, true)) {
                            reqsAmount.set(NODE_AMOUNT - exceptionCount.get());
                            resumeR(r, reqsAmount.get() > 0);
                        }
                        log.debug("get response2 {} {} response status {} respCount {} excCount {} reqsAmout {} auth {}", request.method().name(),
                                request.getHost() + request.uri(), response.statusCode(), responseCount.get(), exceptionCount.get(), reqsAmount.get(), r.getHeader(AUTHORIZATION));
                        processor.onNext(new Tuple2<>(ip, response));
                        retryProcessor.onComplete();
                    });

                    r.headers().forEach(header -> request.putHeader(header.getKey(), header.getValue()));
                    specialHeaders.forEach(request::putHeader);
                    if (request.method() == HttpMethod.DELETE && !r.headers().contains(CONTENT_LENGTH)) {
                        request.headers().remove(CONTENT_LENGTH);
                    }
                    if (r.headers().contains(X_AMZ_COPY_SOURCE)) {
                        request.putHeader(CONTENT_LENGTH, "0");
                    }
                    encodeAmzHeaders(request);
                    request.putHeader(SYNC_STAMP, syncStamp);
                    request.putHeader(CLUSTER_ALIVE_HEADER, ip);
                    if ((r.headers().contains(CONTENT_LENGTH)
                            && Long.parseLong(r.headers().get(CONTENT_LENGTH)) > 0)
                            || (r.headers().contains(TRANSFER_ENCODING) && "chunked".equals(r.headers().get(TRANSFER_ENCODING)))) {
                        request.putHeader(EXPECT, EXPECT_100_CONTINUE);
                        if (r.getMember(CONTENT_LENGTH) != null) {
                            request.putHeader(CONTENT_LENGTH, r.getMember(CONTENT_LENGTH));
                        }
                        if (r.headers().contains(TRANSFER_ENCODING) && "chunked".equals(r.headers().get(TRANSFER_ENCODING))) {
                            request.setChunked(true);
                        }
                        request.sendHead();
                    } else {
                        // v4上传0字节对象，需要使用request.end()避免连接无法关闭，此时request不能有请求体，但直接设置content-length=0会有签名问题。
                        if (r.getMember(CONTENT_LENGTH) != null && Long.parseLong(r.getMember(CONTENT_LENGTH)) > 0) {
                            request.putHeader(SYNC_V4_CONTENT_LENGTH, r.getMember(CONTENT_LENGTH));
                        }
                        if (needBuffer.get()) {
                            request.sendHead();
                        } else {
                            request.end();
                        }
                    }
                } catch (Exception e) {
                    log.error("send {} request to {} error2! {}, {}", request.method().name(), request.getHost(), e.getClass().getName(), e.getMessage());
                    dealException(r, e);
                }
            }, e -> {
                log.error("redirect error, ip: {}", tempIp[0], e);
//                dealException(r, e);
                processor.onNext(new Tuple2<>(tempIp[0], null));
                retryProcessor.onComplete();

            });
            r.addResponseCloseHandler(s -> Optional.ofNullable(subscribe).ifPresent(Disposable::dispose));

            retryProcessor.onNext(1);
        });
    }

    /**
     * 客户端请求恢复传输数据。
     *
     * @param r            客户端请求
     * @param readyToFetch 是否需要使用fetch。一般带content_length的客户端请求都使用fetch，但如果请求都返回了错误码（如UPLOAD_PART都返回了NoSuchUpload）就直接resume。
     */
    private void resumeR(MsHttpRequest r, boolean readyToFetch) {

        if (!readyToFetch) {
//            r.resume();
        } else if (!r.headers().contains(CONTENT_LENGTH) || "0".equals(r.getHeader(CONTENT_LENGTH))) {
            r.resume();
        } else {
            r.fetch(DEFAULT_FETCH_AMOUNT);
        }
        if (r.headers().contains("Expect") && readyToFetch) {
            r.response().writeContinue();
        }
    }

    /**
     * 根据成功站点响应信息进行响应返回
     *
     * @param r        request
     * @param response response
     * @param b        响应体
     */
    private void dealResponse(MsHttpRequest r, HttpClientResponse response, Buffer b) {
        try {
            r.response().setStatusCode(response.statusCode());
            r.response().setStatusMessage(response.statusMessage());
            for (Entry<String, String> entry : response.headers()) {
                r.response().headers().add(entry.getKey(), entry.getValue());
            }
            if (b.length() > 0) {
                r.response().write(io.vertx.core.buffer.Buffer.buffer(b.getBytes()));
            }
            r.response().headers().remove(IFF_TAG);
            r.response().end();
//            r.connection().close();
        } catch (Exception e) {
            log.error("dealResponse error, ", e);
        }
    }


    private void dealErrResponse(MsHttpRequest r, HttpResponseSummary responseSummary, AtomicBoolean needBuffer) {
        Handler<Buffer> bufferHandler = buffer -> {
            try {
                r.response().setStatusCode(responseSummary.localResponse.statusCode());
                r.response().setStatusMessage(responseSummary.localResponse.statusMessage());
                for (Entry<String, String> entry : responseSummary.localResponse.headers()) {
                    r.response().headers().add(entry.getKey(), entry.getValue());
                }
//                                                log.info("body {}, {}", r.uri(), buffer.toString());
                if (buffer.length() > 0) {
                    r.response().write(io.vertx.core.buffer.Buffer.buffer(buffer.getBytes()));
                }
                r.response().end();
                r.connection().close();

            } catch (Exception e) {
                log.error("deal fail response error, ", e);
            }
        };
        Buffer buffer = responseSummary.respBuffers.get(responseSummary.localResponse);
        if (needBuffer.get() && buffer != null) {
            bufferHandler.handle(buffer);
            return;
        }

        responseSummary.localResponse.bodyHandler(bufferHandler).resume();
    }

    @ToString
    private class HttpResponseSummary {
        private int successAmount = 0;

        private Flux<Tuple2<String, HttpClientResponse>> responses;

        //本端的响应
        private HttpClientResponse localResponse;

        //保存第一个成功的响应
        private HttpClientResponse successResponse;

        private String errorIp;

        private String successIp;

        private AtomicInteger errorNum = new AtomicInteger(0);

        // 已有返回200的响应。
        private AtomicBoolean hasAsSucc = new AtomicBoolean();

        private Map<HttpClientResponse, Buffer> respBuffers = new HashMap<>();

        public HttpResponseSummary(Flux<Tuple2<String, HttpClientResponse>> flux) {
            this.responses = flux.doOnNext(tuple2 -> {
                HttpClientResponse aliveResponse = tuple2.var2;
                if (aliveResponse == null) {
                    errorIp = tuple2.var1;
                    errorNum.incrementAndGet();
                    return;
                }
//                aliveResponse.exceptionHandler(e -> log.error("aliveResponse error", e));

                HttpClientRequest request = aliveResponse.request();
                log.debug("{} ,statuscode: {}, {}", request.getHost() + request.uri(), aliveResponse.statusCode(), aliveResponse.statusMessage());
                if (aliveResponse.statusCode() == SUCCESS || aliveResponse.statusCode() == DEL_SUCCESS) {
                    this.successAmount++;
                    // 都返回成功的情况优先记录本地ip为successIp
                    if (this.successResponse == null || LOCAL_IP_ADDRESS.equals(tuple2.var1) || hasAsSucc.compareAndSet(false, true)) {
                        this.successResponse = aliveResponse;
                        successIp = tuple2.var1;
                    }
                } else {
                    boolean asSucc = false;
                    // abort不存在的分段视为响应成功
                    if (request.method() == HttpMethod.DELETE) {
                        UnSynchronizedRecord.Type type = UnSynchronizedRecord.type(request.uri(), request.method().toString());
                        if (type == ERROR_PART_ABORT) {
                            if (aliveResponse.statusCode() == NOT_FOUND) {
                                asSucc = true;
                            }
                        }
                    }
                    if (asSucc) {
                        this.successAmount++;
                        // 未有返回成功的响应时才将asSucc情况的响应内容当作成功的响应返回客户端。
                        if (!hasAsSucc.get() && (this.successResponse == null || LOCAL_IP_ADDRESS.equals(tuple2.var1))) {
                            this.successResponse = aliveResponse;
                            successIp = tuple2.var1;
                        }
                    } else {
                        errorIp = tuple2.var1;
                        if (aliveResponse.statusCode() == INTERNAL_SERVER_ERROR) {
                            errorNum.incrementAndGet();
                        }
                    }

                }
                if (LOCAL_IP_ADDRESS.equals(tuple2.var1) || IP_INDEX_MAP.get(tuple2.var1).equals(LOCAL_CLUSTER_INDEX)) {
                    localResponse = aliveResponse;
                }
            });
        }
    }

    public Mono<Tuple3<Integer, String, MultiMap>> sendRequest(UnSynchronizedRecord record) {
        return sendRequest(record.index, record.bucket, record.object, record.uri, record.method
                , record.headers, null, record)
                .onErrorResume(e -> {
                    log.error("sendRequest err1, ", e);
                    return Mono.just(new Tuple3<>(INTERNAL_SERVER_ERROR, "", null));
                });
    }

    public Mono<Tuple3<Integer, String, MultiMap>> sendRequest(int clusterIndex, String bucket, String object, String uri, HttpMethod method,
                                                               Map<String, String> headers, byte[] requestBody) {
        return sendRequest(clusterIndex, bucket, object, uri, method, headers, requestBody, null)
                .onErrorResume(e -> {
                    log.error("sendRequest err2, ", e);
                    return Mono.just(new Tuple3<>(INTERNAL_SERVER_ERROR, "", null));
                });
    }

    /**
     * 发送请求到对端，重试一次。
     * 只适用于请求体较小的请求的发送，否则会有内存问题。
     */
    public Mono<Tuple3<Integer, String, MultiMap>> sendRequest(int clusterIndex, String bucket, String object, String uri, HttpMethod method,
                                                               Map<String, String> headers, byte[] requestBody, UnSynchronizedRecord record) {
        String[] clusterIps = INDEX_IPS_MAP.get(clusterIndex);
        Disposable[] disposables = new Disposable[1];
        int currentIndex = ThreadLocalRandom.current().nextInt(0, clusterIps.length);
        if (isDebug) {
            currentIndex = 0;
        }
        MonoProcessor<Tuple3<Integer, String, MultiMap>> res = MonoProcessor.create();

        boolean isExtra = EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex);
        if (isExtra) {
            try {
                String authString = "AWS " + EXTRA_AK_SK_MAP.get(bucket).var1 + ":0000";
                headers.put(AUTHORIZATION, authString);
            } catch (Exception e) {
                log.error("get extra aksk err, {}", bucket);
                res.onError(e);
                return res;
            }
        }
        String ip = clusterIps[currentIndex];
        HttpClientRequest request = getClient().request(method, isExtra ? EXTRA_PORT : DA_PORT, ip, uri);
        request.putHeader(SYNC_AUTH, PASSWORD);
        request.putHeader(HOST, ip);
        request.setHost(ip + ":" + (isExtra ? EXTRA_PORT : DA_PORT))
                .setTimeout(60_000)
                .setWriteQueueMaxSize(WriteQueueMaxSize)
                .exceptionHandler(e -> {
                    res.onNext(new Tuple3<>(INTERNAL_SERVER_ERROR, "", null));
                    log.debug("sendRequest error! method:{}, uri:{}, {}", request.method().name(), uri, e.getMessage());
                    request.reset();
                    DoubleActiveUtil.streamDispose(disposables);
                });
        if (requestBody != null) {
            MessageDigest digest = Md5DigestPool.acquire();
            digest.update(requestBody);
            String contentMd5 = Hex.encodeHexString(digest.digest());
            Md5DigestPool.release(digest);
            request.putHeader(CONTENT_MD5, contentMd5);
        }
        headers.entrySet().stream()
                .filter(entry -> !escapeHeaders.contains(entry.getKey().toLowerCase()))
                .forEach(entry -> request.putHeader(entry.getKey(), entry.getValue()));
        request.putHeader(CLUSTER_ALIVE_HEADER, ip);
        request.putHeader(ORIGIN_INDEX_CRYPTO, StringUtils.isBlank(headers.get(ORIGIN_INDEX_CRYPTO)) ? "" : headers.get(ORIGIN_INDEX_CRYPTO));
        request.handler(response -> {
            UnSynchronizedRecord.Type type = UnSynchronizedRecord.type(uri, method.name());
            if (type == null) {
                type = NONE;
            }

            if (headers.containsKey(AssignClusterHandler.ClUSTER_NAME_HEADER)) {
                Buffer body = Buffer.buffer();
                response.handler(buf -> {
                    if (isDebug) {
                        log.info("print buf: {} {}", uri, buf.toString());
                    }
                    body.appendBuffer(buf);
                }).exceptionHandler(e -> {
                    log.error("assign response error. {} ", uri, e);
                    res.onNext(new Tuple3<>(INTERNAL_SERVER_ERROR, "", null));
                }).endHandler(v -> {
                    if (body.getBytes() != null && body.length() > 0) {
                        response.headers().add("moss-body", new String(body.getBytes()));
                    }
                    if (DataSynChecker.isDebug) {
                        log.info("ip:{}, uri:{}, statusCode:{}, msg:{}, body:{}", request.getHost(), request.uri(), response.statusCode(), response.statusMessage(), body);
                    } else {
                        log.debug("ip:{}, uri:{}, statusCode:{}, msg:{}, body:{}", request.getHost(), request.uri(), response.statusCode(), response.statusMessage(), body);
                    }
                    if (response.statusCode() == SUCCESS || response.statusCode() == DEL_SUCCESS) {
                        // 打印异构站点的删除结果。
                        if (method.equals(HttpMethod.DELETE) && isExtra) {
                            if (response.headers().contains("moss-body")) {
                                log.info("extra cluster {} multi-delete result success. bucket:{}, delete result:{}", clusterIndex, bucket, response.getHeader("moss-body"));
                            } else {
                                log.info("extra cluster {} delete result success. bucket:{}, obj:{}", clusterIndex, bucket, object);
                            }
                        }
                    }
                    res.onNext(new Tuple3<>(response.statusCode(), response.statusMessage(), response.headers()));

                });
                return;
            }

            if (DataSynChecker.isDebug) {
                log.info("req->type:{}, uri:{}, method:{}, code:{}, msg:{}", type.name(), uri, method.name(), response.statusCode(), response.statusMessage());
            } else {
                log.debug("req->type:{}, uri:{}, method:{}, code:{}, msg:{}", type.name(), uri, method.name(), response.statusCode(), response.statusMessage());
            }
            if (response.statusCode() == SUCCESS || response.statusCode() == DEL_SUCCESS) {
                if (isExtra) {
                    if (record != null && record.type() == ERROR_INIT_PART_UPLOAD) {
                        response.bodyHandler(buffer -> {
                            Uploads uploads = (Uploads) JaxbUtils.toObject(new String(buffer.getBytes()));
                            String afterInitRecordKey = record.afterInitRecordKey();
                            UnSynchronizedRecord afterInitRecord = new UnSynchronizedRecord();
                            HashMap<String, String> recordHeaders = new HashMap<>();
                            recordHeaders.put(UPLOAD_ID, uploads.getUploadId());
                            afterInitRecord.setRecordKey(afterInitRecordKey)
                                    .setUri(bucket)
                                    .setBucket(bucket)
                                    .setObject(object)
                                    .setIndex(clusterIndex)
                                    .setHeaders(recordHeaders)
                                    .setVersionNum(VersionUtil.getVersionNum(false));

                            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(record.bucket);
                            String bucketVnode = storagePool.getBucketVnodeId(record.bucket);
                            storagePool.mapToNodeInfo(bucketVnode)
                                    .flatMap(nodeList -> ErasureClient.updateAfterInitRecord(storagePool, afterInitRecordKey, Json.encode(afterInitRecord), nodeList))
                                    .subscribe(b -> {
                                        if (b) {
                                            res.onNext(new Tuple3<>(SUCCESS, "", response.headers()));
                                        } else {
                                            res.onNext(new Tuple3<>(INTERNAL_SERVER_ERROR, "", null));
                                        }
                                    });

                        });
                    } else {
                        // 添加minio复制删除对象返回的打印
                        if (method.equals(HttpMethod.DELETE)) {
                            log.info("extra cluster {} delete result success. bucket:{}, obj:{}", clusterIndex, bucket, object);
                        }
                        res.onNext(new Tuple3<>(SUCCESS, "", response.headers()));
                    }
                } else {
                    if (response.statusCode() == DEL_SUCCESS) {
                        String statusMessage = response.statusMessage() == null ? "" : response.statusMessage();
                        res.onNext(new Tuple3<>(SUCCESS, statusMessage, response.headers()));
                    } else {
                        res.onNext(new Tuple3<>(SUCCESS, "", response.headers()));
                    }
                }
//                closeConn(request);
            } else {
                if (method != HttpMethod.HEAD) {
                    response.bodyHandler(body -> {
                        Error errorObj = null;
                        try {
                            String cs = new String(body.getBytes());
                            if (StringUtils.isNotEmpty(cs) && !"".equals(cs)) {
                                errorObj = (Error) JaxbUtils.toErrorObject(cs);
                            }
                        } catch (Exception e) {

                        }
                        if (DataSynChecker.isDebug) {
                            log.info("ip:{}, uri:{}, statusCode:{}, msg:{}, body:{}", request.getHost(), request.uri(), response.statusCode(), response.statusMessage(), body);
                        } else {
                            log.debug("ip:{}, uri:{}, statusCode:{}, msg:{}, body:{}", request.getHost(), request.uri(), response.statusCode(), response.statusMessage(), body);
                        }
                        res.onNext(new Tuple3<>(response.statusCode(), errorObj == null ? "" : errorObj.getCode(), response.headers()));
//                        closeConn(request);
                    });
                } else {
                    res.onNext(new Tuple3<>(response.statusCode(), "", null));
//                    closeConn(request);
                }
            }
        });

        if (requestBody != null && requestBody.length > 0) {
            request.putHeader(CONTENT_LENGTH, String.valueOf(requestBody.length));
        } else {
            request.putHeader(CONTENT_LENGTH, "0");
        }
        DataSyncSignHandler signHandler = new DataSyncSignHandler(bucket, clusterIndex, request);
        disposables[0] = signHandler.getSignType()
                .flatMap(signType -> {
                    switch (signType) {
                        case AWS_V2:
                            return AuthorizeV2.getAuthorizationHeader(bucket, object, headers, request, isExtra);
                        case AWS_V4_NONE:
                            String payloadHash = "";
                            if ("0".equals(request.headers().get(CONTENT_LENGTH))) {
                                payloadHash = EMPTY_SHA256;
                            }
                            return AuthorizeV4.getAuthorizationHeader(bucket, headers.get(AUTHORIZATION), request, payloadHash, request.headers().contains(IS_SYNCING), isExtra);
                        case AWS_V4_SINGLE:
                            byte[] finalRequestBody = requestBody;
                            if (requestBody == null) {
                                finalRequestBody = new byte[0];
                            }
                            return signHandler.singleCheckSHA256(headers.get(AUTHORIZATION), finalRequestBody, request.headers().contains(IS_SYNCING), isExtra);
                        case AWS_V4_CHUNK:
                        default:
                            log.error("no such signType. {} {}", bucket, signType);
                            throw new RuntimeException("no such signType");
                    }
                })
                .timeout(Duration.ofSeconds(30))
                .subscribeOn(SCAN_SCHEDULER)
                .subscribe(authString -> {
                    try {
                        if (StringUtils.isNotBlank(authString)) {
                            request.putHeader(AUTHORIZATION, authString);
                        } else {
                            //匿名访问则移除AUTHORIZATION
                            request.headers().remove(AUTHORIZATION);
                        }

                        // x-amz开头的头部字段要算完签名后url编码。
                        encodeAmzHeaders(request);

                        if (requestBody != null && requestBody.length > 0) {
                            request.write(Buffer.buffer(requestBody));
                        } else {
                            request.write(Buffer.buffer());
                        }
                        if (request.getDelegate().connection() != null) {
                            synchronized (request.getDelegate().connection()) {
                                request.end();
                            }
                        } else {
                            request.end();
                        }
                    } catch (Exception e) {
                        log.error("", e);
                        closeConn(request);
//                        DoubleActiveUtil.streamDispose(disposables);
                        res.onError(e);
                    }
                }, cause -> {
                    log.error("", cause);
                    closeConn(request);
                    res.onError(cause);
                });

        return res;
    }

    public Mono<Boolean> sendSyncRequest(int clusterIndex, String bucket, String object, String uri, HttpMethod method,
                                         Map<String, String> headers, byte[] requestBody) {
        return sendRequest(clusterIndex, bucket, object, uri, method, headers, requestBody)
                .flatMap(tuple3 -> {
                    if (method.equals(HttpMethod.DELETE) && tuple3.var1 == NOT_FOUND) {
                        if (("NoSuchUpload".equals(tuple3.var2) || "NoSuchKey".equals(tuple3.var2))) {
                            return Mono.just(true);
                        } else {
                            return Mono.just(false);
                        }
                    } else if (method.equals(HttpMethod.DELETE) && "ObjectImmutable".equals(tuple3.var2)) {
                        return Mono.just(true);
                    } else {
                        return Mono.just(tuple3.var1 == SUCCESS || tuple3.var1 == DEL_SUCCESS);
                    }
                });
    }

    public Mono<Boolean> sendSyncRequest(UnSynchronizedRecord record) {
        return sendSyncRequest(record.index, record.bucket, record.object, record.uri, record.method
                , record.headers, null);
    }

    /**
     * 双活请求的头部字段CLUSTER_VALUE的值，表示该请求是对站点转发的，直接走异步复制流程。
     */
    public static final String SYNC_REDIRECT = "sync-redirect";

    public void redirectHttpRequest(String[] ips, MsHttpRequest r, boolean isRedirected) {
        if (AuthorizeV4.CONTENT_SHA256_MULTIPLE_CHUNKS_PAYLOAD.equals(r.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256))) {
            String decodedContentLength = r.getHeader(X_AMZ_DECODED_CONTENT_LENGTH);
            if (decodedContentLength == null) {
                dealException(r, new MsException(ErrorNo.MISSING_DECODED_CONTENT_LENGTH, "You must provide the Content-Length HTTP header."));
                return;
            }
            try {
                Long.parseLong(decodedContentLength);
            } catch (NumberFormatException e) {
                dealException(r, new MsException(ErrorNo.MISSING_DECODED_CONTENT_LENGTH, e.getMessage()));
                return;
            }
            //原来的Content-Length缓存下来
            r.addMember(CONTENT_LENGTH, r.getHeader(CONTENT_LENGTH));
            r.headers().set(CONTENT_LENGTH, decodedContentLength);
        }

        AtomicInteger ipIndex = new AtomicInteger(ThreadLocalRandom.current().nextInt(ips.length));

        UnicastProcessor<Integer> retryProcessor = UnicastProcessor.create();
        HttpClientRequest[] request = new HttpClientRequest[1];
        AtomicBoolean startResume = new AtomicBoolean();

        Disposable subscribe = retryProcessor.subscribe(tryTime -> {
            String ip = ips[ipIndex.getAndIncrement() % ips.length];
            AtomicBoolean hasExc = new AtomicBoolean(false);

            request[0] = getFrontClient().request(r.method(), DA_PORT, ip, r.uri())
                    .setWriteQueueMaxSize(WriteQueueMaxSize)
                    .setTimeout(10_000)
                    .setHost(ip + ":" + DA_PORT)
                    .exceptionHandler(e -> {
                        if (!hasExc.compareAndSet(false, true)) {
                            return;
                        }
                        log.error("redirect {} request to {} error1! {}, {}", request[0].method().name(), request[0].getHost() + request[0].uri(), e.getClass().getName(), e.getMessage());
                        // 似乎加上后出现大量异常request的情况下httpstream回收变快
                        request[0].reset();

                        boolean originReqClosed = false;
                        // 如果已经开始数据传输途中出现了连接问题，也不必重试了。
                        if (r.response().ended() || r.response().closed() || startResume.get()) {
                            originReqClosed = true;
                        }

                        if (tryTime >= ips.length || originReqClosed) {
                            dealException(r, e);
                            retryProcessor.onComplete();
                        } else {
                            retryProcessor.onNext(tryTime + 1);
                        }
                    })
                    .continueHandler(v -> {
                        startResume.compareAndSet(false, true);
                        resumeR(r, true);
                    })
                    .handler(response -> {
                        response.bodyHandler(b -> dealResponse(r, response, b));
                        retryProcessor.onComplete();
                    });

            if (isRedirected) {
                // 加此请求头的请求会直接走异步复制流程
                request[0].putHeader(CLUSTER_VALUE, SYNC_REDIRECT);
            }
            r.headers().forEach(header -> request[0].putHeader(header.getKey(), header.getValue()));
            if (r.headers().contains(X_AMZ_COPY_SOURCE) && r.headers().contains(CONTENT_LENGTH)) {
                request[0].putHeader(CONTENT_LENGTH, "0");
            }
            encodeAmzHeaders(request[0]);
            if (r.headers().contains(CONTENT_LENGTH) && Long.parseLong(r.headers().get(CONTENT_LENGTH)) > 0) {
                request[0].putHeader(EXPECT, EXPECT_100_CONTINUE);
                if (r.getMember(CONTENT_LENGTH) != null) {
                    request[0].putHeader(CONTENT_LENGTH, r.getMember(CONTENT_LENGTH));
                }
                request[0].sendHead();
            } else {
                // v4上传0字节对象，需要使用request.end()避免连接无法关闭，此时request不能有请求体，但直接设置content-length=0会有签名问题。
                if (r.getMember(CONTENT_LENGTH) != null && Long.parseLong(r.getMember(CONTENT_LENGTH)) > 0) {
                    request[0].putHeader(SYNC_V4_CONTENT_LENGTH, r.getMember(CONTENT_LENGTH));
                }
                request[0].end();
            }
        }, e -> {
            log.error("redirect error, ", e);
            dealException(r, e);
            retryProcessor.dispose();
        });

        r.addResponseCloseHandler(v -> {
            subscribe.dispose();
        });

        AtomicBoolean waitForFetch = new AtomicBoolean(false);
        //每个双活请求实际写入元素的次数的总和。总增加量 = 原请求fetch数 * 双活个数
        AtomicLong writeTotal = new AtomicLong();
        // 起始为1表示只需要往一处发送
        AtomicLong fetchTotal = new AtomicLong(DEFAULT_FETCH_AMOUNT);

        if (r.headers().contains(CONTENT_LENGTH)
                && Long.parseLong(r.headers().get(CONTENT_LENGTH)) > 0) {
            r.handler(b -> {
                try {
                    Buffer buffer = Buffer.newInstance(b);
                    request[0].write(buffer, v -> {
                        // 用fetch实现限流。处理完当前fetch到的才会去fetch下一轮数据。
                        if (!waitForFetch.get() && writeTotal.incrementAndGet() >= fetchTotal.get()) {
//                                log.info("write:{}, fetch:{}", writeTotal.get(),fetchTotal.get());
                            r.fetch(DEFAULT_FETCH_AMOUNT);
                            fetchTotal.addAndGet(DEFAULT_FETCH_AMOUNT);
                        }
                    });

                    if (!waitForFetch.get() && request[0].writeQueueFull()) {
                        waitForFetch.set(true);
                        request[0].drainHandler(done -> {
                            waitForFetch.set(false);
                            r.fetch(DEFAULT_FETCH_AMOUNT);
                            fetchTotal.addAndGet(DEFAULT_FETCH_AMOUNT);
                        });
                    }
                } catch (Exception e) {
                    log.error("", e);
                }
            });

            r.endHandler(a -> {
                try {
                    if (request[0].getDelegate().connection() != null) {
                        synchronized (request[0].getDelegate().connection()) {
                            request[0].end();
                        }
                    }
                } catch (Exception e) {
                    log.error("", e);
                }
            });
        }
        r.exceptionHandler(e -> {
            try {
                log.debug("origin request exception, {}", r.uri(), e);
                r.connection().close();
                subscribe.dispose();
            } catch (Exception exception) {
                log.error("", e);
            }
        });
        retryProcessor.onNext(1);
    }

    public static void putSyncDumpFile(MsHttpRequest request) {
        HttpServerRequest r = request.getDelegate();
        String fileName = r.getHeader("fileName");
        String targetDir = r.getHeader("targetDir");

        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            log.error("putSyncDumpFile error: no auth to sync");
            request.response().setStatusCode(400).end("no auth to sync.");
            return;
        }
        if (fileName == null || fileName.isEmpty()) {
            log.error("putSyncDumpFile fileName error: {}", fileName);
            r.response().setStatusCode(400).end("missing fileName");
            return;
        }
        if (targetDir == null || targetDir.isEmpty()) {
            log.error("putSyncDumpFile targetDir error: {}", targetDir);
            r.response().setStatusCode(400).end("missing targetDir");
            return;
        }


        Path targetPath = Paths.get(targetDir, fileName);
        Path parentDir = targetPath.getParent();
        AtomicBoolean finished = new AtomicBoolean(false);

        Vertx vertx = Vertx.currentContext().owner();
        FileSystem fs = vertx.fileSystem();

        fs.mkdirs(parentDir.toString(), mkAr -> {
            if (mkAr.failed()) {
                fail(r, finished, "create parent dir failed", mkAr.cause());
                return;
            }

            fs.exists(targetPath.toString(), existAr -> {
                if (existAr.failed()) {
                    fail(r, finished, "check file exist failed", existAr.cause());
                    return;
                }

                if (existAr.result()) {
                    log.info("target file exists, deleting: {}", targetPath);
                    fs.delete(targetPath.toString(), delAr -> {
                        if (delAr.failed()) {
                            fail(r, finished, "delete existing file failed", delAr.cause());
                        } else {
                            openAndUpload(vertx, request, r, targetPath, finished);
                        }
                    });
                } else {
                    openAndUpload(vertx, request, r, targetPath, finished);
                }
            });
        });

    }

    private static void openAndUpload(Vertx vertx, MsHttpRequest request, HttpServerRequest r, Path targetPath, AtomicBoolean finished) {
        AtomicBoolean waitForFetch = new AtomicBoolean(false);
        AtomicLong writeTotal = new AtomicLong();
        AtomicLong fetchTotal = new AtomicLong(BACKUP_FETCH_AMOUNT);
        final AtomicInteger pendingWrites = new AtomicInteger(0);
        FileSystem fileSystem = vertx.fileSystem();
        AtomicBoolean closed = new AtomicBoolean(false);
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            fileSystem.open(targetPath.toString(), new OpenOptions()
                            .setWrite(true)
                            .setCreate(true)
                            .setTruncateExisting(true),
                    ar -> {
                        if (ar.failed()) {
                            fail(r, finished, "open file failed", ar.cause());
                            return;
                        }

                        log.info("file open success: {}", targetPath);
                        AsyncFile file = ar.result();
                        AtomicInteger fileSize = new AtomicInteger(0);

                        Runnable tryCloseFile = () -> {
                            if (finished.get() && pendingWrites.get() == 0) {
                                file.close();
                            }
                        };

                        // ===== 上传数据 handler =====
                        r.handler(buffer -> {
                            if (closed.get()) return;
                            pendingWrites.incrementAndGet();
                            try {
                                md5.update(buffer.getBytes());
                                file.write(Buffer.newInstance(buffer), wr -> {
                                    if (wr.failed()) {
                                        fail(r, finished, "write failed", wr.cause());
                                    }
                                    fileSize.addAndGet(buffer.length());
                                    log.debug("waitForFetch: false, writeTotal: {}, fetchTotal: {}, writeSize: {}", writeTotal.get(), fetchTotal.get(), fileSize.get() / 1024.0 / 1024.0);

                                    // fetch 限流：写完成就 fetch 新数据
                                    if (writeTotal.incrementAndGet() >= fetchTotal.get() && !waitForFetch.get()) {
                                        fetchFile(r, fetchTotal);
                                    }

                                    // 背压处理：只注册一次 drainHandler
                                    if (!waitForFetch.get() && file.writeQueueFull()) {
                                        waitForFetch.set(true);
                                        file.drainHandler(v -> {
                                            waitForFetch.set(false);
                                            fetchFile(r, fetchTotal);
                                        });
                                    }
                                });
                            } finally {
                                if (pendingWrites.decrementAndGet() == 0 && finished.get()) {
                                    tryCloseFile.run();
                                }
                            }
                        }).endHandler(v -> {
                            log.info("request end handler called");
                            finished.compareAndSet(false, true);
                            log.info("closing file safely: {}", targetPath);
                            // ===== 计算最终 MD5 =====
                            byte[] md5Bytes = md5.digest();
                            String localMd5 = bytesToHex(md5Bytes);

                            log.info("upload success, md5={}", localMd5);
                            r.response().putHeader("Content-MD5", localMd5).setStatusCode(200).end("upload ok");
                        }).exceptionHandler(e -> {
                            closed.compareAndSet(false, true);
                            fail(r, finished, "request error", e);
                        });

                        // ===== 启动第一批 fetch =====
                        r.fetch(BACKUP_FETCH_AMOUNT);

                        // ===== 超时保护 =====
                        vertx.setTimer(300_000, id -> {
                            if (!finished.get()) {
                                log.warn("upload timeout, closing");
                                finished.compareAndSet(false, true);
                                closed.compareAndSet(false, true);
                                try {
                                    r.response().setStatusCode(504).end("upload timeout");
                                    fail(r, finished, "upload timeout", new MsException(504, "upload timeout"));
                                    file.close();
                                } catch (Exception ignored) {
                                }
                                request.connection().close();
                            }
                        });
                    });
        } catch (NoSuchAlgorithmException e) {
            log.error("", e);
        }
    }

    // 上传失败统一处理
    private static void fail(HttpServerRequest r, AtomicBoolean finished, String msg, Throwable e) {
        if (finished.compareAndSet(false, true)) {
            log.error(msg, e);
            try {
                r.response().setStatusCode(500).end(msg + ": " + e.getMessage());
            } catch (Exception ex) {
                log.error("response failed", ex);
            }
            r.connection().close();
        }
    }

    private static void fetchFile(HttpServerRequest r, AtomicLong fetchTotal) {
        r.fetch(BACKUP_FETCH_AMOUNT);
        fetchTotal.addAndGet(BACKUP_FETCH_AMOUNT);
    }

    public static String syncEthToType(String syncEth, String ethType) {
        String eth = "";
        if (StringUtils.isEmpty(ethType) || "ipv4".equals(ethType)) {
            eth = getString(syncEth, BUSINESS_ETH1, BUSINESS_ETH2, BUSINESS_ETH3);
        } else if ("ipv6".equals(ethType)) {
            eth = getString(syncEth, BUSINESS_ETH1_IPV6, BUSINESS_ETH2_IPV6, BUSINESS_ETH3_IPV6);
        }
        return eth;
    }

    private static String getString(String syncEth, String businessEth1, String businessEth2, String businessEth22) {
        String eth;
        if ("eth2".equals(syncEth)) {
            eth = businessEth1;
        } else if ("eth3".equals(syncEth)) {
            eth = businessEth2;
        } else if ("eth18".equals(syncEth)) {
            eth = businessEth22;
        } else if ("eth4".equals(syncEth)) {
            eth = HEART_ETH1;
        } else {
            eth = SYNC_IP;
        }
        return eth;
    }

}
