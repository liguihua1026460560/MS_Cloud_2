package com.macrosan.action.managestream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.TypeReference;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.etcd.EtcdClient;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.doubleActive.HeartBeatChecker;
import com.macrosan.doubleActive.PoolingRequest;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.filesystem.utils.IpWhitelistUtils;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.inventory.InventoryService;
import com.macrosan.message.jsonmsg.BucketInfo;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.socketmsg.SocketSender;
import com.macrosan.message.socketmsg.StringResMsg;
import com.macrosan.message.xmlmsg.AccessControlPolicy;
import com.macrosan.message.xmlmsg.BucketVersioning;
import com.macrosan.message.xmlmsg.Buckets;
import com.macrosan.message.xmlmsg.clear.ClearConfigration;
import com.macrosan.message.xmlmsg.cors.CORSConfiguration;
import com.macrosan.message.xmlmsg.referer.BlackRefererList;
import com.macrosan.message.xmlmsg.referer.RefererConfiguration;
import com.macrosan.message.xmlmsg.referer.WhiteRefererList;
import com.macrosan.message.xmlmsg.section.*;
import com.macrosan.message.xmlmsg.tagging.Tagging;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.metaserver.BucketShardCache;
import com.macrosan.storage.metaserver.ObjectSplitTree;
import com.macrosan.storage.metaserver.ShardingWorker;
import com.macrosan.utils.cors.CORSUtils;
import com.macrosan.utils.functional.Entry;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.PolicyCheckUtils;
import com.macrosan.utils.quota.QuotaRecorder;
import com.macrosan.utils.regions.MultiRegionUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import com.macrosan.utils.trash.TrashUtils;
import com.macrosan.utils.worm.WormUtils;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.macrosan.clearmodel.ClearModelExecutor.DELETE_MARK;
import static com.macrosan.clearmodel.ClearModelUtils.getXmlConfig;
import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.ServerConstants.VERSION_NUM;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.DoubleActiveUtil.*;
import static com.macrosan.doubleActive.HeartBeatChecker.syncPolicy;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.getBucketSyncStatus;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.isSwitchOn;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.UPDATE_BUCKET_STRATEGY;
import static com.macrosan.filesystem.utils.CheckUtils.bucketFsCheck;
import static com.macrosan.filesystem.utils.CheckUtils.checkCreateBucketParamWithFs;
import static com.macrosan.httpserver.MossHttpClient.*;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;
import static com.macrosan.snapshot.SnapshotMarkGenerator.MIN_SNAPSHOT_MARK;
import static com.macrosan.snapshot.utils.SnapshotUtil.hasSnapshotInBucket;
import static com.macrosan.storage.metaserver.ObjectSplitTree.SEPARATOR_LINE_PREFIX;
import static com.macrosan.storage.metaserver.ShardingWorker.deleteBucketShardInfo;
import static com.macrosan.utils.essearch.EsMetaTaskScanner.ES_SCHEDULER;
import static com.macrosan.utils.quota.QuotaRecorder.BUCKET_MAX_OBJECTS_QUOTA;
import static com.macrosan.utils.quota.QuotaRecorder.SOFT_OBJECTS_ALARM_THRESHOLD;
import static com.macrosan.utils.regex.PatternConst.BUCKET_NAME_PATTERN;
import static com.macrosan.utils.regex.PatternConst.BUCKET_QUOTA_PATTERN;

/**
 * BucketService
 * 非线程安全单例模式
 *
 * @author liyixin
 * @date 2018/10/31
 */
@Log4j2
public class BucketService extends BaseService {

    private static final Logger logger = LogManager.getLogger(BucketService.class.getName());

    private static BucketService instance = null;

    private BucketService() {
        super();
    }

    /**
     * 每一个Service都必须提供一个getInstance方法
     */
    public static BucketService getInstance() {
        if (instance == null) {
            instance = new BucketService();
        }
        return instance;
    }

    /**
     * 判断一个桶是否存在以及是否有权限访问的操作
     *
     * @param paramMap 请求参数
     * @return 存在 or 不存在
     */
    public ResponseMsg headBucket(UnifiedMap<String, String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        String method = "HeadBucket";

        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            MsAclUtils.checkReadAcl(bucketInfo, userId, bucketName);
        }

        if (paramMap.containsKey(X_AUTH_TOKEN)) {
            MonoProcessor<String> res = MonoProcessor.create();
            ErasureClient.reduceBucketInfo(bucketName)
                    .filter(BucketInfo::isAvailable)
                    .map(BucketInfo::getObjectNum)
                    .doOnError(e -> logger.error("get bucketInfo error,{}", e))
                    .defaultIfEmpty("0")
                    .subscribe(res::onNext);
            String bucketObjectNum = res.block();

            MonoProcessor<String> res1 = MonoProcessor.create();
            ErasureClient.reduceBucketInfo(bucketName)
                    .filter(BucketInfo::isAvailable)
                    .map(BucketInfo::getBucketStorage)
                    .doOnError(e -> logger.error("get bucketInfo error,{}", e))
                    .defaultIfEmpty("0")
                    .subscribe(res1::onNext);
            String bucketObjectBytes = res1.block();
            return new ResponseMsg(SUCCESS_STATUS, new byte[0])
                    .addHeader(CONTENT_LENGTH, "0")
                    .setHttpCode("0".equals(bucketObjectNum) ? DEL_SUCCESS : SUCCESS)
                    .addHeader(X_CONTAINER_OBJECT_COUNT, bucketObjectNum)
                    .addHeader(X_CONTAINER_BYTES_USED, bucketObjectBytes);
        }
        return new ResponseMsg(SUCCESS_STATUS, new byte[0]).addHeader(CONTENT_LENGTH, "0");
    }

    /**
     * 获取请求者自己创建的所有桶列表
     *
     * @param paramMap 请求参数
     * @return 桶列表
     */
    public ResponseMsg listBuckets(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);

        MsAclUtils.checkIfAnonymous(userId);

        String userBucketSet = userId + USER_BUCKET_SET_SUFFIX;
        Set<String> bucketSet = pool.getCommand(REDIS_USERINFO_INDEX).smembers(userBucketSet);

        List<Bucket> list = new ArrayList<>(bucketSet.size());
        for (String bucket : bucketSet) {
            if (pool.getCommand(REDIS_BUCKETINFO_INDEX).exists(bucket) == 0) {
                continue;
            }
            String strategy = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, "storage_strategy");
            String bucketCreateDate = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, USER_DATABASE_ID_CREATE_TIME);
            String createTime = MsDateUtils.stampToISO8601(bucketCreateDate);
            Bucket bucketInfo = new Bucket()
                    .setName(bucket)
                    .setCreationDate(createTime)
                    .setStorageStrategy(strategy);
            list.add(bucketInfo);
        }
        String userName = pool.getCommand(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_NAME_TYPE);
        Owner owner = new Owner().setId(userId).setDisplayName(userName);
        Buckets buckets = new Buckets().setOwners(owner);
        if (list.size() > 0) {
            buckets.setBucketList(list);
        }

        if (paramMap.containsKey(X_AUTH_TOKEN)) {
            String containerCount = list.size() + "";
            List<BucketSwift> bucketSwiftList = new ArrayList<>(list.size());
            for (Bucket bucket : list) {
                BucketSwift bucketSwift = new BucketSwift()
                        .setName(bucket.getName())
                        .setCreation_date(bucket.getCreationDate());
                bucketSwiftList.add(bucketSwift);
            }
            ResponseMsg responseMsg = new ResponseMsg()
                    .addHeader(X_ACCOUNT_CONTAINER_COUNT, containerCount);
            if (StringUtils.isNotEmpty(paramMap.get("marker"))) {
                bucketSwiftList = new ArrayList<>(0);
            }
            responseMsg.setHttpCode(list.size() == 0 ? DEL_SUCCESS : SUCCESS);
            if (StringUtils.equalsIgnoreCase("json", paramMap.get("format"))) {
                responseMsg.setData(new JsonArray(bucketSwiftList).toString()).addHeader(CONTENT_TYPE, "application/json");
            } else if (StringUtils.equalsIgnoreCase("xml", paramMap.get("format"))) {
                BucketsSwift bucketsSwift = new BucketsSwift();
                bucketsSwift.setBucketList(bucketSwiftList);
                responseMsg.setData(bucketsSwift).addHeader(CONTENT_TYPE, "application/xml");
            } else {
                StringBuilder bucketName = new StringBuilder(bucketSwiftList.size());
                for (BucketSwift bucketSwift : bucketSwiftList) {
                    bucketName.append(bucketSwift.getName()).append("\n");
                }
                responseMsg.setData(bucketName.toString());
                if (bucketSwiftList.size() != 0) {
                    responseMsg.addHeader(CONTENT_TYPE, "application/xml");
                }
            }
            return responseMsg;
        }
        return new ResponseMsg().setData(buckets).addHeader(CONTENT_TYPE, "application/xml");
    }

    /**
     * 创建桶
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg putBucket(UnifiedMap<String, String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(userId);
        String operationUser = pool.getCommand(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_ID_NAME);
        String strategy = pool.getCommand(REDIS_USERINFO_INDEX).hget(operationUser, "storage_strategy");
        JSONArray metaArray = JSONArray.parseArray(Optional.ofNullable(pool.getCommand(REDIS_POOL_INDEX).hget(strategy, "meta")).orElse("[]"));
        JSONArray dataArray = JSONArray.parseArray(Optional.ofNullable(pool.getCommand(REDIS_POOL_INDEX).hget(strategy, "data")).orElse("[]"));
        log.info("create bucket paramMap:{}", paramMap);
        String defSyncSwitch = "";
        if (metaArray.size() == 0 || dataArray.size() == 0) {
            throw new MsException(INVALID_STORAGE_STRATEGY,
                    "The strategy does not init data and meta.");
        }
        if (paramMap.containsKey("metadata-analysis") && "on".equals(paramMap.get("metadata-analysis"))) {
            JSONArray esArray = JSONArray.parseArray(Optional.ofNullable(pool.getCommand(REDIS_POOL_INDEX).hget(strategy, "es")).orElse("[]"));

            if (esArray.size() == 0) {
                throw new MsException(NO_SUPPORT_METADATA_SEARCH,
                        "The bucket does not support metadata search.");
            }
        }

        if (!paramMap.containsKey("sourcesite".toLowerCase())) {
            paramMap.put("sourcesite", LOCAL_SITE);
        }

        if (paramMap.containsKey("metadata-analysis")) {
            if (!"on".equals(paramMap.get("metadata-analysis")) && !"off".equals(paramMap.get("metadata-analysis"))) {
                throw new MsException(INVALID_ARGUMENT, "metadata switch parameter input error.");
            }
        }

        if (paramMap.containsKey(DATA_SYNC_SWITCH)) {
            if (!"on".equals(paramMap.get(DATA_SYNC_SWITCH)) && !"off".equals(paramMap.get(DATA_SYNC_SWITCH))) {
                throw new MsException(INVALID_ARGUMENT, "datasync switch parameter input error.");
            }

            if (!HeartBeatChecker.isMultiAliveStarted && "on".equals(paramMap.get(DATA_SYNC_SWITCH))) {
                throw new MsException(INSUFFICIENT_SITE_CONDITIONS, "The current site does not meet the setting conditions.");
            }
        } else {
            defSyncSwitch = pool.getCommand(REDIS_SYSINFO_INDEX).get(DEFAULT_DATA_SYNC_SWITCH);
            if ("".equals(defSyncSwitch) || defSyncSwitch == null) {
                defSyncSwitch = "off";
            }
            paramMap.put(DATA_SYNC_SWITCH, defSyncSwitch);
        }

        if (paramMap.containsKey("object-lock-enabled-for-bucket")) {
            if (!"on".equals(paramMap.get("object-lock-enabled-for-bucket")) && !"off".equals(paramMap.get("object-lock-enabled-for-bucket"))) {
                throw new MsException(INVALID_ARGUMENT, "object-lock-enabled-for-bucket switch parameter input error.");
            }
        }
        if (paramMap.containsKey(SNAPSHOT_SWITCH)) {
            if (!"on".equals(paramMap.get(SNAPSHOT_SWITCH)) && !"off".equals(paramMap.get(SNAPSHOT_SWITCH))) {
                throw new MsException(INVALID_ARGUMENT, "snapshot switch parameter input error.");
            }
            if ("on".equals(paramMap.get(SNAPSHOT_SWITCH)) && ("on".equals(paramMap.get("metadata-analysis")) || HeartBeatChecker.isMultiAliveStarted || "on".equals(paramMap.get(NFS_SWITCH)))) {
                throw new MsException(SNAPSHOT_CONFLICT, "The Bucket snapshot conflicts with some other features.");
            }
        }

        FastList<Entry<String, String>> grantAclList = new FastList<>(16);
        paramMap.forEach((k, v) -> {
            if (k.startsWith("x-amz-grant-")) {
                for (String value : v.split(",")) {
                    String[] id = value.split("=", 2);
                    if (id.length != 2) {
                        throw new MsException(INVALID_ARGUMENT, "acl input error.");
                    }
                    grantAclList.add(new Entry<>(id[1], k));
                }
            }
        });

        // 多区域与多站点共存的情况下，创建桶时所指定的站点必须在所指定的区域当中
        if (!MultiRegionUtils.siteInSpecifiedRegion(paramMap)) {
            throw new MsException(INVALID_SITE_CONSTRAINT, "The specified site not int the specified region.");
        }

        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        boolean isMasterCluster = StringUtils.isEmpty(masterCluster) || localCluster.equals(masterCluster);
        final boolean doubleFlag = paramMap.containsKey(DOUBLE_FLAG) && "1".equals(paramMap.get(DOUBLE_FLAG));
        paramMap.put("ctime", Long.toString(System.currentTimeMillis()));

        checkCreateBucketParamWithFs(paramMap, strategy);

        /**主站点执行区域转发**/
        if (!doubleFlag && isMasterCluster && !MultiRegionUtils.dealRegionsCreateBucket(paramMap)) {
            return new ResponseMsg(SUCCESS_STATUS, new byte[0]);
        }

        /**从站点请求主站点执行**/
        if (!doubleFlag && !DoubleActiveUtil.dealSiteCreateBucket(paramMap, localCluster, masterCluster)) {
            // 从站点请求主站点创桶成功，sleep 3秒，避免立刻配容量配额，桶还未同步到从站点
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                logger.info("sleep error", e);
            }
            return new ResponseMsg(SUCCESS_STATUS, new byte[0]);
        }

        try {
            final boolean lock = acquireLock(bucketName, 1000);
            try {
                if (lock) {
                    createBucketInternal(paramMap);
                } else {
                    throw new MsException(BUCKET_EXISTS, "try get create bucket lock error!");
                }
            } finally {
                if (lock) {
                    releaseLock(bucketName);
                }
            }
        } catch (Exception e) {
            MultiRegionUtils.deleteSharedLog(paramMap, true);
            DoubleActiveUtil.deleteShareLog(paramMap, "createBucket");
            if (e.getMessage() != null && e.getMessage().contains("allocate bucket shard error")) {
                deleteBucket(paramMap);
            }
            throw e;
        }

        try {
            if (paramMap.containsKey("x-amz-acl")) {
                setAcl(userId, bucketName, paramMap);
            } else if (!grantAclList.isEmpty()) {
                setGrantAcl(userId, bucketName, grantAclList, paramMap);
            }

            int res = notifySlaveSite(paramMap, ACTION_CREATE_BUCKET);
            if (res != SUCCESS_STATUS) {
                throw new MsException(res, "slave create bucket error");
            }

            MultiRegionUtils.createBucketToCenter(paramMap, isMasterCluster);
        } catch (MsException e) {
            MultiRegionUtils.deleteSharedLog(paramMap, true);
            DoubleActiveUtil.deleteShareLog(paramMap, "createBucket");
            if (e.getErrCode() != BUCKET_EXISTS) {
                paramMap.put("rollBack", NO_SYNCHRONIZATION_VALUE);
                deleteBucket(paramMap);
            }
            throw e;
        } catch (Exception e) {
            MultiRegionUtils.deleteSharedLog(paramMap, true);
            DoubleActiveUtil.deleteShareLog(paramMap, "createBucket");
            deleteBucket(paramMap);
            throw e;
        }

        Mono.just(true).publishOn(SCAN_SCHEDULER)
                .subscribe(s -> {
                    if (paramMap.containsKey(DATA_SYNC_SWITCH) && "on".equals(paramMap.get(DATA_SYNC_SWITCH))) {
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(SYNC_BUCKET_SET, bucketName);
                    }
                });
        Mono.just(true).publishOn(ES_SCHEDULER)
                .subscribe(s -> {
                    if (paramMap.containsKey("metadata-analysis") && "on".equals(paramMap.get("metadata-analysis"))) {
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(ES_BUCKET_SET, bucketName);
                    }
                });
        if (paramMap.containsKey(X_AUTH_TOKEN)) {
            return new ResponseMsg(SUCCESS_STATUS, new byte[0]).setHttpCode(CREATED);
        }
        return new ResponseMsg(SUCCESS_STATUS, new byte[0]);
    }

    private JsonArray getVBucketJson(RedisCommands<String, String> commands, String vnode, String bucket) {
        JsonArray bucketsJson;
        String buckets = commands.hget(vnode, "buckets");
        if (StringUtils.isNotBlank(buckets)) {
            bucketsJson = new JsonArray(buckets);
        } else {
            bucketsJson = new JsonArray();
        }

        return bucketsJson;
    }

    /**
     * 删除某用户指定的桶
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg deleteBucket(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);

        if (paramMap.containsKey(DOUBLE_FLAG) && paramMap.containsKey(VERSION_NUM)) {
            String versionNum = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, VERSION_NUM);
            if (!paramMap.get(VERSION_NUM).equals(versionNum)) {
                return new ResponseMsg().setHttpCode(DEL_SUCCESS);
            }
        }
        /* 删除bucket之前的检查 */
        Map<String, String> bucketInfo = getBucketMapByName(paramMap);
        String bucketUserId = bucketInfo.get("user_id");
        String bucketUserName = bucketInfo.get("user_name");
        String bucketSite = bucketInfo.get(CLUSTER_NAME);

        // 多站点环境下，未启用数据同步功能的桶，只允许在桶的所属站点删除桶
        if (!paramMap.containsKey(REGION_FLAG) && !paramMap.containsKey(REGION_FLAG.toLowerCase())
                && !paramMap.containsKey(SITE_FLAG) && !paramMap.containsKey(SITE_FLAG.toLowerCase())) {
            if (HeartBeatChecker.isMultiAliveStarted && !isSwitchOn(bucketInfo) && !LOCAL_SITE.equals(bucketSite)) {
                throw new MsException(INVALID_SITE_CONSTRAINT, "can't delete the bucket with the specified site in this site.");
            }
        }

        String uploadNr = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "upload_nr");
        String method = "DeleteBucket";
        String bucketPolicy = bucketName + "_meta";

        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        final boolean offSyncSwitch = "off".equals(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, DATA_SYNC_SWITCH));
        deleteBucketCheck(userId, bucketUserId, uploadNr, offSyncSwitch);
        // 桶否存在已创建快照或正在删除中的快照
        if (hasSnapshotInBucket(bucketName)) {
            throw new MsException(BUCKET_SNAPSHOT_EXIST, "Can not delete bucket,snapshot exist in bucket.");
        }
        boolean hasObject = ECUtils.hadObject(bucketName).block();
        if (hasObject) {
            if (offSyncSwitch) {
                throw new MsException(OBJ_EXIST, "Can not delete bucket,object exist in bucket.");
            }
            // 使用数组映射 res 到对应错误码
            final int[] errorCodes = {OBJ_EXIST_0, OBJ_EXIST_1, OBJ_EXIST_2};

            // 防止 res 超出数组范围
            int errorNum = (LOCAL_CLUSTER_INDEX >= 0 && LOCAL_CLUSTER_INDEX < errorCodes.length) ? errorCodes[LOCAL_CLUSTER_INDEX] : OBJ_EXIST;
            throw new MsException(errorNum, "Can not delete bucket, object exist in bucket, cluster " + LOCAL_CLUSTER_INDEX + ".");
        }
        //关闭桶的NFS
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        boolean isMasterCluster = StringUtils.isEmpty(masterCluster) || localCluster.equals(masterCluster);
        paramMap.put("ctime", Long.toString(System.currentTimeMillis()));

        /**从站点请求主站点执行**/
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_DELETE_BUCKET, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(DEL_SUCCESS);
        }
        /**主站点执行多区域转发**/
        if (isMasterCluster && !MultiRegionUtils.dealRegionsDeleteBucket(paramMap)) {
            return new ResponseMsg().setHttpCode(DEL_SUCCESS);
        }


        if (!"1".equals(paramMap.get(SITE_FLAG)) && !"1".equals(paramMap.get(SITE_FLAG.toLowerCase())) && !offSyncSwitch) {
            final Integer res = checkSiteBucketCap(bucketName).block();
            if (res != null && res != -1) {
                if (res == -2) {
                    throw new MsException(UNKNOWN_ERROR, "Check other cluster request error in " + bucketName + ".");
                }
                // 使用数组映射 res 到对应错误码
                final int[] errorCodes = {OBJ_EXIST_0, OBJ_EXIST_1, OBJ_EXIST_2};

                // 防止 res 超出数组范围
                int errorNum = (res >= 0 && res < errorCodes.length) ? errorCodes[res] : OBJ_EXIST;
                throw new MsException(errorNum, "Can not delete bucket, object exist in bucket, cluster " + res + ".");
            }
        }

        final boolean lock = acquireLock(bucketName, 1000);
        try {
            if (!lock) {
                throw new MsException(UNKNOWN_ERROR, "get delete bucket lock error!");
            }

            if (ShardingWorker.bucketHasShardingTask(bucketName)) {
                throw new MsException(UNKNOWN_ERROR, "The bucket " + bucketName + " is being hash!");
            }

            // 如果开启NFS，释放相关资源
            if (bucketFsCheck(bucketName)) {
                startOrStopNfs(bucketName, false, false, false);
                FSQuotaUtils.delQuotas(bucketName, "");
                IpWhitelistUtils.deleteNFSIpWhitelists(bucketName);
            }

            long delCode = pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).del(bucketName);
            if (-1 == delCode) {
                throw new MsException(UNKNOWN_ERROR, "deleteBucket delete db bucket info error, bucket_name:"
                        + bucketName + ", bucket_user:" + bucketUserId);
            }
            // 删除bucket下的所有失败记录
            ComponentErrorService.getInstance().deleteBucketComponentErrors(bucketName);

            BucketLifecycleService.deleteLifecycleConfig(bucketName);
            BucketLifecycleService.deleteLifecycleRecord(bucketName);
            BucketLifecycleService.deleteBucketArchiveCount(bucketName, null);
            BucketLifecycleService.deleteBackupConfig(bucketName);
            BucketLifecycleService.deleteBackupRecord(bucketName);
            InventoryService.deleteBucketInventoryConfiguration(bucketName);
            BucketPolicyService.deleteBucketPolicy(bucketName);
            deleteBucketGrantAcl(bucketName);
            //移除user_bucket列表
            pool.getShortMasterCommand(REDIS_USERINFO_INDEX).srem(userId + USER_BUCKET_SET_SUFFIX, bucketName);
            //删除桶后,删去桶的占用容量的结果
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);

            List<String> bucketVnodeList = storagePool.getBucketVnodeList(bucketName);
            Flux.fromIterable(bucketVnodeList)
                    .flatMap(vnode -> storagePool.mapToNodeInfo(vnode)
                            .flatMap(nodeList -> ErasureClient.deleteRocketsValue(bucketName, BucketInfo.getCpacityKey(vnode, bucketName), nodeList)
                                    .flatMap(b -> ErasureClient.deleteRocketsValue(bucketName, BucketInfo.getObjNumKey(vnode, bucketName), nodeList))))
                    .subscribe();

            deleteBucketShardInfo(bucketName);
            // 桶被删除时需要及时检测更新账户的配额标记
            QuotaRecorder.addCheckAccount(bucketUserName);
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd("del_bucket_set", bucketName);

            int resCode = notifySlaveSite(paramMap, ACTION_DELETE_BUCKET);
            if (resCode != SUCCESS_STATUS) {
                throw new MsException(resCode, "master delete bucket error");
            }
            MultiRegionUtils.deleteBucketToCenter(paramMap, isMasterCluster);

            //删除桶时将已配置的桶通知相关配置删除
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel("bucket_notification", bucketName);
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem(ES_BUCKET_SET, bucketName);
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(bucketName + "_clear_model");
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel(CORS_CONFIG_KEY, bucketName);
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel(BUCKET_TAG_KEY, bucketName);

            BucketShardCache.removeBucketCache(bucketName).block(Duration.ofSeconds(60));
            NFSBucketInfo.removeBucketInfo(bucketName);

            log.info("delete bucket {} successfully.", bucketName);
        } finally {
            if (lock) {
                releaseLock(bucketName);
            }
        }

//        Optional.ofNullable(AddClusterHandler.bucketsList).ifPresent(bucketsList -> bucketsList.remove(bucketName));
        return new ResponseMsg().setHttpCode(DEL_SUCCESS);
    }

    private Mono<Integer> checkSiteBucketCap(String bucketName) {
        // 过滤出需要检查的站点
        Set<Integer> indexSet = INDEX_NAME_MAP.keySet().stream()
                .filter(index -> !LOCAL_CLUSTER_INDEX.equals(index))
                .filter(index -> !EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(index))
                .collect(Collectors.toSet());

        if (indexSet.isEmpty()) {
            // 没有其他站点要检查
            return Mono.just(-1);
        }

        // 并发检查所有站点，任意一个返回false就取那个index返回
        return Flux.fromIterable(indexSet)
                .flatMap(index -> checkClusterBucket(bucketName, index)
                        .flatMap(valid -> valid == 0 ? Mono.empty() : Mono.just(index))
                        .onErrorResume(e -> {
                            log.error("check bucket {} failed at cluster {}: {}", bucketName, index, e.getMessage());
                            return Mono.just(-2);
                        })
                )
                .next() // 只取第一个出问题的index
                .defaultIfEmpty(-1);
    }

    private Mono<Integer> checkClusterBucket(String bucketName, Integer index) {
        return Mono.create(sink -> {
            PoolingRequest poolingRequest = new PoolingRequest(HttpMethod.GET, index, "?checkBucketCapacity", getClient());
            poolingRequest.setAuthString(INNER_AUTH);
            poolingRequest.request().subscribe(request -> {
                request.putHeader(IS_SYNCING, "1");
                request.putHeader("bucketName", bucketName);

                request.exceptionHandler(e -> {
                    log.error("check bucket {} error at cluster {}: {}", bucketName, index, e.getMessage());
                    sink.success(-2);
                }).setTimeout(30 * 1000).handler(resp -> {
                    if (SUCCESS == resp.statusCode()) {
                        String checkRes = resp.getHeader("checkRes");
                        if ("1".equals(checkRes)) {
                            log.error("bucket {} snapshot exists in cluster {}", bucketName, index);
                            sink.success(1);
                        } else {
                            sink.success(0);
                        }
                    } else {
                        log.debug("check obj error: bucket {} cluster {}", bucketName, index);
                        sink.success(-2);
                    }
                });

                request.end();
            }, e -> {
                log.error("check obj request failed for bucket {} cluster {}", bucketName, index, e);
                sink.success(-2);
            });
        });
    }

    /**
     * 元数据分析开关
     *
     * @param paramMap 请求参数(on or off)
     * @return 结果
     */
    public ResponseMsg putMetadataAnalysisSwitch(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        String switchAction = paramMap.get(METADATA_ANALYSIS);

        /**从站点请求主站点执行**/
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_PUT_METADATA, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(SUCCESS);
        }

        // 判断用户是否拥有权限或者是否为桶的所有者
        String method = "PutMetadataAnalysisSwitch";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketInfo.get(BUCKET_USER_ID));
        }

        //检查桶是否存在
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket: " + bucketName);
        }

        // 判断是否存在检索池
        if ("on".equals(switchAction)) {
            String operationUser = pool.getCommand(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_ID_NAME);
            String strategy = pool.getCommand(REDIS_USERINFO_INDEX).hget(operationUser, "storage_strategy");
            JSONArray esArray = JSONArray.parseArray(Optional.ofNullable(pool.getCommand(REDIS_POOL_INDEX).hget(strategy, "es")).orElse("[]"));
            if (esArray.size() == 0) {
                throw new MsException(NO_SUPPORT_METADATA_SEARCH, "The bucket does not support metadata search.");
            }
        }

        if ("on".equals(switchAction)) {
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(ES_BUCKET_SET, bucketName);
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, "mda", "on");
        } else if ("off".equals(switchAction)) {
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem(ES_BUCKET_SET, bucketName);
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, "mda", "off");
        } else {
            throw new MsException(INVALID_ARGUMENT, "switch parameter input error.");
        }

        int res = notifySlaveSite(paramMap, ACTION_PUT_BUCKET_METADATA);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave put bucket version error!");
        }
        return new ResponseMsg();
    }


    /**
     * 获取元数据分析开关
     *
     * @param paramMap 请求参数
     * @return on or off
     */
    public ResponseMsg getMetadata(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);

        Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }

        String method = "GetBucketMetadataSwitch";

        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        if (policyResult == 0) {
            MsAclUtils.checkReadAcl(bucketInfo, userId, bucketName);
        }

        String value = Optional.ofNullable(bucketInfo.get("mda")).orElse("off");
        List<Grant> list = new ArrayList<>(1);
        Grant grant = new Grant().setPermission(value);
        list.add(grant);
        AccessControlPolicy accessControlPolicy = new AccessControlPolicy().setAccessControlList(list);

        return new ResponseMsg().setData(accessControlPolicy).addHeader(METADATA_ANALYSIS, value);
    }

    /**
     * 获取桶的数据同步开关
     *
     * @param paramMap 请求参数
     * @return on or off
     */
    public ResponseMsg getBucketDataSynchronizationSwitch(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);

        Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }

        userCheck(userId, bucketInfo.get(BUCKET_USER_ID));
        String value = getBucketSyncStatus(bucketInfo);
        return new ResponseMsg().addHeader(DATA_SYNC_SWITCH, value);
    }

    /**
     * 设置桶权限
     *
     * @param paramMap 请求参数
     * @return 相应的返回值
     */
    public ResponseMsg putBucketAcl(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String contentLength = paramMap.get(LOWER_CASE_CONTENT_LENGTH);
        String acl = paramMap.get("x-amz-acl");

        getBucketMapByName(bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

        MsAclUtils.checkIfAnonymous(userId);
        FastList<Entry<String, String>> grantAclList = new FastList<>(16);
        paramMap.forEach((k, v) -> {
            if (k.startsWith("x-amz-grant-")) {
                for (String value : v.split(",")) {
                    String[] id = value.split("=", 2);
                    if (id.length != 2) {
                        throw new MsException(INVALID_ARGUMENT, "acl input error.");
                    }
                    grantAclList.add(new Entry<>(id[1], k));
                }
            }
        });
        if (StringUtils.isNotBlank(acl) && grantAclList.size() > 0) {
            throw new MsException(INVALID_REQUEST, "the acl input is invlid");
        }
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        paramMap.put("ctime", Long.toString(System.currentTimeMillis()));
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_SET_ACL, localCluster, masterCluster)) {
            return new ResponseMsg(SUCCESS_STATUS);
        }

        ResponseMsg responseMsg;
        if (0 == Integer.parseInt(contentLength)) {
            if (StringUtils.isNotBlank(acl)) {
                responseMsg = setAcl(userId, bucketName, paramMap);
            } else if (grantAclList.size() > 0) {
                responseMsg = setGrantAcl(userId, bucketName, grantAclList, paramMap);
            } else {
                throw new MsException(MISSING_SECURITY_HEADER, "missing required acl header");
            }
        } else {
            if (StringUtils.isNotBlank(acl) || grantAclList.size() > 0) {
                throw new MsException(UNEXPECTEDCONTENT, "set acl does not support both header and content");
            }
            AccessControlPolicy result = (AccessControlPolicy) JaxbUtils.toObject(paramMap.get(BODY));
            checkOwner(result.getOwner().getId(), result.getOwner().getDisplayName(), bucketName);
            List<Grant> grantList = result.getAccessControlList();
            FastList<Entry<String, String>> xmlList = new FastList<>(grantList.size());
            for (Grant grant : grantList) {
                Entry<String, String> xmlEntry = new Entry<>(null, null);
                String userName = grant.getGrantee().getDisplayName();
                String id = grant.getGrantee().getId();
                checkIdAndName(id, userName);
                xmlEntry.setKey(grant.getGrantee().getId());
                xmlEntry.setValue(grant.getPermission());
                xmlList.add(xmlEntry);
            }
            responseMsg = setGrantAcl(userId, bucketName, xmlList, paramMap);
        }
        int res = notifySlaveSite(paramMap, ACTION_PUT_BUCKET_ACL);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave set acl error,roll back!");
        }
        return responseMsg;
    }

    /**
     * 设置桶的对象数配额
     *
     * @param paramMap 请求参数
     * @return 响应消息
     */
    public ResponseMsg putBucketObjectsQuota(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);

        HashMap<String, String> map = new HashMap<>();
        String bucketHardObjects = paramMap.get("bucket-hard-objects");
        String bucketSoftObjects = paramMap.get("bucket-soft-objects");

        if (StringUtils.isEmpty(bucketHardObjects) && StringUtils.isEmpty(bucketSoftObjects)) {
            throw new MsException(INVALID_ARGUMENT, "objects and soft objects choose at least one of the two.");
        }
        if (StringUtils.isNotEmpty(bucketHardObjects)) {
            checkBucketObjectsQuota(bucketHardObjects);
        }
        if (StringUtils.isNotEmpty(bucketSoftObjects)) {
            checkBucketObjectsQuota(bucketSoftObjects);
            if (bucketSoftObjects.equals("0")) {
                String softObjnumValue = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "soft_objnum_value")).orElse("0");
                if (!softObjnumValue.equals("0")) {
                    Long exists = pool.getCommand(REDIS_SYSINFO_INDEX).exists(bucketName + "_clear_model");
                    if (exists != 0) {
                        throw new MsException(EXIST_CLEAN_MODEL_CONFIGURATION, "bucket quota can not be set when clear model is enabled.");
                    }
                }
            }
        }

        /**
         *  从站点请求主站点执行
         */
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        paramMap.put("bucket-hard-objects", bucketHardObjects);
        paramMap.put("bucket-soft-objects", bucketSoftObjects);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_PUT_BUCKET_OBJECTS, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(SUCCESS);
        }

        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

        userCheck(userId, bucketInfo.get(BUCKET_USER_ID));
        String method = "PutBucketObjectsQuota";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        if (StringUtils.isNotEmpty(bucketHardObjects) && StringUtils.isNotEmpty(bucketSoftObjects)
                && !"0".equals(bucketHardObjects)
                && Long.parseLong(bucketSoftObjects) >= Long.parseLong(bucketHardObjects)) {
            throw new MsException(INVALID_ARGUMENT, "bucket soft objects must less than hard objects.");
        }

        if (StringUtils.isNotEmpty(bucketSoftObjects) && StringUtils.isEmpty(bucketHardObjects)) {
            String objNumValue = Optional.ofNullable(bucketInfo.get("objnum_value"))
                    .orElse("0");
            if (!"0".equals(objNumValue) && (Long.parseLong(bucketSoftObjects) >= Long.parseLong(objNumValue))) {
                throw new MsException(INVALID_ARGUMENT, "bucket soft objects must less than hard objects.");
            }
        }

        if (StringUtils.isNotEmpty(bucketHardObjects) && StringUtils.isEmpty(bucketSoftObjects)) {
            String softQuotaValue = Optional.ofNullable(bucketInfo.get("soft_objnum_value")).orElse("0");
            if (!"0".equals(bucketHardObjects) && Long.parseLong(softQuotaValue) >= Long.parseLong(bucketHardObjects)) {
                throw new MsException(INVALID_ARGUMENT, "bucket soft objects must less than hard objects.");
            }
        }

        String objectsNumFlag = bucketInfo.get("objnum_flag");

        MonoProcessor<String> res = MonoProcessor.create();
        ErasureClient.reduceBucketInfo(bucketName)
                .filter(BucketInfo::isAvailable)
                .map(BucketInfo::getObjectNum)
                .doOnError(e -> logger.error("get bucketInfo error,{}", e))
                .defaultIfEmpty("0")
                .subscribe(res::onNext);
        String bucketObjectNum = res.block();
        logger.debug("已使用桶对象数量：" + bucketObjectNum);


        //设置桶的对象数配额
        if (StringUtils.isNotEmpty(bucketHardObjects)) {
            if ("0".equals(bucketHardObjects) || Long.parseLong(bucketHardObjects) > Long.parseLong(bucketObjectNum)) {
                //如果新设置的配额大于已使用容量或设置为0，flag调整为0，表示允许上传
                map.put("objnum_flag", "0");
                objectsNumFlag = "0";

                // 判断已使用对象数是否已经超过10亿
                if (Long.parseLong(bucketObjectNum) >= BUCKET_MAX_OBJECTS_QUOTA) {
                    map.put("objnum_flag", "1");
                    objectsNumFlag = "1";
                } else {
                    map.put("objnum_flag", "0");
                    objectsNumFlag = "0";
                }

            } else if (StringUtils.isNotBlank(bucketObjectNum) && Long.parseLong(bucketHardObjects) <= Long.parseLong(bucketObjectNum)) {
                //新设置配额值小于已使用容量，设置为桶不可用
                map.put("objnum_flag", "1");
                objectsNumFlag = "1";
            }
        }

        if (!"1".equals(objectsNumFlag) && StringUtils.isNotEmpty(bucketSoftObjects)) {
            boolean exceedDefaultSoftQuota = StringUtils.isNotBlank(bucketObjectNum) && Long.parseLong(bucketObjectNum) >= SOFT_OBJECTS_ALARM_THRESHOLD;
            if ("0".equals(bucketSoftObjects) && !exceedDefaultSoftQuota || Long.parseLong(bucketSoftObjects) > Long.parseLong(bucketObjectNum)) {
                map.put("objnum_flag", "0");
            } else if ("0".equals(bucketSoftObjects) && exceedDefaultSoftQuota || StringUtils.isNotBlank(bucketObjectNum) && Long.parseLong(bucketSoftObjects) <= Long.parseLong(bucketObjectNum)) {
                map.put("objnum_flag", "5");
            }
        }

        if (StringUtils.isNotEmpty(bucketHardObjects)) {
            map.put("objnum_value", bucketHardObjects);
        }
        if (StringUtils.isNotEmpty(bucketSoftObjects)) {
            map.put("soft_objnum_value", bucketSoftObjects);
        }

        logger.info("objects quota info:" + map);
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hmset(bucketName, map);

        int resp = notifySlaveSite(paramMap, ACTION_PUT_BUCKET_OBJECTS);
        if (resp != SUCCESS_STATUS) {
            throw new MsException(resp, "slave put bucket objects error!");
        }

        return new ResponseMsg();
    }

    /**
     * 获取桶对象数的配额信息
     *
     * @param paramMap
     * @return
     */
    public ResponseMsg getBucketObjectsQuota(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);

        userCheck(userId, bucketInfo.get(BUCKET_USER_ID));
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        String method = "GetBucketObjectsQuota";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        String objNumValue = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "objnum_value"))
                .orElse("0");
        String softObjNumValue = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "soft_objnum_value"))
                .orElse("0");

        return new ResponseMsg().addHeader("bucket-hard-objects", objNumValue)
                .addHeader("bucket-soft-objects", softObjNumValue);
    }

    /**
     * 设置桶配额
     *
     * @param paramMap 请求参数
     * @return 响应消息
     */
    public ResponseMsg putBucketQuota(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String bucketQuota = paramMap.get("bucket-quota");
        String bucketSoftQuota = paramMap.get("bucket-soft-quota");

        if (StringUtils.isEmpty(bucketQuota) && StringUtils.isEmpty(bucketSoftQuota)) {
            throw new MsException(BUCKET_QUOTA_INPUT_ERR, "quota and soft quota choose at least one of the two.");
        }
        if (StringUtils.isNotEmpty(bucketQuota)) {
            checkBucketQuota(bucketQuota);
        }
        if (StringUtils.isNotEmpty(bucketSoftQuota)) {
            checkBucketQuota(bucketSoftQuota);
            if (bucketSoftQuota.equals("0")) {
                String softQuotaValue = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "soft_quota_value")).orElse("0");
                if (!softQuotaValue.equals("0")) {
                    Long exists = pool.getCommand(REDIS_SYSINFO_INDEX).exists(bucketName + "_clear_model");
                    if (exists != 0) {
                        throw new MsException(EXIST_CLEAN_MODEL_CONFIGURATION, "bucket quota can not be set 0 when clear model is enabled.");
                    }
                }
            }
        }

        /**
         *  从站点请求主站点执行
         */
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        paramMap.put("bucket-quota", bucketQuota);
        paramMap.put("bucket-soft-quota", bucketSoftQuota);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_PUT_BUCKET_QUOTA, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(SUCCESS);
        }

        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

        userCheck(userId, bucketInfo.get(BUCKET_USER_ID));
        String method = "PutBucketQuota";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        if (StringUtils.isNotEmpty(bucketQuota) && StringUtils.isNotEmpty(bucketSoftQuota)
                && !"0".equals(bucketQuota)
                && Long.parseLong(bucketSoftQuota) >= Long.parseLong(bucketQuota)) {
            throw new MsException(BUCKET_QUOTA_INPUT_ERR, "bucket soft quota must less than hard quota.");
        }

        if (StringUtils.isNotEmpty(bucketSoftQuota) && StringUtils.isEmpty(bucketQuota)) {
            String quotaValue = Optional.ofNullable(bucketInfo.get("quota_value"))
                    .orElse("0");
            if (!"0".equals(quotaValue) && (Long.parseLong(bucketSoftQuota) >= Long.parseLong(quotaValue))) {
                throw new MsException(BUCKET_QUOTA_INPUT_ERR, "bucket soft quota must less than hard quota.");
            }
        }

        if (StringUtils.isNotEmpty(bucketQuota) && StringUtils.isEmpty(bucketSoftQuota)) {
            String softQuotaValue = Optional.ofNullable(bucketInfo.get("soft_quota_value")).orElse("0");
            if (!"0".equals(bucketQuota) && Long.parseLong(softQuotaValue) >= Long.parseLong(bucketQuota)) {
                throw new MsException(BUCKET_QUOTA_INPUT_ERR, "bucket soft quota must less than hard quota.");
            }
        }

        // 获取账户的配额标记
        String quotaFlag = bucketInfo.get("quota_flag");
        HashMap<String, String> map = new HashMap<>();

        // 获取桶的已使用容量
        MonoProcessor<String> res = MonoProcessor.create();
        ErasureClient.reduceBucketInfo(bucketName)
                .filter(BucketInfo::isAvailable)
                .map(BucketInfo::getBucketStorage)
                .doOnError(e -> logger.error("get bucketInfo error,{}", e))
                .defaultIfEmpty("0")
                .subscribe(res::onNext);
        String bucketStorage = res.block();
        logger.debug("已使用桶容量：" + bucketStorage);

        // 调整硬配额标记 如果账户配额之前已经满了,就不调整flag
        if (!StringUtils.isBlank(bucketQuota)) {
            if ("0".equals(bucketQuota) || Long.parseLong(bucketQuota) > Long.parseLong(bucketStorage)) {
                //如果新设置的配额大于已使用容量或设置为0，flag调整为0，表示允许上传
                map.put("quota_flag", "0");
                quotaFlag = "0";
            } else if (StringUtils.isNotBlank(bucketStorage) && Long.parseLong(bucketQuota) <= Long.parseLong(bucketStorage)) {
                //新设置的硬配额值小于已使用容量，设置为桶不可用
                map.put("quota_flag", "1");
                quotaFlag = "1";
            }
        }

        // 调整软配额标记 如果在之前账户配额或者桶的硬配额已经满了，则不调整flag
        if (!"1".equals(quotaFlag) && StringUtils.isNotBlank(bucketSoftQuota)) {
            if ("0".equals(bucketSoftQuota) || Long.parseLong(bucketSoftQuota) > Long.parseLong(bucketStorage)) {
                //如果新设置的配额大于已使用容量或设置为0，flag调整为0
                map.put("quota_flag", "0");
            } else if (StringUtils.isNotBlank(bucketStorage) && Long.parseLong(bucketSoftQuota) <= Long.parseLong(bucketStorage)) {
                //新设置的软配额值小于已使用容量，表示桶的使用量已经超过桶的软配额
                map.put("quota_flag", "5");
            }
        }

        if (StringUtils.isNotBlank(bucketQuota)) {
            map.put("quota_value", bucketQuota);
        }
        if (StringUtils.isNotBlank(bucketSoftQuota)) {
            map.put("soft_quota_value", bucketSoftQuota);
        }

        logger.info("capacity quota info:" + map);
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hmset(bucketName, map);

        int resp = notifySlaveSite(paramMap, ACTION_PUT_BUCKET_QUOTA);
        if (resp != SUCCESS_STATUS) {
            throw new MsException(resp, "slave put bucket quota error!");
        }

        return new ResponseMsg();
    }

    /**
     * 获取桶配额
     *
     * @param paramMap 请求参数
     * @return 响应消息
     */
    public ResponseMsg getBucketQuota(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

        userCheck(userId, bucketInfo.get(BUCKET_USER_ID));
        String method = "GetBucketQuota";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        String quotaValue = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "quota_value"))
                .orElse("0");
        String softQuotaValue = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "soft_quota_value"))
                .orElse("0");
        return new ResponseMsg().addHeader("bucket-quota", quotaValue)
                .addHeader("bucket-soft-quota", softQuotaValue);
    }

    /**
     * 获取bucket权限
     *
     * @param paramMap 请求参数
     * @return 相应的xml
     */
    public ResponseMsg getBucketAcl(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        String method = "GetBucketAcl";

        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        if (policyResult == 0) {
            MsAclUtils.checkIfAnonymous(userId);
        }

        int aclNum = Integer.parseInt(bucketInfo.get(BUCKET_ACL));
        String bucketUserId = bucketInfo.get(BUCKET_USER_ID);
        if (policyResult == 0) {
            MsAclUtils.checkGrantAcl(aclNum, bucketUserId, userId, bucketName, PERMISSION_READ_CAP_NUM, PERMISSION_READ_CAP);
        }
        String bucketUser = bucketInfo.get(BUCKET_USER_NAME);

        Grantee ownerGrantee = new Grantee()
                .setId(bucketUserId)
                .setDisplayName(bucketUser);
        Grant ownerGrant = new Grant()
                .setGrantee(ownerGrantee)
                .setPermission(PERMISSION_FULL_CON);
        List<Grant> list = new FastList<Grant>(16).with(ownerGrant);

        Grantee bucketGrantee = new Grantee()
                .setId("")
                .setDisplayName("ALLUsers");
        if ((aclNum & PERMISSION_SHARE_READ_NUM) != 0) {
            Grant bucketGrant = new Grant()
                    .setGrantee(bucketGrantee)
                    .setPermission(PERMISSION_READ);
            list.add(bucketGrant);
        } else if ((aclNum & PERMISSION_SHARE_READ_WRITE_NUM) != 0) {
            Grant bucketGrant = new Grant()
                    .setGrantee(bucketGrantee)
                    .setPermission(PERMISSION_READ);
            list.add(bucketGrant);

            bucketGrant = new Grant()
                    .setGrantee(bucketGrantee)
                    .setPermission(PERMISSION_WRITE);
            list.add(bucketGrant);
        } else if ((aclNum & PERMISSION_PRIVATE_NUM) == 0) {
            throw new MsException(UNKNOWN_ERROR, "Get bucket acl type error.");
        }

        getBucketGrantAcl(aclNum, bucketName, bucketUser, list);

        Owner owner = new Owner()
                .setId(bucketUserId)
                .setDisplayName(bucketUser);
        AccessControlPolicy accessControlPolicy = new AccessControlPolicy()
                .setOwner(owner)
                .setAccessControlList(list);
        logger.debug("Get bucket acl successful, bucketName:" + bucketName);
        return new ResponseMsg().setData(accessControlPolicy).addHeader(CONTENT_TYPE, "application/xml");
    }

    /**
     * 设置多版本状态
     *
     * @param paramMap
     * @return 响应消息
     */
    public ResponseMsg putBucketVersioning(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        //判断桶内是否有保护对象或者分段任务
        try {
            //获取请求头XML中对象列表
            BucketVersioning bucketVersioning = (BucketVersioning) JaxbUtils.toObject(paramMap.get("body"));
            String status = bucketVersioning.getStatus();
            if (!"Enabled".equals(status) && !"Suspended".equals(status)) {
                throw new MsException(InvalidBucketState, "The bucket state of version is invalid");
            }
            String bucketVersionQuota = paramMap.get(BUCKET_VERSION_QUOTA);
            int versionLimitNumber = checkNumberInRange(bucketVersionQuota);

            TrashUtils.checkEnvTrash(bucketName, pool);
            if (bucketFsCheck(bucketName)) {
                throw new MsException(NFS_NOT_STOP, "The bucket already start nfs or cifs, can not enable version");
            }
            /**从站点请求主站点执行**/
            String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
            String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
            paramMap.put(BUCKET_VERSION_STATUS, status);
            paramMap.put(BUCKET_VERSION_QUOTA, String.valueOf(versionLimitNumber));
            if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_PUT_VERSION, localCluster, masterCluster)) {
                return new ResponseMsg().setHttpCode(SUCCESS);
            }

            String method = "PutBucketVersioning";
            int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

            //判断用户是否是桶的所有者
            DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
            userCheck(userId, bucketInfo.get(BUCKET_USER_ID));
            // 开启了worm功能的桶不能暂停关闭多版本
            if ("Suspended".equals(status) && WormUtils.checkBucketWormEnable(bucketName)) {
                throw new MsException(WORM_STATE_CONFLICT, "The request is not valid with the current state of the bucket");
            }
            HashMap<String, String> map = new HashMap<>(3);
            map.put(BUCKET_VERSION_STATUS, status);
            map.put(BUCKET_VERSION_QUOTA, String.valueOf(versionLimitNumber));
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hmset(bucketName, map);

            int res = notifySlaveSite(paramMap, ACTION_PUT_BUCKET_VERSION);
            if (res != SUCCESS_STATUS) {
                throw new MsException(res, "slave put bucket version error!");
            }
            return new ResponseMsg();
        } catch (NullPointerException e) {
            throw new MsException(InvalidBucketState, "The bucket state of version is invalid");
        }
    }

    private int checkNumberInRange(String quota) {
        try {
            if (StringUtils.isEmpty(quota)) {
                return 0;
            }
            int i = Integer.parseInt(quota);
            if (i < 0 || i > BUCKET_VERSION_QUOTA_DEFAULT) {
                throw new MsException(ErrorNo.InvalidBucketVersionQuota, "version quota is invalid.");
            }
            return i;
        } catch (NumberFormatException e) {
            throw new MsException(ErrorNo.InvalidBucketVersionQuota, "version quota is invalid.");
        }
    }

    /**
     * 获取桶的多版本状态
     *
     * @param paramMap
     * @return xml
     */
    public ResponseMsg getBucketVersioning(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        //判断用户是否是桶的所有者
        userCheck(userId, bucketInfo.get(BUCKET_USER_ID));
        String method = "GetBucketVersioning";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        String status = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, BUCKET_VERSION_STATUS))
                .orElse("NULL");

        String quota = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, BUCKET_VERSION_QUOTA))
                .orElse("0");

        //判断多版本状态的是否无效
        if (!status.equals("Enabled") && !status.equals("Suspended") && !status.equals("NULL")) {
            throw new MsException(InvalidBucketState, "The bucket state of version is invalid");
        }
        BucketVersioning versioning = new BucketVersioning();
        if (status.equals("NULL")) {
            return new ResponseMsg().setData(versioning).addHeader(CONTENT_TYPE, "application/xml");
        } else {
            versioning.setStatus(status);
            return new ResponseMsg().setData(versioning).addHeader(BUCKET_VERSION_QUOTA, quota).addHeader(CONTENT_TYPE, "application/xml");
        }
    }

    private void createBucketInternal(UnifiedMap<String, String> paramMap) {

        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);

        createBucketCheck(userId, bucketName);

        if (ShardingWorker.bucketHasShardingTask(bucketName)) {
            throw new MsException(BUCKET_EXISTS, "A bucket " + bucketName + " is being hash!");
        }

        //获得创建桶需要的数据
        Map<String, String> bucketMap = getBucketMap(paramMap);
        String bucketLocation = MultiRegionUtils.getBucketLocation(paramMap);
        if (StringUtils.isNotEmpty(bucketLocation)) {
            bucketMap.put(REGION_NAME, bucketLocation);
        }

        // 获取桶的站点属性
        String bucketSite = DoubleActiveUtil.getBucketSite(paramMap);
        if (StringUtils.isNotEmpty(bucketSite)) {
            bucketMap.put(CLUSTER_NAME, bucketSite);
            paramMap.put(SITE_NAME, bucketSite);
        }

        //查看其他桶账户配额状态，如果已满就设置为2
        Set<String> bucketSet = pool.getCommand(REDIS_USERINFO_INDEX).smembers(userId + "bucket_set");
        for (String bucket : bucketSet) {
            String quotaFlag = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, "quota_flag");
            if ("2".equals(quotaFlag)) {
                bucketMap.put("quota_flag", "2");
            }
            break;
        }

        //启用对象锁定会自动启用存储桶版本控制,并且将永久锁定桶中的对象。
        String objectLock = paramMap.getOrDefault(OBJECT_LOCK_ENABLED, "off");
        if ("on".equals(objectLock)) {
            bucketMap.put(BUCKET_VERSION_STATUS, "Enabled");
            // 默认设置1天保护期
            bucketMap.put("lock_periodic", "day:1");
            bucketMap.put("lock_status", "Enabled");
        }

        bucketMap.put("mda", paramMap.getOrDefault(METADATA_ANALYSIS, "off"));
        bucketMap.put(DATA_SYNC_SWITCH, paramMap.getOrDefault(DATA_SYNC_SWITCH, "off"));
        addBucketDns(bucketName, bucketLocation);

        // 设置桶的版本号
        String versionNum = paramMap.containsKey(DOUBLE_FLAG) ? paramMap.get("version_num") : VersionUtil.getVersionNum();
        bucketMap.put("version_num", versionNum);

        // 设置桶快照功能
        AtomicBoolean startSnapshot = new AtomicBoolean(false);
        String snapshotSwitch = paramMap.getOrDefault(SNAPSHOT_SWITCH, "off");
        if ("on".equals(snapshotSwitch)) {
            bucketMap.put(CURRENT_SNAPSHOT_MARK, MIN_SNAPSHOT_MARK);
            startSnapshot.set(true);
        }
        bucketMap.put(SNAPSHOT_SWITCH, snapshotSwitch);
        // 在内存中初始化桶的存储策略以及桶散列的信息
        MonoProcessor<Boolean> resMono = MonoProcessor.create();
        String storageStrategy = bucketMap.get("storage_strategy");

        String ak = paramMap.getOrDefault(SERVER_AK, "");
        String sk = paramMap.getOrDefault(SERVER_SK, "");

        // 是否开启NFS
        String nfsSwitch = paramMap.get(NFS_SWITCH);
        String cifsSwitch = paramMap.get(CIFS_SWITCH);
        String ftpSwitch = paramMap.get(FTP_SWITCH);
        boolean nfsOpen = "on".equals(nfsSwitch);
        boolean cifsOpen = "on".equals(cifsSwitch);
        boolean ftpOpen = "on".equals(ftpSwitch);

        //检查开启文件权限时，开启文件协议的桶账户是否配置uid
        ACLUtils.checkAccountFsAcl(userId, bucketName, nfsOpen, cifsOpen, ftpOpen);

        final String syncSwitch = bucketMap.get(DATA_SYNC_SWITCH);
        if ("on".equals(syncSwitch)) {
            if (StringUtils.isNotBlank(ak) && StringUtils.isNotBlank(sk)) {
                bucketMap.put("server_ak", ak);
                bucketMap.put("server_sk", sk);
            }

            if (isNotSyncMessage(paramMap)) {
                // 非多站点etcd同步消息
                if ("0".equals(syncPolicy)) {
                    // 双活，3dc默认
                    // 默认值
                    Map<String, Set<Integer>> syncMap = new HashMap<>();
                    Map<String, Set<Integer>> archiveMap = new HashMap<>();
                    Map<String, int[]> pattern = new HashMap<>();

                    if (NAME_INDEX_MAP.size() == 2) {
                        // 双活
                        pattern.put("0", new int[]{1});
                        pattern.put("1", new int[]{0});
                    } else {
                        // 3dc
                        if ("0".equals(ASYNC_CLUSTERS)) {
                            pattern.put("1", new int[]{0, 2});
                            pattern.put("2", new int[]{0, 1});
                        } else if ("1".equals(ASYNC_CLUSTERS)) {
                            pattern.put("0", new int[]{1, 2});
                            pattern.put("2", new int[]{0, 1});
                        } else {
                            pattern.put("0", new int[]{1, 2});
                            pattern.put("1", new int[]{0, 2});
                        }
                    }

                    for (Map.Entry<String, int[]> entry : pattern.entrySet()) {
                        syncMap.put(entry.getKey(), Arrays.stream(entry.getValue()).boxed().collect(Collectors.toSet()));
                    }
                    final String syncIndex = JSON.toJSONString(syncMap);
                    final String archiveIndex = JSON.toJSONString(archiveMap);
                    paramMap.put(SYNC_INDEX, syncIndex);
                    paramMap.put(ARCHIVE_INDEX, archiveIndex);
                    bucketMap.put(SYNC_INDEX, syncIndex);
                    bucketMap.put(ARCHIVE_INDEX, archiveIndex);
                } else {
                    // 兼容旧方式直接设置索引
                    if (paramMap.containsKey(SYNC_INDEX) || paramMap.containsKey(ARCHIVE_INDEX)) {
                        try {
                            String syncIndex = paramMap.getOrDefault(SYNC_INDEX, "");
                            String archiveIndex = paramMap.getOrDefault(ARCHIVE_INDEX, "");
                            Map<String, Set<Integer>> syncMap = StringUtils.isBlank(syncIndex) ? null : JSON.parseObject(syncIndex, new TypeReference<Map<String, Set<Integer>>>() {
                            });
                            Map<String, Set<Integer>> archiveMap = StringUtils.isBlank(archiveIndex) ? null : JSON.parseObject(archiveIndex, new TypeReference<Map<String, Set<Integer>>>() {
                            });
                            if (syncMap != null) {
                                bucketMap.put(SYNC_INDEX, JSON.toJSONString(syncMap));
                            }
                            if (archiveMap != null) {
                                bucketMap.put(ARCHIVE_INDEX, JSON.toJSONString(archiveMap));
                            }
                        } catch (Exception e) {
                            throw new MsException(INVALID_ARGUMENT, "The synchronous method parameter is invalid, " + bucketName + ".");
                        }
                    } else {
                        if (paramMap.containsKey(SYNC_RELATION)) {
                            Map<String, String> jsonParam;
                            String header = paramMap.get(SYNC_RELATION);
                            try {
                                jsonParam = JSON.parseObject(header, new TypeReference<Map<String, String>>() {});
                            } catch (Exception e) {
                                throw new MsException(INVALID_ARGUMENT, "Invalid JSON in site-sync-relation", e);
                            }
                            if (jsonParam.size() == 0) {
                                throw new MsException(INVALID_ARGUMENT, "Invalid argument in site-sync-relation is null");
                            }
                            Tuple2<Map<String, Set<Integer>>, Map<String, Set<Integer>>> tuple2 = buildIndexes(jsonParam);
                            String syncIndex = JSON.toJSONString(tuple2.var1());
                            String archiveIndex = JSON.toJSONString(tuple2.var2());
                            paramMap.put(SYNC_INDEX, syncIndex);
                            paramMap.put(ARCHIVE_INDEX, archiveIndex);
                            bucketMap.put(SYNC_INDEX, syncIndex);
                            bucketMap.put(ARCHIVE_INDEX, archiveIndex);
                        } else {
                            // 复制默认
                            Map<String, Set<Integer>> syncMap = new HashMap<>();
                            Map<String, Set<Integer>> archiveMap = new HashMap<>();
                            Map<String, int[]> pattern = new HashMap<>();
                            if (NAME_INDEX_MAP.size() == 2) {
                                if (EXTRA_INDEX_IPS_ENTIRE_MAP.size() > 0) {
                                    // 异构
                                    pattern.put("0", new int[]{1});
                                } else {
                                    // 复制
                                    pattern.put("0", new int[]{1});
                                    pattern.put("1", new int[]{0});
                                }
                            } else {
                                if (IS_THREE_SYNC) {
                                    pattern.put("0", new int[]{1, 2});
                                    pattern.put("1", new int[]{0, 2});
                                    pattern.put("2", new int[]{0, 1});
                                } else {
                                    // 复制异构
                                    pattern.put("0", new int[]{1, 2});
                                    pattern.put("1", new int[]{0, 2});
                                }
                            }
                            for (Map.Entry<String, int[]> entry : pattern.entrySet()) {
                                syncMap.put(entry.getKey(), Arrays.stream(entry.getValue()).boxed().collect(Collectors.toSet()));
                            }
                            final String syncIndex = JSON.toJSONString(syncMap);
                            final String archiveIndex = JSON.toJSONString(archiveMap);
                            paramMap.put(SYNC_INDEX, syncIndex);
                            paramMap.put(ARCHIVE_INDEX, archiveIndex);
                            bucketMap.put(SYNC_INDEX, syncIndex);
                            bucketMap.put(ARCHIVE_INDEX, archiveIndex);
                        }
                    }
                }
                logger.info("bucket: {}, syncIndex: {}, archiveIndex: {}", bucketName, paramMap.get(SYNC_INDEX), paramMap.get(ARCHIVE_INDEX));
            } else {
                if (!paramMap.containsKey(ARCHIVE_INDEX) && !paramMap.containsKey(SYNC_INDEX)) {
                    throw new MsException(INVALID_ARGUMENT, "Create bucket " + bucketName + " is missing the synchronous method parameter.");
                }

                try {
                    String syncIndex = paramMap.getOrDefault(SYNC_INDEX, "");
                    String archiveIndex = paramMap.getOrDefault(ARCHIVE_INDEX, "");
                    Map<String, Set<Integer>> syncMap = StringUtils.isBlank(syncIndex) ? null : JSON.parseObject(syncIndex, new TypeReference<Map<String, Set<Integer>>>() {
                    });
                    Map<String, Set<Integer>> archiveMap = StringUtils.isBlank(archiveIndex) ? null : JSON.parseObject(archiveIndex, new TypeReference<Map<String, Set<Integer>>>() {
                    });
                    if (syncMap != null) {
                        bucketMap.put(SYNC_INDEX, JSON.toJSONString(syncMap));
                    }
                    if (archiveMap != null) {
                        bucketMap.put(ARCHIVE_INDEX, JSON.toJSONString(archiveMap));
                    }
                } catch (Exception e) {
                    throw new MsException(INVALID_ARGUMENT, "The synchronous method parameter is invalid, " + bucketName + ".");
                }
            }
        } else if (!"off".equals(syncSwitch)) {
            throw new MsException(INVALID_ARGUMENT, "Invalid data-synchronization-switch parameter for the create bucket " + bucketName + " .");
        }

        boolean startNfsOrCifs;
        if (nfsOpen || cifsOpen || ftpOpen) {
            startNfsOrCifs = true;
            // 发送后端包开启NFS/CIFS
            startOrStopNfs(bucketName, nfsOpen, cifsOpen, ftpOpen);
        } else {
            startNfsOrCifs = false;
            bucketMap.put("cifs", "0");
            bucketMap.put("cifsAcl", "0");
            bucketMap.put("guest", "0");
            bucketMap.put("nfs", "0");
            bucketMap.put("nfsAcl", "0");
            bucketMap.put("ftp", "0");
            bucketMap.put("ftpAcl", "0");
            bucketMap.put("ftp_anonymous", "0");
        }

        Optional.ofNullable(storageStrategy).ifPresent(strategy -> {
            try {
                Queue<Tuple2<String, String>> queue = new ConcurrentLinkedQueue<>();
                Flux<Tuple2<String, Boolean>> res = Flux.empty();

                // 在当前节点初始化桶的散列信息
                StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucketName);
                ObjectSplitTree objectSplitTree = new ObjectSplitTree(bucketName, null);

                // 在原有桶的vnode上分配额新的vnode
                List<String> allLeafNode = objectSplitTree.getAllLeafNode();
                String[] newVnode = metaStoragePool.getShardingMapping().allocate(bucketName, allLeafNode, 1);
                logger.info("allocate Vnode:{}", newVnode != null ? newVnode[0] : null);
                if (newVnode != null && !startNfsOrCifs && !startSnapshot.get()) {
                    boolean b = objectSplitTree.expansionNode(allLeafNode.get(0), newVnode[0], SEPARATOR_LINE_PREFIX + "m", true);
                    if (!b) {
                        throw new MsException(UNKNOWN_ERROR, "create bucket " + bucketName + " expansion split tree error!");
                    }
                }
                // 获取树结构的序列化结果
                String serialized = objectSplitTree.serialize();

                // 将桶的散列信息同步至其他节点
                SocketReqMsg msg = new SocketReqMsg("", 0).put("bucket", bucketName).put("strategy", strategy)
                        .put(DATA_SYNC_SWITCH, paramMap.getOrDefault(DATA_SYNC_SWITCH, "off"));
                msg.put("serialize", serialized);

                // 兼容升级 1.5.4T03
                if (newVnode != null && !startNfsOrCifs && !startSnapshot.get()) {
                    msg.put("newVnode", newVnode[0]);
                }

                for (String ip : RabbitMqUtils.HEART_IP_LIST) {
                    res = res.mergeWith(Mono.just(new Tuple2<>(ip, true)).flatMap(b -> RSocketClient.getRSocket(ip, BACK_END_PORT))
                            .flatMap(r -> r.requestResponse(DefaultPayload.create(Json.encode(msg), UPDATE_BUCKET_STRATEGY.name())))
                            .timeout(Duration.ofSeconds(30))
                            .map(r -> new Tuple2<>(ip, ErasureServer.PayloadMetaType.SUCCESS.name().equalsIgnoreCase(r.getMetadataUtf8())))
                            .onErrorReturn(new Tuple2<>(ip, false)));
                }

                res.publishOn(DISK_SCHEDULER)
                        .doOnNext(t -> {
                            if (!t.var2) {
                                queue.offer(new Tuple2<>(t.var1, bucketName));
                            }
                        })
                        .doOnError(e -> {
                            log.error("", e);
                            resMono.onNext(false);
                        })
                        .doOnComplete(() -> {
                            try {
                                BucketShardCache cache = StoragePoolFactory.getMetaStoragePool(bucketName).getBucketShardCache();
                                ObjectSplitTree tree = cache.get(bucketName);
                                if (tree == null) {
                                    tree = objectSplitTree;
                                    cache.put(bucketName, tree);
                                }
                                String serialize = tree.serialize();
                                log.info("bucket {} serialize:{}", bucketName, serialize);
                                pool.getShortMasterCommand(REDIS_ACTION_INDEX).hset("bucket_index_map", bucketName, serialize);
                                while (!queue.isEmpty()) {
                                    Tuple2<String, String> value = queue.poll();
                                    pool.getShortMasterCommand(REDIS_ACTION_INDEX).lpush(value.var1 + "_reload_bucket_shard_cache_queue", value.var2);
                                }
                                resMono.onNext(true);
                            } catch (Exception e) {
                                log.error("write bucket index error", e);
                                resMono.onNext(false);
                            }
                        })
                        .subscribe();
            } catch (Exception e) {
                log.error("init bucket hash cache error!", e);
                throw new MsException(UNKNOWN_ERROR, "allocate bucket shard error!");
            }

        });

        // 等待桶策略以及桶散列信息加载成功才继续执行
        Boolean res = resMono.block(Duration.ofSeconds(60));
        if (res == null || !res) {
            throw new MsException(UNKNOWN_ERROR, "allocate bucket shard error!");
        }

        // 添加桶的信息到redis中
        String dbRes = pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hmset(bucketName, bucketMap);
        if (!REDIS_OK.equalsIgnoreCase(dbRes)) {
            throw new MsException(UNKNOWN_ERROR, "Add bucket to redis failed");
        }

        pool.getShortMasterCommand(REDIS_USERINFO_INDEX).sadd(userId + USER_BUCKET_SET_SUFFIX, bucketName);
    }

    public static void startOrStopNfs(String bucketName, boolean startNfs, boolean startCifs, boolean startFtp) {
        SocketReqMsg msg = new SocketReqMsg("openNFS", 0)
                .put("bucketName", bucketName)
                .put("nfsAcl", "0")
                .put("cifsAcl", "0")
                .put("guest", "0")
                .put("ftpAcl", "0")
                .put("ftp_anonymous", "0");
        if (startNfs || startCifs || startFtp) {
            msg.put("createBucket", "1").put("action", "start");
            msg.put("startNfs", startNfs ? "1" : "0");
            msg.put("startCifs", startCifs ? "1" : "0");
            msg.put("startFtp", startFtp ? "1" : "0");
        } else {
            msg.put("action", "stop");
        }
        StringResMsg addBackSqlRes = SocketSender.getInstance().sendAndGetResponse(msg, StringResMsg.class, true);
        int code = addBackSqlRes.getCode();

        if (code != SUCCESS_STATUS) {
            throw new MsException(UNKNOWN_ERROR, "start bucket:" + bucketName + " nfs error!");
        }
    }

    public static Tuple2<Map<String, Set<Integer>>, Map<String, Set<Integer>>> buildIndexes(Map<String, String> relations) {
        Map<String, Set<Integer>> syncIndexMap = new HashMap<>();
        Map<String, Set<Integer>> archiveIndexMap = new HashMap<>();
        HashSet<Tuple2<Integer, Integer>> indexSet = new HashSet<>();

        for (Map.Entry<String, String> entry : relations.entrySet()) {
            String key = entry.getKey();
            String relation = entry.getValue();

            if (key == null || !key.contains(",")) {
                log.error("invalid key format, expected 'A,B', key={}", key);
                throw new MsException(INVALID_ARGUMENT, "Sync relation parameter invalid " + key);
            }

            String[] sites = key.split(",", 2);
            String clusterA = fromUtf8HexEscapeSafe(sites[0]);
            String clusterB = fromUtf8HexEscapeSafe(sites[1]);
            Integer a = NAME_INDEX_MAP.get(clusterA);
            Integer b = NAME_INDEX_MAP.get(clusterB);

            if (a == null || b == null) {
                throw new MsException(INVALID_ARGUMENT, "Unknown site in pair: " + key);
            }

            switch (relation) {
                case "ARCHIVE":
                    if (!archiveIndexMap.computeIfAbsent(a + "", k -> new HashSet<>()).add(b)) {
                        throw new MsException(INVALID_ARGUMENT, "Sync relation parameter conflict " + key);
                    }
                    if (!archiveIndexMap.computeIfAbsent(b + "", k -> new HashSet<>()).add(a)) {
                        throw new MsException(INVALID_ARGUMENT, "Sync relation parameter conflict " + key);
                    }
                    indexSet.add(new Tuple2<>(a, b));
                    indexSet.add(new Tuple2<>(b, a));
                    continue;

                case "PUSH":
                    if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(a)) {
                        throw new MsException(INVALID_ARGUMENT, "Sync relation parameter invalid " + key);
                    }
                    if (!syncIndexMap.computeIfAbsent(a + "", k -> new HashSet<>()).add(b)) {
                        throw new MsException(INVALID_ARGUMENT, "Sync relation parameter conflict " + key);
                    }
                    indexSet.add(new Tuple2<>(a, b));
                    indexSet.add(new Tuple2<>(b, a));
                    continue;

                case "PULL":
                    if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(b)) {
                        throw new MsException(INVALID_ARGUMENT, "Sync relation parameter invalid " + key);
                    }
                    if (!syncIndexMap.computeIfAbsent(b + "", k -> new HashSet<>()).add(a)) {
                        throw new MsException(INVALID_ARGUMENT, "Sync relation parameter conflict " + key);
                    }
                    indexSet.add(new Tuple2<>(a, b));
                    indexSet.add(new Tuple2<>(b, a));
                    continue;

                case "BIDIRECTIONAL":
                    if (EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(a) || EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(b)) {
                        throw new MsException(INVALID_ARGUMENT, "Sync relation parameter invalid " + key);
                    }
                    if (!syncIndexMap.computeIfAbsent(a + "", k -> new HashSet<>()).add(b)) {
                        throw new MsException(INVALID_ARGUMENT, "Sync relation parameter conflict " + key);
                    }
                    if (!syncIndexMap.computeIfAbsent(b + "", k -> new HashSet<>()).add(a)) {
                        throw new MsException(INVALID_ARGUMENT, "Sync relation parameter conflict " + key);
                    }
                    indexSet.add(new Tuple2<>(a, b));
                    indexSet.add(new Tuple2<>(b, a));
                    continue;
            }

            if (indexSet.size() != (NAME_INDEX_MAP.size() - 1) * NAME_INDEX_MAP.size()) {
                throw new MsException(INVALID_ARGUMENT, "Not all sites have been added to the sync relation");
            }
        }
        return new Tuple2<>(syncIndexMap, archiveIndexMap);
    }

    public static String fromUtf8HexEscapeSafe(String input) {
        if (input == null) {
            return null;
        }

        // 没有 \xNN，直接返回，零风险
        if (!HEX_ESCAPE.matcher(input).find()) {
            return input;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        for (int i = 0; i < input.length(); ) {
            if (i + 3 < input.length()
                    && input.charAt(i) == '\\'
                    && input.charAt(i + 1) == 'x') {

                String hex = input.substring(i + 2, i + 4);
                try {
                    baos.write(Integer.parseInt(hex, 16));
                    i += 4;
                    continue;
                } catch (NumberFormatException ignore) {
                    // fall through，当普通字符处理
                }
            }

            // 普通字符 → UTF-8 字节
            byte[] bytes = String.valueOf(input.charAt(i))
                    .getBytes(StandardCharsets.UTF_8);
            baos.write(bytes, 0, bytes.length);
            i++;
        }

        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }

    private void addBucketDns(String bucketName, String regionName) {
        String dnsKey = EtcdClient.getDnsRecordPrefix(bucketName);
        String dnsValue = EtcdClient.getRecordStr(EtcdClient.getRegionDns(regionName));
        EtcdClient.put(dnsKey, dnsValue);
    }

    /**
     * 根据桶名字删除权限，在<code>REDIS_BUCKETINFO_INDEX</code> 下删除
     *
     * <p>bucketName + "_" + PERMISSION_READ</p>
     * <p>bucketName + "_" + PERMISSION_WRITE</p>
     * <p>bucketName + "_" + PERMISSION_READ_CAP</p>
     * <p>bucketName + "_" + PERMISSION_WRITE_CAP</p>
     * <p>bucketName + "_" + PERMISSION_FULL_CON</p>
     * <p>
     * 等五条
     *
     * @param bucketName 桶名字
     */
    private void deleteBucketGrantAcl(String bucketName) {
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                .del(bucketName + "_" + PERMISSION_READ,
                        bucketName + "_" + PERMISSION_WRITE,
                        bucketName + "_" + PERMISSION_READ_CAP,
                        bucketName + "_" + PERMISSION_WRITE_CAP,
                        bucketName + "_" + PERMISSION_FULL_CON);
    }

    /**
     * 先检查传入的userId是否具有相应权限，然后根据bucketAcl设置桶权限
     * <p>
     * 其中bucketAcl只能为下列值中的一个：
     * <p>PERMISSION_PRIVATE<p/>
     * <p>PERMISSION_SHARE_READ<p/>
     * <p>PERMISSION_SHARE_READ_WRITE<p/>
     *
     * @param userId     用户ID，用于鉴权
     * @param bucketName 桶名字
     * @param paramMap   桶权限
     * @return ResponseMsg
     */
    private ResponseMsg setAcl(String userId, String bucketName, UnifiedMap<String, String> paramMap) {

        Map<String, String> bucketInfo = getBucketMapByName(paramMap);
        int aclNum = Integer.parseInt(bucketInfo.get(BUCKET_ACL));
        String bucketUserId = bucketInfo.get(BUCKET_USER_ID);
        String bucketAcl = paramMap.get("x-amz-acl");

        switch (bucketAcl) {
            case PERMISSION_PRIVATE:
                bucketAcl = String.valueOf(PERMISSION_PRIVATE_NUM);
                break;
            case PERMISSION_SHARE_READ:
                bucketAcl = String.valueOf(PERMISSION_SHARE_READ_NUM);
                break;
            case PERMISSION_SHARE_READ_WRITE:
                bucketAcl = String.valueOf(PERMISSION_SHARE_READ_WRITE_NUM);
                break;
            default:
                throw new MsException(NO_SUCH_BUCKET_PERMISSION, "No such bucket permission.");
        }
        String method = "PutBucketAcl";
        int policyResult = PolicyCheckUtils.getPolicyAclResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            MsAclUtils.checkGrantAcl(aclNum, bucketUserId, userId, bucketName, PERMISSION_WRITE_CAP_NUM, PERMISSION_WRITE_CAP);
        }

        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, "acl", bucketAcl);
        logger.debug("setBucketAcl modify bucket acl successful, bucketName:" + bucketName + ", bucket_acl:"
                + aclNum + ", bucket_user:" + bucketUserId + ", operation user id:" + userId);

        deleteBucketGrantAcl(bucketName);

        //todo 刷盘
        return new ResponseMsg(SUCCESS_STATUS, new byte[0]);
    }

    private ResponseMsg setGrantAcl(String userId, String bucketName, List<Entry<String, String>> xmlList, UnifiedMap<String, String> paramMap) {
        if (xmlList.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET_PERMISSION, "setBucketGrantAcl error, not acl info when merge.");
        }
        Map<String, String> bucketInfo = getBucketMapByName(paramMap);

        int aclNum = Integer.parseInt(bucketInfo.get(BUCKET_ACL));
        String bucketUserId = bucketInfo.get(BUCKET_USER_ID);
        String method = "PutBucketAcl";
        int policyResult = PolicyCheckUtils.getPolicyGrantAclResult(paramMap, bucketName, userId, method, xmlList);
        if (policyResult == 0) {
            MsAclUtils.checkGrantAcl(aclNum, bucketUserId, userId, bucketName, PERMISSION_WRITE_CAP_NUM, PERMISSION_WRITE_CAP);
        }

        for (Entry<String, String> map : xmlList) {
            String targetUserId = map.getKey();
            String aclType = map.getValue();
            //client 传过来的target user id有可能为空，故此处不能抛异常
            if ("".equals(aclType)) {
                throw new MsException(INVALID_ARGUMENT, "the acl input is invalid.");
            }
            if (targetUserId != null && !targetUserId.isEmpty()) {
                if (!USER_DATABASE_ID_TYPE.equals(pool.getCommand(REDIS_USERINFO_INDEX).hget(targetUserId, USER_DATABASE_HASH_TYPE))) {
                    throw new MsException(NO_SUCH_ID, "no such user id. user id: " + targetUserId + ".");
                }
            } else {
                if (PERMISSION_READ.equals(aclType)) {
                    if (aclNum != PERMISSION_SHARE_READ_WRITE_NUM) {
                        aclNum = PERMISSION_SHARE_READ_NUM;
                    }
                }
                if (PERMISSION_WRITE.equals(aclType)) {
                    aclNum = PERMISSION_SHARE_READ_WRITE_NUM;
                }
            }

            if (!PERMISSION_READ.equals(aclType)
                    && !PERMISSION_READ_LONG.equals(aclType)
                    && !PERMISSION_WRITE.equals(aclType)
                    && !PERMISSION_WRITE_LONG.equals(aclType)
                    && !PERMISSION_READ_CAP.equals(aclType)
                    && !PERMISSION_READ_CAP_LONG.equals(aclType)
                    && !PERMISSION_WRITE_CAP.equals(aclType)
                    && !PERMISSION_WRITE_CAP_LONG.equals(aclType)
                    && !PERMISSION_FULL_CON.equals(aclType)
                    && !PERMISSION_FULL_CON_LONG.equals(aclType)) {
                throw new MsException(NO_SUCH_BUCKET_PERMISSION, "No such permission.");
            }
        }

        deleteBucketGrantAcl(bucketName);

        for (Entry<String, String> map : xmlList) {
            String aclId = map.getKey();
            String aclType = map.getValue();
            if (aclId == null || aclId.isEmpty()) {
                continue;
            }
            if (aclType.equalsIgnoreCase(PERMISSION_READ) || aclType.equalsIgnoreCase(PERMISSION_READ_LONG)) {
                aclNum = addIfAbsent(bucketName, aclNum, aclId, PERMISSION_READ, PERMISSION_READ_NUM);
            } else if (aclType.equalsIgnoreCase(PERMISSION_WRITE) || aclType.equalsIgnoreCase(PERMISSION_WRITE_LONG)) {
                aclNum = addIfAbsent(bucketName, aclNum, aclId, PERMISSION_WRITE, PERMISSION_WRITE_NUM);
            } else if (aclType.equalsIgnoreCase(PERMISSION_READ_CAP) || aclType.equalsIgnoreCase(PERMISSION_READ_CAP_LONG)) {
                aclNum = addIfAbsent(bucketName, aclNum, aclId, PERMISSION_READ_CAP, PERMISSION_READ_CAP_NUM);
            } else if (aclType.equalsIgnoreCase(PERMISSION_WRITE_CAP) || aclType.equalsIgnoreCase(PERMISSION_WRITE_CAP_LONG)) {
                aclNum = addIfAbsent(bucketName, aclNum, aclId, PERMISSION_WRITE_CAP, PERMISSION_WRITE_CAP_NUM);
            } else {
                aclNum = addIfAbsent(bucketName, aclNum, aclId, PERMISSION_FULL_CON, PERMISSION_FULL_CON_NUM);
            }
        }

        if (0 == aclNum) {
            throw new MsException(NO_BUCKET_PERMISSION, "Error grantAclType.");
        } else {
            logger.debug("set bucket " + bucketName + " grant acl successful");
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, "acl", String.valueOf(aclNum));
        }

        //todo 刷盘
        return new ResponseMsg(SUCCESS_STATUS);
    }

    /**
     * 在grantListType中查询是否有相应权限，如果没有则同时添加到redis和aclNum中
     *
     * @param bucketName    桶名字
     * @param aclNum        权限
     * @param aclId         权限id，用于查询redis
     * @param permissionStr 某个权限的字符串形式
     * @param permissionNum 某个权限的数字形式
     * @return 添加之后的权限
     */
    private int addIfAbsent(String bucketName, int aclNum, String aclId, String permissionStr, int permissionNum) {
        String grantListType = bucketName + "_" + permissionStr;
        if (!pool.getCommand(REDIS_BUCKETINFO_INDEX).sismember(grantListType, aclId)) {
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).sadd(grantListType, aclId);
            aclNum |= permissionNum;
        }
        return aclNum;
    }

    /**
     * 封装根据不同的bucket-grant类型获取bucket权限
     *
     * @param aclNum     acl数
     * @param bucketName 桶名
     * @param owner      所有者
     * @param list       xml列表
     */
    private void getBucketGrantAcl(int aclNum, String bucketName, String owner, List<Grant> list) {
        if (((aclNum & PERMISSION_READ_NUM) == 0)
                && ((aclNum & PERMISSION_WRITE_NUM) == 0)
                && ((aclNum & PERMISSION_READ_CAP_NUM) == 0)
                && ((aclNum & PERMISSION_WRITE_CAP_NUM) == 0)
                && ((aclNum & PERMISSION_FULL_CON_NUM) == 0)) {
            return;
        }
        if ((aclNum & PERMISSION_READ_NUM) != 0) {
            getBucketGrantAclByType(bucketName, owner, PERMISSION_READ, list);
        }
        if ((aclNum & PERMISSION_WRITE_NUM) != 0) {
            getBucketGrantAclByType(bucketName, owner, PERMISSION_WRITE, list);
        }
        if ((aclNum & PERMISSION_READ_CAP_NUM) != 0) {
            getBucketGrantAclByType(bucketName, owner, PERMISSION_READ_CAP, list);
        }
        if ((aclNum & PERMISSION_WRITE_CAP_NUM) != 0) {
            getBucketGrantAclByType(bucketName, owner, PERMISSION_WRITE_CAP, list);
        }
        if ((aclNum & PERMISSION_FULL_CON_NUM) != 0) {
            getBucketGrantAclByType(bucketName, owner, PERMISSION_FULL_CON, list);
        }
    }

    /**
     * 根据不同的bucket-grant类型获取bucket权限
     *
     * @param bucketName 桶名
     * @param owner      所有者
     * @param aclType    acl类型
     * @param list       xml列表
     */
    private void getBucketGrantAclByType(String bucketName, String owner, String aclType, List<Grant> list) {
        String grantAclName = bucketName + "_" + aclType;
        Set<String> bucketList = pool.getCommand(REDIS_BUCKETINFO_INDEX).smembers(grantAclName);

        for (String granteeId : bucketList) {
            String granteeName = pool.getCommand(REDIS_USERINFO_INDEX).hget(granteeId, USER_DATABASE_ID_NAME);
            if (owner.equals(granteeName) && aclType.equals(PERMISSION_FULL_CON)) {
                continue;
            }
            Grantee bucketGrantee = new Grantee()
                    .setId(granteeId)
                    .setDisplayName(granteeName);
            Grant grant = new Grant()
                    .setGrantee(bucketGrantee)
                    .setPermission(aclType);
            list.add(grant);
        }
    }

    /**
     * 根据给定的信息填充bucketMap
     *
     * @param paramMap 填充bucketMap的字段
     * @return 填充完毕的bucketMap
     */
    private Map<String, String> getBucketMap(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String operationUser = pool.getCommand(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_ID_NAME);
        String storage_strategy = pool.getCommand(REDIS_USERINFO_INDEX).hget(operationUser, "storage_strategy");
        String vnodeId = StoragePoolFactory.getMetaStoragePool(bucketName).getBucketVnodeId(bucketName);
        String targetUuid = pool.getCommand(REDIS_MAPINFO_INDEX).hget(vnodeId, "s_uuid");
        Map<String, String> bucketMap = new UnifiedMap<>(16);
        bucketMap.put("upload_nr", "0");
        bucketMap.put("ctime", paramMap.get("ctime"));
        bucketMap.put("bucket_name", bucketName);
        bucketMap.put("target_uuid", targetUuid);
        bucketMap.put("user_name", operationUser);
        bucketMap.put("user_id", userId);
        bucketMap.put("acl", Integer.toString(PERMISSION_PRIVATE_NUM));
        bucketMap.put("storage_strategy", storage_strategy);
        return bucketMap;
    }

    /**
     * 创建桶之前对参数进行校验
     *
     * @param userId     用户id
     * @param bucketName 桶名字
     * @throws MsException NAME_INPUT_ERR   : 输入名字不合法
     *                     TOO_MANY_BUCKETS : 桶数量超过上限
     *                     BUCKET_EXISTS    : 桶已经存在
     */
    public static void createBucketCheck(String userId, String bucketName) {
        //bucket输入校验
        if (!BUCKET_NAME_PATTERN.matcher(bucketName).matches()) {
            throw new MsException(BUCKET_NAME_INPUT_ERR,
                    "createBucket failed, bucket name input error, bucket_name: " + bucketName + ".");
        }

        MsAclUtils.checkIfAnonymous(userId);

        //bucket数量到达上限
        long userBucketNum = pool.getCommand(REDIS_USERINFO_INDEX).scard(userId + "bucket_set");
        int maxBucketInUser = Integer.parseInt(pool.getCommand(REDIS_SYSINFO_INDEX).get("max_bucket_in_user"));
        if (userBucketNum >= maxBucketInUser) {
            throw new MsException(TOO_MANY_BUCKETS,
                    "Too many buckets in this user. userId: " + userId + ".");
        }

        /* bucket已经存在 */
        long bucketExist = pool.getCommand(REDIS_BUCKETINFO_INDEX).exists(bucketName);
        if (bucketExist == 1) {
            throw new MsException(BUCKET_EXISTS,
                    "The bucket is already existed.bucket: " + bucketName + ".");
        }
        MultiRegionUtils.checkBucketExist(bucketName);
    }

    /**
     * 检查用户Id是否匹配
     *
     * @param userId       用户id
     * @param bucketUserId 桶用户的id
     */
    @Override
    protected void userCheck(String userId, String bucketUserId) {
        MsAclUtils.checkIfAnonymous(userId);

        if (!userId.equals(bucketUserId)) {
            throw new MsException(NO_BUCKET_PERMISSION,
                    "no permission.user " + userId + " can not delete bucket.");
        }
    }

    /**
     * deleteBucketCheck
     * <p>
     * 删除桶之前的检查
     *
     * @param userId       用户id
     * @param bucketUserId 桶用户的id
     * @param uploadNr     是否能删除桶
     * @throws MsException NO_BUCKET_PERMISSION ：没权限
     *                     OBJECT_EXISTS        ：桶中有对象，不能删除
     */
    private void deleteBucketCheck(String userId, String bucketUserId, String uploadNr, boolean offSyncSwitch) {
        userCheck(userId, bucketUserId);
        //查询upload_nr=0，可以删除bucket，否则不可以删除bucket
        if (!DELETE_BUCKET_FLAG.equals(uploadNr)) {
            if (offSyncSwitch) {
                throw new MsException(OBJ_EXIST,
                        "upload_nr is not 0,can not delete bucket.");
            }
            // 使用数组映射 res 到对应错误码
            final int[] errorCodes = {OBJ_EXIST_0, OBJ_EXIST_1, OBJ_EXIST_2};

            // 防止 res 超出数组范围
            int errorNum = (LOCAL_CLUSTER_INDEX >= 0 && LOCAL_CLUSTER_INDEX < errorCodes.length) ? errorCodes[LOCAL_CLUSTER_INDEX] : OBJ_EXIST;
            throw new MsException(errorNum, "Can not delete bucket, object exist in bucket, cluster " + LOCAL_CLUSTER_INDEX + ".");
        }
    }

    /**
     * 检查桶配额输入是否有效
     *
     * @param quota 输入的配额
     */
    private void checkBucketQuota(String quota) {
        if (StringUtils.isBlank(quota) || !BUCKET_QUOTA_PATTERN.matcher(quota).matches() || quota.startsWith("0") && quota.length() > 1) {
            throw new MsException(BUCKET_QUOTA_INPUT_ERR, "quota is invalid");
        } else {
            try {
                Long.parseLong(quota);
            } catch (Exception e) {
                throw new MsException(BUCKET_QUOTA_INPUT_ERR, "quota is invalid");
            }
        }
    }

    private void checkBucketObjectsQuota(String quota) {
        if (StringUtils.isBlank(quota) || !BUCKET_QUOTA_PATTERN.matcher(quota).matches() || quota.startsWith("0") && quota.length() > 1) {
            throw new MsException(INVALID_ARGUMENT, "objects quota is invalid");
        }

        if (quota.length() > String.valueOf(BUCKET_MAX_OBJECTS_QUOTA).length()) {
            throw new MsException(INVALID_ARGUMENT, "bucket objnum quota is invalid");
        } else {
            try {
                long objectsQuota = Long.parseLong(quota);
                if (objectsQuota > BUCKET_MAX_OBJECTS_QUOTA) {
                    throw new MsException(INVALID_ARGUMENT, "bucket objnum quota is invalid");
                }
            } catch (Exception e) {
                throw new MsException(INVALID_ARGUMENT, "bucket objnum quota is invalid");
            }
        }
    }

    /***
     * 根据桶名查询桶所属站点的名称
     * @param paramMap
     * @return
     */
    public ResponseMsg getClusterName(UnifiedMap<String, String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);

        Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }

        userCheck(userId, bucketInfo.get(BUCKET_USER_ID));
        String clusterName =
                Optional.ofNullable(bucketInfo.get(CLUSTER_NAME)).orElse("NULL");
        ResponseMsg msg = new ResponseMsg();
        msg.addHeader("clusterName", clusterName);
        return msg;
    }

    public void checkIdAndName(String id, String name) {
        if (StringUtils.isNotEmpty(id) && StringUtils.isNotEmpty(name)) {
            String name1 = pool.getCommand(REDIS_USERINFO_INDEX).hget(id, "name");
            if (StringUtils.isNotEmpty(name1)) {
                if (!name1.equals(name)) {
                    throw new MsException(ACCOUNT_NOT_EXISTS, "");
                }
            }
        }
    }

    public void checkOwner(String id, String name, String bucket) {
        if (StringUtils.isNotEmpty(id)) {
            String id1 = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, "user_id");
            if (!id1.equals(id)) {
                throw new MsException(ACCESS_FORBIDDEN, "");
            }
        }
        if (StringUtils.isNotEmpty(name)) {
            String name1 = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, "user_name");
            if (!name1.equals(name)) {
                throw new MsException(ACCESS_FORBIDDEN, "");
            }
        }
    }

    private static final String LOCK_KEY = "creating_bucket_";
    private static final int LOCK_EXPIRE_TIME = 60; // 锁的过期时间，单位秒

    public static boolean acquireLock(String bucketName, long timeout) {
        long startTime = System.currentTimeMillis();
        boolean first = true;
        try {
            while (first || System.currentTimeMillis() - startTime < timeout) {
                first = false;
                // 尝试获取锁并设置过期时间
                SetArgs ex = new SetArgs().nx().ex(LOCK_EXPIRE_TIME);
                String result = pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).set(LOCK_KEY + bucketName, "1", ex);
                if ("OK".equalsIgnoreCase(result)) {
                    return true; // 获取锁成功
                } else if (timeout == 0) {
                    //  如果timeout为0，则直接返回失败
                    return false;
                }
                // 等待一段时间后重新尝试获取锁
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false; // 获取锁超时
    }

    public static void releaseLock(String bucketName) {
        try {
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(LOCK_KEY + bucketName);
        } catch (Exception e) {
            log.error("release bucket {} lock error {}", bucketName, e);
        }
    }

    public ResponseMsg putBucketReferer(UnifiedMap<String, String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        String body = paramMap.get(BODY);
        //权限检查
        MsAclUtils.checkIfAnonymous(userId);
        //桶所有者或拥有PutBucketReferer权限的用户可以操作
        String method = "PutBucketReferer";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketInfo.get("user_id"));
        }
        //检查桶是否存在
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        //检查参数是否有误
        //1.body为空
        if (StringUtils.isBlank(body)) {
            throw new MsException(INVALID_ARGUMENT, "refererConfiguration is invalid");
        }
        RefererConfiguration refererConfiguration = null;
        try {
            refererConfiguration = (RefererConfiguration) JaxbUtils.toObject(body);
        } catch (Exception e) {
            throw new MsException(INVALID_ARGUMENT, "refererConfiguration is invalid");
        }
        if (refererConfiguration == null) {
            throw new MsException(INVALID_ARGUMENT, "refererConfiguration is invalid");
        }
        //2.检查AntiLeechEnabled
        if (StringUtils.isEmpty(refererConfiguration.getAntiLeechEnabled())) {
            throw new MsException(INVALID_ARGUMENT, "AntiLeechEnabled should be included");
        }
        if (!(refererConfiguration.getAntiLeechEnabled().equals("true") || refererConfiguration.getAntiLeechEnabled().equals("false"))) {
            throw new MsException(INVALID_ARGUMENT, "AntiLeechEnabled is invalid");
        }
        //3.检查AllowEmptyReferer是否合法
        if (StringUtils.isEmpty(refererConfiguration.getAllowEmptyReferer())) {
            refererConfiguration.setAllowEmptyReferer("false");
        }
        if (!(refererConfiguration.getAllowEmptyReferer().equals("true") || refererConfiguration.getAllowEmptyReferer().equals("false"))) {
            throw new MsException(INVALID_ARGUMENT, "AllowEmptyReferer is invalid");
        }
        if (CheckUtils.bucketFsCheck(bucketName)){
            throw new MsException(ErrorNo.NFS_NOT_STOP, "The bucket already start nfs or cifs, can not enable bucketRefer");
        }
        //3.检查白名单和黑名单长度
        boolean blackListIsEmpty = refererConfiguration.getBlackRefererList() == null || refererConfiguration.getBlackRefererList().getReferer() == null;
        boolean whiteListIsEmpty = refererConfiguration.getWhiteRefererList() == null || refererConfiguration.getWhiteRefererList().getReferer() == null;
        List<String> blackList = Collections.emptyList();
        List<String> whiteList = Collections.emptyList();
        if (!blackListIsEmpty) {
            if (refererConfiguration.getBlackRefererList().getReferer().toString().length() > 1024) {
                throw new MsException(TOO_MANY_REFERER, "the blackRefererList over 1024");
            }
            //过滤重复referer
            Set<String> set = new LinkedHashSet<>(refererConfiguration.getBlackRefererList().getReferer()); // 保留插入顺序
            blackList = new ArrayList<>(set);
        }
        if (!whiteListIsEmpty) {
            if (refererConfiguration.getWhiteRefererList().getReferer().toString().length() > 1024) {
                throw new MsException(TOO_MANY_REFERER, "the whiteRefererList over 1024");
            }
            //过滤重复referer
            Set<String> set = new LinkedHashSet<>(refererConfiguration.getWhiteRefererList().getReferer()); // 保留插入顺序
            whiteList = new ArrayList<>(set);
        }
        //从站点请求主站点执行：
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_PUT_BUCKET_REFERER, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(SUCCESS);
        }
        //主站点通知从站点
        int res = notifySlaveSite(paramMap, ACTION_PUT_BUCKET_REFERER);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave put bucket referer error!");
        }
        //保存数据
        String LUA_SCRIPT =
                "local antiLeechEnabled = KEYS[1]\n" +
                        "local allowEmptyReferer = KEYS[2]\n" +
                        "local blackRefererList = KEYS[3]\n" +
                        "local whiteRefererList = KEYS[4]\n" +
                        "redis.call('HSET', KEYS[5], 'antiLeechEnabled', antiLeechEnabled)\n" +
                        "redis.call('HSET', KEYS[5], 'allowEmptyReferer', allowEmptyReferer)\n" +
                        "redis.call('HSET', KEYS[5], 'blackRefererList', blackRefererList)\n" +
                        "redis.call('HSET', KEYS[5], 'whiteRefererList', whiteRefererList)\n" +
                        "return 1";
        // 执行 Lua 脚本
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).eval(LUA_SCRIPT, ScriptOutputType.INTEGER, new String[]{refererConfiguration.getAntiLeechEnabled(),
                refererConfiguration.getAllowEmptyReferer(),
                blackListIsEmpty ? "" : blackList.toString(),
                whiteListIsEmpty ? "" : whiteList.toString(),
                bucketName});
        return new ResponseMsg();
    }

    public ResponseMsg getBucketReferer(UnifiedMap<String, String> paramMap) {
        //权限检查
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        String body = paramMap.get(BODY);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);

        MsAclUtils.checkIfAnonymous(userId);
        //桶所有者或拥有GetBucketReferer权限的用户可以操作
        String method = "GetBucketReferer";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketInfo.get("user_id"));
        }
        //检查桶是否存在
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        //获取桶配置信息
        if (StringUtils.isEmpty(bucketInfo.get("antiLeechEnabled"))) {
            throw new MsException(BUCKET_REFERER_NOT_EXIST, "the referer configration is not exist");
        }
        RefererConfiguration refererConfiguration = new RefererConfiguration();
        refererConfiguration.setAntiLeechEnabled(bucketInfo.get("antiLeechEnabled"));
        refererConfiguration.setAllowEmptyReferer(bucketInfo.get("allowEmptyReferer"));
        if (StringUtils.isNotEmpty(bucketInfo.get("blackRefererList"))) {
            BlackRefererList blackRefererList = new BlackRefererList();
            blackRefererList.setReferer(transformToList(bucketInfo.get("blackRefererList")));
            refererConfiguration.setBlackRefererList(blackRefererList);
        }
        if (StringUtils.isNotEmpty(bucketInfo.get("whiteRefererList"))) {
            WhiteRefererList whiteRefererList = new WhiteRefererList();
            whiteRefererList.setReferer(transformToList(bucketInfo.get("whiteRefererList")));
            refererConfiguration.setWhiteRefererList(whiteRefererList);
        }
        //封装xml返回信息
        byte[] bytes = JaxbUtils.toByteArray(refererConfiguration);
        ResponseMsg responseMsg = new ResponseMsg();
        return responseMsg.setData(bytes)
                .addHeader(CONTENT_TYPE, "application/xml")
                .addHeader(CONTENT_LENGTH, String.valueOf(bytes.length));
    }

    public ResponseMsg putBucketClearConfiguration(UnifiedMap<String, String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        String body = paramMap.get(BODY);
        //权限检查
        MsAclUtils.checkIfAnonymous(userId);
        //桶所有者或拥有PutBucketReferer权限的用户可以操作
        String method = "PutBucketClearConfigration";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketInfo.get("user_id"));
        }
        //检查桶是否存在
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        //检查桶是否设置软配额
        String softObjnumQuota = bucketInfo.get("soft_objnum_value");
        String softQuotaValue = bucketInfo.get("soft_quota_value");
        boolean isNotSetsoftObjnumQuota = StringUtils.isEmpty(softObjnumQuota) || softObjnumQuota.equals("0");
        boolean isNotSetsoftQuotaValue = StringUtils.isEmpty(softQuotaValue) || softQuotaValue.equals("0");
        if (isNotSetsoftObjnumQuota && isNotSetsoftQuotaValue) {
            throw new MsException(NO_SOFT_QUOTA, "no such soft quota set. bucket_name: " + bucketName);
        }
        //检查参数是否有误
        //1.body为空
        if (StringUtils.isBlank(body)) {
            throw new MsException(MALFORMED_XML, "the body is blank");
        }
        ClearConfigration clearConfigration = null;
        try {
            clearConfigration = (ClearConfigration) JaxbUtils.toObject(body);
        } catch (Exception e) {
            throw new MsException(MALFORMED_XML, "there is a error when the xml transform to clear Configuration");
        }
        if (clearConfigration == null) {
            throw new MsException(MALFORMED_XML, "the clearConfigration is blank");
        }
        String xmlConfig = getXmlConfig(clearConfigration);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (StringUtils.isNotBlank(masterCluster)) {
            throw new MsException(ErrorNo.METHOD_NOT_ALLOWED, "multi-cluster env");
        }
        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).set(bucketName + "_clear_model", xmlConfig);
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    public ResponseMsg getBucketClearConfiguration(UnifiedMap<String, String> paramMap) {
        //权限检查
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);

        MsAclUtils.checkIfAnonymous(userId);
        //桶所有者或拥有GetBucketReferer权限的用户可以操作
        String method = "GetBucketClearConfigration";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketInfo.get("user_id"));
        }
        //检查桶是否存在
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (StringUtils.isNotBlank(masterCluster)) {
            throw new MsException(ErrorNo.METHOD_NOT_ALLOWED, "multi-cluster env");
        }
        //获取配置信息
        String xmlConfig = pool.getCommand(REDIS_SYSINFO_INDEX).get(bucketName + "_clear_model");
        if (StringUtils.isEmpty(xmlConfig)) {
            throw new MsException(NO_SUCH_CLEAR_MODEL_CONFIGURATION, "the clear configuration is not exist");
        }
        //封装xml返回信息
        byte[] bytes = xmlConfig.getBytes();
        ResponseMsg responseMsg = new ResponseMsg();
        return responseMsg.setData(bytes)
                .addHeader(CONTENT_TYPE, "application/xml")
                .addHeader(CONTENT_LENGTH, String.valueOf(bytes.length));
    }

    public ResponseMsg deleteBucketClearConfiguration(UnifiedMap<String, String> paramMap) {
        //权限检查
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);

        MsAclUtils.checkIfAnonymous(userId);
        //桶所有者或拥有GetBucketReferer权限的用户可以操作
        String method = "DeleteBucketClearConfigration";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketInfo.get("user_id"));
        }
        //检查桶是否存在
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (StringUtils.isNotBlank(masterCluster)) {
            throw new MsException(ErrorNo.METHOD_NOT_ALLOWED, "multi-cluster env");
        }
        //获取桶配置信息
        Long exists = pool.getCommand(REDIS_SYSINFO_INDEX).exists(bucketName + "_clear_model");
        if (exists == 0) {
            throw new MsException(NO_SUCH_CLEAR_MODEL_CONFIGURATION, "the clear configuration is not exist");
        }
        //删除配置信息
        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(bucketName + "_clear_model");
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, DELETE_MARK);
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    public ResponseMsg putBucketCors(UnifiedMap<String, String> paramMap) {
        //权限检查
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        String body = paramMap.get(BODY);

        MsAclUtils.checkIfAnonymous(userId);
        //桶所有者或拥有GetBucketReferer权限的用户可以操作
        String method = "PutBucketCors";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketInfo.get("user_id"));
        }
        //检查桶是否存在
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        //检查参数是否有误
        //1.body为空
        if (StringUtils.isBlank(body)) {
            throw new MsException(MALFORMED_XML, "the body is blank");
        }
        //2.body长度超过16KB
        if (body.length() > 16 * 1024) {
            throw new MsException(CORS_RULE_TOO_LONG, "the body is too long");
        }
        CORSConfiguration CORSConfiguration = null;
        try {
            CORSConfiguration = (CORSConfiguration) JaxbUtils.toObject(body);
        } catch (Exception e) {
            throw new MsException(MALFORMED_XML, "there is a error when the xml transform to CORSConfiguration");
        }
        if (CORSConfiguration == null) {
            throw new MsException(MALFORMED_XML, "the CORSConfiguration is blank");
        }
        String xmlConfig = CORSUtils.getXmlCorsConfig(CORSConfiguration);
        //从站点请求主站点执行：
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_PUT_BUCKET_CORS, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(SUCCESS);
        }
        //主站点通知从站点
        int res = notifySlaveSite(paramMap, ACTION_PUT_BUCKET_CORS);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave put bucket referer error!");
        }
        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(CORS_CONFIG_KEY, bucketName, xmlConfig);
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    public ResponseMsg getBucketCors(UnifiedMap<String, String> paramMap) {
        //权限检查
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);

        MsAclUtils.checkIfAnonymous(userId);
        //桶所有者或拥有GetBucketReferer权限的用户可以操作
        String method = "GetBucketCors";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketInfo.get("user_id"));
        }
        //检查桶是否存在
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        //获取配置信息
        String xmlConfig = pool.getCommand(REDIS_SYSINFO_INDEX).hget(CORS_CONFIG_KEY, bucketName);
        if (StringUtils.isEmpty(xmlConfig)) {
            throw new MsException(NO_CORS_CONFIGURATION, "the CORS configuration is not exist");
        }
        //封装xml返回信息
        byte[] bytes = xmlConfig.getBytes();
        ResponseMsg responseMsg = new ResponseMsg();
        return responseMsg.setData(bytes)
                .addHeader(CONTENT_TYPE, "application/xml")
                .addHeader(CONTENT_LENGTH, String.valueOf(bytes.length));
    }

    public ResponseMsg deleteBucketCors(UnifiedMap<String, String> paramMap) {
        //权限检查
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);

        MsAclUtils.checkIfAnonymous(userId);
        //桶所有者或拥有GetBucketReferer权限的用户可以操作
        String method = "DeleteBucketCors";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketInfo.get("user_id"));
        }
        //检查桶是否存在
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        //获取桶配置信息
        String xmlConfig = pool.getCommand(REDIS_SYSINFO_INDEX).hget(CORS_CONFIG_KEY, bucketName);
        if (StringUtils.isEmpty(xmlConfig)) {
            throw new MsException(NO_CORS_CONFIGURATION, "the CORS configuration is not exist");
        }
        //从站点请求主站点执行：
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_DEL_BUCKET_CORS, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(SUCCESS);
        }
        //主站点通知从站点
        int res = notifySlaveSite(paramMap, ACTION_DEL_BUCKET_CORS);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave put bucket cors error!");
        }
        //删除配置信息
        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel(CORS_CONFIG_KEY, bucketName);
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    public ResponseMsg putBucketTagging(UnifiedMap<String, String> paramMap) {
        //权限检查
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        String body = paramMap.get(BODY);

        MsAclUtils.checkIfAnonymous(userId);
        //桶所有者或拥有GetBucketReferer权限的用户可以操作
        String method = "PutBucketTagging";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketInfo.get("user_id"));
        }
        //检查桶是否存在
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        //检查参数是否有误
        //1.body为空
        if (StringUtils.isBlank(body)) {
            throw new MsException(MALFORMED_XML, "the body is blank");
        }
        Tagging tagging = null;
        try {
            tagging = (Tagging) JaxbUtils.toObject(body);
        } catch (Exception e) {
            throw new MsException(MALFORMED_XML, "there is a error when the xml transform to tagging");
        }
        if (tagging == null) {
            throw new MsException(MALFORMED_XML, "the tagging is blank");
        }
        checkBucketTagging(tagging);
        //从站点请求主站点执行：
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_PUT_BUCKET_TAG, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(SUCCESS);
        }
        //主站点通知从站点
        int res = notifySlaveSite(paramMap, ACTION_PUT_BUCKET_TAG);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave put bucket referer error!");
        }
        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(BUCKET_TAG_KEY, bucketName, body);
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    public ResponseMsg getBucketTagging(UnifiedMap<String, String> paramMap) {
        //权限检查
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);

        MsAclUtils.checkIfAnonymous(userId);
        //桶所有者或拥有GetBucketReferer权限的用户可以操作
        String method = "GetBucketTagging";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketInfo.get("user_id"));
        }
        //检查桶是否存在
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        //获取配置信息
        String xmlConfig = pool.getCommand(REDIS_SYSINFO_INDEX).hget(BUCKET_TAG_KEY, bucketName);
        if (StringUtils.isEmpty(xmlConfig)) {
            throw new MsException(NO_BUCKET_TAGGING, "the bucket tag is not exist");
        }
        //封装xml返回信息
        byte[] bytes = xmlConfig.getBytes();
        ResponseMsg responseMsg = new ResponseMsg();
        return responseMsg.setData(bytes)
                .addHeader(CONTENT_TYPE, "application/xml")
                .addHeader(CONTENT_LENGTH, String.valueOf(bytes.length));
    }

    public ResponseMsg deleteBucketTagging(UnifiedMap<String, String> paramMap) {
        //权限检查
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);

        MsAclUtils.checkIfAnonymous(userId);
        //桶所有者或拥有GetBucketReferer权限的用户可以操作
        String method = "DeleteBucketTagging";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketInfo.get("user_id"));
        }
        //检查桶是否存在
        if (bucketInfo.isEmpty()) {
            throw new MsException(NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        //从站点请求主站点执行：
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_DEL_BUCKET_TAG, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(SUCCESS);
        }
        //主站点通知从站点
        int res = notifySlaveSite(paramMap, ACTION_DEL_BUCKET_TAG);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave put bucket referer error!");
        }
        //删除配置信息
        if (StringUtils.isNotEmpty(paramMap.get("tagging"))) {
            deleteTag(paramMap.get("tagging"), bucketName);
        } else {
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel(BUCKET_TAG_KEY, bucketName);
        }
        return new ResponseMsg().setHttpCode(204);
    }

}
