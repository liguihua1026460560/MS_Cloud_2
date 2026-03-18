package com.macrosan.doubleActive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.etcd.EtcdClient;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.doubleActive.arbitration.DAVersionUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.async.FSUnsyncRecordHandler;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.RestfulVerticle;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.consturct.RequestBuilder;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord.Type;
import com.macrosan.message.socketmsg.BaseResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.socketmsg.SocketSender;
import com.macrosan.message.socketmsg.StringResMsg;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.codec.UrlEncoder;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.http.HttpClientRequest;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.macrosan.action.managestream.BucketService.createBucketCheck;
import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.*;
import static com.macrosan.doubleActive.HeartBeatChecker.*;
import static com.macrosan.doubleActive.arbitration.Arbitrator.MASTER_INDEX;
import static com.macrosan.doubleActive.arbitration.Arbitrator.isEvaluatingMaster;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.*;
import static com.macrosan.doubleActive.deployment.AddClusterHandler.HIS_SYNC_REC_MARK;
import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_PUT_SYNC_RECORD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.IF_PUT_DONE;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.UPDATE_SYNC_RECORD;
import static com.macrosan.httpserver.MossHttpClient.*;
import static com.macrosan.httpserver.RestfulVerticle.getSignPath;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.Type.*;
import static com.macrosan.storage.client.ListSyncRecorderHandler.getRecordType;
import static com.macrosan.utils.regions.MultiRegionUtils.*;
import static com.macrosan.utils.sts.RoleUtils.*;
import static com.macrosan.utils.trash.TrashUtils.bucketTrash;

/**
 * @auther wuhaizhong
 * 处理双活相关util
 * @date 2021/4/10
 */
public class DoubleActiveUtil {

    private static final Logger logger = LogManager.getLogger(DoubleActiveUtil.class.getName());

    private static final RedisConnPool pool = RedisConnPool.getInstance();

    protected static SocketSender sender = SocketSender.getInstance();

    private static final String UUID = ServerConfig.getInstance().getHostUuid();

    private static final AtomicBoolean firstStartMipCheck = new AtomicBoolean(true);

    /**
     * 往httpRequest写buffer时的队列最大值。如果设置过小会造成写buffer时频繁调用drainHandler，导致vertx线程block。
     */
    public static final int WriteQueueMaxSize = 4 * 1024 * 1024;

    /**
     * 双活创建桶处理
     */
    public static boolean dealSiteCreateBucket(UnifiedMap<String, String> paramMap, String localCluster, String masterCluster) {
        if (NODE_AMOUNT == 1) {
            return true;
        }
        regionCheck(paramMap);
        String bucketName = paramMap.get(BUCKET_NAME);
        boolean continueFlag = true;
        //存在双活配置
        if (StringUtils.isNotBlank(masterCluster)) {
            if (paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase())) {
                //主站点请求从站点创桶flag
                logger.info("contains active flag, need not get lock");
                return true;
            }
            if (!localCluster.equals(masterCluster)) {
                checkLinkState();
            }

            String defaultRegion = pool.getCommand(REDIS_SYSINFO_INDEX).get(MULTI_REGION_DEFAULT_REGION);
            if (StringUtils.isEmpty(defaultRegion)) {
                //未配置多区域，向主站点获取锁（配置多区域由多区域获取主站点锁）
                if (localCluster.equals(masterCluster)) {
                    logger.info("get lock in master cluster");
                    //当前站点为主站点，直接获取锁
                    if (ErrorNo.SUCCESS_STATUS != getSingleLock(2, bucketName)) {
                        throw new MsException(ErrorNo.BUCKET_EXISTS,
                                "The bucket is already existed！bucket: " + bucketName + ".");
                    }
                } else {
                    logger.info("get lock to master cluster");
                    //通过后端口从站点向主站点获取锁
                    getRedisLock(2, bucketName, true);
                }
            }

            if (!localCluster.equals(masterCluster)) {
                //从站点向主站点请求创桶，主站点失败返回失败
                String userId = paramMap.get(USER_ID);
                createBucketCheck(userId, bucketName);
                String ips = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_IPS);
                int resCode = remoteSiteRequest(new SocketReqMsg(MSG_TYPE_SITE_CREATE_BUCKET, 0), paramMap, ips.split(","), false);
                if (resCode != ErrorNo.SUCCESS_STATUS) {
                    throw new MsException(resCode, "master create bucket error");
                }
                continueFlag = false;
            } else {
                insertSharedLog(paramMap, "createBucket");
            }

        }
        return continueFlag;
    }

    /**
     * 检查站点连接状态,站点连接状态断开不能进行
     */
    public static void checkLinkState() {
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (StringUtils.isNotBlank(masterCluster)) {
            String linkState = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, LINK_STATE);
            if ("0".equals(linkState)) {
                throw new MsException(ErrorNo.SITE_LIKN_BROKEN, "link state broken!");
            }
        }
    }

    public static void checkDoubleState() {
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        String syncPolicy = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SYNC_POLICY);
        if (StringUtils.isNotBlank(masterCluster)) {
            String linkState = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, LINK_STATE);
            if ("0".equals(linkState)) {
                throw new MsException(ErrorNo.SITE_LIKN_BROKEN, "link state broken!");
            }
            if ("0".equals(syncPolicy)) {

            }
        }
    }

    public static Mono<Boolean> isMasterCluster() {
        if (DA_INDEX_IPS_ENTIRE_MAP.size() == 1) {
            // 单站点只有复制站点时默认通过
            return Mono.just(true);
        }
        return pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME)
                .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME))
                .map(tuple2 -> tuple2.getT1().equals(tuple2.getT2()));
    }

    public static Mono<Boolean> checkLinkStateReactive() {
        if (DA_INDEX_IPS_ENTIRE_MAP.size() == 1) {
            // 单站点只有复制站点时默认通过
            return Mono.just(true);
        }
        return pool.getReactive(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME)
                .zipWith(pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, LINK_STATE).defaultIfEmpty("1"))
                .map(tuple2 -> {
                    if (StringUtils.isNotBlank(tuple2.getT1())) {
                        return !"0".equals(tuple2.getT2());
                    }
                    return true;
                });
    }

    private static void regionCheck(UnifiedMap<String, String> paramMap) {
        String defaultRegion = pool.getCommand(REDIS_SYSINFO_INDEX).get(MULTI_REGION_DEFAULT_REGION);
        if (!StringUtils.isEmpty(defaultRegion)) {
            String bucketLocation = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(paramMap.get(BUCKET_NAME), REGION_NAME);
            if (paramMap.containsKey("bucket_region") && !paramMap.get("bucket_region").equals(bucketLocation)) {
                throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + paramMap.get(BUCKET_NAME));
            }
        }
    }


    /**
     * 通知其他站点创建桶
     */
    public static int remoteSiteRequest(SocketReqMsg msg, UnifiedMap<String, String> paramMap, String[] clusterIps, boolean writeEtcd) {
        try {
            UnifiedMap<String, String> reqMap = new UnifiedMap<>();
            paramMap.forEach((k, v) -> {
                if (k.startsWith("x-amz-grant-") || "x-amz-acl".equals(k) || "metadata-analysis".equals(k)
                        || "object-lock-enabled-for-bucket".equals(k) || "site".equals(k) || "sourcesite".equals(k)
                        || DATA_SYNC_SWITCH.equals(k) || ARCHIVE_INDEX.equals(k) || SYNC_INDEX.equals(k) || SYNC_RELATION.equals(k)|| USER_ID.equals(k)
                        || BUCKET_NAME.equals(k) || SERVER_AK.equals(k) || SERVER_SK.equals(k) || BUCKET_VERSION_STATUS.equals(k)
                        || ROLE_ARN.equals(k) || (k.startsWith(POLICY_ARNS_MEMBER_PREFIX) && k.endsWith(".arn"))
                        || "policyIds".equals(k) || "policyMrns".equals(k) || "assumeRoleName".equals(k) || ROLE_SESSION_NAME.equals(k)
                        || DURATION_SECONDS.equals(k) || POLICY.equals(k) || "roleId".equals(k) || "roleName".equals(k)
                        || "AssumeId".equals(k) || "PolicyId".equals(k) || "AccessKey".equals(k) || "SecretKey".equals(k) || "Deadline".equals(k)
                        || NFS_SWITCH.equals(k) || CIFS_SWITCH.equals(k) || FTP_SWITCH.equals(k) || BUCKET_VERSION_QUOTA.equals(k)
                        || "srcIndex".equals(k) || "dstIndex".equals(k) || "signType".equals(k) || "tagging".equals(k)) {
                    reqMap.put(k, v);
                } else if (k.equals(BODY) || "action".equals(k)) {
                    msg.put(k, v);
                }
            });

            if (paramMap.containsKey(BUCKET_NAME)) {
                Map<String, String> map = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(paramMap.get(BUCKET_NAME));
                if (!map.isEmpty()) {
                    reqMap.put("bucket-user-id", map.get("user_id"));
                }
            }

            if (writeEtcd) {
                // 代表该请求有主站点同步给从站点
                reqMap.put(SITE_FLAG, "1");
            } else {
                // 代表该请求是由从站点转发给主站点
                reqMap.put(SITE_FLAG, "0");
            }

            if (clusterIps != null) {
                String ips = Arrays.toString(clusterIps);
                ips = ips.substring(1, ips.length() - 1);
                msg.put("iplist", ips);
                reqMap.put("iplist", ips);
            }
            if (ACTION_PUT_BUCKET_LIFECYCLE.equals(paramMap.get("action")) || ACTION_PUT_BUCKET_WORM.equals(paramMap.get("action")) ||
                    ACTION_PUT_BUCKET_INVENTORY.equals(paramMap.get("action")) || ACTION_PUT_BUCKET_POLICY.equals(paramMap.get("action")) ||
                    ACTION_PUT_BUCKET_LOGGING.equals(paramMap.get("action")) || ACTION_PUT_BUCKET_NOTIFICATION.equals(paramMap.get("action")) ||
                    ACTION_PUT_BUCKET_ENCRYPTION.equals(paramMap.get("action")) || ACTION_PUT_BUCKET_TRASH.equals(paramMap.get("action")) ||
                    ACTION_ASSUME_ROLE.equals(paramMap.get("action")) || ACTION_PUT_BUCKET_REFERER.equals(paramMap.get("action")) ||
                    ACTION_PUT_BUCKET_CLEAR_CONFIG.equals(paramMap.get("action")) || ACTION_PUT_BUCKET_BACKUP.equals(paramMap.get("action")) ||
                    ACTION_PUT_BUCKET_CORS.equals(paramMap.get("action")) || ACTION_PUT_BUCKET_TAG.equals(paramMap.get("action"))) {
                reqMap.put(BODY, paramMap.get(BODY));
            } else if (ACTION_PUT_BUCKET_QUOTA.equals(paramMap.get("action"))) {
                if (!StringUtils.isEmpty(paramMap.get("bucket-quota"))) {
                    reqMap.put("bucket-quota", paramMap.get("bucket-quota"));
                }
                if (!StringUtils.isEmpty(paramMap.get("bucket-soft-quota"))) {
                    reqMap.put("bucket-soft-quota", paramMap.get("bucket-soft-quota"));
                }
            } else if (ACTION_PUT_BUCKET_OBJECTS.equals(paramMap.get("action"))) {
                if (!StringUtils.isEmpty(paramMap.get("bucket-hard-objects"))) {
                    reqMap.put("bucket-hard-objects", paramMap.get("bucket-hard-objects"));
                }
                if (!StringUtils.isEmpty(paramMap.get("bucket-soft-objects"))) {
                    reqMap.put("bucket-soft-objects", paramMap.get("bucket-soft-objects"));
                }
            } else if (ACTION_PUT_BUCKET_PERF_QUOTA.equals(paramMap.get("action"))) {
                reqMap.put("type", paramMap.get("type"));
                reqMap.put("quota", paramMap.get("quota"));
            } else if (ACTION_DEL_BUCKET_INVENTORY.equals(paramMap.get("action"))) {
                reqMap.put("inventoryId", paramMap.get(ID));
            }

            addReqParam(paramMap, reqMap, msg);
            msg.put("bucket", paramMap.get(BUCKET_NAME)).put("param", JSON.toJSONString(reqMap));
            if (paramMap.containsKey("syncSite")) {
                msg.put("syncSite", paramMap.get("syncSite"));
            }
            if (isMultiAliveStarted && !USE_ETH4 && !Arrays.asList(INDEX_IPS_MAP.get(LOCAL_CLUSTER_INDEX)).contains(LOCAL_NODE_IP) && ETH12_INDEX_MAP.get(INDEX_IPS_MAP.get(LOCAL_CLUSTER_INDEX)[0]) != null) {
                return sender.sendAndGetResponse(ETH12_INDEX_MAP.get(INDEX_IPS_MAP.get(LOCAL_CLUSTER_INDEX)[0]), msg, BaseResMsg.class, false).getCode();
            } else {
                return sender.sendAndGetResponse(msg, BaseResMsg.class, false).getCode();
            }
        } catch (MsException e) {
            logger.error(e);
            return UNKNOWN_ERROR;
        }
    }

    /**
     * 若存在双活配置，主通知从站点
     */
    public static int notifySlaveSite(UnifiedMap<String, String> paramMap, String action) {
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        Set<String> tempClusters = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(TEMP_CLUSTERS);
        boolean syncIsEnabled = ACTION_CREATE_BUCKET.equals(action)
                || ACTION_DELETE_BUCKET.equals(action)
                || paramMap.containsKey(BUCKET_NAME) && dataSyncIsEnabled(paramMap);
        boolean isDeploy = tempClusters != null && tempClusters.size() > 0;
        boolean needEtcd = false;
        if (StringUtils.isNotBlank(masterCluster)) {
            if (masterCluster.equals(localCluster)) {
                needEtcd = true;
            }
        } else {
            if (isDeploy) {
                needEtcd = true;
            }
        }
        int res = ErrorNo.SUCCESS_STATUS;
        //单站点+minio环境，暂不同步管理流
        if (NODE_AMOUNT == 1) {
            return res;
        }
        // 创桶流程必须两个站点都成功
        if (needEtcd && syncIsEnabled && isNotSyncMessage(paramMap)) {
            if (paramMap.containsKey(DOUBLE_FLAG)) {
                if (ASYNC_INDEX_IPS_MAP.size() > 0) {
                    ASYNC_INDEX_IPS_MAP.keySet().forEach(index -> {
                        paramMap.put("syncSite", INDEX_NAME_MAP.get(index));
                    });
                } else {
                    if (!isDeploy) {
                        return SUCCESS_STATUS;
                    }
                }
            }
            // 3dc环境生命周期只在双活间同步
            if (paramMap.containsKey("noAsync")) {
                DA_INDEX_IPS_ENTIRE_MAP.keySet().forEach(index -> {
                    if (!LOCAL_CLUSTER_INDEX.equals(index)) {
                        paramMap.put("syncSite", INDEX_NAME_MAP.get(index));
                    }
                });
            }
            logger.info("notify slave ! action {} ", action);
            //后端写ETCD
            SocketReqMsg socketReqMsg = new SocketReqMsg(MSG_TYPE_SITE_SYNC, 0);
            paramMap.put("action", action);
            paramMap.remove(DOUBLE_FLAG);
            res = remoteSiteRequest(socketReqMsg, paramMap, null, true);
            DoubleActiveUtil.deleteShareLog(paramMap, action);
        }
        return res;
    }


    /**
     * 若存在双活配置，主通知从站点
     */
    public static int notifySlaveSiteWithoutBucket(UnifiedMap<String, String> paramMap, String action, boolean syncSuccess) {
        try {
            String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
            String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
            Set<String> tempClusters = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(TEMP_CLUSTERS);
            boolean isDeploy = tempClusters != null && tempClusters.size() > 0;
            boolean needEtcd = false;
            int res = ErrorNo.SUCCESS_STATUS;
            //单站点+minio环境，暂不同步管理流
            if (NODE_AMOUNT == 1) {
                return res;
            }
            if (StringUtils.isNotBlank(masterCluster)) {
                if (masterCluster.equals(localCluster)) {
                    needEtcd = true;
                }
            } else {
                if (isDeploy) {
                    needEtcd = true;
                }
            }
            // 创桶流程必须两个站点都成功
            if (needEtcd && isNotSyncMessage(paramMap)) {
                if (syncSuccess) {
                    if (ASYNC_INDEX_IPS_MAP.size() > 0) {
                        ASYNC_INDEX_IPS_MAP.keySet().forEach(index -> {
                            paramMap.put("syncSite", INDEX_NAME_MAP.get(index));
                        });
                    } else {
                        return SUCCESS_STATUS;
                    }
                }
                logger.debug("notify slave ! action {} ", action);
                //后端写ETCD
                paramMap.remove(SITE_FLAG_UPPER);
                paramMap.remove(DOUBLE_FLAG);
                SocketReqMsg socketReqMsg = new SocketReqMsg(MSG_TYPE_SITE_SYNC, 0);
                paramMap.put("action", action);
                res = remoteSiteRequest(socketReqMsg, paramMap, null, true);
            }
            return res;
        } catch (MsException e) {
            logger.error(e);
            return UNKNOWN_ERROR;
        }
    }


    /**
     * 判断当前管理流请求是否属于多站点同步请求
     *
     * @param paramMap 请求参数
     */
    public static boolean isNotSyncMessage(UnifiedMap<String, String> paramMap) {
        String flag;
        if (!paramMap.containsKey(SITE_FLAG) && !paramMap.containsKey(SITE_FLAG.toLowerCase())) {
            flag = paramMap.getOrDefault(SITE_FLAG_UPPER, "");
        } else {
            flag = StringUtils.isEmpty(paramMap.get(SITE_FLAG))
                    ? paramMap.get(SITE_FLAG.toLowerCase()) : paramMap.get(SITE_FLAG);
        }

        return !"1".equals(flag);
    }

    public static boolean isSyncMessage(MultiMap paramMap) {
        String flag;
        if (!paramMap.contains(SITE_FLAG) && !paramMap.contains(SITE_FLAG.toLowerCase())) {
            flag = paramMap.contains(SITE_FLAG_UPPER) ? paramMap.get(SITE_FLAG_UPPER) : "0";
        } else {
            flag = StringUtils.isEmpty(paramMap.get(SITE_FLAG))
                    ? paramMap.get(SITE_FLAG.toLowerCase()) : paramMap.get(SITE_FLAG);
        }

        return "1".equals(flag);
    }

    /**
     * 双活转发请求到主站点
     *
     * @param paramMap      请求参数
     * @param msgType       操作类型
     * @param localCluster  本地站点名
     * @param masterCluster 主站点名
     * @return 是否继续执行流程
     */
    public static boolean dealSiteSyncRequest(UnifiedMap<String, String> paramMap, String msgType, String localCluster, String masterCluster) {
        regionCheck(paramMap);
        boolean continueFlag = true;
        String action = getActionByMsType(msgType);
        if (StringUtils.isNotBlank(masterCluster) && (ACTION_CREATE_BUCKET.equals(action)
                || ACTION_DELETE_BUCKET.equals(action)
                || dataSyncIsEnabled(paramMap))) {
            //存在双活配置
            checkLinkState();
            if (paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase())) {
                //主站点请求从站点创桶flag
                logger.info("contains active flag");
                return true;
            }
            paramMap.put("action", action);
            if (!localCluster.equals(masterCluster)) {
                //非主站点请求主站点处理
                String ips = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SysConstants.CLUSTER_IPS);
                int resCode = remoteSiteRequest(new SocketReqMsg(msgType, 0), paramMap, ips.split(","), false);
                if (resCode != ErrorNo.SUCCESS_STATUS) {
                    throw new MsException(resCode, "master execute error!");
                }
                continueFlag = false;
            } else {
                if (ACTION_DELETE_BUCKET.equals(action) || ACTION_CREATE_BUCKET.equals(action)) {
                    insertSharedLog(paramMap, action);
                }
            }
        }

        return continueFlag;
    }

    public static boolean dealSiteSyncRequestWithoutBucket(UnifiedMap<String, String> paramMap, String msgType, String localCluster, String masterCluster,
                                                           String assumeId, String policyId, String accessKey, String secretKey, String policy, long deadline) {
        boolean continueFlag = true;
        String action = getActionByMsType(msgType);
        if (StringUtils.isNotBlank(masterCluster)) {
            //存在双活配置
            checkLinkState();
            if (paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()) || paramMap.containsKey(SITE_FLAG_UPPER)) {
                //主站点请求从站点创桶flag
                logger.debug("contains active flag");
                return true;
            }
            paramMap.put("action", action);
            if (!localCluster.equals(masterCluster)) {
                //非主站点请求主站点处理
                paramMap.put("AssumeId", assumeId);
                paramMap.put("AccessKey", accessKey);
                paramMap.put("SecretKey", secretKey);
                paramMap.put("Deadline", String.valueOf(deadline));
                if (policy != null) {
                    paramMap.put("PolicyId", policyId);
                }
                String ips = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SysConstants.CLUSTER_IPS);
                int resCode = remoteSiteRequest(new SocketReqMsg(msgType, 0), paramMap, ips.split(","), false);
                if (resCode != ErrorNo.SUCCESS_STATUS) {
                    throw new MsException(resCode, "master execute error!");
                }
                continueFlag = false;
            }
        }

        return continueFlag;
    }


    /**
     * 同步ectd key
     *
     * @param paramMap 参数map
     * @param action   操作类型
     * @return
     */
    private static String getShareLogKey(UnifiedMap<String, String> paramMap, String action) {
        return "/log/bucket/sites/" + paramMap.get("ctime") + "/" + UUID + "/" + paramMap.get(BUCKET_NAME) + "/" + action;
    }

    private static String getShareLogKey(Map<String, String> paramMap, String action) {
        return "/log/bucket/double/" + paramMap.get("stamp") + "/" + paramMap.get(BUCKET_NAME) + "/" + action;
    }

    /**
     * 插入shareLog防止掉电share未写入
     *
     * @param paramMap 请求参数
     * @param action   同步请求
     */
    private static void insertSharedLog(UnifiedMap<String, String> paramMap, String action) {
        UnifiedMap<String, String> reqMap = new UnifiedMap<>();
        paramMap.forEach((k, v) -> {
            if (k.startsWith("x-amz-grant-") || "x-amz-acl".equals(k) || "metadata-analysis".equals(k) || "site".equals(k)
                    || "object-lock-enabled-for-bucket".equals(k) || USER_ID.equals(k) || BUCKET_NAME.equals(k)
                    || DATA_SYNC_SWITCH.equals(k) || "sourcesite".equals(k)) {
                reqMap.put(k, v);
            }
        });
        String userId = paramMap.get(USER_ID);
        String ak = pool.getCommand(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_ID_AK1);
        if (StringUtils.isEmpty(ak) || "null".equals(ak)) {
            ak = pool.getCommand(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_ID_AK2);
        }
        String sk = pool.getCommand(REDIS_USERINFO_INDEX).hget(ak, USER_DATABASE_AK_SK);
        reqMap.put("ak", ak);
        reqMap.put("sk", sk);
        reqMap.put(SITE_FLAG, "1");
        reqMap.put("bucket", paramMap.get(BUCKET_NAME));
        JSONObject json = new JSONObject();
        json.put("bucket", paramMap.get(BUCKET_NAME));
        json.put("param", JSON.toJSONString(reqMap));
        json.put("action", action);
        String key = getShareLogKey(paramMap, action);
        EtcdClient.put(key, json.toJSONString());
    }

    public static void insertSharedLog(Map<String, String> paramMap, String action) {
        UnifiedMap<String, String> reqMap = new UnifiedMap<>();
        reqMap.put(BUCKET_NAME, paramMap.get(BUCKET_NAME));
        reqMap.put(USER_ID, paramMap.get(USER_ID));

        String ak = paramMap.get("ak");
        String sk = paramMap.get("sk");
        reqMap.put("ak", ak);
        reqMap.put("sk", sk);
        reqMap.put(DOUBLE_FLAG, "1");
        reqMap.put(SITE_FLAG, "1");
        reqMap.put("bucket", paramMap.get(BUCKET_NAME));
        JSONObject json = new JSONObject();
        reqMap.put("version_num", paramMap.get("version_num"));
        json.put("bucket", paramMap.get(BUCKET_NAME));
        json.put("param", JSON.toJSONString(reqMap));
        json.put("action", action);
        String key = getShareLogKey(paramMap, action);
        EtcdClient.put(key, json.toJSONString());
    }

    public static void deleteShareLog(UnifiedMap<String, String> paramMap, String action) {
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (StringUtils.isNotBlank(masterCluster)) {
            String key = getShareLogKey(paramMap, action);
            EtcdClient.delete(key);
            deleteRedisLock(paramMap.get(BUCKET_NAME));
        }
    }

    public static Mono<Boolean> deleteShareLog(Map<String, String> paramMap, String action) {
        return Mono.just(getShareLogKey(paramMap, action))
                .doOnError(logger::error)
                .flatMap(key -> {
                    EtcdClient.delete(key);
                    return Mono.just(true);
                })
                .onErrorReturn(false);
    }

    public static void dealUnsynRecord(HttpServerRequest request, int errorCode) {
        if ("1".equals(syncPolicy) && request.headers().contains(SYNC_RECORD_HEADER)
                && errorCode != INTERNAL_SERVER_ERROR && errorCode != UNKNOWN_ERROR) {
            String asyncOperate = IS_THREE_SYNC ? DELETE_SYNC_RECORD : WRITE_ASYNC_RECORD ? DELETE_ASYNC_RECORD : null;
            deleteUnsyncRecord(((MsHttpRequest) request).getBucketName(), request.getHeader(SYNC_RECORD_HEADER), null, asyncOperate);
        }
    }

    public static Disposable deleteUnsyncRecord(String bucket, String key, UnSynchronizedRecord record, String asyncOperate) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);
        return storagePool.mapToNodeInfo(bucketVnode)
                .publishOn(SCAN_SCHEDULER)
                .flatMap(nodeList -> ErasureClient.deleteUnsyncRocketsValue(bucket, key, record, nodeList, asyncOperate))
//                .timeout(Duration.ofSeconds(10))
                .subscribe(b -> {
                    if (!b) {
                        logger.error("delete sync record fail {}", key);
                    }
                }, e -> logger.error("", e));
    }

    public static Mono<Boolean> deleteUnsyncRecordMono(String bucket, String key, UnSynchronizedRecord record, String asyncOperate) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);
        return storagePool.mapToNodeInfo(bucketVnode)
                .publishOn(SCAN_SCHEDULER)
                .flatMap(nodeList -> ErasureClient.deleteUnsyncRocketsValue(bucket, key, record, nodeList, asyncOperate))
                .doOnNext(b -> {
                    if (!b) {
                        logger.error("delete sync record fail {}", key);
                    } else {
                        if (isDebug) {
                            logger.info("delete sync record succeeded {}", key);
                        }
                    }
                })
                .doOnError(e -> logger.error("", e));
    }

    public static Mono<Boolean> deleteUnsyncRecord(String bucket, String key, UnSynchronizedRecord record, String asyncOperate, MsHttpRequest request, boolean noRecord) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        if (noRecord) {
            res.onNext(true);
        } else {
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
            String bucketVnode = storagePool.getBucketVnodeId(bucket);
            Disposable subscribe = storagePool.mapToNodeInfo(bucketVnode)
                    .flatMap(nodeList -> ErasureClient.deleteUnsyncRocketsValue(bucket, key, record, nodeList, asyncOperate, request))
                    .map(b -> b != 0)
//                .timeout(Duration.ofSeconds(10))
                    .subscribe(b -> {
                        if (!b) {
                            logger.error("delete sync record fail {}", key);
                        }
                        res.onNext(b);
                    }, e -> {
                        logger.error("delete sync record fail", e);
                        res.onNext(false);
                    });
            Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        }
        return res;
    }

    /**
     * 获取双活的下一个站点索引。
     */
    public static int getOtherSiteIndex() {

        for (int index : DA_INDEX_IPS_MAP.keySet()) {
            if (index != LOCAL_CLUSTER_INDEX) {
                return index;
            }
        }
        return -1;
    }

    /**
     * 包含redis同步查询，涉及vertx线程建议用下一个方法
     */
    public static UnSynchronizedRecord buildSyncRecord(MsHttpRequest r) {
        String versionStatus = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(r.getBucketName(), BUCKET_VERSION_STATUS);
        return buildSyncRecord(r, versionStatus);
    }

    public static UnSynchronizedRecord buildSyncRecord(MsHttpRequest r, String version) {
        return buildSyncRecord(r, version, null);
    }

    public static UnSynchronizedRecord buildSyncRecord(MsHttpRequest r, String version, String versionNum0) {
        String requestId = RequestBuilder.getRequestId();
        String versionNum = versionNum0 == null ? VersionUtil.getVersionNum(false) : versionNum0;
        String versionStatus = version;
        if (version == null) {
            versionStatus = "NULL";
        }
        String versionId;
        if (r.params().contains(VERSIONID)) {
            versionId = r.getParam(VERSIONID);
        } else {
            versionId = "NULL".equals(versionStatus) ? "null" : "";
        }
        String newVersionId = "NULL".equals(versionStatus) || VERSION_SUSPENDED.equals(versionStatus) ?
                "null" : RandomStringUtils.randomAlphanumeric(32);
        boolean formUpload = HttpMethod.POST == r.method() && r.headers().contains(CONTENT_TYPE) && r.getHeader(CONTENT_TYPE).contains("multipart/form-data");
        UnSynchronizedRecord.Type type = r.headers().contains(MULTI_SYNC) && "1".equals(r.headers().get(MULTI_SYNC)) ? ERROR_PUT_BUCKET : UnSynchronizedRecord.type(r.uri(), formUpload ? "PUT" : r.method().toString());

        Map<String, String> headers = getSpecialHeaders(r.uri(), type);
        r.headers().entries().stream().filter(entry -> entry.getKey().toLowerCase().startsWith(AUTH_HEADER)
                || SPECIAL_HEADER.contains(entry.getKey().toLowerCase().hashCode()))
                .forEach(entry -> headers.put(entry.getKey().toLowerCase(), entry.getValue()));
        if (StringUtils.isNotBlank(r.getHeader(AUTHORIZATION))) {
            headers.put(AUTHORIZATION, r.getHeader(AUTHORIZATION));
        }
        String trashDir = bucketTrash.get(r.getBucketName());

        if (ERROR_DELETE_OBJECT.equals(type) && StringUtils.isNotEmpty(trashDir)) {
            headers.put("trashDir", trashDir);
            if (StringUtils.isEmpty(r.getHeader("recoverObject")) && r.uri().startsWith("/" + r.getBucketName() + "/" + trashDir)) {
                headers.put("cleanObject", "true");
            }
        }

        if (ERROR_DELETE_OBJECT.equals(type) && StringUtils.isNotEmpty(r.getHeader("recoverObject"))) {
            headers.put("recoverObject", "true");
        }
        headers.put(REQUEST_ID, requestId);
        headers.put(CLUSTER_ALIVE_HEADER, "ip");
        headers.put(SYNC_STAMP, versionNum);
        headers.put("stamp", String.valueOf(System.currentTimeMillis()));
        boolean copySource = r.headers().contains(X_AMZ_COPY_SOURCE);
        if (copySource) {
            headers.put(COPY_SOURCE_KEY, parseSourceKey(r.headers().get(X_AMZ_COPY_SOURCE)));
        }
        if (copySource || type == ERROR_DELETE_OBJECT) {
            headers.put(NEW_VERSION_ID, newVersionId);
        }

        if (!copySource && type != ERROR_DELETE_OBJECT && type != ERROR_PUT_OBJECT_ACL && type != ERROR_PUT_OBJECT_WORM && type != ERROR_PUT_OBJECT_TAG && type != ERROR_DEL_OBJECT_TAG && StringUtils.isEmpty(versionId)) {
            versionId = newVersionId;
        } else if (copySource && StringUtils.isEmpty(versionId)) {
            versionId = newVersionId;
        }
        headers.put(VERSIONID, versionId);
        if (r.headers().contains(CONTENT_LENGTH)) {
            if (Long.parseLong(r.headers().get(CONTENT_LENGTH)) > 0) {
//                headers.put(EXPECT, EXPECT_100_CONTINUE);
            }
            headers.put(CONTENT_LENGTH, r.headers().get(CONTENT_LENGTH));
        }
        if (r.headers().contains(CONTENT_TYPE)) {
            headers.put(CONTENT_TYPE, r.headers().get(CONTENT_TYPE));
        }
        // 添加影像压缩相关信息的header
        Optional.ofNullable(r.getHeader(DECOMPRESSED_ETAG)).ifPresent(etag -> headers.put(DECOMPRESSED_ETAG, etag));
        Optional.ofNullable(r.getHeader(DECOMPRESSED_LENGTH)).ifPresent(length -> headers.put(DECOMPRESSED_LENGTH, length));
        Optional.ofNullable(r.getHeader(COMPRESSION_TYPE)).ifPresent(compressionType -> headers.put(COMPRESSION_TYPE, compressionType));

        String pathStr = getSignPath(r.host(), r.path());
        String param = StringUtils.substringAfter(r.uri(), "?");
        pathStr = StringUtils.isNotEmpty(param) ? pathStr + "?" + param : pathStr;
        return new UnSynchronizedRecord()
                .setVersionNum(VersionUtil.getVersionNum(false))
                .setSyncStamp(versionNum)
                .setUri(pathStr)
                .setHeaders(headers)
                .setMethod(formUpload ? HttpMethod.PUT : r.method())
                .setBucket(r.getBucketName())
                .setObject(r.getObjectName())
                .setVersionId(versionId)
                .setSuccessIndex(LOCAL_CLUSTER_INDEX)
                .setIndex(getOtherSiteIndex());
    }

    public static UnSynchronizedRecord buildDeleteRecord(UnSynchronizedRecord r) {
        final String uri = SLASH + r.bucket + SLASH + r.object;
        String requestId = RequestBuilder.getRequestId();

        String versionId = r.versionId;
        Map<String, String> headers = getSpecialHeaders(uri, ERROR_DELETE_OBJECT);
        r.headers.forEach((k, v) -> {
            if (k.toLowerCase().startsWith(AUTH_HEADER) || SPECIAL_HEADER.contains(k.toLowerCase().hashCode())) {
                headers.put(k, v);
            }
        });
        if (StringUtils.isNotBlank(r.headers.get(AUTHORIZATION))) {
            headers.put(AUTHORIZATION, r.headers.get(AUTHORIZATION));
        }
        String trashDir = bucketTrash.get(r.getBucket());

        if (StringUtils.isNotEmpty(trashDir)) {
            headers.put("trashDir", trashDir);
        }

        headers.put("", "");
        headers.put(REQUEST_ID, requestId);
        headers.put(CLUSTER_ALIVE_HEADER, "ip");
        headers.put(SYNC_STAMP, r.syncStamp);
        headers.put("stamp", String.valueOf(System.currentTimeMillis()));
        boolean copySource = r.headers.containsKey(X_AMZ_COPY_SOURCE);
        if (copySource) {
            headers.put(COPY_SOURCE_KEY, parseSourceKey(r.headers.get(X_AMZ_COPY_SOURCE)));
        }

        headers.put(VERSIONID, versionId);
        headers.put("deleteSource", "1");
        String param = "versionId=" + versionId + "&xx-id=DeleteObject";
        String pathStr = StringUtils.substringBefore(r.uri, "?");
        pathStr = pathStr + "?" + param;
        return new UnSynchronizedRecord()
                .setVersionNum(VersionUtil.getVersionNum(false))
                .setSyncStamp(r.syncStamp)
                .setUri(pathStr)
                .setHeaders(headers)
                .setMethod(HttpMethod.DELETE)
                .setBucket(r.getBucket())
                .setObject(r.getObject())
                .setVersionId(versionId)
                .setSuccessIndex(LOCAL_CLUSTER_INDEX)
                .setIndex(LOCAL_CLUSTER_INDEX);
    }


    public static Mono<Integer> updateSyncRecord(UnSynchronizedRecord record, List<Tuple3<String, String, String>> nodeList) {
        if (record == null) {
            return Mono.just(1);
        }
        record.setCommited(true);
        MonoProcessor<Integer> res = MonoProcessor.create();
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("key", record.rocksKey())
                            .put("value", Json.encode(record))
                            .put("oldVersion", record.versionNum);

                    msg.put("lun", MSRocksDB.getSyncRecordLun(tuple.var2));
                    msg.put(DELETE_SOURCE, "1");
                    if (record.headers.containsKey(HIS_SYNC_REC_MARK)) {
                        msg.put(HIS_SYNC_SIGNAL, "1");
                    }
                    return msg;
                })
                .collect(Collectors.toList());
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, UPDATE_SYNC_RECORD, String.class, nodeList);
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(record.bucket);
        responseInfo.responses.subscribe(s -> {
        }, e -> logger.error("", e), () -> {
            if (responseInfo.successNum == pool.getK() + pool.getM()) {
                res.onNext(1);
            } else if (responseInfo.writedNum == pool.getK() + pool.getM()) {
                res.onNext(2);
            } else if (responseInfo.errorNum == pool.getK() + pool.getM()) {
                res.onNext(0);
            } else if (responseInfo.successNum >= pool.getK()) {
//                String strategyName = "storage_" + pool.getVnodePrefix();
//                String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());
//                if (StringUtils.isEmpty(poolQueueTag)) {
//                    String strategyName = "storage_" + pool.getVnodePrefix();
//                    poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                }
                msgs.get(0).put("poolQueueTag", poolQueueTag);
                publishEcError(responseInfo.res, nodeList, msgs.get(0), ERROR_PUT_SYNC_RECORD);
                res.onNext(1);
            } else if (responseInfo.errorNum < pool.getK() && responseInfo.writedNum >= 1) {
                res.onNext(2);
            } else {
                //未成功，但有部分节点成功，不返回结果
            }
        });

        return res;
    }


    public static Map<String, String> getSpecialHeaders(String uri, Type type) {
        Map<String, String> map = new HashMap<>(4);
        switch (type) {
            case ERROR_PUT_BUCKET:
            case ERROR_PUT_OBJECT: {
                break;
            }
            case ERROR_PART_UPLOAD: {
                MultiMap params = RestfulVerticle.params(uri);
                String uploadId = params.get(URL_PARAM_UPLOADID);
                String partNum = params.get(URL_PARAM_PART_NUMBER);
                map.put(GET_SINGLE_PART, uploadId + "," + partNum);
                break;
            }
            case ERROR_PUT_OBJECT_VERSION:
            case ERROR_PUT_OBJECT_ACL:
            case ERROR_PUT_OBJECT_TAG:
            case ERROR_DEL_OBJECT_TAG:
            case ERROR_PUT_OBJECT_WORM:
            case ERROR_INIT_PART_UPLOAD: {
                String uploadId = RandomStringUtils.randomAlphabetic(32);
                map.put(UPLOAD_ID, uploadId);
                break;
            }
            case ERROR_PART_ABORT:
            case ERROR_COMPLETE_PART:
            case ERROR_DELETE_OBJECT:
            case ERROR_COPY_OBJECT:
            default:
        }
        return map;
    }


    private static String getActionByMsType(String msgType) {
        switch (msgType) {
            case MSG_TYPE_SITE_CREATE_BUCKET:
                return ACTION_CREATE_BUCKET;
            case MSG_TYPE_SITE_DELETE_BUCKET:
                return ACTION_DELETE_BUCKET;
            case MSG_TYPE_SITE_SET_ACL:
                return ACTION_PUT_BUCKET_ACL;
            case MSG_TYPE_SITE_PUT_VERSION:
                return ACTION_PUT_BUCKET_VERSION;
            case MSG_TYPE_SITE_PUT_METADATA:
                return ACTION_PUT_BUCKET_METADATA;
            case MSG_TYPE_SITE_PUT_WORM:
                return ACTION_PUT_BUCKET_WORM;
            case MSG_TYPE_SITE_PUT_INVENTORY:
                return ACTION_PUT_BUCKET_INVENTORY;
            case MSG_TYPE_SITE_DEL_INVENTORY:
                return ACTION_DEL_BUCKET_INVENTORY;
            case MSG_TYPE_SITE_PUT_LIFECYCLE:
                return ACTION_PUT_BUCKET_LIFECYCLE;
            case MSG_TYPE_SITE_DEL_LIFECYCLE:
                return ACTION_DEL_BUCKET_LIFECYCLE;
            case MSG_TYPE_SITE_PUT_BUCKET_QUOTA:
                return ACTION_PUT_BUCKET_QUOTA;
            case MSG_TYPE_SITE_PUT_BUCKET_OBJECTS:
                return ACTION_PUT_BUCKET_OBJECTS;
            case MSG_TYPE_SITE_PUT_BUCKET_PERF_QUOTA:
                return ACTION_PUT_BUCKET_PERF_QUOTA;
            case MSG_TYPE_SITE_PUT_BUCKET_POLICY:
                return ACTION_PUT_BUCKET_POLICY;
            case MSG_TYPE_SITE_DEL_BUCKET_POLICY:
                return ACTION_DEL_BUCKET_POLICY;
            case MSG_TYPE_SITE_PUT_BUCKET_LOGGING:
                return ACTION_PUT_BUCKET_LOGGING;
            case MSG_TYPE_SITE_PUT_BUCKET_NOTIFICATION:
                return ACTION_PUT_BUCKET_NOTIFICATION;
            case MSG_TYPE_SITE_PUT_BUCKET_TRASH:
                return ACTION_PUT_BUCKET_TRASH;
            case MSG_TYPE_SITE_DEL_BUCKET_TRASH:
                return ACTION_DEL_BUCKET_TRASH;
            case MSG_TYPE_SITE_PUT_BUCKET_ENCRYPTION:
                return ACTION_PUT_BUCKET_ENCRYPTION;
            case MSG_TYPE_SITE_DEL_BUCKET_ENCRYPTION:
                return ACTION_DEL_BUCKET_ENCRYPTION;
            case MSG_TYPE_SITE_ASSUME_ROLE:
                return ACTION_ASSUME_ROLE;
            case MSG_TYPE_SITE_PUT_BUCKET_REFERER:
                return ACTION_PUT_BUCKET_REFERER;
            case MSG_TYPE_SITE_PUT_BUCKET_CLEAR_CONFIG:
                return ACTION_PUT_BUCKET_CLEAR_CONFIG;
            case MSG_TYPE_SITE_DEL_BUCKET_CLEAR_CONFIG:
                return ACTION_DEL_BUCKET_CLEAR_CONFIG;
            case MSG_TYPE_SITE_PUT_BUCKET_CORS:
                return ACTION_PUT_BUCKET_CORS;
            case MSG_TYPE_SITE_DEL_BUCKET_CORS:
                return ACTION_DEL_BUCKET_CORS;
            case MSG_TYPE_SITE_PUT_BUCKET_TAG:
                return ACTION_PUT_BUCKET_TAG;
            case MSG_TYPE_SITE_DEL_BUCKET_TAG:
                return ACTION_DEL_BUCKET_TAG;
            case MSG_TYPE_SITE_PUT_BACK_UP:
                return ACTION_PUT_BUCKET_BACKUP;
            case MSG_TYPE_SITE_DEL_BACKUP:
                return ACTION_DEL_BUCKET_BACKUP;
            case MSG_TYPE_STIE_SET_AWS_SIGN:
                return ACTION_SET_AWS_SIGN;
            case MSG_TYPE_SITE_DELETE_AWS_SIGN:
                return ACTION_DELETE_AWS_SIGN;
            default:
                logger.error("unknown type {}", msgType);
                return null;
        }
    }

    private static String parseSourceKey(String sourceKey) {
        if (StringUtils.isNotBlank(sourceKey)) {
            String xAmzCopySource;
            try {
                xAmzCopySource = URLDecoder.decode(sourceKey, "UTF-8");
                if (xAmzCopySource.startsWith(SLASH)) {
                    xAmzCopySource = xAmzCopySource.substring(1);
                }
            } catch (UnsupportedEncodingException e) {
                logger.error("", e);
                return "";
            }
            String[] array = xAmzCopySource.split("/", 2);
            if (array.length < 2) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "The format of x-amz-copy-source is incorrect!");
            }
            String[] arr2 = array[1].split("\\?", 2);
            String sourceBucket = array[0];
            String sourceObject = arr2[0];
            String sourceVersionId = arr2.length == 1 ? "" : arr2[1].substring(arr2[1].indexOf("=") + 1);
            return sourceBucket + "/" + sourceObject + "/" + sourceVersionId;
        }
        return "";
    }

    public static void closeConn(HttpClientRequest req) {
        try {
            req.reset();
        } catch (Exception e) {
            logger.error("req reset err, ", e);
        }
    }

    /**
     * 主动关闭指定流。
     */
    public static void streamDispose(Disposable[] disposables) {
        for (Disposable disposable : disposables) {
            Optional.ofNullable(disposable).ifPresent(Disposable::dispose);
        }
    }

    public static void streamDispose(List<Disposable> disposables) {
        for (Disposable disposable : disposables) {
            Optional.ofNullable(disposable).ifPresent(Disposable::dispose);
        }
    }

    public static void streamDispose(Set<Disposable> disposables) {
        for (Disposable disposable : disposables) {
            Optional.ofNullable(disposable).ifPresent(Disposable::dispose);
        }
    }

    public static void encodeAmzHeaders(HttpClientRequest request) {
        List<String> removeHeaderList = new ArrayList<>();
        for (Map.Entry<String, String> entry : request.headers()) {
            final String lowerKey = entry.getKey().toLowerCase();
            if (lowerKey.startsWith(AUTH_HEADER)) {
                String encodeKey = UrlEncoder.encode(lowerKey, "UTF-8");
                if (!lowerKey.equals(encodeKey)) {
                    request.putHeader(encodeKey, entry.getValue());
                    removeHeaderList.add(entry.getKey());
                }
            }
        }
        removeHeaderList.forEach(key -> request.headers().remove(key));
    }


    static void preDealSyncReq(UnSynchronizedRecord record,
                               List<UnSynchronizedRecord> recordList, AtomicLong dataSize) {
        long objSize = Long.parseLong(record.headers.getOrDefault(CONTENT_LENGTH, "0"));
        recordList.add(record);
        dataSize.addAndGet(objSize);
    }

    public static AtomicLong TOTAL_SYNCING_AMOUNT = new AtomicLong();

    public static AtomicLong TOTAL_SYNCING_SIZE = new AtomicLong();

    public static AtomicLong PART_SYNCING_AMOUNT = new AtomicLong();

    static void releaseSyncingPayload(UnSynchronizedRecord record) {
        int removeRec = 0;
        try {
            if (record == null) {
                return;
            }
            if (record.isFSUnsyncRecord()) {
                String fsRecordMapKey = FSUnsyncRecordHandler.getFSRecordMapKey(record);
                synchronized (recordSet) {
                    if (recordSet.get(record.bucket) != null && recordSet.get(record.bucket).remove(record.rocksKey())) {
                        removeRec = 2;
                        checkRecordMap.get(fsRecordMapKey).remove(record.recordKey);
                    }
                }
            } else {
                synchronized (recordSet) {
                    String uploadId = record.headers.get(RECORD_ORIGIN_UPLOADID);
                    String type = getRecordType(record);
                    if (StringUtils.isNotEmpty(uploadId)) {
                        if (record.type() == UnSynchronizedRecord.Type.ERROR_INIT_PART_UPLOAD) {
                            if (initUploadRecordSet.get(record.bucket) != null) {
                                initUploadRecordSet.get(record.bucket).remove(uploadId);
                            }
                        } else if (record.type() == UnSynchronizedRecord.Type.ERROR_PART_UPLOAD) {
                            if (uploadCountSet.get(record.bucket + uploadId) != null && uploadCountSet.get(record.bucket + uploadId).decrementAndGet() == 0) {
                                uploadCountSet.remove(record.bucket + uploadId);
                                if (uploadIdRecordSet.get(record.bucket) != null) {
                                    uploadIdRecordSet.get(record.bucket).remove(uploadId);
                                }
                            }
                        } else if (record.type() == UnSynchronizedRecord.Type.ERROR_COMPLETE_PART) {
                            if (mergeUploadRecordSet.get(record.bucket) != null) {
                                mergeUploadRecordSet.get(record.bucket).remove(uploadId);
                            }
                        }

                        if (recordSet.get(record.bucket) != null && recordSet.get(record.bucket).remove(record.rocksKey())) {
                            removeRec = 1;
                            checkRecordMap.get(record.index + File.separator + record.bucket + File.separator + record.object + File.separator + type + File.separator + record.versionId)
                                    .remove(record.rocksKey());
                        }
                    } else {
                        if (recordSet.get(record.bucket) != null && recordSet.get(record.bucket).remove(record.rocksKey())) {
                            removeRec = 2;
                            checkRecordMap.get(record.index + File.separator + record.bucket + File.separator + record.object + File.separator + type + File.separator + record.versionId)
                                    .remove(record.recordKey);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("releaseSyncingPayload error", e);
        } finally {
            if (removeRec != 0) {
                if (removeRec == 1) {
                    PART_SYNCING_AMOUNT.decrementAndGet();
                }
                TOTAL_SYNCING_AMOUNT.decrementAndGet();
                long dataSize = getDataSizeFromRecord(record);
                TOTAL_SYNCING_SIZE.addAndGet(-dataSize);
            }
        }

    }

    public static long getDataSizeFromRecord(UnSynchronizedRecord record) {
        try {
            if (record.isFSUnsyncRecord()) {
                if (StringUtils.isBlank(record.headers.get("inodeData"))){
                    return 0L;
                }
                Inode.InodeData inodeData = Json.decodeValue(record.headers.get("inodeData"), Inode.InodeData.class);
                return inodeData.size / 1024;
            }
            return Long.parseLong(record.headers.getOrDefault(CONTENT_LENGTH, "0")) / 1024;
        } catch (Exception e) {
            logger.error("getDataSizeFromRecord err, {}", Json.encode(record), e);
            return 0L;
        }
    }

    static void clearDataCount() {
        synchronized (recordSet) {
            uploadIdRecordSet.clear();
            initUploadRecordSet.clear();
            mergeUploadRecordSet.clear();
            uploadCountSet.clear();
            recordSet.clear();
            checkRecordMap.clear();
            TOTAL_SYNCING_AMOUNT.set(0);
            TOTAL_SYNCING_SIZE.set(0);
            PART_SYNCING_AMOUNT.set(0);
//            SyncRecordLimiter.clearLimiter();
        }
    }

    public static boolean isBucketCluster(Map<String, String> bucketInfo) {
        if (!isSwitchOn(bucketInfo)) {
            return true;
        }
        if (!isMultiAliveStarted) {
            return true;
        }

        if (NODE_AMOUNT == 1) {
            //表示单站点+minio，生命周期由单站点执行
            return true;
        }

        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        return localCluster.equals(masterCluster);
//        return isBucketCluster(bucketInfo.get("bucket_name"));
    }

    public static boolean isSourceBucketCluster(String sourceSite) {
        final Set<Integer> indexSet = INDEX_NAME_MAP.keySet();
        for (Integer index : indexSet) {
            if (INDEX_NAME_MAP.get(index).equals(sourceSite)) {
                if (LOCAL_CLUSTER_INDEX.equals(index)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static String getBackUpSiteIndex(String backupSite) {
        final Set<Integer> indexSet = INDEX_NAME_MAP.keySet();
        for (Integer index : indexSet) {
            if (INDEX_NAME_MAP.get(index).equals(backupSite)) {
                return index + "";
            }
        }
        return null;
    }

    public static void setSyncState() {
        Mono.just(true).publishOn(SCAN_SCHEDULER)
                .subscribe(s -> {
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, CLUSTERS_SYNC_STATE, Json.encode(currClusterSyncMap));
                    HeartBeatChecker.siteSyncStateChange();
                });
    }


    public static final String LOCAL_SITE = ServerConfig.getInstance().getSite();

    /**
     * 桶是否开启了数据同步
     *
     * @param bucketName 桶名
     * @return
     */
    public static Mono<String> datasyncIsEnabled(String bucketName) {
        if (StringUtils.isEmpty(bucketName)) {
            return Mono.just("off");
        }
        return pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucketName)
                .defaultIfEmpty(new HashMap<>())
                .doOnNext(info -> {
                    BucketSyncSwitchCache.getInstance().check(bucketName, info);
                })
                .flatMap(info -> Mono.just(info.getOrDefault(DATA_SYNC_SWITCH, SWITCH_OFF)));
    }

    public static Boolean dataSyncIsEnabled(UnifiedMap<String, String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        if (StringUtils.isEmpty(bucketName)) {
            return false;
        }
        // 界面请求需要转发给主站点
        if ("1".equals(paramMap.get("trans-master"))) {
            return true;
        }

//        if ("0".equals(paramMap.get(SITE_FLAG.toLowerCase()))) {
//            return true;
//        }

        Map<String, String> bucketMap = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketMap.isEmpty()) {
            return true;
        }

        return isSwitchOn(bucketMap);
    }

    public static Mono<Boolean> checkBucObjNumIsEnable(String bucketName) {
        if (StringUtils.isEmpty(bucketName)) {
            return Mono.just(false);
        }
        return pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucketName)
                .defaultIfEmpty(new HashMap<>())
                .map(bucketInfo -> {
                    final String syncSwitch = bucketInfo.getOrDefault(DATA_SYNC_SWITCH, "off");
                    final String delSwitch = bucketInfo.getOrDefault(DELETE_SOURCE_SWITCH, "off");
                    BucketSyncSwitchCache.getInstance().check(bucketName, bucketInfo);
                    return SWITCH_ON.equals(syncSwitch) && SWITCH_OFF.equals(delSwitch) && getSyncIndexMap(bucketName, LOCAL_CLUSTER_INDEX).contains(MASTER_INDEX)
                            && getSyncIndexMap(bucketName, MASTER_INDEX).contains(LOCAL_CLUSTER_INDEX);
                })
                .doOnError(e -> logger.error("checkBucObjNumIsEnable error", e));
    }

    public static Mono<Boolean> archiveSwitchIsOn(String bucket) {
        if (StringUtils.isEmpty(bucket)) {
            return Mono.just(false);
        }
        return pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hget(bucket, ARCHIVE_SWITCH)
                .defaultIfEmpty("off")
                .map("on"::equals);
    }

    /**
     * 获取所需创建通的站点
     *
     * @param paramMap 请求参数
     * @return 所属站点
     */
    public static String getBucketSite(UnifiedMap<String, String> paramMap) {
        String site = LOCAL_SITE;
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!StringUtils.isEmpty(masterCluster)) {
            site = masterCluster;
        }
        if (paramMap.containsKey("site")) {
            site = paramMap.get("site");
            boolean valid = true;
            if (!paramMap.containsKey(REGION_FLAG.toLowerCase()) && StringUtils.isNotEmpty(site) && !site.equals(LOCAL_SITE)) {
                if (!paramMap.containsKey(SITE_FLAG.toLowerCase()) && !StringUtils.isEmpty(masterCluster) && !site.equals(masterCluster) && !paramMap.containsKey(DOUBLE_FLAG)) {
                    throw new MsException(ErrorNo.INVALID_SITE_CONSTRAINT, "can't create the bucket with " +
                            "the specified site in this site.");
                }
                valid = false;
                Set<String> otherClusters = pool.getCommand(REDIS_SYSINFO_INDEX).smembers("other_clusters");
                for (String otherCluster : otherClusters) {
                    JSONObject json = JSON.parseObject(otherCluster);
                    if (site.equals(json.getString(CLUSTER_NAME))) {
                        valid = true;
                        break;
                    }
                }

                String sourceSite = paramMap.get("sourcesite");
                if (
                        valid && StringUtils.isNotEmpty(sourceSite)
                                && !StringUtils.isEmpty(masterCluster)
                                && !sourceSite.equals(masterCluster)
                                && !site.equals(masterCluster)
                                && !site.equals(sourceSite)
                                && LOCAL_SITE.equals(masterCluster)) {
                    valid = false;
                }

            }

            if (!valid) {
                throw new MsException(ErrorNo.INVALID_SITE_CONSTRAINT, "the specified site does not exist or is invalid");
            }
        }

        return site;
    }

    public static void siteConstraintCheck(String bucketName, boolean containsFlag) {
        if (!isMultiAliveStarted) {
            return;
        }
        Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketInfo.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }

        if (!containsFlag && !isSwitchOn(bucketInfo) && !LOCAL_SITE.equals(bucketInfo.get(CLUSTER_NAME))) {
            throw new MsException(INVALID_SITE_CONSTRAINT, "the bucket does not belong to the current site.");
        }
    }

    public static void noSyncSiteConstraintCheck(String bucketName, boolean containsFlag) {
        if (!isMultiAliveStarted) {
            return;
        }
        Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketInfo.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        String syncPolicy = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SYNC_POLICY);
        if ("0".equals(syncPolicy)) {
            if (!containsFlag && !isSwitchOn(bucketInfo) && !LOCAL_SITE.equals(bucketInfo.get(CLUSTER_NAME))) {
                throw new MsException(INVALID_SITE_CONSTRAINT, "the bucket does not belong to the current site.");
            }
        }
    }

    public static String getOringinUploadId(UnSynchronizedRecord record) {
        if (StringUtils.isNotBlank(record.headers.get(RECORD_ORIGIN_UPLOADID))) {
            return record.headers.get(RECORD_ORIGIN_UPLOADID);
        }
        return getUploadId(record);
    }

    /**
     * 确认主备节点的前端包状态检测
     *
     * @param availSyncIpSet 心跳响应中带的本地前端包状态检测结果
     * @return 如果主备节点中有一个正常，返回true
     */
    public static boolean checkConfigNodeSet(Set<String> configNodeSyncIpSet, Set<String> availSyncIpSet) {
        for (String syncIP : configNodeSyncIpSet) {
            if (availSyncIpSet.contains(syncIP)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 1. local_cluster中的字段，保存当前的mip分布情况。
     * 2. M_IPS_ + LOCAL_CLUSTER_INDEX，set，表示目前本地有那些mip，目前没有在本地保存对端站点的mip情况
     */
    public static final String M_IPS = "m_ips";

    public static final String active_live_cluster_ip_list = "active_live_cluster_ip_list";

    public static final String active_live_ip_eth_list = "active_live_ip_eth_list";

    public static final String active_live_ip_list = "active_live_ip_list";

    private static final Set<String> local_run_mip = new ConcurrentHashSet<>();

    public static final Set<String> FRONT_ETH_NAME = new HashSet<>();

    public static void readMIps() {
        String hgetMIps = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, M_IPS);
        if (StringUtils.isBlank(hgetMIps)) {
            return;
        }

        Map<Integer, String[]> map = new HashMap<>();
        String[] split = hgetMIps.split(";");
        for (int i = 0; i < split.length; i++) {
            String[] split1 = split[i].split(",");
            map.put(i, split1);
        }

        // 未部署仲裁，手动切换ip，m_ip_ 是需要加上的vip
        if (!DAVersionUtils.isStrictConsis()) {
            Set<String> mipSet = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(M_IPS + "_" + LOCAL_CLUSTER_INDEX);
            map.put(LOCAL_CLUSTER_INDEX, mipSet.toArray(new String[0]));
            // 前端包异常恢复时，添加一遍需要加的mip
            if (firstStartMipCheck.compareAndSet(true, false)) {
                local_run_mip.addAll(mipSet);
                Mono.delay(Duration.ofSeconds(10)).subscribe(s -> {
                    Set<String> addMips = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(M_IPS + "_" + LOCAL_CLUSTER_INDEX);
                    if (addMips.size() > 0) {
                        addMultiClusterIp(LOCAL_CLUSTER_INDEX, mipSet.toArray(new String[0]), "all");
                    }
                });
            }
            // 如果本地 m_ip_index 发送变化，增加或减掉对应的 mip
            if (!firstStartMipCheck.get()) {
                ArrayList<String> addIps = new ArrayList<>();
                ArrayList<String> moveIps = new ArrayList<>();
                mipSet.forEach(ip -> {
                    if (!local_run_mip.contains(ip)) {
                        addIps.add(ip);
                    }
                });
                local_run_mip.forEach(ip -> {
                    if (!mipSet.contains(ip)) {
                        moveIps.add(ip);
                    }
                });
                if (addIps.size() > 0) {
                    addMultiClusterIp(LOCAL_CLUSTER_INDEX, addIps.toArray(new String[0]), "all");
                }
                if (moveIps.size() > 0) {
                    removeMultiClusterIp(LOCAL_CLUSTER_INDEX, moveIps.toArray(new String[0]), "all");
                }
                local_run_mip.clear();
                local_run_mip.addAll(mipSet);
            }
        }

        // 初次启动读入mips
        if (M_IPS_ENTIRE_MAP.isEmpty()) {
            M_IPS_ENTIRE_MAP.putAll(map);
            if (pool.getCommand(REDIS_SYSINFO_INDEX).exists(M_IPS + "_" + LOCAL_CLUSTER_INDEX) == 0
                    && (M_IPS_ENTIRE_MAP.get(LOCAL_CLUSTER_INDEX) != null && M_IPS_ENTIRE_MAP.get(LOCAL_CLUSTER_INDEX).length > 0)) {
                logger.info("mip init. {}", M_IPS_ENTIRE_MAP);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(M_IPS + "_" + LOCAL_CLUSTER_INDEX, M_IPS_ENTIRE_MAP.get(LOCAL_CLUSTER_INDEX));
                sendMIpChanged();
            }
            return;
        }

        Set<Integer> changedIndexSet = new HashSet<>();
        for (Map.Entry<Integer, String[]> entry : map.entrySet()) {
            if (!Arrays.equals(entry.getValue(), M_IPS_ENTIRE_MAP.get(entry.getKey()))) {
                M_IPS_ENTIRE_MAP.put(entry.getKey(), entry.getValue());
                changedIndexSet.add(entry.getKey());
            }
        }

        // 本站点只记录本站点的M_IPS_X
        if (changedIndexSet.contains(LOCAL_CLUSTER_INDEX)) {
            if (DAVersionUtils.isStrictConsis()) {
                synchronized (M_IPS_ENTIRE_MAP) {
                    logger.info("mip changed. {}", M_IPS_ENTIRE_MAP);
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(M_IPS + "_" + LOCAL_CLUSTER_INDEX);
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(M_IPS + "_" + LOCAL_CLUSTER_INDEX, M_IPS_ENTIRE_MAP.get(LOCAL_CLUSTER_INDEX));
                }
            }
            sendMIpChanged();
        }
    }

    /**
     * 考虑到后端更改需要耗时，不允许在9秒内连续添加或删除同一站点下的mip。
     */
    private static final Map<String, Long> ClusterStampMap = new ConcurrentHashMap<>();

    private static synchronized boolean mipUnderChange(int index, String marker) {
        long currentStamp = System.currentTimeMillis();
        String key = index + marker;
        Long stamp = ClusterStampMap.get(key);
        if (stamp == null) {
            ClusterStampMap.put(key, currentStamp);
            return false;
        }

        if (currentStamp - stamp > 9_000) {
            ClusterStampMap.put(key, currentStamp);
            return false;
        } else {
            return true;
        }
    }

    /**
     * 站点添加mip。
     */
    public static void addMultiClusterIp(int index, String[] ips, String marker) {
        if (ips == null) {
            return;
        }
        pool.getReactive(REDIS_SYSINFO_INDEX).hexists(LOCAL_CLUSTER, M_IPS)
                .filter(b -> b)
                .flatMapMany(b -> Flux.fromArray(ips))
                .flatMap(ip -> pool.getReactive(REDIS_SYSINFO_INDEX).hexists(active_live_cluster_ip_list, ip)
                        .filter(b -> b)
                        .flatMap(b -> pool.getReactive(REDIS_SYSINFO_INDEX).hget(active_live_ip_list, ip))
                        .filter(str -> {
                            // none表示mip在对站点，0001表示mip在本地站点的0001节点
                            if (index == LOCAL_CLUSTER_INDEX) {
                                return "none".equals(str);
                            } else {
                                return !"none".equals(str);
                            }
                        })
                        .map(s -> ip)
                )
                .collectList()
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(list -> {
                            if (list.isEmpty()) {
                                return;
                            }
                            if (mipUnderChange(index, "add_" + marker)) {
                                return;
                            }
                            String[] ipArray = list.toArray(new String[0]);
                            try {
                                synchronized (M_IPS_ENTIRE_MAP) {
                                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(M_IPS + "_" + index, ipArray);
                                }
                                logger.info("add mip, {}, {}", index, ipArray);
                                sendMIpChanged();
                            } catch (Exception e) {
                                logger.error("addMultiClusterIp", e);
                            }
                        }
                )
                .timeout(Duration.ofSeconds(30))
                .doOnError(e -> logger.error("addMultiClusterIp error2, {}, {}", index, ips, e))
                .subscribe();
    }

    /**
     * 站点下掉mip。
     */
    public static void removeMultiClusterIp(int index, String[] ips, String marker) {
        if (ips == null) {
            return;
        }

        pool.getReactive(REDIS_SYSINFO_INDEX).hexists(LOCAL_CLUSTER, M_IPS)
                .filter(b -> b)
                .flatMapMany(b -> Flux.fromArray(ips))
                .flatMap(ip -> pool.getReactive(REDIS_SYSINFO_INDEX).hget(active_live_ip_list, ip)
                        .filter(str -> {
                            // none表示mip在对站点，0001表示mip在本地站点的0001节点
                            if (index == LOCAL_CLUSTER_INDEX) {
                                return !"none".equals(str);
                            } else {
                                return "none".equals(str);
                            }
                        })
                        .map(s -> ip)
                )
                .collectList()
                .publishOn(SCAN_SCHEDULER)
                .doOnNext(list -> {
                    if (list.isEmpty()) {
                        return;
                    }
                    if (mipUnderChange(index, "remove_" + marker)) {
                        return;
                    }
                    String[] ipArray = list.toArray(new String[0]);
                    try {
                        synchronized (M_IPS_ENTIRE_MAP) {
                            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem(M_IPS + "_" + index, ipArray);
                        }
                        logger.info("remove mip, {}, {}", index, ipArray);
                        sendMIpChanged();
                    } catch (Exception e) {
                        logger.error("removeMultiClusterIp", e);
                    }
                })
                .timeout(Duration.ofSeconds(30))
                .doOnError(e -> logger.error("removeMultiClusterIp error, {}, {}", index, ips, e))
                .subscribe();
    }

    public static void checkMipInSuccess(int successIndex) {
        //没有部署仲裁也可以指定mip，只是漂移失效。复制站点也不需要。
        if (!DAVersionUtils.isStrictConsis() || ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(LOCAL_CLUSTER_INDEX)
                || ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(successIndex)) {
            return;
        }
        // 自身心跳检测成功且isIsolated判断通过，且能正常获取主站点的DA_VERSIONNUM，恢复自己的mip
        if (successIndex == LOCAL_CLUSTER_INDEX && !isEvaluatingMaster.get() && DAVersionUtils.canGet.get()) {
            addMultiClusterIp(LOCAL_CLUSTER_INDEX, M_IPS_ENTIRE_MAP.get(LOCAL_CLUSTER_INDEX), "all" + successIndex);
        }

        // 本地检测到有其他站点连接状态恢复。
        if (successIndex != LOCAL_CLUSTER_INDEX) {
            // 该站点的某个前端网口坏了，且该站点和本地站点起码有一个节点可用，添加该网口的mip至本地对应网口
            // 该站点的某个前端网口正常，就把该网口的mip从本地下掉
            for (String eth : FRONT_ETH_NAME) {
                if (!checkEthAvail(eth, successIndex) && checkIndexAvail(successIndex) && checkIndexAvail(LOCAL_CLUSTER_INDEX)) {
                    getEthIps(successIndex, eth)
                            .subscribe(ips -> addMultiClusterIp(LOCAL_CLUSTER_INDEX, ips, eth + successIndex));
                } else {
                    getEthIps(successIndex, eth)
                            .subscribe(ips -> removeMultiClusterIp(LOCAL_CLUSTER_INDEX, ips, eth + successIndex));
                }
            }
        }
    }

    /**
     * 站点的节点状态有一个可用就返回true
     */
    private static boolean checkIndexAvail(int index) {
        Map<String, String> map = INDEX_AVAIL_MAP.get(index);
        if (map == null) {
            return false;
        }
        return map.containsValue("1");
    }

    private static final Map<Integer, AtomicInteger> addFailCountMap = new ConcurrentHashMap<>();
    private static final int maxFailCount = 3;

    public static void checkMipInFail(int failIndex) {
        //没有部署仲裁也可以指定mip，只是漂移失效。复制站点也不需要。
        if (!DAVersionUtils.isStrictConsis() || ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(LOCAL_CLUSTER_INDEX)
                || ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(failIndex)) {
            return;
        }

        // 自身心跳检测失败且isIsolated，下掉所有的mip
        if (failIndex == LOCAL_CLUSTER_INDEX && isEvaluatingMaster.get()) {
            for (Integer index : M_IPS_ENTIRE_MAP.keySet()) {
                removeMultiClusterIp(LOCAL_CLUSTER_INDEX, M_IPS_ENTIRE_MAP.get(index), "all");
            }
            return;
        }

        // 自己心跳正常且isIsolated判断通过，表示检测到其他站点连接状态异常
        // 如果本地是主站点，直接接管对站点mip（isEvaluatingMaster更改不一定及时，稍微延后防止双ip）
        if (LOCAL_CLUSTER_INDEX.equals(MASTER_INDEX) && !isEvaluatingMaster.get()) {
            if (failIndex != LOCAL_CLUSTER_INDEX && (!checkIndexAvail(failIndex) || !checkIndexAvail(LOCAL_CLUSTER_INDEX))) {
                // 对端站点如果不可用或者本地站点也不可用，对端的ip也不会漂移到本地站点
                return;
            }
            // 主站点eth12 down后要切主，到切主完成前不能添加对端vip，因为isEvaluatingMaster变化为true需要时间，这里根据收到所有站点心跳失败响应的三倍进行延迟
            if (addFailCountMap.computeIfAbsent(failIndex, k -> new AtomicInteger()).incrementAndGet() < maxFailCount * CLUSTERS_AMOUNT) {
                return;
            }
            addMultiClusterIp(LOCAL_CLUSTER_INDEX, M_IPS_ENTIRE_MAP.get(failIndex), "all");
            addFailCountMap.computeIfAbsent(failIndex, k -> new AtomicInteger()).set(0);
        } else {
            if (failIndex == MASTER_INDEX) {
                // 如果本地是从站点，异常的是主站点，直接下掉自己的mip。因为获取不到全局版本号所以本就无法处理业务。
                removeMultiClusterIp(LOCAL_CLUSTER_INDEX, M_IPS_ENTIRE_MAP.get(LOCAL_CLUSTER_INDEX), "all");
            } else {
                /*
                多活带仲裁的情况。
                 */
            }
        }
    }

    private static void sendMIpChanged() {
        SocketReqMsg dmSocket = new SocketReqMsg("sendMIpChanged", 0);
        sender.sendAndGetResponse(dmSocket, StringResMsg.class, true);
        logger.info("send msg to ms_cloud change mip !!!");
    }

    /**
     * 确认指定站点的eth2状态。有一个节点eth2正常就返回true。
     */
    private static boolean checkEthAvail(String eth, int clusterIndex) {
        Map<String, String> map = null;
        if (eth.equals("eth2")) {
            map = INDEX_ETH2_MAP.get(clusterIndex);
        } else if (eth.equals("eth3")) {
            map = INDEX_ETH3_MAP.get(clusterIndex);
        }

        // 未初始化，默认eth正常
        if (map == null || map.size() == 0) {
            return true;
        }
        for (String eth2Status : map.values()) {
            String substring = eth2Status.substring(0, 1);
            if ("1".equals(substring)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取指定站点的指定网口的mips
     */
    private static Mono<String[]> getEthIps(int clusterIndex, String eth) {
        String[] totalIps = M_IPS_ENTIRE_MAP.get(clusterIndex);
        if (totalIps == null || totalIps.length == 0) {
            return Mono.empty();
        }
        return Flux.fromArray(totalIps)
                .flatMap(ip ->
                        pool.getReactive(REDIS_SYSINFO_INDEX).hget(active_live_cluster_ip_list, ip)
                                .filter(clusterName -> StringUtils.isNotBlank(clusterName) && clusterName.equals(INDEX_NAME_MAP.get(clusterIndex)))
                                .flatMap(s -> pool.getReactive(REDIS_SYSINFO_INDEX).hget(active_live_ip_eth_list, ip))
                                .filter(eth::equals)
                                .map(s -> ip)
                )
                .collectList()
                .map(list -> list.toArray(new String[0]))
                .doOnError(e -> logger.error("getEthIps error, ", e))
                .onErrorReturn(null);
    }

    public static final AtomicBoolean isCheckDealingNormal = new AtomicBoolean();

    private static final Map<String, String> BUCKET_STAMP_MAP = new ConcurrentHashMap<>();

    /**
     * 获取所有节点几个上传相关的接口目前正在处理的最小的versionNum。
     */
    public static void checkDealingVersionNum() {
        List<String> ipList = NodeCache.getCache().keySet().stream().sorted().map(NodeCache::getIP).collect(Collectors.toList());
        List<Tuple3<String, String, String>> nodeList = new ArrayList<>(ipList.size());
        for (String ip : ipList) {
            Tuple3<String, String, String> tuple3 = new Tuple3<>(ip, "", "");
            nodeList.add(tuple3);
        }
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(t -> new SocketReqMsg("", 0)
                        .put("getMinStamp", "1")
                )
                .collect(Collectors.toList());
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, IF_PUT_DONE, String.class, nodeList);
        // 所有节点最小的versionNum
        Map<String, String> resMap = new ConcurrentHashMap<>();
        AtomicBoolean check = new AtomicBoolean(true);
        responseInfo.responses
                .timeout(Duration.ofSeconds(10))
                .subscribe(s -> {
                    if (s.var2 == ErasureServer.PayloadMetaType.SUCCESS) {
                        Map<String, String> map = Json.decodeValue(s.var3, new TypeReference<Map<String, String>>() {
                        });
                        synchronized (resMap) {
                            map.forEach((bucket, stamp) -> {
                                String curS = resMap.get(bucket);
                                if (StringUtils.isBlank(curS)) {
                                    resMap.put(bucket, stamp);
                                } else {
                                    if (curS.compareTo(stamp) > 0) {
                                        resMap.put(bucket, stamp);
                                    }
                                }
                            });
                        }
                    } else {
                        check.compareAndSet(true, false);
                    }
                }, e -> {
                    isCheckDealingNormal.compareAndSet(true, false);
                    logger.error("", e);
                }, () -> {
                    logger.debug("checkStampSchedule complete, {}", check.get());
                    if (!check.get()) {
                        isCheckDealingNormal.compareAndSet(true, false);
                        return;
                    }

                    BUCKET_STAMP_MAP.putAll(resMap);

                    BUCKET_STAMP_MAP.entrySet().removeIf(next -> !resMap.containsKey(next.getKey()));


                    isCheckDealingNormal.compareAndSet(false, true);
                    logger.debug("BUCKET_STAMP_MAP: {}", BUCKET_STAMP_MAP);
                });

    }

    /**
     * 判断预提交记录能否开始处理。所有节点当前未处理完毕的最小的stamp比差异记录的stamp更小或相等，说明该差异记录需要延迟处理
     *
     * @return true表示该预提交记录尚不能处理
     */
    public static boolean needDelayRecordDeal(String bucket, UnSynchronizedRecord record) {
        // 非预提交记录不需要延迟处理
        if (record.commited) {
            return false;
        }

        // 预提交记录且生成时间小于五分钟，默认需要延迟处理
        String stamp = getStampFromRecord(record);
        long nodeAmount = 60 * 5 * 1000L;
        if (!record.commited && ((System.currentTimeMillis() - Long.parseLong(stamp)) < nodeAmount)) {
            return true;
        }

        String curS = BUCKET_STAMP_MAP.get(bucket);
        if (StringUtils.isBlank(curS)) {
            return false;
        } else {
            return curS.compareTo(stamp) <= 0;
        }
    }

    public static String getStampFromRecord(UnSynchronizedRecord record) {
        String stamp = record.headers.get("stamp");
        if (StringUtils.isBlank(stamp)) {
            stamp = DAVersionUtils.verNum2timeStamp(record.syncStamp);
        }
        return stamp;
    }

    public static Tuple2<String, Long> getRequestTimeout(String[] ips, long timeout, int unitCount, AtomicInteger count) {
        if (count.get() > ips.length - 1) {
            count.set(0);
        }
        String ip = ips[count.get() % ips.length];
        count.incrementAndGet();
        long l = timeout / unitCount;
        return new Tuple2<>(ip, l);
    }

    public static void deleteBySyncAction(Map<String, String> headers) {
        JSONObject jsonParam = new JSONObject();
        jsonParam.put("stamp", headers.get("stamp"));
        jsonParam.put("bucketName", headers.get(BUCKET_NAME));
        SocketReqMsg socket = new SocketReqMsg("deleteBySyncAction", 0).put("bucketInfo", jsonParam.toString());
        Mono.just(socket)
                .publishOn(SCAN_SCHEDULER)
                .subscribe(s -> {
                    StringResMsg resMsg = sender.sendAndGetResponse(s, StringResMsg.class, true);
                    if (resMsg.getCode() != ErrorNo.SUCCESS_STATUS) {
                        logger.error("deleteBySyncAction error {} {}", headers.get("stamp"), headers.get(BUCKET_NAME));
                    }
                });
    }
}
