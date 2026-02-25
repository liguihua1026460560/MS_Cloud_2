package com.macrosan.utils.regions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.action.managestream.BucketService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.etcd.EtcdClient;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.Utils;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.BaseResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.socketmsg.SocketSender;
import com.macrosan.message.xmlmsg.region.CreateBucketConfiguration;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.serialize.JaxbUtils;
import io.etcd.jetcd.KeyValue;
import io.lettuce.core.SetArgs;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DoubleActiveUtil.LOCAL_SITE;
import static com.macrosan.doubleActive.HeartBeatChecker.isMultiAliveStarted;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.httpserver.MossHttpClient.*;

/**
 * @author chengyinfeng
 */
@Log4j2
public class MultiRegionUtils {

    private static final Logger logger = LogManager.getLogger(MultiRegionUtils.class.getName());

    private static RedisConnPool pool = RedisConnPool.getInstance();

    protected static SocketSender sender = SocketSender.getInstance();

    private static final String DNS = ServerConfig.getInstance().getDns();

    private static final String UUID = ServerConfig.getInstance().getHostUuid();

    private static final String BUCKET_LOCK = "_bucket_lock";

    private static final String LOCAL_REGION = ServerConfig.getInstance().getRegion();

    private static SetArgs args = new SetArgs().ex(30).nx();

    /**
     * @param db  redis表
     * @param key redis字段
     * @return 锁
     */
    public static int getSingleLock(int db, String key) {

        String lockRes = pool.getShortMasterCommand(db).set(key + BUCKET_LOCK, MULTI_REGION_TYPE_LOCK, args);
        if (!REDIS_OK.equals(lockRes)) {
            return ErrorNo.UNKNOWN_ERROR;
        }
        return ErrorNo.SUCCESS_STATUS;
    }

    public static void deleteRedisLock(String key) {
        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(key + BUCKET_LOCK);
    }

    /**
     * 获取redis锁
     *
     * @param db  redis表
     * @param key 锁字段
     */
    public static void getRedisLock(int db, String key, boolean isActive) {
        SocketReqMsg msg;
        if (isActive) {
            msg = new SocketReqMsg(MSG_TYPE_SITE_GET_REDIS_LOCK, 0)
                    .put("key", key).put("db", String.valueOf(db));
        } else {
            msg = new SocketReqMsg(MSG_TYPE_REGIONS_GET_REDIS_LOCK, 0)
                    .put("key", key).put("db", String.valueOf(db));
        }

        int resCode;
        if (isMultiAliveStarted && !USE_ETH4 && !Arrays.asList(INDEX_IPS_MAP.get(LOCAL_CLUSTER_INDEX)).contains(LOCAL_NODE_IP) && ETH12_INDEX_MAP.get(INDEX_IPS_MAP.get(LOCAL_CLUSTER_INDEX)[0]) != null) {
            resCode = sender.sendAndGetResponse(ETH12_INDEX_MAP.get(INDEX_IPS_MAP.get(LOCAL_CLUSTER_INDEX)[0]), msg, BaseResMsg.class, false).getCode();
        } else {
            resCode = sender.sendAndGetResponse(msg, BaseResMsg.class, false).getCode();
        }

        if (resCode != ErrorNo.SUCCESS_STATUS) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "obtain lock failed, key:" + key);
        }
    }

    /**
     * 将源region的bucket信息写入远端的region
     *
     * @param paramMap 请求参数
     * @param region   区域
     */
    private static void createRemoteBucketReq(UnifiedMap<String, String> paramMap, String region, String bucketLocation, boolean onlyEtcd) {

        SocketReqMsg msg;
        if (onlyEtcd) {
            msg = new SocketReqMsg(MSG_TYPE_REGIONS_BUCKET_TOETCD, 0).put("action", "createBucket");
        } else {
            msg = new SocketReqMsg(MSG_TYPE_REGIONS_CREATE_BUCKET, 0);
        }
        UnifiedMap<String, String> reqMap = new UnifiedMap<>();
        getParam(paramMap, reqMap, msg);
        reqMap.put("bucketLocation", bucketLocation);
        msg.put("region", region).put("bucketLocation", bucketLocation)
                .put("bucket", paramMap.get(BUCKET_NAME)).put("param", JSON.toJSONString(reqMap));
        BaseResMsg resMsg = sender.sendAndGetResponse(msg, BaseResMsg.class, false);
        int resCode = resMsg.getCode();
        if (resCode != ErrorNo.SUCCESS_STATUS) {
            throw new MsException(resCode, "create remote bucket failed.");
        }
    }

    private static void getParam(UnifiedMap<String, String> paramMap, UnifiedMap<String, String> reqMap, SocketReqMsg msg) {
        paramMap.forEach((k, v) -> {
            if (k.startsWith("x-amz-grant-") || "x-amz-acl".equals(k) || "metadata-analysis".equals(k) || "site".equals(k)
                    || "object-lock-enabled-for-bucket".equals(k) || DATA_SYNC_SWITCH.equals(k) || USER_ID.equals(k) || SERVER_AK.equals(k)
                    || SERVER_SK.equals(k) || BUCKET_NAME.equals(k) || "sourcesite".equals(k) || ARCHIVE_INDEX.equals(k) || SYNC_INDEX.equals(k)) {
                reqMap.put(k, v);
            }
        });
        reqMap.put(REGION_FLAG, "1");
        addReqParam(paramMap, reqMap, msg);
    }

    /**
     * 多区域下创建桶的处理
     *
     * @param paramMap 参数
     * @return 是否流程继续
     */
    public static boolean dealRegionsCreateBucket(UnifiedMap<String, String> paramMap) {
        String bucketLocation = getBucketLocation(paramMap);
        String bucketName = paramMap.get(BUCKET_NAME);
        String defaultRegion = pool.getCommand(REDIS_SYSINFO_INDEX).get(MULTI_REGION_DEFAULT_REGION);
        boolean continueFlag = true;
        try {
            if (StringUtils.isNotEmpty(defaultRegion)) {
                if (LOCAL_REGION.equals(defaultRegion)) {
                    // 先记录到etcd 写日志，创桶成功同步到etcd后删日志
                    insertSharedLog(paramMap, defaultRegion, bucketLocation, true);
                }
                if (paramMap.containsKey(REGION_FLAG) || paramMap.containsKey(REGION_FLAG.toLowerCase())) {
                    logger.info("contains flag, need not get lock");
                    return true;
                }
                //1、多区域 获取许可
                if (LOCAL_REGION.equals(defaultRegion)) {
                    //本身是中心区域
                    if (ErrorNo.SUCCESS_STATUS != getSingleLock(2, bucketName)) {
                        throw new MsException(ErrorNo.UNKNOWN_ERROR,
                                "get bucket lock error！bucket: " + bucketName + ".");
                    }
                    logger.info("get lock in center, bucket name :{}", bucketName);
                } else {
                    //本身不是中心区域,向中心区域获取锁
                    logger.info("get lock from center, bucket name :{}", bucketName);
                    getRedisLock(2, bucketName, false);
                }

                //true：当前区域即为桶所属区域，创桶即可，创桶成功之后请求中心区域创桶
                if (!LOCAL_REGION.equals(bucketLocation)) {
                    //false：当前区域不是桶所属区域,向桶所在区域请求创桶
                    continueFlag = false;
                    createRemoteBucketReq(paramMap, bucketLocation, bucketLocation, false);
                }
            }
        } catch (Exception e) {
            deleteSharedLog(paramMap, true);
            throw e;
        }
        return continueFlag;
    }

    private static String getSharedKey(boolean add, UnifiedMap<String, String> paramMap) {
        if (add) {
            return "/log/bucket/regions/" + paramMap.get("ctime") + "/" + UUID + "/" + paramMap.get(BUCKET_NAME);
        } else {
            return "/log/bucket/regions/" + paramMap.get("ctime") + "/" + UUID + "/" + paramMap.get(BUCKET_NAME) + "/";
        }
    }

    private static String getCacheKey(String bucketName) {
        return "/cache/" + bucketName;
    }

    public static void checkBucketExist(String bucketName) {
        KeyValue keyValue = EtcdClient.get(getCacheKey(bucketName));
        if (keyValue == null) {
            return;
        }
        UnifiedMap<String, String> cacheMap = Json.decodeValue(new String(keyValue.getValue().getBytes()),
                new TypeReference<UnifiedMap<String, String>>() {
                });
        if (cacheMap.containsKey("operateType") && "add".equals(cacheMap.get("operateType"))) {
            cacheMap.put("operateTime", String.valueOf(System.currentTimeMillis()));
            EtcdClient.put(getCacheKey(bucketName), Json.encode(cacheMap));
            cacheMap.remove("operateTime");
            cacheMap.remove("operateType");
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hmset(bucketName, cacheMap);
            pool.getShortMasterCommand(REDIS_USERINFO_INDEX).sadd(cacheMap.get("user_id") + USER_BUCKET_SET_SUFFIX, bucketName);
            throw new MsException(ErrorNo.BUCKET_EXISTS,
                    "The bucket is already existed.bucket: " + bucketName + ".");
        }
    }

    public static void startDeleteExpireCacheScheduler() {
        deleteExpireCache();
    }

    private static void deleteExpireCache() {
        try {
            if (!"master".equals(Utils.getRoleState())) {
                return;
            }
            long cacheCount = EtcdClient.getCountByPrefix("/cache/");
            if (cacheCount == 0) {
                return;
            }
            List<KeyValue> keyValues = EtcdClient.selectByPrefixWithAscend("/cache/");
            long currentTime = System.currentTimeMillis();
            long time = 5 * 60 * 1000;
            if (cacheCount >= 10000) {
                time = 10 * 1000;
            } else if (cacheCount >= 100) {
                time = 60 * 1000;
            }
            for (KeyValue kv : keyValues) {
                String key = new String(kv.getKey().getBytes());
                String bucketName = key.split("/")[2];
                boolean lock = false;
                try {
                    lock = BucketService.acquireLock(bucketName, 0);
                    if (!lock) {
                        log.info("A bucket {} is being created or deleted", bucketName);
                        continue;
                    }
                    // 获取锁成功后，再次获取最新的日志
                    KeyValue keyValue = EtcdClient.get(key);
                    if (keyValue == null) {
                        continue;
                    }
                    String value = new String(keyValue.getValue().getBytes());
                    UnifiedMap<String, String> cacheMap = Json.decodeValue(value,
                            new TypeReference<UnifiedMap<String, String>>() {
                            });
                    long bucketExist = pool.getCommand(REDIS_BUCKETINFO_INDEX).exists(bucketName);
                    long cacheTime = Long.parseLong(cacheMap.get("operateTime"));
                    if (cacheMap.containsKey("operateType") && "add".equals(cacheMap.get("operateType"))) {
                        if (bucketExist != 1) {
                            cacheMap.put("operateTime", String.valueOf(System.currentTimeMillis()));
                            EtcdClient.put(key, Json.encode(cacheMap));
                            cacheMap.remove("operateTime");
                            cacheMap.remove("operateType");
                            for (String ip : RabbitMqUtils.HEART_IP_LIST) {
                                pool.getShortMasterCommand(REDIS_ACTION_INDEX).lpush(ip + "_reload_bucket_shard_cache_queue", bucketName);
                            }
                            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hmset(bucketName, cacheMap);
                            pool.getShortMasterCommand(REDIS_USERINFO_INDEX).sadd(cacheMap.get("user_id") + USER_BUCKET_SET_SUFFIX, bucketName);
                            log.info("create bucket {} recover.", bucketName);
                        } else if (currentTime - cacheTime > time) {
                            EtcdClient.delete(key);
                        }
                    } else {
                        if (bucketExist == 1) {
                            cacheMap.put("operateTime", String.valueOf(System.currentTimeMillis()));
                            EtcdClient.put(key, Json.encode(cacheMap));
                            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).del(bucketName);
                            pool.getShortMasterCommand(REDIS_USERINFO_INDEX).srem(cacheMap.get("user_id") + USER_BUCKET_SET_SUFFIX, bucketName);
                            log.info("delete bucket {} recover.", bucketName);
                        } else if (currentTime - cacheTime > time) {
                            EtcdClient.delete(key);
                        }
                    }
                } finally {
                    if (lock) {
                        BucketService.releaseLock(bucketName);
                    }
                }
            }
        } catch (Exception e) {
            log.error("deleteExpireCache error", e);
        } finally {
            DISK_SCHEDULER.schedule(MultiRegionUtils::deleteExpireCache, 1, TimeUnit.MINUTES);
        }

    }

    private static void insertSharedLog(UnifiedMap<String, String> paramMap, String region, String bucketLocation, boolean add) {
        UnifiedMap<String, String> reqMap = new UnifiedMap<>();
        getParam(paramMap, reqMap, new SocketReqMsg(MSG_TYPE_REGIONS_BUCKET_TOETCD, 0));
        reqMap.put("bucketLocation", bucketLocation);
        reqMap.put("bucket", paramMap.get(BUCKET_NAME));
        reqMap.put("bucketRegion", bucketLocation);
        JSONObject json = new JSONObject();
        json.put("region", region);
        json.put("bucket", paramMap.get(BUCKET_NAME));
        json.put("param", JSON.toJSONString(reqMap));
        json.put("bucketLocation", bucketLocation);
        String key = getSharedKey(add, paramMap);
        json.put("action", add ? "createBucket" : "deleteBucket");
        EtcdClient.put(key, json.toJSONString());
    }

    public static void deleteSharedLog(UnifiedMap<String, String> paramMap, boolean add) {
        String defaultRegion = pool.getCommand(REDIS_SYSINFO_INDEX).get(MULTI_REGION_DEFAULT_REGION);
        if (StringUtils.isNotEmpty(defaultRegion) && LOCAL_REGION.equals(defaultRegion)) {
            EtcdClient.delete(getSharedKey(add, paramMap));
            if (add) {
                deleteRedisLock(paramMap.get(BUCKET_NAME));
            }
        }
    }

    private static void deleteExpireCache(UnifiedMap<String, String> paramMap, boolean add) {
        try {
            String bucketName = paramMap.get(BUCKET_NAME);
            List<KeyValue> keyValues = EtcdClient.selectByPrefixWithAscend("/cache/");
            long currentTime = System.currentTimeMillis();
            long time = 5 * 60 * 1000;
            if (keyValues.size() >= 10000) {
                time = 10 * 1000;
            } else if (keyValues.size() >= 100) {
                time = 60 * 1000;
            }
            boolean exist = false;
            Set<String> bucketSet = new HashSet<>();
            for (KeyValue kv : keyValues) {
                String key = new String(kv.getKey().getBytes());
                String value = new String(kv.getValue().getBytes());
                String[] keySplits = key.split("/");
                long cacheTime = Long.parseLong(keySplits[2]);
                String cacheBucket = keySplits[3];
                long bucketExist = pool.getCommand(REDIS_BUCKETINFO_INDEX).exists(cacheBucket);
                if (key.endsWith("*")) {
                    //直接删除redis 和 etcd
                    if (bucketName.equals(cacheBucket) && add) {
                        EtcdClient.delete(key);
                        continue;
                    }
                    if (bucketExist == 1) {
                        recoverDelBucket(bucketName, value, cacheBucket, key);
                        bucketSet.add(bucketName);
                    }
                    if (currentTime - cacheTime > time && !bucketSet.contains(bucketName)) {
                        EtcdClient.delete(key);
                    }
                } else if (key.endsWith("!")) {
                    String sourceKey = key.substring(0, key.length() - 16);
                    if (key.contains("*")) {
                        if (bucketName.equals(cacheBucket) && add) {
                            EtcdClient.delete(key);
                            continue;
                        }
                        if (bucketExist == 1) {
                            recoverDelBucket(bucketName, value, cacheBucket, sourceKey);
                            EtcdClient.delete(key);
                            bucketSet.add(bucketName);
                        }
                        cacheTime = Long.parseLong(keySplits[5]);
                    } else {
                        if (bucketName.equals(cacheBucket) && !add) {
                            EtcdClient.delete(key);
                            continue;
                        }
                        if (bucketExist != 1) {
                            recoverBucket(sourceKey, value, cacheBucket);
                            EtcdClient.delete(key);
                            bucketSet.add(bucketName);
                        }
                        cacheTime = Long.parseLong(keySplits[4]);
                    }
                    if (currentTime - cacheTime > time && !bucketSet.contains(bucketName)) {
                        EtcdClient.delete(key);
                    }
                } else {
                    //查看当前redis中是否存在桶 不存在则进行补全
                    try {
                        if (bucketName.equals(cacheBucket) && !add) {
                            EtcdClient.delete(key);
                            continue;
                        }
                        if (bucketExist != 1) {
                            recoverBucket(key, value, cacheBucket);
                            EtcdClient.delete(key);
                            bucketSet.add(bucketName);
                        }
                        if (currentTime - cacheTime > time && !bucketSet.contains(bucketName)) {
                            EtcdClient.delete(key);
                        }
                    } catch (Exception e) {
                        logger.info("update bucket: {} error.", cacheBucket, e);
                    }
                }
                if (bucketName.equals(cacheBucket)) {
                    if (key.contains("*")) {
                        exist = false;
                    } else {
                        exist = add;
                    }
                }
            }
            if (exist) {
                throw new MsException(ErrorNo.BUCKET_EXISTS,
                        "The bucket is already existed.bucket: " + bucketName + ".");
            }
        } catch (MsException e) {
            throw e;

        } catch (Exception e) {
            logger.error("delete expire cache error.", e);
        }
    }

    private static void recoverBucket(String key, String value, String cacheBucket) {
        logger.info("recover new bucket: " + cacheBucket);
        Map<String, String> bucketMap = Json.decodeValue(value,
                new TypeReference<Map<String, String>>() {
                });
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hmset(cacheBucket, bucketMap);
        pool.getShortMasterCommand(REDIS_USERINFO_INDEX).sadd(bucketMap.get("user_id") + USER_BUCKET_SET_SUFFIX, cacheBucket);
        EtcdClient.put(key + "/" + System.currentTimeMillis() + "/!", value);
    }

    private static void recoverDelBucket(String bucketName, String value, String cacheBucket, String sourceKey) {
        UnifiedMap<String, String> bucketMap = Json.decodeValue(value,
                new TypeReference<UnifiedMap<String, String>>() {
                });
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).del(cacheBucket);
        pool.getShortMasterCommand(REDIS_USERINFO_INDEX).srem(bucketMap.get("_id") + USER_BUCKET_SET_SUFFIX, bucketName);
        EtcdClient.put(sourceKey + "/" + System.currentTimeMillis() + "/!", value);
    }

    /**
     * 将桶和区域写入etcd
     *
     * @param regionName 区域
     * @param paramMap   请求参数
     */
    private static void insertDnsAndCacheLog(String regionName, UnifiedMap<String, String> paramMap, boolean add) {

        String bucketName = paramMap.get(BUCKET_NAME);
        Map<String,String> dnsRecored = new LinkedHashMap<>();
        Set<String> dnsNameSet = new HashSet<>();
        boolean flag = false;
        int businessNum = Integer.parseInt(pool.getCommand(REDIS_SYSINFO_INDEX).get("businessEthNum"));
        Long existVlan = pool.getCommand(REDIS_SYSINFO_INDEX).exists("eth2" + BUSINESS_VLAN_NETMASK_SUFFIX);
        Long existIpv6Vlan = pool.getCommand(REDIS_SYSINFO_INDEX).exists("eth2" + BUSINESS_VLAN_IPV6_NETMASK_SUFFIX);
        if (existVlan == 1 || existIpv6Vlan == 1) {
            Long existVlanDns = pool.getCommand(REDIS_SYSINFO_INDEX).exists("eth2" + BUSINESS_VLAN_DNS_SUFFIX);
            if (existVlanDns == 1) {
                Map<String, String> vlanDnsMap = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall("eth2" + BUSINESS_VLAN_DNS_SUFFIX);
                for (Map.Entry<String, String> entry : vlanDnsMap.entrySet()) {
                    String dnsNames = entry.getValue();
                    String[] dnsNameArr = dnsNames.split(",");
                    for (String dnsName : dnsNameArr) {
                        dnsNameSet.add(dnsName);
                    }
                }
            }
        }else {
            flag = true;
        }
        existVlan = pool.getCommand(REDIS_SYSINFO_INDEX).exists("eth3" + BUSINESS_VLAN_NETMASK_SUFFIX);
        existIpv6Vlan = pool.getCommand(REDIS_SYSINFO_INDEX).exists("eth3" + BUSINESS_VLAN_IPV6_NETMASK_SUFFIX);
        if (existVlan == 1 || existIpv6Vlan == 1) {
            Long existVlanDns = pool.getCommand(REDIS_SYSINFO_INDEX).exists("eth3" + BUSINESS_VLAN_DNS_SUFFIX);
            if (existVlanDns == 1) {
                Map<String, String> vlanDnsMap = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall("eth3" + BUSINESS_VLAN_DNS_SUFFIX);
                for (Map.Entry<String, String> entry : vlanDnsMap.entrySet()) {
                    String dnsNames = entry.getValue();
                    String[] dnsNameArr = dnsNames.split(",");
                    for (String dnsName : dnsNameArr) {
                        dnsNameSet.add(dnsName);
                    }
                }
            }
        }else {
            Long dns_name1 = pool.getCommand(REDIS_SYSINFO_INDEX).exists(DNS_NAME1);
            if (dns_name1 == 1){
                String dnsNames = pool.getCommand(REDIS_SYSINFO_INDEX).get(DNS_NAME1);
                String[] dnsNameArr = dnsNames.split(",");
                for (String dnsName : dnsNameArr) {
                    dnsNameSet.add(dnsName);
                }
            }else {
                if (businessNum >= 2) {
                    flag = true;
                }
            }
        }
        existVlan = pool.getCommand(REDIS_SYSINFO_INDEX).exists("eth18" + BUSINESS_VLAN_NETMASK_SUFFIX);
        existIpv6Vlan = pool.getCommand(REDIS_SYSINFO_INDEX).exists("eth18" + BUSINESS_VLAN_IPV6_NETMASK_SUFFIX);
        if (existVlan == 1 || existIpv6Vlan == 1) {
            Long existVlanDns = pool.getCommand(REDIS_SYSINFO_INDEX).exists("eth18" + BUSINESS_VLAN_DNS_SUFFIX);
            if (existVlanDns == 1) {
                Map<String, String> vlanDnsMap = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall("eth18" + BUSINESS_VLAN_DNS_SUFFIX);
                for (Map.Entry<String, String> entry : vlanDnsMap.entrySet()) {
                    String dnsNames = entry.getValue();
                    String[] dnsNameArr = dnsNames.split(",");
                    for (String dnsName : dnsNameArr) {
                        dnsNameSet.add(dnsName);
                    }
                }
            }
        }else {
            Long dns_name2 = pool.getCommand(REDIS_SYSINFO_INDEX).exists(DNS_NAME2);
            if (dns_name2 == 1){
                String dnsNames = pool.getCommand(REDIS_SYSINFO_INDEX).get(DNS_NAME2);
                String[] dnsNameArr = dnsNames.split(",");
                for (String dnsName : dnsNameArr) {
                   dnsNameSet.add(dnsName);
                }
            }else {
                if (businessNum >= 3) {
                    flag = true;
                }
            }
        }
        if (flag) {
            Long res = pool.getCommand(REDIS_SYSINFO_INDEX).exists(DNS_NAME);
            String dnsNames = pool.getCommand(REDIS_SYSINFO_INDEX).get(DNS_NAME);
            String[] dnsNameArr = dnsNames.split(",");
            for (String dnsName : dnsNameArr) {
                dnsNameSet.add(dnsName);
            }
        }
        for (String dnsName : dnsNameSet) {
            String dnsKey = EtcdClient.getDnsRecordPrefix(bucketName,dnsName);            String dnsValue = EtcdClient.getRecordStr(EtcdClient.getRegionDns(regionName,dnsName));
            dnsRecored.put(dnsKey,dnsValue);
        }
        String cacheKey = getCacheKey(bucketName);
        try {
            Map<String, String> cacheMap = new HashMap<>();
            cacheMap.put("operateTime", String.valueOf(System.currentTimeMillis()));
            if (add) {
                Map<String, String> bucketMap = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
                if (bucketMap.isEmpty()) {
                    return;
                }
                cacheMap.put("operateType", "add");
                dnsRecored.entrySet().stream()
                        .forEach(entry -> {
                            EtcdClient.put(entry.getKey(), entry.getValue());
                        });
                bucketMap.putAll(cacheMap);
                EtcdClient.put(cacheKey, Json.encode(bucketMap));
            } else {
                cacheMap.put("operateType", "delete");
                cacheMap.put("user_id", paramMap.get(USER_ID));
                dnsRecored.entrySet().stream()
                        .forEach(entry -> {
                            EtcdClient.delete(entry.getKey());
                        });
                EtcdClient.put(cacheKey, Json.encode(cacheMap));
            }
        } catch (Exception e) {
            logger.error("insert dnsKey , cacheKey: {} to etcd error.",  cacheKey, e);
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "");
        }
    }

    /**
     * 请求中心区域创桶
     *
     * @param paramMap 创桶的参数
     */
    public static void createBucketToCenter(UnifiedMap<String, String> paramMap, boolean masterCluster) {
        String defaultRegion = pool.getCommand(REDIS_SYSINFO_INDEX).get(MULTI_REGION_DEFAULT_REGION);
        String bucketLocation = getBucketLocation(paramMap);
        insertDnsAndCacheLog(bucketLocation, paramMap, true);
        if (StringUtils.isNotEmpty(defaultRegion) && masterCluster) {
            //主站点执行多区域转发
            if (LOCAL_REGION.equals(bucketLocation) && !LOCAL_REGION.equals(defaultRegion)) {
                createRemoteBucketReq(paramMap, defaultRegion, bucketLocation, false);
            } else if (LOCAL_REGION.equals(defaultRegion)) {
                createRemoteBucketReq(paramMap, defaultRegion, bucketLocation, true);
                // 删除etcd 日志
                deleteSharedLog(paramMap, true);
            }
        }
    }

    /**
     * 判断创建桶时指定的站点属性是否存在于指定的区域当中
     * @param paramMap 创建桶时的请求参数
     */
    public static boolean siteInSpecifiedRegion(UnifiedMap<String, String> paramMap) {
        if (paramMap.containsKey(REGION_FLAG.toLowerCase()) || paramMap.containsKey(SITE_FLAG.toLowerCase())) {
            return true;
        }

        // 获取创建桶时指定的区域
        String region = LOCAL_REGION;
        String defaultRegion = pool.getCommand(REDIS_SYSINFO_INDEX).get(MULTI_REGION_DEFAULT_REGION);
        if (!StringUtils.isEmpty(defaultRegion)) {
            region = defaultRegion;
        }
        if (paramMap.containsKey(BODY)) {
            CreateBucketConfiguration conf = (CreateBucketConfiguration) JaxbUtils.toObject(paramMap.get(BODY));
            if (conf != null && StringUtils.isNotEmpty(conf.getLocationConstraint())) {
                region = conf.getLocationConstraint();
            }
        }

        // 获取创建桶时指定的站点
        String site = LOCAL_SITE;
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!StringUtils.isEmpty(masterCluster)) {
            site = masterCluster;
        }
        if (paramMap.containsKey(SITE_NAME)) {
            site = paramMap.get(SITE_NAME);
        }

        // 判断site是否存在于region当中
        if (LOCAL_REGION.equals(region)) {
            if (LOCAL_SITE.equals(site)) {
                return true;
            } else {
                Set<String> otherClusters = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(OTHER_CLUSTERS);
                for (String otherCluster : otherClusters) {
                    JSONObject cluster = JSON.parseObject(otherCluster);
                    if (site.equals(cluster.getString(CLUSTER_NAME))) {
                        return true;
                    }
                }
            }
        } else {
            Set<String> otherRegions = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(OTHER_REGIONS);
            boolean validRegion = false;
            for (String otherRegion : otherRegions) {
                JSONObject jsonObject = JSON.parseObject(otherRegion);
                String otherRegionName = jsonObject.getString(REGION_NAME);
                if (region.equals(otherRegionName)) {
                    validRegion = true;
                    JSONArray clusterNames = jsonObject.getJSONArray("cluster_names");
                    if (clusterNames.contains(site)) {
                        return true;
                    }
                }
            }
            if (!validRegion) {
                throw new MsException(ErrorNo.INVALID_LOCATION_CONSTRAINT, "The specified location " + region + " is invalid!");
            }
        }

        return false;
    }

    /**
     * 获取所需创建桶的区域
     *
     * @param paramMap 请求参数
     * @return 所属区域
     */
    public static String getBucketLocation(UnifiedMap<String, String> paramMap) {
        String location = LOCAL_REGION;
        String defaultRegion = pool.getCommand(REDIS_SYSINFO_INDEX).get(MULTI_REGION_DEFAULT_REGION);
        if (!StringUtils.isEmpty(defaultRegion)) {
            location = defaultRegion;
        }
        if (paramMap.containsKey(BODY)) {
            CreateBucketConfiguration conf = (CreateBucketConfiguration) JaxbUtils.toObject(paramMap.get(BODY));
            if (conf != null && StringUtils.isNotEmpty(conf.getLocationConstraint())) {
                location = conf.getLocationConstraint();
            }

            boolean valid = true;
            if (!location.equals(LOCAL_REGION)) {
                if (!paramMap.containsKey(REGION_FLAG.toLowerCase()) && !StringUtils.isEmpty(defaultRegion)
                        && !location.equals(defaultRegion)) {
                    //非中心区域
                    throw new MsException(ErrorNo.INVALID_LOCATION_CONSTRAINT, "can't create the bucket with " +
                            "the specified location in this region.");
                }
                valid = false;
                Set<String> otherRegions = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(OTHER_REGIONS);
                for (String otherRegion : otherRegions) {
                    JSONObject json = JSON.parseObject(otherRegion);
                    if (location.equals(json.getString(REGION_NAME))) {
                        valid = true;
                        break;
                    }
                }
            }
            if (!valid) {
                throw new MsException(ErrorNo.INVALID_LOCATION_CONSTRAINT, "the specified location does not exist or is invalid");
            }
        }
        return location;
    }

    /**
     * 多区域下删除桶的处理
     *
     * @param paramMap 参数
     * @return 是否流程继续
     */
    public static boolean dealRegionsDeleteBucket(UnifiedMap<String, String> paramMap) {
        String bucketLocation = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(paramMap.get(BUCKET_NAME), REGION_NAME);
        String defaultRegion = pool.getCommand(REDIS_SYSINFO_INDEX).get(MULTI_REGION_DEFAULT_REGION);
        paramMap.put("bucketLocation", bucketLocation);
        boolean continueFlag = true;
        boolean isRollBackAction = NO_SYNCHRONIZATION_VALUE.equals(paramMap.get("rollBack"));
        if (!StringUtils.isEmpty(defaultRegion) && !isRollBackAction) {
            if (paramMap.containsKey("bucket_region") && !paramMap.get("bucket_region").equals(bucketLocation)) {
                throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + paramMap.get(BUCKET_NAME));
            }
            if (LOCAL_REGION.equals(defaultRegion)) {
                // 先记录到etcd 写日志，删桶成功同步到etcd后删日志
                insertSharedLog(paramMap, defaultRegion, bucketLocation, false);
            }
            if (paramMap.containsKey(REGION_FLAG) || paramMap.containsKey(REGION_FLAG.toLowerCase())) {
                logger.info("contains flag.");
                return true;
            }
            if (!LOCAL_REGION.equals(defaultRegion)) {
                //false：当前区域不是桶所属区域,向location区域请求删桶
                continueFlag = false;
                deleteRemoteBucketReq(paramMap, false);
            }
        }
        return continueFlag;
    }

    /**
     * 请求中心区域删桶
     *
     * @param paramMap 创桶的参数
     */
    public static void deleteBucketToCenter(UnifiedMap<String, String> paramMap,boolean isMasteCluster) {
        String defaultRegion = pool.getCommand(REDIS_SYSINFO_INDEX).get(MULTI_REGION_DEFAULT_REGION);
        String bucketLocation = paramMap.get("bucketLocation");
        insertDnsAndCacheLog(bucketLocation, paramMap, false);
        boolean isRollBackAction = NO_SYNCHRONIZATION_VALUE.equals(paramMap.get("rollBack"));
        if (!isRollBackAction && isMasteCluster && StringUtils.isNotEmpty(defaultRegion) && LOCAL_REGION.equals(defaultRegion)) {
            deleteRemoteBucketReq(paramMap, true);
            // 删除etcd 日志
            deleteSharedLog(paramMap, false);
        }
    }

    /**
     * 请求中心区域删除桶
     *
     * @param paramMap 请求参数
     * @param onlyEtcd 写etcd
     */
    private static void deleteRemoteBucketReq(UnifiedMap<String, String> paramMap, boolean onlyEtcd) {
        SocketReqMsg msg;
        if (onlyEtcd) {
            msg = new SocketReqMsg(MSG_TYPE_REGIONS_BUCKET_TOETCD, 0).put("action", "deleteBucket");
        } else {
            msg = new SocketReqMsg(MSG_TYPE_REGIONS_DELETE_BUCKET, 0);
        }
        UnifiedMap<String, String> reqMap = new UnifiedMap<>();
        reqMap.put("userId", paramMap.get(USER_ID));
        reqMap.put("bucket", paramMap.get(BUCKET_NAME));
        reqMap.put(REGION_FLAG, "1");
        if (onlyEtcd) {
            reqMap.put("bucketRegion", paramMap.get("bucketLocation"));
        }
        addReqParam(paramMap, reqMap, msg);
        msg.put("bucket", paramMap.get(BUCKET_NAME)).put("param", JSON.toJSONString(reqMap));
        BaseResMsg resMsg = sender.sendAndGetResponse(msg, BaseResMsg.class, false);
        int resCode = resMsg.getCode();
        if (resCode != ErrorNo.SUCCESS_STATUS) {
            throw new MsException(resCode, "delete remote bucket failed.");
        }
    }

    /**
     * 每5分钟向其他数据发送访问流量和容量的统计数据
     *
     * @param storage    容量数据
     * @param statistics 访问统计的数据
     */
    public static void sendRegionStatisticsData(String storage, String statistics) {
        UnifiedMap<String, String> reqMap = new UnifiedMap<>();
        reqMap.put(MULTI_REGION_REQ_TYPE, MSG_TYPE_REGIONS_STATISTICS_SYNC);
        reqMap.put("storage", storage);
        reqMap.put("statistics", statistics);
        sendMsg(reqMap);
    }

    /**
     * socket发给ms.jar去调用sdk向其他区域传递数据
     *
     * @param reqMap 数据构成的map
     */
    private static void sendMsg(UnifiedMap<String, String> reqMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_REGIONS_STATISTICS_SYNC, 0);
        msg.put("param", JSON.toJSONString(reqMap));
        BaseResMsg resMsg = sender.sendAndGetResponse(msg, BaseResMsg.class, false);
        int resCode = resMsg.getCode();
        if (resCode != ErrorNo.SUCCESS_STATUS) {
            throw new MsException(resCode, "send region statistics data failed.");
        }
    }

    /**
     * 每天向其他数据发送本月访问流量统计数据, 用来校准
     *
     * @param monthRecords 每月的统计数据
     */
    public static void sendMonthRegionStatisticsData(String monthRecords) {
        UnifiedMap<String, String> reqMap = new UnifiedMap<>();
        reqMap.put(MULTI_REGION_REQ_TYPE, MSG_TYPE_REGIONS_STATISTICS_MONTH);
        reqMap.put("monthRecords", monthRecords);
        sendMsg(reqMap);
    }

    public static void addReqParam(UnifiedMap<String, String> paramMap, UnifiedMap<String, String> reqMap, SocketReqMsg msg) {
        String userId = paramMap.get(USER_ID);
        String ak = pool.getCommand(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_ID_AK1);
        if (StringUtils.isEmpty(ak) || "null".equals(ak)) {
            ak = pool.getCommand(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_ID_AK2);
        }
        String sk = pool.getCommand(REDIS_USERINFO_INDEX).hget(ak, USER_DATABASE_AK_SK);
        reqMap.put("ak", ak);
        reqMap.put("sk", sk);
        msg.put("ak", ak);
        msg.put("sk", sk);
    }
}
