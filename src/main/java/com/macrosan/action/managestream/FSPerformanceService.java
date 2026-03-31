package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.Redis6380ConnPool;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.BucketAddressFSPerfQuota;
import com.macrosan.message.xmlmsg.BucketFSPerfQuota;
import com.macrosan.message.xmlmsg.InstancePerformanceQuota;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.perf.AddressFSPerfLimiter;
import com.macrosan.utils.regex.PatternConst;
import com.macrosan.utils.serialize.JaxbUtils;
import io.vertx.core.impl.ConcurrentHashSet;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.constants.ErrorNo.SUCCESS_STATUS;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DoubleActiveUtil.notifySlaveSite;

@Log4j2
public class FSPerformanceService extends BaseService {
    private static FSPerformanceService instance = null;

    private FSPerformanceService() {
        super();
    }

    public static FSPerformanceService getInstance() {
        if (instance == null) {
            instance = new FSPerformanceService();
        }
        return instance;
    }

    private static Redis6380ConnPool iamPool = Redis6380ConnPool.getInstance();

    public static final String Instance = "instance";

    public static final String Address = "address";

    public enum Instance_Type {
        fs_write(1, 1),
        fs_read(1, 1),
        fs_create(1, 0),
        fs_mkdir(1, 0),
        fs_remove(1, 0),
        fs_rmdir(1, 0),
        fs_lookup(1, 0),
        fs_readdir(1, 0);

        public int tps;
        public int bandWidth;

        Instance_Type(int tps, int bandWidth) {
            this.tps = tps;
            this.bandWidth = bandWidth;
        }

        public static Instance_Type match(String s) {
            for (Instance_Type value : values()) {
                if (value.name().equals(s.toLowerCase())) {
                    return value;
                }
            }
            return null;
        }

        public boolean checkQoSType(String type) {
            switch (type) {
                case THROUGHPUT_QUOTA:
                    return tps == 1;
                case BAND_WIDTH_QUOTA:
                    return bandWidth == 1;
                default:
                    return false;
            }
        }
    }

    public void checkBucketIsNFS(String bucket) {
        if (!"1".equals(RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, "nfs"))
                && !"1".equals(RedisConnPool.getInstance().getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, "cifs"))) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "This bucket is not enable NFS or CIFS. bucket_name: " + bucket);
        }
    }

    //http://172.20.82.104/fjx1/?fsperformancequota
    public ResponseMsg getBucketFSPerformanceQuota(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);

        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        if (!userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,
                    "no permission.user " + userId + " can not get bucket performance quota.");
        }

        checkBucketIsNFS(bucketName);

        BucketFSPerfQuota bucketFSPerfQuota = new BucketFSPerfQuota();
        List<InstancePerformanceQuota> list = new ArrayList<>();
        for (Instance_Type value : Instance_Type.values()) {
            String instance = value.name();
            InstancePerformanceQuota instanceQuota = new InstancePerformanceQuota().setName(instance);
            String tp0 = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, instance + "-" + THROUGHPUT_QUOTA)).orElse("0");
            String bw0 = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, instance + "-" + BAND_WIDTH_QUOTA)).orElse("0");
            instanceQuota.setThroughput(tp0).setBandWidth(bw0);
            list.add(instanceQuota);
        }
        bucketFSPerfQuota.setInstancePerformanceQuotaList(list);
        return new ResponseMsg()
                .setData(bucketFSPerfQuota)
                .addHeader(CONTENT_TYPE, "application/xml");
    }

    //http://172.20.82.104/fjx1/?fsperformancequota&type=throughput_quota&instance=nfs_create&quota=10000
    public ResponseMsg putBucketFSPerformanceQuota(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String type = paramMap.get(PERFORMANCE_QUOTA_TYPE);
        String instance0 = paramMap.get(Instance).toLowerCase();
        String quota = paramMap.get(PERFORMANCE_QUOTA_VALUE);

        Instance_Type instance = Instance_Type.match(instance0);
        if (instance == null) {
            throw new MsException(ErrorNo.NO_SUCH_PERF_TYPE, "This instance is not supported QoS: " + instance0);
        }

        if (!instance.checkQoSType(type)) {
            throw new MsException(ErrorNo.NO_SUCH_PERF_TYPE, instance0 + " is not support this performance type: " + type);
        }

        if (!PatternConst.PERF_QUOTA_PATTERN.matcher(quota).matches()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, type + "quota must be a positive long number or 0 : " + quota);
        }

        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        if (!userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,
                    "no permission.user " + userId + " can not get bucket performance quota.");
        }

        checkBucketIsNFS(bucketName);

        String redisKey = instance0 + "-" + type;
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, redisKey, quota);
        clearTokenBucketData(bucketName, redisKey);

        return new ResponseMsg();
    }

    public void checkMountRecord(String bucket, String address) {
        if (!checkIp(bucket, address)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, bucket + " has no such mount address: " + address);
        }
    }

    public ResponseMsg getBucketAddressPerformanceQuota(UnifiedMap<String, String> paramMap) {
        String address = paramMap.get(Address);
        if ("all".equals(address)) {
            return getBucketAddress(paramMap);
        }

        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);

        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        if (!userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,
                    "no permission.user " + userId + " can not get bucket performance quota.");
        }

        checkBucketIsNFS(bucketName);
        checkMountRecord(bucketName, address);

        BucketFSPerfQuota bucketFSPerfQuota = new BucketFSPerfQuota();
        List<InstancePerformanceQuota> list = new ArrayList<>();
        String key0 = getAddressPerfRedisKey(address, bucketName);
        for (Instance_Type value : Instance_Type.values()) {
            String instance = value.name();
            InstancePerformanceQuota instanceQuota = new InstancePerformanceQuota().setName(instance);
            String tp0 = Optional.ofNullable(pool.getCommand(REDIS_SYSINFO_INDEX).hget(key0, instance + "-" + THROUGHPUT_QUOTA)).orElse("0");
            String bw0 = Optional.ofNullable(pool.getCommand(REDIS_SYSINFO_INDEX).hget(key0, instance + "-" + BAND_WIDTH_QUOTA)).orElse("0");
            instanceQuota.setThroughput(tp0).setBandWidth(bw0);
            list.add(instanceQuota);
        }
        bucketFSPerfQuota.setInstancePerformanceQuotaList(list);
        return new ResponseMsg()
                .setData(bucketFSPerfQuota)
                .addHeader(CONTENT_TYPE, "application/xml");
    }

    public ResponseMsg putBucketAddressPerformanceQuota(UnifiedMap<String, String> paramMap) {
        log.info("putBucketAddressPerformanceQuota {}", paramMap);
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String type = paramMap.get(PERFORMANCE_QUOTA_TYPE);
        String instance0 = paramMap.get(Instance).toLowerCase();
        String quota = paramMap.get(PERFORMANCE_QUOTA_VALUE);
        String address = paramMap.get(Address);

        Instance_Type instance = Instance_Type.match(instance0);
        if (instance == null) {
            throw new MsException(ErrorNo.NO_SUCH_PERF_TYPE, "This instance is not supported QoS: " + instance0);
        }

        if (!instance.checkQoSType(type)) {
            throw new MsException(ErrorNo.NO_SUCH_PERF_TYPE, instance0 + " is not support this performance type: " + type);
        }

        if (!PatternConst.PERF_QUOTA_PATTERN.matcher(quota).matches()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, type + "quota must be a positive long number or 0 : " + quota);
        }

        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        if (!userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,
                    "no permission.user " + userId + " can not get bucket performance quota.");
        }
        DoubleActiveUtil.siteConstraintCheck(bucketInfo, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(new UnifiedMap<>(paramMap), MSG_TYPE_PUT_ADDRESS_PERFORMANCE_QUOTA, localCluster, masterCluster)) {
            return new ResponseMsg();
        }

        checkBucketIsNFS(bucketName);
        checkMountRecord(bucketName, address);

        String key0 = getAddressPerfRedisKey(address, bucketName);
        String redisKey = instance0 + "-" + type;
        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(key0, redisKey, quota);
        clearTokenBucketData(key0, redisKey);

        int resCode = notifySlaveSite(paramMap, ACTION_PUT_ADDRESS_PERFORMANCE_QUOTA);
        if (resCode != SUCCESS_STATUS) {
            throw new MsException(resCode, "master delete bucket error");
        }
        return new ResponseMsg();
    }

    public ResponseMsg putBucketMultipleFSPerformanceQuota(UnifiedMap<String, String> paramMap) {
        log.info("putBucketMultipleFSPerformanceQuota {}", paramMap);
        String bodyStr = paramMap.get(BODY);
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String address = paramMap.get(Address);

        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        if (!userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,
                    "no permission.user " + userId + " can not get bucket performance quota.");
        }

        checkBucketIsNFS(bucketName);

        if (StringUtils.isBlank(bodyStr)) {
            throw new MsException(ErrorNo.INPUT_EMPTY_OBJECT, "no body content in multiFSPerf request");
        }

        DoubleActiveUtil.siteConstraintCheck(bucketInfo, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(new UnifiedMap<>(paramMap), MSG_TYPE_PUT_MULTi_FS_PERFORMANCE_QUOTA, localCluster, masterCluster)) {
            return new ResponseMsg();
        }

        BucketFSPerfQuota bucketFSPerfQuota = (BucketFSPerfQuota) JaxbUtils.toObject(bodyStr);
        for (InstancePerformanceQuota instancePerformanceQuota : bucketFSPerfQuota.getInstancePerformanceQuotaList()) {
            String instance0 = instancePerformanceQuota.getName();
            Instance_Type instance = Instance_Type.match(instance0);
            if (instance == null) {
                throw new MsException(ErrorNo.NO_SUCH_PERF_TYPE, "This instance is not supported QoS: " + instance0);
            }
            if (StringUtils.isNotBlank(address)) {
                checkMountRecord(bucketName, address);
            }
            dealSingleInstanceQuota(bucketName, instancePerformanceQuota, instance, THROUGHPUT_QUOTA, address);
            dealSingleInstanceQuota(bucketName, instancePerformanceQuota, instance, BAND_WIDTH_QUOTA, address);
        }

        int resCode = notifySlaveSite(paramMap, ACTION_PUT_MULTI_FS_PERFORMANCE_QUOTA);
        if (resCode != SUCCESS_STATUS) {
            throw new MsException(resCode, "master delete bucket error");
        }

        return new ResponseMsg();
    }

    public void dealSingleInstanceQuota(String bucketName, InstancePerformanceQuota instancePerformanceQuota, Instance_Type instance, String quotaType, String address) {
        String instance0 = instancePerformanceQuota.getName();
        String quota = instancePerformanceQuota.getThroughput();
        if (quotaType.equals(BAND_WIDTH_QUOTA)) {
            quota = instancePerformanceQuota.getBandWidth();
        }
        if (!PatternConst.PERF_QUOTA_PATTERN.matcher(quota).matches()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, instance0 + " " + quotaType + " quota must be a positive long number or 0 : " + quota);
        }
        long quotaL = Long.parseLong(quota);
        if (quotaL > 0 && !instance.checkQoSType(quotaType)) {
            throw new MsException(ErrorNo.NO_SUCH_PERF_TYPE, instance0 + " is not support this performance type: " + quotaType);
        }
        if (StringUtils.isBlank(address)) {
            String redisKey = instance0 + "-" + quotaType;
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, redisKey, quota);
            clearTokenBucketData(bucketName, redisKey);
        } else {
            String key0 = getAddressPerfRedisKey(address, bucketName);
            String redisKey = instance0 + "-" + quotaType;
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(key0, redisKey, quota);
            clearTokenBucketData(key0, redisKey);
        }
    }

    public static String getAddressPerfRedisKey(String address, String bucket) {
        return "fsAddressPerf-" + address;
    }

    public static final Map<String, Set<String>> FS_IP_SET = new ConcurrentHashMap<>();

    public static void addIp(String bucket, String ip0) {
        try {
            String ip;
            if (ip0.contains("%")) {
                ip = ip0.split("%")[0];
            } else {
                ip = ip0;
            }
            if (StringUtils.isBlank(ip) || StringUtils.isBlank(bucket)) {
                return;
            }
            if (FS_IP_SET.computeIfAbsent(bucket, v -> new ConcurrentHashSet<>()).add(ip)) {
                AddressFSPerfLimiter.addTaskSet.add(AddressFSPerfLimiter.getAddTaskKey(bucket, ip));
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static boolean checkIp(String bucket, String ip) {
        return FS_IP_SET.computeIfAbsent(bucket, v -> new ConcurrentHashSet<>()).contains(ip);
    }

    // 根据桶名查找所有挂载了该桶的ip。
    public ResponseMsg getBucketAddress(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);

        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        if (!userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,
                    "no permission.user " + userId + " can not get bucket performance quota.");
        }

        checkBucketIsNFS(bucketName);
        List<String> ipList = new ArrayList<>();
        Set<String> set = FS_IP_SET.computeIfAbsent(bucketName, v -> new ConcurrentHashSet<>());
        ipList.addAll(set);
        Collections.sort(ipList);
        if (ipList == null || ipList.isEmpty()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, bucketName + " has no mount address");
        }

        int currentPage = StringUtils.isNotBlank(paramMap.get("currentPage")) ? Integer.parseInt(paramMap.get("currentPage")) : 1;
        int pageSize = StringUtils.isNotBlank(paramMap.get("pageSize")) ? Integer.parseInt(paramMap.get("pageSize")) : ipList.size();
        BucketAddressFSPerfQuota bucketAddressFSPerfQuota = new BucketAddressFSPerfQuota();
        bucketAddressFSPerfQuota.setBucketFSPerfQuotaList(new ArrayList<>());
        bucketAddressFSPerfQuota.setTotalSize(String.valueOf(ipList.size()));

        int cur = (currentPage - 1) * pageSize;
        if (cur < ipList.size()) {
            int bound = Math.min(cur + pageSize, ipList.size());
            for (int i = cur; i < bound; i++) {
                String ip = ipList.get(i);
                BucketFSPerfQuota bucketFSPerfQuota = new BucketFSPerfQuota();
                List<InstancePerformanceQuota> list = new ArrayList<>();
                String key0 = getAddressPerfRedisKey(ip, bucketName);
                for (Instance_Type value : Instance_Type.values()) {
                    String instance = value.name();
                    InstancePerformanceQuota instanceQuota = new InstancePerformanceQuota().setName(instance);
                    String tp0 = Optional.ofNullable(pool.getCommand(REDIS_SYSINFO_INDEX).hget(key0, instance + "-" + THROUGHPUT_QUOTA)).orElse("0");
                    String bw0 = Optional.ofNullable(pool.getCommand(REDIS_SYSINFO_INDEX).hget(key0, instance + "-" + BAND_WIDTH_QUOTA)).orElse("0");
                    instanceQuota.setThroughput(tp0).setBandWidth(bw0);
                    list.add(instanceQuota);
                }
                bucketFSPerfQuota.setInstancePerformanceQuotaList(list);
                bucketFSPerfQuota.setAddress(ip);
                bucketAddressFSPerfQuota.getBucketFSPerfQuotaList().add(bucketFSPerfQuota);
            }
        }
        return new ResponseMsg()
                .setData(bucketAddressFSPerfQuota)
                .addHeader(CONTENT_TYPE, "application/xml");
    }

    /**
     * 清空令牌桶中令牌
     *
     * @param keyPrefix 所情况令牌桶数据key前缀
     * @param tokenType 令牌类型
     */
    private void clearTokenBucketData(String keyPrefix, String tokenType) {
        try {
            Map<String, String> map = new HashMap<>(2);
            map.put(tokenType, "0");
            map.put("last_modified_" + tokenType, "0");

            iamPool.getShortMasterCommand(REDIS_TOKEN_INDEX).hmset(keyPrefix + "_token", map);
        } catch (Exception e) {
            log.error("get asyncPool connection is interrupted,clear tokenMessage in 6380 fail", e);
        }
    }
}
