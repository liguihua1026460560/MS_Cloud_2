package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.Redis6380ConnPool;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.ec.ErasureClient;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.*;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.PolicyCheckUtils;
import com.macrosan.utils.ratelimiter.JobScheduler;
import com.macrosan.utils.ratelimiter.LimitTimeSet;
import com.macrosan.utils.ratelimiter.RecoverLimiter;
import com.macrosan.utils.regex.PatternConst;
import com.macrosan.utils.serialize.JaxbUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.quartz.SchedulerException;

import java.util.*;

import static com.macrosan.constants.AccountConstants.DEFAULT_MGT_USER_ID;
import static com.macrosan.constants.ErrorNo.SUCCESS_STATUS;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DoubleActiveUtil.notifySlaveSite;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.isSwitchOn;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.NOTIFY_DELETE_QOS;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.NOTIFY_RECOVER_QOS;
import static com.macrosan.httpserver.MossHttpClient.INDEX_NAME_MAP;
import static com.macrosan.httpserver.MossHttpClient.LOCAL_CLUSTER_INDEX;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.buildUpdateAccountPerfQuota;
import static com.macrosan.utils.ratelimiter.LimitTimeSet.DEFAULT_STRATEGY;
import static com.macrosan.utils.regex.PatternConst.ACCOUNT_NAME_PATTERN;

/**
 * PerformanceService
 * 账户相关接口服务
 *
 * @author wuhaizhong
 * @date 2019/7/24
 */
public class PerformanceService extends BaseService {

    /**
     * logger日志引用
     */
    private static Logger logger = LogManager.getLogger(PerformanceService.class.getName());

    private static PerformanceService instance = null;

    private static Redis6380ConnPool iamPool = Redis6380ConnPool.getInstance();

    private PerformanceService() {
        super();
    }

    /**
     * 每一个Service都必须提供一个getInstance方法
     */
    public static PerformanceService getInstance() {
        if (instance == null) {
            instance = new PerformanceService();
        }
        return instance;
    }

    /**
     * @param paramMap 请求参数
     * @return 账户配额信息
     */
    public ResponseMsg getPerformanceQuota(Map<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(userId);

        //根据参数中账户名查询性能配额信息
        String accountName = paramMap.get(PERFORMANCE_QUOTA_ACCOUNT_NAME);
        checkAccountName(accountName);
        String accountId = pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "id");

        //获取throughput
        String throughPut = Optional.ofNullable(pool.getCommand(REDIS_USERINFO_INDEX).hget(accountId, THROUGHPUT_QUOTA))
                .orElse("0");
        //获取bandwidth
        String bandwidth = Optional.ofNullable(pool.getCommand(REDIS_USERINFO_INDEX).hget(accountId, BAND_WIDTH_QUOTA))
                .orElse("0");

        PerformanceQuota accountPerformanceQuota = new PerformanceQuota()
                .setThroughPut(throughPut)
                .setBandWidth(bandwidth);

        logger.debug("{}get accountPerformanceQuota successful,accountName:{},throughPut:{},bandwidth:{}", userId
                , accountName, throughPut, bandwidth);
        return new ResponseMsg()
                .setData(accountPerformanceQuota)
                .addHeader(CONTENT_TYPE, "application/xml");
    }

    /**
     * 设置账户性能配额
     *
     * @param paramMap 请求参数
     * @return 响应信息
     */
    public ResponseMsg putPerformanceQuota(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(userId);

        String accountName = paramMap.get(PERFORMANCE_QUOTA_ACCOUNT_NAME);
        checkAccountName(accountName);
        String accountId = pool.getCommand(REDIS_USERINFO_INDEX).hget(accountName, "id");
        String type = paramMap.getOrDefault(PERFORMANCE_QUOTA_TYPE,"");
        String quota = paramMap.getOrDefault(PERFORMANCE_QUOTA_VALUE,"");

        if (!type.equals(THROUGHPUT_QUOTA) && !type.equals(BAND_WIDTH_QUOTA)) {
            throw new MsException(ErrorNo.NO_SUCH_PERF_TYPE, "No such bucket performance type: " + type);
        }

        if (!PatternConst.PERF_QUOTA_PATTERN.matcher(quota).matches()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, type + "quota's value must be a positive long number or 0 : " + quota);
        }

        switch (type) {
            case THROUGHPUT_QUOTA:
                clearTokenBucketData(accountId, THROUGHPUT_QUOTA);
                paramMap.put("perfQuotaType", THROUGHPUT_QUOTA);
                paramMap.put("perfQuotaValue", quota);
                break;
            case BAND_WIDTH_QUOTA:
                clearTokenBucketData(accountId, BAND_WIDTH_QUOTA);
                paramMap.put("perfQuotaType", BAND_WIDTH_QUOTA);
                paramMap.put("perfQuotaValue", quota);
                break;
            default:
                throw new MsException(ErrorNo.NO_SUCH_PERF_TYPE, "No such account performance type: " + type);
        }
        paramMap.put("AccountName", accountName);
        if(ServerConfig.isVm()){
            //虚拟机版本不发消息到iam,直接修改redis6379表3性能配额记录
            pool.getShortMasterCommand(REDIS_USERINFO_INDEX).hset(accountId, type, quota);
        }else{
            SocketReqMsg msg = buildUpdateAccountPerfQuota(paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.NO_SUCH_ACCOUNT) {
                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "No such account.");
            } else if (code == ErrorNo.MOSS_FAIL || code == -1) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS update account performance quota failed.");
            }
        }

        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    /**
     * 获取桶性能配额
     *
     * @param paramMap 请求参数
     * @return 响应消息
     */
    public ResponseMsg getBucketPerfQuota(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

        if (!userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,
                    "no permission.user " + userId + " can not get bucket performance quota.");
        }

        String method = "GetBucketPerfQuota";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        //获取throughput
        String throughPut = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, THROUGHPUT_QUOTA))
                .orElse("0");
        //获取bandwidth
        String bandwidth = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, BAND_WIDTH_QUOTA))
                .orElse("0");
        // 获取datasync_throughput
        String datasyncThroughput = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, DATASYNC_THROUGHPUT_QUOTA))
                .orElse("0");
        // 获取datasync_bandwidth
        String datasyncBandwidth = Optional.ofNullable(pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, DATASYNC_BAND_WIDTH_QUOTA))
                .orElse("0");

        BucketPerformanceQuota bucketPerformanceQuota = new BucketPerformanceQuota()
                .setThroughPut(throughPut)
                .setBandWidth(bandwidth);

        List<DataSyncPerformanceQuota> quotaList = new ArrayList<>();
        INDEX_NAME_MAP.entrySet().stream()
                .filter(entry -> !LOCAL_CLUSTER_INDEX.equals(entry.getKey()))
                .forEach(entry -> {
                    Integer key = entry.getKey();
                    String value = entry.getValue();
                    DataSyncPerformanceQuota quota = new DataSyncPerformanceQuota();
                    String clusterThroughput = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, DATASYNC_THROUGHPUT_QUOTA + "_" + key);
                    if (StringUtils.isNotEmpty(clusterThroughput)) {
                        quota.setDatasyncThroughput(clusterThroughput).setClusterName(value);
                    } else {
                        quota.setDatasyncThroughput(datasyncThroughput).setClusterName(value);
                    }
                    String clusterBandWidth = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, DATASYNC_BAND_WIDTH_QUOTA + "_" + key);
                    if (StringUtils.isNotEmpty(clusterBandWidth)) {
                        quota.setDatasyncBandWidth(clusterBandWidth).setClusterName(value);
                    } else {
                        quota.setDatasyncBandWidth(datasyncBandwidth).setClusterName(value);
                    }
                    quotaList.add(quota);
                });
        if (quotaList.size() > 0) {
            bucketPerformanceQuota.setDataSyncPerformanceQuotaList(quotaList);
        } else {
            bucketPerformanceQuota.setDatasyncThroughput(datasyncThroughput)
                    .setDatasyncBandWidth(datasyncBandwidth);
        }

        logger.debug("{}get bucketPerformanceQuota successful,bucketName:{},throughPut:{},bandwidth:{},synct:{},syncb:{}", userId
                , bucketName, throughPut, bandwidth, datasyncThroughput, datasyncBandwidth);

        return new ResponseMsg()
                .setData(bucketPerformanceQuota)
                .addHeader(CONTENT_TYPE, "application/xml");
    }

    /**
     * 设置桶性能配额
     *
     * @param paramMap 请求参数
     * @return 响应信息
     */
    public ResponseMsg putBucketPerfQuota(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String type = paramMap.get(PERFORMANCE_QUOTA_TYPE);
        String quota = paramMap.get(PERFORMANCE_QUOTA_VALUE);
        String clusterName = paramMap.get(CLUSTER_NAME);

        if (type.equals(DATASYNC_THROUGHPUT_QUOTA) || type.equals(DATASYNC_BAND_WIDTH_QUOTA)) {
//            String syncPolicy = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SYNC_POLICY);
//            if (!"1".equals(syncPolicy)) {
//                throw new MsException(ErrorNo.INSUFFICIENT_SITE_CONDITIONS, "The current site does not meet the setting conditions.");
//            }
            if (!isSwitchOn(pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName))) {
                throw new MsException(ErrorNo.INSUFFICIENT_BUCKET_CONDITIONS, "The bucket does not open the data sync switch.");
            }
        }

        String method = "PutBucketPerfQuota";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        paramMap.put("type", type);
        paramMap.put("quota", quota);
        logger.info("type:" + type + "quota:" + quota);

        Map<String, String> quotaMap = new HashMap<>();
        if (type.equals(DATASYNC_THROUGHPUT_QUOTA) || type.equals(DATASYNC_BAND_WIDTH_QUOTA)) {
            if (StringUtils.isNotEmpty(clusterName)) {
                if (!INDEX_NAME_MAP.values().contains(clusterName) || clusterName.equals(INDEX_NAME_MAP.get(LOCAL_CLUSTER_INDEX))) {
                    throw new MsException(ErrorNo.NO_SUCH_CLUSTER_NAME, "No such cluster name: " + clusterName);
                } else {
                    INDEX_NAME_MAP.entrySet().stream()
                            .filter(entry -> !LOCAL_CLUSTER_INDEX.equals(entry.getKey()))
                            .forEach(entry -> {
                                Integer key = entry.getKey();
                                String value = entry.getValue();
                                if (clusterName.equals(value)) {
                                    quotaMap.put(type + "_" + key, quota);
                                } else {
                                    String curQuota = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, type);
                                    if (StringUtils.isNotEmpty(curQuota)) {
                                        quotaMap.put(type + "_" + key, curQuota);
                                    }
                                }
                            });
                }
            }
//            else {
//                INDEX_NAME_MAP.forEach((key, value) -> quotaMap.put(type + "_" + key, quota));
//            }
        }

        if (!DATASYNC_THROUGHPUT_QUOTA.equals(type) && !DATASYNC_BAND_WIDTH_QUOTA.equals(type)) {
            if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_PUT_BUCKET_PERF_QUOTA, localCluster, masterCluster)) {
                return new ResponseMsg().setHttpCode(SUCCESS);
            }
        }

        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

        if (!userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,
                    "no permission.user " + userId + " can not put bucket performance quota.");
        }

        /**
         *  从站点请求主站点执行
         */

        if (!type.equals(THROUGHPUT_QUOTA) && !type.equals(BAND_WIDTH_QUOTA) && !type.equals(DATASYNC_THROUGHPUT_QUOTA) && !type.equals(DATASYNC_BAND_WIDTH_QUOTA)) {
            throw new MsException(ErrorNo.NO_SUCH_PERF_TYPE, "No such bucket performance type: " + type);
        }

        if (!PatternConst.PERF_QUOTA_PATTERN.matcher(quota).matches()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, type + "quota  must be a positive long number or 0 : " + quota);
        }

        switch (type) {
            case THROUGHPUT_QUOTA:
                pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, THROUGHPUT_QUOTA, quota);
                clearTokenBucketData(bucketName, THROUGHPUT_QUOTA);
                break;
            case BAND_WIDTH_QUOTA:
                pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, BAND_WIDTH_QUOTA, quota);
                clearTokenBucketData(bucketName, BAND_WIDTH_QUOTA);
                break;
            case DATASYNC_THROUGHPUT_QUOTA:
//                pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, DATASYNC_THROUGHPUT_QUOTA, quota);
            case DATASYNC_BAND_WIDTH_QUOTA:
//                pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, DATASYNC_BAND_WIDTH_QUOTA, quota);
                if (quotaMap.isEmpty()) {
                    pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, type, quota);
                    INDEX_NAME_MAP.keySet().forEach(i -> pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, type + "_" + i));
                } else {
                    pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hmset(bucketName, quotaMap);
                    pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, type);
                }
                break;
            default:
                throw new MsException(ErrorNo.NO_SUCH_PERF_TYPE, "No such bucket performance type: " + type);
        }

        if (!DATASYNC_THROUGHPUT_QUOTA.equals(type) && !DATASYNC_BAND_WIDTH_QUOTA.equals(type)) {
            int resp = notifySlaveSite(paramMap, ACTION_PUT_BUCKET_PERF_QUOTA);
            if (resp != SUCCESS_STATUS) {
                throw new MsException(resp, "slave put bucket perf quota error!");
            }
        }

        return new ResponseMsg();
    }

    /**
     * @param paramMap 请求参数
     * @return 全局复制性能配额信息
     */
    public ResponseMsg getSysPerformanceQuota(Map<String, String> paramMap) {
//        String userId = paramMap.get(USER_ID);
//        MsAclUtils.checkIfManageAccount(userId);

        //获取throughput
        String throughPut = Optional.ofNullable(pool.getCommand(REDIS_SYSINFO_INDEX).hget(DATA_SYNC_QUOTA, THROUGHPUT_QUOTA))
                .orElse("0");
        //获取bandwidth
        String bandwidth = Optional.ofNullable(pool.getCommand(REDIS_SYSINFO_INDEX).hget(DATA_SYNC_QUOTA, BAND_WIDTH_QUOTA))
                .orElse("0");

        List<PerformanceQuota> quotaList = new ArrayList<>();
        INDEX_NAME_MAP.entrySet().stream()
                .filter(entry -> !LOCAL_CLUSTER_INDEX.equals(entry.getKey()))
                .forEach(entry -> {
                    Integer key = entry.getKey();
                    String value = entry.getValue();
                    PerformanceQuota quota = new PerformanceQuota();
                    String clusterThroughput = pool.getCommand(REDIS_SYSINFO_INDEX).hget(DATA_SYNC_QUOTA, THROUGHPUT_QUOTA + "_" + key);
                    if (StringUtils.isNotEmpty(clusterThroughput)) {
                        quota.setThroughPut(clusterThroughput).setClusterName(value);
                    } else {
                        quota.setThroughPut(throughPut).setClusterName(value);
                    }
                    String clusterBandWidth = pool.getCommand(REDIS_SYSINFO_INDEX).hget(DATA_SYNC_QUOTA, BAND_WIDTH_QUOTA + "_" + key);
                    if (StringUtils.isNotEmpty(clusterBandWidth)) {
                        quota.setBandWidth(clusterBandWidth).setClusterName(value);
                    } else {
                        quota.setBandWidth(bandwidth).setClusterName(value);
                    }
                    quotaList.add(quota);
                });
        SysPerformanceQuota sysPerformanceQuota = new SysPerformanceQuota();
        sysPerformanceQuota.setSysPerformanceQuotaList(quotaList);

        return new ResponseMsg()
                .setData(sysPerformanceQuota)
                .addHeader(CONTENT_TYPE, "application/xml");
    }

    /**
     * 设置账户性能配额
     *
     * @param paramMap 请求参数
     * @return 全局复制性能配额信息
     */
    public ResponseMsg putSysPerformanceQuota(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String type = paramMap.get(PERFORMANCE_QUOTA_TYPE);
        String quota = paramMap.get(PERFORMANCE_QUOTA_VALUE);
        String clusterName = paramMap.get(CLUSTER_NAME);

        if (!THROUGHPUT_QUOTA.equals(type) && !BAND_WIDTH_QUOTA.equals(type)) {
            throw new MsException(ErrorNo.NO_SUCH_PERF_TYPE, "No such bucket performance type: " + type);
        }

        if (pool.getCommand(REDIS_SYSINFO_INDEX).exists(MASTER_CLUSTER) == 0) {
            throw new MsException(ErrorNo.INSUFFICIENT_SITE_CONDITIONS, "The current site does not meet the setting conditions.");
        }

        logger.info("type:" + type + "quota:" + quota);

        if (!PatternConst.PERF_QUOTA_PATTERN.matcher(quota).matches()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, type + "quota  must be a positive long number or 0 : " + quota);
        }

        Map<String, String> quotaMap = new HashMap<>();
        if (StringUtils.isNotEmpty(clusterName)) {
            if (!INDEX_NAME_MAP.values().contains(clusterName) || clusterName.equals(INDEX_NAME_MAP.get(LOCAL_CLUSTER_INDEX))) {
                throw new MsException(ErrorNo.NO_SUCH_CLUSTER_NAME, "No such cluster name: " + clusterName);
            } else {
                INDEX_NAME_MAP.entrySet().stream()
                        .filter(entry -> !LOCAL_CLUSTER_INDEX.equals(entry.getKey()))
                        .forEach(entry -> {
                            Integer key = entry.getKey();
                            String value = entry.getValue();
                            if (clusterName.equals(value)) {
                                quotaMap.put(type + "_" + key, quota);
                            } else {
                                String curQuota = pool.getCommand(REDIS_SYSINFO_INDEX).hget(DATA_SYNC_QUOTA, type);
                                if (StringUtils.isNotEmpty(curQuota)) {
                                    quotaMap.put(type + "_" + key, curQuota);
                                }
                            }
                        });
            }
        }

        switch (type) {
            case THROUGHPUT_QUOTA:
            case BAND_WIDTH_QUOTA:
                if (quotaMap.isEmpty()) {
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(DATA_SYNC_QUOTA, type, quota);
                    INDEX_NAME_MAP.keySet().forEach(i -> pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel(DATA_SYNC_QUOTA, type + "_" + i));
                } else {
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hmset(DATA_SYNC_QUOTA, quotaMap);
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel(DATA_SYNC_QUOTA, type);
                }
                break;
            default:
                throw new MsException(ErrorNo.NO_SUCH_PERF_TYPE, "No such bucket performance type: " + type);
        }

        return new ResponseMsg();
    }

    /**
     * 检验账户是否存在
     *
     * @param accountName 账户名
     */
    private void checkAccountName(String accountName) {
        if (StringUtils.isEmpty(accountName) || pool.getCommand(REDIS_USERINFO_INDEX).exists(accountName) == 0 || !ACCOUNT_NAME_PATTERN.matcher(accountName).matches()) {
            throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "no such account,accountName:" + accountName);
        }
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
            logger.error("get asyncPool connection is interrupted,clear tokenMessage in 6380 fail", e);
        }
    }

    /**
     * 在指定时间段对系统所有存储池附加同一除业务优先外的QoS策略
     *
     * @param paramMap 请求参数
     * @return 响应信息
     * @author wangchenxing
     * */
    public ResponseMsg putQosLimitStrategy(UnifiedMap<String, String> paramMap) {

        logger.info("Received the request in putQosLimitStrategy");
        // 判断是否为管理账户，非管理账户则返回报错信息，账户名称区分大小写
        String userId = paramMap.get(USER_ID);
        if (!DEFAULT_MGT_USER_ID.equals(userId)){
            throw new MsException(ErrorNo.ACCESS_DENY, "Not manage user, no permission.");
        }

        QoSLimitStrategy qoSLimitStrategy = (QoSLimitStrategy) JaxbUtils.toObject(paramMap.get(BODY));
        List<StrategyPart> strategyParts = qoSLimitStrategy.getStrategyParts();

        // 把多个strategyPart中的enabledHour都拼接在一起，首先进行冲突检测与策略名检测
        List<QoSEnabledHour> concatList = new LinkedList<>();
        for (StrategyPart strategyPart : strategyParts) {

            // 获取请求附加的性能限制策略，并判断该策略是否为LIMIT/NO_LIMIT/ADAPT三者之一
            String limitStrategy = strategyPart.getLimitStrategy();
            checkLimitStrategy(limitStrategy);
            List<QoSEnabledHour> enabledHours = strategyPart.getQosEnabledHour();
            concatList.addAll(enabledHours);
        }
        checkEnabledTime(concatList);
        for (StrategyPart strategyPart : strategyParts) {
            String limitStrategy = strategyPart.getLimitStrategy();
            List<QoSEnabledHour> enabledHours = strategyPart.getQosEnabledHour();
            for (int i = 0; i < enabledHours.size(); i++) {
                QoSEnabledHour qoSEnabledHour = enabledHours.get(i);
                String startTime = qoSEnabledHour.getStartTime();
                String endTime = qoSEnabledHour.getEndTime();

                if (Integer.parseInt(startTime)>=0 && Integer.parseInt(startTime) <=9){
                    startTime = "0" + startTime;
                }
                if (Integer.parseInt(endTime)>=0 && Integer.parseInt(endTime) <=9){
                    endTime = "0" + endTime;
                }

                Map<String, String> curRecoverMap = new HashMap<>();
                curRecoverMap.put(QOS_LIMIT_STRATEGY, limitStrategy);
                curRecoverMap.put(QOS_START_TIME, startTime);
                curRecoverMap.put(QOS_END_TIME, endTime);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hmset("recover_"+limitStrategy+"_"+startTime, curRecoverMap);
            }
        }

        // 当前节点设置QoS策略
        LimitTimeSet.init();
        // 通知其它节点设置QoS策略
        ErasureClient.notifyNodes(NOTIFY_RECOVER_QOS);

        logger.info("Manage account id " + userId + " set the recover QoS limit strategy successfully");
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }


    /**
     * 获取存储池对应的QoS性能限制策略
     *
     * @param paramMap 请求参数
     * @return 响应信息
     * @author wangchenxing
     * */
    public ResponseMsg getQosLimitStrategy(UnifiedMap<String, String> paramMap) {

        logger.info("Received the request in getQosLimitStrategy");

        // 判断是否为管理账户，非管理账户则返回报错信息，账户名称区分大小写
        String userId = paramMap.get(USER_ID);
        if (!DEFAULT_MGT_USER_ID.equals(userId)){
            throw new MsException(ErrorNo.ACCESS_DENY, "Not manage user, no permission.");
        }

        // 读取所有的结果，并将之放在map中保存
        Map<String, List<QoSEnabledHour>> qosMap = new HashMap<>();

        // 读取redis表2qos相关records放入xml返回
        List<String> qosRecoverList = pool.getCommand(REDIS_SYSINFO_INDEX).keys(QOS_RECOVER_KEY);
        if (qosRecoverList != null && qosRecoverList.size() != 0){
            for (int i = 0; i < qosRecoverList.size(); i++) {
                Map<String, String> timeMap = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(qosRecoverList.get(i));
                String strategyName = timeMap.get(QOS_LIMIT_STRATEGY);
                String startTime = timeMap.get(QOS_START_TIME);
                String endTime = timeMap.get(QOS_END_TIME);

                if (Integer.parseInt(startTime)>=0 && Integer.parseInt(startTime) <=9){ // 00 -> 0
                    startTime = String.valueOf(startTime.charAt(1));
                }

                if (Integer.parseInt(endTime)>=0 && Integer.parseInt(endTime) <=9){
                    endTime = String.valueOf(endTime.charAt(1));
                }

                QoSEnabledHour curHour = new QoSEnabledHour().setStartTime(startTime).setEndTime(endTime);
                qosMap.computeIfAbsent(strategyName, s -> new LinkedList<>()).add(curHour);
            }
        }

        // 根节点
        QoSLimitStrategy rootElement = new QoSLimitStrategy();
        List<StrategyPart> partList = new LinkedList<>();
        for (String strategyName : qosMap.keySet()) {
            StrategyPart curPart = new StrategyPart().setQosEnabledHour(qosMap.get(strategyName)).setLimitStrategy(strategyName);
            partList.add(curPart);
        }
        rootElement.setStrategyParts(partList);
        logger.info("Manage account id " + userId + " got the recover QoS limit strategy successfully");

        return new ResponseMsg()
                .setData(rootElement)
                .addHeader(CONTENT_TYPE, "application/xml");
    }


    /**
     * 删除所有已配置Redis表2中的QoS策略及生效时间段记录，对于正在生效的策略则立即恢复为业务优先策略
     *
     * @param paramMap 请求参数
     * @return 响应信息
     * @author wangchenxing
     * */
    public ResponseMsg deleteQosLimitStrategy(UnifiedMap<String, String> paramMap){
        logger.info("Received the request in deleteQosLimitStrategy");
        // 判断是否为管理账户，非管理账户则返回报错信息，账户名称区分大小写
        String userId = paramMap.get(USER_ID);
        if (!DEFAULT_MGT_USER_ID.equals(userId)){
            throw new MsException(ErrorNo.ACCESS_DENY, "Not manage user, no permission.");
        }

        // 读取当前存在的策略配置记录，并将之进行删除
        List<String> qosRecoverList = pool.getCommand(REDIS_SYSINFO_INDEX).keys(QOS_RECOVER_KEY);
        if (qosRecoverList != null && qosRecoverList.size() != 0){

            // 清除主redis中的恢复QoS记录，会自动同步至所有节点的从redis
            for (int i = 0; i < qosRecoverList.size(); i++) {
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(qosRecoverList.get(i));  // 写操作必须使用getShortMasterCommand
                logger.info("Delete the record: " + qosRecoverList.get(i) + " successfully");
            }
        }

        try {
            JobScheduler.removeJobs();  // 移除系统存在的所有数据恢复QoS定时配置
            RecoverLimiter.updateStrategy(DEFAULT_STRATEGY);  // 重置策略和rabbitmq并发量
        } catch (SchedulerException e) {
            logger.error("", e);
            throw new MsException(ErrorNo.QOS_SET_ERROR, "Current node reset QoS strategy error");
        }

        // 通知其它节点将当前的恢复QoS重置，定时器删除、策略变更为默认策略、并发量同步变更
        ErasureClient.notifyNodes(NOTIFY_DELETE_QOS);

        logger.info("Manage account id " + userId + " deleted the recover QoS limit strategy successfully");
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }


    /**
     * 检验请求配置QoS性能限制策略是否合规
     *
     * @param limitStrategy 请求性能限制策略名称
     * @author wangchenxing
     */
    private void checkLimitStrategy(String limitStrategy) {
        if (StringUtils.isEmpty(limitStrategy)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "invalid limit strategy: " + limitStrategy);
        }

        if (!(QOS_LIMIT_STRATEGY_DATA.equalsIgnoreCase(limitStrategy) || (QOS_LIMIT_STRATEGY_RECOVER.equalsIgnoreCase(limitStrategy)) || (QOS_LIMIT_STRATEGY_ADAPT.equalsIgnoreCase(limitStrategy)))){
            throw new MsException(ErrorNo.NO_SUCH_LIMITSTRATEGY, "no such limit strategy: " + limitStrategy);
        }
    }

    /**
     * 检验请求的时间段是否合规
     * 1、startTime>endTime 且时间间隔在6小时以上；
     * 2、设置的时间是否为0-24的自然数；
     * 3、startTime与endTime是否为空及enabledHours是否为空；
     * 4、本次请求的策略生效时间是否存在冲突：
     *    a) 本次请求的时间段之间发生冲突；
     *    b) 本次请求的时间段与已设置的时间段发生冲突；
     * 冲突检测方法：初始化一个24元素数组，每确立一个时间段即把对应元素从0置1；在确立新的时间段时，求取两个端点之间连续元素的和，若大于0则存在冲
     * 突；若为0则表示可以在此时间段确立；必须所有时间段不冲突才能正确配置
     *
     * @param enabledHours 时间段对象的列表
     * @author wangchenxing
     * */
    private void checkEnabledTime(List<QoSEnabledHour> enabledHours){

        // 初始化时间数组
        int[] timeArray = new int[24];

        // enabledHours判空
        if (enabledHours == null || enabledHours.size() == 0){
            throw new MsException(ErrorNo.INVALID_QOS_CONFIGURATION, "No strategy enabled time set.");
        }

        // 读取表2中已存有的字段，若能读到则表示已存在配置，将其加入时间段数组
        List<String> qosRecoverList = pool.getCommand(REDIS_SYSINFO_INDEX).keys(QOS_RECOVER_KEY);
        if (qosRecoverList != null && qosRecoverList.size() != 0){

            // 将读取到的值放入数组中，不必对策略进行区分，只需确定时间段即可
            for (int i = 0; i < qosRecoverList.size(); i++) {
                Map<String, String> timeMap = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(qosRecoverList.get(i));
                int curStartTime = Integer.parseInt(timeMap.get(QOS_START_TIME));
                int curEndTime = Integer.parseInt(timeMap.get(QOS_END_TIME));
                timeArray = fillTimeArray(curStartTime, curEndTime, timeArray);
            }
        }

        for (int i = 0; i < enabledHours.size(); i++) {
            QoSEnabledHour qoSEnabledHour = enabledHours.get(i);
            int startTime = Integer.parseInt(qoSEnabledHour.getStartTime());
            int endTime = Integer.parseInt(qoSEnabledHour.getEndTime());

            // 判断时间点是否在0-24之间
            if (startTime < 0 || startTime > 24 || endTime < 0 || endTime > 24){
                throw new MsException(ErrorNo.INVALID_QOS_CONFIGURATION, "Illegal QoS enable time setting.");
            }

            int duration = endTime - startTime;
            // 判断不跨越24点和跨越24点的配置时间段是否大于6小时小于24小时
            if ((duration >= 6 && duration <= 24) || (duration+24 >= 6 && duration+24 <= 24 )){

                // 根据数组判断当前的时间段是否与已有的存在冲突
                if (checkTimeArray(startTime, endTime, timeArray)){  // 如果存在有冲突
                    throw new MsException(ErrorNo.INVALID_QOS_CONFIGURATION, "Conflicting time settings.");
                }

                timeArray = fillTimeArray(startTime, endTime, timeArray);

            } else {
                throw new MsException(ErrorNo.INVALID_QOS_CONFIGURATION, "Illegal time point setting.");
            }

        }
    }

    /**
     * 根据读取到的限流字段把时间段填入时间数组中
     *
     * @param startTime 起始时间点
     * @param endTime 终止时间点
     * @param timeArray 时间数组
     * @return 填入时间段的时间数组
     * @author wangchenxing
     * */
    private int[] fillTimeArray(int startTime, int endTime, int[] timeArray) {

        // 如果endTime是比startTime大的
        if (endTime > startTime){
            for (int i = startTime; i < endTime; i++) {
                timeArray[i] = 1;
            }
            return timeArray;
        }

        // 如果endTime是比startTime小的
        for (int i = startTime; i < endTime + 24; i++) {
            timeArray[i%24] = 1;
        }

        return timeArray;
    }

    /**
     * 根据读取到的限流字段和时间数组检测当前请求的时间段是否存在冲突，如果数组连续元素和大于0即存在冲突
     *
     * @param startTime 起始时间点
     * @param endTime 终止时间点
     * @param timeArray 时间数组
     * @return true 存在有冲突
     *         false 不存在冲突
     * @author wangchenxing
     * */
    private boolean checkTimeArray(int startTime, int endTime, int[] timeArray) {

        int sum = 0;

        if (endTime > startTime){
            for (int i = startTime; i < endTime; i++) {
                sum = sum + timeArray[i];
            }
            if (sum > 0){
                return true;
            }
            return false;
        }

        for (int i = startTime; i < endTime + 24; i++) {
            sum = sum + timeArray[i%24];
        }
        if (sum > 0){
            return true;
        }
        return false;
    }

}
