package com.macrosan.utils.quota;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.BucketInfo;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.message.jsonmsg.BucketInfo.ERROR_BUCKET_INFO;

/**
 * 记录存储用量信息
 *
 * @author gaozhiyuan
 * @date 2019.11.12
 */
@Log4j2
public class QuotaRecorder {

    private static final RedisConnPool redisConnPool = RedisConnPool.getInstance();

    private static final long UPDATE_TIME = 300 * 1000;
    private static final String BUCKET_QUOTA_VALUE = "quota_value";
    private static final String BUCKET_SOFT_QUOTA_VALUE = "soft_quota_value";
    private static final String BUCKET_QUOTA_FLAG = "quota_flag";
    private static final String ACCOUNT_QUOTA_VALUE = "account_quota";
    private static final String ACCOUNT_SOFT_QUOTA_VALUE = "soft_account_quota";
    private static final String ACCOUNT_OBJNUM_VALUE = "account_objnum";
    private static final String ACCOUNT_SOFT_OBJNUM_VALUE = "soft_account_objnum";
    private static final String BUCKET_OBJNUM_VALUE = "objnum_value";
    private static final String BUCKET_SOFT_OBJNUM_VALUE = "soft_objnum_value";
    private static final String BUCKET_OBJNUM_FLAG = "objnum_flag";
    private static final String ACCOUNT_QUOTA_FLAG = "account_quota_flag";
    private static final String ACCOUNT_OBJNUM_FLAG = "account_objnum_flag";

    /**
     * 桶的默认最大对象数配额
     */
    public static long BUCKET_MAX_OBJECTS_QUOTA = Long.MAX_VALUE;

    static {
        try {
            String bucketMaxObjectsQuota = redisConnPool.getCommand(REDIS_SYSINFO_INDEX).get("bucket_max_objects_quota");
            if (!StringUtils.isBlank(bucketMaxObjectsQuota)) {
                BUCKET_MAX_OBJECTS_QUOTA = Long.parseLong(bucketMaxObjectsQuota);
            }
        } catch (Exception e) {
            log.error("", e);
        }
        log.info("bucket max-objects-quota={}", BUCKET_MAX_OBJECTS_QUOTA);
    }

    /**
     * 桶的默认最大对象数软配额
     */
    public static final double SOFT_OBJECTS_ALARM_THRESHOLD = BUCKET_MAX_OBJECTS_QUOTA * 0.8;


    /**
     * 账户配额满，不可用
     */
    private static final String ACCOUNT_ENOUGH_FLAG = "2";

    /**
     * 达到账户的软配额，需要出发告警
     */
    private static final String ACCOUNT_QUOTA_EXCEED_SOFT = "6";

    /**
     * 桶配额满，不可用
     */
    private static final String BUCKET_ENOUGH_FLAG = "1";

    /**
     * 达到桶的软配额, 需要触发告警
     */
    private static final String BUCKET_SOFT_ENOUGH_FLAG = "5";


    /**
     * 桶可用，且未达到软配额
     */
    private static final String BUCKET_NOT_ENOUGH_FLAG = "0";

    private static final Set<String> checkAccounts = ConcurrentHashMap.newKeySet();
    private static final Set<String> checkBuckets = ConcurrentHashMap.newKeySet();
    private static ScheduledThreadPoolExecutor quotaExecutor;
    private static StoragePool QUOTA_STORAGE_POOL;

    private QuotaRecorder() {
    }

    public static void init() {
        quotaExecutor = new ScheduledThreadPoolExecutor(1, runnable -> new Thread(runnable, "quotaUpdater"));
        quotaExecutor.scheduleAtFixedRate(() -> checkQuota(), 0, UPDATE_TIME, TimeUnit.MILLISECONDS);
        QUOTA_STORAGE_POOL = StoragePoolFactory.getStoragePool(StorageOperate.META);
    }

    public static void addCheckBucket(String bucket) {
        if (!checkBuckets.contains(bucket)) {
            checkBuckets.add(bucket);
        }
    }

    public static void addCheckAccount(String account) {
        if (!checkAccounts.contains(account)) {
            checkAccounts.add(account);
        }
    }

    public static void checkBucketQuota(String bucket, String capacity, String objNum) {
        // 桶容量的软硬配额
        String quota = redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, BUCKET_QUOTA_VALUE);
        String softQuota = redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, BUCKET_SOFT_QUOTA_VALUE);
        // 对象数的软硬配额
        String objNum_quota = redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, BUCKET_OBJNUM_VALUE);
        String softObjNumQuota = redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, BUCKET_SOFT_OBJNUM_VALUE);
        if (capacity != null) {
            checkBucketCapacityOrObjNum(bucket, quota, softQuota, BUCKET_QUOTA_FLAG, Long.parseLong(capacity));
        }
        if (objNum != null) {
            checkBucketCapacityOrObjNum(bucket, objNum_quota, softObjNumQuota, BUCKET_OBJNUM_FLAG, Long.parseLong(objNum));
        }
    }

    private static void checkBucketCapacityOrObjNum(String bucket, String quota, String softQuota, String quota_flag, long curValue) {
        String flag = redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, quota_flag);
        log.debug("bucket:" + bucket + ", quota:" + quota + ", softQuota:" + softQuota + ", quota_flag:" + quota_flag + ", curValue:" + curValue);
        // 检测是否达到硬配额值
        if ((null != quota && !BUCKET_NOT_ENOUGH_FLAG.equals(quota))) {
            if (curValue >= Long.parseLong(quota) && !BUCKET_ENOUGH_FLAG.equals(flag)) {
                redisConnPool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                        .hset(bucket, quota_flag, BUCKET_ENOUGH_FLAG);
                flag = "1";
            }

            if (curValue < Long.parseLong(quota) && !BUCKET_NOT_ENOUGH_FLAG.equals(flag)) {
                redisConnPool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                        .hset(bucket, quota_flag, BUCKET_NOT_ENOUGH_FLAG);
                flag = "0";
            }
            // 当对象数配额为0时, 桶的对象数默认不能超过30亿
        } else if (quota_flag.equals(BUCKET_OBJNUM_FLAG) && curValue >= BUCKET_MAX_OBJECTS_QUOTA) {
            redisConnPool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                    .hset(bucket, quota_flag, BUCKET_ENOUGH_FLAG);
            flag = "1";
        } else if (quota_flag.equals(BUCKET_OBJNUM_FLAG)) {
            redisConnPool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                    .hset(bucket, quota_flag, BUCKET_NOT_ENOUGH_FLAG);
            flag = "0";
        }

        // 检测是否达到软配额值, 若没有设置软配额或桶已达到硬配额,则不作检测
        if ((null != softQuota && !BUCKET_NOT_ENOUGH_FLAG.equals(softQuota)) && !"1".equals(flag)) {
            if (curValue >= Long.parseLong(softQuota) && !BUCKET_SOFT_ENOUGH_FLAG.equals(flag)) {
                redisConnPool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                        .hset(bucket, quota_flag, BUCKET_SOFT_ENOUGH_FLAG);
            }

            if (curValue < Long.parseLong(softQuota) && !BUCKET_NOT_ENOUGH_FLAG.equals(flag)) {
                redisConnPool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                        .hset(bucket, quota_flag, BUCKET_NOT_ENOUGH_FLAG);
            }
        }

        // 当对象数软配额为0时, 对象数超过硬配额的百分之八十，则触发告警
        if ((softQuota == null || "0".equals(softQuota)) && quota_flag.equals(BUCKET_OBJNUM_FLAG) && (quota == null || "0".equals(quota))
                && curValue >= SOFT_OBJECTS_ALARM_THRESHOLD && !"1".equals(flag)) {
            redisConnPool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                    .hset(bucket, quota_flag, BUCKET_SOFT_ENOUGH_FLAG);
        } else if ((softQuota == null || "0".equals(softQuota)) && quota_flag.equals(BUCKET_OBJNUM_FLAG) && (quota == null || "0".equals(quota))
                && curValue < SOFT_OBJECTS_ALARM_THRESHOLD && !"1".equals(flag)) {
            redisConnPool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX)
                    .hset(bucket, quota_flag, BUCKET_NOT_ENOUGH_FLAG);
        }

    }

    private static void checkQuota() {
        try {
            String[] buckets = checkBuckets.toArray(new String[0]);
            checkBuckets.clear();
            for (String bucketName : buckets) {
                final Long exists = redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX).exists(bucketName);
                if (exists == 0) {
                    continue;
                }
                String accountName = redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucketName, "user_name");
                if (null == accountName) {
                    // 账户不存在删除桶信息
                    //deleteBucketInfo(bucketName, QUOTA_STORAGE_POOL.getBucketVnodeId(bucketName));
                    deleteBucketInfo(bucketName);
                } else {
                    String[] bucketQuota = new String[]{null};
                    getBucketInfo(bucketName, BucketInfo::getBucketStorage)
                            .doOnNext(capacity -> bucketQuota[0] = capacity)
                            .flatMap(capacity -> getBucketInfo(bucketName, BucketInfo::getObjectNum))
                            .publishOn(ErasureServer.DISK_SCHEDULER)
                            .subscribe(objNum -> {
                                if (bucketQuota[0] != null || objNum != null) {
                                    checkBucketQuota(bucketName, bucketQuota[0], objNum);
                                }
                            });

                    checkAccounts.add(accountName);
                }
            }
            checkAccounts.forEach(QuotaRecorder::checkAccountQuota);
        } catch (Exception e) {
            log.error("check quota error!", e);
        }
    }

    /**
     * 获取账户使用总容量
     *
     * @param accountName 账户名
     * @return 账户使用容量
     */
    public static Mono<Long> getAccountTotalStorage(String accountName) {
        return getAccountBucketInfo(accountName, BucketInfo::getBucketStorage);
    }

    /**
     * 获取账户下对象的总数量
     *
     * @param accountName 账户名
     * @return 对象数量
     */
    public static Mono<Long> getAccountTotalObject(String accountName) {
        return getAccountBucketInfo(accountName, BucketInfo::getObjectNum);
    }

    private static Mono<Long> getAccountBucketInfo(String accountName, Function<BucketInfo, String> mapFunction) {
        return getBucketSetReactive(accountName)
                .flatMap(bucketName -> getBucketInfo(bucketName, mapFunction))
                .map(Long::parseLong)
                .reduce(Long::sum)
                .defaultIfEmpty(0L)
                .onErrorMap(e -> new MsException(ErrorNo.UNKNOWN_ERROR, "get account used capacity error!"))
                .map(cap -> cap < 0 ? 0 : cap);
    }

    private static <T> Mono<T> getBucketInfo(String bucketName, Function<BucketInfo, T> mapFunction) {
        return ErasureClient.reduceBucketInfo(bucketName)
                .doOnNext(bucketInfo -> {
                    if (bucketInfo.equals(ERROR_BUCKET_INFO)) {
                        checkBuckets.add(bucketName);
                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "get bucket storage info error!");
                    }
                })
//                .filter(BucketInfo::isAvailable)
                .map(mapFunction)
                .doOnError(e -> log.error("get bucket storage info error,{}", e));
    }

    /**
     * 获取账户下桶集合
     *
     * @param accountName 账户名
     * @return 账户下桶集合流
     */
    private static Flux<String> getBucketSetReactive(String accountName) {
        return redisConnPool.getReactive(REDIS_USERINFO_INDEX).hget(accountName, "id")
                .flatMap(accountId -> Mono.just(accountId + USER_BUCKET_SET_SUFFIX))
                .flatMapMany(accountBucketSetKey -> redisConnPool.getReactive(REDIS_USERINFO_INDEX)
                        .smembers(accountBucketSetKey))
                .doOnError(e -> log.error("",e));
    }

    private static void deleteBucketInfo(String bucket, String bucketVnode) {
        QUOTA_STORAGE_POOL.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> ErasureClient.deleteRocketsValue(bucket,
                        BucketInfo.getBucketKey(nodeList.get(0).var3, bucket), nodeList))
                .subscribe(b -> {
                    if (!b) {
                        log.error("delete bucket:{} storage record fail", bucket);
                    }
                });
    }

    private static void deleteBucketInfo(String bucket) {
        List<String> bucketVnodeList = QUOTA_STORAGE_POOL.getBucketVnodeList(bucket);
        Flux.fromIterable(bucketVnodeList)
                .flatMap(vnode -> {
                    return QUOTA_STORAGE_POOL.mapToNodeInfo(vnode)
                            .flatMap(nodeList -> ErasureClient.deleteRocketsValue(bucket,
                                    BucketInfo.getBucketKey(nodeList.get(0).var3, bucket), nodeList));
                })
                .subscribe();
    }

    private static void checkAccountQuota(String account) {
        String[] accountQuota = new String[]{null};
        String[] objNumQuota = new String[]{null};
        String[] accountSoftQuota = new String[]{null};
        String[] accountSoftObjNum = new String[]{null};
        Mono.just(redisConnPool.getCommand(REDIS_USERINFO_INDEX)
                .hgetall(account))
                .doOnNext(userInfo -> accountQuota[0] = userInfo.get(ACCOUNT_QUOTA_VALUE))
                .doOnNext(userInfo -> objNumQuota[0] = userInfo.get(ACCOUNT_OBJNUM_VALUE))
                .doOnNext(userInfo -> accountSoftQuota[0] = userInfo.get(ACCOUNT_SOFT_QUOTA_VALUE))
                .doOnNext(userInfo -> accountSoftObjNum[0] = userInfo.get(ACCOUNT_SOFT_OBJNUM_VALUE))
                .filter(userInfo -> (null != accountQuota[0] && !BUCKET_NOT_ENOUGH_FLAG.equals(accountQuota[0]))
                        || (null != objNumQuota[0] && !BUCKET_NOT_ENOUGH_FLAG.equals(objNumQuota[0]))
                        || (null != accountSoftQuota[0] && !BUCKET_NOT_ENOUGH_FLAG.equals(accountSoftQuota[0]))
                        || (null != accountSoftObjNum[0] && !BUCKET_NOT_ENOUGH_FLAG.equals(accountSoftObjNum[0])))
                .flatMap(userInfo -> getAccountTotalStorage(account)
                        .zipWith(Mono.just(Long.parseLong(accountQuota[0] == null ? "0" : accountQuota[0]))))
                .map(tuple2 -> {
                    long soft = Long.parseLong(accountSoftQuota[0] == null ? "0" : accountSoftQuota[0]);
                    if (tuple2.getT2() != 0 || soft != 0) {
                        return checkAccountCapacityOrObjNum(account, tuple2.getT2(), soft, ACCOUNT_QUOTA_FLAG, tuple2.getT1());
                    }
                    return true;
                })
                .flatMap(flag -> getAccountTotalObject(account)
                        .zipWith(Mono.just(Long.parseLong(objNumQuota[0] == null ? "0" : objNumQuota[0])))
                        .map(tuple2 -> {
                            long soft = Long.parseLong(accountSoftObjNum[0] == null ? "0" : accountSoftObjNum[0]);
                            if (tuple2.getT2() != 0 || soft != 0) {
                                return checkAccountCapacityOrObjNum(account, tuple2.getT2(), soft, ACCOUNT_OBJNUM_FLAG, tuple2.getT1()) && flag;
                            }
                            return flag;
                        })
                )
                .subscribe(b -> {
                    if (!b) {
                        log.error("set bucket quota error!");
                    } else {
                        checkAccounts.remove(account);
                    }
                }, log::error);
    }

    private static Boolean checkAccountCapacityOrObjNum(String account, long quota, long softQuota, String quota_flag, long curValue) {
        try {
            log.debug("account:" + account + ", quota:" + quota + ", softQuota:" + softQuota + ", quota_flag:" + quota_flag + ", curValue:" + curValue);
            // 检测硬配额
            String account_flag = redisConnPool.getCommand(REDIS_USERINFO_INDEX).hget(account, quota_flag);
            if (quota != 0 && curValue >= quota && !ACCOUNT_ENOUGH_FLAG.equals(account_flag)) {
                redisConnPool.getShortMasterCommand(REDIS_USERINFO_INDEX)
                        .hset(account, quota_flag, ACCOUNT_ENOUGH_FLAG);
                account_flag = ACCOUNT_ENOUGH_FLAG;
            }
            if (quota == 0 || (curValue < quota && !BUCKET_NOT_ENOUGH_FLAG.equals(account_flag))) {
                redisConnPool.getShortMasterCommand(REDIS_USERINFO_INDEX)
                        .hset(account, quota_flag, BUCKET_NOT_ENOUGH_FLAG);
                account_flag = BUCKET_NOT_ENOUGH_FLAG;
            }

            // 检测软配额, 如果之前已经达到硬配额则不作检测
            if (!ACCOUNT_ENOUGH_FLAG.equals(account_flag)) {
                if (softQuota != 0 && curValue >= softQuota) {
                    redisConnPool.getShortMasterCommand(REDIS_USERINFO_INDEX)
                            .hset(account, quota_flag, ACCOUNT_QUOTA_EXCEED_SOFT);
                }
                if (softQuota == 0 || (curValue < softQuota)) {
                    redisConnPool.getShortMasterCommand(REDIS_USERINFO_INDEX)
                            .hset(account, quota_flag, BUCKET_NOT_ENOUGH_FLAG);
                }
            }

            return true;
        } catch (Exception e) {
            log.error("set account quota flag error");
            return false;
        }
    }

}
