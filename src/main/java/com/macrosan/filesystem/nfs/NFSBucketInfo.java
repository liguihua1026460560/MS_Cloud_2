package com.macrosan.filesystem.nfs;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.utils.InodeUtils;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.macrosan.constants.AccountConstants.DEFAULT_USER_ID;
import static com.macrosan.constants.SysConstants.*;

@Log4j2
public class NFSBucketInfo {
    public static String FSID_BUCKET = "fsid_bucket";
    public static Map<Integer, FsInfo> fsToBucket = new ConcurrentHashMap<>();
    public static Map<String, Map<String, String>> bucketInfo = new ConcurrentHashMap<>();
    private static final RedisConnPool redisConnPool = RedisConnPool.getInstance();

    static {
        try {
            if (redisConnPool.getCommand(REDIS_SYSINFO_INDEX).exists(FSID_BUCKET) > 0) {
                Map<String, String> fsidToBucket = redisConnPool.getCommand(REDIS_SYSINFO_INDEX).hgetall(FSID_BUCKET);
                for (String fsid : fsidToBucket.keySet()) {
                    String bucket = fsidToBucket.get(fsid);
                    FsInfo fsInfo = new FsInfo();
                    fsInfo.setBucket(bucket);
                    fsToBucket.put(Integer.parseInt(fsid), fsInfo);
                    Map<String, String> infoMap = redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucket);
                    bucketInfo.put(bucket, infoMap);
                    InodeUtils.getAndPutRootInode(bucket, infoMap);
                }
            }
        } catch (Exception e) {
            log.error("init fsid to bucket error", e);
        }

    }

    // 用于初始化静态代码块
    public static void init() {

    }

    @Data
    public static class FsInfo {
        String bucket = "";
    }

    public static String getBucketName(int fsid) {
        if (fsToBucket.containsKey(fsid)) {
            return fsToBucket.get(fsid).getBucket();
        } else {
            String bucketName = "";
            try {
                bucketName = redisConnPool.getCommand(REDIS_SYSINFO_INDEX).hget(FSID_BUCKET, String.valueOf(fsid));
                if (StringUtils.isEmpty(bucketName)) {
                    String errorMsg = "The fsid " + fsid + " does not exits, can not find bucket " + bucketName + ", please remount nfs.";
                    throw new NFSException(FsConstants.NfsErrorNo.NFS3ERR_STALE, errorMsg);
                }
                log.info("fsid:{}, bucket:{}", fsid, bucketName);
                Map<String, String> bucketInfo0 = redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
                synchronized (fsToBucket) {
                    FsInfo fsInfo = new FsInfo();
                    fsInfo.setBucket(bucketName);
                    fsToBucket.put(fsid, fsInfo);
                    bucketInfo.put(bucketName, bucketInfo0);
                }
            } catch (NFSException e) {
                throw e;
            } catch (Exception e) {
                log.error("get bucket error", e);
            }
            return bucketName;
        }
    }

    public static Map<String, String> getBucketInfo(String bucket) {
        if (StringUtils.isBlank(bucket)) {
            throw new NFSException(FsConstants.NfsErrorNo.NFS3ERR_STALE, "The bucket does not exits,please remount nfs.");
        }
        return bucketInfo.compute(bucket, (k, v) -> {
            if (v != null) {
                return v;
            }
            return redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucket);
        });
    }

    public static Mono<Map<String, String>> getBucketInfoReactive(String bucket) {
        return redisConnPool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucket)
                .defaultIfEmpty(Collections.emptyMap())
                .doOnNext(map -> {
                    if (map != null && !map.isEmpty()) {
                        bucketInfo.compute(bucket, (k, v) -> map);
                    }
                });
    }

    public static Mono<Map<String, String>> getFTPBucketInfoReactive(String bucket) {
        if (StringUtils.isBlank(bucket)) {
            throw new NFSException(FsConstants.NfsErrorNo.NFS3ERR_STALE, "The bucket does not exits,please remount nfs.");
        }

        return redisConnPool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucket)
                .defaultIfEmpty(Collections.emptyMap())
                .doOnNext(map -> {
                    if (map != null && !map.isEmpty()) {
                        bucketInfo.compute(bucket, (k, v) -> map);
                    }
                });
    }

    public static void removeBucketInfo(String bucket) {
        try {
            if (redisConnPool.getCommand(REDIS_SYSINFO_INDEX).exists(FSID_BUCKET) > 0) {
                // 清理内存中的信息
                AtomicReference<String> fsid = new AtomicReference<>("");
                bucketInfo.computeIfPresent(bucket, (k, v) -> {
                    fsid.set(v.get("fsid"));
                    if (StringUtils.isNotBlank(fsid.get())) {
                        fsToBucket.remove(Integer.parseInt(fsid.get()));
                    }
                    return null;
                });

                if (!fsid.get().equalsIgnoreCase("")) {
                    redisConnPool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel(FSID_BUCKET, fsid.get());
                } else {
                    Map<String, String> infoMap = redisConnPool.getCommand(REDIS_SYSINFO_INDEX).hgetall(FSID_BUCKET);
                    for (Map.Entry<String, String> entry : infoMap.entrySet()) {
                        if (entry.getValue().equals(bucket)) {
                            redisConnPool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel(FSID_BUCKET, entry.getKey());
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("remove bucketInfo error", e);
        }
    }

    public static Mono<Boolean> isFsidExist(int fsid) {
        return redisConnPool.getReactive(REDIS_SYSINFO_INDEX).hexists(FSID_BUCKET, String.valueOf(fsid));
    }

    /**
     * 查看当前桶是否已经开启 nfs 共享
     **/
    public static boolean isNFSShare(String bucket) {
        if (null != bucketInfo.get(bucket) && "1".equals(bucketInfo.get(bucket).get("nfs"))) {
            return true;
        }

        return false;
    }

    public static boolean isNFSShare(Map<String, String> bucketInfo) {
        if (null != bucketInfo && "1".equals(bucketInfo.get("nfs"))) {
            return true;
        }

        return false;
    }

    /**
     * 查看当前桶是否已经开启 cifs 共享
     **/
    public static boolean isCIFSShare(String bucket) {
        if (null != bucketInfo.get(bucket) && "1".equals(bucketInfo.get(bucket).get("cifs"))) {
            return true;
        }

        return false;
    }

    public static boolean isCIFSShare(Map<String, String> bucketInfo) {
        if (null != bucketInfo && "1".equals(bucketInfo.get("cifs"))) {
            return true;
        }

        return false;
    }

    public static Mono<Boolean> isCIFSShareReactive (String bucket) {
        return redisConnPool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucket)
                .defaultIfEmpty(Collections.emptyMap())
                .map(info -> {
                    if (null == info || info.isEmpty()) {
                        return false;
                    }

                    if ("1".equals(info.get("cifs"))) {
                        return true;
                    }

                    return false;
                });
    }

    public static boolean isExistBucketOwner(String bucket) {
        if (StringUtils.isBlank(bucket) || null == bucketInfo
                || bucketInfo.isEmpty() || !bucketInfo.containsKey(bucket)
                || null == bucketInfo.get(bucket)
                || StringUtils.isBlank(bucketInfo.get(bucket).get(BUCKET_USER_ID))) {
            return false;
        }

        return true;
    }

    public static String getBucketOwner(String bucket) {
        try {
            if (bucketInfo.containsKey(bucket)) {
                Map<String, String> info = bucketInfo.get(bucket);
                String owner = info.getOrDefault(BUCKET_USER_ID, DEFAULT_USER_ID);
                return owner;
            }
        } catch (Exception e) {
            log.error("get bucket owner error", e);
        }

        return DEFAULT_USER_ID;
    }
}
