package com.macrosan.filesystem.utils;

import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.move.CacheFlushConfigRefresher;
import com.macrosan.utils.layerMonitor.BackStoreAccessedData;
import com.macrosan.utils.layerMonitor.ObjectAccessCache;
import io.vertx.core.json.Json;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;

import static com.macrosan.constants.SysConstants.ROCKS_CACHE_BACK_STORE_KEY;
import static com.macrosan.storage.move.CacheMove.*;
import static com.macrosan.storage.strategy.StorageStrategy.BUCKET_STRATEGY_NAME_MAP;

/**
 * @author DaiFengtao
 * {@code @date} 2026年03月16日 11:02
 */
@Slf4j
public class FsTierUtils {

    public static boolean FS_TIER_DEBUG = false;

    public static boolean isEnableCacheAccessTimeFlushByBucket(String bucketName) {
        return Optional.ofNullable(bucketName)
                .map(BUCKET_STRATEGY_NAME_MAP::get)
                .map(poolStrategy -> {
                    if (!isEnableCacheAccessTimeFlush(poolStrategy)) {
                        return false;
                    }
                    for (StoragePool cachePool : getCachePools(poolStrategy)) {
                        if (isEnableCacheAccessTimeFlush(cachePool)) {
                            return true;
                        }
                    }
                    return false;
                })
                .orElse(false);
    }

    public static void addAccessRecord(String bucketName, String objName, String versionId) {
        ObjectAccessCache.addAccessRecord(bucketName, objName, versionId);
    }

    public static void addBackStoreRecord(String bucketName, String objName, String versionId, long lastAccessTime) {
        if (maybeHasFlush(bucketName, lastAccessTime)) {
            BackStoreAccessedData.addNeedBackStoreRecord(bucketName, objName, versionId);
        }
    }

    /**
     * 也许已经文件已经下刷到了冷层
     *
     * @param bucketName     桶名
     * @param lastAccessTime 上次访问时间
     * @return res
     */
    public static boolean maybeHasFlush(String bucketName, long lastAccessTime) {

        return Optional.ofNullable(bucketName)
                .map(BUCKET_STRATEGY_NAME_MAP::get)
                .map(poolStrategyName -> {
                    if (!CacheFlushConfigRefresher.getInstance().getStrategyFlushConfig(poolStrategyName).isEnableAccessTimeFlush()) {
                        return false;
                    }
                    List<StoragePool> cachePools = getCachePools(poolStrategyName);
                    if (cachePools != null && !cachePools.isEmpty()) {
                        //需判断 时间和容量是否符合
                        for (StoragePool cachePool : cachePools) {
                            //假设是30天,目前代码这里是每个池单独设置，按规格的话同一个策略下的缓存池配置应该统一
                            long days = getLowFrequencyAccessDays(cachePool);
                            //本次扫描往前推30天
                            long endStamp = System.currentTimeMillis() - days * 24 * 60 * 60 * 1000;
                            boolean canBackStore = false;
                            long singleUsed = 0L;
                            long singleTotal = 0L;
                            float used = 0.0f;
                            singleUsed = cachePool.getCache().size;
                            singleTotal = cachePool.getCache().totalSize;
                            if (singleTotal != 0L) {
                                used = singleUsed * 100.0f / singleTotal;//每一个缓存池都单独计算水位线
                            }
                            if (used < getCachePoolFullWaterMark(cachePool)) {
                                canBackStore = true;
                            }
                            if (lastAccessTime < endStamp && canBackStore) {
                                return true;
                            }
                        }
                    }
                    return false;
                })
                .orElse(false);
    }

    public static void fileFsAccessHandle(Inode accessInode) {
        if (accessInode != null) {
            if (isEnableCacheAccessTimeFlushByBucket(accessInode.getBucket())) {
                addAccessRecord(accessInode.getBucket(), accessInode.getObjName(), accessInode.getVersionId());
                addBackStoreRecord(accessInode.getBucket(), accessInode.getObjName(), accessInode.getVersionId(), Inode.getLastAccessTime(accessInode));
            }
        }
    }

    public static void fileS3AccessHandle(MetaData metaData) {
        if (metaData != null && metaData.inode > 0) {
            if (StringUtils.isNotBlank(metaData.getTmpInodeStr())) {
                try {
                    Inode accessInode = Json.decodeValue(metaData.getTmpInodeStr(), Inode.class);
                    fileFsAccessHandle(accessInode);
                } catch (Exception e) {

                }
            }
        }
    }

    public static String getBackStoreKey(String storage, long nodeId, String bucket, String objVnodeId) {
        return ROCKS_CACHE_BACK_STORE_KEY + "_" + storage + "_" + nodeId + "_" + bucket + "_" + objVnodeId;
    }

    public static boolean isFsEnableCacheAccessTimeFlush(Inode inode, StoragePool pool) {
        return isEnableCacheAccessTimeFlush(pool)
                && isEnableCacheAccessTimeFlush(BUCKET_STRATEGY_NAME_MAP.get(inode.getBucket()))
                && Inode.getLastAccessTime(inode) != 0;
    }

    public static String buildTierRecordValue(String fileName, long fileOffset, long fileSize, String stamp) {
        return fileName + "#" + fileOffset + "#" + fileSize + "#" + stamp;
    }

}
