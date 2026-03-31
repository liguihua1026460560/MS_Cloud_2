package com.macrosan.filesystem.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.database.rocksdb.batch.WriteBatch;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.quota.FSQuotaConstants;
import com.macrosan.filesystem.quota.FSQuotaRealService;
import com.macrosan.message.jsonmsg.DirInfo;
import com.macrosan.message.jsonmsg.FSQuotaConfig;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import io.lettuce.core.SetArgs;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.ServerConstants.SLASH;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.database.rocksdb.MSRocksDB.READ_OPTIONS;
import static com.macrosan.database.rocksdb.batch.BatchRocksDB.toByte;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_DQUOT;
import static com.macrosan.filesystem.quota.FSQuotaConstants.*;
import static com.macrosan.filesystem.quota.FSQuotaRealService.QUOTA_CONFIG_CACHE;
import static com.macrosan.filesystem.quota.FSQuotaRealService.delFsDirQuotaInfo;
import static com.macrosan.message.jsonmsg.FSQuotaConfig.getCapKey;
import static com.macrosan.message.jsonmsg.FSQuotaConfig.getNumKey;
import static com.macrosan.message.jsonmsg.Inode.*;

@Log4j2
public class FSQuotaUtils {
    protected static RedisConnPool pool = RedisConnPool.getInstance();

    public static void updateCapInfo(WriteBatch writeBatch, String bucket, String vnode, long num, long capacity, long dirNodeId, int type, int id) throws RocksDBException {
        if (capacity == 0 && num == 0) {
            return;
        }
        String capKey = getCapKey(vnode, bucket, dirNodeId, type, id);
        String objNumKey = getNumKey(vnode, bucket, dirNodeId, type, id);
        writeBatch.merge(capKey.getBytes(), toByte(capacity));
        writeBatch.merge(objNumKey.getBytes(), toByte(num));
    }

    /**
     * 更新目录配额信息，包含文件数和容量的更新
     *
     * @param writeBatch writeBatch
     * @param bucket     bucket
     * @param vnode      vnode
     * @param num        num
     * @param capacity   capacity
     * @param dirNodeId  dirName
     * @throws RocksDBException RocksDBException
     */
    public static void updateDirCap(WriteBatch writeBatch, String bucket, String vnode, long num, long capacity, long dirNodeId) throws RocksDBException {
        updateCapInfo(writeBatch, bucket, vnode, num, capacity, dirNodeId, FS_DIR_QUOTA, 0);
    }

    public static void updateUserQuota(WriteBatch writeBatch, String bucket, String vnode, long num, long capacity, long dirNodeId, int uid) throws RocksDBException {
        updateCapInfo(writeBatch, bucket, vnode, num, capacity, dirNodeId, FS_USER_QUOTA, uid);
    }

    public static void updateGroupQuota(WriteBatch writeBatch, String bucket, String vnode, long num, long capacity, long dirNodeId, int gid) throws RocksDBException {

        updateCapInfo(writeBatch, bucket, vnode, num, capacity, dirNodeId, FS_GROUP_QUOTA, gid);
    }

    /**
     * 同步同名覆盖处理：
     * 1、原文件若所属用户/组为root用户/组时，处理与s3对象的同名覆盖处理一致。
     * 2、原文件所属用户/组，为其用户/组，则root用户/组文件配额，文件数加1，容量加上新上传对象的容量；
     * 原用户/组文件配额，文件数减1，容量需减去原文件的大小。
     *
     * @param writeBatch w
     * @param bucket     桶名
     * @param objName    对象名
     * @param vnode      vnode
     * @param num        对象数
     * @param oldCap     旧对象大小
     * @param newCap     新对象大小
     * @param oldInode   就对象inode元数据
     */
    public static void updateCapInfoSyncOverWrite(WriteBatch writeBatch, String bucket, String objName, String vnode, long num, long oldCap, long newCap, Inode oldInode) {
        if (oldCap == 0 && num == 0 && newCap == 0) {
            return;
        }
        List<Integer> uids = new LinkedList<>();

        uids.add(oldInode.getUid());
        uids.add(0);
        List<Integer> gids = new LinkedList<>();
        gids.add(oldInode.getGid());
        gids.add(0);
        long delta = newCap - oldCap;
        Tuple2<Boolean, List<String>> tuple2 = existQuotaInfoListUserOrGroup(bucket, objName, System.currentTimeMillis(), uids, gids).block();
        if (tuple2 != null && tuple2.var1) {
            List<String> quotaKeys = tuple2.var2;
            for (String key : quotaKeys) {
                String realKey = key.substring(key.indexOf("_") + 1);
                QUOTA_CONFIG_CACHE.computeIfPresent(bucket, (k, v) -> {
                    v.computeIfPresent(realKey, (k1, v1) -> {
                        try {
                            if (realKey.startsWith(FS_USER_QUOTA_PREFIX)) {
                                int id = getIdFromKey(realKey);
                                long dirNodeId = Long.parseLong(getDirNameFromKey(bucket, realKey, FS_USER_QUOTA_PREFIX, realKey.lastIndexOf("/")));
                                if (id == oldInode.getUid() && id == 0) {
                                    updateUserQuota(writeBatch, bucket, vnode, 0, delta, dirNodeId, id);
                                    return v1;
                                }

                                if (id == oldInode.getUid()) {
                                    updateUserQuota(writeBatch, bucket, vnode, -num, -oldCap, dirNodeId, id);
                                } else {
                                    updateUserQuota(writeBatch, bucket, vnode, num, newCap, dirNodeId, id);
                                }
                            } else if (realKey.startsWith(FS_GROUP_QUOTA_PREFIX)) {
                                int id = getIdFromKey(realKey);
                                long dirNodeId = Long.parseLong(getDirNameFromKey(bucket, realKey, FS_GROUP_QUOTA_PREFIX, realKey.lastIndexOf("/")));
                                if (id == oldInode.getGid() && id == 0) {
                                    updateGroupQuota(writeBatch, bucket, vnode, 0, delta, dirNodeId, id);
                                    return v1;
                                }
                                if (id == oldInode.getGid()) {
                                    updateGroupQuota(writeBatch, bucket, vnode, -num, -oldCap, dirNodeId, id);
                                } else {
                                    updateGroupQuota(writeBatch, bucket, vnode, num, newCap, dirNodeId, id);
                                }
                            } else if (realKey.startsWith(FS_DIR_QUOTA_PREFIX)) {
                                long dirNodeId = Long.parseLong(getDirNameFromKey(bucket, realKey, FS_DIR_QUOTA_PREFIX, realKey.length()));
                                updateDirCap(writeBatch, bucket, vnode, 0, delta, dirNodeId);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        return v1;
                    });
                    return v;
                });
            }
        }

    }

    public static boolean isNotRootUserOrGroup(Inode inode) {
        return inode.getUid() != 0 || inode.getGid() != 0;
    }

    public static void updateCapAfterSetAttr(String updateCapKeyStr, WriteBatch writeBatch, String bucket, String vnode, long num, long capacity, Inode oldInode, Inode newInode) throws RocksDBException {
        log.debug("updateAllKeyCap bucket:{}, updateCapKeyStr:{}", bucket, updateCapKeyStr);
        if (StringUtils.isBlank(updateCapKeyStr)) {
            return;
        }
        if (capacity == 0 && num == 0) {
            return;
        }
        List<String> updateCapKeys = Json.decodeValue(updateCapKeyStr, new TypeReference<List<String>>() {
        });
        if (!updateCapKeys.isEmpty()) {
            //更新目录容量
            for (String key : updateCapKeys) {
                String realKey = key.substring(key.indexOf("_") + 1);
                QUOTA_CONFIG_CACHE.computeIfPresent(bucket, (k, v) -> {
                    v.computeIfPresent(realKey, (k1, v1) -> {
                        try {
                            if (realKey.startsWith(FS_USER_QUOTA_PREFIX)) {
                                int id = getIdFromKey(realKey);
                                long dirNodeId = Long.parseLong(getDirNameFromKey(bucket, realKey, FS_USER_QUOTA_PREFIX, realKey.lastIndexOf("/")));
                                if (id == oldInode.getUid()) {
                                    updateUserQuota(writeBatch, bucket, vnode, -num, -capacity, dirNodeId, id);

                                } else {
                                    updateUserQuota(writeBatch, bucket, vnode, num, capacity, dirNodeId, id);

                                }
                            } else if (realKey.startsWith(FS_GROUP_QUOTA_PREFIX)) {
                                int id = getIdFromKey(realKey);
                                long dirNodeId = Long.parseLong(getDirNameFromKey(bucket, realKey, FS_GROUP_QUOTA_PREFIX, realKey.lastIndexOf("/")));
                                if (id == oldInode.getGid()) {

                                    updateGroupQuota(writeBatch, bucket, vnode, -num, -capacity, dirNodeId, id);

                                } else {
                                    updateGroupQuota(writeBatch, bucket, vnode, num, capacity, dirNodeId, id);

                                }
                            }
                        } catch (RocksDBException e) {
                            throw new RuntimeException(e);
                        }
                        return v1;
                    });
                    return v;
                });

            }
        }
    }

    public static void updateAllKeyOnCreateS3Inode(String updateCapKeyStr, WriteBatch writeBatch, String bucket, String vnode, long num, long capacity) throws RocksDBException {
        log.debug("updateAllKeyCap bucket:{}, updateCapKeyStr:{}", bucket, updateCapKeyStr);
        if (StringUtils.isBlank(updateCapKeyStr)) {
            return;
        }
        if (capacity == 0 && num == 0) {
            return;
        }
        List<String> updateCapKeys = Json.decodeValue(updateCapKeyStr, new TypeReference<List<String>>() {
        });
        if (!updateCapKeys.isEmpty()) {
            //更新目录容量
            for (String key : updateCapKeys) {
                String realKey = key.substring(key.indexOf("_") + 1);
                QUOTA_CONFIG_CACHE.computeIfPresent(bucket, (k, v) -> {
                    v.computeIfPresent(realKey, (k1, v1) -> {
                        try {
                            if (realKey.startsWith(FS_USER_QUOTA_PREFIX)) {
                                int id = getIdFromKey(realKey);
                                long dirNodeId = Long.parseLong(getDirNameFromKey(bucket, realKey, FS_USER_QUOTA_PREFIX, realKey.lastIndexOf("/")));
                                if (id == 0) {
                                    updateUserQuota(writeBatch, bucket, vnode, -num, -capacity, dirNodeId, id);
                                } else {
                                    updateUserQuota(writeBatch, bucket, vnode, num, capacity, dirNodeId, id);
                                }
                            } else if (realKey.startsWith(FS_GROUP_QUOTA_PREFIX)) {
                                int id = getIdFromKey(realKey);
                                long dirNodeId = Long.parseLong(getDirNameFromKey(bucket, realKey, FS_GROUP_QUOTA_PREFIX, realKey.lastIndexOf("/")));
                                if (id == 0) {
                                    updateGroupQuota(writeBatch, bucket, vnode, -num, -capacity, dirNodeId, id);
                                } else {
                                    updateGroupQuota(writeBatch, bucket, vnode, num, capacity, dirNodeId, id);
                                }
                            }
                        } catch (RocksDBException e) {
                            throw new RuntimeException(e);
                        }
                        return v1;
                    });
                    return v;
                });

            }
        }
    }

    public static void updateAllKeyCap(String updateCapKeyStr, WriteBatch writeBatch, String bucket, String vnode, long num, long capacity) throws RocksDBException {
        if (StringUtils.isBlank(updateCapKeyStr)) {
            return;
        }
        if (capacity == 0 && num == 0) {
            return;
        }
        List<String> updateCapKeys = Json.decodeValue(updateCapKeyStr, new TypeReference<List<String>>() {
        });
        updateAllKeyCap0(updateCapKeys, writeBatch, bucket, vnode, num, capacity);
    }

    public static void updateAllKeyCap0(List<String> updateCapKeys, WriteBatch writeBatch, String bucket, String vnode, long num, long capacity) throws RocksDBException {
        if (!updateCapKeys.isEmpty()) {
            //更新目录容量
            for (String key : updateCapKeys) {
                String realKey = key.substring(key.indexOf("_") + 1);
                QUOTA_CONFIG_CACHE.computeIfPresent(bucket, (k, v) -> {
                    v.computeIfPresent(realKey, (k1, v1) -> {
                        try {
                            if (realKey.startsWith(FS_DIR_QUOTA_PREFIX)) {
                                long dirNodeId = Long.parseLong(getDirNameFromKey(bucket, realKey, FS_DIR_QUOTA_PREFIX, realKey.length()));
                                updateDirCap(writeBatch, bucket, vnode, num, capacity, dirNodeId);
                            } else if (realKey.startsWith(FS_USER_QUOTA_PREFIX)) {
                                int id = getIdFromKey(realKey);
                                long dirNodeId = Long.parseLong(getDirNameFromKey(bucket, realKey, FS_USER_QUOTA_PREFIX, realKey.lastIndexOf("/")));
                                updateUserQuota(writeBatch, bucket, vnode, num, capacity, dirNodeId, id);
                            } else if (realKey.startsWith(FS_GROUP_QUOTA_PREFIX)) {
                                int id = getIdFromKey(realKey);
                                long dirNodeId = Long.parseLong(getDirNameFromKey(bucket, realKey, FS_GROUP_QUOTA_PREFIX, realKey.lastIndexOf("/")));
                                updateGroupQuota(writeBatch, bucket, vnode, num, capacity, dirNodeId, id);
                            }
                        } catch (RocksDBException e) {
                            throw new RuntimeException(e);
                        }
                        return v1;
                    });
                    return v;
                });

            }
        }
    }

    public static Mono<Boolean> updateQuotaInfoError(String bucket, long dirNodeId, String vnode, String lun, long cap, long num, int quotaType, int id_, String quotaTypeKey) {
        BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
            QUOTA_CONFIG_CACHE.computeIfPresent(bucket, (k, v) -> {
                v.computeIfPresent(quotaTypeKey, (k1, v1) -> {
                    try {
                        if (quotaType == FS_DIR_QUOTA) {
                            updateDirCap(writeBatch, bucket, vnode, num, cap, dirNodeId);
                        } else if (quotaType == FS_USER_QUOTA) {
                            updateUserQuota(writeBatch, bucket, vnode, num, cap, dirNodeId, id_);
                        } else if (quotaType == FS_GROUP_QUOTA) {
                            updateGroupQuota(writeBatch, bucket, vnode, num, cap, dirNodeId, id_);
                        }
                    } catch (RocksDBException e) {
                        throw new RuntimeException(e);
                    }
                    return v1;
                });
                return v;
            });

        };
        return BatchRocksDB.customizeOperateMeta(lun, String.valueOf(dirNodeId).hashCode(), consumer).onErrorReturn(false).map(str -> true);
    }

    public static Mono<Boolean> updateQuotaInfoError0(String bucket, long dirNodeId, String vnode, String lun, long cap, long num, int quotaType, int id_, String quotaTypeKey) {
        return updateQuotaInfoError(bucket, dirNodeId, vnode, lun, cap, num, quotaType, id_, quotaTypeKey).flatMap(r -> {
            if (!r) {
                return updateQuotaInfoError(bucket, dirNodeId, vnode, lun, cap, num, quotaType, id_, quotaTypeKey);
            }
            return Mono.just(r);
        });
    }

    public static FSQuotaConfig buildQuotaConfig(Map<String, String> paramMap, String bucketName, String s3AccountName) {
        try {
            int quotaType = Integer.parseInt(paramMap.getOrDefault(QUOTA_TYPE, "-1"));
            // check the quota type
            if (!FS_QUOTA_TYPE_SET.contains(quotaType)) {
                throw new MsException(INVALID_ARGUMENT, "the quota type input is invalid.");
            }

            long filesSoftQuota = Long.parseLong(paramMap.getOrDefault(FILES_SOFT_QUOTA, "0"));
            if (filesSoftQuota < 0) {
                throw new MsException(INVALID_ARGUMENT, "the files soft quota input is invalid.");
            }
            long filesHardQuota = Long.parseLong(paramMap.getOrDefault(FILES_HARD_QUOTA, "0"));
            if (filesHardQuota < 0) {
                throw new MsException(INVALID_ARGUMENT, "the files hard quota input is invalid.");
            }

            checkSoftAndHardLimit(filesSoftQuota, filesHardQuota);
            long capacitySoftQuota = Long.parseLong(paramMap.getOrDefault(CAPACITY_SOFT_QUOTA, "0"));
            if (capacitySoftQuota < 0) {
                throw new MsException(INVALID_ARGUMENT, "the capacity soft quota input is invalid.");
            }
            long capacityHardQuota = Long.parseLong(paramMap.getOrDefault(CAPACITY_HARD_QUOTA, "0"));
            if (capacityHardQuota < 0) {
                throw new MsException(INVALID_ARGUMENT, "the capacity hard quota input is invalid.");
            }

            checkSoftAndHardLimit(capacitySoftQuota, capacityHardQuota);
            if (filesHardQuota == 0 && filesSoftQuota == 0 && capacityHardQuota == 0 && capacitySoftQuota == 0) {
                throw new MsException(INVALID_ARGUMENT, "the quota value input is invalid.");
            }

            String dirName = paramMap.get(DIR_NAME);
            if (StringUtils.isBlank(dirName)) {
                throw new MsException(INVALID_ARGUMENT, "the dirName input is invalid.");
            }
            try {
                dirName = URLDecoder.decode(dirName, "UTF-8");
            } catch (UnsupportedEncodingException e) {
            }
            if ("/".equals(dirName)) {
                throw new MsException(INVALID_ARGUMENT, "the dirName input is invalid.");
            }
            if (!dirName.endsWith("/")) {
                dirName += "/";
            }
            int gid = 0;
            int uid = 0;
            if (quotaType == FS_USER_QUOTA) {
                String uidStr = paramMap.get(FS_UID);
                uid = checkInputIdAndGet(uidStr);
            }

            if (quotaType == FS_GROUP_QUOTA) {
                String gidStr = paramMap.get(FS_GID);
                gid = checkInputIdAndGet(gidStr);
            }

            String cifsUserId = paramMap.getOrDefault(FS_CIFS_USER, "");
            String modify = paramMap.getOrDefault(MODIFY, "0");
            if (!"0".equals(modify) && !"1".equals(modify)) {
                throw new MsException(INVALID_ARGUMENT, "the modify input is invalid.");
            }
            long stamp = System.currentTimeMillis();
            FSQuotaConfig config = new FSQuotaConfig()
                    .setBucket(bucketName)
                    .setDirName(dirName)
                    .setQuotaType(quotaType)
                    .setFilesSoftQuota(filesSoftQuota)
                    .setFilesHardQuota(filesHardQuota)
                    .setCapacitySoftQuota(capacitySoftQuota)
                    .setCapacityHardQuota(capacityHardQuota)
                    .setModify("1".equals(modify))
                    .setGid(gid)
                    .setUid(uid)
                    .setS3AccountName(s3AccountName)
                    .setModifyTime(stamp)
                    .setCifsUserId(cifsUserId);
            if (!config.isModify()) {
                config.setStartTime(stamp);
            }
            return config;
        } catch (Exception e) {
            log.info("buildQuotaConfig error: ", e);
            throw new MsException(INVALID_ARGUMENT, "the quota config input is invalid.");
        }
    }

    public static int checkInputIdAndGet(String id) {
        if (StringUtils.isBlank(id) || id.matches("0+") && id.length() > 1) {
            throw new MsException(INVALID_ARGUMENT, "the quota id_ input is invalid.");
        }
        int res;
        try {
            res = Integer.parseInt(id);
            if (res < 0) {
                throw new MsException(INVALID_ARGUMENT, "the quota id_ input is invalid.");
            }
            return res;
        } catch (Exception e) {
            throw new MsException(INVALID_ARGUMENT, "the quota id_ input is invalid.");
        }
    }

    public static void checkSoftAndHardLimit(long soft, long hard) {
        if (hard > 0 && soft >= hard) {
            throw new MsException(INVALID_ARGUMENT, "the soft quota input is invalid.");
        }
    }

    public static boolean checkLimitValue(int hardLimit, int softLimit) {
        return hardLimit == 0 || softLimit < hardLimit;
    }

    /**
     * 检查目录配额是否已经设置，或者正在设置（即正在扫描目录文件还没设置成功）
     *
     * @param fsQuotaConfig  目录配额信息
     * @param quotaBucketKey ""
     * @param quotaTypeKey   ""
     */
    public static void checkQuotaConfigSet(FSQuotaConfig fsQuotaConfig, String quotaBucketKey, String quotaTypeKey, Map<String, String> bucketInfo) {
        String recoverKey = pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).get(quotaTypeKey);
        //上锁，防止同时设置而同时扫描
        if (!tryLock(quotaTypeKey) || StringUtils.isNotBlank(recoverKey)) {
            log.error("checkQuotaConfigSet, quota is setting, bucket: {}, dirName: {}", fsQuotaConfig.getBucket(), fsQuotaConfig.getDirName());
            throw new MsException(DIR_NAME_IS_SETTING, "dir is setting.");
        }
        String dirQuotaInfo = pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hget(quotaBucketKey, quotaTypeKey);
        //已存在相同的配额，不允许覆盖设置，只允许修改
        if (StringUtils.isNotBlank(dirQuotaInfo) && !fsQuotaConfig.isModify()) {
            log.error("setFsDirQuotaInfo, dir has been setting, bucket: {}, dirName: {}", fsQuotaConfig.getBucket(), fsQuotaConfig.getDirName());
            throw new MsException(DIR_NAME_HAS_SET, "dir has set quota info.");
        }
        // 不存在则不允许修改
        if (StringUtils.isBlank(dirQuotaInfo) && fsQuotaConfig.isModify()) {
            log.error("dir quota info does not exits,can not be modify. bucket: {}, dirName: {}", fsQuotaConfig.getBucket(), fsQuotaConfig.getDirName());
            throw new MsException(QUOTA_INFO_NOT_EXITS, "dir quota info does not exits,can not be modify.");
        }
        // 检查目录是否正在设置其他配额类型
        if (!fsQuotaConfig.isModify()) {
            List<String> dirNameList = getDirNameList(fsQuotaConfig.getDirName());
            if (!dirNameList.isEmpty()) {
                List<String> quotaKeys = getAllTmpQuotaKey(fsQuotaConfig, dirNameList);
                if (!quotaKeys.isEmpty()) {
                    if (pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).exists(quotaKeys.toArray(new String[0])) >= 1) {
                        log.error("dir or parent dir  is setting other type quota info. bucket: {}, dirName: {}", fsQuotaConfig.getBucket(), fsQuotaConfig.getDirName());
                        throw new MsException(DIR_NAME_IS_SETTING, "dir or parent dir  is setting other type quota info.");
                    }
                }
            }

        }

        if (fsQuotaConfig.getFilesSoftQuota() == 0
                && fsQuotaConfig.getFilesHardQuota() == 0
                && fsQuotaConfig.getCapacityHardQuota() == 0
                && fsQuotaConfig.getCapacitySoftQuota() == 0) {
            throw new MsException(INVALID_ARGUMENT, "the quota value input is invalid.");
        }

        checkQuotaSetCountAndLimit(fsQuotaConfig, bucketInfo);

    }

    public static List<String> getAllTmpQuotaKey(FSQuotaConfig fsQuotaConfig, List<String> dirNameList) {
        List<String> quotaKeys = new ArrayList<>();
        for (String dirName : dirNameList) {
            if (SLASH.equals(dirName)) {
                continue;
            }
            QUOTA_CONFIG_CACHE.compute(fsQuotaConfig.bucket, (k1, v) -> {
                if (v != null) {
                    v.forEach((k2, v2) -> {
                        if (v2.getDirName().equals(dirName)) {
                            String quotaDirKey = getTmpCapAndFilesKey(fsQuotaConfig.getBucket(), v2.getNodeId(), v2.getQuotaType(), getIdByQuotaType(v2));
                            quotaKeys.add(quotaDirKey);
                        }
                    });
                }
                return v;
            });
        }
        return quotaKeys;
    }

    /**
     * 检查配额设置数量，并检查软硬配额设置是否合法
     *
     * @param fsQuotaConfig fsQuotaConfig
     * @param bucketInfo    bucketInfo
     */
    public static void checkQuotaSetCountAndLimit(FSQuotaConfig fsQuotaConfig, Map<String, String> bucketInfo) {
        String prefix = "";
        int maxCount = 0;
        switch (fsQuotaConfig.getQuotaType()) {
            case FS_DIR_QUOTA:
                prefix = FS_DIR_QUOTA_PREFIX;
                maxCount = MAX_DIR_QUOTA_COUNT;
                break;
            case FS_USER_QUOTA:
                prefix = FS_USER_QUOTA_PREFIX;
                maxCount = MAX_USER_QUOTA_COUNT;
                break;
            case FS_GROUP_QUOTA:
                prefix = FS_GROUP_QUOTA_PREFIX;
                maxCount = MAX_GROUP_QUOTA_COUNT;
                break;
        }
        List<String> keys = pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hkeys(getQuotaBucketKey(fsQuotaConfig.getBucket()));
        if (!fsQuotaConfig.isModify()) {
            int count = 0;
            for (String key : keys) {
                if (key.startsWith(prefix)) {
                    count++;
                }
            }
            if (count >= maxCount) {
                log.error("checkQuotaConfigSet,  quota is too much, bucket: {}, dirName: {}", fsQuotaConfig.getBucket(), fsQuotaConfig.getDirName());
                throw new MsException(QUOTA_TOO_MUCH, "dir quota is too much.");
            }
        }

        long bucketObjHardQuota = Long.parseLong(bucketInfo.getOrDefault("objnum_value", "0"));
        long bucketCapHardQuota = Long.parseLong(bucketInfo.getOrDefault("quota_value", "0"));
        Map<String, String> userInfo = pool.getShortMasterCommand(REDIS_USERINFO_INDEX).hgetall(fsQuotaConfig.getS3AccountName());
        long accountObjHardQuota = Long.parseLong(userInfo.getOrDefault("account_objnum", "0"));
        long accountCapHardQuota = Long.parseLong(userInfo.getOrDefault("account_quota", "0"));


        //文件配额的硬配额需小于桶配额的硬配额
        compareS3QuotaVal(bucketCapHardQuota, fsQuotaConfig.getCapacityHardQuota(), QUOTA_CAP_EXCEED_EXITS_LIMIT, "exceed exits quota cap hard quota.", fsQuotaConfig);
        compareS3QuotaVal(bucketObjHardQuota, fsQuotaConfig.getFilesHardQuota(), QUOTA_FILE_EXCEED_EXITS_LIMIT, "exceed exits quota file hard quota.", fsQuotaConfig);
        //文件配额的硬配额需小于账户配额的硬配额

        compareS3QuotaVal(accountObjHardQuota, fsQuotaConfig.getFilesHardQuota(), QUOTA_FILE_EXCEED_EXITS_LIMIT, "exceed exits quota file hard quota.", fsQuotaConfig);
        compareS3QuotaVal(accountCapHardQuota, fsQuotaConfig.getCapacityHardQuota(), QUOTA_CAP_EXCEED_EXITS_LIMIT, "exceed exits quota cap hard quota.", fsQuotaConfig);


        for (String key : keys) {
            if (!key.startsWith(QUOTA_PREFIX)) {
                continue;
            }
            String configStr = pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hget(getQuotaBucketKey(fsQuotaConfig.getBucket()), key);
            FSQuotaConfig exitsConfig = Json.decodeValue(configStr, FSQuotaConfig.class);
            String dirName = exitsConfig.getDirName();
            if (isParentDir(dirName, fsQuotaConfig.getDirName()) || isParentDir(fsQuotaConfig.getDirName(), dirName)) {
                boolean isParentDir = dirName.length() < fsQuotaConfig.getDirName().length();
                if (isParentDir && exitsConfig.getQuotaType() == FS_DIR_QUOTA) {
                    //如果当前目录是父目录且设置了目录配额，则需要判断当前目录的硬配额是否大于等于父目录的硬配额
                    compareQuotaVal(exitsConfig, fsQuotaConfig);
                }
                //子目录，设置用户/组配额，如果父目录也存在相同类型的配额，且uid或者gid相同，则需比较硬配额值
                if (isParentDir && fsQuotaConfig.getQuotaType() != FS_DIR_QUOTA
                        && Objects.equals(exitsConfig.getQuotaType(), fsQuotaConfig.getQuotaType())
                ) {
                    if (fsQuotaConfig.getQuotaType() == FS_USER_QUOTA && exitsConfig.getUid() == fsQuotaConfig.getUid()) {
                        compareQuotaVal(exitsConfig, fsQuotaConfig);
                    }
                    if (fsQuotaConfig.getQuotaType() == FS_GROUP_QUOTA && exitsConfig.getGid() == fsQuotaConfig.getGid()) {
                        compareQuotaVal(exitsConfig, fsQuotaConfig);
                    }
                }
                //目录相同，目录配额的限制范围要大于用户配额和用户组配额，所以存在目录配额时，不能设置软硬配额值更大的其他类型配额
                if (fsQuotaConfig.getDirName().equals(dirName)) {
                    if (!fsQuotaConfig.isModify() && ((fsQuotaConfig.getQuotaType() == FS_DIR_QUOTA && exitsConfig.getQuotaType() == FS_GROUP_QUOTA)
                            || (exitsConfig.getQuotaType() == FS_DIR_QUOTA && fsQuotaConfig.getQuotaType() == FS_GROUP_QUOTA))) {
                        throw new MsException(QUOTA_METHOD_CONFLICT, "one dir can not set group quota and dir quota in the same time.");
                    }

                    if (exitsConfig.getQuotaType() == FS_DIR_QUOTA && fsQuotaConfig.getQuotaType() != FS_DIR_QUOTA) {
                        compareQuotaVal(exitsConfig, fsQuotaConfig);
                    }

                    //要设置的目录配额，需比已设置的其他配额的硬配额值要大
                    if (exitsConfig.getQuotaType() != FS_DIR_QUOTA && fsQuotaConfig.getQuotaType() == FS_DIR_QUOTA) {
                        compareQuotaVal(fsQuotaConfig, exitsConfig);
                    }
                }
            }
        }
    }

    public static void compareS3QuotaVal(long s3Val, long fsHardQuotaVal, int errorNo, String errorMsg, FSQuotaConfig fsQuotaConfig) {
        if (s3Val != 0 && s3Val < fsHardQuotaVal) {
            log.error("exceed exits quota file hard quota.curDir:{},bucket:{}", fsQuotaConfig.getDirName(), fsQuotaConfig.getBucket());
            throw new MsException(errorNo, errorMsg);
        }
    }

    public static void compareQuotaVal(FSQuotaConfig existConfig, FSQuotaConfig fsQuotaConfig) {
        if (existConfig.getCapacityHardQuota() != 0 && existConfig.getCapacityHardQuota() < fsQuotaConfig.getCapacityHardQuota()) {
            log.error("exceed exits quota cap hard quota.curDir:{},exitsDir:{}", fsQuotaConfig.getDirName(), existConfig.getDirName());
            throw new MsException(QUOTA_CAP_EXCEED_EXITS_LIMIT, "exceed exits quota cap hard quota.");
        }

        if (existConfig.getFilesHardQuota() != 0 && existConfig.getFilesHardQuota() < fsQuotaConfig.getFilesHardQuota()) {
            log.error("exceed exits quota file hard quota.curDir:{},exitsDir:{}", fsQuotaConfig.getDirName(), existConfig.getDirName());
            throw new MsException(QUOTA_FILE_EXCEED_EXITS_LIMIT, "exceed exits quota file hard quota.");
        }
    }

    /**
     * 获取锁
     *
     * @param quotaDirKey
     * @return
     */
    public static boolean tryLock(String quotaDirKey) {
        SetArgs setArgs = new SetArgs().nx().ex(QUOTA_SET_INTERVAL);
        String lockKey = "lock_" + quotaDirKey;
        return "OK".equals(pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).set(lockKey, "1", setArgs));
    }

    public static void tryUnLock(String key) {
        if (StringUtils.isBlank(key)) {
            return;
        }
        String lockKey = "lock_" + key;
        pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).del(lockKey);
    }


    public static String getQuotaDirKey(String bucket, long dirNodeId) {
        return FS_DIR_QUOTA_PREFIX + bucket + "_" + dirNodeId;
    }

    public static String getQuotaUserKey(String bucket, long dirNodeId, int uid) {
        return FS_USER_QUOTA_PREFIX + bucket + "_" + dirNodeId + "/" + uid;
    }

    public static String getQuotaGroupKey(String bucket, long dirNodeId, int gid) {
        return FS_GROUP_QUOTA_PREFIX + bucket + "_" + dirNodeId + "/" + gid;
    }

    public static String getQuotaTypeKey(FSQuotaConfig config) {
        switch (config.getQuotaType()) {
            case FS_DIR_QUOTA:
                return getQuotaDirKey(config.bucket, config.nodeId);
            case FS_USER_QUOTA:
                return getQuotaUserKey(config.bucket, config.nodeId, config.getUid());
            case FS_GROUP_QUOTA:
                return getQuotaGroupKey(config.bucket, config.nodeId, config.getGid());
        }
        return "";
    }

    public static List<String> getAllTypeQuotaKeys(String bucket, long dirNodeId) {
        List<String> keys = new ArrayList<>();
        QUOTA_CONFIG_CACHE.computeIfPresent(bucket, (k, v) -> {
            v.forEach((k2, v2) -> {
                if (v2.getNodeId() == dirNodeId) {
                    keys.add(k2);
                }
            });
            return v;
        });
        return keys;
    }

    public static String getQuotaTypeKey(String bucket, long dirNodeId, int quotaType, int id) {
        switch (quotaType) {
            case FS_DIR_QUOTA:
                return getQuotaDirKey(bucket, dirNodeId);
            case FS_USER_QUOTA:
                return getQuotaUserKey(bucket, dirNodeId, id);
            case FS_GROUP_QUOTA:
                return getQuotaGroupKey(bucket, dirNodeId, id);
        }
        return "";
    }

    public static String getQuotaBucketKey(String bucket) {
        return bucket + QUOTA_SUFFIX;
    }

    public static String getQuotaTaskKey(String bucket, long dirNodeId, int quotaType) {
        return "scan_" + bucket + "_" + dirNodeId + "_" + quotaType;
    }

    /**
     * 目录配额告警key
     */
    public static String getQuotaAlarmKey(String bucket, long dirNodeId, String prefix, String prefix2) {
        return prefix + prefix2 + bucket + "_" + dirNodeId;
    }

    public static String getQuotaAlarmKey(String bucket, long dirNodeId, String prefix1, String prefix2, int id) {
        if (FS_QUOTA_DIR_ALARM_PREFIX.equals(prefix2)) {
            return getQuotaAlarmKey(bucket, dirNodeId, prefix1, prefix2);
        }
        return prefix1 + prefix2 + bucket + "_" + dirNodeId + "/" + id;
    }

    public static Tuple2<String, String> getCapAndFilesAlarmKey(long dirNodeId, String bucketName, int quotaType, int id) {
        String prefix2 = FS_QUOTA_DIR_ALARM_PREFIX;
        if (quotaType == FS_GROUP_QUOTA) {
            prefix2 = FS_QUOTA_GROUP_ALARM_PREFIX;
        }
        if (quotaType == FS_USER_QUOTA) {
            prefix2 = FS_QUOTA_USR_ALARM_PREFIX;
        }
        String capAlarmKey = getQuotaAlarmKey(bucketName, dirNodeId, FS_QUOTA_CAP_ALARM_PREFIX, prefix2, id);
        String filesAlarmKey = getQuotaAlarmKey(bucketName, dirNodeId, FS_QUOTA_FILES_ALARM_PREFIX, prefix2, id);
        return new Tuple2<>(capAlarmKey, filesAlarmKey);
    }

    public static String getDirNameFromKey(String bucket, String quotaDirKey, String prefix, int endIndex) {
        return quotaDirKey.substring(prefix.length() + bucket.length() + 1, endIndex);
    }

    public static int getIdFromKey(String key) {
        return Integer.parseInt(key.substring(key.lastIndexOf("/") + 1));
    }

    public static String getTmpCapAndFilesKey(String bucket, long dirNodeId, int type, int id0) {
        return bucket + "_" + dirNodeId + "_" + type + "_" + id0 + "_tmp_cap_files";
    }

    public static String getDelLockKey(String bucket, String dirName, int type) {
        return "del_lock_" + bucket + "_" + dirName + "/" + type;
    }


    /**
     * 获取目录名称列表（dirName自己以及所有上层目录）
     *
     * @param dirName
     * @return
     */
    public static List<String> getDirNameList(String dirName) {
        List<String> dirs = new LinkedList<>();
        if (StringUtils.isBlank(dirName)) {
            dirs.add("/");
            return dirs;
        }
        String[] dirArray = dirName.split("/");
        StringBuilder sb = new StringBuilder();
        for (String dir : dirArray) {
            if (StringUtils.isNotBlank(dir)) {
                sb.append(dir).append("/");
                dirs.add(sb.toString());
            }
        }
        return dirs;
    }

    public static String getQuotaKeys(String bucket, String objName, long stamp, int uid, int gid) {
        Tuple2<Boolean, List<String>> tuple2 = existQuotaInfo(bucket, objName, stamp, uid, gid).block();
        if (tuple2 != null && tuple2.var1) {
            return Json.encode(tuple2.var2);
        } else {
            return "";
        }
    }

    public static Mono<Tuple2<Boolean, List<String>>> existQuotaInfoS3(String bucket, String objName, long stamp, String s3UserId) {
        return existQuotaInfo(bucket, objName, stamp, 0, 0);
    }

    public static Mono<Tuple2<Boolean, List<String>>> existQuotaInfo(String bucket, String objName, long stamp, int uid, int gid) {
        return existQuotaInfoFromCache(bucket, objName, stamp, Collections.singletonList(uid), Collections.singletonList(gid));
    }

    public static Mono<Tuple2<Boolean, List<String>>> existQuotaInfoWithCifsUsr(String bucket, String objName, long stamp, int uid, int gid, String cifsUser) {
        return existQuotaInfoFromCache(bucket, objName, stamp, Collections.singletonList(uid), Collections.singletonList(gid), cifsUser);
    }

    public static Mono<Tuple2<Boolean, List<String>>> existQuotaInfoListUserOrGroup(String bucket, String objName, long stamp, List<Integer> uidList, List<Integer> gidList) {
        return existQuotaInfoFromCache(bucket, objName, stamp, uidList, gidList);
    }

    public static Mono<Boolean> isNotScanComplete(List<String> quotaTypeKeys) {
        if (quotaTypeKeys == null || quotaTypeKeys.isEmpty()) {
            return Mono.just(false);
        }
        return pool.getReactive(REDIS_FS_QUOTA_INFO_INDEX)
                .exists(quotaTypeKeys.stream().map(quotaTypeKey -> quotaTypeKey.substring(quotaTypeKey.indexOf("_") + 1)).toArray(String[]::new))
                .map(a -> a >= 1L);
    }

    public static Mono<Tuple2<Boolean, List<String>>> existQuotaInfoHasScanComplete(String bucket, String objName, long stamp, List<Integer> uids, List<Integer> gids) {
        return FSQuotaUtils.existQuotaInfoFromCache(bucket, objName, System.currentTimeMillis(), uids, gids)
                .flatMap(t2 -> {
                    if (t2.var1) {
                        return existQuotaInfoHasScanComplete0(bucket, objName, stamp, uids, gids, t2);
                    }
                    return Mono.just(t2);
                });
    }

    /**
     * 判断设置配额的目录是否已经扫描完成。
     * 对于有容量或者文件数减少的情况，需判断目录是否扫描完成；
     * 在设置配额2分钟之后，对文件或者对象继续操作，此时会自动更新配额的容量和文件信息，删除操作则会减去响应的数值，
     * 但是如果此时是还未扫描完成的状态，就记录了减去的值，而当目录扫描完成之后，会对比两次扫描的结果，同样会减去这一部分值，即会导致多减了；
     * 所以在此时需要判断目录是否扫描完成。
     *
     * @param bucket  桶名
     * @param objName 对象名
     * @param stamp   时间戳
     * @param uids    uids
     * @param gids    gids
     * @param t2      上一次查询结果
     * @return 最终结果
     */
    public static Mono<Tuple2<Boolean, List<String>>> existQuotaInfoHasScanComplete0(String bucket, String objName, long stamp, List<Integer> uids, List<Integer> gids, Tuple2<Boolean, List<String>> t2) {
        return isNotScanComplete(t2.var2)
                .flatMap(res -> {
                    if (res) {
                        return existQuotaInfoFromCache(bucket, objName, stamp, uids, gids)
                                .flatMap(r -> {
                                    if (!r.var1) {
                                        //此时需返回空数组，防止恢复情况，又在缓存中去查询
                                        r.var2 = new LinkedList<>();
                                        r.var1 = true;
                                    }
                                    return Mono.just(r);
                                });
                    }
                    return Mono.just(t2);
                });
    }


    public static void selectQuota(String key, String dirName, long stamp, FSQuotaConfig fsQuotaConfig, List<Integer> uidList, List<Integer> gidList, List<String> keys, String... cifsUser) {
        if (key.startsWith(FS_DIR_QUOTA_PREFIX)) {
            String dir = fsQuotaConfig.getDirName();
            if (isParentDir(dirName, dir) && (fsQuotaConfig.getStartTime() + QUOTA_UPDATE_DIR_EXPAND_TIME) <= stamp) {
                keys.add(getCapInfoUpdateKey(fsQuotaConfig.getStartTime(), key));
                FSQuotaRealService.fsAddQuotaCheck(fsQuotaConfig.bucket, key);
            }
        } else if (key.startsWith(FS_USER_QUOTA_PREFIX)) {

            String dir = fsQuotaConfig.getDirName();
            int _uid = getIdFromKey(key);
            boolean bindCifs = false;
            if (cifsUser.length > 0 && StringUtils.isNotBlank(cifsUser[0])) {
                bindCifs = cifsUser[0].equals(fsQuotaConfig.getCifsUserId());
            }
            if (isParentDir(dirName, dir) && (uidList.contains(_uid) || bindCifs) && (fsQuotaConfig.getStartTime() + QUOTA_UPDATE_DIR_EXPAND_TIME) <= stamp) {
                keys.add(getCapInfoUpdateKey(fsQuotaConfig.getStartTime(), key));
                FSQuotaRealService.fsAddQuotaCheck(fsQuotaConfig.bucket, key);
            }
        } else if (key.startsWith(FS_GROUP_QUOTA_PREFIX)) {
            String dir = fsQuotaConfig.getDirName();
            int _gid = getIdFromKey(key);
            if (isParentDir(dirName, dir) && gidList.contains(_gid) && (fsQuotaConfig.getStartTime() + QUOTA_UPDATE_DIR_EXPAND_TIME) <= stamp) {
                keys.add(getCapInfoUpdateKey(fsQuotaConfig.getStartTime(), key));
                FSQuotaRealService.fsAddQuotaCheck(fsQuotaConfig.bucket, key);
            }
        }
    }

    /**
     * 判断是否已经设置配额信息
     * 返回要更新容量或者文件数的key列表
     *
     * @param bucket  bucket
     * @param objName obj
     * @return Tuple2<Boolean, List < String>>
     */
    public static Mono<Tuple2<Boolean, List<String>>> existQuotaInfoFromCache(String bucket, String objName, long stamp, List<Integer> uidList, List<Integer> gidList, String... cifsUser) {
        Tuple2<Boolean, List<String>> res = new Tuple2<>(false, null);
        String dirName = CifsUtils.getParentDirName(objName) + "/";
        if ("/".equals(dirName)) {
            return Mono.just(new Tuple2<>(false, null));
        }
        QUOTA_CONFIG_CACHE.compute(bucket, (k, v) -> {
            if (v != null) {
                List<String> keys = new LinkedList<>();
                for (String key : v.keySet()) {
                    if (!key.startsWith(QUOTA_PREFIX)) {
                        continue;
                    }
                    FSQuotaConfig fsQuotaConfig = v.get(key);
                    if (fsQuotaConfig != null) {
                        selectQuota(key, dirName, stamp, fsQuotaConfig, uidList, gidList, keys, cifsUser);
                    }
                }
                if (!keys.isEmpty()) {
                    res.var1 = true;
                    res.var2 = keys;
                }
            }
            return v;
        });
        return Mono.just(res);
    }

    public static void quotaConfigRename(String bucket, long dirNodeId, String newDirName) {
        QUOTA_CONFIG_CACHE.compute(bucket, (k, v) -> {
            if (v != null) {
                v.forEach((k1, v1) -> {
                    if (v1.getNodeId() == dirNodeId) {
                        v1.setDirName(newDirName);
                        v.put(k1, v1);
                    }
                });
            }
            return v;
        });
    }

    /**
     * 判断配额目录是否是上层目录
     *
     * @param dirName      需校验的目录
     * @param quotaDirName 配额目录
     * @return
     */
    public static boolean isParentDir(String dirName, String quotaDirName) {
        if (dirName.equals(quotaDirName)) {
            return true;
        }
        String parentDirName = CifsUtils.getParentDirName(dirName) + "/";
        while (!"/".equals(parentDirName)) {
            if (parentDirName.equals(quotaDirName)) {
                return true;
            }
            parentDirName = CifsUtils.getParentDirName(parentDirName) + "/";
        }
        return false;
    }

    public static String getCapInfoUpdateKey(long startTime, String str) {
        return startTime + "_" + str;
    }

    public static Mono<Boolean> canWrite(String bucket, List<String> quotaKeys) {
        return canOpt0(getAlarmKeyFromQuotaKeys(quotaKeys, true), getQuotaBucketKey(bucket));
    }

    public static Mono<Boolean> fsCheckS3Quota(String bucket, boolean isWrite) {
        return pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucket)
                .flatMap(bucketInfo -> pool.getReactive(REDIS_USERINFO_INDEX).hgetall(bucketInfo.get("user_name")).zipWith(Mono.just(bucketInfo)))
                .flatMap(bucketInfoAndAccountInfo -> {
                    Map<String, String> bucketInfo = bucketInfoAndAccountInfo.getT2();
                    Map<String, String> accountInfo = bucketInfoAndAccountInfo.getT1();
                    String accountQuotaFlag = accountInfo.get(USER_DATABASE_ACCOUNT_QUOTA_FLAG);
                    String accountObjNumFlag = accountInfo.get(USER_DATABASE_ACCOUNT_OBJNUM_FLAG);
                    if (isWrite) {
                        if ("1".equals(bucketInfo.get(QUOTA_FLAG))) {
                            throw new NFSException(NFS3ERR_DQUOT, "No Enough Space Because of the bucket quota.");
                        } else if ("2".equals(accountQuotaFlag)) {
                            throw new NFSException(NFS3ERR_DQUOT, "No Enough Space Because of the account quota.");
                        }
                    } else {
                        if ("1".equals(bucketInfo.get(OBJNUM_FLAG))) {
                            throw new NFSException(NFS3ERR_DQUOT, "The bucket hard-max-objects was exceeded.");
                        } else if ("2".equals(accountObjNumFlag)) {
                            throw new NFSException(NFS3ERR_DQUOT, "The account hard-max-objects was exceeded.");
                        }
                    }

                    return Mono.just(true);
                });
    }

    public static Mono<Boolean> fsCanWrite(String bucket, Tuple2<Boolean, List<String>> tuple2) {
        return Mono.just(tuple2.var1).flatMap(b -> {
            if (!b) {
                return Mono.just(true);
            }
            return canWrite(bucket, tuple2.var2);
        });
    }

    public static Mono<Boolean> canCreate0(String bucket, List<String> quotaKeys) {
        return canOpt0(getAlarmKeyFromQuotaKeys(quotaKeys, false), getQuotaBucketKey(bucket));
    }

    public static Mono<Boolean> S3CanCreate(String bucket, String objName, String s3UserId) {
        return existQuotaInfo(bucket, objName, System.currentTimeMillis(), 0, 0).flatMap(t2 -> {
            if (t2.var1) {
                return canCreate0(bucket, t2.var2);
            }
            return Mono.just(true);
        }).doOnNext(create -> {
            if (!create) {
                throw new MsException(NO_ENOUGH_OBJECTS, "The dir hard-max-objects was exceeded.");
            }
        });
    }

    public static Mono<Boolean> S3CanWrite(String bucket, List<String> quotaKeys) {
        return canOpt0(getAlarmKeyFromQuotaKeys(quotaKeys, true), getQuotaBucketKey(bucket)).doOnNext(write -> {
            if (!write) {
                throw new MsException(NO_ENOUGH_SPACE, "The dir hard-max-capacity was exceeded.");
            }
        });
    }

    public static Mono<Boolean> canOpt0(List<String> alarmKeys, String quotaBucketKey) {
        log.debug("alarmKeys:{}", alarmKeys);
        return Flux.fromIterable(alarmKeys).flatMap(alarmKey -> {
            return pool.getReactive(REDIS_FS_QUOTA_INFO_INDEX).hget(quotaBucketKey, alarmKey)
                    .defaultIfEmpty(NOT_EXCEED_QUOTA)
                    .flatMap(alarmStatus -> {
                        if (EXCEED_QUOTA_HARD_LIMIT.equals(alarmStatus)) {
                            return Mono.just(false);
                        }
                        return Mono.just(true);
                    });
        }, FS_ALARM_CHECK_CONCURRENCY).collectList().map(l -> !l.contains(false));
    }

    public static Mono<Boolean> checkS3Put(String bucketName, List<String> quotaKeys) {
        List<String> alarmCapKeys = getAlarmKeyFromQuotaKeys(quotaKeys, true);
        List<String> alarmFilesKeys = getAlarmKeyFromQuotaKeys(quotaKeys, false);
        String quotaBucketKey = getQuotaBucketKey(bucketName);
        return canOpt0(alarmFilesKeys, quotaBucketKey).flatMap(create -> {
            if (!create) {
                throw new MsException(NO_ENOUGH_OBJECTS, "The dir hard-max-objects was exceeded.");
            }
            return canOpt0(alarmCapKeys, quotaBucketKey).flatMap(write -> {
                if (!write) {
                    throw new MsException(NO_ENOUGH_SPACE, "The dir hard-max-capacity was exceeded.");
                }
                return Mono.just(true);
            });
        });
    }

    public static boolean updateInodeSetQuotaDirSize(String dirListStr, Inode inode, Inode oldInode, long oldSize) {
        if (StringUtils.isNotBlank(dirListStr)) {
            List<String> keys = Json.decodeValue(dirListStr, new TypeReference<List<String>>() {
            });
            for (String key : keys) {
                int index = key.indexOf("_");
                String stamp = key.substring(0, index);
                String realKey = key.substring(index + 1);
                if (oldInode == null || oldInode.getQuotaStartStampAndSize() == null) {
                    if (inode.getQuotaStartStampAndSize() == null) {
                        inode.setQuotaStartStampAndSize(new HashMap<>());
                    }
                    inode.getQuotaStartStampAndSize().put(realKey, stamp + "-" + oldSize);
                } else {
                    String oldVal = oldInode.getQuotaStartStampAndSize().get(realKey);
                    inode.setQuotaStartStampAndSize(oldInode.getQuotaStartStampAndSize());
                    if (StringUtils.isBlank(oldVal)) {
                        inode.getQuotaStartStampAndSize().put(realKey, stamp + "-" + oldSize);
                    } else {
                        String oldValStamp = oldVal.split("-")[0];
                        if (oldValStamp.compareTo(stamp) < 0) {
                            inode.getQuotaStartStampAndSize().put(realKey, stamp + "-" + oldSize);
                        } else {
                            inode.getQuotaStartStampAndSize().put(realKey, oldVal);
                        }
                    }
                }
            }
            return true;
        } else {
            if (oldInode != null && oldInode.getQuotaStartStampAndSize() != null && !oldInode.getQuotaStartStampAndSize().isEmpty()) {
                inode.setQuotaStartStampAndSize(oldInode.getQuotaStartStampAndSize());
                return true;
            }
        }
        return false;
    }

    public static void checkDirInfo(DirInfo dirInfo) {
        if (dirInfo == null) {
            throw new MsException(ErrorNo.DIR_NAME_NOT_FOUND, "No such dir");
        }
        if (DirInfo.ERROR_DIR_INFO.getFlag().equals(dirInfo.getFlag())) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "get error");
        }
    }

    public static int safeToInt(long value) {
        if (value > Integer.MAX_VALUE) return Integer.MAX_VALUE;
        return (int) value;
    }

    public static int safeToInt(double value) {
        return safeToInt((long) value);
    }

    public static Mono<String> checkDelFrequency(String delLockKey, String dirName) {
        return pool.getReactive(REDIS_FS_QUOTA_INFO_INDEX).exists(delLockKey).flatMap(exist -> {
            if (exist == 1) {
                return Mono.just(dirName + "-");
            }
            return Mono.just(dirName);
        });
    }

    public static void checkQuotaLimit(String bucket, long curVal, String alarmKey, long softLimit, long hardLimit) {
        if (softLimit == 0 && hardLimit == 0) {
            return;
        }
        if (tryLock(alarmKey + "_lock")) {
            try {
                String quotaBucketKey = getQuotaBucketKey(bucket);
                String alarmFlag = pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hget(quotaBucketKey, alarmKey);
                String lastStatus = StringUtils.isBlank(alarmFlag) ? NOT_EXCEED_QUOTA : alarmFlag;
                String lastAlarmKey = alarmKey + LAST_ALARM_SUFFIX;
                String quotaBucketKeyLast = quotaBucketKey + LAST_ALARM_SUFFIX;
                log.debug("checkQuotaLimit,bucket:{},info:{},alarmKey:{},softLimit:{},hardLimit:{},alarmFlag:{}", bucket, curVal, alarmKey, softLimit, hardLimit, alarmFlag);
                if (!NOT_EXCEED_QUOTA.equals(alarmFlag)) {
                    if (hardLimit > 0 && curVal >= hardLimit) {
                        pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hset(quotaBucketKey, alarmKey, EXCEED_QUOTA_HARD_LIMIT);
                        alarmFlag = EXCEED_QUOTA_HARD_LIMIT;
                    }
                    if (curVal < hardLimit) {
                        pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hset(quotaBucketKey, alarmKey, NOT_EXCEED_QUOTA);
                        alarmFlag = NOT_EXCEED_QUOTA;
                    }
                }

                if (!EXCEED_QUOTA_HARD_LIMIT.equals(alarmFlag)) {
                    if (hardLimit > 0 && curVal >= hardLimit) {
                        pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hset(quotaBucketKey, alarmKey, EXCEED_QUOTA_HARD_LIMIT);
                        alarmFlag = EXCEED_QUOTA_HARD_LIMIT;
                    }
                    if (softLimit > 0 && curVal >= softLimit
                            && (hardLimit == 0 || curVal < hardLimit)
                            && !EXCEED_QUOTA_SOFT_LIMIT.equals(alarmFlag)) {
                        pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hset(quotaBucketKey, alarmKey, EXCEED_QUOTA_SOFT_LIMIT);
                        alarmFlag = EXCEED_QUOTA_SOFT_LIMIT;
                    }
                    if (curVal < softLimit && EXCEED_QUOTA_SOFT_LIMIT.equals(alarmFlag)) {
                        pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hset(quotaBucketKey, alarmKey, NOT_EXCEED_QUOTA);
                        alarmFlag = NOT_EXCEED_QUOTA;
                    }
                }
                //软配额为0，超过硬配额80% ,触发软配额告警
                if (softLimit == 0 && hardLimit > 0 && curVal >= (hardLimit * 0.8) && !EXCEED_QUOTA_HARD_LIMIT.equals(alarmFlag) && !EXCEED_QUOTA_SOFT_LIMIT.equals(alarmFlag)) {
                    pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hset(quotaBucketKey, alarmKey, EXCEED_QUOTA_SOFT_LIMIT);
                    alarmFlag = EXCEED_QUOTA_SOFT_LIMIT;
                }
                log.debug("checkQuotaLimit,bucket:{},info:{},alarmKey:{},softLimit:{},hardLimit:{},alarmFlag:{},lastStatus:{}", bucket, curVal, alarmKey, softLimit, hardLimit, alarmFlag, lastStatus);
                if (!lastStatus.equals(alarmFlag)) {
                    pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hset(quotaBucketKeyLast, lastAlarmKey, lastStatus);
                }
            } finally {
                tryUnLock(alarmKey + "_lock");
            }
        }
    }

    public static void deleteAlarmKey(String quotaTypeKey, String bucket) {
        String quotaBucketKey = getQuotaBucketKey(bucket);
        String quotaBucketLastAlarmKey = quotaBucketKey + LAST_ALARM_SUFFIX;
        String filesAlarmKey = "alarm_files_" + quotaTypeKey;
        String capAlarmKey = "alarm_cap_" + quotaTypeKey;
        String filesAlarmKeyLast = filesAlarmKey + LAST_ALARM_SUFFIX;
        String capAlarmKeyLast = capAlarmKey + LAST_ALARM_SUFFIX;
        pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hdel(quotaBucketKey, filesAlarmKey, capAlarmKey);
        pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hdel(quotaBucketLastAlarmKey, filesAlarmKeyLast, capAlarmKeyLast);
    }

    public static Mono<Boolean> s3AddQuotaDirInfo(String bucketName, String objName, String stamp, List<String> updateQuotaDir, String s3UserId) {
        return FSQuotaUtils.existQuotaInfo(bucketName, objName, Long.parseLong(stamp), 0, 0).flatMap(tuple2 -> {
            if (tuple2.var1) {
                updateQuotaDir.addAll(tuple2.var2);
                return FSQuotaUtils.checkS3Put(bucketName, tuple2.var2);
            }
            return Mono.just(false);
        });
    }

    public static Mono<Inode> addQuotaDirInfo(Inode inode, long stamp, boolean isWrite) {
        return fsCheckS3Quota(inode.getBucket(), isWrite).flatMap(res -> {
            return existQuotaInfo(inode.getBucket(), inode.getObjName(), stamp, inode.getUid(), inode.getGid()).flatMap(tuple2 -> {
                if (tuple2.var1) {
                    return canOpt0(getAlarmKeyFromQuotaKeys(tuple2.var2, isWrite), getQuotaBucketKey(inode.getBucket())).map(l -> {
                        if (!l) {
                            if (isWrite) {
                                return CAP_QUOTA_EXCCED_INODE;
                            }
                            return FILES_QUOTA_EXCCED_INODE;
                        }
                        inode.getXAttrMap().put(QUOTA_KEY, Json.encode(tuple2.var2));
                        return inode;
                    });
                }
                return Mono.just(inode);
            });
        });

    }

    public static List<String> getAlarmKeyFromQuotaKeys(List<String> quotaKeys, boolean isWrite) {
        return quotaKeys.stream().map(key -> {
            if (isWrite) {
                return FS_QUOTA_CAP_ALARM_PREFIX0 + key.substring(key.indexOf("_") + 1);
            }
            return FS_QUOTA_FILES_ALARM_PREFIX0 + key.substring(key.indexOf("_") + 1);
        }).collect(Collectors.toList());
    }

    public static void delQuotas(String bucket, String dirName) {
        List<FSQuotaConfig> fsQuotaConfigDeleteList = new ArrayList<>();
        QUOTA_CONFIG_CACHE.computeIfPresent(bucket, (k, v) -> {
            v.forEach((k1, v1) -> {
                if (StringUtils.isBlank(dirName) || v1.getDirName().equals(dirName)) {
                    fsQuotaConfigDeleteList.add(v1);
                }
            });
            return v;
        });
        if (!fsQuotaConfigDeleteList.isEmpty()) {
            Flux.fromIterable(fsQuotaConfigDeleteList)
                    .flatMap(config -> {
                        MonoProcessor<Integer> delRes = MonoProcessor.create();
                        delQuota0(config, bucket, delRes);
                        return delRes;
                    }, 4)
                    .collectList()
                    .timeout(Duration.ofSeconds(60))
                    .subscribe();
        }
    }

    public static void delQuota0(FSQuotaConfig config, String bucket, MonoProcessor<Integer> delRes) {
        int id = getIdByQuotaType(config);
        delFsDirQuotaInfo(bucket, config.getNodeId(), config.getDirName(), config.getQuotaType(), id, delRes, config.getS3AccountName(), true);
    }

    /**
     * 重命名文件时，如果正在设置配额信息 ，则不允许进行重命名
     *
     * @param bucket
     * @param dirNodeId
     * @param inode
     * @return
     */
    public static Mono<Boolean> canRename(String bucket, long dirNodeId, Inode inode) {
        List<String> allTypeQuotaKeys = FSQuotaUtils.getAllTypeQuotaKeys(bucket, dirNodeId);
        if (allTypeQuotaKeys.isEmpty()) {
            return Mono.just(true);
        }
        return RedisConnPool.getInstance().getReactive(REDIS_FS_QUOTA_INFO_INDEX)
                .exists(allTypeQuotaKeys.toArray(new String[0]))
                .map(l -> l <= 0);
    }

    public static void checkQuotaTypeAndId(String quotaType0, String id0, int[] quotaType, int[] id_) {
        try {
            if (StringUtils.isBlank(quotaType0) || !quotaType0.matches("0|[1-9]\\d*")) {
                throw new MsException(INVALID_ARGUMENT, "the argument quota type  is invalid.");
            }
            quotaType[0] = Integer.parseInt(quotaType0);
        } catch (Exception e) {
            throw new MsException(INVALID_ARGUMENT, "the argument quota type  is invalid.");
        }
        if (!FS_QUOTA_TYPE_SET.contains(quotaType[0])) {
            throw new MsException(INVALID_ARGUMENT, "the quotaType input is invalid.");
        }
        if (quotaType[0] == FS_USER_QUOTA || quotaType[0] == FS_GROUP_QUOTA) {
            try {
                if (StringUtils.isBlank(id0) || !id0.matches("0|[1-9]\\d*")) {
                    throw new MsException(INVALID_ARGUMENT, "the argument quota type  is invalid.");
                }
                id_[0] = Integer.parseInt(id0);
            } catch (Exception e) {
                throw new MsException(INVALID_ARGUMENT, "the argument id input is invalid.");
            }
            if (id_[0] < 0) {
                throw new MsException(INVALID_ARGUMENT, "the argument id  input is invalid.");
            }
        }
    }

    public static int getIdByQuotaType(FSQuotaConfig config) {
        if (config.getQuotaType() == null) {
            return -1;
        }
        switch (config.getQuotaType()) {
            case FSQuotaConstants.FS_DIR_QUOTA:
                return 0;
            case FSQuotaConstants.FS_USER_QUOTA:
                return config.getUid();
            case FSQuotaConstants.FS_GROUP_QUOTA:
                return config.getGid();
            default:
                return -1;
        }
    }

    public static void addQuotaInfoToMetaData(MetaData metaData, String quotaStr) {
        Inode inode = new Inode();
        if (metaData.isAvailable()) {
            if (StringUtils.isNotBlank(metaData.tmpInodeStr)) {
                inode = Json.decodeValue(metaData.tmpInodeStr, Inode.class);
            } else {
                inode = Inode.defaultInode(metaData);
            }
        } else {
            inode.setSize(1L);
        }
        List<String> updateCapKeys = Json.decodeValue(quotaStr, new TypeReference<List<String>>() {
        });
        inode.getXAttrMap().put(QUOTA_KEY, Json.encode(updateCapKeys));
        inode.getInodeData().clear();
        metaData.tmpInodeStr = Json.encode(inode);
    }

    public static void addQuotaInfoToS3Inode(Inode inode) {
        Tuple2<Boolean, List<String>> quotaKey = getQuotaInfoByInode(inode);
        if (quotaKey != null && quotaKey.var1) {
            inode.getXAttrMap().put(QUOTA_KEY_CREATE_S3_INODE, Json.encode(quotaKey.var2));
            if (StringUtils.isBlank(inode.getXAttrMap().get(CREATE_S3_INODE_TIME))) {
                inode.getXAttrMap().put(CREATE_S3_INODE_TIME, String.valueOf(System.currentTimeMillis()));
            }
            List<String> newKeys = new ArrayList<>();
            for (String key : quotaKey.var2) {
                String realKey = key.substring(key.indexOf("_") + 1);
                if (!realKey.startsWith(FS_DIR_QUOTA_PREFIX)) {
                    int id = getIdFromKey(realKey);
                    if (id > 0) {
                        newKeys.add(key);
                    }
                }
            }
            updateInodeSetQuotaDirSize(Json.encode(newKeys), inode, null, 0);
        }
    }

    public static void addQuotaInfoToInode(Inode inode, String objName) {
        String quotaKeys = FSQuotaUtils.getQuotaKeys(inode.getBucket(), objName, System.currentTimeMillis(), inode.getUid(), inode.getGid());
        inode.getXAttrMap().put(QUOTA_KEY, quotaKeys);
    }

    public static Tuple2<Boolean, List<String>> getQuotaInfoByInode(Inode inode) {
        List<Integer> uids = new ArrayList<>();
        uids.add(inode.getUid());
        uids.add(0);
        List<Integer> gids = new ArrayList<>();
        gids.add(inode.getGid());
        gids.add(0);
        return FSQuotaUtils.existQuotaInfoFromCache(inode.getBucket(), inode.getObjName(), System.currentTimeMillis(), uids, gids).block();
    }

    public static Mono<reactor.util.function.Tuple2<FSQuotaConfig, Long>> findFinalMinFsQuotaConfig(String bucketName, long inodeId) {
        // 获取当前目录的 Quota 配置
        Mono<reactor.util.function.Tuple2<FSQuotaConfig, Long>> currentConfigMono = FSQuotaRealService.getFsQuotaConfig(bucketName, inodeId, FS_DIR_QUOTA, 0)
                .onErrorResume(e -> Mono.empty()) // 发生错误视作无配置
                .filter(config -> StringUtils.isNotBlank(config.getBucket()) && config.getCapacityHardQuota() > 0)
                .map(config -> Tuples.of(config, inodeId));

        return currentConfigMono.switchIfEmpty(Mono.defer(() -> {
            Mono<reactor.util.function.Tuple2<FSQuotaConfig, Long>> bucketQuotaMono = FSQuotaRealService
                    .getFsQuotaConfig(bucketName, 1L, FS_DIR_QUOTA, 0)
                    .onErrorResume(e -> Mono.empty())
                    .filter(config -> StringUtils.isNotBlank(config.getBucket()) && config.getCapacityHardQuota() > 0)
                    .map(config -> Tuples.of(config, 1L));

            return Flux.concat(bucketQuotaMono, findMinimumDirQuota(bucketName, inodeId))
                    .reduce((t1, t2) -> {
                        long quota1 = t1.getT1().getCapacityHardQuota();
                        long quota2 = t2.getT1().getCapacityHardQuota();
                        // 返回配额较小的那个 Tuple
                        return quota1 <= quota2 ? t1 : t2;
                    });
        }));

    }

    /**
     * 获取父目录的 Inode，用于 expand 的步进
     */
    private static Mono<Inode> findParentInode(Inode currentInode, String bucketName) {
        // 终止条件：无效节点或已到达根目录
        if (currentInode == null || InodeUtils.isError(currentInode) || currentInode.getNodeId() <= 0
                || StringUtils.isEmpty(currentInode.getObjName()) || "/".equals(currentInode.getObjName())) {
            return Mono.empty();
        }

        // 根据当前路径计算出父路径
        Path parentDir = Paths.get(currentInode.getObjName()).getParent();
        if (parentDir == null || parentDir.toString().equals(currentInode.getObjName())) {
            return Mono.empty();
        }

        return FsUtils.lookup(bucketName, parentDir.toString(), null, false, false, -1, null);
    }

    private static Flux<reactor.util.function.Tuple2<FSQuotaConfig, Long>> findMinimumDirQuota(String bucketName, long startDirInodeId) {
        // 首先获取起始目录的 Inode
        return com.macrosan.filesystem.cache.Node.getInstance().getInode(bucketName, startDirInodeId)
                .expand(currentInode -> {
                    // 循环查找当前目录的父目录
                    return findParentInode(currentInode, bucketName);
                })
                .flatMap(inode -> {
                    // 对流中的每一个目录尝试获取配置
                    return FSQuotaRealService.getFsQuotaConfig(bucketName, inode.getNodeId(), FS_DIR_QUOTA, 0)
                            .onErrorResume(e -> Mono.empty())
                            .filter(config -> StringUtils.isNotBlank(config.getBucket()) && config.getCapacityHardQuota() > 0)
                            .map(config -> Tuples.of(config, inode.getNodeId()));
                });
    }

    public static Tuple2<Long, Long> getFirstPartInfo(String lun) {
        long capacity = 0L;
        long usedSize = 0L;
        try {
            FileStore store = Files.getFileStore(Paths.get("/").resolve(lun));

            capacity = store.getTotalSpace();      // 总空间
            usedSize = capacity - store.getUnallocatedSpace(); // 已用空间
        } catch (IOException ioe) {
            //  log.error("cannot get first part info", ioe);
        } catch (Exception e) {
            //  log.error(e.getMessage());
        }

        if (lun.contains("cache")) {
            usedSize = 0L;
            capacity = 0L;
        }

        return new Tuple2<>(capacity, usedSize);
    }

    public static void getQuotaKeyByMetadata(MetaData metaData, MetaData oldMetadata, String[] updateCapKeys, WriteBatch writeBatch, RocksDB db, String vnode) throws RocksDBException {
        try {
            if (metaData.inode > 0 || (oldMetadata != null && oldMetadata.inode > 0)) {

                long inodeId = metaData.inode == 0 ? oldMetadata.inode : metaData.inode;
                String inodeKey = getKey(vnode, metaData.getBucket(), inodeId);
                byte[] curInodeBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, inodeKey.getBytes());
                if (curInodeBytes != null) {
                    Inode curInode = Json.decodeValue(new String(curInodeBytes), Inode.class);
                    updateCapKeys[0] = FSQuotaUtils.getQuotaKeys(curInode.getBucket(), metaData.key, System.currentTimeMillis(), curInode.getUid(), curInode.getGid());
                } else {
                    updateCapKeys[0] = FSQuotaUtils.getQuotaKeys(metaData.getBucket(), metaData.key, System.currentTimeMillis(), -1, -1);
                }
            } else {
                updateCapKeys[0] = FSQuotaUtils.getQuotaKeys(metaData.getBucket(), metaData.key, System.currentTimeMillis(), 0, 0);
            }
        } catch (RocksDBException e) {
            throw e;
        } catch (Exception e) {
            log.info("", e);
        }

    }

    public static Mono<Inode> checkFsQuota(Inode inode) {
        return FSQuotaUtils.addQuotaDirInfo(inode, System.currentTimeMillis(), true)
                .flatMap(i -> {
                    if (i.getLinkN() == CAP_QUOTA_EXCCED_INODE.getLinkN()) {
                        throw new NFSException(NFS3ERR_DQUOT, "can not write ,because of exceed quota.bucket:" + inode.getBucket() + ",objName:" + inode.getObjName() + ",nodeId:" + inode.getNodeId());
                    }
                    return Mono.just(i);
                });
    }

    private static final long QUOTA_LOG_INTERVAL_MS = Duration.ofMinutes(1).toMillis();
    private static final long QUOTA_LOG_TTL_MS = Duration.ofMinutes(10).toMillis();
    private static final long QUOTA_LOG_CLEANUP_INTERVAL_MS = Duration.ofMinutes(5).toMillis();
    private static final Map<String, QuotaLogThrottle> QUOTA_LOG_THROTTLE_MAP = new ConcurrentHashMap<>();
    private static final AtomicLong QUOTA_LOG_LAST_CLEANUP_TIME = new AtomicLong();

    private static class QuotaLogThrottle {
        private volatile long lastLogTime;
        private volatile long lastAccessTime;
        private final AtomicLong suppressedCount = new AtomicLong();
    }

    public static void logQuotaExceeded(Inode inode, Throwable e) {
        long currentTime = System.currentTimeMillis();
        cleanupExpiredQuotaLogThrottle(currentTime);
        String bucket = inode == null ? "" : inode.getBucket();
        long nodeId = inode == null ? -1L : inode.getNodeId();
        String object = inode == null ? "" : inode.getObjName();
        String logKey = bucket + ":" + nodeId;
        QuotaLogThrottle throttle = QUOTA_LOG_THROTTLE_MAP.computeIfAbsent(logKey, k -> new QuotaLogThrottle());
        throttle.lastAccessTime = currentTime;
        long lastLogTime = throttle.lastLogTime;
        if (lastLogTime == 0 || currentTime - lastLogTime >= QUOTA_LOG_INTERVAL_MS) {
            synchronized (throttle) {
                lastLogTime = throttle.lastLogTime;
                if (lastLogTime == 0 || currentTime - lastLogTime >= QUOTA_LOG_INTERVAL_MS) {
                    long suppressedCount = throttle.suppressedCount.getAndSet(0);
                    throttle.lastLogTime = currentTime;
                    if (suppressedCount > 0) {
                        log.info("NFS quota exceeded. bucket:{}, obj:{}, nodeId:{}, msg:{} (suppressed {} repeated logs)",
                                bucket, object, nodeId, e.getMessage(), suppressedCount);
                    } else {
                        log.info("NFS quota exceeded. bucket:{}, obj:{}, nodeId:{}, msg:{}",
                                bucket, object, nodeId, e.getMessage());
                    }
                    return;
                }
            }
        }
        throttle.suppressedCount.incrementAndGet();
    }

    private static void cleanupExpiredQuotaLogThrottle(long currentTime) {
        long lastCleanupTime = QUOTA_LOG_LAST_CLEANUP_TIME.get();
        if (currentTime - lastCleanupTime < QUOTA_LOG_CLEANUP_INTERVAL_MS) {
            return;
        }
        if (!QUOTA_LOG_LAST_CLEANUP_TIME.compareAndSet(lastCleanupTime, currentTime)) {
            return;
        }
        QUOTA_LOG_THROTTLE_MAP.entrySet().removeIf(entry ->
                currentTime - entry.getValue().lastAccessTime >= QUOTA_LOG_TTL_MS);
    }
}
