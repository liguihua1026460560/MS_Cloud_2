package com.macrosan.filesystem.utils;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.HeartBeatChecker;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.macrosan.constants.AccountConstants.DEFAULT_USER_ID;
import static com.macrosan.constants.ErrorNo.INVALID_ARGUMENT;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.SWITCH_ON;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.SWITCH_SUSPEND;
import static com.macrosan.filesystem.FsConstants.BUCKET_CASE_SENSITIVE;
import static com.macrosan.filesystem.FsConstants.FSConfig.FTP_ANONYMOUS_SWITCH;
import static com.macrosan.filesystem.FsConstants.SMB_MAX_FILE_NAME_LENGTH;
import static com.macrosan.filesystem.async.AsyncUtils.MOUNT_CLUSTER;
import static com.macrosan.filesystem.nfs.NFSBucketInfo.FSID_BUCKET;
import static com.macrosan.filesystem.nfs.NFSBucketInfo.getBucketInfoReactive;
import static com.macrosan.snapshot.SnapshotMarkGenerator.MIN_SNAPSHOT_MARK;

@Slf4j
public class CheckUtils {
    private static RedisConnPool redisConnPool = RedisConnPool.getInstance();

    private static final String INVALID_SMB_FILENAME_REGEX = "[\\\\/*:?\"<>|]+";

    protected static final String SITE = ServerConfig.getInstance().getSite();

    public static Mono<Boolean> writePermissionCheckReactive(String bucket, int opt) {
        return redisConnPool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hget(bucket, "nfsAcl")
                .flatMap(res -> Mono.just("0".equals(res)));
    }

    public static boolean writePermissionCheck(Map<String, String> bucketInfo) {
        if (bucketInfo == null || null == bucketInfo.get("nfsAcl")) {
            return false;
        }

        if ("0".equals(bucketInfo.get("nfsAcl"))) {
            return true;
        } else {
            return false;
        }
    }

    public static Mono<Boolean> cifsWritePermissionCheck(String bucket) {
        return getBucketInfoReactive(bucket)
                .flatMap(bucketInfo -> {
                    // 1是公共读，0是公共读写，旧版本不存在该字段默认公共读写
                    if (bucketInfo == null || "1".equals(bucketInfo.get("cifsAcl"))) {
                        return Mono.just(false);
                    } else {
                        return Mono.just(true);
                    }
                });
    }

    public static boolean cifsWritePermissionCheck(Map<String, String> bucketInfo) {
        // 1是公共读，0是公共读写，旧版本不存在该字段默认公共读写
        if (bucketInfo == null || "1".equals(bucketInfo.get("cifsAcl"))) {
            return false;
        } else {
            return true;
        }
    }

    public static Mono<Boolean> ftpWritePermissionCheck(String bucket, String user) {
        return getBucketInfoReactive(bucket)
                .flatMap(bucketInfo -> {
                    // 1是公共读，0是公共读写，旧版本不存在该字段默认公共读写
                    if (bucketInfo == null || "1".equals(bucketInfo.get("ftpAcl"))) {
                        return Mono.just(false);
                    } else {
                        int acl = Integer.parseInt(bucketInfo.get(BUCKET_ACL));
                        if ("anonymous".equalsIgnoreCase(user) && ((acl & PERMISSION_SHARE_READ_WRITE_NUM) == 0)) {
                            return Mono.just(false);
                        }
                        return Mono.just(true);
                    }
                });
    }

    public static Mono<Boolean> nfsOpenCheck(long fsid, ReqInfo reqHeader) {
        return redisConnPool.getReactive(REDIS_SYSINFO_INDEX)
                .hexists(FSID_BUCKET, String.valueOf(fsid))
                .flatMap(isExist -> {
                    if (!isExist) {
                        return Mono.just(false);
                    }
                    return redisConnPool.getReactive(REDIS_SYSINFO_INDEX).hget(FSID_BUCKET, String.valueOf(fsid))
                            .flatMap(bucket -> redisConnPool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket))
                            .flatMap(bucketInfo -> {
                                String nfs = bucketInfo.get("nfs");
                                return Mono.just("1".equals(nfs) && siteCanAccess(bucketInfo) && FSIPACLUtils.hasMountAccess(bucketInfo, reqHeader.nfsHandler.getClientAddress()));
                            });
                });
    }

    /**
     * 检查当前cifs文件系统，或者当前访问的账户是否还开启与存在，若不存在则返错
     *
     * @param fsid    桶对应的 fsid
     * @param session 请求的session
     **/
    public static Mono<Boolean> cifsOpenCheck(long fsid, Session session, boolean[] caseSensitive) {
        return redisConnPool.getReactive(REDIS_SYSINFO_INDEX)
                .hexists(FSID_BUCKET, String.valueOf(fsid))
                .flatMap(isExist -> {
                    if (isExist) {
                        return redisConnPool.getReactive(REDIS_SYSINFO_INDEX).hget(FSID_BUCKET, String.valueOf(fsid));
                    } else {
                        return Mono.just("");
                    }
                })
                .flatMap(bucketName -> {
                    if (StringUtils.isEmpty(bucketName)) {
                        return Mono.just(false);
                    } else {
                        return NFSBucketInfo.getBucketInfoReactive(bucketName)
                                .flatMap(bucketInfo -> {
                                    if (bucketInfo != null && !bucketInfo.isEmpty()) {
                                        //判断桶有没有设置大小写不敏感，若未设置则仍以全局大小写不敏感开关为准，若设置则已桶级别大小写
                                        //不敏感开关为准
                                        if (bucketInfo.containsKey(BUCKET_CASE_SENSITIVE)) {
                                            //大小写敏感关闭，即大小写不敏感
                                            if ("0".equals(bucketInfo.get(BUCKET_CASE_SENSITIVE))) {
                                                caseSensitive[0] = false;
                                            } else if ("1".equals(bucketInfo.get(BUCKET_CASE_SENSITIVE))) {
                                                caseSensitive[0] = true;
                                            }
                                        }

                                        //1.5.10版本前开启NFS的桶,默认开启了CIFS
                                        return Mono.just(!"0".equals(bucketInfo.get("cifs")) && siteCanAccess(bucketInfo));
                                    }
                                    return Mono.just(false);
                                });
                    }
                })
                .flatMap(isFine -> {
                    String reqS3ID = session.getTreeAccount((int) fsid);
                    if (isFine) {
                        //请求的s3账户为空，不允许访问文件系统
                        if (StringUtils.isBlank(reqS3ID)) {
                            return Mono.just(false);
                        }

                        //请求的s3账户为匿名账户，无需再检查redis
                        if (DEFAULT_USER_ID.equals(reqS3ID)) {
                            return Mono.just(true);
                        }
                        Mono<Boolean> domainMono = Mono.just(true);
                        if (session.domainLogin) {
                            domainMono = redisConnPool.getReactive(REDIS_SYSINFO_INDEX).exists(LDAP_CONFIG_KEY)
                                    .flatMap(exist -> {
                                        if (exist > 0) {
                                            return redisConnPool.getReactive(REDIS_SYSINFO_INDEX).hget(LDAP_CONFIG_KEY, LDAP_DOMAIN_NAME).defaultIfEmpty("")
                                                    .flatMap(domain -> {
                                                        if (domain.equalsIgnoreCase(session.domain)) {
                                                            return Mono.just(true);
                                                        }
                                                        return Mono.just(false);
                                                    });
                                        }
                                        return Mono.just(false);
                                    });
                        }
                        //请求的s3账户为其它账户，需要检查redis中是否还存在该账户，若不存在则不允许访问文件系统
                        Mono<Boolean> finalDomainMono = domainMono;
                        return redisConnPool.getReactive(REDIS_USERINFO_INDEX).exists(reqS3ID)
                                .map(isExist -> isExist > 0).flatMap(b -> {
                                    if (b) {
                                        return finalDomainMono;
                                    }
                                    return Mono.just(b);
                                });
                    }

                    return Mono.just(isFine);
                });
    }

    public static boolean fileLicenseCheckSync() {
        return "on".equals(redisConnPool.getCommand(REDIS_SYSINFO_INDEX).get("file_license"));
    }

    public static Map<String, String> cifsControls = new ConcurrentHashMap<>();

    public static boolean cifsLeaseOpenCheckSync() {
        return "1".equals(redisConnPool.getCommand(REDIS_SYSINFO_INDEX).hget("cifs_controls", "lease"));
    }

    public static Mono<Boolean> cifsLeaseOpenCheck() {
        if (cifsControls.containsKey("lease")) {
            return Mono.just("1".equals(cifsControls.get("lease")));
        } else {
            return redisConnPool.getReactive(REDIS_SYSINFO_INDEX)
                    .hexists("cifs_controls", "lease")
                    .flatMap(isExist -> {
                        if (isExist) {
                            return redisConnPool.getReactive(REDIS_SYSINFO_INDEX)
                                    .hget("cifs_controls", "lease")
                                    .flatMap(s -> {
                                        cifsControls.put("lease", s);
                                        return Mono.just(s);
                                    })
                                    .map(s -> "1".equals(s));
                        } else {
                            cifsControls.put("lease", "0");
                            return Mono.just(false);
                        }
                    });
        }
    }

    public static Mono<Boolean> nfs4DelegateOpenCheck() {
        return redisConnPool.getReactive(REDIS_SYSINFO_INDEX)
                .hexists("nfs4_controls", "delegate")
                .flatMap(isExist -> {
                    if (isExist) {
                        return redisConnPool.getReactive(REDIS_SYSINFO_INDEX)
                                .hget("nfs4_controls", "delegate")
                                .map("1"::equals);
                    } else {
                        return Mono.just(false);
                    }
                });
    }

    /**
     * 0 Never(默认)
     * 1 Bad User
     * 2 Bad Password
     *
     * @return guest
     */
    public static Mono<Integer> cifsGuest() {
        return redisConnPool.getReactive(REDIS_SYSINFO_INDEX)
                .exists("cifs_controls")
                .flatMap(isExist -> {
                    if (isExist > 0) {
                        return redisConnPool.getReactive(REDIS_SYSINFO_INDEX)
                                .hgetall("cifs_controls")
                                .map(map -> {
                                    try {
                                        if (map.containsKey("guest")) {
                                            return Integer.parseInt(map.get("guest"));
                                        }
                                        return 0;
                                    } catch (Exception e) {
                                        log.error("cifs guest error: {}", map);
                                        return 0;
                                    }
                                });
                    } else {
                        return Mono.just(0);
                    }
                });
    }

    public static Mono<Boolean> ftpOpenCheck(String bucket, String user) {
        return redisConnPool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucket)
                .map(bucketInfo -> {
                    if (bucketInfo != null && !bucketInfo.isEmpty()) {
                        String ftp = bucketInfo.get("ftp");
                        int acl = Integer.parseInt(bucketInfo.get(BUCKET_ACL));
                        if (!"anonymous".equalsIgnoreCase(user)) {
                            return "1".equals(ftp) && siteCanAccess(bucketInfo);
                        } else {
                            return "1".equals(ftp) && siteCanAccess(bucketInfo) && ((acl & PERMISSION_SHARE_READ_WRITE_NUM) != 0 || (acl & PERMISSION_SHARE_READ_NUM) != 0);
                        }
                    }
                    return false;
                });
    }

    public static boolean bucketFsCheck(String bucketName) {
        return redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX).hexists(bucketName, "fsid");
    }

    public static void bucketFsCheck(Map<String, String> bucketInfo){
        if (bucketInfo.containsKey("fsid")){
            throw new MsException(ErrorNo.NFS_NOT_STOP, "The bucket already start nfs or cifs");
        }
    }

    public static void validateNfsCanEnabled(String bucketName, Map<String, String> bucketMap) {
        // 若开启了多版本
        if ("Enabled".equals(bucketMap.get("versionstatus"))) {
            throw new MsException(ErrorNo.NFS_CONFLICT, "bucket: " + bucketName + ", already enable bucket version");
        }
        // 是否存在旧的桶清单
        if (isBucketInventory(bucketName)) {
            throw new MsException(ErrorNo.NFS_CONFLICT, "bucket: " + bucketName + ", already enable bucket inventory");
        }
        // 是否存在旧的生命周期配置
        if (isBucketLifecycle(bucketName)) {
            throw new MsException(ErrorNo.NFS_CONFLICT, "bucket: " + bucketName + ", already exist bucket lifecycle");
        }
        // 是否挂载的太多
        if (isMountTooMany()) {
            throw new MsException(ErrorNo.NFS_MOUNT_TOO_MANY, "bucket: " + bucketName + ", already shared more than 1024 buckets");
        }
        // 是否存在桶快照功能
        if ("on".equals(bucketMap.get(SNAPSHOT_SWITCH))) {
            throw new MsException(ErrorNo.NFS_CONFLICT, "bucket: " + bucketName + ", already enable bucket snapshot");
        }
    }

    public static void checkCreateBucketParamWithFs(UnifiedMap<String, String> paramMap, String strategy) {
        // 是否开启NFS
        String bucketName = paramMap.get(BUCKET_NAME);
        String nfsSwitch = paramMap.get(NFS_SWITCH);
        String cifsSwitch = paramMap.get(CIFS_SWITCH);
        String ftpSwitch = paramMap.get(FTP_SWITCH);
        boolean nfsOpen = "on".equals(nfsSwitch);
        boolean cifsOpen = "on".equals(cifsSwitch);
        boolean ftpOpen = "on".equals(ftpSwitch);
        if ((nfsSwitch != null && !"on".equals(nfsSwitch) && !"off".equals(nfsSwitch))
                || (cifsSwitch != null && !"on".equals(cifsSwitch) && !"off".equals(cifsSwitch)
                || (ftpSwitch != null && !"on".equals(ftpSwitch) && !"off".equals(ftpSwitch))
        )) {
            throw new MsException(INVALID_ARGUMENT, "the nfs-switch input is invalid.");
        }

//        if ("on".equals(paramMap.getOrDefault(DATA_SYNC_SWITCH, "off"))) {
//            if (nfsOpen || cifsOpen || ftpOpen) {
//                throw new MsException(NFS_SYNC_CONFLICTS, "File gateway conflicts with bucket sync.");
//            }
//        }

        if (nfsOpen || cifsOpen || ftpOpen) {
            if (!CheckUtils.fileLicenseCheckSync()) {
                throw new MsException(INVALID_ARGUMENT, "file license is not activated.");
            }
            Map<String, String> bucketMap = new UnifiedMap<>();
            String snapshotSwitch = paramMap.getOrDefault(SNAPSHOT_SWITCH, "off");
            if ("on".equals(snapshotSwitch)) {
                bucketMap.put(SNAPSHOT_SWITCH, snapshotSwitch);
                bucketMap.put(CURRENT_SNAPSHOT_MARK, MIN_SNAPSHOT_MARK);
            }
            String objectLock = paramMap.getOrDefault(OBJECT_LOCK_ENABLED, "off");
            if ("on".equals(objectLock)) {
                bucketMap.put(BUCKET_VERSION_STATUS, "Enabled");
            }
            String deduplicate = redisConnPool.getCommand(REDIS_POOL_INDEX).hget(strategy, "deduplicate");
            if ("on".equals(deduplicate)) {
                throw new MsException(ErrorNo.NFS_CONFLICT, "strategy: " + strategy + ", already enable deduplicate");
            }
            CheckUtils.validateNfsCanEnabled(bucketName, bucketMap);
        }
    }

    /**
     * 检查当前开启共享的桶数量是否已经达到限制
     **/
    public static boolean isMountTooMany() {
        boolean isTooMany = false;
        try {
            if (RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hgetall(FSID_BUCKET).size() >= 1024) {
                isTooMany = true;
            }
        } catch (Exception e) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "check shared buckets error");
        }
        return isTooMany;
    }

    /**
     * 检查当前桶所在存储策略是否存在缓存池
     **/
    private static boolean existPoolCache(String strategy) {
        boolean existPoolCache = false;
        try {
            if (redisConnPool.getCommand(REDIS_POOL_INDEX).hexists(strategy, "cache")
                    && !"[]".equals(redisConnPool.getCommand(REDIS_POOL_INDEX).hget(strategy, "cache"))) {
                existPoolCache = true;
            }
        } catch (Exception e) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "check bucket strategy error");
        }
        return existPoolCache;
    }

    /**
     * 检查当前桶是否开启桶清单
     **/
    private static boolean isBucketInventory(String bucket) {
        boolean isBucketInventory = false;
        try {
            if (redisConnPool.getCommand(REDIS_TASKINFO_INDEX).exists(bucket + "_inventory") != 0) {
                isBucketInventory = true;
            }
        } catch (Exception e) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "check bucket inventory error");
        }
        return isBucketInventory;
    }

    /**
     * 检查当前桶是否开启桶加密
     **/
    private static boolean isBucketEncryption(String bucket) {
        boolean isBucketEncryption = false;
        try {
            if (redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX).hexists(bucket, "crypto")) {
                isBucketEncryption = true;
            }
        } catch (Exception e) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "check bucket crypto error");
        }
        return isBucketEncryption;
    }

    /**
     * 检查当前桶是否开启桶回收站
     **/
    private static boolean isBucketTrashDir(String bucket) {
        boolean isBucketTrashDir = false;
        try {
            if (redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX).hexists(bucket, "trashDir")) {
                isBucketTrashDir = true;
            }
        } catch (Exception e) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "check bucket trashDir error");
        }
        return isBucketTrashDir;
    }

    /**
     * 检查当前桶是否存在生命周期
     **/
    private static boolean isBucketLifecycle(String bucket) {
        boolean isBucketLifecycle = false;
        try {
            if (redisConnPool.getCommand(REDIS_SYSINFO_INDEX).exists("bucket_lifecycle_rules") != 0
                    && redisConnPool.getCommand(REDIS_SYSINFO_INDEX).hexists("bucket_lifecycle_rules", bucket)) {
                isBucketLifecycle = true;
            }
        } catch (Exception e) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "check bucket lifecycle_rules error");
        }
        return isBucketLifecycle;
    }

    public static boolean isInValidSMB2FileName(String fileName) {
        Pattern pattern = Pattern.compile(INVALID_SMB_FILENAME_REGEX);
        Matcher matcher = pattern.matcher(fileName);
        return matcher.find();
    }

    public static boolean isFileNameTooLong(String fileName) {
        if (fileName.endsWith("/")) {
            fileName = fileName.substring(0, fileName.length() - 1);
        }
        String name = fileName.substring(fileName.lastIndexOf("/") + 1);
        return name.getBytes(StandardCharsets.UTF_8).length > SMB_MAX_FILE_NAME_LENGTH;
    }

    /**
     * 检查文件接口上传的文件大小是否超过限定的 5T
     *
     * @param offset write上传时文件中数据块的写入位置
     * @param length write上传时的数据块长度
     * @return true 超过限定范围； false 没有超过限定范围
     **/
    public static boolean checkIfOverFlow(long offset, long length) {
        if (offset + length > MAX_UPLOAD_TOTAL_SIZE) {
            return true;
        }

        return false;
    }

    public static boolean hasStartFS(Map<String, String> bucketInfo, boolean[] hasStartFS) {
        if (bucketInfo.containsKey("fsid")) {
            if (HeartBeatChecker.isMultiAliveStarted && !"1".equals(bucketInfo.get(MOUNT_CLUSTER))) {
                // 复制环境的文件如果不是挂在本地站点，跳过
                return false;
            }

            if (hasStartFS != null) {
                hasStartFS[0] = true;
            }
            return true;
        }

        return false;
    }

    /**
     * 文件系统访问前的站点检查
     */
    public static boolean siteCanAccess(Map<String, String> bucketInfo) {
        String site = bucketInfo.get(CLUSTER_NAME);
        String datasync = bucketInfo.get(DATA_SYNC_SWITCH);
        if (SWITCH_ON.equals(datasync) || SWITCH_SUSPEND.equals(datasync)) {
            return true;
        }
        if (StringUtils.isNotEmpty(site) && !SITE.equals(site)) {
            return false;
        }
        return true;
    }

    public static Mono<Boolean> checkFtpAnonymousPrimarySwitch() {
        return RedisConnPool.getInstance().getReactive(REDIS_SYSINFO_INDEX).get(FTP_ANONYMOUS_SWITCH)
                .defaultIfEmpty("true")
                .map(boolStr -> {
                    if (StringUtils.isBlank(boolStr)) {
                        //默认开
                        return true;
                    } else {
                        boolean res = true;
                        try {
                            res = Boolean.parseBoolean(boolStr);
                        } catch (Exception e) {
                            log.error("parse ftp anonymoust switch error", e);
                            res = false;
                        }

                        return res;
                    }
                })
                .onErrorReturn(false);
    }
}
