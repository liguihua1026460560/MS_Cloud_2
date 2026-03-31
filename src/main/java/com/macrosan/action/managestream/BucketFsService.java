package com.macrosan.action.managestream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.NFSIpWhitelist;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.socketmsg.SocketSender;
import com.macrosan.message.socketmsg.StringResMsg;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.List;
import java.util.Map;

import static com.macrosan.constants.ErrorNo.SUCCESS_STATUS;
import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.BUCKET_NAME;
import static com.macrosan.constants.ServerConstants.USER_ID;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DoubleActiveUtil.notifySlaveSite;

/**
 * Bucket 文件系统服务类
 * 提供桶级别的 NFS、CIFS、FTP 等文件系统配置和管理功能
 */
@Log4j2
public class BucketFsService extends BaseService {

    protected static RedisConnPool pool = RedisConnPool.getInstance();

    protected static SocketSender sender = SocketSender.getInstance();

    protected static ServerConfig config = ServerConfig.getInstance();

    protected static final String SITE = config.getSite();

    private static BucketFsService instance = null;

    public static BucketFsService getInstance() {
        if (instance == null) {
            instance = new BucketFsService();
        }
        return instance;
    }
    private BucketFsService() {
        super();
    }
    /**
     * 验证桶权限
     *
     * @param bucketName 桶名称
     * @param userId 用户 ID
     * @return 桶信息 Map
     * @throws MsException 当桶不存在或用户无权限时抛出异常
     */
    private Map<String, String> validateBucketPermission(String bucketName, String userId) {
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        if (bucketInfo == null || bucketInfo.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        if (StringUtils.isBlank(userId) || !userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,
                    "no permission.user " + userId + " can not operate bucket: " + bucketName);
        }
        return bucketInfo;
    }

    /**
     * 解析 NFS IP 白名单列表
     *
     * @param nfsIpWhiteStr NFS IP 白名单 JSON 字符串
     * @return NFS IP 白名单列表，如果输入为空则返回 null
     * @throws MsException 当 JSON 格式无效时抛出异常
     */
    private List<NFSIpWhitelist> parseNfsIpWhitelists(String nfsIpWhiteStr) {
        if (StringUtils.isBlank(nfsIpWhiteStr)) {
            return null;
        }
        try {
            return JSON.parseObject(nfsIpWhiteStr, new TypeReference<List<NFSIpWhitelist>>() {});
        } catch (Exception e) {
            log.error("Failed to parse NFS IP whitelists: {}", nfsIpWhiteStr, e);
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "Invalid NFS IP whitelist format");
        }
    }

    /**
     * 验证状态值有效性
     *
     * @param status 状态值，应为"0"或"1"
     * @param protocol 协议类型名称，用于错误提示
     * @throws MsException 当状态值无效时抛出异常
     */
    private void validateStatusValue(String status, String protocol) {
        if (StringUtils.isNotBlank(status) && !"1".equals(status) && !"0".equals(status)) {
            throw new MsException(ErrorNo.INVALID_FS_STATUS, "Invalid " + protocol + " status value: " + status);
        }
    }

    /**
     * 验证 ACL 状态并检查用户 UID
     *
     * @param status 状态值
     * @param userId 用户 ID
     * @param operation 操作描述，用于错误提示
     * @throws MsException 当 ACL 已启动但用户无 UID 时抛出异常
     */
    private void validateAclStatus(String status, String userId, String operation) {
        if (StringUtils.isNotBlank(status)) {
            boolean nfsAclStart = isAclStarted("nfs_acl_start");
            boolean cifsAclStart = isAclStarted("cifs_acl_start");

            if (nfsAclStart || cifsAclStart) {
                if (pool.getCommand(REDIS_USERINFO_INDEX).hget(userId, "uid") == null) {
                    throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,
                            "no uid.user " + userId + " can not " + operation);
                }
            }
        }
    }

    /**
     * 检查 ACL 是否已启动
     *
     * @param configKey Redis 配置键名
     * @return true 表示已启动，false 表示未启动
     */
    private boolean isAclStarted(String configKey) {
        String aclStartStr = pool.getCommand(REDIS_SYSINFO_INDEX).get(configKey);
        return "1".equals(aclStartStr);
    }

    /**
     * 转换大小写敏感配置值为存储值
     *
     * @param caseSensitive 大小写敏感配置，"enable"或"disable"
     * @return "1"表示启用，"0"表示禁用
     * @throws MsException 当参数值无效时抛出异常
     */
    private String convertCaseSensitiveToValue(String caseSensitive) {
        if (StringUtils.isBlank(caseSensitive) || (!"enable".equals(caseSensitive) && !"disable".equals(caseSensitive))) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "Invalid caseSensitive value: " + caseSensitive + ", must be 'enable' or 'disable'");
        }
        return "enable".equals(caseSensitive) ? "1" : "0";
    }

    /**
     * 通知从站并检查结果
     *
     * @param paramMap 请求参数 Map
     * @param action 操作名称
     * @throws MsException 当同步失败时抛出异常
     */
    private void notifySlaveAndCheckResult(UnifiedMap<String, String> paramMap, String action) {
        int resCode = notifySlaveSite(paramMap, action);
        if (resCode != SUCCESS_STATUS) {
            throw new MsException(resCode, "master sync failed for action: " + action);
        }
    }

    /**
     * 添加 NFS IP 白名单
     *
     * @param paramMap 请求参数 Map，包含 USER_ID、BUCKET_NAME、NFS_IP_WHITELISTS 等
     * @return 响应消息，成功返回空响应
     * @throws MsException 当权限验证失败、参数无效或同步失败时抛出异常
     */
    public ResponseMsg addNfsIpWhitelists(UnifiedMap<String, String> paramMap) {
        log.info("addNfsIpWhitelists {}", paramMap);
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String nfsIpWhiteStr = paramMap.get(NFS_IP_WHITELISTS);

        List<NFSIpWhitelist> nfsIpWhiteLists = parseNfsIpWhitelists(nfsIpWhiteStr);
        Map<String, String> bucketInfo = validateBucketPermission(bucketName, userId);

        DoubleActiveUtil.siteConstraintCheck(bucketInfo, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(new UnifiedMap<>(paramMap), MSG_TYPE_ADD_NFS_IP_WHITELISTS, localCluster, masterCluster)) {
            return new ResponseMsg();
        }

        addNFSIpWhitelists(bucketName, JSON.toJSONString(nfsIpWhiteLists));

        notifySlaveAndCheckResult(paramMap, ACTION_ADD_NFS_IP_WHITELISTS);

        return new ResponseMsg();
    }

    /**
     * 删除 NFS IP 白名单
     *
     * @param paramMap 请求参数 Map，包含 USER_ID、BUCKET_NAME、NFS_IP_WHITELISTS 等
     * @return 响应消息，成功返回空响应
     * @throws MsException 当权限验证失败、参数无效或同步失败时抛出异常
     */
    public ResponseMsg delNfsIpWhitelists(UnifiedMap<String, String> paramMap) {
        log.info("delNfsIpWhitelists {}", paramMap);
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String nfsIpWhiteStr = paramMap.get(NFS_IP_WHITELISTS);

        List<NFSIpWhitelist> nfsIpWhiteLists = parseNfsIpWhitelists(nfsIpWhiteStr);
        Map<String, String> bucketInfo = validateBucketPermission(bucketName, userId);

        DoubleActiveUtil.siteConstraintCheck(bucketInfo, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(new UnifiedMap<>(paramMap), MSG_TYPE_DEL_NFS_IP_WHITELISTS, localCluster, masterCluster)) {
            return new ResponseMsg();
        }

        delNFSIpWhitelists(bucketName, JSON.toJSONString(nfsIpWhiteLists));

        notifySlaveAndCheckResult(paramMap, ACTION_DEL_NFS_IP_WHITELISTS);

        return new ResponseMsg();
    }

    /**
     * 设置桶的 NFS 配置
     *
     * @param paramMap 请求参数 Map，包含 USER_ID、BUCKET_NAME、NFS_ACL、FS_STATUS、FS_SQUASH、ANON_UID、ANON_GID 等
     * @return 响应消息，成功返回空响应
     * @throws MsException 当权限验证失败、状态值无效或同步失败时抛出异常
     */
    public ResponseMsg setBucketNfs(UnifiedMap<String, String> paramMap) {
        log.info("setBucketNfs {}", paramMap);
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String nfsAcl = paramMap.get(NFS_ACL);
        String status = paramMap.getOrDefault(FS_STATUS, "");
        String squash = paramMap.get(FS_SQUASH);
        String anonUid = paramMap.get(ANON_UID);
        String anonGid = paramMap.get(ANON_GID);

        Map<String, String> bucketInfo = validateBucketPermission(bucketName, userId);

        validateStatusValue(status, "NFS");

        DoubleActiveUtil.siteConstraintCheck(bucketInfo, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(new UnifiedMap<>(paramMap), MSG_TYPE_SET_BUCKET_NFS, localCluster, masterCluster)) {
            return new ResponseMsg();
        }

        updateNfs(bucketName, status, nfsAcl, squash, anonUid, anonGid);

        notifySlaveAndCheckResult(paramMap, ACTION_SET_BUCKET_NFS);

        return new ResponseMsg();
    }

    /**
     * 设置桶的 CIFS 配置
     *
     * @param paramMap 请求参数 Map，包含 USER_ID、BUCKET_NAME、CIFS_ACL、GUEST、FS_STATUS、CASE_SENSITIVE 等
     * @return 响应消息，成功返回空响应
     * @throws MsException 当权限验证失败、ACL 状态检查不通过、大小写敏感参数无效或同步失败时抛出异常
     */
    public ResponseMsg setBucketCifs(UnifiedMap<String, String> paramMap) {
        log.info("setCifsAcl {}", paramMap);
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String acl = paramMap.get(CIFS_ACL);
        String guest = paramMap.get(GUEST);
        String status = paramMap.getOrDefault(FS_STATUS, "");
        String caseSensitive = paramMap.get(CASE_SENSITIVE);

        Map<String, String> bucketInfo = validateBucketPermission(bucketName, userId);

        validateAclStatus(status, userId, "set bucket cifs");

        String saveRes = convertCaseSensitiveToValue(caseSensitive);

        DoubleActiveUtil.siteConstraintCheck(bucketInfo, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(new UnifiedMap<>(paramMap), MSG_TYPE_SET_BUCKET_CIFS, localCluster, masterCluster)) {
            return new ResponseMsg();
        }

        if (StringUtils.isNotBlank(status)) {
            validateStatusValue(status, "CIFS");
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, "cifs", status);
        }
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, CIFS_ACL, acl);
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, GUEST, guest);
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, CASE_SENSITIVE, saveRes);

        notifySlaveAndCheckResult(paramMap, ACTION_SET_BUCKET_CIFS);

        return new ResponseMsg();
    }

    /**
     * 设置桶的 FTP 配置
     *
     * @param paramMap 请求参数 Map，包含 USER_ID、BUCKET_NAME、FTP_ACL、FS_STATUS、ANONYMOUS 等
     * @return 响应消息，成功返回空响应
     * @throws MsException 当权限验证失败、ACL 状态检查不通过、状态值无效或同步失败时抛出异常
     */
    public ResponseMsg setBucketFtp(UnifiedMap<String, String> paramMap) {
        log.info("setFtpAcl {}", paramMap);
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String acl = paramMap.get(FTP_ACL);
        String status = paramMap.getOrDefault(FS_STATUS, "");
        String anonymous = paramMap.get(ANONYMOUS);

        Map<String, String> bucketInfo = validateBucketPermission(bucketName, userId);

        validateAclStatus(status, userId, "set bucket ftp");

        DoubleActiveUtil.siteConstraintCheck(bucketInfo, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(new UnifiedMap<>(paramMap), MSG_TYPE_SET_BUCKET_FTP, localCluster, masterCluster)) {
            return new ResponseMsg();
        }

        if (StringUtils.isNotBlank(status)) {
            validateStatusValue(status, "FTP");
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, "ftp", status);
        }
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, "ftpAcl", acl);
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, "ftp_anonymous", anonymous);

        notifySlaveAndCheckResult(paramMap, ACTION_SET_BUCKET_FTP);

        return new ResponseMsg();
    }

    /**
     * 更新 NFS 配置
     *
     * @param bucketName 桶名称
     * @param status 状态值
     * @param acl NFS 访问控制列表
     * @param squash 压缩配置
     * @param anonUid 匿名用户 UID
     * @param anonGid 匿名用户 GID
     * @throws MsException 当更新失败时抛出异常
     */
    public static void updateNfs(String bucketName, String status, String acl, String squash, String anonUid, String anonGid) {
        SocketReqMsg msg = new SocketReqMsg("updateNFS", 0)
                .put("bucketName", bucketName)
                .put("status", status)
                .put("nfsAcl", acl)
                .put("squash", squash)
                .put("anonUid", anonUid)
                .put("anonGid", anonGid);
        StringResMsg addBackSqlRes = SocketSender.getInstance().sendAndGetResponse(msg, StringResMsg.class, true);
        int code = addBackSqlRes.getCode();

        if (code != SUCCESS_STATUS) {
            throw new MsException(UNKNOWN_ERROR, "start bucket:" + bucketName + " nfs error!");
        }
    }

    /**
     * 添加 NFS IP 白名单到系统
     *
     * @param bucketName 桶名称
     * @param nfsIpWhitelists NFS IP 白名单 JSON 字符串
     * @throws MsException 当添加失败时抛出异常
     */
    public static void addNFSIpWhitelists(String bucketName, String nfsIpWhitelists) {
        SocketReqMsg msg = new SocketReqMsg("addNFSIpWhitelists", 0)
                .put("bucketName", bucketName)
                .put("nfsIpWhitelists", nfsIpWhitelists);

        StringResMsg addBackSqlRes = SocketSender.getInstance().sendAndGetResponse(msg, StringResMsg.class, true);
        int code = addBackSqlRes.getCode();

        if (code != SUCCESS_STATUS) {
            log.error("addNFSIpWhitelists error:{}", addBackSqlRes.getData());
            throw new MsException(UNKNOWN_ERROR, "add bucket:" + bucketName + " nfs ip white list error!");
        }
    }

    /**
     * 从系统删除 NFS IP 白名单
     *
     * @param bucketName 桶名称
     * @param nfsIpWhitelists NFS IP 白名单 JSON 字符串
     * @throws MsException 当删除失败时抛出异常
     */
    public static void delNFSIpWhitelists(String bucketName, String nfsIpWhitelists) {
        SocketReqMsg msg = new SocketReqMsg("delNFSIpWhitelists", 0)
                .put("bucketName", bucketName)
                .put("nfsIpWhitelists", nfsIpWhitelists);

        StringResMsg addBackSqlRes = SocketSender.getInstance().sendAndGetResponse(msg, StringResMsg.class, true);
        int code = addBackSqlRes.getCode();

        if (code != SUCCESS_STATUS) {
            log.error("delNFSIpWhitelists error:{}", addBackSqlRes.getData());
            throw new MsException(UNKNOWN_ERROR, "delete bucket:" + bucketName + " nfs ip white list error!");
        }
    }

}
