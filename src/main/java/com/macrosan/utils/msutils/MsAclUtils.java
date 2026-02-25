package com.macrosan.utils.msutils;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.message.jsonmsg.FSIdentity;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import reactor.core.publisher.Mono;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.macrosan.constants.AccountConstants.DEFAULT_MGT_USER_ID;
import static com.macrosan.constants.AccountConstants.DEFAULT_USER_ID;
import static com.macrosan.constants.ErrorNo.INVALID_MD5;
import static com.macrosan.constants.ErrorNo.NO_SUCH_OBJECT_WRITE_PERMISSION;
import static com.macrosan.constants.ServerConstants.BODY;
import static com.macrosan.constants.ServerConstants.GRANT_ACL;
import static com.macrosan.constants.SysConstants.*;

/**
 * describe:
 *
 * @author chengyinfeng
 * @date 2018/12/19
 */
public class MsAclUtils {
    private static final Logger logger = LogManager.getLogger(MsAclUtils.class.getName());
    private static RedisConnPool pool = RedisConnPool.getInstance();

    private MsAclUtils() {
    }


    public static void checkObjWriteAcl(JsonObject aclJson, String bucketUserId, String userId, boolean checkBucketAcl) {
        if ((aclJson == null || aclJson.isEmpty()) && checkBucketAcl) {
            throw new IllegalStateException(NO_SUCH_OBJECT_WRITE_PERMISSION + "");
        }

        int aclNum = Integer.parseInt(aclJson.getString("acl"));
        if (!userId.equals(aclJson.getString("owner"))
                && ((aclNum & OBJECT_PERMISSION_SHARE_READ_WRITE_NUM) == 0)
                && (!userId.equals(bucketUserId))) {
            throw new IllegalStateException(NO_SUCH_OBJECT_WRITE_PERMISSION + "");
        }
    }

    public static void checkObjWriteAcl(SocketReqMsg msg, MetaData objMeta) {
        JsonObject aclJson = new JsonObject(objMeta.getObjectAcl());
        //桶所有者id
        String bucketUserId = msg.get("bucket_userId");
        //请求发起者id
        String userId = msg.get("userId");
        boolean checkBucketAcl = msg.dataMap.containsKey("bucket_acl");
        checkObjWriteAcl(aclJson, bucketUserId, userId, checkBucketAcl);
    }

    public static void checkObjectReadAcl(SocketReqMsg msg, MetaData objMeta) {
        Map objAclMap = Json.decodeValue(objMeta.getObjectAcl(), Map.class);
        //桶所有者id
        String bucketUserId = msg.get("bucketUserId");
        //请求发起者id
        String userId = msg.get("userId");
        checkObjectReadAcl(objAclMap, bucketUserId, userId);
    }

    public static void checkFsObjectReadAcl(SocketReqMsg msg, MetaData objMeta) {
        Map objAclMap = Json.decodeValue(objMeta.getObjectAcl(), Map.class);
        //桶所有者id
        String bucketUserId = msg.get("bucketUserId");
        //请求发起者id
        String[] userId = {msg.get("userId")};

        try {
            boolean needTransfer = ACLUtils.NFS_ACL_START || ACLUtils.CIFS_ACL_START;
            if (needTransfer && objMeta.inode > 0 && null != objMeta.tmpInodeStr) {
                Inode inode = Json.decodeValue(objMeta.tmpInodeStr, Inode.class);
                if (!ACLUtils.checkNewInode(inode)) {
                    //发起请求的moss账户已经配置uid，且发起请求的账户不是匿名账户；检查发起请求的账户与被请求的inode是否
                    //存在uid或者s3Id的相同，如果有则允许访问
                    if (ACLUtils.userInfo.containsKey(userId) && !DEFAULT_USER_ID.equals(userId) && ACLUtils.userInfo.get(userId).getUid() != 0) {
                        int inoUid = inode.getUid();
                        String inoS3Id = (String) objAclMap.get("owner");
                        FSIdentity identity = ACLUtils.userInfo.get(userId);
                        int reqUid = identity.getUid();
                        String reqS3Id = identity.getS3Id();
                        if (inoUid == reqUid || reqS3Id.equals(inoS3Id)) {
                            userId[0] = inoS3Id;
                        }
                    }
                }
            }
        } catch (Exception e) {
            if (e instanceof MsException) {
                throw e;
            } else {
                logger.error("judge Meta FS ACL error, key: {}", objMeta.getKey(), e);
            }
        }

        checkObjectReadAcl(objAclMap, bucketUserId, userId[0]);
    }

    /**
     * 校验开启文件协议后的对象是否满足READ权限
     *
     * @param objectAclInfo 对象ACL信息
     * @param userId        用户id
     */
    public static void checkObjectReadAcl(Map<String, String> objectAclInfo, String bucketUserId, String userId) {
        int objectAclNum = Integer.parseInt(objectAclInfo.get("acl"));
        String objectUserId = objectAclInfo.get("owner");

        if (objectUserId.equals(userId)) {
            return;
        }
        if ((objectAclNum & OBJECT_PERMISSION_SHARE_READ_NUM) != 0 || (objectAclNum & OBJECT_PERMISSION_SHARE_READ_WRITE_NUM) != 0) {
            return;
        }
        if (((objectAclNum & OBJECT_PERMISSION_SHARE_BUCKET_OWNER_READ_NUM) != 0 || (objectAclNum & OBJECT_PERMISSION_SHARE_OWNER_FULL_CONTROL_NUM) != 0)
                && bucketUserId.equals(userId)) {
            return;
        } else if ((objectAclNum & OBJECT_PERMISSION_READ_NUM) != 0 || (objectAclNum & OBJECT_PERMISSION_FULL_CON_NUM) != 0) {
            checkObjectGrantAcl(objectAclInfo, userId, OBJECT_PERMISSION_READ_NUM);
        } else {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such object permission.");
        }
    }

    /**
     * 校验开启文件协议后的对象是否满足READ权限
     *
     * @param objectAclInfo 对象ACL信息
     * @param reqUserId        用户id
     */
    public static void checkFsObjectReadAcl(Map<String, String> objectAclInfo, String bucketUserId, String reqUserId, MetaData objMeta) {
        String[] userId = {reqUserId};
        try {
            boolean needTransfer = ACLUtils.NFS_ACL_START || ACLUtils.CIFS_ACL_START;
            if (needTransfer && objMeta.inode > 0 && null != objMeta.tmpInodeStr) {
                Inode inode = Json.decodeValue(objMeta.tmpInodeStr, Inode.class);
                if (!ACLUtils.checkNewInode(inode)) {
                    //发起请求的moss账户已经配置uid，且发起请求的账户不是匿名账户；检查发起请求的账户与被请求的inode是否
                    //存在uid或者s3Id的相同，如果有则允许访问
                    if (ACLUtils.userInfo.containsKey(reqUserId) && !DEFAULT_USER_ID.equals(reqUserId) && ACLUtils.userInfo.get(reqUserId).getUid() != 0) {
                        int inoUid = inode.getUid();
                        String inoS3Id = objectAclInfo.get("owner");
                        FSIdentity identity = ACLUtils.userInfo.get(reqUserId);
                        int reqUid = identity.getUid();
                        String reqS3Id = identity.getS3Id();
                        if (inoUid == reqUid || reqS3Id.equals(inoS3Id)) {
                            userId[0] = inoS3Id;
                        }
                    }
                }
            }
        } catch (Exception e) {
            if (e instanceof MsException) {
                throw e;
            } else {
                logger.error("judge Meta FS ACL error, key: {}", objMeta.getKey(), e);
            }
        }


        int objectAclNum = Integer.parseInt(objectAclInfo.get("acl"));
        String objectUserId = objectAclInfo.get("owner");

        if (objectUserId.equals(userId[0])) {
            return;
        }
        if ((objectAclNum & OBJECT_PERMISSION_SHARE_READ_NUM) != 0 || (objectAclNum & OBJECT_PERMISSION_SHARE_READ_WRITE_NUM) != 0) {
            return;
        }
        if (((objectAclNum & OBJECT_PERMISSION_SHARE_BUCKET_OWNER_READ_NUM) != 0 || (objectAclNum & OBJECT_PERMISSION_SHARE_OWNER_FULL_CONTROL_NUM) != 0)
                && bucketUserId.equals(userId[0])) {
            return;
        } else if ((objectAclNum & OBJECT_PERMISSION_READ_NUM) != 0 || (objectAclNum & OBJECT_PERMISSION_FULL_CON_NUM) != 0) {
            checkObjectGrantAcl(objectAclInfo, userId[0], OBJECT_PERMISSION_READ_NUM);
        } else {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such object permission.");
        }
    }

    /**
     * 校验桶是否满足READ权限
     *
     * @param bucketInfo 桶信息
     * @param userId     用户id
     * @param bucketName 桶名
     */
    public static void checkReadAcl(Map<String, String> bucketInfo, String userId, String bucketName) {
        int aclNum = Integer.parseInt(bucketInfo.get(BUCKET_ACL));
        String bucketUserId = bucketInfo.get(BUCKET_USER_ID);

        if (!bucketUserId.equals(userId) &&
                (aclNum & PERMISSION_SHARE_READ_NUM) == 0 &&
                (aclNum & PERMISSION_SHARE_READ_WRITE_NUM) == 0) {
            if ((aclNum & PERMISSION_READ_NUM) != 0 || (aclNum & PERMISSION_FULL_CON_NUM) != 0) {
                checkGrantAcl(aclNum, bucketUserId, userId, bucketName, PERMISSION_READ_NUM, PERMISSION_READ);
            } else {
                throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such bucket permission.");
            }
        }
    }

    /**
     * 校验桶是否满足WRITE权限
     *
     * @param bucketInfo 桶信息
     * @param userId     用户id
     * @param bucketName 桶名
     */
    public static void checkWriteAcl(Map<String, String> bucketInfo, String userId, String bucketName) {
        //异步复制跳过。mossserver-2956
        int aclNum = Integer.parseInt(bucketInfo.get(BUCKET_ACL));
        String bucketUserId = bucketInfo.get(BUCKET_USER_ID);

        if (!bucketUserId.equals(userId) && (aclNum & PERMISSION_SHARE_READ_WRITE_NUM) == 0) {
            if ((aclNum & PERMISSION_WRITE_NUM) != 0 || (aclNum & PERMISSION_FULL_CON_NUM) != 0) {
                checkGrantAcl(aclNum, bucketUserId, userId, bucketName, PERMISSION_WRITE_NUM, PERMISSION_WRITE);
            } else {
                throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such bucket permission.");
            }
        }
    }

    private static void checkObjWriteAcl(Map<String, String> bucketInfo, String userId, Map<String, Object> aclMap) {
        String bucketUserId = bucketInfo.get(BUCKET_USER_ID);
        int aclNum = Integer.parseInt(aclMap.get("acl").toString());
        if (!userId.equals(aclMap.get("owner"))
                && ((aclNum & OBJECT_PERMISSION_SHARE_READ_WRITE_NUM) == 0)
                && (!userId.equals(bucketUserId))) {
            throw new IllegalStateException(NO_SUCH_OBJECT_WRITE_PERMISSION + "");
        }
    }

    /**
     * 判断bucket权限是否满足相应type操作
     *
     * @param bucketAcl    桶的acl数
     * @param bucketUserId 桶的用户id
     * @param userId       用户id
     * @param bucketName   桶名
     */
    public static void checkGrantAcl(int bucketAcl, String bucketUserId,
                                     String userId, String bucketName, int grantTypeNum, String grantType) {
        if (bucketUserId.equals(userId)) {
            return;
        }

        if ((bucketAcl & grantTypeNum) != 0) {
            String grantListType = bucketName + "_" + grantType;
            if (pool.getCommand(REDIS_BUCKETINFO_INDEX).sismember(grantListType, userId)) {
                return;
            }
        }
        if ((bucketAcl & PERMISSION_FULL_CON_NUM) != 0) {
            String grantListType = bucketName + "_" + PERMISSION_FULL_CON;
            if (pool.getCommand(REDIS_BUCKETINFO_INDEX).sismember(grantListType, userId)) {
                return;
            }
        }
        throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such bucket permission.");
    }

    /**
     * 校验账户是否满足ACP权限
     */
    public static void checkObjAcp(Map<String, String> objAclMap, String userId, String bucketUserId, int permission) {
        //异步复制跳过。mossserver-2956
        int aclNum = Integer.parseInt(objAclMap.get("acl"));
        String objectUserId = objAclMap.get("owner");

        if (userId.equals(objectUserId) || (bucketUserId.equals(userId) && (aclNum & OBJECT_PERMISSION_SHARE_OWNER_FULL_CONTROL_NUM) != 0)) {
            return;
        }

        if ((aclNum & permission) != 0 || (aclNum & OBJECT_PERMISSION_FULL_CON_NUM) != 0) {
            checkObjectGrantAcl(objAclMap, userId, permission);
        } else {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such object permission.");
        }
    }

    /**
     * 开启文件协议后校验账户是否满足ACP权限；需兼容旧版本的文件数据
     */
    public static void checkFsObjAcp(Map<String, String> objAclMap, String s3Id, String bucketUserId, int permission, MetaData metaData) {
        //异步复制跳过。mossserver-2956
        int aclNum = Integer.parseInt(objAclMap.get("acl"));
        String objectUserId = objAclMap.get("owner");
        String[] userId = {s3Id};

        try {
            boolean needTransfer = ACLUtils.NFS_ACL_START || ACLUtils.CIFS_ACL_START;
            if (needTransfer && metaData.inode > 0 && null != metaData.tmpInodeStr) {
                Inode inode = Json.decodeValue(metaData.tmpInodeStr, Inode.class);
                if (!ACLUtils.checkNewInode(inode)) {
                    //发起请求的moss账户已经配置uid，且发起请求的账户不是匿名账户；检查发起请求的账户与被请求的inode是否
                    //存在uid或者s3Id的相同，如果有则允许访问
                    if (ACLUtils.userInfo.containsKey(s3Id) && !DEFAULT_USER_ID.equals(s3Id) && ACLUtils.userInfo.get(s3Id).getUid() != 0) {
                        int inoUid = inode.getUid();
                        String inoS3Id = (String) objAclMap.get("owner");
                        FSIdentity identity = ACLUtils.userInfo.get(s3Id);
                        int reqUid = identity.getUid();
                        String reqS3Id = identity.getS3Id();
                        if (inoUid == reqUid || reqS3Id.equals(inoS3Id)) {
                            userId[0] = inoS3Id;
                        }
                    }
                }
            }
        } catch (Exception e) {
            if (e instanceof MsException) {
                throw e;
            } else {
                logger.error("judge Meta FS ACL error, key: {}", metaData.getKey(), e);
            }
        }



        if (userId[0].equals(objectUserId) || (bucketUserId.equals(userId[0]) && (aclNum & OBJECT_PERMISSION_SHARE_OWNER_FULL_CONTROL_NUM) != 0)) {
            return;
        }

        if ((aclNum & permission) != 0 || (aclNum & OBJECT_PERMISSION_FULL_CON_NUM) != 0) {
            checkObjectGrantAcl(objAclMap, userId[0], permission);
        } else {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such object permission.");
        }
    }

    private static void checkObjectGrantAcl(Map<String, String> objAclMap, String userId, int grantTypeNum) {
        objAclMap.remove("owner");
        objAclMap.remove("acl");
        objAclMap.remove("bucketName");
        objAclMap.remove("keyName");

        for (Map.Entry<String, String> entry : objAclMap.entrySet()) {
            final String aclNum = entry.getKey().split("-")[0];
            final String id = entry.getKey().split("-")[1];
            if (userId.equals(id)) {
                if ((Integer.parseInt(aclNum) & grantTypeNum) != 0) {
                    return;
                }
                if ((Integer.parseInt(aclNum) & OBJECT_PERMISSION_FULL_CON_NUM) != 0) {
                    return;
                }
            }
        }

        throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such object permission.");
    }

    /**
     * 判断bucket权限是否满足相应type操作
     * <p>
     * 响应式版本
     *
     * @param bucketAcl    桶的acl数
     * @param bucketUserId 桶的用户
     * @param userId       用户id
     * @param bucketName   桶名
     * @return Mono 封装了checkGrantAcl过程的计算流
     */
    private static <T> Mono<T> checkGrantAclReactive(int bucketAcl, String bucketUserId,
                                                     String userId, String bucketName, int grantTypeNum, String grantType) {
        if (bucketUserId.equals(userId)) {
            return Mono.empty();
        }
        Mono<Boolean> mono = Mono.just(false);
        if ((bucketAcl & grantTypeNum) != 0) {
            mono = mono.flatMap(flag -> flag ?
                    Mono.empty() : pool.getReactive(REDIS_BUCKETINFO_INDEX).sismember(bucketName + "_" + grantType, userId));
        }

        //bucket_FULL_CONTROL的set存在，存在有FULL_CONTROL权限的账户。
        //刚创建时bucket_FULL_CONTROL不存在，设置过任意grant_acl后才会创建，默认包含桶所有者的id。之后即便删除了grant_acl，buckt_FULL_CONTROL依然保留。
        if ((bucketAcl & PERMISSION_FULL_CON_NUM) != 0) {
            mono = mono.flatMap(flag -> flag ?
                    Mono.empty() : pool.getReactive(REDIS_BUCKETINFO_INDEX).sismember(bucketName + "_" + PERMISSION_FULL_CON, userId));
        }

        return mono.flatMap(flag -> {
            if (!flag) {
                throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such bucket permission.");
            }
            return Mono.empty();
        });
    }


    /**
     * 校验桶是否满足WRITE权限
     * <p>
     * 响应式版本
     *
     * @param bucketInfo 桶信息
     * @param userId     用户id
     * @param bucketName 桶名
     * @return Mono 封装了checkWriteAcl过程的计算流
     */
    public static <T> Mono<T> checkWriteAclReactive(Map<String, String> bucketInfo, String userId, String bucketName, SocketReqMsg msg) {
        //异步复制跳过。mossserver-2956
        if (bucketInfo.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket name :" + bucketName + ".");
        }

        int aclNum = Integer.parseInt(bucketInfo.get(BUCKET_ACL));
        String bucketUserId = bucketInfo.get(BUCKET_USER_ID);
        //是桶所有者，或者桶没有给出读写权限
        if (bucketUserId.equals(userId) || (aclNum & PERMISSION_SHARE_READ_WRITE_NUM) != 0) {
            return Mono.empty();
            //桶不存在write或者fullcontrol的权限，需要检查是否有账户权限
        } else if ((aclNum & PERMISSION_WRITE_NUM) != 0 || (aclNum & PERMISSION_FULL_CON_NUM) != 0) {
            return checkGrantAclReactive(aclNum, bucketUserId, userId, bucketName, PERMISSION_WRITE_NUM, PERMISSION_WRITE);
        } else {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such bucket permission.");
        }
    }

    /**
     * 校验桶是否满足WRITE权限
     * <p>
     * 响应式版本
     *
     * @param bucketInfo 桶信息
     * @param userId     用户id
     * @param bucketName 桶名
     * @return Mono 封装了checkWriteAcl过程的计算流
     */
    public static <T> Mono<T> checkWriteAclReactive(Map<String, String> bucketInfo, String userId, String bucketName) {
        return checkWriteAclReactive(bucketInfo, userId, bucketName, null);
    }

    /**
     * 校验桶是否满足READ权限
     * <p>
     * 响应式版本
     *
     * @param bucketInfo 桶信息
     * @param userId     用户id
     * @param bucketName 桶名
     * @return Mono 封装了checkReadAcl过程的计算流
     */
    public static <T> Mono<T> checkReadAclReactive(Map<String, String> bucketInfo, String userId, String bucketName, SocketReqMsg msg) {
        if (bucketInfo.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET, "No such bucket. bucket name :" + bucketName);
        }
        int aclNum = Integer.parseInt(bucketInfo.get(BUCKET_ACL));
        String bucketUserId = bucketInfo.get(BUCKET_USER_ID);
        msg.put("bucketUserId", bucketUserId);
        msg.put("userId", userId);
        //如果桶所属用户与当前用户id相同或者桶是公共读或者公共读写的
        if (bucketUserId.equals(userId) ||
                (aclNum & PERMISSION_SHARE_READ_NUM) != 0 ||
                (aclNum & PERMISSION_SHARE_READ_WRITE_NUM) != 0) {
            return Mono.empty();
            //如果桶有grant read 或者grant full control
        } else if ((aclNum & PERMISSION_READ_NUM) != 0 || (aclNum & PERMISSION_FULL_CON_NUM) != 0) {
            return checkGrantAclReactive(aclNum, bucketUserId, userId, bucketName, PERMISSION_READ_NUM, PERMISSION_READ);
        } else {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such bucket permission.");
        }
    }

    public static void checkIfAnonymous(String userId) {
        if (userId.equalsIgnoreCase(DEFAULT_USER_ID)) {
            throw new MsException(ErrorNo.ACCESS_DENY, "No such user.");
        }
    }

    /**
     * 判断是否是管理账户
     *
     * @param userId 账户id
     * @date 2019/7/3
     */
    public static void checkIfManageAccount(String userId) {
        if (!userId.equalsIgnoreCase(DEFAULT_MGT_USER_ID)) {
            throw new MsException(ErrorNo.ACCESS_DENY, "Not manage user, no permission.");
        }
    }

    /**
     * 检测消息体中的Content-MD5值
     *
     * @param paramMap 请求参数
     */
    public static void checkContentMD5(UnifiedMap<String, String> paramMap) {
        logger.info("CHECK MD5 is beginning");
        String contentMD5 = paramMap.get("content-md5");
        logger.info("The ContentMD5 in  request body is :" + contentMD5);
        String body = paramMap.get(BODY);
        logger.info("The receive body is :" + body);

        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            Base64.Encoder encoder = Base64.getEncoder();
            String MD5 = encoder.encodeToString(messageDigest.digest(body.getBytes("utf-8")));
            logger.info("The MD5 Computed just now is :" + MD5);
            if (!MD5.equals(contentMD5)) {
                throw new MsException(INVALID_MD5, "The request is changed ");
            }
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, String> getObjAcl(UnifiedMap<String, String> paramMap) {
        Map<String, String> map = new HashMap<>(16);
        paramMap.forEach((k, v) -> {
            aclCheck(k, v, map, true);
        });
        return map;
    }

    public static Map<String, String> getObjAcl(HttpServerRequest request) {
        List<Map.Entry<String, String>> headerList = request.headers().entries();
        Map<String, String> map = new HashMap<>(16);
        headerList.forEach(entry -> {
            aclCheck(entry.getKey().toLowerCase(), entry.getValue().toLowerCase(), map, false);
        });
        return map;
    }

    public static String objAclCheck(String objectAcl) {
        switch (objectAcl) {
            case PERMISSION_PRIVATE:
                objectAcl = String.valueOf(OBJECT_PERMISSION_PRIVATE_NUM);
                break;
            case PERMISSION_SHARE_READ:
                objectAcl = String.valueOf(OBJECT_PERMISSION_SHARE_READ_NUM);
                break;
            case PERMISSION_SHARE_READ_WRITE:
                objectAcl = String.valueOf(OBJECT_PERMISSION_SHARE_READ_WRITE_NUM);
                break;
            case PERMISSION_BUCKET_OWNER_READ:
                objectAcl = String.valueOf(OBJECT_PERMISSION_SHARE_BUCKET_OWNER_READ_NUM);
                break;
            case PERMISSION_BUCKET_OWNER_FULL_CONTROL:
                objectAcl = String.valueOf(OBJECT_PERMISSION_SHARE_OWNER_FULL_CONTROL_NUM);
                break;
            default:
                throw new MsException(ErrorNo.NO_SUCH_OBJECT_PERMISSION, "No such object permission.");
        }
        return objectAcl;
    }

    private static void aclCheck(String k, String v, Map<String, String> map, boolean state) {
        try {
            if ("x-amz-acl".equals(k)) {
                v = objAclCheck(v);
                map.put(k, v);
            }

            if (k.startsWith(GRANT_ACL)) {
                for (String value : v.split(",")) {
                    String[] id = value.split("=", 2);
                    if (id.length != 2) {
                        throw new MsException(ErrorNo.INVALID_ARGUMENT, "acl input error.");
                    }
                    if (state) {
                        if (!USER_DATABASE_ID_TYPE.equals(pool.getCommand(REDIS_USERINFO_INDEX).hget(id[1], USER_DATABASE_HASH_TYPE))) {
                            throw new MsException(ErrorNo.NO_SUCH_ID, "no such user id. user id: " + id[1] + ".");
                        }
                    }

                    if (!PERMISSION_READ_LONG.equals(k)
                            && !PERMISSION_READ_CAP_LONG.equals(k)
                            && !PERMISSION_WRITE_CAP_LONG.equals(k)
                            && !PERMISSION_FULL_CON_LONG.equals(k)) {
                        throw new MsException(ErrorNo.NO_SUCH_OBJECT_PERMISSION, "No such object permission.");
                    }
                    map.put(k + "=" + id[1], id[1]);
                }
            }
        } catch (Exception e) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "acl input error.");
        }
    }

    public static int getAclNum(String aclStr) {
        switch (aclStr) {
            case "private":
                return OBJECT_PERMISSION_PRIVATE_NUM;
            case "public-read":
                return OBJECT_PERMISSION_SHARE_READ_NUM;
            case "public-read-write":
                return OBJECT_PERMISSION_SHARE_READ_WRITE_NUM;
            case "bucket-owner-read":
                return OBJECT_PERMISSION_SHARE_BUCKET_OWNER_READ_NUM;
            case "bucket-owner-full-control":
                return OBJECT_PERMISSION_SHARE_OWNER_FULL_CONTROL_NUM;
            default:
                throw new MsException(ErrorNo.NO_SUCH_OBJECT_PERMISSION, "No such object permission.");
        }
    }

}
