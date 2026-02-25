package com.macrosan.message.consturct;

import com.macrosan.action.core.MergeMap;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.utils.functional.ImmutableTuple;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.serialize.JsonUtils;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.constants.SysConstants.MSG_TYPE_DELETE_ACCESSKEY_ACCOUNT;
import static com.macrosan.utils.sts.RoleUtils.*;

/**
 * SocketReqMsgBuilder
 * 统一构造调用各种方法的消息
 * 目前已经有的消息类型：
 * <p>
 * UpLoad Object
 *
 * <p>
 * DownLoad Object
 * <p>
 * Part Upload
 * <p>
 * Delete bucket
 * <p>
 * List Object
 * <p>
 * Create Bucket
 * <p>
 * Delete Object DB File
 * <p>
 * Delete Object
 * <p>
 * Head Object
 * <p>
 * Init Part Upload
 * <p>
 * List Part Upload
 * <p>
 *
 * @author liyixin
 * @date 2019/1/29
 */
public class SocketReqMsgBuilder {
    protected static RedisConnPool pool = RedisConnPool.getInstance();
    private SocketReqMsgBuilder() {
    }

    public static SocketReqMsg buildRefactorCopyMessage(MsHttpRequest request, String sourceBucket, String sourceObject) {
        SocketReqMsg reqMsg = new SocketReqMsg(MSG_TYPE_COPY_OBJECT, 0)
                .put("userId", request.getUserId())
                .put("targetKey", request.getObjectName())
                .put("targetBucket", request.getBucketName())
                .put("sourceBucket", sourceBucket)
                .put("sourceObject", sourceObject);

        request.headers().forEach(entry -> {
            final String key = entry.getKey().toLowerCase();
            reqMsg.put(key, entry.getValue());
        });
        return reqMsg;
    }

    public static SocketReqMsg buildUploadMsg(MsHttpRequest request, String bucketName,
                                              String objName, String requestId) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_UPLOAD_OBJECT, 0)
                .put("obj_name", objName)
                .put("bucket_name", bucketName)
                .put("userId", request.getUserId())
                .put("request_id", requestId);
        if (request.getHeader(TRANSFER_ENCODING) == null || !request.getHeader(TRANSFER_ENCODING).contains("chunked")) {
            final String contentLength = request.getHeader(CONTENT_LENGTH);
            if (contentLength == null) {
                throw new MsException(ErrorNo.MISSING_CONTENT_LENGTH, "upload error, no content-length param");
            }
            msg.put("obj_size", contentLength);
        }

        String md5 = request.getHeader(CONTENT_MD5);
        return StringUtils.isBlank(md5) ? msg : msg.put("content_md5", md5);
    }

    public static SocketReqMsg buildDownLoadMsg(MsHttpRequest request, Map<String, String> targetInfo, String downLoadSize, String startIndex) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_DOWNLOAD_OBJECT, 0)
                .put(VNODE_LUN_NAME, targetInfo.get(VNODE_LUN_NAME)).put("source_path", targetInfo.get("target_path"))
                .put("start_index", startIndex).put("down_size", downLoadSize)
                .put("bucket_name", request.getBucketName()).put("userId", request.getUserId());

        return targetInfo.get("fileType") == null ? msg : msg.put("fileType", targetInfo.get("fileType"));
    }

    public static SocketReqMsg buildPartUploadMsg(MsHttpRequest request, String num,
                                                  String uploadId, String vnodeId) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_UPLOAD_PART, 0)
                .put("vnode_id", vnodeId).put("partNumber", num)
                .put("uploadId", uploadId).put("len_size", request.getHeader(CONTENT_LENGTH))
                .put("userId", request.getUserId()).put("bucket_name", request.getBucketName());

        return request.getHeader(CONTENT_MD5) == null ? msg : msg.put("content_md5", request.getHeader(CONTENT_MD5));
    }

    public static SocketReqMsg buildDelBucketMsg(String requestId, String bucketName, String vnodeId, MergeMap<String, String> mergeMap) {
        String targetLun = mergeMap.get(VNODE_LUN_NAME);
        return buildCreateBucketMsg(requestId, bucketName, vnodeId, mergeMap)
                .setMsgType(MSG_TYPE_DEL_BUCKET);
    }

    public static SocketReqMsg buildLsObjMsg(String bucketName, MergeMap<String, String> mergeMap, JsonObject jsonObject) {
        return new SocketReqMsg(MSG_TYPE_LS_BUCKET, 0)
                .put("bucketName", bucketName)
                .put("take_over", mergeMap.get("take_over"))
                .put("params", jsonObject.toString())
                .put(HEART_ETH1, mergeMap.get(HEART_ETH1))
                .put(HEART_ETH2, mergeMap.get(HEART_ETH2))
                .put("bucket_lun_name", mergeMap.get(VNODE_LUN_NAME));
    }

    public static SocketReqMsg buildLsMulUploadsMsg(String bucketName, MergeMap<String, String> mergeMap, JsonObject jsonObject) {
        return new SocketReqMsg(MSG_TYPE_LS_UPLOADS, 0)
                .put("bucketName", bucketName)
                .put("take_over", mergeMap.get("take_over"))
                .put("params", jsonObject.toString())
                .put(HEART_ETH1, mergeMap.get(HEART_ETH1))
                .put(HEART_ETH2, mergeMap.get(HEART_ETH2))
                .put("bucket_lun_name", mergeMap.get(VNODE_LUN_NAME));
    }

    public static SocketReqMsg buildCreateBucketMsg(String requestId, String bucketName, String vnodeId, MergeMap<String, String> mergeMap) {
        String targetLun = mergeMap.get(VNODE_LUN_NAME);
        return new SocketReqMsg(MSG_TYPE_CREATE_BUCKET, 0)
                .put("vnode", vnodeId)
                .put("request_id", requestId)
                .put("bucket_name", bucketName)
                .put("take_over", mergeMap.get("take_over"))
                .put(VNODE_LUN_NAME, targetLun)
                .put(HEART_ETH1, mergeMap.get(HEART_ETH1))
                .put(HEART_ETH2, mergeMap.get(HEART_ETH2));
    }

    public static SocketReqMsg buildDelObjDBFileMsg(String bucketName, String objName, MergeMap<String, String> bucketMergeMap) {
        return new SocketReqMsg(MSG_TYPE_DEL_BUCKET_OBJ_DBFIEL, 0).put("obj_name", objName)
                .put("bucket_name", bucketName)
                .put("take_over", bucketMergeMap.get("take_over"))
                .put("bucket_lun_name", bucketMergeMap.get(VNODE_LUN_NAME))
                .put(HEART_ETH1, bucketMergeMap.get(HEART_ETH1))
                .put(HEART_ETH2, bucketMergeMap.get(HEART_ETH2));
    }

    public static SocketReqMsg buildDelObjMsg(String uploadId, String vnodeId, MergeMap<String, String> mergeMap) {
        String lunName = mergeMap.get(VNODE_LUN_NAME);
        String targetPath = File.separator + lunName + File.separator + "V" + vnodeId + File.separator + uploadId;
        return new SocketReqMsg(MSG_TYPE_DEL_OBJECT, 0)
                .put("target_path", targetPath)
                .put(VNODE_LUN_NAME, lunName);
    }

    /**
     * 构建删除ec中对象的请求
     *
     * @return
     */
    public static SocketReqMsg buildDelEcObjMsg(String bucket, String objectName, String requestId) {
        return new SocketReqMsg(MSG_TYPE_DEL_OBJECT, 0)
                .put("bucket_name", bucket)
                .put("obj_name", objectName)
                .put("request_id", requestId);
    }

    public static SocketReqMsg buildDelAllObjMsg(String savedObjName, String vnodeId, MergeMap<String, String> mergeMap,
                                                 String mda, String userId, String bucketName, String objectName) {
        return buildDelObjMsg(savedObjName, vnodeId, mergeMap)
                .put("take_over", mergeMap.get("take_over"))
                .put("mda", mda)
                .put("userId", userId).put("bucket_name", bucketName).put("obj_name", objectName)
                .setMsgType(MSG_TYPE_DEL_ALL_OBJECT);
    }

    public static SocketReqMsg buildHeadObjMsg(String bucketUserId, String userId, String vnodeId, String savedObjName, MergeMap<String, String> mergeMap) {
        return buildDelObjMsg(savedObjName, vnodeId, mergeMap)
                .put("bucketUserId", bucketUserId)
                .put("userId", userId)
                .setMsgType(MSG_TYPE_GET_OBJECTINFO);
    }

    public static SocketReqMsg buildGetObjAclMsg(String vnodeId, String savedObjName, MergeMap<String, String> mergeMap) {
        return buildDelObjMsg(savedObjName, vnodeId, mergeMap)
                .setMsgType(MSG_TYPE_GET_OBJECTACL);
    }

    public static SocketReqMsg buildUpdateObjAclMsg(Map<String, String> objectAclMap, String vnodeId, String savedObjName, MergeMap<String, String> mergeMap) {
        return buildDelObjMsg(savedObjName, vnodeId, mergeMap)
                .put(objectAclMap)
                .setMsgType(MSG_TYPE_UPDATE_OBJECT_ACL);
    }

    public static SocketReqMsg buildInitPartUploadMsg(UnifiedMap<String, String> paramMap,
                                                      ImmutableTuple<String, String> objectVnodeId,
                                                      String uploadId,
                                                      MergeMap<String, String> mergeMap,
                                                      String bucketUserId,
                                                      String userId) {
        String lunName = mergeMap.get(VNODE_LUN_NAME);
        String targetPath = File.separator + lunName + File.separator + "V" + objectVnodeId.var1 + File.separator + uploadId;

        JsonObject jsonUserMeta = new JsonObject();
        paramMap.forEach((k, v) -> {
            if (k.startsWith(USER_META)) {
                try {
                    jsonUserMeta.put(URLDecoder.decode(k, "UTF-8"), URLDecoder.decode(v, "UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    throw new IllegalArgumentException(e.getMessage());
                } catch (IllegalArgumentException e) {
                    jsonUserMeta.put(k, v);
                }
            }
        });

        String userMetaStr = jsonUserMeta.toString();
        if (userMetaStr.length() > META_USR_MAX_SIZE) {
            throw new MsException(ErrorNo.META_DATA_TOO_LARGE, "user meta is too long , user meta size :" + userMetaStr.length());
        }

        JsonObject jsonSysMeta = new JsonObject()
                .put(CONTENT_LENGTH, paramMap.get("content-length"))
                .put(CONTENT_TYPE, paramMap.getOrDefault("content-type", "application/octet-stream"))
                .put(OBJECT_TYPE, "1");

        //获取对象权限
        Map objAclMap = MsAclUtils.getObjAcl(paramMap);

        return new SocketReqMsg(MSG_TYPE_INIT_MUL_UPLOAD, 0)
                .put("target_path", targetPath)
                .put("uploadId", uploadId)
                .put("sys_meta", jsonSysMeta.toString())
                .put("usr_meta", userMetaStr)
                .put(objAclMap)
                .put("bucket_userId", bucketUserId)
                .put("userId", userId)
                .put("take_over", mergeMap.get("take_over"))
                .put("lun_name", mergeMap.get(VNODE_LUN_NAME));
    }

    public static SocketReqMsg buildAbortPartMsg(String uploadId, String vnodeId, MergeMap<String, String> mergeMap) {
        return new SocketReqMsg(MSG_TYPE_ABORT_UPLOAD, 0)
                .put("lun_name", mergeMap.get(VNODE_LUN_NAME))
                .put("vnode_id", vnodeId)
                .put("upload_id", uploadId)
                .put("take_over", mergeMap.get("take_over"));
    }

    public static SocketReqMsg buildLsPartUploadMsg(String uploadId, String maxPart, String partMaker, MergeMap<String, String> mergeMap) {
        if (Integer.parseInt(maxPart) < 0) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "invalid maxPart , maxPart :" + maxPart);
        }

        if (Integer.parseInt(partMaker) < 0) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "invalid partMaker , partMaker :" + partMaker);
        }

        return new SocketReqMsg(MSG_TYPE_LIST_PARTS, 0)
                .put("uploadId", uploadId)
                .put("max_parts", maxPart)
                .put("part_num_marker", partMaker)
                .put("take_over", mergeMap.get("take_over"));
    }

    public static SocketReqMsg buildMergeMsg(Document uploadIdMap, MergeMap<String, String> bucketInfoMergeMap, MergeMap<String, String> objInfoMergeMap,
                                             String uploadId, String objName, String bucketName, JsonObject allPartEtag, String mda) {


        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_END_MUL_UPLOAD, 0)
                .put("uploadId", uploadId)
                .put("obj_name", objName)
                .put("saved_obj_name", uploadIdMap.getString("saved_obj_name"))
                .put("bucket_ip1", bucketInfoMergeMap.get(HEART_ETH1))
                .put("bucket_ip2", bucketInfoMergeMap.get(HEART_ETH2))
                .put("bucket_name", bucketName)
                .put("bucket_lun_name", bucketInfoMergeMap.get(VNODE_LUN_NAME))
                .put(VNODE_LUN_NAME, objInfoMergeMap.get(VNODE_LUN_NAME))
                .put("allPartEtag", allPartEtag.toString())
                .put("user", uploadIdMap.getString("user_name"))
                .put("userId", uploadIdMap.getString("user_id"))
                .put("bucket_take_over", bucketInfoMergeMap.get("take_over"))
                .put("obj_take_over", objInfoMergeMap.get("take_over"))
                .put("vnode_id", uploadIdMap.getString("vnode_id"));
        if (uploadIdMap.containsKey("backup_userId")) {
            msg.put("backup_userId", uploadIdMap.getString("backup_userId"));
            msg.put("backup_time", uploadIdMap.getString("backup_time"));
        }

        return mda == null ? msg : msg.put("mda", mda);
    }

    public static SocketReqMsg buildCopyMsg(UnifiedMap<String, String> paramMap, MergeMap<String, String> mergeMap, ImmutableTuple<String, String> objectVnodeId,
                                            String requestId, String metaDirective, String userMeta, String ifMatch) {
        String localPath = File.separator + mergeMap.get(VNODE_LUN_NAME) + File.separator + "V" + objectVnodeId.var1;
        SocketReqMsg socketReqMsg = new SocketReqMsg(MSG_TYPE_COPY_OBJECT, 0)
                .put("vnodeId", objectVnodeId.var1)
                .put("path", localPath)
                .put("saved_objName", objectVnodeId.var2)
                .put("object_name", paramMap.get(OBJECT_NAME))
                .put("bucket_name", paramMap.get(BUCKET_NAME))
                .put("meta_directive", metaDirective)
                .put("usr_meta", userMeta)
                .put("request_id", requestId)
                .put("userId", paramMap.get(USER_ID))
                .put("ifMatch", ifMatch)
                .put(VNODE_LUN_NAME, mergeMap.get(VNODE_LUN_NAME));
        if (paramMap.containsKey("x-amz-acl")) {
            socketReqMsg.put("x-amz-acl", paramMap.get("x-amz-acl"));
        }
        return socketReqMsg;
    }

    private static String getIfMatch(HttpServerRequest request) {
        HashMap<String, String> result = new HashMap<>(16);
        request.headers().forEach(entry -> {
            final String key = entry.getKey();
            if (key.startsWith(IF_MATCH)) {
                result.put(key, entry.getValue());
            }
        });

        if (result.size() > 0) {
            if ((result.containsKey("x-amz-copy-source-if-match") || result.containsKey("x-amz-copy-source-if-unmodified-since")) &&
                    (result.containsKey("x-amz-copy-source-if-none-match") || result.containsKey("x-amz-copy-source-if-modified-since"))) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "parameters cannot exist together！" + result);
            }
        }
        return JsonUtils.toString(result, HashMap.class);
    }

    public static SocketReqMsg buildCopyMsg(MsHttpRequest request, String sourceBucket, String sourceObject) {
        String metaDirective = Optional.ofNullable(request.getHeader("x-amz-metadata-directive")).orElse("COPY");
        String userMetaStr;
        if ("COPY".equals(metaDirective)) {
            userMetaStr = "";
            if (sourceBucket.equals(request.getBucketName()) && sourceObject.equals(request.getObjectName())) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "x-amz-metadata-directive must be REPLACE!");
            }
        } else if ("REPLACE".equals(metaDirective)) {
            HashMap<String, String> result = new HashMap<>(16);
            request.headers().forEach(entry -> {
                final String key = entry.getKey().toLowerCase();
                if (key.startsWith(USER_META) || key.equalsIgnoreCase(CACHE_CONTROL)
                        || key.equalsIgnoreCase(EXPIRES) || key.equalsIgnoreCase(CONTENT_ENCODING)
                        || key.equalsIgnoreCase(CONTENT_DISPOSITION) || key.equalsIgnoreCase(CONTENT_TYPE)) {
                    result.put(key, entry.getValue());
                }
            });
            userMetaStr = JsonUtils.toString(result);

            if (userMetaStr.length() > META_USR_MAX_SIZE) {
                throw new MsException(ErrorNo.META_DATA_TOO_LARGE, "user meta is too long , user meta size :" + userMetaStr.length());
            }
        } else {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "x-amz-metadata-directive is INVALID_ARGUMENT!");
        }

        SocketReqMsg reqMsg = new SocketReqMsg(MSG_TYPE_COPY_OBJECT, 0)
                .put("userId", request.getUserId())
                .put("targetKey", request.getObjectName())
                .put("targetBucket", request.getBucketName())
                .put("sourceBucket", sourceBucket)
                .put("ifMatch", getIfMatch(request))
                .put("metaDirective", metaDirective)
                .put("newUserMeta", userMetaStr);

        Set<String> aclHearder = new HashSet<>();
        aclHearder.add("x-amz-acl");
        aclHearder.add("x-amz-grant-read");
        aclHearder.add("x-amz-grant-read-acp");
        aclHearder.add("x-amz-grant-write-acp");
        aclHearder.add("x-amz-grant-full-control");
        request.headers().forEach(entry -> {
            final String key = entry.getKey().toLowerCase();
            if (aclHearder.contains(key)) {
                reqMsg.put(key, entry.getValue());
            }
        });
        return reqMsg;
    }

    //iam
    public static SocketReqMsg buildCreateAccountMsg(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        SocketReqMsg reqMsg = new SocketReqMsg(MSG_TYPE_CREATE_ACCOUNT, 0);
        if (paramMap.containsKey(REGION_FLAG_UPPER)) {
            String regionFlag = paramMap.get(REGION_FLAG_UPPER);
            if ("1".equals(regionFlag)) {
                reqMsg.put(REGION_FLAG, regionFlag);
                reqMsg.put("accountId", paramMap.get("AccountId"));
                reqMsg.put("ak", paramMap.get("AccessKey"));
                reqMsg.put("sk", paramMap.get("SecretKey"));
                reqMsg.put("createTime", paramMap.get("CreateTime"));
            }
        }
        if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
            reqMsg.put(SITE_FLAG, "1");
            reqMsg.put("accountId", paramMap.get("AccountId"));
            reqMsg.put("ak", paramMap.get("AccessKey"));
            reqMsg.put("sk", paramMap.get("SecretKey"));
            reqMsg.put("createTime", paramMap.get("CreateTime"));
        }
        reqMsg.put("accountName", paramMap.get("AccountName"));
        reqMsg.put("passWord", paramMap.get("Password"));
        reqMsg.put("accountNickName", URLEncoder.encode(paramMap.getOrDefault("AccountNickName", ""), "utf-8"));
        reqMsg.put("remark", URLEncoder.encode(paramMap.getOrDefault("Remark", ""), "utf-8"));
        reqMsg.put("storageStrategy", paramMap.get("StorageStrategy"));
        reqMsg.put("validityTime", paramMap.getOrDefault("ValidityTime", "0"));
        reqMsg.put("endTimeSecond", paramMap.get("EndTimeSecond"));
        reqMsg.put("validityGrade", paramMap.get("ValidityGrade"));
        return reqMsg;
    }

    public static SocketReqMsg buildDeleteAccountMsg(String accountId, String accountName) {
        return new SocketReqMsg(MSG_TYPE_DELETE_ACCOUNT, 0)
                .put("accountId", accountId)
                .put("accountName", accountName);
    }

    public static SocketReqMsg buildDeleteRoleMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_DELETE_ROLE, 0);
        msg.put("accountId", paramMap.get(USER_ID));
        msg.put("roleId", paramMap.get("roleId"));
        msg.put("roleName", paramMap.get(ROLE_NAME));
        if (paramMap.containsKey(REGION_FLAG_UPPER) || paramMap.containsKey(SITE_FLAG_UPPER)) {
            if ("1".equals(paramMap.get(REGION_FLAG_UPPER))) {
                msg.put(REGION_FLAG, "1");
            }
            if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
                msg.put(SITE_FLAG, "1");
            }
        }
        return msg;
    }

    public static SocketReqMsg buildEditAccountName(String accountName, String newAccountName) {
        return new SocketReqMsg(MSG_TYPE_UPDATE_ACCOUNT_NAME, 0)
                .put("oldName", accountName)
                .put("newName", newAccountName);
    }

    public static SocketReqMsg buildEditUserName(String accountId, String oldName, String newName) {
        return new SocketReqMsg(MSG_TYPE_UPDATE_USER_NAME, 0)
                .put("accountId", accountId)
                .put("userName", oldName)
                .put("newName", newName);
    }

    public static SocketReqMsg buildUpdateAccountMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_UPDATE_ACCOUNT, 0);
        try {
            String nowStrategy = pool.getCommand(REDIS_USERINFO_INDEX).hget(paramMap.get("AccountName"), "storage_strategy");
            msg.put("accountName", paramMap.get("AccountName"))
                    .put("newPassWord", paramMap.getOrDefault("NewPassWord", ""))
                    .put("newAccountNickName", URLEncoder.encode(paramMap.getOrDefault("NewAccountNickName", ""), "utf-8"))
                    .put("newRemark", URLEncoder.encode(paramMap.getOrDefault("NewRemark", ""), "utf-8"))
                    .put("perfQuotaType", paramMap.getOrDefault("PerfQuotaType", ""))
                    .put("perfQuotaValue", paramMap.getOrDefault("PerfQuotaValue", ""))
                    .put("storageStrategy", paramMap.getOrDefault("StorageStrategy", nowStrategy))
                    .put("validityTime", paramMap.getOrDefault("ValidityTime", ""))
                    .put("endTimeSecond", paramMap.getOrDefault("EndTimeSecond", ""))
                    .put("validityGrade", paramMap.get("ValidityGrade"));
            if (!StringUtils.isEmpty(paramMap.get("CapacityQuota"))) {
                msg.put("capacityQuota", paramMap.get("CapacityQuota"));
            }
            if (!StringUtils.isEmpty(paramMap.get("SoftCapacityQuota"))) {
                msg.put("softCapacityQuota", paramMap.get("SoftCapacityQuota"));
            }
            if (!StringUtils.isEmpty(paramMap.get("ObjNumQuota"))) {
                msg.put("objNumQuota", paramMap.get("ObjNumQuota"));
            }
            if (!StringUtils.isEmpty(paramMap.get("SoftObjNumQuota"))) {
                msg.put("softObjNumQuota", paramMap.get("SoftObjNumQuota"));
            }
            if (!StringUtils.isEmpty(paramMap.get("storageStrategy"))) {
                msg.put("storageStrategy", paramMap.get("storageStrategy"));
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return msg;
    }

    public static SocketReqMsg buildUpdateRoleMsg(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_UPDATE_ROLE, 0);
        msg.put("roleId", paramMap.get("roleId"));
        if (paramMap.containsKey("maxSessionDuration")) {
            msg.put("maxSessionDuration", paramMap.get("maxSessionDuration"));
        }
        msg.put("description", URLEncoder.encode(paramMap.getOrDefault("description", ""), "utf-8"));
        if (paramMap.containsKey(REGION_FLAG_UPPER) || paramMap.containsKey(SITE_FLAG_UPPER)) {
            if ("1".equals(paramMap.get(REGION_FLAG_UPPER))) {
                msg.put(REGION_FLAG, "1");
            }
            if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
                msg.put(SITE_FLAG, "1");
            }
        }
        return msg;
    }

    public static SocketReqMsg buildListRoleMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_LIST_ROLE, 0);
        return msg;
    }

    public static SocketReqMsg buildGetRoleMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_GET_ROLE, 0);
        return msg;
    }

    public static SocketReqMsg buildUpdateRoleDescriptionMsg(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_UPDATE_ROLE_DESCRIPTION, 0);
        msg.put("roleId", paramMap.get("roleId"));
        msg.put("description", paramMap.getOrDefault(DESCRIPTION, ""));
        msg.put("accountId", paramMap.get(USER_ID));
        msg.put("roleName", paramMap.get(ROLE_NAME));
        if (paramMap.containsKey(REGION_FLAG_UPPER) || paramMap.containsKey(SITE_FLAG_UPPER)) {
            if ("1".equals(paramMap.get(REGION_FLAG_UPPER))) {
                msg.put(REGION_FLAG, "1");
            }
            if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
                msg.put(SITE_FLAG, "1");
            }
        }
        return msg;
    }

    public static SocketReqMsg buildAttachRolePolicyMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_ATTACH_ROLE_POLICY, 0);
        msg.put("accountId", paramMap.get(USER_ID));
        msg.put("roleId", paramMap.get("roleId"));
        msg.put("policyId", paramMap.get("policyId"));
        msg.put("roleName", paramMap.get(ROLE_NAME));
        msg.put("policyArn", paramMap.get(POLICY_ARN));
        if (paramMap.containsKey(REGION_FLAG_UPPER) || paramMap.containsKey(SITE_FLAG_UPPER)) {
            if ("1".equals(paramMap.get(REGION_FLAG_UPPER))) {
                msg.put(REGION_FLAG, "1");
            }
            if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
                msg.put(SITE_FLAG, "1");
            }
        }
        return msg;
    }

    public static SocketReqMsg buildPutRolePolicyMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_PUT_ROLE_POLICY, 0);
        msg.put("policyDocument", paramMap.get("policyDocument"));
        msg.put("roleId", paramMap.get("roleId"));
        msg.put("policyName", paramMap.get("policyName"));
        msg.put("accountId", paramMap.get(USER_ID));
        msg.put("roleName", paramMap.get(ROLE_NAME));
        msg.put("remark", paramMap.get(REMARK));
        if (paramMap.containsKey(REGION_FLAG_UPPER) || paramMap.containsKey(SITE_FLAG_UPPER)) {
            if ("1".equals(paramMap.get(REGION_FLAG_UPPER))) {
                msg.put(REGION_FLAG, "1");
                msg.put("policyId", paramMap.get("PolicyId"));
            }
            if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
                msg.put(SITE_FLAG, "1");
                msg.put("policyId", paramMap.get("PolicyId"));
            }
        }
        return msg;
    }

    public static SocketReqMsg buildGetRolePolicyMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_GET_ROLE_POLICY, 0);
        return msg;
    }

    public static SocketReqMsg buildDeleteRolePolicyMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_DELETE_ROLE_POLICY, 0);
        msg.put("accountId", paramMap.get(USER_ID));
        msg.put("policyId", paramMap.get("policyId"));
        msg.put("roleId", paramMap.get("roleId"));
        msg.put("roleName", paramMap.get(ROLE_NAME));
        msg.put("policyName", paramMap.get(POLICY_NAME));
        if (paramMap.containsKey(REGION_FLAG_UPPER) || paramMap.containsKey(SITE_FLAG_UPPER)) {
            if ("1".equals(paramMap.get(REGION_FLAG_UPPER))) {
                msg.put(REGION_FLAG, "1");
            }
            if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
                msg.put(SITE_FLAG, "1");
            }
        }
        return msg;
    }

    public static SocketReqMsg buildListRolePolicyMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_LIST_ROLE_POLICY, 0);
        return msg;
    }

    public static SocketReqMsg buildUpdateAssumeRolePolicyMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_UPDATE_ASSUMEROLE_POLICY, 0);
        msg.put("accountId", paramMap.get(USER_ID));
        msg.put("roleId", paramMap.get("roleId"));
        msg.put("assumePolicy", paramMap.get("assumePolicy"));
        msg.put("roleName", paramMap.get(ROLE_NAME));
        if (paramMap.containsKey(REGION_FLAG_UPPER) || paramMap.containsKey(SITE_FLAG_UPPER)) {
            if ("1".equals(paramMap.get(REGION_FLAG_UPPER))) {
                msg.put(REGION_FLAG, "1");
            }
            if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
                msg.put(SITE_FLAG, "1");
            }
        }
        return msg;
    }

    public static SocketReqMsg buildListAttachRolePolicyMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_LIST_ATTACH_ROLE_POLICY, 0);
        return msg;
    }

    public static SocketReqMsg buildDetachRolePolicyMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_DETACH_ROLE_POLICY, 0);
        msg.put("policyId", paramMap.get("policyId"));
        msg.put("roleId", paramMap.get("roleId"));
        msg.put("accountId", paramMap.get(USER_ID));
        msg.put("roleName", paramMap.get(ROLE_NAME));
        msg.put("policyArn", paramMap.get(POLICY_ARN));
        if (paramMap.containsKey(REGION_FLAG_UPPER) || paramMap.containsKey(SITE_FLAG_UPPER)) {
            if ("1".equals(paramMap.get(REGION_FLAG_UPPER))) {
                msg.put(REGION_FLAG, "1");
            }
            if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
                msg.put(SITE_FLAG, "1");
            }
        }
        return msg;
    }

    public static SocketReqMsg buildAssumeRoleMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_ASSUME_ROLE, 0);
        String policy = paramMap.get("policy");
        if (policy != null) {
            msg.put("policy", paramMap.get("policy"));
        }
        msg.put("policyIds", paramMap.get("policyIds"));
        msg.put("policyMrns", paramMap.get("policyMrns"));
        msg.put("durationSeconds", paramMap.get(DURATION_SECONDS));
        msg.put("roleId", paramMap.get("roleId"));
        msg.put("roleName", paramMap.get("roleName"));
        msg.put("userName", paramMap.get(USERNAME));
        msg.put("accountId", paramMap.get(USER_ID));
        msg.put("sessionName", paramMap.get(ROLE_SESSION_NAME));

        if (paramMap.containsKey(REGION_FLAG_UPPER) || paramMap.containsKey(SITE_FLAG_UPPER)) {
            if ("1".equals(paramMap.get(REGION_FLAG_UPPER))) {
                msg.put(REGION_FLAG, "1");
            }
            if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
                msg.put(SITE_FLAG, "1");
            }
            if (policy != null) {
                msg.put("policyId", paramMap.get("PolicyId"));
            }
            msg.put("assumeId", paramMap.get("AssumeId"));
            msg.put("accessKey", paramMap.getOrDefault("AccessKey", ""));
            msg.put("secretKey", paramMap.getOrDefault("SecretKey", ""));
        }
        return msg;
    }

    public static SocketReqMsg buildUpdateAccountPasswdMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_UPDATE_ACCOUNT_PASSWORD, 0);
        msg.put("accountName", paramMap.getOrDefault("AccountName", ""));
        msg.put("oldPasswd", paramMap.getOrDefault("OldPassword", ""));
        msg.put("newPassWord", paramMap.getOrDefault("NewPassword", ""));
        msg.put("validityTime", paramMap.getOrDefault("ValidityTime", ""));
        msg.put("endTimeSecond", paramMap.get("EndTimeSecond"));
        msg.put("validityGrade", paramMap.get("ValidityGrade"));
        return msg;
    }

    public static SocketReqMsg buildUpdateUserPasswdMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_UPDATE_USER_PASSWORD, 0);
        msg.put("accountId", paramMap.getOrDefault("accountId", ""));
        msg.put("userName", paramMap.getOrDefault("UserName", ""));
        msg.put("oldPasswd", paramMap.getOrDefault("OldPassWord", ""));
        msg.put("newPassWord", paramMap.getOrDefault("NewPassWord", ""));
        msg.put("validityTime", paramMap.getOrDefault("ValidityTime", ""));
        msg.put("endTimeSecond", paramMap.get("EndTimeSecond"));
        msg.put("validityGrade", paramMap.get("ValidityGrade"));
        return msg;
    }

    public static SocketReqMsg buildUpdateAWSUserPasswdMsg(String accountId, String newPassword, String oldPassword, String userName) {
        return new SocketReqMsg(MSG_TYPE_UPDATE_USER_PASSWORD, 0)
                .put("accountId", accountId)
                .put("oldPasswd", oldPassword)
                .put("newPassWord", newPassword)
                .put("validityTime", "")
                .put("endTimeSecond", "")
                .put("userName", userName)
                .put("validityGrade", "0");
    }

    public static SocketReqMsg buildCreateAccessKeyAccountMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg reqMsg = new SocketReqMsg(MSG_TYPE_CREATE_ACCESSKEY_ACCOUNT, 0);
        String accountId = paramMap.get("AccountId");
        String accessKey = paramMap.getOrDefault("AccessKey", "");
        String secretKey = paramMap.getOrDefault("SecretKey", "");
        dealSyncFlag(paramMap, reqMsg);
        reqMsg.put("accountId", accountId);
        if (!"".equals(accessKey) || !"".equals(secretKey)) {
            reqMsg.put("AccessKey", accessKey);
            reqMsg.put("SecretKey", secretKey);
        }
        return reqMsg;
    }

    public static SocketReqMsg buildCreateAdminAccessKeyMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg reqMsg = new SocketReqMsg(MSG_TYPE_CREATE_ADMIN_ACCESSKEY, 0);
        String accountId = paramMap.get("AccountId");
        dealSyncFlag(paramMap, reqMsg);
        String akAcl = paramMap.getOrDefault("AkAcl", "");
        if (!"".equals(akAcl) && !"1".equals(akAcl)) {
            throw new MsException(ErrorNo.INVALID_AK_ACL, "The acl of ak is invalid.");
        }
        reqMsg.put("accountId", accountId).put("acl", akAcl);

        return reqMsg;
    }

    public static SocketReqMsg buildDeleteAccessKeyAccountMsg(String accountId, String accessKeyId) {
        return new SocketReqMsg(MSG_TYPE_DELETE_ACCESSKEY_ACCOUNT, 0)
                .put("accountId", accountId)
                .put("accessKeyId", accessKeyId);
    }

    public static SocketReqMsg buildDeleteAdminAccessKeyMsg(String accessKeyId) {
        return new SocketReqMsg(MSG_TYPE_DELETE_ADMIN_ACCESSKEY, 0)
                .put("accessKeyId", accessKeyId);
    }

    public static SocketReqMsg buildUpdateAccountCapacityQuota(UnifiedMap<String, String> paramMap) {
        SocketReqMsg socketReqMsg = new SocketReqMsg(MSG_TYPE_UPDATE_ACCOUNT_CAPACITY, 0)
                .put("accountName", paramMap.get("AccountName"));
        String capacityQuota = paramMap.get("CapacityQuota");
        String softCapacityQuota = paramMap.get("SoftCapacityQuota");
        if (StringUtils.isNotBlank(capacityQuota)) {
            socketReqMsg.put("CapacityQuota", capacityQuota);
        }
        if (StringUtils.isNotBlank(softCapacityQuota)) {
            socketReqMsg.put("SoftCapacityQuota", softCapacityQuota);
        }
        return socketReqMsg;
    }

    public static SocketReqMsg buildUpdateAccountObjectsQuota(UnifiedMap<String, String> paramMap) {
        SocketReqMsg socketReqMsg = new SocketReqMsg(MSG_TYPE_UPDATE_ACCOUNT_CAPACITY, 0)
                .put("accountName", paramMap.get("AccountName"));
        String objectNumQuota = paramMap.get("ObjectNumQuota");
        String softObjectNumQuota = paramMap.get("SoftObjectNumQuota");
        if (StringUtils.isNotBlank(objectNumQuota)) {
            socketReqMsg.put("ObjnumQuota", objectNumQuota);
        }
        if (StringUtils.isNotBlank(softObjectNumQuota)) {
            socketReqMsg.put("SoftObjnumQuota", softObjectNumQuota);
        }
        return socketReqMsg;
    }

    public static SocketReqMsg buildUpdateAccountPerfQuota(UnifiedMap<String, String> paramMap) {
        SocketReqMsg socketReqMsg = new SocketReqMsg(MSG_TYPE_UPDATE_ACCOUNT_CAPACITY, 0)
                .put("accountName", paramMap.get("AccountName"));
        String perfType = paramMap.get("perfQuotaType");
        String perfValue = paramMap.get("perfQuotaValue");
        if (StringUtils.isNotBlank(perfType) && StringUtils.isNotBlank(perfValue)) {
            socketReqMsg.put("perfQuotaType", perfType)
                    .put("perfQuotaValue", perfValue);
        }
        return socketReqMsg;
    }

    public static SocketReqMsg buildGetAccountInfo(String accountName) {
        return new SocketReqMsg(MSG_TYPE_GET_ACCOUNT_INFO, 0)
                .put("accountName", accountName);
    }

    public static SocketReqMsg buildListAccountsMsg(String accountId) {
        return new SocketReqMsg(MSG_TYPE_LIST_ACCOUNTS, 0)
                .put("accountId", accountId);
    }

    public static SocketReqMsg buildCreateUserMsg(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        SocketReqMsg reqMsg = new SocketReqMsg(MSG_TYPE_CREATE_USER, 0);
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.get("UserName");
        String passWord = paramMap.get("PassWord");
        String userNickName = paramMap.getOrDefault("UserNickName", "");
        String remark = paramMap.getOrDefault("Remark", "");

        if (paramMap.containsKey(REGION_FLAG_UPPER)) {
            String regionFlag = paramMap.get(REGION_FLAG_UPPER);
            if ("1".equals(regionFlag)) {
                reqMsg.put(REGION_FLAG, regionFlag);
                reqMsg.put("userId", paramMap.get("UserId"));
                reqMsg.put("ak", paramMap.get("AccessKey"));
                reqMsg.put("sk", paramMap.get("SecretKey"));
                reqMsg.put("createTime", paramMap.get("CreateTime"));
            }
        }
        if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
            reqMsg.put(SITE_FLAG, "1");
            reqMsg.put("userId", paramMap.get("UserId"));
            reqMsg.put("ak", paramMap.get("AccessKey"));
            reqMsg.put("sk", paramMap.get("SecretKey"));
            reqMsg.put("createTime", paramMap.get("CreateTime"));
        }
        reqMsg.put("validityGrade", paramMap.get("ValidityGrade"));
        reqMsg.put("accountId", accountId);
        reqMsg.put("userName", userName);
        reqMsg.put("passWord", passWord);
        reqMsg.put("userNickName", URLEncoder.encode(userNickName, "UTF-8"));
        reqMsg.put("remark", URLEncoder.encode(remark, "UTF-8"));
        reqMsg.put("validityTime", paramMap.getOrDefault("ValidityTime", "0"));
        reqMsg.put("endTimeSecond", paramMap.get("EndTimeSecond"));

        return reqMsg;
    }

    public static SocketReqMsg buildDeleteUserMsg(String accountId, String userName) {
        return new SocketReqMsg(MSG_TYPE_DELETE_USER, 0)
                .put("msgType", "deleteUser")
                .put("accountId", accountId)
                .put("userName", userName);
    }

    public static SocketReqMsg buildUpdateUserMsg(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_UPDATE_USER, 0);
        msg.put("accountId", paramMap.get("accountId"));
        msg.put("userName", paramMap.get("UserName"));
        msg.put("newPassWord", paramMap.getOrDefault("NewPassWord", ""));
        msg.put("newRemark", URLEncoder.encode(paramMap.getOrDefault("NewRemark", ""), "utf-8"));
        msg.put("newUserNickName", URLEncoder.encode(paramMap.getOrDefault("NewUserNickName", ""), "utf-8"));
        msg.put("validityTime", paramMap.getOrDefault("ValidityTime", ""));
        msg.put("endTimeSecond", paramMap.get("EndTimeSecond"));
        msg.put("validityGrade", paramMap.get("ValidityGrade"));
        return msg;
    }

    public static SocketReqMsg buildListUsersMsg(String accountId) {
        return new SocketReqMsg(MSG_TYPE_LIST_USERS, 0)
                .put("accountId", accountId);
    }

    public static SocketReqMsg buildGetUserMsg(String accountId, String userName) {
        return new SocketReqMsg(MSG_TYPE_GET_USER, 0)
                .put("accountId", accountId)
                .put("userName", userName);
    }

    public static SocketReqMsg buildCreateAccessKeyMsg(String accountId, String userName) {
        return new SocketReqMsg(MSG_TYPE_CREATE_ACCESSKEY, 0)
                .put("accountId", accountId)
                .put("userName", userName);
    }

    public static SocketReqMsg buildDeleteAccessKeyMsg(String accountId, String userName, String accessKeyId) {
        return new SocketReqMsg(MSG_TYPE_DELETE_ACCESSKEY_USER, 0)
                .put("accountId", accountId)
                .put("userName", userName)
                .put("accessKeyId", accessKeyId);
    }

    public static SocketReqMsg buildCreateAccessKeyMsg(UnifiedMap<String, String> paramMap) {
        SocketReqMsg reqMsg = new SocketReqMsg(MSG_TYPE_CREATE_ACCESSKEY_USER, 0);
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.get("UserName");
        String ak = paramMap.getOrDefault("AccessKey", "");
        String sk = paramMap.getOrDefault("SecretKey", "");

        dealSyncFlag(paramMap, reqMsg);
        reqMsg.put("accountId", accountId);
        reqMsg.put("userName", userName);
        if (!"".equals(ak) || !"".equals(sk)) {
            reqMsg.put("AccessKey", ak);
            reqMsg.put("SecretKey", sk);
        }

        return reqMsg;
    }

    private static void dealSyncFlag(UnifiedMap<String, String> paramMap, SocketReqMsg reqMsg) {
        if (paramMap.containsKey(REGION_FLAG_UPPER)) {
            String regionFlag = paramMap.get(REGION_FLAG_UPPER);
            if ("1".equals(regionFlag)) {
                reqMsg.put(REGION_FLAG, regionFlag);
                reqMsg.put("ak", paramMap.get("AccessKey"));
                reqMsg.put("sk", paramMap.get("SecretKey"));
                reqMsg.put("createTime", paramMap.get("CreateTime"));
            }
        }
        if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
            reqMsg.put(SITE_FLAG, "1");
            reqMsg.put("ak", paramMap.get("AccessKey"));
            reqMsg.put("sk", paramMap.get("SecretKey"));
            reqMsg.put("createTime", paramMap.get("CreateTime"));
        }
    }

    public static SocketReqMsg buildListAccessKeysMsg(String accountId, String userName) {
        return new SocketReqMsg(MSG_TYPE_LIST_ACCESSKEYS, 0)
                .put("accountId", accountId)
                .put("userName", userName);
    }

    public static SocketReqMsg buildListAccountAccessKeysMsg(String accountId, String AccountName) {
        return new SocketReqMsg(MSG_TYPE_LIST_ACCESSKEYS_ACCOUNT, 0)
                .put("accountId", accountId)
                .put("accountName", AccountName);
    }

    public static SocketReqMsg buildCreateGroupMsg(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        SocketReqMsg reqMsg = new SocketReqMsg(MSG_TYPE_CREATE_GROUP, 0);
        String accountId = paramMap.get(USER_ID);
        String groupName = paramMap.get("GroupName");
        String groupNickName = URLEncoder.encode(paramMap.getOrDefault("GroupNickName", ""), "UTF-8");
        String remark = URLEncoder.encode(paramMap.getOrDefault("Remark", ""), "UTF-8");

        if (paramMap.containsKey(REGION_FLAG_UPPER)) {
            String regionFlag = paramMap.get(REGION_FLAG_UPPER);
            if ("1".equals(regionFlag)) {
                reqMsg.put(REGION_FLAG, regionFlag);
                reqMsg.put("groupId", paramMap.get("GroupId"));
                reqMsg.put("createTime", paramMap.get("CreateTime"));
            }
        }
        if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
            reqMsg.put(SITE_FLAG, "1");
            reqMsg.put("groupId", paramMap.get("GroupId"));
            reqMsg.put("createTime", paramMap.get("CreateTime"));
        }
        reqMsg.put("accountId", accountId);
        reqMsg.put("groupName", groupName);
        reqMsg.put("groupNickName", groupNickName);
        reqMsg.put("remark", remark);

        return reqMsg;
    }

    public static SocketReqMsg buildDeleteGroupMsg(String accountId, String groupName) {
        return new SocketReqMsg(MSG_TYPE_DELETE_GROUP, 0)
                .put("accountId", accountId)
                .put("groupName", groupName);
    }

    public static SocketReqMsg buildUpdateGroupMsg(String accountId, String groupName, String newGroupNickName, String newRemark) throws UnsupportedEncodingException {
        return new SocketReqMsg(MSG_TYPE_UPDATE_GROUP, 0)
                .put("accountId", accountId)
                .put("groupName", groupName)
                .put("newGroupNickName", URLEncoder.encode(newGroupNickName, "UTF-8"))
                .put("newRemark", URLEncoder.encode(newRemark, "UTF-8"));
    }

    public static SocketReqMsg buildListGroupsMsg(String accountId) {
        return new SocketReqMsg(MSG_TYPE_LIST_GROUPS, 0)
                .put("accountId", accountId);
    }

    public static SocketReqMsg buildGetGroupMsg(String accountId, String groupName) {
        return new SocketReqMsg(MSG_TYPE_GET_GROUP, 0)
                .put("accountId", accountId)
                .put("groupName", groupName);
    }

    public static SocketReqMsg buildAddUsersToGroupsMsg(String accountId, String userNames, String groupNames) {
        return new SocketReqMsg(MSG_TYPE_ADD_USERS_TO_GROUPS, 0)
                .put("accountId", accountId)
                .put("userNames", userNames)
                .put("groupNames", groupNames);
    }

    public static SocketReqMsg buildRemoveUserFromGroupMsg(String accountId, String userName, String groupName) {
        return new SocketReqMsg(MSG_TYPE_REMOVE_USER_FROM_GROUP, 0)
                .put("accountId", accountId)
                .put("userName", userName)
                .put("groupName", groupName);
    }

    public static SocketReqMsg buildListUsersForGroupMsg(String accountId, String groupName) {
        return new SocketReqMsg(MSG_TYPE_LIST_USERS_FOR_GROUP, 0)
                .put("accountId", accountId)
                .put("groupName", groupName);
    }

    public static SocketReqMsg buildListGroupsForUserMsg(String accountId, String userName) {
        return new SocketReqMsg(MSG_TYPE_LIST_GROUPS_FOR_USER, 0)
                .put("accountId", accountId)
                .put("userName", userName);
    }

    public static SocketReqMsg buildCreatePolicyMsg(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        SocketReqMsg reqMsg = new SocketReqMsg(MSG_TYPE_CREATE_POLICY, 0);
        String accountId = paramMap.get(USER_ID);
        String policyName = paramMap.get("PolicyName");
        String remark = URLEncoder.encode(paramMap.getOrDefault("Remark", ""), "UTF-8");
        String policyDocument = URLEncoder.encode(paramMap.getOrDefault("PolicyDocument", ""), "UTF-8");

        if (paramMap.containsKey(REGION_FLAG_UPPER)) {
            String regionFlag = paramMap.get(REGION_FLAG_UPPER);
            if ("1".equals(regionFlag)) {
                reqMsg.put(REGION_FLAG, regionFlag);
                reqMsg.put("policyId", paramMap.get("PolicyId"));
                reqMsg.put("createTime", paramMap.get("CreateTime"));
            }
        }
        if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
            reqMsg.put(SITE_FLAG, "1");
            reqMsg.put("policyId", paramMap.get("PolicyId"));
            reqMsg.put("createTime", paramMap.get("CreateTime"));
        }
        reqMsg.put("accountId", accountId);
        reqMsg.put("policyName", policyName);
        reqMsg.put("policyDocument", policyDocument);
        reqMsg.put("remark", remark);

        return reqMsg;
    }

    public static SocketReqMsg buildDeletePolicyMsg(String accountId, String policyName) {
        return new SocketReqMsg(MSG_TYPE_DELETE_POLICY, 0)
                .put("msgType", "deleteUser")
                .put("accountId", accountId)
                .put("policyName", policyName);
    }

    public static SocketReqMsg buildUpdatePolicyMsg(String accountId, String policyName, String newPolicyDocument, String newRemark) throws UnsupportedEncodingException {
        return new SocketReqMsg(MSG_TYPE_UPDATE_POLICY, 0)
                .put("accountId", accountId)
                .put("policyName", policyName)
                .put("newPolicyDocument", URLEncoder.encode(newPolicyDocument, "UTF-8"))
                .put("newRemark", URLEncoder.encode(newRemark, "UTF-8"));
    }

    public static SocketReqMsg buildListPoliciesMsg(String accountId) {
        return new SocketReqMsg(MSG_TYPE_LIST_POLICIES, 0)
                .put("accountId", accountId);
    }

    public static SocketReqMsg buildGetPolicyMsg(String accountId, String policyName) {
        return new SocketReqMsg(MSG_TYPE_GET_POLICY, 0)
                .put("accountId", accountId)
                .put("policyName", policyName);
    }

    public static SocketReqMsg buildListEntitiesForPolicyMsg(String accountId, String policyName) {
        return new SocketReqMsg(MSG_TYPE_LIST_ENTITIES_FOR_POLICY, 0)
                .put("accountId", accountId)
                .put("policyName", policyName);
    }

    public static SocketReqMsg buildAttachEntityPolicyMsg(String accountId, String entityNames, String policyNames, String entityType) {
        return new SocketReqMsg(MSG_TYPE_ATTACH_ENTITY_POLICY, 0)
                .put("accountId", accountId)
                .put("entityNames", entityNames)
                .put("policyNames", policyNames)
                .put("entityType", entityType);
    }

    public static SocketReqMsg buildDetachEntityPolicyMsg(String accountId, String entityName, String policyName, String entityType) {
        return new SocketReqMsg(MSG_TYPE_DETACH_ENTITY_POLICY, 0)
                .put("accountId", accountId)
                .put("entityName", entityName)
                .put("policyName", policyName)
                .put("entityType", entityType);
    }

    public static SocketReqMsg buildListEntityPoliciesMsg(String accountId, String entityName, String entityType) {
        return new SocketReqMsg(MSG_TYPE_LIST_ENTITY_POLICIES, 0)
                .put("accountId", accountId)
                .put("entityName", entityName)
                .put("entityType", entityType);
    }

//    public static SocketReqMsg buildAttachGroupPolicyMsg(String accountId, String groups, String policies) {
//        return new SocketReqMsg(MSG_TYPE_ATTACH_GROUP_POLICY, 0)
//                .put("accountId", accountId)
//                .put("groups", groups)
//                .put("policies", policies);
//    }
//
//    public static SocketReqMsg buildDetachGroupPolicyMsg(String accountId, String groupName, String policyName) {
//        return new SocketReqMsg(MSG_TYPE_DETACH_GROUP_POLICY, 0)
//                .put("accountId", accountId)
//                .put("groupName", groupName)
//                .put("policyName", policyName);
//    }
//
//    public static SocketReqMsg buildListGroupPoliciesMsg(String accountId, String groupName) {
//        return new SocketReqMsg(MSG_TYPE_LIST_GROUP_POLICIES, 0)
//                .put("accountId", accountId)
//                .put("groupName", groupName);
//    }

    public static SocketReqMsg buildGetAccountAuthorizationDetailsMsg(String accountId) {
        return new SocketReqMsg(MSG_TYPE_GET_ACCOUNT_AUTHORIZATION_DETAILS, 0)
                .put("accountId", accountId);
    }

    public static void addSyncFlagParam(SocketReqMsg msg, UnifiedMap<String, String> paramMap) {
        if (paramMap.containsKey("RegionFlag") || paramMap.containsKey(SITE_FLAG_UPPER)) {
            String regionFlag = paramMap.get("RegionFlag");
            if ("1".equals(regionFlag)) {
                msg.put("regionFlag", regionFlag);
            }
            if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
                msg.put(SITE_FLAG, "1");
            }
        }
    }

    public static void addReqRegionFlag(SocketReqMsg msg, UnifiedMap<String, String> paramMap) {
        if (paramMap.containsKey("RegionFlag")) {
            String regionFlag = paramMap.get("RegionFlag");
            if ("1".equals(regionFlag)) {
                msg.put("regionFlag", regionFlag);
            }
        }
    }

    public static SocketReqMsg buildCreateRoleMsg(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        SocketReqMsg reqMsg = new SocketReqMsg(MSG_TYPE_CREATE_ROLE, 0)
                .put("roleName", paramMap.get(ROLE_NAME))
                .put("description", URLEncoder.encode(paramMap.getOrDefault("description", ""), "utf-8"))
                .put("maxSessionDuration", paramMap.get("maxSessionDuration"))
                .put("assumeRolePolicyDocument", paramMap.get("assumeRolePolicyDocument"))
                .put("accountId", paramMap.get("accountId"))
                .put("createDate", paramMap.get("createDate"))
                .put("mrn", paramMap.get("mrn"));
        if (paramMap.containsKey(REGION_FLAG_UPPER) || paramMap.containsKey(SITE_FLAG_UPPER)) {
            if ("1".equals(paramMap.get(REGION_FLAG_UPPER))) {
                reqMsg.put(REGION_FLAG, "1");
            }
            if ("1".equals(paramMap.get(SITE_FLAG_UPPER))) {
                reqMsg.put(SITE_FLAG, "1");
            }
            reqMsg.put("policyId", paramMap.get("PolicyId")).put("roleId", paramMap.get("RoleId"));
        }
        return reqMsg;
    }
}
