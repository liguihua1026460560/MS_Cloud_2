package com.macrosan.action.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.socketmsg.SocketSender;
import com.macrosan.message.xmlmsg.tagging.Tag;
import com.macrosan.message.xmlmsg.tagging.TagSet;
import com.macrosan.message.xmlmsg.tagging.Tagging;
import com.macrosan.utils.functional.TiConsumer;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.serialize.JaxbUtils;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.HeartBeatChecker.isMultiAliveStarted;
import static com.macrosan.doubleActive.HeartBeatChecker.syncPolicy;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.*;
import static com.macrosan.httpserver.ResponseUtils.*;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.macrosan.utils.msutils.MsAclUtils.getAclNum;
import static com.macrosan.utils.regex.PatternConst.TAG_PATTERN;

/**
 * BaseService
 * <p>
 * 所有Service的父类，提供一些公共方法
 *
 * @author liyixin
 * @date 2018/12/23
 */
@Log4j2
public class BaseService {

    protected static RedisConnPool pool = RedisConnPool.getInstance();

    protected static SocketSender sender = SocketSender.getInstance();

    protected static ServerConfig config = ServerConfig.getInstance();

    protected static final String REGION = config.getRegion();

    protected static final String SITE = config.getSite();

    public BaseService() {
    }

    /**
     * 提取出查询redis的流程(响应式版本)
     * <p>
     * <p>
     * 先根据 vnodeId查询vnodeInfo，然后根据vnodeInfo中的s_uuid查询targetNodeInfo，然后将两个Map聚合到一起返回
     * <p>
     * 注：额外查询了节点状态，如果节点状态异常则将消息发到对端节点
     *
     * @param msg     要发送的消息
     * @param vnodeId vnode编号
     * @return 目标节点的信息
     */
    protected Mono<Tuple2<Map<String, String>, FastList<String>>> getTargetInfoReactive(String vnodeId, SocketReqMsg msg) {
        return pool.getReactive(REDIS_MAPINFO_INDEX).hgetall(vnodeId)
                .zipWhen(vnodeInfo -> {
                    String uuid = vnodeInfo.get("s_uuid");
                    return pool.getReactive(REDIS_NODEINFO_INDEX)
                            .hget(uuid, NODE_SERVER_STATE)
                            .flatMap(state -> {
                                msg.put("state", state).put("vnode", vnodeId);
                                return "0".equals(state) ?
                                        pool.getReactive(REDIS_NODEINFO_INDEX).hget(uuid, "opp_uuid") :
                                        Mono.just(uuid);
                            })
                            .flatMapMany(targetUuid -> pool.getReactive(REDIS_NODEINFO_INDEX).hmget(targetUuid, HEART_ETH1, HEART_ETH2))
                            .collect(() -> new FastList<String>(2), (list, keyValue) -> list.add(keyValue.getValue()));
                }, Tuple2::new);
    }

    /**
     * 提取出查询redis的流程(响应式版本)
     * <p>
     * 先根据 vnodeId查询vnodeInfo，然后根据vnodeInfo中的s_uuid查询targetNodeInfo中的ip
     *
     * @param vnodeId vnode编号
     * @return 目标节点的信息
     */
    protected Mono<SocketReqMsg> getTargetIpReactive(SocketReqMsg msg, String vnodeId) {
        return pool.getReactive(REDIS_MAPINFO_INDEX)
                .hmget(vnodeId, "s_uuid", "lun_name", "take_over")
                .collect(() -> msg, (tmpMsg, keyValue) -> tmpMsg.put(keyValue.getKey(), keyValue.getValue()))
                .flatMapMany(resMsg -> pool.getReactive(REDIS_NODEINFO_INDEX).hmget(resMsg.getAndRemove("s_uuid"), HEART_ETH1, HEART_ETH2))
                .doOnNext(resultIp -> {
                    if (StringUtils.isBlank(resultIp.getValue())) {
                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "getUploadPartPath error, can not get target_uuid node info");
                    }
                })
                .collect(() -> msg, (tmpMsg, keyValue) -> tmpMsg.put(keyValue.getKey(), keyValue.getValue()));
    }

    /**
     * 提取出查询redis的流程（同步阻塞版本）
     * <p>
     * 先根据 vnodeId查询vnodeInfo，然后根据vnodeInfo中的s_uuid查询targetNodeInfo，然后将两个Map聚合到一起返回
     *
     * @param vnodeId vnode编号
     * @return 目标节点的信息
     */
    protected MergeMap<String, String> getTargetInfo(String vnodeId) {
        Map<String, String> vnodeInfo = pool.getCommand(REDIS_MAPINFO_INDEX).hgetall(vnodeId);
        if (vnodeInfo.isEmpty()) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "No such vnode.");
        }

        String targetUuid = vnodeInfo.get("s_uuid");
        Map<String, String> targetNodeMap = pool.getCommand(REDIS_NODEINFO_INDEX).hgetall(targetUuid);
        if (targetNodeMap.isEmpty()) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get the node info failed.");
        }

        return new MergeMap<>(vnodeInfo, targetNodeMap);
    }

    /**
     * 基础的检测，先看bucketinfo是否为空，然后用给定的consumer检查bucketinfo、用户id和桶名字
     *
     * @param userId     用户id
     * @param bucketName 桶名字
     * @param consumer   用于检测权限的方法
     */
    protected void baseCheck(String userId, String bucketName, TiConsumer<Map<String, String>, String, String> consumer) {
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        if (consumer != null) {
            consumer.apply(bucketInfo, userId, bucketName);
        }
    }

    /**
     * 根据用户的id获取accountMap,入股accountMap为空，则抛出异常
     *
     * @param userId 用户id
     * @return accountMap
     */
    protected Map<String, String> getAccountMapById(String userId) {
        String userName = pool.getCommand(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_ID_NAME);
        Map<String, String> accountMap = pool.getCommand(REDIS_USERINFO_INDEX).hgetall(userName);
        if (accountMap.isEmpty()) {
            throw new MsException(NO_SUCH_ACCOUNT, "no such account. account_name: " + userName);
        }
        return accountMap;
    }

    /**
     * 根据桶名字获取bucketMap，如果bucketMap为空则抛出异常
     *
     * @param bucketName 桶名字
     * @return bucketMap
     */
    protected Map<String, String> getBucketMapByName(String bucketName) {
        Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketInfo.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        } else if (!REGION.equals(bucketInfo.get(REGION_NAME))) {
            throw new MsException(ErrorNo.INVALID_LOCATION_CONSTRAINT, "the bucket does not belong to the current region.");
        } else if (!isSwitchOn(bucketInfo) && !SITE.equals(bucketInfo.get(CLUSTER_NAME))) {
            throw new MsException(INVALID_SITE_CONSTRAINT, "the bucket does not belong to the current site.");
        }
        return bucketInfo;
    }

    /**
     * 根据桶名字获取bucketMap，如果bucketMap为空或者桶不属于该区域 则抛出异常
     *
     * @param paramMap 请求参数
     * @return bucketMap
     */
    protected Map<String, String> getBucketMapByName(UnifiedMap<String, String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketInfo.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        } else {
            String localRegion = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MULTI_REGION_LOCAL_REGION, REGION_NAME);
            if (!localRegion.equals(bucketInfo.get(REGION_NAME)) && !paramMap.containsKey(REGION_FLAG.toLowerCase())) {
                throw new MsException(ErrorNo.INVALID_LOCATION_CONSTRAINT, "the bucket does not belong to the current region.");
            }
        }
        return bucketInfo;
    }

    public Mono<Boolean> createObjAcl(MsHttpRequest request, JsonObject aclJson) {
        final MonoProcessor<Boolean> processor = MonoProcessor.create();
        List<String> idList = new LinkedList<>();
        for (Map.Entry<String, String> entry : request.headers().entries()) {
            String k = entry.getKey().toLowerCase();
            String v = entry.getValue().toLowerCase();
            if ("x-amz-acl".equals(k)) {
                v = MsAclUtils.objAclCheck(v);
            }

            if (k.startsWith(GRANT_ACL)) {
                for (String value : v.split(",")) {
                    String[] id = value.split("=", 2);
                    if (id.length != 2) {
                        processor.onNext(false);
                        throw new MsException(INVALID_ARGUMENT, "acl input error.");
                    }

                    if (!PERMISSION_READ_LONG.equals(k)
                            && !PERMISSION_READ_CAP_LONG.equals(k)
                            && !PERMISSION_WRITE_CAP_LONG.equals(k)
                            && !PERMISSION_FULL_CON_LONG.equals(k)) {
                        processor.onNext(false);
                        throw new MsException(NO_SUCH_OBJECT_PERMISSION, "No such object permission.");
                    }
                }
            }
        }
        int acl = request.headers().contains("x-amz-acl") ?
                getAclNum(request.getHeader("x-amz-acl")) :
                OBJECT_PERMISSION_PRIVATE_NUM;

        String id = request.getHeader(PERMISSION_READ_LONG);
        if (StringUtils.isNotBlank(id)) {
            String[] ids = id.split(",");
            for (String str : ids) {
                if (StringUtils.isNotBlank(str)) {
                    String[] split = str.split("=");
                    if (split.length > 1) {
                        idList.add(split[1]);
                        if (acl != OBJECT_PERMISSION_SHARE_BUCKET_OWNER_READ_NUM) {
                            aclJson.put(OBJECT_PERMISSION_READ + '-' + split[1], OBJECT_PERMISSION_READ);
                            acl |= OBJECT_PERMISSION_READ_NUM;
                        }
                    }
                }
            }
        }

        id = request.getHeader(PERMISSION_READ_CAP_LONG);
        if (StringUtils.isNotBlank(id)) {
            String[] ids = id.split(",");
            for (String str : ids) {
                if (StringUtils.isNotBlank(str)) {
                    String[] split = str.split("=");
                    if (split.length > 1) {
                        idList.add(split[1]);
                        aclJson.put(OBJECT_PERMISSION_READ_CAP + '-' + split[1], OBJECT_PERMISSION_READ_CAP);
                        acl |= OBJECT_PERMISSION_READ_CAP_NUM;
                    }
                }
            }
        }

        id = request.getHeader(PERMISSION_WRITE_CAP_LONG);
        if (StringUtils.isNotBlank(id)) {
            String[] ids = id.split(",");
            for (String str : ids) {
                if (StringUtils.isNotBlank(str)) {
                    String[] split = str.split("=");
                    if (split.length > 1) {
                        idList.add(split[1]);
                        aclJson.put(OBJECT_PERMISSION_WRITE_CAP + '-' + split[1], OBJECT_PERMISSION_WRITE_CAP);
                        acl |= OBJECT_PERMISSION_WRITE_CAP_NUM;
                    }
                }
            }
        }

        id = request.getHeader(PERMISSION_FULL_CON_LONG);
        if (StringUtils.isNotBlank(id)) {
            String[] ids = id.split(",");
            for (String str : ids) {
                if (StringUtils.isNotBlank(str)) {
                    String[] split = str.split("=");
                    if (split.length > 1) {
                        idList.add(split[1]);
                        acl |= OBJECT_PERMISSION_FULL_CON_NUM;
                        aclJson.put(OBJECT_PERMISSION_FULL_CON + '-' + split[1], OBJECT_PERMISSION_FULL_CON);

                        // if ((acl & OBJECT_PERMISSION_SHARE_READ_NUM) == 0 || acl != OBJECT_PERMISSION_SHARE_BUCKET_OWNER_READ_NUM) {
                        //     acl |= OBJECT_PERMISSION_READ_NUM;
                        //     aclJson.put(OBJECT_PERMISSION_READ + '-' + split[1], OBJECT_PERMISSION_READ);
                        // }
                        //
                        // acl |= OBJECT_PERMISSION_READ_CAP_NUM;
                        // aclJson.put(OBJECT_PERMISSION_READ_CAP + '-' + split[1], OBJECT_PERMISSION_READ_CAP);
                        //
                        // acl |= OBJECT_PERMISSION_WRITE_CAP_NUM;
                        // aclJson.put(OBJECT_PERMISSION_WRITE_CAP + '-' + split[1], OBJECT_PERMISSION_WRITE_CAP);
                    }
                }
            }
        }


        aclJson.put("acl", Integer.toString(acl));
        aclJson.put("owner", request.getUserId());

        if (idList.isEmpty()) {
            processor.onNext(true);
        } else {
            pool.getReactive(REDIS_USERINFO_INDEX)
                    .exists(idList.toArray(new String[0]))
                    .subscribe(count -> processor.onNext(count == idList.size()));
        }

        return processor.filter(authority -> {
            if (!authority) {
                throw new MsException(NO_SUCH_ID, "no such user id.");
            }
            return authority;
        });
    }

    /**
     * 获取用户元数据，即x-amz-meta-开头的http请求头。收集到map中后转换成json字符串。
     * <p>
     * 注：请求头的遍历最优情况下应该是放在鉴权过程中处理，仅需遍历一次。但考虑到把过多的流程揉到鉴权流程中
     * 会使代码难以维护，以此换来微不足道的性能提升有点得不偿失。
     * <p>
     * TODO 改用byte[] 减少拷贝
     *
     * @param request http请求
     * @return 用户元数据
     */
    public JsonObject getUserMeta(HttpServerRequest request) {
        List<Map.Entry<String, String>> headerList = request.headers().entries();
        JsonObject result = new JsonObject();
        headerList.forEach(entry -> {
            final String key = entry.getKey();
            final String lowerKey = entry.getKey().toLowerCase();
            try {
                if (lowerKey.startsWith(USER_META)) {
                    result.put(new String(key.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8),
                            new String(entry.getValue().getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8));
                }
            } catch (IllegalArgumentException e) {
                log.info("URLDecode error : {}", e.getMessage());
                result.put(key, entry.getValue());
            }
        });
        String resStr = result.encode();
        if (resStr.length() > META_USR_MAX_SIZE) {
            throw new MsException(ErrorNo.META_DATA_TOO_LARGE, "user meta is too long , user meta size :" + resStr.length());
        }

        return result;
    }

    protected JsonObject getSysMetaMap(HttpServerRequest request) {
        JsonObject res = new JsonObject();
        request.headers().forEach(entry -> {
            final String key = entry.getKey().toLowerCase();
            if ("content-length".equals(key) || SPECIAL_HEADER.contains(key.hashCode())) {
                res.put(entry.getKey(), entry.getValue());
            }

            if (NO_SYNCHRONIZATION_KEY.equals(key) && NO_SYNCHRONIZATION_VALUE.equals(entry.getValue())) {
                res.put(entry.getKey(), entry.getValue());
            }
        });
        if (StringUtils.isEmpty(request.headers().get(CONTENT_TYPE))) {
            res.put(CONTENT_TYPE, "application/octet-stream");
        }
        return res;
    }


    protected void checkMetaData(String objName, MetaData meta, String versionId, String currentSnapshotMark) {
        if (meta.equals(ERROR_META)) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get Object Meta Data fail");
        }

        if (meta.equals(NOT_FOUND_META) || meta.deleteMark || (StringUtils.isEmpty(versionId) && meta.deleteMarker) || meta.isUnView(currentSnapshotMark)) {
            if (!StringUtils.isEmpty(versionId)) {
                throw new MsException(ErrorNo.NO_SUCH_VERSION, "The specified key version does not exist.");
            } else {
                throw new MsException(ErrorNo.NO_SUCH_OBJECT, "no such object, object name: " + objName + ".");
            }
        }

        if (StringUtils.isNotEmpty(versionId) && meta.deleteMarker) {
            throw new MsException(ErrorNo.METHOD_NOT_ALLOWED, "The specified method is not allowed against this resource.");
        }
    }

    protected void userCheck(String userId, String bucket) {
        Map<String, String> bucketInfo = getBucketMapByName(bucket);
        if (!userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.ACCESS_FORBIDDEN,
                    "no permission.user " + userId + " can not configure " + bucket + " worm.");
        }
    }

    /**
     * @param contentMD5 请求头中contentMd5
     * @param md5        实际数据md5
     */
    protected void checkMd5(String contentMD5, String md5) {
        try {
            String MD5 = Hex.encodeHexString(Base64.getDecoder().decode(contentMD5));
            if (!MD5.equals(md5)) {
                log.error("md5 verify fail. md5Client:{},md5ServerEncoding:{}", contentMD5, md5);
                throw new MsException(ErrorNo.INVALID_MD5, "");
            }
        } catch (Exception e) {
            log.error("md5 verify fail. md5Client:{},md5ServerEncoding:{}", contentMD5, md5);
            throw new MsException(ErrorNo.INVALID_MD5, "");
        }
    }

    /**
     * 检测消息体中的Content-MD5值
     *
     * @param request 请求参数
     */
    protected static void checkContentMD5(MsHttpRequest request, String body) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        String contentMD5 = request.getHeader("Content-Md5");
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        Base64.Encoder encoder = Base64.getEncoder();
        String MD5 = encoder.encodeToString(messageDigest.digest(body.getBytes("utf-8")));
        if (!MD5.equals(contentMD5)) {
            throw new MsException(ErrorNo.INVALID_MD5, "invalid content-md5:" + contentMD5 + ", md5:" + MD5);
        }
    }

    /**
     * 构造返回的xml以及消息头等
     *
     * @param request   请求
     * @param requestId 请求id
     * @param xml       构造的xml对象
     */
    protected void constructResult(MsHttpRequest request, String requestId, Object xml) {
        byte[] res = JaxbUtils.toByteArray(xml);
        addPublicHeaders(request, requestId)
                .putHeader(CONTENT_TYPE, "application/xml")
                .putHeader(CONTENT_LENGTH, String.valueOf(res.length))
                .write(Buffer.buffer(res));
        addAllowHeader(request.response()).end();
    }

    /**
     * 上传前的区域检查
     *
     * @param region 桶所在区域
     */
    protected void regionCheck(String region) {
        if (StringUtils.isNotEmpty(region) && !REGION.equals(region)) {
            throw new MsException(ErrorNo.INVALID_LOCATION_CONSTRAINT, "the bucket does not belong to the current region.");
        }
    }

    /**
     * 上传前的站点检查
     */
    protected void siteCheck(Map<String, String> bucketInfo) {
        String site = bucketInfo.get(CLUSTER_NAME);
        String datasync = bucketInfo.get(DATA_SYNC_SWITCH);
        siteCheck(site, datasync);
    }

    /**
     * 上传前的站点检查
     */
    protected void siteCheck(String site, String datasync) {
        if (SWITCH_ON.equals(datasync) || SWITCH_SUSPEND.equals(datasync)) {
            return;
        }
        if (StringUtils.isNotEmpty(site) && !SITE.equals(site)) {
            throw new MsException(INVALID_SITE_CONSTRAINT, "the bucket does not belong to the current site.");
        }
    }

    /**
     * copy接口上传前的站点检查
     */
    protected void copySyncCheck(String sourceBuc, String targetBuc) {
        if ("on".equals(sourceBuc)) {
            return;
        }

        if (isMultiAliveStarted && "on".equals(targetBuc) && "0".equals(syncPolicy)) {
            throw new MsException(SOURCE_BUCKET_NO_POLICY, "the source bucket does not open data sync switch.");
        }
    }


    /**
     * 根据http版本和Expect头参数返回100-continue
     *
     * @param request 请求
     */
    public void responseContinue(io.vertx.core.http.HttpServerRequest request) {
        String expect = request.getHeader(EXPECT);
        if (expect != null
                && HttpVersion.HTTP_1_0.compareTo(request.version()) < 0
                && expect.toLowerCase().startsWith(EXPECT_100_CONTINUE)) {
            request.response().writeContinue();
        }
    }

    protected static String getWebAddr() {
        return pool.getCommand(REDIS_SYSINFO_INDEX).get("webaddr");
    }

    protected void checkIfMatch(Map<String, String> sysMetaMap, Map<String, String> heads, boolean lengthCheck) {
        String eTag = sysMetaMap.get("ETag");
        String lastModified = sysMetaMap.get("Last-Modified");

        //处理copyPart 不带range时,源对象大于5G x-amz-copy-source-range,copy对象时不含有该字段,修改不受影响
        if (lengthCheck && Long.parseLong(sysMetaMap.get(CONTENT_LENGTH)) > MAX_PUT_SIZE) {
            if (!heads.containsKey("x-amz-copy-source-range")) {
                throw new MsException(SOURCE_TOO_LARGE, "the size of object is too large");
            }
        }

        if (heads.get(X_AMZ_COPY_SOURCE_IF_MATCH) != null) {
            String match = heads.get(X_AMZ_COPY_SOURCE_IF_MATCH);
            if (!match.equals(eTag)) {
                throw new MsException(ErrorNo.PRECONDITION_FAILED, "source: " + eTag + ", headers: " + match);
            }
        }

        if (heads.get(X_AMZ_COPY_SOURCE_IF_NONE_MATCH) != null) {
            String noneMatch = heads.get(X_AMZ_COPY_SOURCE_IF_NONE_MATCH);
            if (noneMatch.equals(eTag)) {
                throw new MsException(ErrorNo.PRECONDITION_FAILED, "source: " + eTag + ", headers: " + noneMatch);
            }
        }

        Long lastModifiedStamp = MsDateUtils.dateToStamp(lastModified) + GMT_TO_LOCAL;

        if (heads.get(X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE) != null) {
            String modifiedSince = heads.get(X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE);
            Long sinceStamp = MsDateUtils.dateToStamp(modifiedSince) + GMT_TO_LOCAL;
            if (modifiedSince.length() == 19) {
                sinceStamp = MsDateUtils.dateToStamp(modifiedSince);
            }

            if (lastModifiedStamp <= sinceStamp) {
                throw new MsException(ErrorNo.PRECONDITION_FAILED, "x-amz-copy-source-if-modified-since :" + sinceStamp);
            }
        }

        if (heads.get(X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE) != null) {
            String unmodifiedSince = heads.get(X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE);
            Long unmodifiedSinceStamp = MsDateUtils.dateToStamp(unmodifiedSince) + GMT_TO_LOCAL;
            if (unmodifiedSince.length() == 19) {
                unmodifiedSinceStamp = MsDateUtils.dateToStamp(unmodifiedSince);
            }

            if (lastModifiedStamp > unmodifiedSinceStamp || unmodifiedSinceStamp == 0) {
                throw new MsException(ErrorNo.PRECONDITION_FAILED, "x-amz-copy-source-if-unmodified-since :" + unmodifiedSinceStamp);
            }
        }
    }

    protected void checkCondition(Map<String, String> heads, MsHttpRequest request) {
        request.headers().forEach(entry -> {
            String key = entry.getKey().toLowerCase();
            heads.put(key, entry.getValue());
        });

        if ((heads.get(X_AMZ_COPY_SOURCE_IF_MATCH) != null && heads.get(X_AMZ_COPY_SOURCE_IF_NONE_MATCH) != null) ||
                (heads.get(X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE) != null && heads.get(X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE) != null)) {
            throw new MsException(ErrorNo.PRECONDITION_FAILED, "Copy conditions conflict.");
        }
    }

    protected boolean isFromSyncing(MsHttpRequest request) {
        return isMultiAliveStarted && request.headers().contains(IS_SYNCING);
    }

    public static boolean isCurrentTimeWithinRange(String start, String end) {
        if (StringUtils.isEmpty(start) && StringUtils.isEmpty(end)) {
            return true;
        }
        // 开始时间和结束时间相同，表示全天
        if (StringUtils.equals(start,end)){
            return true;
        }

        String startMin = "0";
        String startSec = "0";
        String endMin = "0";
        String endSec = "0";
        if (StringUtils.isNotEmpty(start)) {
            startMin = start.split(":")[0];
            startSec = start.split(":")[1];
        }
        if (StringUtils.isNotEmpty(end)) {
            endMin = end.split(":")[0];
            endSec = end.split(":")[1];
        }
        // 定义开始时间和结束时间
        LocalTime startTime = LocalTime.of(Integer.parseInt(startMin), Integer.parseInt(startSec)); // 开始时间 09:00
        LocalTime endTime = LocalTime.of(Integer.parseInt(endMin), Integer.parseInt(endSec)); // 结束时间 17:00

        // 获取当前时间
        LocalTime currentTime = LocalTime.now();

        // 如果结束时间小于开始时间，说明是跨天的情况
        if (endTime.isBefore(startTime)) {
            return currentTime.isAfter(startTime) || currentTime.isBefore(endTime);
        }

        // 判断当前时间是否在时间段内
        return currentTime.isAfter(startTime) && currentTime.isBefore(endTime);
    }

    public static void assertRequestBodyNotEmpty(String body) {
        if (StringUtils.isEmpty(body)) {
            throw new MsException(MISSING_REQUEST_BODY, "request body is empty");
        }
    }

    public static void assertRequestBodyNotEmpty(Buffer body) {
        if (body == null || body.length() == 0) {
            throw new MsException(MISSING_REQUEST_BODY, "request body is empty");
        }
    }

    public static void assertParseXmlError(Object body) {
        if (body == null) {
            throw new MsException(ErrorNo.MALFORMED_XML, "Parsing xml error");
        }
    }

    /**
     * 检查防盗链配置
     * @param bucketInfo 包含桶的配置信息，例如防盗链开启标志、是否允许空Referer、黑名单、白名单等
     * @param request 封装的请求信息
     * @return 是否允许访问
     */
    public static void referCheck(Map<String, String> bucketInfo, MsHttpRequest request) {
        // 获取防盗链配置
        String antiLeechEnabled = bucketInfo.get("antiLeechEnabled"); // 防盗链开启标志（"0"：关闭，"1"：开启）
        String allowEmptyReferer = bucketInfo.get("allowEmptyReferer"); // 是否允许空Referer（"0"：不允许，"1"：允许）
        String blackList = bucketInfo.get("blackRefererList"); // 黑名单，逗号分隔的字符串
        String whiteList = bucketInfo.get("whiteRefererList"); // 白名单，逗号分隔的字符串

        // 如果防盗链功能未开启，直接允许访问
        if ("false".equals(antiLeechEnabled) || StringUtils.isEmpty(antiLeechEnabled)) {
            return;
        }

        // 检查是否是签名URL访问
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        Map<String, List<String>> params = decoder.parameters();
        boolean isSignedUrl = params.containsKey("Signature") && params.containsKey("Expires") && params.containsKey("AccessKeyId");
        if (!isSignedUrl) {
            // 不是签名URL访问，不进行防盗链验证
            return;
        }
        // 获取请求的Referer
        String referer = request.headers().get("Referer");
        // 将黑名单和白名单转换为列表
        List<String> blackListPatterns = transformToList(blackList);
        List<String> whiteListPatterns = transformToList(whiteList);

        // 防盗链验证逻辑
        if (referer == null || referer.isEmpty()) {
            // Referer为空
            if ("true".equals(allowEmptyReferer)) {
                // 允许空Referer，允许访问
                return;
            } else {
                if (whiteListPatterns.isEmpty()) {
                    // 白名单为空，允许访问
                    return;
                } else {
                    // 白名单不为空，拒绝访问
                    throw new MsException(ErrorNo.REFERER_NOT_ALLOWED, "Referer is not allowed.");
                }
            }
        } else {
            // Referer不为空
            if (blackListPatterns.isEmpty() && whiteListPatterns.isEmpty()) {
                // 黑白名单都为空，允许访问
                return;
            }

            if (!blackListPatterns.isEmpty()) {
                // 检查黑名单
                for (String pattern : blackListPatterns) {
                    if (matchReferer(referer, pattern)) {
                        // 匹配黑名单，拒绝访问
                        throw new MsException(ErrorNo.REFERER_NOT_ALLOWED, "Referer is not allowed.");
                    }
                }
            }

            if (!whiteListPatterns.isEmpty()) {
                // 检查白名单
                for (String pattern : whiteListPatterns) {
                    if (matchReferer(referer, pattern)) {
                        // 匹配白名单，允许访问
                        return;
                    }
                }
                // 不匹配白名单，拒绝访问
                throw new MsException(ErrorNo.REFERER_NOT_ALLOWED, "Referer is not allowed.");
            } else {
                // 白名单为空，允许访问
                return;
            }
        }
    }

    /**
     * 匹配 Referer 方法
     *
     * @param referer 请求中的 Referer 地址
     * @param pattern 配置的匹配模式
     * @return 是否匹配
     */
    public static boolean matchReferer(String referer, String pattern) {
        if (referer == null || pattern == null) {
            return false;
        }

        // 转义正则表达式特殊字符
        String escapedPattern = pattern.replaceAll("([\\.\\[\\]\\{\\}\\(\\)\\^\\$\\+\\|\\-\\\\])", "\\\\$1");

        // 将通配符替换为正则表达式
        String regexPattern = "(?i)^" + escapedPattern.replace("*", ".*").replace("?", ".") + ".*$";

        // 编译正则表达式，不区分大小写
        return referer.matches(regexPattern);
    }

    public static List<String> transformToList(String jsonString) {
        if (StringUtils.isEmpty(jsonString)){
            return new ArrayList<>();
        }
        // 1. 去掉首尾的方括号
        String trimmed = jsonString.substring(1, jsonString.length() - 1);

        // 2. 按逗号分割，并去掉可能存在的空格
        String[] array = trimmed.split("\\s*,\\s*");

        // 3. 转为List
        return Arrays.asList(array);
    }

    public static void checkTagging(Tagging tagging){
        if (tagging.getTagSet() == null){
            throw new MsException(ErrorNo.INVALID_TAG, "the tag set is empty");
        }
        if (tagging.getTagSet().getTags() == null){
            tagging.getTagSet().setTags(new ArrayList<>());
        }
        AtomicInteger count = new AtomicInteger(0);
        Set<String> keySet = new HashSet<>();
        if (!tagging.getTagSet().getTags().isEmpty()){
            tagging.getTagSet().getTags().forEach(tag -> {
                if (!keySet.add(tag.getKey())){
                    throw new MsException(ErrorNo.INVALID_TAG, "the tag key is repeated");
                }
                if (count.incrementAndGet() > 10){
                    throw new MsException(ErrorNo.INVALID_TAG, "the user meta numbers over the limitation(10)");
                }
                validateAllowedCharacters(tag.getKey(), true);
                validateAllowedCharacters(tag.getValue(), false);
            });
        }
    }

    public static void checkBucketTagging(Tagging tagging){
        if (tagging.getTagSet() == null || tagging.getTagSet().getTags() ==  null){
            throw new MsException(ErrorNo.INVALID_TAG, "the tag set is empty");
        }
        if (tagging.getTagSet().getTags().size() > 20){
            throw new MsException(ErrorNo.INVALID_TAG, "the user meta numbers over the limitation(20)");
        }
        Set<String> keySet = new HashSet<>();
        if (!tagging.getTagSet().getTags().isEmpty()){
            tagging.getTagSet().getTags().forEach(tag -> {
                if (!keySet.add(tag.getKey())){
                    throw new MsException(ErrorNo.INVALID_TAG, "the tag key is repeated");
                }
                validateKeyAndValue(tag.getKey(), true);
                validateKeyAndValue(tag.getValue(), false);
            });
        }
    }

    public static void validateKeyAndValue(String input, boolean isKey) {
        if (isKey && StringUtils.isEmpty(input)){
            throw new MsException(ErrorNo.INVALID_TAG, "The key is null.");
        }
        int maxLength = isKey ? 64 : 128;
        if (StringUtils.isNotEmpty(input) && input.length() > maxLength) {
            throw new MsException(ErrorNo.INVALID_TAG, "The " + (isKey ? "key" : "value") + " is too long.");
        }
    }

    public static void deleteTag(String keys, String bucketName) {
        String tagConfig = pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hget(BUCKET_TAG_KEY, bucketName);
        if (StringUtils.isEmpty(tagConfig)){
            return;
        }
        Tagging tagging = (Tagging) JaxbUtils.toObject(tagConfig);
        Arrays.stream(keys.split(",")).distinct().forEach(key -> {
            tagging.getTagSet().getTags().removeIf(tag -> tag.getKey().equals(key));
        });
        if (tagging.getTagSet().getTags().isEmpty()){
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel(BUCKET_TAG_KEY, bucketName);
        }else {
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(BUCKET_TAG_KEY, bucketName, new String(JaxbUtils.toByteArray(tagging)));
        }
    }
    /**
     * 校验字符串是否包含非法字符
     *
     * @param input 待校验的字符串
     * @throws MsException 若包含非法字符，抛出异常
     */
    public static void validateAllowedCharacters(String input, boolean isKey) {
        if (StringUtils.isEmpty(input)) {
            throw new MsException(ErrorNo.INVALID_TAG, "The input is null.");
        }
        int maxLength = isKey ? 128 : 256;
        if (input.length() > maxLength) {
            throw new MsException(ErrorNo.INVALID_TAG, "The " + (isKey ? "key" : "value") + " is too long.");
        }
        Matcher matcher = TAG_PATTERN.matcher(input);
        if (!matcher.matches()) {
            throw new MsException(ErrorNo.INVALID_TAG, "Illegal character found in " + input);
        }
    }

    public static void setUserMetaData(MetaData mataData, Tagging tagging) {
        JsonObject userMeta = new JsonObject();
        if (!tagging.getTagSet().getTags().isEmpty()){
            tagging.getTagSet().getTags().forEach(tag -> {
                userMeta.put(new String((USER_META + tag.getKey()).getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8),
                        new String(tag.getValue().getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8));
            });
        }
        String resStr = userMeta.encode();
        if (resStr.length() > META_USR_MAX_SIZE) {
            throw new MsException(ErrorNo.INVALID_TAG, "user meta is too long , user meta size :" + resStr.length());
        }
        mataData.setUserMetaData(userMeta.encode());
    }

    public static Tagging getTagging(MetaData mataData) {
        String userMetaData = mataData.userMetaData;
        Map<String, String> userMetaDataMap = Json.decodeValue(userMetaData, new TypeReference<Map<String, String>>() {
        });
        Tagging tagging = new Tagging();
        tagging.setTagSet(new TagSet());
        tagging.getTagSet().setTags(new ArrayList<>());
        List<Tag> tagList = new ArrayList<>();
        if (userMetaDataMap.isEmpty()){
            throw new MsException(ErrorNo.EMPTY_TAG, "The user meta data is empty.");
        }
        userMetaDataMap.forEach((key_, value) -> {
            final String lowerKey = key_.toLowerCase();
            if (lowerKey.startsWith(USER_META)) {
                Tag tag = new Tag();
                tag.setKey(key_.substring(USER_META.length()));
                tag.setValue(value);
                tagList.add(tag);
            }
        });
        tagging.getTagSet().setTags(tagList);
        return tagging;
    }

    public static boolean checkUserMetaData(MetaData metaData) {
        String userMetaData = metaData.userMetaData;
        Map<String, String> userMetaDataMap = Json.decodeValue(userMetaData, new TypeReference<Map<String, String>>() {});
        return userMetaDataMap.isEmpty();
    }
}
