package com.macrosan.action.datastream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.EsMeta;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.PostObjectResponse;
import com.macrosan.message.xmlmsg.tagging.Tagging;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.utils.authorize.PostFormAuthorize;
import com.macrosan.utils.codec.UrlEncoder;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsObjVersionUtils;
import com.macrosan.utils.policy.ReactorPolicyCheckUtils;
import com.macrosan.utils.quota.QuotaRecorder;
import com.macrosan.utils.serialize.JaxbUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.ECUtils.createMeta;
import static com.macrosan.ec.ErasureClient.getAndUpdateDeduplicate;
import static com.macrosan.ec.ErasureClient.putMetaData;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.httpserver.ResponseUtils.addAllowHeader;
import static com.macrosan.httpserver.ResponseUtils.addPublicHeaders;
import static com.macrosan.httpserver.RestfulVerticle.REQUEST_VERSIONNUM;
import static com.macrosan.httpserver.RestfulVerticle.requestVersionNumMap;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.buildUploadMsg;
import static com.macrosan.storage.StorageOperate.PoolType.DATA;
import static com.macrosan.utils.authorize.PostFormAuthorize.getOptionalField;
import static com.macrosan.utils.msutils.MsAclUtils.getAclNum;
import static com.macrosan.utils.msutils.MsException.dealException;
import static com.macrosan.utils.msutils.MsException.throwWhenEmpty;
import static com.macrosan.utils.worm.WormUtils.*;

/**
 * S3 POST Object 服务实现
 * <p>
 * 处理浏览器端 POST 表单上传请求 (multipart/form-data)，
 * 实现 S3 POST Policy 签名验证，并将文件保存到 /moss/ 目录。
 * <p>
 * AWS S3 POST Object API 参考:
 * - https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html
 * - https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTForms.html
 * - https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html
 */
@Log4j2
public class PostObjectService extends BaseService {
    private static PostObjectService instance = null;

    private PostObjectService() {
        super();
    }

    public static PostObjectService getInstance() {
        if (instance == null) {
            instance = new PostObjectService();
        }
        return instance;
    }

    /**
     * 处理 S3 POST Object 请求
     * <p>
     * 流程:
     * 1. 设置 expectMultipart 并注册 uploadHandler
     * 2. 在 upload 事件中:
     * a. 从 formAttributes 提取表单字段 (key, policy, x-amz-signature, x-amz-credential 等)
     * b. Base64 解码 policy 文档
     * c. 验证 policy expiration
     * d. 从 x-amz-credential 提取 AK，从 Redis 查询 SK
     * e. 计算 HMAC-SHA256 签名并验证
     * f. 验证 policy conditions
     * g. 将文件保存到 /moss/bucket/key 路径
     * h. 根据 success_action_status 返回响应
     */
    public int postObject(MsHttpRequest request) {
        final String method = "PutObject";
        final String requestId = getRequestId();
        MonoProcessor<Boolean> recoverDataProcessor = MonoProcessor.create();

        request.setExpectMultipart(true); // 设置表单上传
        final boolean isChunked = true; // 默认采用分块传输

        String stamp = StringUtils.isBlank(request.getHeader("stamp")) ? String.valueOf(System.currentTimeMillis()) : request.getHeader("stamp");
        String lastModify = MsDateUtils.stampToGMT(Long.parseLong(stamp));

        final String bucketName = request.getBucketName();
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucketName);

        final String[] bucketVnode = new String[]{null}; // 计算对象元数据存储位置
        final String[] migrateVnode = new String[]{null};  // 判断当前节点是否正在进行扩容，是否开启双写

        final boolean[] dedup = new boolean[1]; // 重删
        final String[] snapshotLink = new String[1]; // 桶快照


        final String[] user = new String[1]; // 用户ID，需要从表单字段中获取

        AtomicReference<JsonObject> userMeta = new AtomicReference<>(new JsonObject()); // 用户元数据， x-amz-meta-*

        final JsonObject sysMetaMap = new JsonObject(); // 系统元数据
        sysMetaMap.put(LAST_MODIFY, lastModify);

        String tmpObjName = ServerConfig.getInstance().getHostUuid()  // 采用临时对象名称,需要从表单中获取真实对象名
                + "/" + VersionUtil.getVersionNum()
                + "/" + requestId;

        StorageOperate dataOperate = new StorageOperate(DATA, tmpObjName, Long.MAX_VALUE); // PostObject 选择默认的数据池
        StoragePool dataPool = StoragePoolFactory.getStoragePool(dataOperate, bucketName);

        MetaData metaData = createMeta(dataPool, bucketName, tmpObjName, requestId);
        metaData.setStamp(stamp);

        final String[] objName = new String[1]; // 实际的对象名，从表单字段中获取

        //  ---------------------------------------------设置worm start-----------------------------------------------------------
        final String wormExpire = request.getHeader(X_AMZ_WORM_DATE_HEADER); // todo 是否需要通过表单字段填充
        final String wormMode = request.getHeader(X_AMZ_WORM_MODE_HEADER); // todo 是否需要通过表单字段填充
        final String syncWormExpire = request.getHeader(SYNC_WORM_EXPIRE);  // todo 适配多站点
        String wormStamp = request.getHeader(WORM_STAMP);  // todo 适配多站点
        final long[] currentWormStamp = new long[1]; // 当前worm时钟的时间

        // -----------------------------------------------桶加密处理 ----------------------------------------------------------------
        final String[] crypto = {request.getHeader(X_AMZ_SERVER_SIDE_ENCRYPTION)}; // todo 是否需要通过表单字段填充
        CryptoUtils.containsCryptoAlgorithm(crypto[0]);
        String originIndexCrypto = StringUtils.isBlank(request.getHeader(ORIGIN_INDEX_CRYPTO)) ? null : request.getHeader(ORIGIN_INDEX_CRYPTO); // todo 适配多站点

        request.headers().add(TRANSFER_ENCODING, "chunked");
        SocketReqMsg msg = buildUploadMsg(request, bucketName, "", requestId);
        request.headers().remove(TRANSFER_ENCODING);

        JsonObject aclJson = new JsonObject(); // 对象权限

        PostFormAuthorize authorize = new PostFormAuthorize();

        final String[] md5sum = new String[]{""};
        Disposable disposable = currentWormTimeMillis()
                // 设置对象写保护信息
                .doOnNext(wormTime -> currentWormStamp[0] = wormStamp != null ? Long.valueOf(wormStamp) : wormTime)
                .flatMap(wormTime -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName).defaultIfEmpty(new HashMap<>()))
                // -------------------------------------------------- 桶基本信息检查 -----------------------------------------------------------
                .doOnNext(bucketInfo -> {
                    // 校验桶是否存在
                    throwWhenEmpty(bucketInfo, new MsException(NO_SUCH_BUCKET, "No such bucket"));

                    // 判断桶操作是否合法
                    regionCheck(bucketInfo.get(REGION_NAME));
                    siteCheck(bucketInfo);

                    // 设置对象版本号
                    String versionStatus = bucketInfo.getOrDefault(BUCKET_VERSION_STATUS, "NULL");
                    String versionId = "NULL".equals(versionStatus) || VERSION_SUSPENDED.equals(versionStatus) ?
                            "null" : RandomStringUtils.randomAlphanumeric(32);
                    versionId = StringUtils.isNoneBlank(request.getHeader(VERSIONID)) ? request.getHeader(VERSIONID) : versionId;
                    metaData.setVersionId(versionId);
                    metaData.setReferencedVersionId(versionId);
                    // 桶快照，设置对象快照
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                    metaData.setSnapshotMark(bucketInfo.get(CURRENT_SNAPSHOT_MARK));
                    // 桶加密处理
                    metaData.setCrypto((crypto[0] = StringUtils.isBlank(crypto[0]) ? request.headers().contains(ORIGIN_INDEX_CRYPTO) ? originIndexCrypto : bucketInfo.get("crypto") : crypto[0]));
                    // 设置对象写保护(worm)
                    if (StringUtils.isEmpty(syncWormExpire) && !request.headers().contains(IS_SYNCING)) {
                        setObjectRetention(bucketInfo, sysMetaMap, currentWormStamp[0], wormExpire, wormMode);
                    } else if (StringUtils.isNotEmpty(syncWormExpire)) {
                        sysMetaMap.put(EXPIRATION, syncWormExpire);
                    }
                    // 判断桶是否设置了quickReturn，对象上传失败后立刻返回
                    if (request.getQuickReturn() == 2
                            && "1".equals(bucketInfo.getOrDefault("quickReturn", "0"))) {
                        request.setQuickReturn(1);
                    }
                    request.addMember("address", bucketInfo.getOrDefault("address", ""));
                    msg.put("mda", bucketInfo.getOrDefault("mda", "off"));
                    if ("on".equals(bucketInfo.getOrDefault("mda", "off"))) {
                        msg.put("mda", "on");
                    } else {
                        msg.put("mda", "off");
                    }
                    msg.put("status", versionStatus);
                })
                // --------------------------------------------------------计算对象数据块的存储位置，读取表单文件，写入数据池 --------------------------------------------------
                .flatMap(bucketInfo -> dataPool.mapToNodeInfo(dataPool.getObjectVnodeId(metaData.fileName)))
                .zipWith(BucketSyncSwitchCache.isSyncSwitchOffMono(bucketName))
                .map(tuple2 -> {
                    String versionNum = VersionUtil.getVersionNum();
                    request.addMember(REQUEST_VERSIONNUM, versionNum);
                    requestVersionNumMap.computeIfAbsent(bucketName, k -> new ConcurrentSkipListMap<>()).put(versionNum, request);
                    return tuple2.getT1();
                })
                .flatMap(list -> ErasureClient.putObject(dataPool, list, request, metaData, recoverDataProcessor, isChunked, sysMetaMap))
                .doOnNext(md5 -> {
                    md5sum[0] = md5;
                    if (StringUtils.isBlank(md5sum[0])) {
                        throw new MsException(UNKNOWN_ERROR, "put object failed");
                    }
                    // todo ? 需要对比contentMD5
                    String contentMd5 = getOptionalField(request, CONTENT_MD5);
                    if (contentMd5 != null){
                        checkMd5(contentMd5, md5);
                    }
                    // 对象名称
                    objName[0] = getOptionalField(request, "key");
                    if (StringUtils.isBlank(objName[0])) {
                        throw new MsException(INVALID_REQUEST, "key is empty");
                    }else {
                        String filename = request.getMember(FILENAME);
                        if(objName[0].contains("${filename}") && request.getMembers().containsKey(FILENAME)){
                            objName[0] = objName[0].replace("${filename}", filename);
                            request.formAttributes().set("key", objName[0]);
                        }
                    }
                    checkObjectName(objName[0]);
                    msg.put("key", objName[0]);
                    metaData.setKey(objName[0]);
                    request.setObjectName(objName[0]);
                })
                // -------------------------------------------------- POST 签名校验 (V4 / V2) --------------------------------------------------
                .flatMap(b -> authorize.verifyPostSignatureReactive(request, bucketName,metaData.endIndex - metaData.startIndex + 1))
                // 初始化对象权限
                .flatMap(md5 -> createObjAcl(request, aclJson))
                // -------------------------------------------------------- 解析表单字段 ---------------------------------------------
                .doOnNext(b -> {
                    user[0] = request.getUserId();
                    msg.put("userId", user[0]);

                    // 设置用户元数据
                    userMeta.set(getUserMeta(request));
                    metaData.setUserMetaData(userMeta.get().encode());

                    // 设置系统元数据
                    JsonObject sysMetaMap0 = getSysMetaMap(request);
                    if (sysMetaMap.containsKey(CONTENT_TYPE)) {
                        sysMetaMap0.remove(CONTENT_TYPE);
                    }
                    sysMetaMap0.forEach(entry -> sysMetaMap.put(entry.getKey(), entry.getValue()));
                    sysMetaMap.put("owner", user[0]);
                    sysMetaMap.put(CONTENT_LENGTH, metaData.endIndex - metaData.startIndex + 1);

                    // 设置对象权限ACL
                    metaData.setObjectAcl(aclJson.encode());
                })
                // ------------------------------------------------------ 账户/桶 配额校验 -----------------------------------------------------
                .flatMap(b -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName))
                .flatMap(bucketInfo -> pool.getReactive(REDIS_USERINFO_INDEX).hgetall(bucketInfo.getOrDefault("user_name", "")).zipWith(Mono.just(bucketInfo)))
                .flatMap(tuple2 -> {
                    String accountCapacityQuotaFlag = tuple2.getT1().get(USER_DATABASE_ACCOUNT_QUOTA_FLAG);
                    String accountObjectsQuotaFlag = tuple2.getT1().get(USER_DATABASE_ACCOUNT_OBJNUM_FLAG);
                    if (!request.headers().contains(IS_SYNCING)) {
                        quotaCheck(tuple2.getT2(), accountCapacityQuotaFlag, accountObjectsQuotaFlag);
                    }
                    return Mono.just(tuple2.getT2());
                })
                // ------------------------------------------------------- 桶策略权限校验 -------------------------------------------------
                // todo 桶策略校验暂时不支持表单数据，需支持
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyPutCheckResult(request, bucketName, objName[0], method, aclJson, crypto[0]).zipWith(Mono.just(bucketInfo)))
                .flatMap(tuple2 -> {
                    int policy = tuple2.getT1();
                    Map<String, String> bucketInfo = tuple2.getT2();
                    if (!PASSWORD.equals(request.getHeader(SYNC_AUTH)) && policy == 0) {
                        return MsAclUtils.checkWriteAclReactive(bucketInfo, user[0], bucketName, msg);
                    } else {
                        return Mono.empty();
                    }
                })
                .switchIfEmpty(Mono.defer(() -> pool.getReactive(REDIS_USERINFO_INDEX).hget(user[0], "name")))
                .doOnNext(userName -> msg.put("name", (String) userName))
                // ----------------------------------------------------------检查对象版本数量限制 --------------------------------------------
                .flatMap(userName -> MsObjVersionUtils.checkObjVersionsLimit(bucketName, objName[0], request))
                // --------------------------------------------- -------------  重删处理 ---------------------------------------------------------
                .flatMap(b -> StoragePoolFactory.getDeduplicateByBucketName(bucketName).zipWith(Mono.just(md5sum[0])))
                .doOnNext(tuple2 -> dedup[0] = tuple2.getT1())
                .flatMap(tuple2 -> {
                    String md5 = tuple2.getT2();
                    sysMetaMap.put(ETAG, md5);

                    if (!dedup[0] || metaData.partInfos != null || metaData.storage.startsWith("cache")) {
                        // 数据块落在缓存池，不进行重删处理，缓存下刷时处理小文件的重删
                        return Mono.just(true);
                    }
                    StoragePool dedupPool = StoragePoolFactory.getMetaStoragePool(md5);
                    String dedupVnode = dedupPool.getBucketVnodeId(md5);
                    String duplicate = Utils.getDeduplicatMetaKey(dedupVnode, md5, metaData.storage, requestId);
                    return dedupPool.mapToNodeInfo(dedupVnode)
                            .flatMap(nodeList -> getAndUpdateDeduplicate(dedupPool, duplicate, nodeList, request, recoverDataProcessor, metaData, md5))
                            .flatMap(b -> {
                                if (b) {
                                    metaData.duplicateKey = duplicate;
                                }
                                return Mono.just(b);
                            });
                })
                .doOnNext(b -> {
                    if (!b) {
                        throw new MsException(UNKNOWN_ERROR, "put deduplicate meta failed");
                    }
                })
                // ------------------------------------------- 设置对象元数据的全局唯一递增ID，完善元数据信息 ---------------------------------------------------------
                .flatMap(usrName -> VersionUtil.getVersionNum(bucketName, objName[0]))
                .flatMap(versionNum -> {
                    if (StringUtils.isNotEmpty(request.getHeader(CONTENT_LENGTH)) && request.getHeader(CONTENT_LENGTH).contains("change-")) {
                        String realLength = request.getHeader(CONTENT_LENGTH).replace("change-", "");
                        sysMetaMap.put(CONTENT_LENGTH, realLength);
                        metaData.endIndex = Long.parseLong(realLength) - 1;
                    }
                    sysMetaMap.put("displayName", msg.get("name"));
                    metaData.setVersionNum(versionNum);
                    metaData.setSysMetaData(sysMetaMap.encode());
                    metaData.setSyncStamp(StringUtils.isBlank(request.getHeader(SYNC_STAMP)) ? versionNum : request.getHeader(SYNC_STAMP));
                    metaData.setShardingStamp(VersionUtil.getVersionNum());
                    metaData.setReferencedKey(objName[0]);
                    metaData.setReferencedBucket(bucketName);
                    Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(bucketName, objName[0]);
                    bucketVnode[0] = bucketVnodeIdTuple.var1;
                    // 判断当前节点是否正在进行扩容，是否开启双写
                    migrateVnode[0] = bucketVnodeIdTuple.var2;
                    return bucketPool.mapToNodeInfo(bucketVnode[0]);
                })
                // ------------------------------------------------- 若前面报错进行回滚处理 -------------------------------------------------
                .onErrorResume(e -> {
                    // 写元数据之前出现失败则，进行回滚处理
                    if (!StringUtils.isBlank(md5sum[0])) {
                        log.info("put object error, need rollback bucket:{} key:{} fileName:{} {}", bucketName, objName[0], metaData.fileName, e.getMessage());
                        String objectVnodeId = dataPool.getObjectVnodeId(metaData.fileName);
                        return dataPool.mapToNodeInfo(objectVnodeId)
                                .flatMap(nodeList -> ErasureClient.deleteObjectFile(dataPool, new String[]{metaData.fileName}, request))
                                .onErrorResume(x -> Mono.error(e))
                                .then(Mono.error(e)); // 不管删除是否成功，都抛出异常
                    }
                    return Mono.error(e);
                })
                // ------------------------------------------------- 对象元数据写入 -------------------------------------------------
                .flatMap(bucketList -> {
                    EsMeta esMeta = new EsMeta()
                            .setUserId(user[0])
                            .setSysMetaData(sysMetaMap.encode())
                            .setUserMetaData(userMeta.get().encode())
                            .setBucketName(bucketName)
                            .setObjName(objName[0])
                            .setVersionId(metaData.versionId)
                            .setStamp(stamp)
                            .setObjSize(String.valueOf(metaData.endIndex + 1))
                            .setInode(metaData.inode);
                    Optional.ofNullable(request.getAllowCommit()).ifPresent(req -> request.setAllowCommit(false));

                    return Mono.just(migrateVnode[0] != null)
                            .flatMap(b -> {
                                if (b) {
                                    if (migrateVnode[0].equals(bucketVnode[0])) {
                                        return Mono.just(true);
                                    }
                                    return bucketPool.mapToNodeInfo(migrateVnode[0])
                                            .flatMap(nodeList -> putMetaData(Utils.getMetaDataKey(migrateVnode[0], bucketName,
                                                            objName[0], metaData.versionId, metaData.stamp, metaData.snapshotMark), metaData, nodeList, recoverDataProcessor, request, msg.get("mda"), esMeta,
                                                    snapshotLink[0]));
                                }
                                return Mono.just(true);
                            })
                            .doOnNext(b -> {
                                if (!b) {
                                    throw new MsException(UNKNOWN_ERROR, "put object meta to new mapping error!");
                                }
                            })
                            .flatMap(b -> putMetaData(Utils.getMetaDataKey(bucketVnode[0], bucketName,
                                            objName[0], metaData.versionId, metaData.stamp, metaData.snapshotMark), metaData, bucketList, recoverDataProcessor, request, msg.get("mda"), esMeta,
                                    snapshotLink[0]))
                            .doOnNext(b -> {
                                if (!b) {
                                    throw new MsException(UNKNOWN_ERROR, "put meta data error!");
                                }
                                Optional.ofNullable(request.getAllowCommit()).ifPresent(req -> request.setAllowCommit(true));
                            })
                            .flatMap(b -> {
                                if (!"off".equals(msg.get("mda"))) {
                                    MonoProcessor<Boolean> esRes = MonoProcessor.create();
                                    Mono.just(1).publishOn(DISK_SCHEDULER).flatMap(l -> EsMetaTask.putEsMeta(esMeta, false)).subscribe(esRes::onNext);
                                    return esRes;
                                } else {
                                    return Mono.just(b);
                                }
                            });
                })
                // ----------------------------------------- 请求响应处理 -------------------------------------------------
                .subscribe(b -> {
                    try {
                        QuotaRecorder.addCheckBucket(bucketName);
                        addPublicHeaders(request, requestId)
                                .putHeader(CONTENT_LENGTH, "0")
                                .putHeader(META_ETAG, '"' + sysMetaMap.getString(ETAG) + '"')
                                .putHeader(X_AMX_VERSION_ID, metaData.versionId);
                        CryptoUtils.addCryptoResponse(request, crypto[0]);
                        addAllowHeader(request.response());
                        sendSuccessResponse(request, requestId, bucketName, objName[0], '"' + sysMetaMap.getString(ETAG) + '"');
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }, e -> dealException(requestId, request, e, false));

        request.addResponseCloseHandler(v -> disposable.dispose());

        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 根据 success_action_status 发送成功响应
     */
    private void sendSuccessResponse(MsHttpRequest request, String requestId, String bucketName, String key, String etag) throws UnsupportedEncodingException {
        String successActionStatus = getOptionalField(request, "success_action_status");
        String redirectUrl = getOptionalField(request, "success_action_redirect");
        String location = "http://" + request.host() + "/" + bucketName + "/" + URLEncoder.encode(key, "UTF-8");

        
        // 如果指定了重定向 URL，校验其有效性
        if (StringUtils.isNotBlank(redirectUrl)) {
            if (isValidRedirectUrl(redirectUrl)) {
                String newRedirectUrl = redirectUrl + "?bucket=" + bucketName + "&key=" + URLEncoder.encode(key, "UTF-8") + "&etag=" + URLEncoder.encode(etag, "UTF-8");
                addPublicHeaders(request, requestId)
                        .setStatusCode(303)
                        .putHeader("Location", newRedirectUrl)
                        .putHeader(CONTENT_LENGTH, "0")
                        .end();
                return;
            } else {
                // URL 无效，视为未提供该字段，继续按 success_action_status 处理
                log.warn("PostObject: invalid redirect URL '{}', treating as not provided", redirectUrl);
            }
        }

        if ("201".equals(successActionStatus)) {
            // 返回 201 XML 响应
            PostObjectResponse response = new PostObjectResponse();
            response.setLocation(location)
                    .setBucket(bucketName)
                    .setKey(key)
                    .setEtag(etag);

            byte[] xmlBytes = JaxbUtils.toByteArray(response);
            Buffer buffer = Buffer.buffer(xmlBytes);

            addPublicHeaders(request, requestId)
                    .setStatusCode(201)
                    .putHeader(CONTENT_TYPE, "application/xml")
                    .putHeader(CONTENT_LENGTH, String.valueOf(xmlBytes.length))
                    .putHeader(META_ETAG, etag)
                    .putHeader("Location", location)
                    .end(buffer);
        } else if ("200".equals(successActionStatus)) {
            // 返回 200 空文档
            addPublicHeaders(request, requestId)
                    .setStatusCode(200)
                    .putHeader(CONTENT_LENGTH, "0")
                    .putHeader(META_ETAG, etag)
                    .putHeader("Location", location)
                    .end();
        } else {
            // 默认返回 204 空文档
            addPublicHeaders(request, requestId)
                    .setStatusCode(204)
                    .putHeader(CONTENT_LENGTH, "0")
                    .putHeader(META_ETAG, etag)
                    .putHeader("Location", location)
                    .end();
        }
    }

    /**
     * 校验重定向 URL 是否有效
     * <p>
     * AWS 规范：如果 Amazon S3 无法解析该 URL，则视为该字段未提供
     *
     * @param url 待校验的 URL
     * @return true 表示有效，false 表示无效
     */
    private boolean isValidRedirectUrl(String url) {
        if (StringUtils.isBlank(url)) {
            return false;
        }
        try {
            // 校验 URL 格式：必须是 http 或 https 协议
            if (!url.toLowerCase().startsWith("http://") && !url.toLowerCase().startsWith("https://")) {
                return false;
            }
            // 尝试解析 URL
            new java.net.URL(url);
            return true;
        } catch (Exception e) {
            log.debug("PostObject: failed to parse redirect URL: {}", url, e);
            return false;
        }
    }

    @Override
    public Mono<Boolean> createObjAcl(MsHttpRequest request, JsonObject aclJson) {
        final MonoProcessor<Boolean> processor = MonoProcessor.create();
        List<String> idList = new LinkedList<>();
        for (Map.Entry<String, String> entry : request.formAttributes().entries()) {
            String k = entry.getKey().toLowerCase();
            String v = entry.getValue().toLowerCase();
            if ("acl".equals(k)) {
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
        int acl = request.formAttributes().contains("acl") ?
                getAclNum(request.getFormAttribute("acl")) :
                OBJECT_PERMISSION_PRIVATE_NUM;

        String id = request.getFormAttribute(PERMISSION_READ_LONG);
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

        id = request.getFormAttribute(PERMISSION_READ_CAP_LONG);
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

        id = request.getFormAttribute(PERMISSION_WRITE_CAP_LONG);
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

        id = request.getFormAttribute(PERMISSION_FULL_CON_LONG);
        if (StringUtils.isNotBlank(id)) {
            String[] ids = id.split(",");
            for (String str : ids) {
                if (StringUtils.isNotBlank(str)) {
                    String[] split = str.split("=");
                    if (split.length > 1) {
                        idList.add(split[1]);
                        acl |= OBJECT_PERMISSION_FULL_CON_NUM;
                        aclJson.put(OBJECT_PERMISSION_FULL_CON + '-' + split[1], OBJECT_PERMISSION_FULL_CON);
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
            return true;
        });
    }

    @Override
    public JsonObject getUserMeta(HttpServerRequest request) {
        List<Map.Entry<String, String>> headerList = request.formAttributes().entries();
        JsonObject result = new JsonObject();
        headerList.forEach(entry -> {
            final String key = entry.getKey();
            final String lowerKey = entry.getKey().toLowerCase();
            try {
                if (lowerKey.startsWith(USER_META)) {
                    result.put(new String(key.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8),
                            new String(entry.getValue().getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8));
                }
                if ("tagging".equals(lowerKey)) {
                    Tagging tagging = null;
                    tagging = (Tagging) JaxbUtils.toObject(entry.getValue());
                    if (tagging == null){
                        throw new MsException(INVALID_ARGUMENT, "tagging xml is not valid!");
                    }
                    checkTagging(tagging);
                    setUserMetaData(result, tagging);
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

    public static void setUserMetaData(JsonObject result, Tagging tagging) {
        if (!tagging.getTagSet().getTags().isEmpty()){
            tagging.getTagSet().getTags().forEach(tag -> {
                result.put(new String((USER_META + tag.getKey()).getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8),
                        new String(tag.getValue().getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8));
            });
        }
    }

    @Override
    protected JsonObject getSysMetaMap(HttpServerRequest request) {
        JsonObject res = new JsonObject();
        request.formAttributes().forEach(entry -> {
            final String key = entry.getKey().toLowerCase();
            if (SPECIAL_HEADER.contains(key.hashCode())) {
                res.put(entry.getKey(), entry.getValue());
            }
        });
        if (StringUtils.isEmpty(request.headers().get(CONTENT_TYPE))) {
            res.put(CONTENT_TYPE, "application/octet-stream");
        }
        return res;
    }
}
