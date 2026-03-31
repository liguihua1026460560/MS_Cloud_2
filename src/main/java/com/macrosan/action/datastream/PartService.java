package com.macrosan.action.datastream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.part.PartClient;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.cifs.notify.NotifyServer;
import com.macrosan.filesystem.cifs.reply.smb2.NotifyReply;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.filesystem.utils.acl.S3ACL;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.CompleteMultipartUploadResult;
import com.macrosan.message.xmlmsg.CopyPartResult;
import com.macrosan.message.xmlmsg.ListPartsResult;
import com.macrosan.message.xmlmsg.Uploads;
import com.macrosan.message.xmlmsg.section.Owner;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.utils.authorize.AuthorizeV4;
import com.macrosan.utils.functional.LongLongTuple;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsObjVersionUtils;
import com.macrosan.utils.policy.ReactorPolicyCheckUtils;
import com.macrosan.utils.quota.QuotaRecorder;
import com.macrosan.utils.regex.PatternConst;
import com.macrosan.utils.serialize.JaxbUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.function.Tuple2;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;

import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.ECUtils.dealCopyRange;
import static com.macrosan.ec.ErasureClient.getObjectMetaVersionUnlimited;
import static com.macrosan.ec.ErasureClient.putPartDeduplicate;
import static com.macrosan.filesystem.cifs.call.smb2.NotifyCall.FILE_NOTIFY_CHANGE_FILE_NAME;
import static com.macrosan.filesystem.quota.FSQuotaConstants.QUOTA_KEY;
import static com.macrosan.httpserver.ResponseUtils.addAllowHeader;
import static com.macrosan.httpserver.ResponseUtils.addPublicHeaders;
import static com.macrosan.httpserver.RestfulVerticle.REQUEST_VERSIONNUM;
import static com.macrosan.httpserver.RestfulVerticle.requestVersionNumMap;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.message.jsonmsg.InitPartInfo.ERROR_INIT_PART_INFO;
import static com.macrosan.message.jsonmsg.InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO;
import static com.macrosan.storage.StorageOperate.PoolType.DATA;
import static com.macrosan.utils.authorize.AuthorizeV4.X_AMZ_DECODED_CONTENT_LENGTH;
import static com.macrosan.utils.msutils.MsAclUtils.*;
import static com.macrosan.utils.msutils.MsException.dealException;
import static com.macrosan.utils.msutils.MsException.throwWhenEmpty;
import static com.macrosan.utils.regex.PatternConst.OBJECT_NAME_PATTERN;
import static com.macrosan.utils.worm.WormUtils.*;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class PartService extends BaseService {
    private static PartService instance = null;

    public static final Vertx vertx = ServerConfig.getInstance().getVertx();

    private PartService() {
        super();
    }

    public static PartService getInstance() {
        if (instance == null) {
            instance = new PartService();
        }
        return instance;
    }

    /**
     * 初始化分段上传之前的检查
     *
     * @param objName 对象名
     */
    private void checkObjectName(String objName) {
        Matcher matcher = OBJECT_NAME_PATTERN.matcher(objName);
        if (!matcher.matches()) {
            throw new MsException(ErrorNo.NAME_INPUT_ERR, "InitiateMultipartUpload obj name input error, obj_name:" + objName);
        }

        if (objName.getBytes(StandardCharsets.UTF_8).length > 1024) {
            throw new MsException(ErrorNo.NAME_INPUT_ERR, "InitiateMultipartUpload obj name input error, obj_name:" + objName);
        }
    }

    private void checkPart(InitPartInfo info, String userId, String uploadId, String currentSnapshotMark) {
        if (info.equals(ERROR_INIT_PART_INFO)) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get Upload Part fail");
        }

        if (info.equals(NO_SUCH_UPLOAD_ID_INIT_PART_INFO) || info.delete || info.isUnView(currentSnapshotMark)) {
            throw new MsException(ErrorNo.NO_SUCH_UPLOAD_ID, "No such upload id.upload id: " + uploadId + ".");
        }

        if (!userId.equalsIgnoreCase(info.initAccount)) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such bucket permission.userId: " + userId + ".");
        }
    }

    private void checkPart(InitPartInfo info, String userId, String uploadId, Map<String, String> bucketInfo, MsHttpRequest request) {
        if (info.equals(ERROR_INIT_PART_INFO)) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get Upload Part fail");
        }

        if (info.equals(NO_SUCH_UPLOAD_ID_INIT_PART_INFO) || info.delete || info.isUnView(bucketInfo.get(CURRENT_SNAPSHOT_MARK))) {
            throw new MsException(ErrorNo.NO_SUCH_UPLOAD_ID, "No such upload id.upload id: " + uploadId + ".");
        }

        if (!request.headers().contains(IS_SYNCING) && !userId.equalsIgnoreCase(bucketInfo.get(BUCKET_USER_ID)) && !userId.equalsIgnoreCase(info.initAccount)) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION, "No such bucket permission.userId: " + userId + ".");
        }
    }

    private void quotaCheck(Map<String, String> bucketInfo, String accountQuotaFlag, String accountObjNumFlag) {
        if ("1".equals(bucketInfo.get(QUOTA_FLAG))) {
            throw new MsException(NO_ENOUGH_SPACE, "No Enough Space Because of the bucket quota.");
        } else if ("1".equals(bucketInfo.get(OBJNUM_FLAG))) {
            throw new MsException(NO_ENOUGH_OBJECTS, "The hard-max-objects was exceeded.");
        } else if ("2".equals(accountQuotaFlag)) {
            throw new MsException(NO_ENOUGH_SPACE, "No Enough Space Because of the account quota.");
        } else if ("2".equals(accountObjNumFlag)) {
            throw new MsException(NO_ENOUGH_OBJECTS, "The hard-max-objects was exceeded.");
        }
    }

    /**
     * 处理初始化分段上传请求
     *
     * @param request
     * @return
     */
    public int initiateMultiPartUpload(MsHttpRequest request) {
        String userId = request.getUserId();
        String bucketName = request.getBucketName();
        String objName = request.getObjectName();

        final int[] policy = new int[1];

        final String method = "PutObject";

        JsonObject aclJson = new JsonObject();
        final JsonObject userMeta = getUserMeta(request);
        final JsonObject sysMetaMap = getSysMetaMap(request);
        sysMetaMap.put("owner", userId);
        String uploadId = StringUtils.isNotBlank(request.getHeader(UPLOAD_ID)) ? request.getHeader(UPLOAD_ID) : RandomStringUtils.randomAlphabetic(32);
        String[] accountQuota = new String[]{null};
        String[] objNumQuota = new String[]{null};
        long[] currentWormStamp = new long[1];
        final String wormExpire = request.getHeader(X_AMZ_WORM_DATE_HEADER);
        final String wormMode = request.getHeader(X_AMZ_WORM_MODE_HEADER);
        final String syncWormExpire = request.getHeader(SYNC_WORM_EXPIRE);
        String wormStamp = request.getHeader(WORM_STAMP);
        StorageOperate dataOperate = new StorageOperate(DATA, objName, Long.MAX_VALUE);
        StoragePool dataPool = StoragePoolFactory.getStoragePool(dataOperate, bucketName);
        final String[] crypto = {request.getHeader(X_AMZ_SERVER_SIDE_ENCRYPTION)};
        final String originIndexCrypto = StringUtils.isBlank(request.getHeader(ORIGIN_INDEX_CRYPTO)) ? null : request.getHeader(ORIGIN_INDEX_CRYPTO);
        final String[] currentSnapshotMark = new String[]{null};
        final String[] mergeTargetMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        boolean[] hasStartFS = new boolean[1];
        int[] startFsType = {0};
        Disposable disposable = currentWormTimeMillis()
                .doOnNext(x -> CryptoUtils.containsCryptoAlgorithm(crypto[0]))
                .doOnNext(wormTime -> currentWormStamp[0] = StringUtils.isNotEmpty(wormStamp) ? Long.parseLong(wormStamp) : wormTime)
                .flatMap(wormTime -> pool.getReactive(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_ID_NAME))
                .doOnNext(displayName -> sysMetaMap.put("displayName", displayName))
                .flatMap(displayName -> pool.getReactive(REDIS_USERINFO_INDEX).hgetall(displayName))
                .doOnNext(accountInfo -> {
                    accountQuota[0] = accountInfo.get(USER_DATABASE_ACCOUNT_QUOTA_FLAG);
                    objNumQuota[0] = accountInfo.get(USER_DATABASE_ACCOUNT_OBJNUM_FLAG);
                })
                .flatMap(s -> createObjAcl(request, aclJson))
                .flatMap(b -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName))
                .defaultIfEmpty(new HashMap<>(0))
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo, new MsException(NO_SUCH_BUCKET, "no such bucket. bucket name :" + bucketName + ".")))
                .doOnNext(bucketInfo -> crypto[0] = StringUtils.isBlank(crypto[0]) ? request.headers().contains(ORIGIN_INDEX_CRYPTO) ? originIndexCrypto : bucketInfo.get("crypto") : crypto[0])
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, objName, method).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .doOnNext(tuple2 -> {
                    if (StringUtils.isEmpty(syncWormExpire)) {
                        setObjectRetention(tuple2.getT2(), sysMetaMap, currentWormStamp[0], wormExpire, wormMode);
                    } else {
                        sysMetaMap.put(EXPIRATION, syncWormExpire);
                    }
                })
                .flatMap(tuple2 -> {
                    Map<String, String> bucketInfo = tuple2.getT2();
                    CheckUtils.hasStartFS(bucketInfo, hasStartFS);
                    startFsType[0] = ACLUtils.setFsProtoType(bucketInfo);
                    regionCheck(bucketInfo.get(REGION_NAME));
                    siteCheck(bucketInfo);
                    if (!request.headers().contains(IS_SYNCING)) {
                        quotaCheck(bucketInfo, accountQuota[0], objNumQuota[0]);
                    }
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                    if (!PASSWORD.equals(request.getHeader(SYNC_AUTH)) && policy[0] == 0) {
                        if (hasStartFS[0]) {
                            return S3ACL.checkFSACL(objName, bucketName, request.getUserId(), null, 7, false, startFsType[0], ACLUtils.setDirOrFile(false))
                                    .flatMap(b -> MsAclUtils.checkWriteAclReactive(bucketInfo, request.getUserId(), bucketName, null));
                        } else {
                            return MsAclUtils.checkWriteAclReactive(bucketInfo, request.getUserId(), bucketName, null);
                        }
                    } else {
                        return Mono.empty();
                    }
                })
                .switchIfEmpty(Mono.just(true))
                .flatMap(obj -> {
                    if (hasStartFS[0]) {
                        return FSQuotaUtils.S3CanCreate(bucketName, objName,request.getUserId());
                    }
                    return Mono.just(obj);
                })
                .flatMap(object -> {
                    checkObjectName(objName);
                    StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
                    //String bucketVnode = storagePool.getBucketVnodeId(bucketName, objName);
                    com.macrosan.utils.functional.Tuple2<String, String> bucketVnodeIdTuple = storagePool.getBucketVnodeIdTuple(bucketName, objName);
                    String bucketVnode = bucketVnodeIdTuple.var1;
                    String bucketMigrateVnode = bucketVnodeIdTuple.var2;

                    MetaData metaData = new MetaData();
                    metaData.setObjectAcl(aclJson.encode());
                    metaData.setSysMetaData(Json.encode(sysMetaMap));
                    metaData.setUserMetaData(userMeta.encode());
                    metaData.setBucket(bucketName);
                    metaData.setKey(objName);
                    metaData.setReferencedBucket(bucketName);
                    metaData.setReferencedKey(objName);
                    metaData.setPartUploadId(uploadId);
                    String stamp = StringUtils.isNotBlank(request.headers().get("Initiated")) ? request.headers().get("Initiated") : Long.toString(System.currentTimeMillis());
                    metaData.setStamp(stamp);
                    metaData.versionNum = "";
                    metaData.setCrypto(crypto[0]);
                    metaData.setSnapshotMark(currentSnapshotMark[0]);
                    return storagePool.mapToNodeInfo(bucketVnode)
                            .flatMap(list -> PartClient.getInitPartInfo(bucketName, objName, uploadId, list, request, currentSnapshotMark[0], snapshotLink[0]))
                            .doOnNext(info -> {
                                if (info.equals(ERROR_INIT_PART_INFO)) {
                                    throw new MsException(UNKNOWN_ERROR, "InitiateMultipartUpload fail");
                                }
                            })
                            .flatMap(info -> MsObjVersionUtils.getObjVersionIdReactive(bucketName))
                            .doOnNext(versionId -> {
                                versionId = StringUtils.isNoneBlank(request.getHeader(VERSIONID)) ? request.getHeader(VERSIONID) : versionId;
                                metaData.setVersionId(versionId);
                                metaData.setReferencedVersionId(versionId);
                            })
                            .flatMap(info -> storagePool.mapToNodeInfo(bucketVnode))
                            .flatMap(list -> pool.getReactive(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_ID_NAME)
                                    .flatMap(userName -> {
                                        if (bucketMigrateVnode != null) {
                                            return storagePool.mapToNodeInfo(bucketMigrateVnode).flatMap(nodeList -> PartClient.initPartUpload(metaData, uploadId, userName, nodeList,
                                                    dataPool.getVnodePrefix(), request, snapshotLink[0]))
                                                    .doOnNext(b -> {
                                                        if (!b) {
                                                            throw new MsException(UNKNOWN_ERROR, "InitiateMultipartUpload to new mapping fail");
                                                        }
                                                    })
                                                    .flatMap(b -> Mono.just(userName));
                                        } else {
                                            return Mono.just(userName);
                                        }
                                    })
                                    .flatMap(userName -> PartClient.initPartUpload(metaData, uploadId, userName, list, dataPool.getVnodePrefix(), request, snapshotLink[0])))
                            .doOnNext(b -> {
                                if (!b) {
                                    throw new MsException(UNKNOWN_ERROR, "InitiateMultipartUpload fail");
                                }
                            });

                })
                .subscribe(res -> {
                    try {
                        Uploads uploads = new Uploads();
                        uploads.setBucket(bucketName).setKey(objName).setUploadId(uploadId);
                        byte[] initRes = JaxbUtils.toByteArray(uploads);
                        addPublicHeaders(request, getRequestId())
                                .putHeader(CONTENT_TYPE, "application/xml")
                                .putHeader(CONTENT_LENGTH, String.valueOf(initRes.length))
                                .write(Buffer.buffer(initRes));
                        CryptoUtils.addCryptoResponse(request, crypto[0]);
                        addAllowHeader(request.response()).end();
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }, e -> dealException(request, e));
        request.addResponseCloseHandler(v -> disposable.dispose());

        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 处理上传分段请求
     *
     * @param request
     * @return
     */
    public int uploadPart(MsHttpRequest request) {
        if (request.headers().contains(SYNC_PART_DATA)) {
            return uploadPartData(request);
        }
        if (request.headers().contains("x-amz-copy-source")) {
            return uploadPartCopy(request);
        }

        request.setExpectMultipart(true);
        String requestId = getRequestId();

        String numStr = request.getParam(PART_NUMBER);
        final String uploadId = request.getParam(UPLOAD_ID);
        boolean flag = false;
        if (request.getHeader(TRANSFER_ENCODING) != null && request.getHeader(TRANSFER_ENCODING).contains("chunked")) {//分块传输
            flag = true;
        }
        final String contentLength = request.getHeader(CONTENT_LENGTH);
        final String decodeLength = request.getHeader(X_AMZ_DECODED_CONTENT_LENGTH);
        if (!request.headers().contains(IS_SYNCING)) {
            partUploadCheck(numStr, uploadId, contentLength, decodeLength, flag);
        }

        final boolean isChunked = flag;
        String contentMD5 = request.getHeader(CONTENT_MD5);

        final String bucketName = request.getBucketName();
        final String objectName = request.getObjectName();
        final String userId = request.getUserId();

        final int[] policy = new int[1];
        final boolean[] dedup = new boolean[1];
        final String method = "PutObject";
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        PartInfo partInfo = new PartInfo()
                .setUploadId(uploadId)
                .setBucket(request.getBucketName())
                .setObject(request.getObjectName())
                .setPartNum(numStr)
                .setLastModified(MsDateUtils.stampToISO8601(System.currentTimeMillis()))
                .setPartSize(isChunked ? Long.MAX_VALUE : Long.parseLong(request.headers().get(CONTENT_LENGTH)));

        String[] fileName = new String[]{null};
        StoragePool[] dataPool = new StoragePool[]{null};
        String[] accountQuota = new String[]{null};
        String[] objNumQuota = new String[]{null};
        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        boolean[] hasStartFS = new boolean[1];
        int[] startFsType = {0};
        AtomicReference<String> crypto = new AtomicReference<>();
        Inode[] dirInodes = {null};
        //控制partUpload中数据修复消息发送的流。
        MonoProcessor<Boolean> recoverDataProcessor = MonoProcessor.create();
        Disposable disposable = pool.getReactive(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_ID_NAME)
                .flatMap(username -> pool.getReactive(REDIS_USERINFO_INDEX).hgetall(username))
                .flatMap(b -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName))
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo,
                        new MsException(ErrorNo.NO_SUCH_BUCKET, "No such bucket")))
                .flatMap(bucketInfo -> pool.getReactive(REDIS_USERINFO_INDEX).hgetall(bucketInfo.get("user_name")).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> {
                    accountQuota[0] = tuple2.getT1().get(USER_DATABASE_ACCOUNT_QUOTA_FLAG);
                    objNumQuota[0] = tuple2.getT1().get(USER_DATABASE_ACCOUNT_OBJNUM_FLAG);
                })
                .map(Tuple2::getT2)
                .doOnNext(bucketInfo -> {
                    regionCheck(bucketInfo.get(REGION_NAME));
                    siteCheck(bucketInfo);
//                    AuthorizeV4.checkTransferEncoding(request);
                    if (!request.headers().contains(IS_SYNCING)) {
                        quotaCheck(bucketInfo, accountQuota[0], objNumQuota[0]);
                    }
                    CheckUtils.hasStartFS(bucketInfo, hasStartFS);
                    startFsType[0] = ACLUtils.setFsProtoType(bucketInfo);
                })
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, objectName, method).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .map(Tuple2::getT2)
                .flatMap(bucketInfo -> {
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
//                    String bucketVnodeId = bucketPool.getBucketVnodeId(bucketName, objectName);
                    com.macrosan.utils.functional.Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(bucketName, objectName);
                    String bucketVnodeId = bucketVnodeIdTuple.var1;
                    String bucketMigrateVnodeId = bucketVnodeIdTuple.var2;
                    return bucketPool.mapToNodeInfo(bucketVnodeId)
                            .flatMap(bucketVnodeList -> PartClient.getInitPartInfo(bucketName, objectName, uploadId, bucketVnodeList, request, currentSnapshotMark[0], snapshotLink[0])
                                    .flatMap(info -> {
                                        if (hasStartFS[0]) {
                                            // 此处查询是为了判断是否存在告警，如果存在告警则不允许上传。
                                            return FSQuotaUtils.existQuotaInfoS3(bucketName, objectName, System.currentTimeMillis(), request.getUserId())
                                                    .flatMap(tuple2 -> {
                                                        if (tuple2.var1) {
                                                            partInfo.setTmpUpdateQuotaKeyStr(Json.encode(tuple2.var2));
                                                            return FSQuotaUtils.S3CanWrite(bucketName, tuple2.var2)
                                                                    .map(l -> info);
                                                        }
                                                        return Mono.just(info);
                                                    });
                                        }
                                        return Mono.just(info);
                                    })
                                    .flatMap(initPartInfo -> {
                                        checkPart(initPartInfo, request.getUserId(), uploadId, currentSnapshotMark[0]);
                                        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH)) && policy[0] == 0) {
                                            MsAclUtils.checkWriteAcl(bucketInfo, request.getUserId(), bucketName);
                                            if (hasStartFS[0]) {
                                                return S3ACL.checkFSACL(objectName, bucketName, request.getUserId(), dirInodes, 7, false, startFsType[0], ACLUtils.setDirOrFile(false))
                                                        .map(b -> initPartInfo);
                                            }
                                        }

                                        return Mono.just(initPartInfo);
                                    })
                                    .flatMap(initPartInfo -> {
                                        dataPool[0] = StoragePoolFactory.getStoragePool(initPartInfo);
                                        partInfo.setStorage(initPartInfo.storage);
                                        // 使用当前最新快照标记；partInfo中的mark不一定和initPartInfo中的mark相同（快照创建前initpart，快照创建后upload part）
                                        partInfo.setSnapshotMark(currentSnapshotMark[0]);
                                        fileName[0] = Utils.getPartFileName(dataPool[0], request.getBucketName(), request.getObjectName(), uploadId, numStr, requestId);
                                        partInfo.setFileName(fileName[0]);
                                        partInfo.setVersionId(initPartInfo.metaData.versionId);
                                        partInfo.setInitSnapshotMark(initPartInfo.snapshotMark);
                                        crypto.set(initPartInfo.metaData.crypto);
                                        String objectVnodeId = dataPool[0].getObjectVnodeId(fileName[0]);
                                        return dataPool[0].mapToNodeInfo(objectVnodeId);
                                    })
//                                    .doOnError(e -> dealException(request, e))
                                    .zipWith(BucketSyncSwitchCache.isSyncSwitchOffMono(bucketName))
                                    .map(tuple2 -> {
                                        String versionNum = VersionUtil.getVersionNum();
                                        request.addMember(REQUEST_VERSIONNUM, versionNum);
                                        requestVersionNumMap.computeIfAbsent(bucketName, k -> new ConcurrentSkipListMap<>()).put(versionNum, request);
                                        return tuple2.getT1();
                                    })
                                    .flatMap(nodeList -> PartClient.partUpload(dataPool[0], fileName[0], nodeList, request, partInfo, recoverDataProcessor, crypto.get(), isChunked))
                                    .flatMap(md5 -> StoragePoolFactory.getDeduplicateByBucketName(bucketName).zipWith(Mono.just(md5)))
                                    .doOnNext(tuple2 -> dedup[0] = tuple2.getT1())
                                    .zipWith(VersionUtil.getVersionNum(bucketName, objectName))
                                    .flatMap(tuple3 -> {
                                        String md5 = tuple3.getT1().getT2();
                                        String versionNum = tuple3.getT2();
                                        if (StringUtils.isBlank(md5)) {
                                            return Mono.error(new MsException(ErrorNo.UNKNOWN_ERROR, "Upload Part fail"));
                                        }

                                        if (contentMD5 != null && !PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
                                            checkMd5(contentMD5, md5);
                                        }
                                        partInfo.setEtag(md5);
                                        Optional.ofNullable(request.getAllowCommit()).ifPresent(req -> request.setAllowCommit(false));
                                        partInfo.setVersionNum(versionNum)
                                                .setSyncStamp(StringUtils.isNotBlank(request.getHeader(SYNC_STAMP)) ? request.getHeader(SYNC_STAMP) : versionNum);

                                        if (dedup[0]) {
                                            StoragePool dedupPool = StoragePoolFactory.getMetaStoragePool(md5);
                                            String dedupVnode = dedupPool.getBucketVnodeId(md5);
                                            String suffix = Utils.getDeduplicatMetaKey(dedupVnode, md5, partInfo.storage, requestId);
                                            return dedupPool.mapToNodeInfo(dedupVnode)
                                                    .flatMap(nodeList -> putPartDeduplicate(dedupPool, suffix,
                                                            nodeList, request, recoverDataProcessor, partInfo))
                                                    .flatMap(b -> {
                                                        if (!b) {
                                                            throw new MsException(UNKNOWN_ERROR, "put deduplicate meta failed");
                                                        }
                                                        if (b) {
                                                            partInfo.deduplicateKey = suffix;
                                                        }
                                                        return Mono.just(b);
                                                    }).flatMap(b -> {
                                                        return Mono.just(bucketMigrateVnodeId != null)
                                                                .flatMap(b1 -> {
                                                                    if (b1) {
                                                                        return bucketPool.mapToNodeInfo(bucketMigrateVnodeId)
                                                                                .flatMap(nodeList -> PartClient.partUploadMeta(partInfo, nodeList, recoverDataProcessor, request, snapshotLink[0]));
                                                                    }
                                                                    return Mono.just(true);
                                                                })
                                                                .doOnNext(b1 -> {
                                                                    if (!b1) {
                                                                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "Upload Part Meta to new mapping fail");
                                                                    }
                                                                })
                                                                .flatMap(b1 -> PartClient.partUploadMeta(partInfo, bucketVnodeList, recoverDataProcessor, request, snapshotLink[0]))
                                                                .doOnNext(b1 -> {
                                                                    if (!b1) {
                                                                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "Upload Part Meta fail");
                                                                    }
                                                                    Optional.ofNullable(request.getAllowCommit()).ifPresent(req -> request.setAllowCommit(true));
                                                                    QuotaRecorder.addCheckBucket(bucketName);
                                                                })
                                                                .map(b1 -> md5);
                                                    });
                                        } else {
                                            return Mono.just(bucketMigrateVnodeId != null)
                                                    .flatMap(b -> {
                                                        if (b) {
                                                            return bucketPool.mapToNodeInfo(bucketMigrateVnodeId)
                                                                    .flatMap(nodeList -> PartClient.partUploadMeta(partInfo, nodeList, recoverDataProcessor, request, snapshotLink[0]));
                                                        }
                                                        return Mono.just(true);
                                                    })
                                                    .doOnNext(b -> {
                                                        if (!b) {
                                                            throw new MsException(ErrorNo.UNKNOWN_ERROR, "Upload Part Meta to new mapping fail");
                                                        }
                                                    })
                                                    .flatMap(b -> PartClient.partUploadMeta(partInfo, bucketVnodeList, recoverDataProcessor, request, snapshotLink[0]))
                                                    .doOnNext(b -> {
                                                        if (!b) {
                                                            throw new MsException(ErrorNo.UNKNOWN_ERROR, "Upload Part Meta fail");
                                                        }
                                                        Optional.ofNullable(request.getAllowCommit()).ifPresent(req -> request.setAllowCommit(true));
                                                        QuotaRecorder.addCheckBucket(bucketName);
                                                    })
                                                    .map(b -> md5);
                                        }
                                    }));
                })
                .subscribe(md5 -> {
                    try {
                        if (!request.response().ended()) {
                            addPublicHeaders(request, requestId)
                                    .putHeader(CONTENT_LENGTH, "0")
                                    .putHeader(ETAG, '"' + md5 + '"');
                            CryptoUtils.addCryptoResponse(request, crypto.get());
                            addAllowHeader(request.response()).end();
                        }
                    } catch (Exception e) {
                        log.error("requestChannel frame error :", e);
                    }

                }, e -> dealException(request, e));

        request.addResponseCloseHandler(v -> disposable.dispose());

        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 处理上传分段请求
     *
     * @param request
     * @return
     */
    private int uploadPartCopy(MsHttpRequest request) {
        String copySource = request.getHeader(X_AMZ_COPY_SOURCE);
        if (StringUtils.isNotBlank(request.getHeader(SYNC_COPY_SOURCE))) {
            copySource = request.getHeader(SYNC_COPY_SOURCE);
        }

        String numStr = request.getParam(PART_NUMBER);
        final String uploadId = request.getParam(UPLOAD_ID);
        partUploadCheck(numStr, uploadId);

        String sourceVersionId;
        try {
            String[] copySourceArr = copySource.split("\\?", 2);
            sourceVersionId = copySourceArr.length == 1 ? "" : copySourceArr[1].substring(copySourceArr[1].indexOf("=") + 1);
            copySource = URLDecoder.decode(copySourceArr[0], "UTF-8");
            if (copySource.startsWith(SLASH)) {
                copySource = copySource.substring(1);
            }
        } catch (UnsupportedEncodingException e) {
            throw new MsException(INVALID_ARGUMENT, "The charset of x-amz-copy-source is incorrect!");
        }
        String[] array = copySource.split("/", 2);
        if (array.length < 2) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "The format of x-amz-copy-source is incorrect!");
        }

        final String sourceBucket = ServerConfig.isBucketUpper() ? array[0].toLowerCase() : array[0];
        final String sourceObject = array[1];
        final String targetBucket = request.getBucketName();
        final String targetObject = request.getObjectName();
        final String userId = request.getUserId();
        final String requestId = getRequestId();

        final String lastModified = MsDateUtils.stampToISO8601(System.currentTimeMillis());
        final int[] policy = new int[1];
        final int[] targrtPolicy = new int[1];
        final boolean[] dedup = new boolean[1];
        final String sourceMethod = "".equals(sourceVersionId) ? "GetObject" : "GetObjectVersion";
        final String targetMethod = "PutObject";

        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(sourceBucket);
        PartInfo partInfo = new PartInfo()
                .setUploadId(uploadId)
                .setBucket(request.getBucketName())
                .setObject(request.getObjectName())
                .setPartNum(numStr)
                .setLastModified(lastModified);
        com.macrosan.utils.functional.Tuple2<String, String> targetBucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(targetBucket, targetObject);
        String targetBucketVnodeId = targetBucketVnodeIdTuple.var1;
        String targetBucketMigrateVnodeId = targetBucketVnodeIdTuple.var2;

        String[] fileName = new String[]{null};
        StoragePool[] targetPool = new StoragePool[]{null};
        String[] bucketUserId = new String[]{null};
        MetaData[] sourceMeta = new MetaData[]{null};
        String[] versionIdHeader = new String[]{null};

        Map<String, String> heads = new HashMap<>(16);
        Map<String, String> sysMetaMap = new HashMap<>();
        checkCondition(heads, request);
        String[] accountQuota = new String[]{null};
        String[] objNumQuota = new String[]{null};

        String[] crypto = new String[1];

        final byte[] xmlHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>".getBytes();
        final AtomicInteger times = new AtomicInteger(0);
        long timerId = vertx.setPeriodic(20000, (idP) -> {
            if (times.incrementAndGet() == 1) {
                addPublicHeaders(request, requestId).putHeader(CONTENT_TYPE, "application/xml")
                        .putHeader(TRANSFER_ENCODING, "chunked")
                        .write(Buffer.buffer(xmlHeader));
            }
            request.response().write("\n");
        });
        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        boolean[] hasStartFS = new boolean[1];
        int[] sourceStartFsType = {0};
        int[] targetStartFsType = {0};
        //控制partUpload中数据修复消息发送的流。
        MonoProcessor<Boolean> recoverDataProcessor = MonoProcessor.create();
        Disposable disposable = pool.getReactive(REDIS_USERINFO_INDEX).hget(userId, USER_DATABASE_ID_NAME)
                .flatMap(username -> pool.getReactive(REDIS_USERINFO_INDEX).hgetall(username))
                .doOnNext(accountInfo -> {
                    accountQuota[0] = accountInfo.get(USER_DATABASE_ACCOUNT_QUOTA_FLAG);
                    objNumQuota[0] = accountInfo.get(USER_DATABASE_ACCOUNT_OBJNUM_FLAG);
                })
                .flatMap(b -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(sourceBucket))
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo, new MsException(NO_SUCH_BUCKET, "no such bucket. bucket name :" + sourceBucket + ".")))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .flatMap(obj -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(targetBucket).zipWith(Mono.just(obj)))
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo.getT1(), new MsException(NO_SUCH_BUCKET, "no such bucket. bucket name :" + targetBucket + ".")))
                .doOnNext(bucketInfo -> copySyncCheck(bucketInfo.getT2().get(DATA_SYNC_SWITCH), bucketInfo.getT1().get(DATA_SYNC_SWITCH)))
                .map(twoBucketInfo -> {
                    sourceStartFsType[0] = ACLUtils.setFsProtoType(twoBucketInfo.getT2());
                    targetStartFsType[0] = ACLUtils.setFsProtoType(twoBucketInfo.getT1());
                    return twoBucketInfo.getT1();
                })
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .doOnNext(bucketInfo -> {
                    if (!request.headers().contains(IS_SYNCING)) {
                        quotaCheck(bucketInfo, accountQuota[0], objNumQuota[0]);
                    }
                })
                .doOnNext(bucketInfo -> {
                    CheckUtils.hasStartFS(bucketInfo, hasStartFS);
                })
                .doOnNext(bucketInfo -> bucketUserId[0] = bucketInfo.get(BUCKET_USER_ID))
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, targetBucket, targetObject, targetMethod).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> targrtPolicy[0] = tuple2.getT1())
                .flatMap(tuple2 -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, sourceBucket, sourceObject, sourceMethod).zipWith(Mono.just(tuple2.getT2())))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .map(Tuple2::getT2)
                .doOnNext(bucketInfo -> {
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                })
                .flatMap(bucketInfo -> {
                    if (!PASSWORD.equals(request.getHeader(SYNC_AUTH)) && targrtPolicy[0] == 0) {
                        if (hasStartFS[0]) {
                            return S3ACL.checkFSACL(targetObject, targetBucket, userId, null, 7, false, targetStartFsType[0], ACLUtils.setDirOrFile(false))
                                    .flatMap(b -> MsAclUtils.checkWriteAclReactive(bucketInfo, userId, targetBucket, null));
                        } else {
                            return MsAclUtils.checkWriteAclReactive(bucketInfo, userId, targetBucket, null);
                        }
                    }
                    return Mono.empty();
                })
                .switchIfEmpty(bucketPool.getBucketShardCache().getCache().containsKey(sourceBucket) ?
                        Mono.just(bucketPool.getBucketVnodeId(sourceBucket, sourceObject)) :
                        Mono.error(new MsException(NO_SUCH_BUCKET, "no such bucket. bucket name :" + sourceBucket + ".")))
                .flatMap(vnode -> bucketPool.mapToNodeInfo(String.valueOf(vnode)))
                .flatMap(list -> getObjectMetaVersionUnlimited(sourceBucket, sourceObject, sourceVersionId, list, request, currentSnapshotMark[0], snapshotLink[0]))
                .flatMap(metadata -> {
                    sourceMeta[0] = metadata;
                    AuthorizeV4.checkTransferEncoding(request);
                    checkMetaData(sourceObject, metadata, sourceVersionId, currentSnapshotMark[0]);
                    Map objAclMap = Json.decodeValue(metadata.objectAcl, Map.class);
                    if (policy[0] == 0 && !hasStartFS[0]) {
                        checkObjectReadAcl(objAclMap, bucketUserId[0], userId);
                    }
                    JsonObject sysMeta = new JsonObject(metadata.sysMetaData);
                    sysMeta.fieldNames().forEach(key -> sysMetaMap.put(key, sysMeta.getString(key)));
                    checkIfMatch(sysMetaMap, heads, true);
                    return Mono.just(hasStartFS[0])
                            .flatMap(b -> {
                                if (!b || policy[0] != 0) {
                                    return Mono.just(true);
                                }

                                checkFsObjectReadAcl(objAclMap, bucketUserId[0], userId, metadata);
                                S3ACL.judgeMetaFSACL(userId, metadata, 6, sourceStartFsType[0]);
                                return S3ACL.checkFSACL(sourceObject, sourceBucket, userId, null, 6, false, sourceStartFsType[0], 0);
                            })
                            .flatMap(b -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(targetBucket)
                                    .zipWith(bucketPool.mapToNodeInfo(targetBucketVnodeId)));
                })
                .flatMap(tuple2 -> PartClient.getInitPartInfo(targetBucket, targetObject, uploadId, tuple2.getT2(), request, currentSnapshotMark[0], snapshotLink[0])
                                .flatMap(info -> {
                                    if (hasStartFS[0]) {
                                        // 此处查询是为了判断是否存在告警，如果存在告警则不允许上传。
                                        return FSQuotaUtils.existQuotaInfoS3(targetBucket, targetObject, System.currentTimeMillis(), userId)
                                                .flatMap(t2 -> {
                                                    if (t2.var1) {
                                                        partInfo.setTmpUpdateQuotaKeyStr(Json.encode(t2.var2));
                                                        return FSQuotaUtils.S3CanWrite(targetBucket, t2.var2)
                                                                .map(l -> info);
                                                    }
                                                    return Mono.just(info);
                                                });
                                    }
                                    return Mono.just(info);
                                })
                                .flatMap(initPartInfo -> {
                                    checkPart(initPartInfo, request.getUserId(), uploadId, currentSnapshotMark[0]);
                                    targetPool[0] = StoragePoolFactory.getStoragePool(initPartInfo);
                                    fileName[0] = Utils.getPartFileName(targetPool[0], request.getBucketName(), request.getObjectName(), uploadId, numStr, requestId);
                                    partInfo.setFileName(fileName[0]);
                                    partInfo.setStorage(initPartInfo.storage);
                                    partInfo.setVersionId(initPartInfo.metaData.versionId);
                                    partInfo.setSnapshotMark(currentSnapshotMark[0]);
                                    partInfo.setInitSnapshotMark(initPartInfo.snapshotMark);
                                    versionIdHeader[0] = initPartInfo.metaData.versionId;
                                    crypto[0] = initPartInfo.metaData.getCrypto();
                                    return targetPool[0].mapToNodeInfo(targetPool[0].getObjectVnodeId(fileName[0]));
                                })
                                .doOnError(e -> dealException(request, e))
                                .zipWith(BucketSyncSwitchCache.isSyncSwitchOffMono(targetBucket))
                                .map(tuple2S -> {
                                    String versionNum = VersionUtil.getVersionNum();
                                    request.addMember(REQUEST_VERSIONNUM, versionNum);
                                    requestVersionNumMap.computeIfAbsent(targetBucket, k -> new ConcurrentSkipListMap<>()).put(versionNum, request);
                                    return tuple2S.getT1();
                                })
                                .flatMap(targetNodeList -> {
                                    return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(sourceBucket, sourceObject))
                                            .flatMap(nodeList -> getObjectMetaVersionUnlimited(sourceBucket, sourceObject, sourceVersionId, nodeList, request, currentSnapshotMark[0], snapshotLink[0]))
                                            .flatMap(metaData -> {
                                                checkMetaData(sourceObject, metaData, sourceVersionId, currentSnapshotMark[0]);
                                                sourceMeta[0] = metaData;
                                                long total = sourceMeta[0].endIndex - sourceMeta[0].startIndex + 1;
                                                long startIndex = 0;
                                                long endIndex = sourceMeta[0].endIndex;
                                                if (request.headers().contains(X_AMZ_COPY_SOURCE_RANGE)) {
                                                    String range = request.getHeader(X_AMZ_COPY_SOURCE_RANGE);
                                                    LongLongTuple t = dealCopyRange(range, total);
                                                    startIndex = t.var1;
                                                    endIndex = t.var2;
                                                    sourceMeta[0].setEndIndex(endIndex - startIndex);
                                                }
                                                partInfo.setPartSize(endIndex - startIndex + 1);
                                                return PartClient.getThenPartUpload(targetPool[0], fileName[0], sourceMeta[0],
                                                        startIndex, endIndex, targetNodeList, request, partInfo, crypto[0], recoverDataProcessor);
                                            });
                                })
                                .flatMap(md5 -> StoragePoolFactory.getDeduplicateByBucketName(targetBucket).zipWith(Mono.just(md5)))
                                .doOnNext(stringTuple2 -> dedup[0] = stringTuple2.getT1())
                                .zipWith(VersionUtil.getVersionNum(targetBucket, targetObject))
                                .flatMap(t2 -> {
                                    String md5 = t2.getT1().getT2();
                                    String versionNum = t2.getT2();
                                    if (StringUtils.isBlank(md5)) {
                                        return Mono.error(new MsException(UNKNOWN_ERROR, "Upload Part copy fail"));
                                    }
                                    partInfo.setEtag(md5);
                                    partInfo.setVersionNum(versionNum)
                                            .setSyncStamp(StringUtils.isNotBlank(request.getHeader(SYNC_STAMP)) ? request.getHeader(SYNC_STAMP) : versionNum);
//                                    String deduplicate = StoragePoolFactory.getDeduplicate(StorageOperate.DATA, targetBucket);

                                    if (dedup[0]) {
                                        StoragePool dedupPool = StoragePoolFactory.getMetaStoragePool(md5);
                                        String dedupVnode = dedupPool.getBucketVnodeId(md5);
                                        String suffix = Utils.getDeduplicatMetaKey(dedupVnode, md5, partInfo.storage, requestId);
                                        return dedupPool.mapToNodeInfo(dedupVnode)
                                                .flatMap(nodeList -> putPartDeduplicate(dedupPool, suffix,
                                                        nodeList, request, recoverDataProcessor, partInfo))
                                                .flatMap(b -> {
                                                    if (!b) {
                                                        throw new MsException(UNKNOWN_ERROR, "put deduplicate meta failed");
                                                    }
                                                    if (b) {
                                                        partInfo.deduplicateKey = suffix;
                                                    }
                                                    return Mono.just(b);
                                                }).flatMap(b1 -> {
                                                    return Mono.just(targetBucketMigrateVnodeId != null)
                                                            .flatMap(b -> {
                                                                if (b) {
                                                                    return bucketPool.mapToNodeInfo(targetBucketMigrateVnodeId)
                                                                            .flatMap(nodeList -> PartClient.partUploadMeta(partInfo, nodeList, recoverDataProcessor, request, snapshotLink[0]));
                                                                }
                                                                return Mono.just(true);
                                                            })
                                                            .doOnNext(b -> {
                                                                if (!b) {
                                                                    throw new MsException(UNKNOWN_ERROR, "Upload Part Meta to new Mapping fail");
                                                                }
                                                            })
                                                            .flatMap(b -> PartClient.partUploadMeta(partInfo, tuple2.getT2(), recoverDataProcessor, request, snapshotLink[0]))
                                                            .doOnNext(b -> {
                                                                if (!b) {
                                                                    throw new MsException(UNKNOWN_ERROR, "Upload Part Meta fail");
                                                                }
                                                                QuotaRecorder.addCheckBucket(targetBucket);

                                                            })
                                                            .map(b -> md5);
                                                });
                                    } else {
                                        return Mono.just(targetBucketMigrateVnodeId != null)
                                                .flatMap(b -> {
                                                    if (b) {
                                                        return bucketPool.mapToNodeInfo(targetBucketMigrateVnodeId)
                                                                .flatMap(nodeList -> PartClient.partUploadMeta(partInfo, nodeList, recoverDataProcessor, request, snapshotLink[0]));
                                                    }
                                                    return Mono.just(true);
                                                })
                                                .doOnNext(b -> {
                                                    if (!b) {
                                                        throw new MsException(UNKNOWN_ERROR, "Upload Part Meta to new Mapping fail");
                                                    }
                                                })
                                                .flatMap(b -> PartClient.partUploadMeta(partInfo, tuple2.getT2(), recoverDataProcessor, request, snapshotLink[0]))
                                                .doOnNext(b -> {
                                                    if (!b) {
                                                        throw new MsException(UNKNOWN_ERROR, "Upload Part Meta fail");
                                                    }
                                                    QuotaRecorder.addCheckBucket(targetBucket);

                                                })
                                                .map(b -> md5);
                                    }

//                            return PartClient.partUploadMeta(partInfo, tuple2.getT2(), recoverDataProcessor, request)
//                                    .doOnNext(b -> {
//                                        if (!b) {
//                                            throw new MsException(ErrorNo.UNKNOWN_ERROR, "Upload Part Meta fail");
//                                        }
//                                        QuotaRecorder.addCheckBucket(targetBucket);
//
//                                    })
//                                    .map(b -> md5);
                                })
                )
                .doFinally(md5 -> vertx.cancelTimer(timerId))
                .subscribe(md5 -> {
                    try {
                        vertx.cancelTimer(timerId);
                        CopyPartResult copyPartResult = new CopyPartResult()
                                .setLastModified(lastModified)
                                .setEtag('"' + md5 + '"');
                        byte[] res = JaxbUtils.toByteArray(copyPartResult);
                        if (!request.response().ended()) {
                            if (times.get() > 0) {
                                request.response().end(Buffer.buffer(res).slice(xmlHeader.length, res.length));
                            } else {
                                addPublicHeaders(request, requestId)
                                        .putHeader(CONTENT_TYPE, "application/xml")
                                        .putHeader(CONTENT_LENGTH, String.valueOf(res.length))
                                        .putHeader(X_AMX_VERSION_ID, versionIdHeader[0])
                                        .putHeader(X_AMX_SOURCE_VERSION_ID, sourceVersionId)
                                        .write(Buffer.buffer(res));
                                CryptoUtils.addCryptoResponse(request, crypto[0]);
                                addAllowHeader(request.response()).end();
                            }
                        }
                    } catch (Exception e) {
                        log.error("requestChannel frame error :", e);
                    }

                }, e -> {
                    if (times.get() > 0) {
                        vertx.cancelTimer(timerId);
                        dealException(request, e, true);
                    } else {
                        vertx.cancelTimer(timerId);
                        dealException(request, e);
                    }
                });

        request.addResponseCloseHandler(v -> disposable.dispose());

        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * partUpload前的参数检查
     *
     * @param partNumber    分段编号
     * @param uploadId      上传id
     * @param contentLength 请求体长度
     */
    private void partUploadCheck(String partNumber, String uploadId, String contentLength, String decodeLength, boolean isChunked) {
        partUploadCheck(partNumber, uploadId);
        if (!isChunked) {
            try {
                if (contentLength == null) {
                    throw new MsException(ErrorNo.MISSING_CONTENT_LENGTH, "uploads_part error, no content-length param");
                }
                if (decodeLength != null && Long.parseLong(decodeLength) > MAX_PUT_SIZE) {
                    throw new MsException(OBJECT_SIZE_OVERFLOW, "part size too large");
                }
                if (decodeLength == null && Long.parseLong(contentLength) > MAX_PUT_SIZE) {
                    throw new MsException(OBJECT_SIZE_OVERFLOW, "part size too large");
                }
            } catch (NumberFormatException e) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "uploads_part error, part num " + partNumber + " is invalid.");
            }
        } else {
            if (contentLength != null || decodeLength != null) {
                throw new MsException(CONTENT_LENGTH_NOT_ALLOWED, "content-length is not allowed when using Chunked transfer encoding");
            }
        }

    }

    private void partUploadCheck(String partNumber, String uploadId) {
        try {
            int num = Integer.parseInt(partNumber);
            if (num < 1 || num > MAX_UPLOAD_PART_NUM) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "uploads_part error, part num " + partNumber + " is invalid.");
            }
            if (StringUtils.isBlank(uploadId)) {
                throw new MsException(ErrorNo.NO_SUCH_UPLOAD_ID, "uploads_part error,uploadId is invalid.");
            }
        } catch (NumberFormatException e) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "uploads_part error, part num " + partNumber + " is invalid.");
        }
    }

    /**
     * 处理列出所有未完成分段任务请求
     *
     * @param request
     * @return
     */
    public int listMultiPartUploads(MsHttpRequest request) {
        String bucketName = request.getBucketName();
        String marker = request.getParam(KEY_MARKER, "");
        String uploadIdMarker = request.getParam(UPLOAD_ID_MARKER, "");
        String prefix = request.getParam(PREFIX, "");
        String delimiter = request.getParam(DELIMITER, "");
        int maxKyesInt = getMaxKey(request);

        final int[] policy = new int[1];
        final String method = "ListMultipartUploads";
        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("bucket", bucketName)
                .put("maxKeys", String.valueOf(maxKyesInt))
                .put("prefix", prefix)
                .put("marker", marker)
                .put("delimiter", delimiter);

        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);

        Disposable[] disposables = new Disposable[3];
        disposables[0] = pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                .defaultIfEmpty(new HashMap<>(0))
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo, new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName)))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, method).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .map(Tuple2::getT2)
                .flatMap(bucketInfo -> {
                    if (policy[0] == 0) {
                        return checkReadAclReactive(bucketInfo, request.getUserId(), bucketName, msg)
                                .switchIfEmpty(Mono.just(bucketInfo)).map(s -> bucketInfo);
                    }
                    return Mono.just(bucketInfo);
                })
                .flatMap(bucketInfo -> {
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                    String bucketVnode = storagePool.getBucketVnodeId(bucketName, marker);
                    return pool.getReactive(REDIS_USERINFO_INDEX).hget(bucketInfo.get(BUCKET_USER_ID), USER_DATABASE_ID_NAME)
                            .map(userName -> new Owner(bucketInfo.get(BUCKET_USER_ID), userName))
                            .zipWith(storagePool.mapToNodeInfo(bucketVnode))
                            .flatMap(tuple -> {
                                if (StringUtils.isNotBlank(uploadIdMarker) && StringUtils.isNotBlank(marker)) {
                                    return PartClient.getInitPartInfo(bucketName, marker, uploadIdMarker, tuple.getT2(), request, currentSnapshotMark[0], snapshotLink[0])
                                            .doOnNext(info -> {
                                                if (info.equals(NO_SUCH_UPLOAD_ID_INIT_PART_INFO)) {
                                                    throw new MsException(ErrorNo.INVALID_ARGUMENT, "Invalid uploadId marker");
                                                }
                                            }).map(info -> tuple);
                                } else {
                                    return Mono.just(tuple);
                                }
                            });

                }).subscribe(tuple ->
                {
                    try {
                        PartClient.listMultiPartUploads(tuple.getT1(), tuple.getT2(), request, currentSnapshotMark[0], snapshotLink[0], disposables);
                    } catch (Exception e) {
                        dealException(request, e);
                    }
                }, e -> dealException(request, e));

        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> {
            for (Disposable disposable : disposables) {
                if (disposable != null) {
                    disposable.dispose();
                }
            }
        }));

        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 列出以上传成功的分段文件
     *
     * @param request
     * @return
     */
    public int listParts(MsHttpRequest request) {
        final String bucketName = request.getBucketName();
        final String userId = request.getUserId();
        final String uploadId = request.getParam(UPLOAD_ID);
        final String objectName = request.getObjectName();
        String partNumMarker = request.getParam(PART_NUMBER_MARKER, "0");
        String maxPart = request.getParam(MAX_PART, "1000");
        int maxPartInt;
        final int partNumberMarKerInt;

        final String method = "ListParts";

        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};

        try {
            partNumberMarKerInt = Integer.parseInt(partNumMarker);
            maxPartInt = Integer.parseInt(maxPart);
            if (maxPartInt < 1 || maxPartInt > 1000) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, " max_parts param error, must be int in [1, 1000]");
            }
            if (partNumberMarKerInt < 0 || partNumberMarKerInt > MAX_UPLOAD_PART_NUM - 1) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, " part_number_marker param error, must be int in [0,"+ (MAX_UPLOAD_PART_NUM - 1) + "]");
            }
        } catch (Exception e) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, " max_parts or partNumMarker param error,max_parts must be int in [1, 1000], partNumMarker must be int in [0, 9999]");
        }

        ListPartsResult listPartsResult = new ListPartsResult()
                .setBucket(bucketName)
                .setMaxParts(maxPartInt)
                .setKey(objectName)
                .setUploadId(uploadId)
                .setPartNumberMarker(partNumberMarKerInt);
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);

        Disposable subscribe = pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo, new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName)))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, objectName, method).zipWith(Mono.just(bucketInfo)))
                .map(Tuple2::getT2)
                .flatMap(bucketInfo -> {
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                    String bucketVnode = storagePool.getBucketVnodeId(bucketName, objectName);
                    return pool.getReactive(REDIS_USERINFO_INDEX).hget(bucketInfo.get(BUCKET_USER_ID), USER_DATABASE_ID_NAME)
                            .flatMap(userName -> {
                                Owner owner = new Owner().setDisplayName(userName).setId(bucketInfo.get(BUCKET_USER_ID));
                                listPartsResult.setOwner(owner);
                                return storagePool.mapToNodeInfo(bucketVnode);
                            })
                            .flatMap(list -> PartClient.getInitPartInfo(bucketName, objectName, uploadId, list, request, currentSnapshotMark[0], snapshotLink[0])
                                    .doOnNext(info -> {
                                        checkPart(info, userId, uploadId, bucketInfo, request);
                                        Owner owner = new Owner().setDisplayName(info.initAccountName).setId(info.initAccount);
                                        listPartsResult.setInitiator(owner);
                                    }).map(info -> list))
                            .flatMap(nodeList -> PartClient.listParts(listPartsResult, nodeList, request, currentSnapshotMark[0], snapshotLink[0]));
                })
                .doOnNext(b -> {
                    if (!b) {
                        throw new MsException(UNKNOWN_ERROR, "list parts fail");
                    }
                })
                .subscribe(b -> {
                    try {
                        byte[] res = JaxbUtils.toByteArray(listPartsResult);
                        addPublicHeaders(request, getRequestId())
                                .putHeader(CONTENT_TYPE, "application/xml")
                                .putHeader(CONTENT_LENGTH, String.valueOf(res.length))
                                .write(Buffer.buffer(res));
                        addAllowHeader(request.response()).end();
                    } catch (Exception e) {
                        log.error("list parts error:{}", e.getMessage());
                    }
                }, e -> dealException(request, e));
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));

        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 处理合并分段文件的请求
     *
     * @param request
     * @return
     */
    public int completeMultiPartUpload(MsHttpRequest request) {
        final String bucketName = request.getBucketName();
        final String uploadId = request.getParam(UPLOAD_ID);
        final String objectName = request.getObjectName();
        AtomicReference<String> versionId = new AtomicReference<>();
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        com.macrosan.utils.functional.Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(bucketName, objectName);
        String bucketVnodeId = bucketVnodeIdTuple.var1;
        String bucketMigrateVnodeId = bucketVnodeIdTuple.var2;

        final String method = "PutObject";
        final int[] policy = new int[1];
        String[] crypto = new String[1];
        boolean[] hasStartFS = new boolean[1];
        int[] startFsType = {0};
        boolean[] isHardLink = new boolean[1];
        Inode[] oldInode0 = new Inode[1];
        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        Inode[] dirInodes = {null};
        Disposable subscribe = pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucketName)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo, new MsException(NO_SUCH_BUCKET, "No such bucket")))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, objectName, method).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .map(Tuple2::getT2)
                .flatMap(bucketInfo -> {
                    CheckUtils.hasStartFS(bucketInfo, hasStartFS);
                    startFsType[0] = ACLUtils.setFsProtoType(bucketInfo);
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                    return bucketPool.mapToNodeInfo(bucketVnodeId)
                            .flatMap(list -> PartClient.getInitPartInfo(bucketName, objectName, uploadId, list, request, currentSnapshotMark[0], snapshotLink[0])
                                    .flatMap(initPartInfo -> {
                                        if (hasStartFS[0]) {
                                            return FsUtils.lookup(bucketName, objectName, null, false, -1, null)
                                                    .flatMap(oldInode -> {
                                                        if (oldInode.getNodeId() > 0 && oldInode.getLinkN() > 1) {
                                                            isHardLink[0] = true;
                                                            oldInode0[0] = oldInode;
                                                        }
                                                        return Mono.just(initPartInfo);
                                                    });
                                        }
                                        return Mono.just(initPartInfo);
                                    })
                                    .flatMap(info -> {
                                        checkPart(info, request.getUserId(), uploadId, currentSnapshotMark[0]);
                                        JsonObject sysMeta = new JsonObject(info.metaData.sysMetaData);
                                        info.metaData.setSysMetaData(sysMeta.encode());
                                        versionId.set(info.getMetaData().getVersionId());
                                        if (!PASSWORD.equals(request.getHeader(SYNC_AUTH)) && policy[0] == 0) {
                                            MsAclUtils.checkWriteAcl(bucketInfo, request.getUserId(), bucketName);
                                            if (hasStartFS[0]) {
                                                return S3ACL.checkFSACL(objectName, bucketName, request.getUserId(), dirInodes, 7, false, startFsType[0], ACLUtils.setDirOrFile(false))
                                                        .map(b -> info);
                                            }
                                        }

                                        return Mono.just(info);
                                    })
                                    .flatMap(info -> {
                                        responseContinue(request);
                                        StoragePool dataPool = StoragePoolFactory.getStoragePool(info);

                                        boolean esEnable = !"off".equals(bucketInfo.getOrDefault("mda", "off"));
                                        crypto[0] = info.metaData.getCrypto();
                                        info.metaData.setSnapshotMark(currentSnapshotMark[0]);

                                        return MsObjVersionUtils.checkObjVersionsLimit(bucketName, objectName, request)
                                                .flatMap(b -> PartClient.completeMultiPartUpload(bucketName, objectName, uploadId, info,
                                                list, bucketMigrateVnodeId, request, esEnable, isHardLink[0], oldInode0[0], currentSnapshotMark[0], snapshotLink[0])
                                                .flatMap(etag -> {
                                                    if (isHardLink[0]) {
                                                        //删除inode缓存，分段覆盖同名对象，原inode缓存还在需进行删除
                                                        return Node.getInstance()
                                                                .getInodeAndUpdateCache(oldInode0[0].getBucket(), oldInode0[0].getNodeId())
                                                                .map(r -> etag);
                                                    }
                                                    return Mono.just(etag);
                                                })
                                                .flatMap(etag0 -> {
                                                    if (hasStartFS[0]) {
                                                        return InodeUtils.updateSpeciDirTime(dirInodes[0], objectName, bucketName)
                                                                .map(i -> etag0);
                                                    }
                                                    return Mono.just(etag0);
                                                }));
                                    })
                            );
                })
                .doOnNext(md5 -> {
                    if (StringUtils.isBlank(md5)) {
                        throw new MsException(UNKNOWN_ERROR, "Complete MultiPart Upload fail");
                    }
                    request.addMember("etag", md5);
                    if (hasStartFS[0]) {
                        NotifyServer.getInstance().maybeNotify(bucketName, objectName, FILE_NOTIFY_CHANGE_FILE_NAME, NotifyReply.NotifyAction.FILE_ACTION_ADDED);
                    }
                })
                .subscribe(md5 -> {
                    try {
                        if (!request.response().ended()) {
                            CompleteMultipartUploadResult completeMultipartUploadResult = new CompleteMultipartUploadResult()
                                    .setBucket(bucketName)
                                    .setKey(objectName)
                                    .setEtag('"' + md5 + '"');
                            byte[] res = JaxbUtils.toByteArray(completeMultipartUploadResult);
                            QuotaRecorder.addCheckBucket(bucketName);
                            HttpServerResponse response = addPublicHeaders(request, getRequestId())
                                    .putHeader(CONTENT_TYPE, "application/xml")
                                    .putHeader(CONTENT_LENGTH, String.valueOf(res.length))
                                    .write(Buffer.buffer(res));
                            if (versionId.get() != null) {
                                response.putHeader(X_AMX_VERSION_ID, versionId.get());
                            }
                            CryptoUtils.addCryptoResponse(request, crypto[0]);
                            addAllowHeader(request.response()).end();
                        }
                    } catch (Exception e) {
                        log.error("requestChannel frame error :", e);
                    }
                }, e -> dealException(request, e));
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 处理舍弃分段任务请求
     *
     * @param request
     * @return
     */
    public int abortMultiPartUpload(MsHttpRequest request) {
        final String bucketName = request.getBucketName();
        final String uploadId = request.getParam(UPLOAD_ID);
        final String objectName = request.getObjectName();

        final String method = "AbortMultipartUpload";
        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        com.macrosan.utils.functional.Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(bucketName, objectName);
        String bucketVnodeId = bucketVnodeIdTuple.var1;
        String migrateVnodeId = bucketVnodeIdTuple.var2;
        StoragePool[] dataPool = new StoragePool[]{null};
        List<String> updateQuotaDir = new LinkedList<>();
        Disposable subscribe = pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucketName)
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, objectName, method).zipWith(Mono.just(bucketInfo)))
                .map(Tuple2::getT2)
                .flatMap(bucketInfo -> {
                    if (bucketInfo.containsKey("fsid")) {
                        return FSQuotaUtils.existQuotaInfo(bucketName, objectName, System.currentTimeMillis(), 0, 0)
                                .flatMap(tuple2 -> {
                                    if (tuple2.var1) {
                                        updateQuotaDir.addAll(tuple2.var2);
                                    }
                                    return Mono.just(bucketInfo);
                                });
                    }
                    return Mono.just(bucketInfo);
                })
                .flatMap(bucketInfo -> {
                    //拿到vnode和节点信息 并进行检查
                    throwWhenEmpty(bucketInfo, new MsException(ErrorNo.NO_SUCH_BUCKET, "No such bucket"));
                    regionCheck(bucketInfo.get(REGION_NAME));
                    siteCheck(bucketInfo);
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                    return bucketPool.mapToNodeInfo(bucketVnodeId)
                            .flatMap(bucketNodeList -> PartClient.getInitPartInfo(bucketName, objectName, uploadId, bucketNodeList, request, currentSnapshotMark[0], snapshotLink[0])
                                    .doOnNext(info -> {
                                        checkPart(info, request.getUserId(), uploadId, bucketInfo, request);
                                        dataPool[0] = StoragePoolFactory.getStoragePool(info);
                                        if (!updateQuotaDir.isEmpty() && info.metaData != null && info.metaData.isAvailable()) {
                                            Inode tmpInode = Inode.defaultInode(info.metaData);
                                            tmpInode.getXAttrMap().put(QUOTA_KEY, Json.encode(updateQuotaDir));
                                            tmpInode.getInodeData().clear();
                                            info.metaData.tmpInodeStr = Json.encode(tmpInode);
                                        }
                                    })
                                    .flatMap(initPartInfo -> {
                                        if (StringUtils.isNotEmpty(migrateVnodeId)) {
                                            return bucketPool.mapToNodeInfo(migrateVnodeId).flatMap(migrateNodeList -> PartClient.abortMultiPartUpload(dataPool[0], migrateNodeList, initPartInfo,
                                                    request, currentSnapshotMark[0]))
                                                    .flatMap(b -> b ? PartClient.abortMultiPartUpload(dataPool[0], bucketNodeList, initPartInfo, request, currentSnapshotMark[0]) : Mono.just(false));
                                        }
                                        return PartClient.abortMultiPartUpload(dataPool[0], bucketNodeList, initPartInfo, request, currentSnapshotMark[0]);
                                    })
                            );
                })
                .subscribe(v -> {
                    try {
                        if (v && !request.response().ended()) {
                            QuotaRecorder.addCheckBucket(bucketName);
                            addPublicHeaders(request, getRequestId()).setStatusCode(204);
                            addAllowHeader(request.response()).end();
                        }
                    } catch (Exception e) {
                        log.error("requestChannel frame error :", e);
                    }
                }, throwable -> dealException(request, throwable));
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));

        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 异步复制的请求，对象已合并状态下仅写数据块
     *
     * @param request
     * @return
     */
    private int uploadPartData(MsHttpRequest request) {

        request.setExpectMultipart(true);
        String requestId = getRequestId();

        String numStr = request.getParam(PART_NUMBER);
        final String uploadId = request.getParam(UPLOAD_ID);
        boolean flag = false;
        if (request.getHeader(TRANSFER_ENCODING) != null && request.getHeader(TRANSFER_ENCODING).contains("chunked")) {//分块传输
            flag = true;
        }

        final boolean isChunked = flag;
        final String contentMd5 = request.getHeader(CONTENT_MD5);
        final String bucketName = request.getBucketName();
        final String objectName = request.getObjectName();

        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        PartInfo partInfo = new PartInfo()
                .setUploadId(uploadId)
                .setBucket(request.getBucketName())
                .setObject(request.getObjectName())
                .setPartNum(numStr)
                .setLastModified(MsDateUtils.stampToISO8601(System.currentTimeMillis()))
                .setPartSize(isChunked ? Long.MAX_VALUE : Long.parseLong(request.headers().get(CONTENT_LENGTH)));

        StoragePool[] dataPool = new StoragePool[]{null};

        AtomicReference<String> crypto = new AtomicReference<>();
        final String versionId = request.getHeader(SYNC_VERSION_ID);

        //控制partUpload中数据修复消息发送的流。
        MonoProcessor<Boolean> recoverDataProcessor = MonoProcessor.create();
        Disposable disposable = bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(bucketName, objectName))
                .flatMap(list -> getObjectMetaVersionUnlimited(bucketName, objectName, versionId, list, request))
                .doOnNext(metaData -> checkMetaData(objectName, metaData, versionId, null))
                .flatMap(metaData -> {
                    String[] fileName = new String[]{null};
                    if (!uploadId.equals(metaData.partUploadId)) {
                        throw new MsException(SYNC_DATA_MODIFIED, "object changed " + objectName);
                    }
                    String storage = "";
                    for (PartInfo info : metaData.partInfos) {
                        if (numStr.equals(info.partNum)) {
                            fileName[0] = info.fileName;
                            storage = info.storage;
                        }
                    }
                    if (StringUtils.isEmpty(fileName[0])) {
                        throw new MsException(SYNC_DATA_MODIFIED, "object changed " + objectName);
                    }

                    dataPool[0] = StoragePoolFactory.getStoragePool(storage, bucketName);
                    partInfo.setStorage(storage);
                    partInfo.setFileName(fileName[0]);
                    partInfo.setVersionId(versionId);
                    crypto.set(metaData.crypto);
                    return dataPool[0].mapToNodeInfo(dataPool[0].getObjectVnodeId(fileName[0]))
                            .flatMap(nodeList -> PartClient.partUpload(dataPool[0], fileName[0], nodeList,
                                    request, partInfo, recoverDataProcessor, crypto.get(), isChunked, true))
                            .doOnNext(md5 -> {
                                if (StringUtils.isBlank(md5)) {
                                    throw new MsException(UNKNOWN_ERROR, "Upload Part fail");
                                }
                                if (contentMd5 != null && !PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
                                    checkMd5(contentMd5, md5);
                                }
                            });
                })
                .subscribe(md5 -> {
                    try {
                        if (!request.response().ended()) {
                            addPublicHeaders(request, requestId)
                                    .putHeader(CONTENT_LENGTH, "0")
                                    .putHeader(ETAG, '"' + md5 + '"');
                            CryptoUtils.addCryptoResponse(request, crypto.get());
                            addAllowHeader(request.response()).end();
                        }
                    } catch (Exception e) {
                        log.error("requestChannel frame error :", e);
                    }

                }, e -> dealException(request, e));

        request.addResponseCloseHandler(v -> disposable.dispose());

        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 校验maxKey
     *
     * @param request 参数值
     * @return maxKey
     */
    private int getMaxKey(MsHttpRequest request) {
        final String maxUploadStr = request.getParam(MAX_KEYS, "1000");
        if (!PatternConst.LIST_PARTS_PATTERN.matcher(maxUploadStr).matches()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "max_keys param error.");
        }
        int maxUpload = Integer.parseInt(maxUploadStr);
        if (maxUpload > 1000 || maxUpload <= 0) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "max_keys param error, must be int in [1, 1000]");
        }
        return maxUpload;
    }
}
