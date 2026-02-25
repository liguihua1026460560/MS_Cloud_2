package com.macrosan.action.datastream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.EsMeta;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.xmlmsg.tagging.Tagging;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.authorize.AuthorizeV4;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.ReactorPolicyCheckUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.function.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.ServerConstants.CONTENT_LENGTH;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.constants.SysConstants.BUCKET_VERSION_STATUS;
import static com.macrosan.httpserver.ResponseUtils.addAllowHeader;
import static com.macrosan.httpserver.ResponseUtils.addPublicHeaders;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.utils.msutils.MsException.dealException;
import static com.macrosan.utils.msutils.MsException.throwWhenEmpty;

public class TaggingService extends BaseService {
    private static final Logger logger = LogManager.getLogger(TaggingService.class.getName());

    private TaggingService() {
        super();
    }

    private static TaggingService instance = null;

    public static TaggingService getInstance() {
        if (instance == null) {
            instance = new TaggingService();
        }
        return instance;
    }
    public int putObjectTaggingVersion(MsHttpRequest request) {
        return putObjectTagging(request);
    }
    public int putObjectTagging(MsHttpRequest request) {
        String userId = request.getUserId();
        String bucketName = request.getBucketName();
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String objName = request.getObjectName();
        com.macrosan.utils.functional.Tuple2<String, String> bucketVnodeIdTuple = storagePool.getBucketVnodeIdTuple(bucketName, objName);
        String bucketVnode = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        final String[] bucketUserId = new String[1];
        final String[] versionId = new String[1];
        final String[] mda = new String[1];
        final int[] policy = new int[1];
        boolean[] isInnerObject = new boolean[]{request.headers().contains(CLUSTER_ALIVE_HEADER)};
        String contentLength = request.getHeader(CONTENT_LENGTH);
        boolean isDelRequest = request.method().equals(HttpMethod.DELETE);
        AtomicBoolean isEmptyUserMetaData = new AtomicBoolean(false);
        if ((StringUtils.isEmpty(contentLength) || Integer.parseInt(contentLength) == 0) && !request.headers().contains(USER_METADATA_HEADER) ) {
            if (!isDelRequest){
                throw new MsException(ErrorNo.MISSING_CONTENT_LENGTH, "put object tagging error, no content-length param");
            }
        }
        final String method;
        if (isDelRequest){
            method = StringUtils.isBlank(request.params().get(VERSIONID)) ? "DeleteObjectTagging" : "DeleteObjectTaggingVersion";
        }else {
            method = StringUtils.isBlank(request.params().get(VERSIONID)) ? "PutObjectTagging" : "PutObjectTaggingVersion";
        }

        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};

        Disposable subscribe = pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucketName)
                .doOnNext(bucketInfo -> {
                    if (bucketInfo.isEmpty()) {
                        throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket name :" + bucketName + ".");
                    }
                    regionCheck(bucketInfo.get(REGION_NAME));
                    siteCheck(bucketInfo);
                    bucketUserId[0] = bucketInfo.get(BUCKET_USER_ID);
                    versionId[0] = StringUtils.isNotEmpty(request.getParam(VERSIONID)) ? request.getParam(VERSIONID)
                            : bucketInfo.containsKey(BUCKET_VERSION_STATUS) ? "" : "null";
                    versionId[0] = StringUtils.isBlank(request.getHeader(VERSIONID)) ? versionId[0] : request.getHeader(VERSIONID);
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                    mda[0] = bucketInfo.get("mda");
                })
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, objName, method))
                .doOnNext(access -> policy[0] = access)
                .flatMap(access -> storagePool.mapToNodeInfo(bucketVnode))
                .flatMap(list -> ErasureClient.getObjectMetaVersionResUnlimited(bucketName, objName, versionId[0], list, request, currentSnapshotMark[0], snapshotLink[0]).zipWith(Mono.just(list)))
                .doOnNext(tuple2 -> checkMetaData(objName, tuple2.getT1().var1, request.getParam(VERSIONID), currentSnapshotMark[0]))
                .flatMap(tuple2 -> {
                    MetaData metaData = tuple2.getT1().var1;
                    String syncStamp = request.getHeader(SYNC_STAMP);
                    if (StringUtils.isNotBlank(syncStamp) && !syncStamp.equals(metaData.syncStamp)) {
                        logger.info("syncStamp is different,curr is {} sync is {}", metaData, syncStamp);
                        throw new MsException(ErrorNo.SYNC_DATA_MODIFIED, "");
                    }
                    isEmptyUserMetaData.set(checkUserMetaData(metaData));
                    Map<String, String> objectAclMap = Json.decodeValue(metaData.getObjectAcl(), new TypeReference<Map<String, String>>() {});
                    String objectUserId = objectAclMap.get("owner");
                    if (policy[0] == 0 && !objectUserId.equals(userId) && !PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
                        throw new MsException(ACCESS_FORBIDDEN, "No put object tag permission");
                    }

                    Map<String, String> sysMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
                    });
                    isInnerObject[0] = isInnerObject[0]
                            && sysMap.containsKey(NO_SYNCHRONIZATION_KEY)
                            && sysMap.get(NO_SYNCHRONIZATION_KEY).equals(NO_SYNCHRONIZATION_VALUE);

                    responseContinue(request);
                    Buffer buffer = Buffer.buffer();
                    MonoProcessor<Boolean> res = MonoProcessor.create();
                    request.handler(buffer::appendBuffer)
                            .endHandler(v -> {
                                try {
                                    if (isDelRequest){
                                        if (isEmptyUserMetaData.get()){
                                            res.onNext(true);
                                            return;
                                        }
                                        metaData.setUserMetaData("{}");
                                        setObjectTag(res, metaData, objectUserId, bucketUserId[0], bucketVnode, migrateVnode, tuple2.getT2(), request, tuple2.getT1().var2, currentSnapshotMark[0], snapshotLink[0], mda[0]);
                                    }else {
                                        int code = AuthorizeV4.checkManageStreamPayloadSHA256(request, buffer.getBytes());
                                        if (code != ErrorNo.SUCCESS_STATUS) {
                                            return;
                                        }
                                        if (request.headers().contains(USER_METADATA_HEADER)){
                                            metaData.setUserMetaData(request.headers().get(USER_METADATA_HEADER));
                                            setObjectTag(res, metaData, objectUserId, bucketUserId[0], bucketVnode, migrateVnode, tuple2.getT2(), request, tuple2.getT1().var2, currentSnapshotMark[0], snapshotLink[0], mda[0]);
                                        }else {
                                            assertRequestBodyNotEmpty(buffer);
                                            Tagging tagging = null;
                                            tagging = (Tagging) JaxbUtils.toObject(new String(buffer.getBytes()));
                                            if (tagging == null){
                                                throw new MsException(MALFORMED_XML, "parse tagging error!");
                                            }
                                            checkTagging(tagging);
                                            setUserMetaData(metaData, tagging);
                                            setObjectTag(res, metaData, objectUserId, bucketUserId[0], bucketVnode, migrateVnode, tuple2.getT2(), request, tuple2.getT1().var2, currentSnapshotMark[0], snapshotLink[0], mda[0]);
                                        }
                                    }
                                } catch (Exception e) {
                                    MsException.dealException(request, e);
                                }
                            })
                            .exceptionHandler(logger::error)
                            .resume();
                    return res;
                })
                .subscribe(b -> {
                    if (!b) {
                        dealException(request, new MsException(UNKNOWN_ERROR, "set object tag error!"));
                    } else {
                        //response返回
                        HttpServerResponse httpServerResponse = addPublicHeaders(request, getRequestId())
                                .putHeader(CONTENT_LENGTH, "0");

                        if (isInnerObject[0]) {
                            httpServerResponse.putHeader(IFF_TAG, "1");
                        }
                        if (isDelRequest){
                            request.response().setStatusCode(204);
                        }else {
                            httpServerResponse.setStatusCode(200);
                        }
                        addAllowHeader(request.response()).end();
                    }
                }, e -> {
                    if (e instanceof MsException && ErrorNo.SYNC_DATA_MODIFIED == ((MsException) e).getErrCode()) {
                        //数据同步已修改，返回成功
                        request.response()
                                .putHeader(CONTENT_LENGTH, "0")
                                .setStatusCode(200)
                                .end();
                    } else {
                        dealException(request, e);
                    }
                });

        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));

        return ErrorNo.SUCCESS_STATUS;
    }

    public void setObjectTag(MonoProcessor<Boolean> res, MetaData metaData, String objectUserId, String bucketUserId,
                                  String bucketVnode, String migrateVnode,
                                  List<Tuple3<String, String, String>> nodeList, MsHttpRequest request,
                                  com.macrosan.utils.functional.Tuple2<ErasureServer.PayloadMetaType, MetaData>[] metaRes, String currentSnapshotMark, String snapshotLink, String mda) {
        Disposable subscribe = Mono.just(metaData)
                .flatMap(meta -> setObjectTag(meta, bucketVnode, migrateVnode, nodeList, bucketUserId, request, metaRes, currentSnapshotMark, snapshotLink))
                .flatMap(b ->{
                    if (b && "on".equals(mda)){
                        EsMeta esMeta = new EsMeta()
                                .setUserId(request.getUserId())
                                .setSysMetaData(metaData.sysMetaData)
                                .setUserMetaData(metaData.userMetaData)
                                .setBucketName(metaData.bucket)
                                .setObjName(metaData.key)
                                .setVersionId(metaData.versionId)
                                .setStamp(metaData.getStamp())
                                .setObjSize(String.valueOf(metaData.endIndex + 1))
                                .setInode(metaData.inode);
                        return EsMetaTask.putEsMeta(esMeta, false);
                    }
                    return Mono.just(b);
                })
                .subscribe(res::onNext, res::onError);
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
    }

    /**
     * EC组各节点rocksdb设置元数据
     *
     * @param metaData    元数据
     * @param bucketVnode 桶对应vnode
     * @param request
     */
    public Mono<Boolean> setObjectTag(MetaData metaData, String bucketVnode, String migrateVnode, List<Tuple3<String, String, String>> nodeList,
                                      String bucketUserId, MsHttpRequest request, com.macrosan.utils.functional.Tuple2<ErasureServer.PayloadMetaType, MetaData>[] metaRes, String currentSnapshotMark, String snapshotLink) {
        String userId = request.getUserId();
        String bucketName = request.getBucketName();
        String objectName = request.getObjectName();
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);

        return Mono.just(StringUtils.isNotEmpty(migrateVnode))
                .flatMap(b -> {
                    if (b) {
                        return storagePool.mapToNodeInfo(migrateVnode)
                                .flatMap(migrateVnodeList -> ErasureClient.getObjectMetaVersionResOnlyRead(bucketName, objectName, metaData.versionId, migrateVnodeList, request, currentSnapshotMark, snapshotLink).zipWith(Mono.just(migrateVnodeList)))
                                .flatMap(tuple2 -> ErasureClient.updateMetaDataAcl(Utils.getVersionMetaDataKey(migrateVnode, bucketName, objectName, metaData.versionId, metaData.snapshotMark),
                                        metaData.clone(), tuple2.getT2(), request, tuple2.getT1().var2))
                                .map(r -> r == 1);
                    }
                    return Mono.just(true);
                })
                .doOnNext(b -> {
                    if (!b) {
                        throw new MsException(UNKNOWN_ERROR, "update bucket "+ bucketName +" object " + objectName + " acl to new vnode error");
                    }
                })
                .flatMap(b -> storagePool.mapToNodeInfo(bucketVnode))
                .flatMap(bucketNodeList -> ErasureClient.updateMetaDataAcl(Utils.getVersionMetaDataKey(bucketVnode, bucketName, objectName, metaData.versionId, metaData.snapshotMark),
                        metaData, bucketNodeList, request, metaRes))
                .flatMap(b -> {
                    if (b != 2) {
                        return Mono.just(b == 1);
                    }
                    //元数据覆盖，重试一次！
                    return ErasureClient.getObjectMetaVersionResUnlimited(bucketName, objectName, metaData.versionId, nodeList, request, currentSnapshotMark, snapshotLink)
                            .doOnNext(tuple2 -> {
                                checkMetaData(objectName, tuple2.var1, request.getParam(VERSIONID), currentSnapshotMark);
                                Map<String, String> objectAclMap = Json.decodeValue(tuple2.var1.getObjectAcl(), new TypeReference<Map<String, String>>() {
                                });
                                if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
                                    MsAclUtils.checkObjAcp(objectAclMap, userId, bucketUserId, OBJECT_PERMISSION_WRITE_CAP_NUM);
                                }
                            })
                            .flatMap(tuple2 -> {
                                MetaData metaData1 = tuple2.var1;
                                metaData1.setUserMetaData(metaData.getUserMetaData());
                                return storagePool.mapToNodeInfo(bucketVnode).flatMap(bucketNodeList ->
                                        ErasureClient.updateMetaDataAcl(Utils.getVersionMetaDataKey(bucketVnode, bucketName, objectName, metaData.versionId, metaData.snapshotMark),
                                                metaData1, bucketNodeList, request, tuple2.var2));
                            })
                            .flatMap(s -> Mono.just(s == 1));
                });
    }

    public int  getObjectTagging(MsHttpRequest request){
        String userId = request.getUserId();
        String bucketName = request.getBucketName();
        String objectName = request.getObjectName();

        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(bucketName, objectName);
        String method = StringUtils.isBlank(request.params().get(VERSIONID)) ? "GetObjectTagging" : "GetObjectTaggingVersion";
        final int[] policy = new int[1];
        Disposable subscribe = pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo,
                        new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket name :" + bucketName + ".")))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, objectName, method).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .map(Tuple2::getT2)
                .flatMap(bucketInfo -> storagePool.mapToNodeInfo(bucketVnode).zipWith(Mono.just(bucketInfo)))
                .flatMap(tuple2 -> {
                    String versionId = StringUtils.isNotEmpty(request.getParam(VERSIONID)) ? request.getParam(VERSIONID)
                            : tuple2.getT2().containsKey(BUCKET_VERSION_STATUS) ? "" : "null";
                    return ErasureClient.getObjectMetaVersionUnlimited(bucketName, objectName, versionId, tuple2.getT1(), request)
                            .doOnNext(metaData -> checkMetaData(objectName, metaData, request.getParam(VERSIONID), null));
                })
                .doOnNext(metaData -> {
                    Map<String, String> objectAclMap = Json.decodeValue(metaData.getObjectAcl(), new TypeReference<Map<String, String>>() {
                    });
                    String objectUserId = objectAclMap.get("owner");
                    if (policy[0] == 0 && !objectUserId.equals(userId)) {
                        throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "No get object tag permission");
                    }
                })
                .flatMap(mataData -> Mono.just(JaxbUtils.toByteArray(getTagging(mataData))))
                .subscribe(bytes -> {
                    addPublicHeaders(request, getRequestId())
                            .putHeader(CONTENT_TYPE, "application/xml")
                            .putHeader(CONTENT_LENGTH, String.valueOf(bytes.length))
                            .write(Buffer.buffer(bytes));
                    addAllowHeader(request.response()).end();
                }, e -> dealException(request, e));
        Optional.ofNullable(request).ifPresent(req -> {
            req.addResponseCloseHandler(v -> subscribe.dispose());
        });
        return ErrorNo.SUCCESS_STATUS;
    }


    public int getObjectTaggingVersion(MsHttpRequest request){
        return getObjectTagging(request);
    }


    public int deleteObjectTagging(MsHttpRequest request){
        return putObjectTagging(request);
    }

    public int deleteObjectTaggingVersion(MsHttpRequest request){
        return putObjectTagging(request);
    }
}
