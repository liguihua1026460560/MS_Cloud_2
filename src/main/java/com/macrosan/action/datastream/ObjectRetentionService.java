package com.macrosan.action.datastream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.xmlmsg.worm.Retention;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.authorize.AuthorizeV4;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.ReactorPolicyCheckUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import com.macrosan.utils.worm.WormUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.macrosan.action.managestream.ObjectLockService.VALID_RETENTION_MODE;
import static com.macrosan.constants.ErrorNo.INVALID_ARGUMENT;
import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.httpserver.ResponseUtils.addAllowHeader;
import static com.macrosan.httpserver.ResponseUtils.addPublicHeaders;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.utils.msutils.MsException.dealException;
import static com.macrosan.utils.msutils.MsException.throwWhenEmpty;
import static com.macrosan.utils.worm.WormUtils.*;

/**
 * <p></p>
 *
 * @author admin
 */
public class ObjectRetentionService extends BaseService {
    public static final Logger logger = LoggerFactory.getLogger(ObjectRetentionService.class);
    public final static String PATTERN = "(?:(?!0000)[0-9]{4}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)-02-29)T([01][0-9]|[2][0-3]):[0-5][0-9]:[0-5][0-9]\\.[0-9]{3}Z";
    public final static Pattern pattern = Pattern.compile(PATTERN);
    public final static String WORM_KEY = "objectRetention";
    private static ObjectRetentionService instance = null;

    private ObjectRetentionService() {
        super();
    }

    public static ObjectRetentionService getInstance() {
        if (instance == null) {
            instance = new ObjectRetentionService();
        }
        return instance;
    }

    /**
     * 根据版本号给指定的对象添加对象保护功能，在一定的时间段内，对象不可被删除或者覆盖
     *
     * @param request
     * @return
     */
    public int putObjectVersionRetention(MsHttpRequest request) {
        return putObjcetRetention(request);
    }

    /**
     * 给对象添加对象保护功能，只有对象的拥有者或者拥有moss:PutObjectWorm权限的用户可以操作
     *
     * @param request
     * @return
     */
    public int putObjcetRetention(MsHttpRequest request) {
        String userId = request.getUserId();
        String bucketName = request.getBucketName();
        String objectName = request.getObjectName();
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        Tuple2<String, String> bucketVnodeIdTuple = storagePool.getBucketVnodeIdTuple(bucketName, objectName);
        String bucketVnodeId = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        final String[] versionId = new String[1];
        final String[] bucketUserId = new String[1];
        final Long[] currentWormTime = new Long[1];
        String contentLength = request.getHeader(CONTENT_LENGTH);
        String method = request.getParam(VERSIONID) == null ? "PutObjectRetention" : "PutObjectVersionRetention";
        if (StringUtils.isEmpty(contentLength) || Integer.parseInt(contentLength) == 0) {
            throw new MsException(ErrorNo.MISSING_CONTENT_LENGTH, "put object retention error, no content-length param");
        }
        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};

        boolean[] isInnerObject = new boolean[]{request.headers().contains(CLUSTER_ALIVE_HEADER)};

        Disposable subscribe = WormUtils.currentWormTimeMillis()
                .doOnNext(wormTime -> currentWormTime[0] = wormTime)
                .flatMap(wormTime -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName))
                .doOnNext(bucketInfo -> {
                    if (bucketInfo.isEmpty()) {
                        throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket name :" + bucketName + ".");
                    }

                    // 桶未开启worm保护，不允许设置对象的worm
                    if (StringUtils.isEmpty(bucketInfo.get("lock_periodic")) || "Disabled".equals(bucketInfo.get("lock_status"))) {
                        throw new MsException(ErrorNo.BUCKET_MISSING_OBJECT_LOCK_CONFIG, "Bucket is missing Object Lock Configuration.");
                    }

                    // 检测桶是否开启桶回收站
                    if (StringUtils.isNotEmpty(bucketInfo.get("trashDir"))) {
                        throw new MsException(ErrorNo.BUCKET_OPEN_VERSION, "the open error");
                    }
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                    regionCheck(bucketInfo.get(REGION_NAME));
                    siteCheck(bucketInfo);
                    bucketUserId[0] = bucketInfo.get(BUCKET_USER_ID);
                    versionId[0] = StringUtils.isNotEmpty(request.getParam(VERSIONID)) ? request.getParam(VERSIONID)
                            : bucketInfo.containsKey(BUCKET_VERSION_STATUS) ? "" : "null";
                    versionId[0] = StringUtils.isBlank(request.getHeader(VERSIONID)) ? versionId[0] : request.getHeader(VERSIONID);
                })
                .flatMap(bucketInfo -> storagePool.mapToNodeInfo(bucketVnodeId))
                .flatMap(list -> ErasureClient.getObjectMetaVersionResUnlimited(bucketName, objectName, versionId[0], list, request, currentSnapshotMark[0], snapshotLink[0]).zipWith(Mono.just(list)))
                .doOnNext(tuple2 -> checkMetaData(objectName, tuple2.getT1().var1, request.getParam(VERSIONID), currentSnapshotMark[0]))
                .flatMap(tuple2 -> {
                    MetaData metaData = tuple2.getT1().var1;
                    String syncStamp = request.getHeader(SYNC_STAMP);
                    if (StringUtils.isNotBlank(syncStamp) && !syncStamp.equals(metaData.syncStamp)) {
                        logger.info("syncStamp is different,curr is {} sync is {}", metaData.syncStamp, syncStamp);
                        throw new MsException(ErrorNo.SYNC_DATA_MODIFIED, "syncStamp is different.");
                    }

                    Map<String, String> sysMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
                    });

                    isInnerObject[0] = isInnerObject[0]
                            && sysMap.containsKey(NO_SYNCHRONIZATION_KEY)
                            && sysMap.get(NO_SYNCHRONIZATION_KEY).equals(NO_SYNCHRONIZATION_VALUE);

                    // 检测worm设置的权限
                    Map<String, String> objectAclMap = Json.decodeValue(metaData.getObjectAcl(), new TypeReference<Map<String, String>>() {
                    });

                    String objectUserId = objectAclMap.get("owner");
                    if (!objectUserId.equals(userId)) {
                        throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "No such object worm permission");
                    }

                    responseContinue(request);
                    Buffer buffer = Buffer.buffer();
                    MonoProcessor<Boolean> res = MonoProcessor.create();
                    request.handler(buffer::appendBuffer)
                            .endHandler(v -> {
                                try {
                                    int code = AuthorizeV4.checkManageStreamPayloadSHA256(request, buffer.getBytes());
                                    if (code != ErrorNo.SUCCESS_STATUS) {
                                        return;
                                    }
                                    Retention retention = (Retention) JaxbUtils.toObject(new String(buffer.getBytes()));
                                    if (!isFromSyncing(request)) {
                                        checkRetention(retention, metaData, currentWormTime[0]);
                                    }
                                    setObjectRetention(metaData, bucketName, objectName, method, bucketVnodeId, migrateVnode, tuple2.getT2(), retention, bucketUserId[0], request, res, tuple2.getT1().var2, currentSnapshotMark[0], snapshotLink[0]);
                                } catch (Exception e) {
                                    logger.error("", e);
                                    dealException(request, e);
                                }
                            })
                            .exceptionHandler(e -> logger.error("", e))
                            .resume();
                    return res;
                })
                .subscribe(b -> {
                    if (!b) {
                        dealException(request, new MsException(UNKNOWN_ERROR, "set object retention error!"));
                    } else {
                        HttpServerResponse httpServerResponse = addPublicHeaders(request, getRequestId())
                                .putHeader(CONTENT_LENGTH, "0");

                        if (isInnerObject[0]) {
                            httpServerResponse.putHeader(IFF_TAG, "1");
                        }

                        httpServerResponse
                                .setStatusCode(200);
                        addAllowHeader(request.response()).end();
                    }
                }, e -> {
                    logger.error("", e);
                    if (e instanceof MsException && ErrorNo.SYNC_DATA_MODIFIED == ((MsException) e).getErrCode()) {
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

    /**
     * 根据版本号获取指定对象的对象保护设置,只有对象的所有者可以获取对象的Worm配置
     *
     * @param request
     * @return
     */
    public int getObjectVersionRetention(MsHttpRequest request) {
        return getObjectRetention(request);
    }

    public int getObjectRetention(MsHttpRequest request) {
        String userId = request.getUserId();
        String bucketName = request.getBucketName();
        String objectName = request.getObjectName();
        StoragePool storagePool = StoragePoolFactory.getStoragePool(StorageOperate.META, bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(bucketName, objectName);
        String method = request.getParam(VERSIONID) == null ? "GetObjectRetention" : "GetObjectVersionRetention";
        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        Disposable subscribe = pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucketName)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo,
                        new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket name :" + bucketName + ".")))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, objectName, method).zipWith(Mono.just(bucketInfo)))
                .map(reactor.util.function.Tuple2::getT2)
                .doOnNext(bucketInfo -> {
                    // 桶未开启worm保护，不允许设置对象的worm
                    if (StringUtils.isEmpty(bucketInfo.get("lock_periodic")) || "Disabled".equals(bucketInfo.get("lock_status"))) {
                        throw new MsException(ErrorNo.BUCKET_MISSING_OBJECT_LOCK_CONFIG, "Bucket is missing Object Lock Configuration.");
                    }
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                })
                .flatMap(bucketInfo -> storagePool.mapToNodeInfo(bucketVnode).zipWith(Mono.just(bucketInfo)))

                .flatMap(tuple2 -> {
                    String versionId = StringUtils.isNotEmpty(request.getParam(VERSIONID)) ? request.getParam(VERSIONID)
                            : tuple2.getT2().containsKey(BUCKET_VERSION_STATUS) ? "" : "null";
                    return ErasureClient.getObjectMetaVersionResUnlimited(bucketName, objectName, versionId, tuple2.getT1(), request, currentSnapshotMark[0], snapshotLink[0])
                            .doOnNext(tuple -> checkMetaData(objectName, tuple.var1, request.getParam(VERSIONID), currentSnapshotMark[0]))
                            .zipWith(Mono.just(tuple2.getT2()));
                })
                .doOnNext(tuple2 -> {
                    MetaData metaData = tuple2.getT1().var1;
                    Map<String, String> objectAclMap = Json.decodeValue(metaData.getObjectAcl(), new TypeReference<Map<String, String>>() {
                    });
                    String objectUserId = objectAclMap.get("owner");
                    if (!objectUserId.equals(userId)) {
                        throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "No such object worm permission");
                    }
                })
                .flatMap(tuple2 -> {
                    MetaData metaData = tuple2.getT1().var1;
                    String sysMetaData = metaData.getSysMetaData();
                    Map<String, String> sysMap = Json.decodeValue(sysMetaData, new TypeReference<Map<String, String>>() {
                    });
                    String value = sysMap.get(EXPIRATION);
                    String mode = sysMap.getOrDefault("mode", "COMPLIANCE");
                    if (StringUtils.isEmpty(value)) {
                        throw new MsException(ErrorNo.NO_SUCH_OBJECT_RETENTION, "Nosuch object lock configuration, bucket:" + bucketName + " object:" + objectName);
                    }
                    String date = MsDateUtils.stampToISO8601(value);
                    Retention retention = new Retention();
                    retention.setMode(mode);
                    retention.setRetainUntilDate(date);
                    return Mono.just(JaxbUtils.toByteArray(retention));
                })
                .subscribe(data -> {
                    addPublicHeaders(request, getRequestId())
                            .putHeader(CONTENT_TYPE, "application/xml")
                            .putHeader(CONTENT_LENGTH, String.valueOf(data.length))
                            .write(Buffer.buffer(data));
                    addAllowHeader(request.response()).end();
                }, e -> dealException(request, e));
        Optional.ofNullable(request).ifPresent(req -> {
            req.addResponseCloseHandler(v -> subscribe.dispose());
        });
        return ErrorNo.SUCCESS_STATUS;
    }

    private void setObjectRetention(MetaData metaData, String bucketName, String objectName, String method, String bucketVnodeId, String migrateVnode
            , List<Tuple3<String, String, String>> nodeList
            , Retention retention, String objectUserId
            , MsHttpRequest request, MonoProcessor<Boolean> res, Tuple2<ErasureServer.PayloadMetaType, MetaData>[] metaRes
            , String currentSnapshotMark, String snapshotLink) {

        Map<String, String> systemMeta = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
        });

        String retainUntilDate = retention.getRetainUntilDate();
        String wormExpire = MsDateUtils.tzToStamp(retainUntilDate);
        systemMeta.put(EXPIRATION, wormExpire);
        metaData.sysMetaData = Json.encode(systemMeta);

        Disposable subscribe = ReactorPolicyCheckUtils.getPolicyWormCheckResult(request, bucketName, objectName, method, retention)
                .flatMap(b -> setObjectRetention(metaData, bucketVnodeId, migrateVnode, nodeList, request, metaRes, currentSnapshotMark, snapshotLink))
                .subscribe(res::onNext, res::onError);

        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
    }


    private Mono<Boolean> setObjectRetention(MetaData metaData, String bucketVnodeId, String migrateVnode
            , List<Tuple3<String, String, String>> nodeList
            , MsHttpRequest request, Tuple2<ErasureServer.PayloadMetaType, MetaData>[] metaRes
            , String currentSnapshotMark, String snapshotLink) {

        String userId = request.getUserId();
        String bucketName = request.getBucketName();
        String objectName = request.getObjectName();
        StoragePool storagePool = StoragePoolFactory.getStoragePool(StorageOperate.META, bucketName);

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
                        throw new MsException(UNKNOWN_ERROR, "set bucket "+ bucketName +" object " + objectName + " worm to new vnode error");
                    }
                })
                .flatMap(b -> storagePool.mapToNodeInfo(bucketVnodeId))
                .flatMap(bucketNodeList -> ErasureClient.updateMetaDataAcl(Utils.getVersionMetaDataKey(bucketVnodeId, bucketName, objectName, metaData.versionId, metaData.snapshotMark),
                        metaData, bucketNodeList, request, metaRes))
                .flatMap(b -> {
                    if (b != 2) {
                        return Mono.just(b == 1);
                    }

                    return ErasureClient.getObjectMetaVersionResUnlimited(bucketName, objectName, metaData.versionId, nodeList, request, currentSnapshotMark, snapshotLink)
                            .doOnNext(tuple -> {
                                checkMetaData(objectName, tuple.var1, request.getParam(VERSIONID), currentSnapshotMark);
                                Map<String, String> objectAclMap = Json.decodeValue(tuple.var1.getObjectAcl(), new TypeReference<Map<String, String>>() {
                                });
                                String objectUserId = objectAclMap.get("owner");
                                if (!objectUserId.equals(userId)) {
                                    throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "No such object worm permission");
                                }
                            })
                            .flatMap(tuple2 -> storagePool.mapToNodeInfo(bucketVnodeId).flatMap(bucketNodeList ->
                                    ErasureClient.updateMetaDataAcl(Utils.getVersionMetaDataKey(bucketVnodeId, bucketName, objectName, metaData.versionId, metaData.snapshotMark),
                                            metaData, bucketNodeList, request, tuple2.var2))
                            )
                            .flatMap(s -> Mono.just(s == 1));
                });
    }

    private void checkRetention(Retention retention, MetaData metaData, long currentWormTime) {
        String retainUtilDate = retention.getRetainUntilDate();
        if (StringUtils.isEmpty(retainUtilDate) || !pattern.matcher(retainUtilDate).matches()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "Invalid retain util date format!");
        }

        String mode = retention.getMode();
        if (StringUtils.isNotEmpty(mode) && !mode.equals(VALID_RETENTION_MODE)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "Invalid protect mode!");
        }

        long current = Long.parseLong(MsDateUtils.tzToStamp(retainUtilDate));
        Map<String, String> systemMeta = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
        });

        String expiration = systemMeta.get(EXPIRATION);

        if (!StringUtils.isEmpty(expiration)) {
            long prev = Long.parseLong(expiration);
            if (current <= prev) {
                throw new MsException(ErrorNo.OBJECT_RETAIN_UNTIL_DATE_ERROR, "Invalid retain util date!, current must grater than prev.");
            }
        }

        if (current <= currentWormTime) {
            throw new MsException(ErrorNo.OBJECT_RETAIN_UNTIL_DATE_ERROR, "Invalid retain util date! current must grater than curren worm time");
        }

        long limit = currentWormTime + ONE_YEAR * 100 + 30 * ONE_DAY;
        if (current > limit) {
            throw new MsException(INVALID_ARGUMENT, "Invalid retain util date! current must less than limit of 100 years");
        }

    }

}
