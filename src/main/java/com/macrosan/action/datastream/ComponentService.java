package com.macrosan.action.datastream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.action.core.BaseService;
import com.macrosan.component.ComponentUtils;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.component.pojo.ComponentStrategy;
import com.macrosan.component.utils.ParamsUtils;
import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.ErasureClient;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.authorize.AuthorizeV4;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.ReactorPolicyCheckUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import com.macrosan.snapshot.utils.SnapshotUtil;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.function.Tuple2;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.macrosan.component.ComponentStarter.DICOM_SUPPORT_DESTINATION;
import static com.macrosan.component.ComponentStarter.MAX_IMAGE_SIZE;
import static com.macrosan.component.pojo.ComponentRecord.ERROR_COMPONENT_RECORD;
import static com.macrosan.component.pojo.ComponentRecord.NOT_FOUND_COMPONENT_RECORD;
import static com.macrosan.constants.ErrorNo.TARGET_BUCKET_NOT_EXISTS;
import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.httpserver.ResponseUtils.addAllowHeader;
import static com.macrosan.httpserver.ResponseUtils.addPublicHeaders;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.utils.msutils.MsException.dealException;
import static com.macrosan.utils.msutils.MsException.throwWhenEmpty;

/**
 * @author zhaoyang
 * @date 2023/09/20
 **/

@Log4j2
public class ComponentService extends BaseService {

    private ComponentService() {
        super();
    }

    private static ComponentService instance = null;

    public static ComponentService getInstance() {
        if (instance == null) {
            instance = new ComponentService();
        }
        return instance;
    }


    /**
     * 针对对象级别的图像处理--直接生成record
     *
     * @param request
     * @return
     */
    public int putObjectComponentStrategy(MsHttpRequest request) {
        String bucket = request.getBucketName();
        String object = request.getObjectName();
        String userId = request.getUserId();
        String contentLength = request.getHeader(CONTENT_LENGTH);
        if (StringUtils.isEmpty(contentLength) || Integer.parseInt(contentLength) == 0) {
            throw new MsException(ErrorNo.MISSING_CONTENT_LENGTH, "put object component error, no content-length param");
        }
        if (CheckUtils.bucketFsCheck(bucket)) {
            throw new MsException(ErrorNo.NFS_NOT_STOP, "The bucket already start nfs or cifs, can not add objectComponentStrategy");
        }
        MultiMap paramMap = request.params();
        //  判断源对象是否存在
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket, object);
        final int[] policy = new int[1];
        final String method = StringUtils.isBlank(paramMap.get(VERSIONID)) ? "PutObjectComponentStrategy" : "PutObjectComponentStrategyVersion";
        pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo, new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket name :" + bucket + ".")))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(SnapshotUtil::checkOperationCompatibility)
                .doOnNext(this::siteCheck)
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucket, object, method).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .map(Tuple2::getT2)
                .flatMap(bucketInfo -> storagePool.mapToNodeInfo(bucketVnode).zipWith(Mono.just(bucketInfo)))
                .flatMap(tuple2 -> {
                    String versionId = StringUtils.isNotEmpty(request.getParam(VERSIONID)) ? request.getParam(VERSIONID)
                            : tuple2.getT2().containsKey(BUCKET_VERSION_STATUS) ? "" : "null";
                    return ErasureClient.getObjectMetaVersionUnlimited(bucket, object, versionId, tuple2.getT1(), request)
                            .doOnNext(metaData -> checkMetaData(object, metaData, request.getParam(VERSIONID), null));
                })
                .doOnNext(metaData -> {
                    Map<String, String> objectAclMap = Json.decodeValue(metaData.getObjectAcl(), new TypeReference<Map<String, String>>() {
                    });
                    String objectUserId = objectAclMap.get("owner");
                    if (policy[0] == 0 && !objectUserId.equals(userId)) {
                        throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "No such object component strategy permission");
                    }
                })
                .flatMap(metaData -> {
                    Buffer buffer = Buffer.buffer();
                    MonoProcessor<Boolean> res = MonoProcessor.create();
                    request.handler(buffer::appendBuffer)
                            .endHandler(v -> {
                                try {
                                    int code = AuthorizeV4.checkManageStreamPayloadSHA256(request, buffer.getBytes());
                                    if (code != ErrorNo.SUCCESS_STATUS) {
                                        return;
                                    }
                                    assertRequestBodyNotEmpty(buffer);
                                    com.macrosan.message.xmlmsg.ComponentStrategy strategy = (com.macrosan.message.xmlmsg.ComponentStrategy) JaxbUtils.toObject(new String(buffer.getBytes()));
                                    assertParseXmlError(strategy);
                                    setComponentRecord(res, strategy, metaData, request);
                                } catch (Exception e) {
                                    res.onError(e);
                                }
                            })
                            .exceptionHandler(res::onError)
                            .resume();
                    return res;
                })
                .subscribe(b -> {
                    if (!b) {
                        dealException(request, new MsException(UNKNOWN_ERROR, "set object component strategy error!"));
                        log.error("putComponentRecord error, bucket: {} key: {}", bucket, object);
                    } else {
                        HttpServerResponse httpServerResponse = addPublicHeaders(request, getRequestId())
                                .putHeader(CONTENT_LENGTH, "0");
                        httpServerResponse
                                .setStatusCode(200);
                        addAllowHeader(request.response()).end();
                    }
                }, e -> dealException(request, e));
        return 0;
    }

    private void setComponentRecord(MonoProcessor<Boolean> res, com.macrosan.message.xmlmsg.ComponentStrategy strategy,
                                    MetaData metaData, MsHttpRequest request) {
        String destination = strategy.getDestination();
        String process = strategy.getProcess();
        String processType = strategy.getType();
        if (StringUtils.isNotBlank(strategy.getDeleteSource()) && !ComponentUtils.isBoolean(strategy.getDeleteSource())) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param deleteSource");
        }
        boolean deleteSource = "true".equals(strategy.getDeleteSource());
        if (StringUtils.isNotBlank(strategy.getCopyUserMetaData()) && !ComponentUtils.isBoolean(strategy.getCopyUserMetaData())) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param copyUserMetaData");
        }
        boolean copyUserMetaData = StringUtils.isBlank(strategy.getCopyUserMetaData()) || "true".equals(strategy.getCopyUserMetaData());

        //校验参数是否为空
        if (StringUtils.isAnyBlank(process, processType)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
        }
        // 校验智能处理类型
        ComponentRecord.Type type = ComponentRecord.Type.parseType(processType);
        //参数校验
        String[] processArray = process.split("/");
        //根据处理名称进行参数校验
        for (String processItem : processArray) {
            ParamsUtils.getParams(processType, processItem).checkParams();
        }
        if (ComponentRecord.Type.DICOM.equals(type) && !DICOM_SUPPORT_DESTINATION) {
            // 影像压缩策略，不支持删源和目标位置
            if (StringUtils.isNotEmpty(destination) || deleteSource) {
                throw new MsException(ErrorNo.NOT_SUPPORTED_COMPONENT_PARAM, "not supported component param");
            }
        } else {
            //判断destination是否为/开头或结尾
            if (!StringUtils.isEmpty(destination) && (destination.startsWith("/") || destination.endsWith("/"))) {
                throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
            }
            // 判断和删源是覆盖否冲突
            if (type == ComponentRecord.Type.IMAGE && StringUtils.isBlank(destination) && deleteSource) {
                throw new MsException(ErrorNo.DELETE_SOURCE_CONFLICT, "destination is empty, delete source conflict");
            }
        }

        if (ComponentUtils.hasCompressed(metaData, type)) {
            // 已经处理过，直接返回
            res.onNext(true);
            return;
        }

        // 格式校验
        if (!ComponentUtils.isSupportFormat(metaData.getKey(), type)) {
            throw new MsException(ErrorNo.IMAGE_FORMAT_NOT_SUPPORTED, "image format not supported");
        }
        checkImageSize(metaData, type);
        String bucket = metaData.bucket;
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        Map<String, String> sysMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
        });
        String userId = sysMap.get("owner");
        Map<String, String> map = new HashMap<>();
        long dataSize = metaData.endIndex - metaData.startIndex + 1;
        map.put(CONTENT_LENGTH, String.valueOf(dataSize));
        map.put("userId", userId);
        String targetBucket = !StringUtils.isEmpty(destination) ? destination.split(File.separator)[0] : metaData.bucket;
        if (ServerConfig.isBucketUpper() && StringUtils.isNotEmpty(targetBucket)) {
            String oldTargetBucket = targetBucket;
            targetBucket = targetBucket.toLowerCase();
            if (StringUtils.isNotBlank(destination)) {
                destination = StringUtils.replace(destination, oldTargetBucket, targetBucket, 1);
            }
        }
        ComponentStrategy componentStrategy = new ComponentStrategy("", processType, process, destination, deleteSource, copyUserMetaData);
        componentStrategy.setStrategyMark(RandomStringUtils.randomAlphanumeric(10).toLowerCase());
        ComponentRecord record = ComponentUtils.createComponentRecord(metaData, componentStrategy, COMPONENT_RECORD_INNER_MARKER, map);
        String targetKey = !StringUtils.isEmpty(destination) && destination.contains(File.separator)
                ? destination.split(File.separator, 2)[1] + File.separator + metaData.key : metaData.key;
        String sourceIp = request.remoteAddress().host();
        String finalTargetBucket = targetBucket;
        Disposable subscribe = pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(targetBucket)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo, new MsException(TARGET_BUCKET_NOT_EXISTS, "The target bucket does not exists,targetBucket:" + finalTargetBucket)))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(SnapshotUtil::checkOperationCompatibility)
                .doOnNext(this::siteCheck)
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(userId, "", finalTargetBucket, targetKey + "-", "PutObject", sourceIp).zipWith(Mono.just(bucketInfo)))
                .flatMap(tuple2 -> {
                    if (tuple2.getT1() == 0) {
                        return MsAclUtils.checkWriteAclReactive(tuple2.getT2(), userId, finalTargetBucket);
                    } else {
                        return Mono.empty();
                    }
                })
                .defaultIfEmpty(true)
                .flatMap(b -> pool.getReactive(REDIS_POOL_INDEX).sismember(COMPONENT_RECORD_BUCKET_SET, bucket))
                .doOnNext(b -> {
                    if (!b) {
                        pool.getShortMasterCommand(REDIS_POOL_INDEX).sadd(COMPONENT_RECORD_BUCKET_SET, bucket);
                    }
                })
                .flatMap(b -> ComponentUtils.getComponentRecord(record))
                .doOnNext(resTu -> {
                    ComponentRecord curRecord = resTu.var1;
                    if (ERROR_COMPONENT_RECORD.equals(curRecord)) {
                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get Object component record fail.");
                    }
                    if (!NOT_FOUND_COMPONENT_RECORD.equals(curRecord) && metaData.syncStamp.equals(curRecord.syncStamp)) {
                        throw new MsException(ErrorNo.STRATEGY_ALREADY_EXISTS, "object component record already exist.");
                    }
                })
                .flatMap(resTu -> storagePool.mapToNodeInfo(storagePool.getBucketVnodeId(record.bucket))
                        .flatMap(bucketList -> ComponentUtils.updateComponentRecord(record, bucketList, request, resTu.var2)))
                .flatMap(s -> Mono.just(s == 1))
                .subscribe(res::onNext, res::onError);
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
    }

    private void checkImageSize(MetaData metaData, ComponentRecord.Type type) {
        if (ComponentRecord.Type.IMAGE.equals(type)) {
            long dataSize = metaData.endIndex - metaData.startIndex + 1;
            if (dataSize > MAX_IMAGE_SIZE) {
                log.debug("image too large,image size:{}", dataSize);
                throw new MsException(ErrorNo.IMAGE_SIZE_TOO_LARGE, "image size too large");
            }
        }
    }

    public int putObjectComponentStrategyVersion(MsHttpRequest request) {
        return putObjectComponentStrategy(request);
    }

    public int getObjectComponentStrategyVersion(MsHttpRequest request) {
        return getObjectComponentStrategy(request);
    }

    public int getObjectComponentStrategy(MsHttpRequest request) {
        String userId = request.getUserId();
        String bucketName = request.getBucketName();
        String objectName = request.getObjectName();
        String processType = request.getParam("processType");
        if (StringUtils.isBlank(processType)) {
            // 兼容旧版本，默认值为video-process
            processType = ComponentRecord.Type.VIDEO.name;
        }
        ComponentRecord.Type.parseType(processType);
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(bucketName, objectName);
        String method = request.getParam(VERSIONID) == null ? "getObjectComponentStrategy" : "getObjectComponentStrategyVersion";
        final int[] policy = new int[1];
        MetaData[] metaDataArray = new MetaData[1];
        String finalProcessType = processType;
        Disposable subscribe = pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo,
                        new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket name :" + bucketName + ".")))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .doOnNext(SnapshotUtil::checkOperationCompatibility)
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
                        throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "No such object component strategy permission");
                    }
                })
                .map(metaData -> {
                    ComponentStrategy componentStrategy = new ComponentStrategy("", finalProcessType, "", "", false, false);
                    metaDataArray[0] = metaData;
                    return ComponentUtils.createComponentRecord(metaData, componentStrategy, COMPONENT_RECORD_INNER_MARKER, null);
                })
                .flatMap(ComponentUtils::getComponentRecord)
                .flatMap(resTu -> {
                    ComponentRecord curRecord = resTu.var1;
                    if (ERROR_COMPONENT_RECORD.equals(curRecord)) {
                        throw new MsException(UNKNOWN_ERROR, "Get Object component record fail.");
                    }
                    if (NOT_FOUND_COMPONENT_RECORD.equals(curRecord)) {
                        throw new MsException(ErrorNo.STRATEGY_NOT_EXISTS, "object component record doesn't exist.");
                    }
                    // 同名对象覆盖
                    if (!curRecord.syncStamp.equals(metaDataArray[0].syncStamp)) {
                        throw new MsException(ErrorNo.STRATEGY_NOT_EXISTS, "object component record doesn't exist.");
                    }
                    com.macrosan.message.xmlmsg.ComponentStrategy strategy = new com.macrosan.message.xmlmsg.ComponentStrategy();
                    strategy.setType(curRecord.strategy.type.name)
                            .setProcess(curRecord.strategy.process)
                            .setDestination(curRecord.strategy.destination)
                            .setDeleteSource(String.valueOf(curRecord.strategy.deleteSource))
                            .setCopyUserMetaData(String.valueOf(curRecord.strategy.copyUserMetaData));
                    return Mono.just(JaxbUtils.toByteArray(strategy));
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

}
