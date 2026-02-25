package com.macrosan.action.datastream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.ErasureClient;
import com.macrosan.filesystem.quota.FSQuotaRealService;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.xmlmsg.FsCapInfoResult;
import com.macrosan.message.xmlmsg.FsUsedInfo;
import com.macrosan.message.xmlmsg.FsUsedObjectResult;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.ReactorPolicyCheckUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.Optional;

import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.filesystem.quota.FSQuotaConstants.DIR_NAME;
import static com.macrosan.filesystem.quota.FSQuotaConstants.QUOTA_TYPE;
import static com.macrosan.filesystem.utils.FSQuotaUtils.checkDirInfo;
import static com.macrosan.filesystem.utils.FSQuotaUtils.checkQuotaTypeAndId;
import static com.macrosan.httpserver.ResponseUtils.addAllowHeader;
import static com.macrosan.httpserver.ResponseUtils.addPublicHeaders;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.utils.msutils.MsException.throwWhenEmpty;

public class FsQuotaService extends BaseService {

    private static final Logger log = LoggerFactory.getLogger(FsQuotaService.class);
    private static FsQuotaService instance;

    public static FsQuotaService getInstance() {
        if (instance == null) {
            instance = new FsQuotaService();
        }
        return instance;
    }

    public int getFsQuotaUsedInfo(MsHttpRequest request) {
        String bucketName = request.getBucketName();
        String userId = request.getUserId();
        String[] dirName = new String[]{request.getHeader(DIR_NAME)};
        if (StringUtils.isBlank(dirName[0]) || SLASH.equals(dirName[0])) {
            throw new MsException(INVALID_ARGUMENT, "the dirName input is invalid.");
        }
        if (!dirName[0].endsWith(SLASH)) {
            dirName[0] += SLASH;
        }
        int[] id_ = new int[]{-1};
        int[] quotaType = new int[]{-1};
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(bucketName, dirName[0]);
        checkQuotaTypeAndId(request.getHeader(QUOTA_TYPE), request.getHeader("id_"), quotaType, id_);
        //0 表示获取使用容量，1表示获取使用对象,2表示获取使用对象数量+使用容量
        int usedType = Integer.parseInt(Optional.ofNullable(request.getHeader("used_type")).orElse("2"));
        String method = "getFsQuotaUsedInfo";
        final int[] policy = new int[1];
        Disposable disposable = pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo,
                        new MsException(NO_SUCH_BUCKET, "No such bucket")))
                .doOnNext(bucketInfo -> {
                    if (!bucketInfo.containsKey("fsid")) {
                        throw new MsException(ErrorNo.QUOTA_BUCKET_NOT_START_FS,
                                "no permission.bucket " + bucketName + " do not start FS");
                    }
                })
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, dirName[0], method).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .map(Tuple2::getT2)
                .doOnNext(bucketInfo -> {
                    if (!bucketInfo.containsKey("fsid")) {
                        throw new MsException(ErrorNo.QUOTA_BUCKET_NOT_START_FS,
                                "no permission.bucket " + bucketName + " do not start FS");
                    }
                })
                .flatMap(bucketInfo -> storagePool.mapToNodeInfo(bucketVnode).zipWith(Mono.just(bucketInfo)))
                .flatMap(tuple2 -> {
                    String versionId = StringUtils.isNotEmpty(request.getParam(VERSIONID)) ? request.getParam(VERSIONID)
                            : tuple2.getT2().containsKey(BUCKET_VERSION_STATUS) ? "" : "null";
                    return ErasureClient.getObjectMetaVersionUnlimited(bucketName, dirName[0], versionId, tuple2.getT1(), request)
                            .doOnNext(metaData -> checkMetaData(dirName[0], metaData, request.getParam(VERSIONID), null));
                })
                .doOnNext(metaData -> {
                    Map<String, String> objectAclMap = Json.decodeValue(metaData.getObjectAcl(), new TypeReference<Map<String, String>>() {
                    });
                    String objectUserId = objectAclMap.get("owner");
                    if (policy[0] == 0 && !objectUserId.equals(userId)) {
                        throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "No such get used info permission");
                    }
                })
                .flatMap(metaData -> {
                    if (metaData.inode <= 0) {
                        throw new MsException(QUOTA_INFO_NOT_EXITS, "No such quota info :" + metaData.inode);
                    }
                    return FSQuotaRealService.getFsQuotaInfo(bucketName, metaData.inode, quotaType[0], id_[0]);
                })
                .subscribe(dirInfo -> {
                    checkDirInfo(dirInfo);
                    try {
                        byte[] data = null;
                        if (usedType == 0) {
                            FsCapInfoResult fsUsedCapResult = new FsCapInfoResult();
                            fsUsedCapResult.setUsedCapacity(dirInfo.getUsedCap());
                            data = JaxbUtils.toByteArray(fsUsedCapResult);
                        } else if (usedType == 1) {
                            FsUsedObjectResult fsUsedObjectResult = new FsUsedObjectResult();
                            fsUsedObjectResult.setUsedObjects(dirInfo.getUsedObjects());
                            data = JaxbUtils.toByteArray(fsUsedObjectResult);
                        } else {
                            FsUsedInfo fsUsedInfo = new FsUsedInfo();
                            fsUsedInfo.setUsedCapacity(dirInfo.getUsedCap());
                            fsUsedInfo.setUsedObjects(dirInfo.getUsedObjects());
                            data = JaxbUtils.toByteArray(fsUsedInfo);
                        }
                        //response返回
                        addPublicHeaders(request, getRequestId())
                                .putHeader(CONTENT_TYPE, "application/xml")
                                .putHeader(CONTENT_LENGTH, String.valueOf(data.length))
                                .write(Buffer.buffer(data));
                        addAllowHeader(request.response()).end();
                    } catch (Exception e) {
                        log.error("", e);
                        MsException.dealException(request, e);
                    }
                }, e -> {
                    log.error("", e);
                    MsException.dealException(request, e);
                });

        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> disposable.dispose()));
        return ErrorNo.SUCCESS_STATUS;
    }

    public int getFsQuotaConfig(MsHttpRequest request) {
        String bucketName = request.getBucketName();
        String userId = request.getUserId();
        String[] dirName = new String[]{request.getHeader(DIR_NAME)};
        if (StringUtils.isBlank(dirName[0])) {
            throw new MsException(INVALID_ARGUMENT, "the dirName input is invalid.");
        }
        try {
            dirName[0] = URLDecoder.decode(dirName[0], "UTF-8");
        } catch (UnsupportedEncodingException e) {
        }
        if (SLASH.equals(dirName[0])) {
            throw new MsException(INVALID_ARGUMENT, "the dirName input is invalid.");
        }
        if (!dirName[0].endsWith(SLASH)) {
            dirName[0] += SLASH;
        }
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(bucketName, dirName[0]);
        int[] id_ = new int[]{-1};
        int[] quotaType = new int[]{-1};
        checkQuotaTypeAndId(request.getHeader(QUOTA_TYPE), request.getHeader("id_"), quotaType, id_);
        final int[] policy = new int[1];
        String method = "getFsQuotaConfig";
        Disposable disposable = pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo,
                        new MsException(NO_SUCH_BUCKET, "No such bucket")))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, dirName[0], method).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .map(Tuple2::getT2)
                .doOnNext(bucketInfo -> {
                    if (!bucketInfo.containsKey("fsid")) {
                        throw new MsException(ErrorNo.QUOTA_BUCKET_NOT_START_FS,
                                "no permission.bucket " + bucketName + " do not start FS");
                    }
                })
                .flatMap(bucketInfo -> storagePool.mapToNodeInfo(bucketVnode).zipWith(Mono.just(bucketInfo)))
                .flatMap(tuple2 -> {
                    String versionId = StringUtils.isNotEmpty(request.getParam(VERSIONID)) ? request.getParam(VERSIONID)
                            : tuple2.getT2().containsKey(BUCKET_VERSION_STATUS) ? "" : "null";
                    return ErasureClient.getObjectMetaVersionUnlimited(bucketName, dirName[0], versionId, tuple2.getT1(), request)
                            .doOnNext(metaData -> checkMetaData(dirName[0], metaData, request.getParam(VERSIONID), null));
                })
                .doOnNext(metaData -> {
                    Map<String, String> objectAclMap = Json.decodeValue(metaData.getObjectAcl(), new TypeReference<Map<String, String>>() {
                    });
                    String objectUserId = objectAclMap.get("owner");
                    if (policy[0] == 0 && !objectUserId.equals(userId)) {
                        throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "No such get quota info  permission");
                    }
                })
                .flatMap(metaData -> {
                    if (metaData.inode <= 0) {
                        throw new MsException(QUOTA_INFO_NOT_EXITS, "No such quota info :" + metaData.inode);
                    }
                    return FSQuotaRealService.realGetFsQuotaConfig(bucketName, metaData.inode, quotaType[0], id_[0]);
                })
                .subscribe(quotaConfigResult -> {
                    try {
                        byte[] data = JaxbUtils.toByteArray(quotaConfigResult);
                        //response返回
                        addPublicHeaders(request, getRequestId())
                                .putHeader(CONTENT_TYPE, "application/xml")
                                .putHeader(CONTENT_LENGTH, String.valueOf(data.length))
                                .write(Buffer.buffer(data));
                        addAllowHeader(request.response()).end();
                    } catch (Exception e) {
                        log.error("", e);
                        MsException.dealException(request, e);
                    }
                }, e -> {
                    log.error("", e);
                    MsException.dealException(request, e);
                });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> disposable.dispose()));
        return ErrorNo.SUCCESS_STATUS;
    }

    public int listFsQuotaConfig(MsHttpRequest request) {
        String bucketName = request.getBucketName();
        String[] dirName = new String[]{request.getHeader(DIR_NAME)};
        try {
            if (StringUtils.isNotBlank(dirName[0])) {
                dirName[0] = URLDecoder.decode(dirName[0], "UTF-8");
            }
        } catch (UnsupportedEncodingException e) {
        }
        Disposable disposable = pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo,
                        new MsException(NO_SUCH_BUCKET, "No such bucket")))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .flatMap(bucketInfo -> {
                    if (!bucketInfo.containsKey("fsid")) {
                        throw new MsException(ErrorNo.QUOTA_BUCKET_NOT_START_FS,
                                "no permission.bucket " + bucketName + " do not start FS");
                    }
                    if (!request.getUserId().equals(bucketInfo.get("user_id"))) {
                        throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "No such get quota info  permission");
                    }
                    return FSQuotaRealService.realListFsQuotaConfig(bucketName, dirName[0]);
                })
                .subscribe(quotaConfigResult -> {
                    try {
                        byte[] data = JaxbUtils.toByteArray(quotaConfigResult);
                        //response返回
                        addPublicHeaders(request, getRequestId())
                                .putHeader(CONTENT_TYPE, "application/xml")
                                .putHeader(CONTENT_LENGTH, String.valueOf(data.length))
                                .write(Buffer.buffer(data));
                        addAllowHeader(request.response()).end();
                    } catch (Exception e) {
                        log.error("", e);
                        MsException.dealException(request, e);
                    }
                }, e -> {
                    log.error("", e);
                    MsException.dealException(request, e);
                });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> disposable.dispose()));
        return ErrorNo.SUCCESS_STATUS;
    }

}
