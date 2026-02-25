package com.macrosan.action.datastream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.ErasureClient;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.BucketInfo;
import com.macrosan.message.xmlmsg.BucketCapInfoResult;
import com.macrosan.message.xmlmsg.BucketUsedObjectsResult;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.ReactorPolicyCheckUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import io.vertx.core.buffer.Buffer;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.function.Tuple2;

import java.util.Optional;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.httpserver.ResponseUtils.addAllowHeader;
import static com.macrosan.httpserver.ResponseUtils.addPublicHeaders;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.message.jsonmsg.BucketInfo.ERROR_BUCKET_INFO;
import static com.macrosan.utils.msutils.MsException.throwWhenEmpty;

/**
 * 桶相关接口
 *
 * @auther chenliaokai
 * @date 2021/08/02
 */
public class BucketService extends BaseService {
    /**
     * logger日志引用
     **/
    private static Logger logger = LogManager.getLogger(BucketService.class.getName());

    private static BucketService bucketService;

    public static BucketService getInstance() {
        if (bucketService == null) {
            bucketService = new BucketService();
        }
        return bucketService;
    }

    /**
     * 获取桶可用容量和剩余容量
     *
     * @param request 请求
     * @return xml结果
     */
    public int getBucketCapInfo(MsHttpRequest request) {
        String userId = request.getUserId();
        MsAclUtils.checkIfAnonymous(userId);
        String bucketName = request.getBucketName();
        String method = "GetBucketCapInfo";

        String[] bucketQuota = new String[]{null};

        MonoProcessor<String> res = MonoProcessor.create();
        Disposable subscribe = pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucketName)
                .doOnNext(info -> throwWhenEmpty(info, new MsException(ErrorNo.NO_SUCH_BUCKET, "No such bucket")))
                .doOnNext(info -> regionCheck(info.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
				.flatMap(info -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, method).zipWith(Mono.just(info)))
                .doOnNext(tuple2 -> {
                    if (!userId.equals(tuple2.getT2().get(BUCKET_USER_ID))) {
                        throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,
                                "no permission.user " + userId + " can not get bucket storage.");
                    }
                    bucketQuota[0] = Optional.ofNullable(tuple2.getT2().get("quota_value")).orElse("0");
                })
                .flatMap(info -> ErasureClient.reduceBucketInfo(bucketName))
                .doOnNext(bucketInfo -> {
                    if (bucketInfo.equals(ERROR_BUCKET_INFO)) {
                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "get bucket storage info error!");
                    }
                })
                .filter(BucketInfo::isAvailable)
                .doOnNext(bucketInfo -> logger.info("bucket {} objSize:{},capacity:{}", bucketName, bucketInfo.objectNum, bucketInfo.bucketStorage))
                .map(BucketInfo::getBucketStorage)
                .doOnError(e -> logger.error("get bucket storage info error,{}", e))
                .defaultIfEmpty("0")
                .map(bucketStorage -> {
                    Long usedCapNum = (StringUtils.isBlank(bucketStorage) || Long.valueOf(bucketStorage) < 0) ? 0L : Long.valueOf(bucketStorage);
                    Long totalCapNum = Long.parseLong(bucketQuota[0]);
                    long avaCapNum;

                    if (0 == totalCapNum) {
                        avaCapNum = -1L;
                    } else {
                        avaCapNum = totalCapNum - usedCapNum;
                        if (avaCapNum <= 0) {
                            avaCapNum = 0L;
                        }
                    }
                    BucketCapInfoResult bucketCapInfoResult = new BucketCapInfoResult();
                    bucketCapInfoResult.setUsedCapacity(usedCapNum.toString());
                    bucketCapInfoResult.setAvailableCapacity(String.valueOf(avaCapNum));
                    return bucketCapInfoResult;
                })
                .subscribe(bucketCapInfoResult -> {
                    try {
                        byte[] data = JaxbUtils.toByteArray(bucketCapInfoResult);
                        //response返回
                        addPublicHeaders(request, getRequestId())
                                .putHeader(CONTENT_TYPE, "application/xml")
                                .putHeader(CONTENT_LENGTH, String.valueOf(data.length))
                                .write(Buffer.buffer(data));
                        addAllowHeader(request.response()).end();
                    } catch (Exception e) {
                        MsException.dealException(request, e);
                    }
                }, e -> MsException.dealException(request, e));


        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return ErrorNo.SUCCESS_STATUS;
    }

    public int getBucketUsedObjects(MsHttpRequest request) {
        String userId = request.getUserId();
        MsAclUtils.checkIfAnonymous(userId);
        String bucketName = request.getBucketName();
        String method = "GetBucketUsedObjects";

        //String[] bucketObjects = new String[]{null};
        MonoProcessor<String> res = MonoProcessor.create();
        Disposable subscribe = pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucketName)
                .doOnNext(info -> throwWhenEmpty(info, new MsException(ErrorNo.NO_SUCH_BUCKET, "No such bucket")))
                .doOnNext(info -> regionCheck(info.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .flatMap(info -> ReactorPolicyCheckUtils.getPolicyCheckResult(request, bucketName, method).zipWith(Mono.just(info)))
                .map(Tuple2::getT2)
                .doOnNext(info -> {
                    if (!userId.equals(info.get(BUCKET_USER_ID))) {
                        throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,
                                "no permission.user " + userId + " can not get bucket objects.");
                    }
                    //bucketObjects[0] = Optional.ofNullable(info.get("quota_value")).orElse("0");
                })
                .flatMap(info -> ErasureClient.reduceBucketInfo(bucketName))
                .doOnNext(bucketInfo -> {
                    if (bucketInfo.equals(ERROR_BUCKET_INFO)) {
                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "get bucket storage info error!");
                    }
                })
                .filter(BucketInfo::isAvailable)
                .doOnNext(bucketInfo -> logger.info("bucket {} objSize:{},capacity:{}", bucketName, bucketInfo.objectNum, bucketInfo.bucketStorage))
                .map(BucketInfo::getObjectNum)
                .doOnError(e -> logger.error("get bucket objects info error,{}", e))
                .defaultIfEmpty("0")
                .map(bucketUsedObjects -> {
                    Long usedObjectNum = (StringUtils.isBlank(bucketUsedObjects) || Long.valueOf(bucketUsedObjects) < 0) ? 0L : Long.valueOf(bucketUsedObjects);
                    BucketUsedObjectsResult bucketUsedObjectsResult = new BucketUsedObjectsResult();
                    bucketUsedObjectsResult.setUsedObjects(String.valueOf(usedObjectNum));
                    return bucketUsedObjectsResult;
                })
                .subscribe(bucketUsedObjectsResult -> {
                    try {
                        byte[] data = JaxbUtils.toByteArray(bucketUsedObjectsResult);
                        //response返回
                        addPublicHeaders(request, getRequestId())
                                .putHeader(CONTENT_TYPE, "application/xml")
                                .putHeader(CONTENT_LENGTH, String.valueOf(data.length))
                                .write(Buffer.buffer(data));
                        addAllowHeader(request.response()).end();
                    } catch (Exception e) {
                        MsException.dealException(request, e);
                    }
                }, e -> MsException.dealException(request, e));

        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return ErrorNo.SUCCESS_STATUS;
    }


}
