package com.macrosan.action.datastream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.ErasureClient;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.xmlmsg.AccountsUsedCapacity;
import com.macrosan.message.xmlmsg.Buckets;
import com.macrosan.message.xmlmsg.section.AccountUsedCapacity;
import com.macrosan.message.xmlmsg.section.Bucket;
import com.macrosan.message.xmlmsg.section.Owner;
import com.macrosan.utils.msutils.CommonUtils;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.quota.QuotaRecorder;
import com.macrosan.utils.serialize.JaxbUtils;
import io.vertx.core.buffer.Buffer;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.Disposable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.httpserver.ResponseUtils.addAllowHeader;
import static com.macrosan.httpserver.ResponseUtils.addPublicHeaders;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.message.jsonmsg.BucketInfo.ERROR_BUCKET_INFO;
import static com.macrosan.utils.regex.PatternConst.ACCOUNT_NAME_PATTERN;

/**
 * 账户相关接口
 *
 * @auther wuhaizhong
 * @date 2020/5/29
 */
public class AccountService extends BaseService {
    /**
     * logger日志引用
     **/
    private static Logger logger = LogManager.getLogger(AccountService.class.getName());

    private static AccountService accountService;

    public static AccountService getInstance() {
        if (accountService == null) {
            accountService = new AccountService();
        }
        return accountService;
    }

    /**
     * 获取账户容量的操作：包括已使用容量
     *
     * @param request 请求
     * @return xml结果
     */
    public int getAccountUsedCap(MsHttpRequest request) {

        String userId = request.getUserId();
        MsAclUtils.checkIfManageAccount(userId);

        // 根据传过来的账户参数获取账户名称
        String accountName = request.getParam(MGT_CAP_ACCOUNT_NAME);
        Disposable subscribe = pool.getReactive(REDIS_USERINFO_INDEX).exists(accountName)
                .doOnNext(num -> {
                    if (num == 0L || !ACCOUNT_NAME_PATTERN.matcher(accountName).matches()) {
                        throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "The account you requested does not exist.");
                    }
                })
                .flatMap(b -> QuotaRecorder.getAccountTotalStorage(accountName)
                        .onErrorMap(e -> new MsException(ErrorNo.UNKNOWN_ERROR, "get account used capacity error!")))
                .zipWith(QuotaRecorder.getAccountTotalObject(accountName))
                .onErrorMap(e -> {
                            if (e instanceof MsException && ((MsException) e).getErrCode() == ErrorNo.NO_SUCH_ACCOUNT){
                                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "The account you requested does not exist.");
                            } else {
                                throw new MsException(ErrorNo.UNKNOWN_ERROR, "get account total object error!");
                            }
                        })
                .zipWith(pool.getReactive(REDIS_USERINFO_INDEX).hget(accountName, USER_DATABASE_ID_TYPE))
                .map(tuple2 -> {
                    AccountUsedCapacity accountUsedCapacity = new AccountUsedCapacity()
                            .setId(tuple2.getT2())
                            .setDisplayName(accountName)
                            .setUsedCapacity(String.valueOf(tuple2.getT1().getT1()))
                            .setObjectCount(String.valueOf(tuple2.getT1().getT2()));
                    List<AccountUsedCapacity> resobject = new ArrayList<>();
                    resobject.add(accountUsedCapacity);
                    return new AccountsUsedCapacity().setResData(resobject);
                })
                .subscribe(accountsUsedCapacity -> {
                    try {
                        byte[] data = JaxbUtils.toByteArray(accountsUsedCapacity);
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

    /**
     * 获取请求者自己创建的所有桶列表
     *
     * @param request 请求参数
     * @return 桶列表
     */
    public int listBucketsInfo(MsHttpRequest request) {
        String requestUserId = request.getUserId();
        String accountName = request.getParam(MGT_CAP_ACCOUNT_NAME);
        MsAclUtils.checkIfManageAccount(requestUserId);
        List<Bucket> bucketList = new ArrayList<>();
        Owner owner = new Owner();

        Disposable subscribe = pool.getReactive(REDIS_USERINFO_INDEX).hgetall(accountName)
                .doOnNext(map -> {
                    if (map.isEmpty() || StringUtils.isEmpty(map.get(USER_DATABASE_NAME_ID))) {
                        throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "The account you requested does not exist, " + accountName);
                    }
                    owner.setId(map.get(USER_DATABASE_NAME_ID)).setDisplayName(accountName);
                })
                .map(map -> map.get(USER_DATABASE_NAME_ID) + USER_BUCKET_SET_SUFFIX)
                .flatMapMany(userBucketSet -> pool.getReactive(REDIS_USERINFO_INDEX).smembers(userBucketSet))
                .flatMap(bucketName -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName))
                .filter(map -> !map.isEmpty())
                .flatMap(map -> ErasureClient.reduceBucketInfo(map.get("bucket_name"))
                        .doOnNext(bucketInfo -> {
                            if (bucketInfo.equals(ERROR_BUCKET_INFO)) {
                                throw new MsException(ErrorNo.UNKNOWN_ERROR, "get bucket storage info error!");
                            }
                            String capacityQuota = map.getOrDefault("quota_value", "0");
                            String objNumQuota = map.getOrDefault("objnum_value", "0");
                            Bucket bucket = new Bucket();
                            String createTime = MsDateUtils.stampToISO8601(map.get(USER_DATABASE_ID_CREATE_TIME));
                            bucket.setName(map.get("bucket_name"))
                                    .setCreationDate(createTime)
                                    .setStorageStrategy(map.get("storage_strategy"))
                                    .setCapacityQuota(capacityQuota)
                                    .setSoftCapacityQuota(map.getOrDefault("soft_quota_value", "0"))
                                    .setUsedCapacity(bucketInfo.getBucketStorage())
                                    .setObjNumQuota(objNumQuota)
                                    .setSoftObjNumQuota(map.getOrDefault("soft_objnum_value", "0"))
                                    .setUsedObjNum(bucketInfo.getObjectNum());
                            String usedCapacityRate = CommonUtils.getPercentageValue(capacityQuota, bucketInfo.getBucketStorage());
                            String usedObjNumRate = CommonUtils.getPercentageValue(objNumQuota, bucketInfo.getObjectNum());
                            bucket.setUsedCapacityRate(usedCapacityRate).setUsedObjNumRate(usedObjNumRate);
                            bucketList.add(bucket);
                        }))
                .collectList()
                .subscribe(accountsUsedCapacity -> {
                    try {
                        Buckets buckets = new Buckets().setOwners(owner);
                        if (bucketList.size() > 0) {
                            buckets.setBucketList(bucketList);
                        }
                        byte[] data = JaxbUtils.toByteArray(buckets);
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
