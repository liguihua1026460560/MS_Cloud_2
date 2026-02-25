package com.macrosan.utils.msutils;

import com.macrosan.database.mongodb.MsMongoClient;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.versions.ListVersionsResult;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.channel.ListVersionsMergeChannel;
import com.macrosan.utils.worm.WormUtils;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.HashMap;
import java.util.Map;

import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.ServerConstants.IS_SYNCING;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

/**
 * Created by zhanglinlin.
 *
 * @description 对象多版本相关接口
 * @date 2019/8/30 16:23
 */
public class MsObjVersionUtils {

    public static final Logger logger = LogManager.getLogger(MsObjVersionUtils.class.getName());

    private static RedisConnPool pool = RedisConnPool.getInstance();


    /**
     * @Description : 获取对象版本ID（响应式版本）
     *
     * @param  bucketName
     * @return  String
     * @author  zhanglinlin
     * @date  2019/9/29 10:55
     * @other TODO
     **/
    public static Mono<String> getObjVersionIdReactive(String bucketName){
       return pool.getReactive(REDIS_BUCKETINFO_INDEX).hexists(bucketName, BUCKET_VERSION_STATUS)
               .flatMap(exist -> exist ? pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, BUCKET_VERSION_STATUS)
                       : Mono.just("NULL"))
               .flatMap(status -> Mono.just("NULL".equals(status) || "Suspended".equals(status) ?
                "null": RandomStringUtils.randomAlphanumeric(32)));

    }



    /**
     * 判断桶版本是否开启(响应式)
     *
     * @param bucketName 桶名
     * @return 返回结果
     */
    public static Mono<String> versionStatusReactive(String bucketName){
        return pool.getReactive(REDIS_BUCKETINFO_INDEX).hexists(bucketName, BUCKET_VERSION_STATUS)
                .flatMap(exist-> exist ? pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, BUCKET_VERSION_STATUS)
                        : Mono.just("NULL"));
    }


    public static Mono<Boolean> checkObjVersionsLimit(String bucket, String object, MsHttpRequest request) {
        if (request != null && request.headers() != null && request.headers().contains(IS_SYNCING)) {
            return Mono.just(true);
        }
        return pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket)
                .flatMap(MsObjVersionUtils::checkObjVersionsLimit)
                .flatMap(num -> {
                    if (num == 0) {
                        return Mono.just(NOT_FOUND_META);
                    }
                    //worm状态下桶开启多版本并开启版本数量限制
                    SocketReqMsg reqMsg = new SocketReqMsg("", 0);
                    reqMsg.put("bucket", bucket);
                    reqMsg.put("prefix", object);
                    reqMsg.put("maxKeys", String.valueOf(num - 1));
                    reqMsg.put("marker", "");
                    reqMsg.put("versionIdMarker", "");
                    reqMsg.put("delimiter", "");
                    reqMsg.put("limit", "1");

                    StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucket);
                    ListVersionsResult listVersionsRes = new ListVersionsResult().setName(bucket).setMaxKeys(num - 1);
                    MonoProcessor<MetaData> metaDataMono = MonoProcessor.create();
                    Mono<MetaData> dataMono = Mono.just(metaPool.getBucketVnodeList(bucket)).flatMap(nodeList -> {
                        ListVersionsMergeChannel channel = (ListVersionsMergeChannel) new ListVersionsMergeChannel(bucket, listVersionsRes, metaPool, nodeList, request)
                                .withBeginPrefix(object, "", "");
                        channel.request(reqMsg);
                        return channel.response()
                                .doOnNext(b -> {
                                    if (!b){
                                        throw new MsException(UNKNOWN_ERROR,  bucket + ":" + object + " check versions error!");
                                    }
                                }).flatMap(s->{
                                    if (listVersionsRes.isTruncated()){
                                        metaDataMono.onNext(new MetaData().setDeleteMarker(false));
                                    } else {
                                        metaDataMono.onNext(NOT_FOUND_META);
                                    }
                                    return metaDataMono;
                                });
                    });

                    return dataMono;
                })
                .flatMap(metaData -> {
                    if (metaData != NOT_FOUND_META && metaData != ERROR_META) {
                        return Mono.just(false);
                    }
                    return Mono.just(true);
                })
                .doOnNext(b -> {
                    if (!b) {
                        throw new MsException(VERSION_LIMIT, "version upper limit.");
                    }
                });
    }

    private static Mono<Integer> checkObjVersionsLimit(Map<String, String> bucketInfo) {
        return Mono.just(0)
                .flatMap(limit -> {
                    String versionStatus = bucketInfo.get(BUCKET_VERSION_STATUS);
                    // 未启用多版本的桶不进行版本数量检测
                    if (!"Enabled".equals(versionStatus)) {
                        return Mono.just(0);
                    }
                    String quota = bucketInfo.get(BUCKET_VERSION_QUOTA);
                    if (StringUtils.isNotEmpty(quota)) {
                        try {
                            int i = Integer.parseInt(quota);
                            return Mono.just(i);
                        } catch (Exception ignored) {}
                    }
                    return Mono.just(limit);
                });
    }
}
