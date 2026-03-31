package com.macrosan.component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.component.enums.ErrorEnum;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.component.pojo.ComponentTask;
import com.macrosan.component.pojo.MediaResult;
import com.macrosan.component.utils.ErrorRecordUtil;
import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.AccessKeyVO;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.ModuleDebug;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.serialize.JsonUtils;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.http.HttpClient;
import io.vertx.reactivex.core.http.HttpClientRequest;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.*;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuples;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.component.ComponentStarter.*;
import static com.macrosan.component.ComponentUtils.addExpire;
import static com.macrosan.component.enums.ErrorEnum.IMAGE_TOTAL_PIXEL_ERROR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.ErasureClient.updateMetaDataAcl;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SEND_DICOM_RECORD;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;

/**
 * 扫描对象存储内部的数据发往外部进程处理。
 */
@Log4j2
public class TaskSender {

    private static final int PROCESSOR_NUM = PROC_NUM;

    private static final RedisConnPool POOL = RedisConnPool.getInstance();

    private static final IamRedisConnPool IAM_POOL = IamRedisConnPool.getInstance();

    private static final UnicastProcessor<ComponentRecord>[] OPERATE_PROCESSOR = new UnicastProcessor[PROCESSOR_NUM];

    public static Vertx vertx;

    public static HttpClient client;

    private static int IMG_PORT;

    private static final Map<String, String> UUID_BACKEND_IP_MAP = new ConcurrentHashMap<>();

    public static void init() {
        vertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(PROC_NUM)
                .setPreferNativeTransport(true));
        HttpClientOptions options = new HttpClientOptions()
                .setKeepAlive(true)
                .setKeepAliveTimeout(60)
                .setMaxPoolSize(500)
                .setMaxWaitQueueSize(10000)
                .setConnectTimeout(3_000)
                .setIdleTimeout(600);
        client = vertx.createHttpClient(options);

        String port = Optional.ofNullable(POOL.getCommand(REDIS_SYSINFO_INDEX).get("img_port")).orElse("2330");
        IMG_PORT = Integer.parseInt(port);

        String nodeListStr = POOL.getCommand(REDIS_SYSINFO_INDEX).get("node_list");
        for (String uuid : JSON.parseArray(nodeListStr, String.class)) {
            String eth4Ip = POOL.getCommand(REDIS_NODEINFO_INDEX).hget(uuid, HEART_ETH1);
            UUID_BACKEND_IP_MAP.put(uuid, eth4Ip);
        }

        for (int i = 0; i < OPERATE_PROCESSOR.length; i++) {
            OPERATE_PROCESSOR[i] = UnicastProcessor.create(Queues.<ComponentRecord>unboundedMultiproducer().get());
            OPERATE_PROCESSOR[i].publishOn(COMP_SCHEDULER).subscribe(record -> {
                // componentRecord处理前先去get元数据，如果该对象已按record.strategy处理过则不再处理。
                StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(record.bucket);
                ErrorEnum[] errorEnums = new ErrorEnum[1];
                POOL.getReactive(REDIS_SYSINFO_INDEX).get(ADMIN_ACCESS_KEY)
                        .defaultIfEmpty(ADMIN_ACCESS_AK)
                        .doOnNext(record::setAk)
                        .flatMap(ak -> IAM_POOL.getReactive(0).get(ak))
                        .doOnNext(accessKeyValue -> {
                            AccessKeyVO accessKeyVO = JsonUtils.toObject(AccessKeyVO.class, accessKeyValue.getBytes());
                            record.setSk(accessKeyVO.getSecretKey());
                        })
                        .flatMap(a -> POOL.getReactive(REDIS_BUCKETINFO_INDEX).exists(record.bucket))
                        .flatMap(b -> {
                            if (b != 1L) {
                                return ComponentUtils.deleteComponentRecord(record).map(a -> false);
                            } else {
                                return Mono.just(true);
                            }
                        })
                        .filter(b -> b)
                        .flatMap(a -> bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(record.bucket, record.object)))
                        .flatMap(bucketVnodeList -> ErasureClient.getObjectMetaVersionResOnlyRead(record.bucket, record.object,
                                record.versionId, bucketVnodeList, null))
                        .map(Tuple2::var1)
                        .publishOn(COMP_SCHEDULER)
                        .filter(meta -> !MetaData.ERROR_META.equals(meta))
                        .flatMap(metaData -> {
                            if (metaData.deleteMarker || metaData.deleteMark
                                    || !record.syncStamp.equals(metaData.syncStamp) || MetaData.NOT_FOUND_META.equals(metaData)
                                    || ComponentUtils.hasCompressed(metaData, record.getStrategy().getType())) {
                                // 如果已经处理过或者已经被同名对象覆盖或者已经被删除，则删除该record。
                                if (ModuleDebug.mediaComponentDebug()) {
                                    log.info("delete component record. {}, {} ", Json.encode(metaData), Json.encode(record));
                                }
                                return ComponentUtils.deleteComponentRecord(record)
                                        .flatMap(b -> Mono.just(false));
                            } else {
                                if (!record.headerMap.containsKey("userId")) {
                                    Map<String, String> sysMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
                                    });
                                    record.headerMap.put("userId", sysMap.get("owner"));
                                }
                                return Mono.just(true);
                            }
                        })
                        .filter(b -> b)
                        .flatMap(b -> dealComponentRecord(record))
                        .filter(b -> {
                            errorEnums[0] = b.var2;
                            return errorEnums[0] != null || b.var1;
                        })
                        .flatMap(f -> {
                            // 添加到失败列中，不更新对象元数据
                            if (errorEnums[0] != null) {
                                return ErrorRecordUtil.putErrorRecord(errorEnums[0], record);
                            } else if (record.strategy.deleteSource) {
                                // 删源
                                return ComponentUtils.deleteSourceObject(record);
                            } else if (!record.taskMarker.equals(COMPONENT_RECORD_INNER_MARKER)) {
                                // 任务级别的才需要更新源对象元数据
                                return ComponentUtils.updateSourceObjectMetaData(record, bucketPool);
                            } else {
                                // 对象级别不更新元数据
                                return Mono.just(true);
                            }
                        })
                        .flatMap(b -> {
                            if (b) {// put失败记录成功或更新元数据成功才会删除记录
                                return ComponentUtils.deleteComponentRecord(record);
                            } else {
                                log.error("put img MetaData fail.{} {}", record.bucket, record.object);
                                return Mono.just(false);
                            }
                        })
                        .doFinally(s -> ComponentUtils.endRecordOperation(record))
                        .subscribe(b -> {
                            // 插件处理及更新元数据均成功，则删除该记录
                            if (!b) {
                                log.error("deleteComponentRecord fail.{}", record.rocksKey());
                            }
                        }, e -> {
                            if (e instanceof TimeoutException) {
                                // timeout应该会有很多
                                log.debug("deal comp record fail, bucket:{}, object:{}, versionId:{} ", record.bucket, record.object, record.versionId, e);
                            } else {
                                log.error("deal comp record fail, bucket:{}, object:{}, versionId:{} ", record.bucket, record.object, record.versionId, e);
                            }
                        });

            }, e -> log.error("dealComponentRecord error,", e));
        }
    }

    private static Mono<Tuple2<Boolean, ErrorEnum>> dealComponentRecord(ComponentRecord record) {
        try {
            switch (record.strategy.getType()) {
                case VIDEO:
                case IMAGE: {
                    return sendComponentRequest(record, IMG_PORT, "/sendImageRecord");
                }
                case DICOM:
                    return sendDicomRequest(record);
                default:
                    log.error("no such type: {}", record.strategy.getType());
                    return Mono.just(new Tuple2<>(false, null));
            }
        } catch (Exception e) {
            log.error("dealMultiMediaTask error, ", e);
            return Mono.just(new Tuple2<>(false, null));
        }
    }

    /**
     * 处理DICOM任务，发出请求
     *
     * @param record
     * @return
     */
    private static Mono<Tuple2<Boolean, ErrorEnum>> sendDicomRequest(ComponentRecord record) {
        String ip = ComponentUtils.getMossServerRadomAvailIp();
        if (ip.equals(LOCAL_IP_ADDRESS)) {
            // 发送rsocket请求，将本地ip替换为本地eth4_ip
            ip = CURRENT_IP;
        }
        long startTime = System.currentTimeMillis();
        String finalIp = ip;
        return RSocketClient.getRSocket(ip, BACK_END_PORT)
                .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(record), SEND_DICOM_RECORD.name())))
                .flatMap(payload -> {
                    if (payload.getMetadataUtf8().equals(ErasureServer.PayloadMetaType.SUCCESS.name())) {
                        if (ModuleDebug.mediaComponentDebug()) {
                            log.info("send dicom request to ip:{} success,bucket:{} object:{} versionId:{} cost: {}", finalIp, record.bucket, record.object, record.versionId, System.currentTimeMillis() - startTime);
                        }
                        return Mono.just(new Tuple2<Boolean, ErrorEnum>(true, null));
                    } else {
                        if (ModuleDebug.mediaComponentDebug()) {
                            log.info("send dicom request to ip:{} fail cause:{},bucket:{} object:{} versionId:{} cost: {}", finalIp, payload.getDataUtf8(), record.bucket, record.object, record.versionId, System.currentTimeMillis() - startTime);
                        }
                        return Mono.just(new Tuple2<Boolean, ErrorEnum>(false, null));
                    }
                })
                .onErrorResume(e -> {
                    AVAIL_MOSS_SERVER_IP_SET.remove(finalIp);
                    return Mono.just(new Tuple2<>(false, null));
                });
    }

    /**
     * 处理ComponentTask，发出请求
     */
    private static Mono<Tuple2<Boolean, ErrorEnum>> sendComponentRequest(ComponentRecord record, int port, String uri) {
        MonoProcessor<Tuple2<Boolean, ErrorEnum>> res = MonoProcessor.create();
        String ip = ComponentUtils.getMediaServerRadomAvailIp();

        byte[] bytes = Json.encode(record).getBytes();
        HttpClientRequest request = client.request(HttpMethod.PUT, port, ip, uri);
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(record.bucket);
        request.setTimeout(600_000)
                .setHost(ip + ":" + port)
                .putHeader(EXPECT, EXPECT_100_CONTINUE)
                .putHeader(CONTENT_LENGTH, String.valueOf(bytes.length))
                .putHeader(SYNC_AUTH, PASSWORD)
                .exceptionHandler(e -> {
                    log.debug("sendComponentRequest to {} error! {}, {}, {}", ip, record, e.getClass().getName(), e.getMessage());
                    AVAIL_MEDIA_SERVER_IP_SET.remove(ip);
                    DoubleActiveUtil.closeConn(request);
                    res.onNext(new Tuple2<>(false, null));
                })
                .continueHandler(b -> {
                    request.write(Buffer.buffer(bytes));
                    request.end();
                })
                .handler(resp -> {
                    String[] dataStr = new String[]{""};
                    AtomicBoolean resFlag = new AtomicBoolean();
                    resp.handler(buffer -> {
                        dataStr[0] += new String(buffer.getBytes());
                        // 判断是否属于完整的json数据
                        if (dataStr[0].startsWith("/") && dataStr[0].endsWith("/")) {
                            String dataJson = dataStr[0].substring(1, dataStr[0].length() - 1);
                            MediaResult result = JSONObject.parseObject(dataJson, MediaResult.class);
                            if ("continue".equals(result.getMsg()) && result.getData() != null) {
                                JSONObject resultData = (JSONObject) result.getData();
                                record.headerMap.put("timeStamp", resultData.getString("timeStamp"));
                                record.headerMap.put("index", resultData.getString("index"));
                                record.setVersionNum(VersionUtil.getVersionNum());
                                bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(record.bucket, record.object))
                                        .flatMap(bucketVnodeList -> ErasureClient.getObjectMetaVersion(record.bucket, record.object,
                                                record.versionId, bucketVnodeList, null))
                                        .flatMap(metaData -> {
                                            if (metaData.deleteMarker || metaData.deleteMark
                                                    || !record.syncStamp.equals(metaData.syncStamp) || MetaData.NOT_FOUND_META.equals(metaData)) {
                                                // 原对象已删除或被覆盖  暂停进行中的视频截帧
                                                request.connection().close();
                                                res.onNext(new Tuple2<>(false, null));// 此处返回false，不直接删除记录  原对象不存在再次扫描到记录时会进行删除
                                                resFlag.set(true);
                                                return Mono.just(false);
                                            }
                                            if (StringUtils.equals(COMPONENT_RECORD_INNER_MARKER, record.taskMarker)) {
                                                return Mono.just(true);
                                            }
                                            return POOL.getReactive(REDIS_POOL_INDEX).hget(ComponentTask.REDIS_KEY + "_" + record.taskName, "marker")
                                                    .defaultIfEmpty("")
                                                    .flatMap(mark -> {
                                                        if (!mark.equals(record.taskMarker)) {
                                                            // 任务已删除  暂停进行中的视频截帧
                                                            request.connection().close();
                                                            res.onNext(new Tuple2<>(false, null));// 此处返回false，不直接删除记录，在ComponentScanner.scanDeleteTaskSet中处理
                                                            resFlag.set(true);
                                                            return Mono.just(false);
                                                        }
                                                        return Mono.just(true);
                                                    });
                                        })
                                        .filter(b -> b)
                                        .flatMap(b -> ComponentUtils.putComponentRecord(record, null))
                                        .doOnNext(b -> addExpire(record))
                                        .doFinally(s -> dataStr[0] = "")
                                        .subscribe();
                            } else if (result.getCode() == MAXIMUM_PIXEL_ERROR_CODE) {
                                // 插件处理的图片总像素已达上限，暂不处理该图片
                                res.onNext(new Tuple2<>(false, null));
                                resFlag.set(true);
                                log.debug("The total number of pixels in the processed image has reached the limit,ip:{}", ip);
                            } else {
                                res.onNext(new Tuple2<>(result.getCode() == SUCCESS, ErrorEnum.getErrorEnum(result.getCode())));
                                resFlag.set(true);
                                log.debug("ip:{}, statusCode:{}, msg:{},", ip, result.getCode(), result.getMsg());
                            }
                        }
                    }).endHandler(h -> {
                        if (!resFlag.get()) {
                            res.onNext(new Tuple2<>(resp.statusCode() == SUCCESS, null));
                            log.debug("ip:{}, statusCode:{}, msg:{},", ip, resp.statusCode(), resp.statusMessage());
                        }
                    });
                })
                .sendHead();
        return res;
    }

    /**
     * 由TaskScanner使用，将扫描后生成的ComponentTask均分给发起请求的线程
     */
    public static void distributeMultiMediaRecord(ComponentRecord record) {
        int i = ThreadLocalRandom.current().nextInt(PROCESSOR_NUM);
        OPERATE_PROCESSOR[i].onNext(record);
    }

}
