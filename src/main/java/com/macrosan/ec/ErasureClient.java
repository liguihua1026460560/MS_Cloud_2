package com.macrosan.ec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.doubleActive.MsClientRequest;
import com.macrosan.ec.server.ErasureServer.PayloadMetaType;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.cifs.notify.NotifyServer;
import com.macrosan.filesystem.cifs.reply.smb2.NotifyReply.NotifyAction;
import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.quota.FSQuotaRealService;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.consturct.RequestBuilder;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.rsocket.LocalPayload;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.aggregation.AggregateFileClient;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.storage.client.GetMultiPartObjectClientHandler;
import com.macrosan.storage.coder.DataDivision;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.storage.coder.LimitEncoder;
import com.macrosan.storage.coder.fs.NoLimitEncoder;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.utils.cache.FastMd5DigestPool;
import com.macrosan.utils.functional.Function4;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsObjVersionUtils;
import com.macrosan.utils.msutils.QuickReturn;
import com.macrosan.utils.msutils.md5.Digest;
import com.macrosan.utils.msutils.md5.FastMd5Digest;
import com.macrosan.utils.msutils.md5.Md5Digest;
import com.macrosan.utils.msutils.md5.PartMd5Digest;
import com.macrosan.utils.params.DelMarkParams;
import com.macrosan.utils.quota.QuotaRecorder;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.lang.reflect.Array;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.archive.ArchiveAnalyzer.ARCHIVE_ANALYZER_KEY;
import static com.macrosan.ec.ECUtils.*;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.NOT_FOUND;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.filesystem.FsConstants.S_IFDIR;
import static com.macrosan.filesystem.FsConstants.S_IFMT;
import static com.macrosan.filesystem.cifs.call.smb2.NotifyCall.FILE_NOTIFY_CHANGE_DIR_NAME;
import static com.macrosan.filesystem.cifs.call.smb2.NotifyCall.FILE_NOTIFY_CHANGE_FILE_NAME;
import static com.macrosan.filesystem.quota.FSQuotaConstants.QUOTA_KEY;
import static com.macrosan.filesystem.utils.ChunkFileUtils.mapToPartInfo;
import static com.macrosan.filesystem.utils.FSQuotaUtils.addQuotaInfoToMetaData;
import static com.macrosan.httpserver.MossHttpClient.WRITE_ASYNC_RECORD;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.message.jsonmsg.BucketInfo.ERROR_BUCKET_INFO;
import static com.macrosan.message.jsonmsg.BucketInfo.NOT_FOUND_BUCKET_INFO;
import static com.macrosan.message.jsonmsg.Credential.*;
import static com.macrosan.message.jsonmsg.DedupMeta.ERROR_DEDUP_META;
import static com.macrosan.message.jsonmsg.DedupMeta.NOT_FOUND_DEDUP_META;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.ERROR_UNSYNC_RECORD;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.NOT_FOUND_UNSYNC_RECORD;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;
import static com.macrosan.storage.move.CacheMove.isEnableCacheOrderFlush;
import static com.macrosan.utils.msutils.MsException.dealException;

@Log4j2
public class ErasureClient {
    private static final RedisConnPool redisConnPool = RedisConnPool.getInstance();

    public static final byte[] PUT_OBJECT_NAME_BYTES = PUT_OBJECT.name().getBytes();
    public static final byte[] ERROR_NAME_BYTES = ERROR.name().getBytes();


    public static Mono<Boolean> putObject(StoragePool storagePool, List<Tuple3<String, String, String>> nodeList,
                                          UnicastProcessor<byte[]>[] dataFlux, MetaData meta, LimitEncoder encoder,
                                          MsHttpRequest request, MonoProcessor<Boolean> recoverDataProcessor) {
        String bucketVnode = StoragePoolFactory.getMetaStoragePool(meta.bucket).getBucketVnodeId(meta.bucket, meta.key);
        String versionMetaKey = Utils.getVersionMetaDataKey(bucketVnode, meta.bucket, meta.key, meta.versionId, meta.snapshotMark);
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("metaKey", versionMetaKey)
                .put("noGet", "1")
                .put("compression", storagePool.getCompression())
                .put("fileName", meta.fileName);
        // 判断是否开启缓冲池 按序下刷
        if (isEnableCacheOrderFlush(storagePool)) {
            msg.put("flushStamp", meta.stamp);
        }

        CryptoUtils.generateKeyPutToMsg(meta.crypto, msg);

        if (meta.smallFile) {
            msg.put("smallFile", "0");
        }

        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> msg.copy()
                        .put("lun", tuple.var2)
                        .put("vnode", tuple.var3))
                .collect(Collectors.toList());

        List<UnicastProcessor<Payload>> publisher = nodeList.stream()
                .map(t -> {
                    UnicastProcessor<Payload> processor = UnicastProcessor.create(Queues.<Payload>unboundedMultiproducer().get());
                    return processor;
                })
                .collect(Collectors.toList());

        boolean[] first = new boolean[publisher.size()];
        Arrays.fill(first, true);
        boolean[] published = new boolean[]{false};

        for (int i = 0; i < publisher.size(); i++) {
            int index = i;
            encoder.request(i, 1L);
            dataFlux[index].subscribe(bytes -> {
                        published[0] = true;
                        if (first[index]) {
                            if (bytes.length < storagePool.getPackageSize()) {
                                Payload payload;
                                if (CURRENT_IP.equals(nodeList.get(index).var1)) {
                                    payload = new LocalPayload<>(PUT_OBJECT_ONCE, new Tuple2<>(msgs.get(index), bytes));
                                } else {
                                    byte[] socketBytes = msgs.get(index).toBytes().array();
                                    payload = DefaultPayload.create(Unpooled.wrappedBuffer(socketBytes, bytes), Unpooled.wrappedBuffer(PUT_OBJECT_ONCE.name().getBytes()));
                                }

                                publisher.get(index).onNext(payload);
                                publisher.get(index).onComplete();
                                log.debug("PUT_OBJECT_ONCE: {}, {}", index, first[index]);
                            } else {
                                publisher.get(index).onNext(DefaultPayload.create(Json.encode(msgs.get(index)), START_PUT_OBJECT.name()));
                                publisher.get(index).onNext(DefaultPayload.create(bytes, PUT_OBJECT_NAME_BYTES));
                            }

                            first[index] = false;
                        } else {
                            publisher.get(index).onNext(DefaultPayload.create(bytes, PUT_OBJECT_NAME_BYTES));
                        }
                    },
                    e -> {
                        log.error("", e);
                        publisher.get(index).onNext(DefaultPayload.create("put file error", ERROR.name()));
                        publisher.get(index).onComplete();
                    },
                    () -> {
                        if (published[0]) {
                            publisher.get(index).onNext(DefaultPayload.create("", COMPLETE_PUT_OBJECT.name()));
                        } else {
                            publisher.get(index).onNext(DefaultPayload.create("", PUT_EMPTY_OBJECT.name()));
                        }
                        publisher.get(index).onComplete();
                    }
            );
        }


        ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(publisher, String.class, nodeList);

        MonoProcessor<Boolean> res = MonoProcessor.create();
        List<Integer> errorChunksList = new ArrayList<>(storagePool.getM());
//        String storageName = "storage_" + storagePool.getVnodePrefix();
//        String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
//        if (StringUtils.isEmpty(poolName)) {
//            String strategyName = "storage_" + storagePool.getVnodePrefix();
//            poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//        }
//        String poolQueueTag = poolName;
        AtomicBoolean quickReturn = new AtomicBoolean(false);

        Disposable disposable = responseInfo.responses
                .subscribe(s -> {
                    if (s.var2.equals(ERROR)) {
                        encoder.completeN(s.var1, Long.MAX_VALUE);
                        errorChunksList.add(s.var1);
                        encoder.request(s.var1, Long.MAX_VALUE);
                    } else {
                        encoder.completeN(s.var1, 1L);
                        encoder.request(s.var1, 1L);
                    }

                    if (request.getQuickReturn() == 1 && responseInfo.successNum >= storagePool.getK()
                            && !quickReturn.get() && QuickReturn.acquire()) {
                        quickReturn.set(true);
                        res.onNext(true);
                    }
                }, e -> log.error("", e), () -> {
                    if (quickReturn.compareAndSet(true, false)) {
                        //quick return
                        QuickReturn.release();
                    }
                    if (responseInfo.successNum == storagePool.getK() + storagePool.getM()) {
                        res.onNext(true);
                    } else if (responseInfo.successNum >= storagePool.getK()) {
                        res.onNext(true);

                        //订阅数据修复消息的发出。b表示k+m个元数据是否至少写上了一个。
                        recoverDataProcessor.subscribe(b -> {
                            if (b) {
                                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                        .put("errorChunksList", Json.encode(errorChunksList))
                                        .put("bucket", meta.bucket)
                                        .put("object", meta.key)
                                        .put("fileName", meta.fileName)
                                        .put("versionId", meta.versionId)
                                        .put("storage", storagePool.getVnodePrefix())
                                        .put("stamp", meta.stamp)
                                        .put("fileSize", String.valueOf(encoder.size()))
                                        .put("poolQueueTag", poolQueueTag)
                                        .put("fileOffset", "");
                                Optional.ofNullable(meta.snapshotMark).ifPresent(v -> errorMsg.put("snapshotMark", v));
                                Optional.ofNullable(msg.get("flushStamp")).ifPresent(v -> errorMsg.put("flushStamp", v));

                                CryptoUtils.putCryptoInfoToMsg(meta.crypto, msg.get("secretKey"), errorMsg);

                                publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_PUT_OBJECT_FILE);
                            }
                        });

                    } else {
                        res.onNext(false);
                        //响应成功数量达不到k，发布回退消息，删掉成功的节点上的文件
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0);
                        errorMsg.put("bucket", meta.bucket);
                        errorMsg.put("storage", storagePool.getVnodePrefix());
                        errorMsg.put("object", meta.key);
                        errorMsg.put("fileName", meta.fileName);
                        errorMsg.put("poolQueueTag", poolQueueTag);
                        ECUtils.publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_ROLL_BACK_FILE);
                    }
                });

        request.addResponseCloseHandler(v -> {
            for (UnicastProcessor<Payload> processor : publisher) {
                processor.onNext(ERROR_PAYLOAD);
                processor.onComplete();
            }

            disposable.dispose();

            if (quickReturn.compareAndSet(true, false)) {
                //quick return
                QuickReturn.release();
            }
        });

        return res;
    }

    public static Mono<MetaData> getObjectMeta(String bucket, String object, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        return getObjectMeta(bucket, object, nodeList, request, null, null);
    }

    public static Mono<MetaData> getObjectMeta(String bucket, String object, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String currentSnapshotMark, String snapshotLink) {
        return getObjectMetaRes(bucket, object, nodeList, request, currentSnapshotMark, snapshotLink)
                .flatMap(tuple2 -> Mono.just(tuple2.var1));
    }

    public static Mono<Tuple2<MetaData, Tuple2<PayloadMetaType, MetaData>[]>> getObjectMetaRes(
            String bucket, String object, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String currentSnapshotMark, String snapshotLink, boolean... isUnLimit) {
        String vnode = nodeList.get(0).var3;
        String metaKey = ROCKS_SPECIAL_KEY + Utils.getMetaDataKey(vnode, bucket, object, currentSnapshotMark);

        return ECUtils.getRocksKey(StoragePoolFactory.getMetaStoragePool(bucket), metaKey, GET_OBJECT_META,
                        NOT_FOUND_META, ERROR_META, nodeList, request, currentSnapshotMark, snapshotLink)
                .flatMap(tuple3 -> {
                    if (!tuple3.var1 || tuple3.var2.equals(NOT_FOUND_META)) {
                        return Mono.just(new Tuple2<>(tuple3.var2, tuple3.var3));
                    } else {
                        if (isUnLimit.length > 0) {
                            return getObjectMetaVersionResUnlimited(bucket, object, tuple3.var2.versionId, nodeList, request, currentSnapshotMark, snapshotLink);
                        } else {
                            return getObjectMetaVersionRes(bucket, object, tuple3.var2.versionId, nodeList, request, currentSnapshotMark, snapshotLink);
                        }
                    }
                });
    }

    //获取指定版本的对象元数据
    public static Mono<MetaData> getObjectMetaVersion(String bucket, String object, String versionId,
                                                      List<Tuple3<String, String, String>> nodeList) {
        return getObjectMetaVersion(bucket, object, versionId, nodeList, null);
    }

    //获取指定版本的对象元数据；被业务性质功能调用，不受QoS限制
    public static Mono<MetaData> getObjectMetaVersionUnlimited(String bucket, String object, String versionId,
                                                               List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        return getObjectMetaVersionUnlimited(bucket, object, versionId, nodeList, request, null, null);
    }

    /**
     * 仅在文件端 deleteInode中的 setFsDelMark调用
     **/
    public static Mono<MetaData> getFsMetaVerUnlimited(String bucket, String object, String versionId,
                                                       List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String currentSnapshotMark, String snapshotLink, String quotaStr) {
        return getObjectMetaVersion(bucket, object, versionId, nodeList, request,
                (key, metaData, nodeList1, res) -> {
                    if (StringUtils.isNotBlank(quotaStr)) {
                        Inode inode = new Inode();
                        if (metaData.isAvailable()) {
                            inode = Inode.defaultInode(metaData);
                            if (StringUtils.isNotBlank(metaData.tmpInodeStr)) {
                                inode = Json.decodeValue(metaData.tmpInodeStr, Inode.class);
                            } else {
                                inode = Inode.defaultInode(metaData);
                            }
                        } else {
                            inode.setSize(1L);
                        }
                        List<String> updateCapKeys = Json.decodeValue(quotaStr, new TypeReference<List<String>>() {
                        });
                        inode.getXAttrMap().put(QUOTA_KEY, Json.encode(updateCapKeys));
                        inode.getInodeData().clear();
                        metaData.tmpInodeStr = Json.encode(inode);
                    }
                    return updateFsMeta(key, metaData, nodeList1, request, res, snapshotLink, true);
                }, currentSnapshotMark, snapshotLink, true);
    }

    /**
     * 获取指定版本的对象元数据；被业务性质功能调用，不受QoS限制
     * 桶快照版：先去currentSnapshotMark开头的key中查找对象，如果不存在则去snapshotLink开头的key中查找
     *
     * @param currentSnapshotMark 当前桶最新的快照标记
     * @param snapshotLink        当前桶已创建快照的链接
     * @return 元数据
     */
    public static Mono<MetaData> getObjectMetaVersionUnlimited(String bucket, String object, String versionId,
                                                               List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String currentSnapshotMark, String snapshotLink) {
        return getObjectMetaVersion(bucket, object, versionId, nodeList, request,
                (key, metaData, nodeList1, res) -> updateMetaData(key, metaData, nodeList1, request, res, snapshotLink, true), currentSnapshotMark, snapshotLink, true);
    }

    /***
     * getObject Meta 不进行恢复
     */
    public static Mono<MetaData> getObjectMetaVersionUnlimitedNotRecover(String bucket, String object, String versionId,
                                                                         List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String currentSnapshotMark, String snapshotLink) {

        Function4<String, MetaData, List<Tuple3<String, String, String>>,
                Tuple2<PayloadMetaType, MetaData>[], Mono<Integer>> repairFunction = (key, metaData, nodeList1, res) -> Mono.just(1);

        String vnode = nodeList.get(0).var3;
        //版本号未指定
        if (StringUtils.isEmpty(versionId)) {
            String metaKey = ROCKS_SPECIAL_KEY + Utils.getMetaDataKey(vnode, bucket, object, currentSnapshotMark);
            return ECUtils.getRocksKey(StoragePoolFactory.getMetaStoragePool(bucket), metaKey, GET_OBJECT_META,
                            NOT_FOUND_META, ERROR_META, nodeList, request, currentSnapshotMark, snapshotLink)
                    .flatMap(tuple3 -> {
                        if (!tuple3.var1 || tuple3.var2.equals(NOT_FOUND_META)) {
                            MetaData metaData = (MetaData) tuple3.var2;
                            return Mono.just(metaData);
                        } else {
                            return getObjectMetaVersionUnlimitedNotRecover(bucket, object, tuple3.var2.versionId, nodeList, request, currentSnapshotMark, snapshotLink);
                        }
                    });
        }

        String versionKey = Utils.getVersionMetaDataKey(vnode, bucket, object, versionId, currentSnapshotMark);
        MetaData deleteMark = new MetaData()
                .setBucket(bucket)
                .setKey(object)
                .setDeleteMark(true)
                .setVersionId(versionId)
                .setVersionNum(VersionUtil.getLastVersionNum(""))
                .setStamp(String.valueOf(System.currentTimeMillis()))
                .setSnapshotMark(currentSnapshotMark);

        Comparator<MetaData> comparator = (o1, o2) -> {
            // 删除标记没有syncStamp
            if (!VersionUtil.isMultiCluster || o1.syncStamp == null || o2.syncStamp == null || o1.syncStamp.equals(o2.syncStamp)) {
                if (snapshotLink != null && !Objects.equals(o1.snapshotMark, o2.snapshotMark)) {
                    // 创建了快照，且o1，o2不在同一个快照下，此时不能使用versionNum大小去判断数据的新旧
                    // currentSnapshotMark中数据始终是最新的
                    return Objects.equals(o1.snapshotMark, currentSnapshotMark) ? 1 : -1;
                }
                if ((o1.discard || o2.discard) && (o1.shardingStamp != null || o2.shardingStamp != null)) {
                    if (o1.shardingStamp == null) {
                        return -1;
                    }
                    if (o2.shardingStamp == null) {
                        return 1;
                    }
                    int cmp = o1.shardingStamp.compareTo(o2.shardingStamp);
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                return o1.versionNum.compareTo(o2.versionNum);
            } else {
                return o1.syncStamp.compareTo(o2.syncStamp);
            }
        };

        return ECUtils.getRocksKey(StoragePoolFactory.getMetaStoragePool(bucket), versionKey, MetaData.class,
                        GET_OBJECT_META, NOT_FOUND_META, ERROR_META, deleteMark,
                        MetaData::getVersionNum, comparator, repairFunction, nodeList, request, currentSnapshotMark, snapshotLink)
                .flatMap(tuple2 -> Mono.just(tuple2.var1));
    }


    //获取指定版本的对象元数据；被数据恢复性质功能调用，需受QoS限制
    public static Mono<MetaData> getObjectMetaVersion(String bucket, String object, String versionId,
                                                      List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {

        return getObjectMetaVersion(bucket, object, versionId, nodeList, request, (String) null, null);
    }

    /**
     * 获取指定版本的对象元数据；被数据恢复性质功能调用，需受QoS限制
     * 桶快照版：先去currentSnapshotMark开头的key中查找对象，如果不存在则去snapshotLink开头的key中查找
     *
     * @param currentSnapshotMark 当前桶最新的快照标记（如果获取指定快照下的对象，则该值为MetaData.snapshotMark,并将snapshotLink参数置为空）
     * @param snapshotLink        当前桶已创建快照的链接
     * @param strongConsistency   是否强一致性，即要求update操作后所有的在线节点都返回相同的结果，否则返回error
     * @return 元数据
     */
    public static Mono<MetaData> getObjectMetaVersion(String bucket, String object, String versionId,
                                                      List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String currentSnapshotMark, String snapshotLink, boolean... strongConsistency) {
        // 如果request不为null，即说明为前台请求，则不予限流；
        if (null != request) {
            return getObjectMetaVersion(bucket, object, versionId, nodeList, request,
                    (key, metaData, nodeList1, res) -> updateMetaData(key, metaData, nodeList1, request, res, snapshotLink, true), currentSnapshotMark, snapshotLink, true);
        }

        boolean isStrongConsistency = strongConsistency.length > 0 && strongConsistency[0];

        return getObjectMetaVersion(bucket, object, versionId, nodeList, request,
                (key, metaData, nodeList1, res) -> updateMetaData(key, metaData, nodeList1, request, res, snapshotLink).map(r -> isStrongConsistency && r == 2 ? 0 : r),
                currentSnapshotMark, snapshotLink);
    }

    public static Mono<MetaData> getObjectMetaVersionFsQuotaRecover(String bucket, String object, String versionId,
                                                                    List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String currentSnapshotMark, String snapshotLink, String quotaStr, boolean needCheck, boolean... strongConsistency) {
        boolean isStrongConsistency = strongConsistency.length > 0 && strongConsistency[0];
        return getObjectMetaVersion(bucket, object, versionId, nodeList, request,
                (key, metaData, nodeList1, res) -> {
                    if (StringUtils.isNotBlank(quotaStr) && !needCheck) {
                        addQuotaInfoToMetaData(metaData, quotaStr);
                    }
                    return Mono.just(needCheck)
                            .flatMap(b -> {
                                if (b && StringUtils.isNotBlank(quotaStr)) {
                                    List<String> updateCapKeys = Json.decodeValue(quotaStr, new TypeReference<List<String>>() {
                                    });
                                    Tuple2<Boolean, List<String>> tuple2 = new Tuple2<>(true, updateCapKeys);
                                    return FSQuotaUtils.existQuotaInfoHasScanComplete0(metaData.getBucket(), metaData.key, Long.parseLong(metaData.stamp), Collections.singletonList(0), Collections.singletonList(0), tuple2)
                                            .flatMap(t2 -> {
                                                if (t2.var1) {
                                                    addQuotaInfoToMetaData(metaData, Json.encode(t2.var2));
                                                }
                                                return updateMetaData(key, metaData, nodeList1, request, res, snapshotLink).map(r -> isStrongConsistency && r == 2 ? 0 : r);
                                            });
                                }
                                return updateMetaData(key, metaData, nodeList1, request, res, snapshotLink).map(r -> isStrongConsistency && r == 2 ? 0 : r);
                            });

                },
                currentSnapshotMark, snapshotLink, true);
    }

    //获取指定版本的对象元数据 TODO 生命周期专用，QoS单独判定权重
    public static Mono<MetaData> getLifecycleMetaVersion(String bucket, String object, String versionId,
                                                         List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String currentSnapshotMark) {
        return getObjectMetaVersion(bucket, object, versionId, nodeList, request,
                (key, metaData, nodeList1, res) -> lifecycleMetaData(key, metaData, nodeList1, request, res), currentSnapshotMark, null);
    }

    public static Mono<Tuple2<MetaData, Tuple2<PayloadMetaType, MetaData>[]>> getObjectMetaVersionResOnlyRead(String bucket, String object, String versionId,
                                                                                                              List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {

        // 如果前台请求不为空，说明为前端业务流量，不需要限流
        if (null != request) {
            return getObjectMetaVersionRes(bucket, object, versionId, nodeList, request, (key, metaData, nodeList1, res) -> Mono.just(1), null, null, true);
        }

        return getObjectMetaVersionRes(bucket, object, versionId, nodeList, request, (key, metaData, nodeList1, res) -> Mono.just(1), null, null);
    }

    public static Mono<Tuple2<MetaData, Tuple2<PayloadMetaType, MetaData>[]>> getObjectMetaVersionResOnlyRead(String bucket, String object, String versionId,
                                                                                                              List<Tuple3<String, String, String>> nodeList, MsHttpRequest request,
                                                                                                              String currentSnapshotMark, String snapshotLink) {

        // 如果前台请求不为空，说明为前端业务流量，不需要限流
        if (null != request) {
            return getObjectMetaVersionRes(bucket, object, versionId, nodeList, request, (key, metaData, nodeList1, res) -> Mono.just(1), currentSnapshotMark, snapshotLink, true);
        }

        return getObjectMetaVersionRes(bucket, object, versionId, nodeList, request, (key, metaData, nodeList1, res) -> Mono.just(1), currentSnapshotMark, snapshotLink);
    }

    //获取指定版本的对象元数据；被数据恢复性质功能调用，需受QoS限制
    public static Mono<Tuple2<MetaData, Tuple2<PayloadMetaType, MetaData>[]>> getObjectMetaVersionRes(String bucket, String object, String versionId,
                                                                                                      List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        return getObjectMetaVersionRes(bucket, object, versionId, nodeList, request,
                (key, metaData, nodeList1, res) -> updateMetaData(key, metaData, nodeList1, request, res), null, null);
    }

    //获取指定版本的对象元数据；被数据恢复性质功能调用，需受QoS限制--桶快照版
    public static Mono<Tuple2<MetaData, Tuple2<PayloadMetaType, MetaData>[]>> getObjectMetaVersionRes(String bucket, String object, String versionId,
                                                                                                      List<Tuple3<String, String, String>> nodeList, MsHttpRequest request,
                                                                                                      String currentSnapshotMark, String snapshotLink) {
        return getObjectMetaVersionRes(bucket, object, versionId, nodeList, request,
                (key, metaData, nodeList1, res) -> updateMetaData(key, metaData, nodeList1, request, res, snapshotLink), currentSnapshotMark, snapshotLink);
    }

    //获取指定版本的对象元数据；被业务性质功能调用，不受QoS限制
    public static Mono<Tuple2<MetaData, Tuple2<PayloadMetaType, MetaData>[]>> getObjectMetaVersionResUnlimited(String bucket, String object, String versionId,
                                                                                                               List<Tuple3<String, String, String>> nodeList, MsHttpRequest request,
                                                                                                               String currentSnapshotMark, String snapshotLink) {
        return getObjectMetaVersionRes(bucket, object, versionId, nodeList, request,
                (key, metaData, nodeList1, res) -> updateMetaData(key, metaData, nodeList1, request, res, snapshotLink, true), currentSnapshotMark, snapshotLink, true);
    }

    //获取指定md5在dataPool的重删引用信息
    public static Mono<DedupMeta> getDeduplicateMeta(String etag, String storage, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        return getDeduplicateMeta(etag, storage, null, nodeList, request,
                (key, meta, nodeList1, res) -> updateDedupMetaData(key, meta, nodeList1, request, res));
    }

    //获取指定md5在dataPool的重删引用信息
    public static Mono<DedupMeta> getDeduplicateMeta(String etag, String storage, String realKey, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        return getDeduplicateMeta(etag, storage, realKey, nodeList, request,
                (key, meta, nodeList1, res) -> updateDedupMetaData(key, meta, nodeList1, request, res));
    }

    public static Mono<DedupMeta> getDeduplicateMeta(String etag, String storage, String realKey, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request,
                                                     Function4<String, DedupMeta, List<Tuple3<String, String, String>>,
                                                             Tuple2<PayloadMetaType, DedupMeta>[], Mono<Integer>> repairFunction) {
        return getObjectDeduplicateRes(etag, storage, realKey, nodeList, request, repairFunction)
                .flatMap(tuple2 -> Mono.just(tuple2.var1));
    }


    public static Mono<Tuple2<DedupMeta, Tuple2<PayloadMetaType, DedupMeta>[]>> getObjectDeduplicateRes(
            String etag, String storage, String realKey, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request,
            Function4<String, DedupMeta, List<Tuple3<String, String, String>>, Tuple2<PayloadMetaType, DedupMeta>[], Mono<Integer>> repairFunction) {

        String vnode = nodeList.get(0).var3;
        String dedupKey = Utils.getDeduplicatMetaKey(vnode, etag, storage);
        if (StringUtils.isNotEmpty(realKey)) {
            dedupKey = realKey;

        }

        DedupMeta deleteMark = new DedupMeta()
                .setEtag(etag)
                .setStorage(storage)
                .setDeleteMark(true)
                .setVersionNum(VersionUtil.getLastVersionNum(""));
        return ECUtils.getRocksKey(StoragePoolFactory.getMetaStoragePool(etag), dedupKey, DedupMeta.class,
                GET_DEDUPLICATE_META, NOT_FOUND_DEDUP_META, ERROR_DEDUP_META, deleteMark,
                DedupMeta::getVersionNum, Comparator.comparing(a -> a.versionNum), repairFunction, nodeList, request);
    }

    /**
     * 生命周期专用，QoS单独判定权重
     **/
    public static Mono<Tuple2<MetaData, Tuple2<PayloadMetaType, MetaData>[]>> getLifecycleMetaVersionRes(String bucket, String object, String versionId,
                                                                                                         List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String currentSnapshotMark, String snapshotLink) {
        return getObjectMetaVersionRes(bucket, object, versionId, nodeList, request,
                (key, metaData, nodeList1, res) -> lifecycleMetaData(key, metaData, nodeList1, request, res), currentSnapshotMark, snapshotLink);
    }

    public static Mono<Tuple2<MetaData, Tuple2<PayloadMetaType, MetaData>[]>> getLifecycleMetaVersionResOnlyRead(String bucket, String object, String versionId,
                                                                                                                 List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String snapshotMark) {
        return getObjectMetaVersionRes(bucket, object, versionId, nodeList, request,
                (key, metaData, nodeList1, res) -> Mono.just(1), snapshotMark, null);
    }

    public static Mono<MetaData> getObjectMetaVersion(String bucket, String object, String versionId,
                                                      List<Tuple3<String, String, String>> nodeList, MsHttpRequest request,
                                                      Function4<String, MetaData, List<Tuple3<String, String, String>>,
                                                              Tuple2<PayloadMetaType, MetaData>[], Mono<Integer>> repairFunction, boolean... isUnLimit) {

        return getObjectMetaVersion(bucket, object, versionId, nodeList, request, repairFunction, null, null, isUnLimit);
    }

    public static Mono<MetaData> getObjectMetaVersion(String bucket, String object, String versionId,
                                                      List<Tuple3<String, String, String>> nodeList, MsHttpRequest request,
                                                      Function4<String, MetaData, List<Tuple3<String, String, String>>,
                                                              Tuple2<PayloadMetaType, MetaData>[], Mono<Integer>> repairFunction,
                                                      String currentSnapshotMark, String snapshotLink, boolean... isUnLimit) {
        if (isUnLimit.length > 0) {
            return getObjectMetaVersionRes(bucket, object, versionId, nodeList, request, repairFunction, currentSnapshotMark, snapshotLink, true)
                    .flatMap(tuple2 -> Mono.just(tuple2.var1));
        }

        return getObjectMetaVersionRes(bucket, object, versionId, nodeList, request, repairFunction, currentSnapshotMark, snapshotLink)
                .flatMap(tuple2 -> Mono.just(tuple2.var1));
    }

    public static Mono<Tuple2<MetaData, Tuple2<PayloadMetaType, MetaData>[]>> getObjectMetaVersionRes(
            String bucket, String object, String versionId, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request,
            Function4<String, MetaData, List<Tuple3<String, String, String>>, Tuple2<PayloadMetaType, MetaData>[], Mono<Integer>> repairFunction,
            String currentSnapshotMark, String snapshotLink, boolean... isUnLimit) {
        if (StringUtils.isEmpty(versionId)) {
            if (isUnLimit.length > 0) {
                return getObjectMetaRes(bucket, object, nodeList, request, currentSnapshotMark, snapshotLink, true);
            } else {
                return getObjectMetaRes(bucket, object, nodeList, request, currentSnapshotMark, snapshotLink);
            }
        }
        String vnode = nodeList.get(0).var3;
        String versionKey = Utils.getVersionMetaDataKey(vnode, bucket, object, versionId, currentSnapshotMark);

        MetaData deleteMark = new MetaData()
                .setBucket(bucket)
                .setKey(object)
                .setDeleteMark(true)
                .setVersionId(versionId)
                .setVersionNum(VersionUtil.getLastVersionNum(""))
                .setStamp(String.valueOf(System.currentTimeMillis()))
                .setSnapshotMark(currentSnapshotMark);

        Comparator<MetaData> comparator = (o1, o2) -> {
            // 删除标记没有syncStamp
            if (!VersionUtil.isMultiCluster || o1.syncStamp == null || o2.syncStamp == null || o1.syncStamp.equals(o2.syncStamp)) {
                if (snapshotLink != null && !Objects.equals(o1.snapshotMark, o2.snapshotMark)) {
                    // 创建了快照，且o1，o2不在同一个快照下，此时不能使用versionNum大小去判断数据的新旧
                    // currentSnapshotMark中数据始终是最新的
                    return Objects.equals(o1.snapshotMark, currentSnapshotMark) ? 1 : -1;
                }
                if ((o1.discard || o2.discard) && (o1.shardingStamp != null || o2.shardingStamp != null)) {
                    if (o1.shardingStamp == null) {
                        return -1;
                    }
                    if (o2.shardingStamp == null) {
                        return 1;
                    }
                    int cmp = o1.shardingStamp.compareTo(o2.shardingStamp);
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                return o1.versionNum.compareTo(o2.versionNum);
            } else {
                return o1.syncStamp.compareTo(o2.syncStamp);
            }
        };

        return ECUtils.getRocksKey(StoragePoolFactory.getMetaStoragePool(bucket), versionKey, MetaData.class,
                GET_OBJECT_META, NOT_FOUND_META, ERROR_META, deleteMark,
                MetaData::getVersionNum, comparator, repairFunction, nodeList, request, currentSnapshotMark, snapshotLink);
    }

//    public static Mono<BucketInfo> getBucketInfo(String bucket) {
//        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
//        return storagePool.mapToNodeInfo(storagePool.getBucketVnodeId(bucket))
//                .flatMap(nodeList -> ECUtils.getRocksKey(storagePool, BucketInfo.getBucketKey(nodeList.get(0).var3, bucket)
//                        , BucketInfo.class, GET_BUCKET_INFO, NOT_FOUND_BUCKET_INFO, ERROR_BUCKET_INFO
//                        , null, BucketInfo::getVersionNum, Comparator.comparingLong(info -> Long.parseLong(info.getBucketStorage())),
//                        (a, b, c, d) -> Mono.just(1), nodeList, null))
//                .flatMap(tuple -> Mono.just(tuple.var1));
//    }

    public static Flux<BucketInfo> getBucketInfoFlux(String bucket) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        List<String> bucketVnodeList = storagePool.getBucketVnodeList(bucket);
        return Flux.fromIterable(bucketVnodeList)
                .flatMap(storagePool::mapToNodeInfo)
                .flatMap(nodeList -> ECUtils.getRocksKey(storagePool, BucketInfo.getBucketKey(nodeList.get(0).var3, bucket)
                        , BucketInfo.class, GET_BUCKET_INFO, NOT_FOUND_BUCKET_INFO, ERROR_BUCKET_INFO
                        , null, BucketInfo::getVersionNum, Comparator.comparingLong(info -> Long.parseLong(info.getObjectNum())),
                        (a, b, c, d) -> Mono.just(1), nodeList, null))
                .flatMap(tuple -> Mono.just(tuple.var1))
                .doOnNext(bucketInfo -> {
                    if (ERROR_BUCKET_INFO.equals(bucketInfo)) {
                        throw new MsException(UNKNOWN_ERROR, "get bucket " + bucket + " storage info error!");
                    }
                });
    }

    public static Mono<BucketInfo> reduceBucketInfo(String bucket) {
        return getBucketInfoFlux(bucket)
                .reduce((bucketInfo, bucketInfo2) -> {
                    if (bucketInfo2.equals(NOT_FOUND_BUCKET_INFO) && bucketInfo.equals(NOT_FOUND_BUCKET_INFO)) {
                        return NOT_FOUND_BUCKET_INFO;
                    }
                    if (bucketInfo2.equals(NOT_FOUND_BUCKET_INFO)) {
                        return bucketInfo;
                    }
                    if (bucketInfo.equals(NOT_FOUND_BUCKET_INFO)) {
                        return bucketInfo2;
                    }
                    String totalObjNum = String.valueOf(Long.parseLong(bucketInfo.getObjectNum()) + Long.parseLong(bucketInfo2.getObjectNum()));
                    String totalStorage = String.valueOf(Long.parseLong(bucketInfo.getBucketStorage()) + Long.parseLong(bucketInfo2.getBucketStorage()));
                    String versionNum = bucketInfo.versionNum.compareTo(bucketInfo2.versionNum) < 0 ? bucketInfo2.versionNum : bucketInfo.versionNum;
                    BucketInfo info = new BucketInfo();
                    info.setBucketName(bucketInfo.getBucketName());
                    info.setObjectNum(totalObjNum);
                    info.setBucketStorage(totalStorage);
                    info.setVersionNum(versionNum);
                    return info;
                });
    }

    public static Mono<BucketInfo> getBucketInfo(String bucket, String vnode) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        return storagePool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> ECUtils.getRocksKey(storagePool, BucketInfo.getBucketKey(nodeList.get(0).var3, bucket)
                        , BucketInfo.class, GET_BUCKET_INFO, NOT_FOUND_BUCKET_INFO, ERROR_BUCKET_INFO
                        , null, BucketInfo::getVersionNum, Comparator.comparingLong(info -> Long.parseLong(info.getObjectNum())),
                        (a, b, c, d) -> Mono.just(1), nodeList, null))
                .flatMap(tuple -> Mono.just(tuple.var1));
    }

    public static Mono<Integer> updateBucketInfo(String key, BucketInfo bucketInfo, List<Tuple3<String, String, String>> nodeList, Boolean isSyncSwitchOff) {
        if (bucketInfo == null) {
            return Mono.just(1);
        }
        String versionNum = bucketInfo.isAvailable() ? bucketInfo.getVersionNum() : "0";
        Map<String, String> oldVersionNum = new HashMap<>();
        for (Tuple3<String, String, String> tuple3 : nodeList) {
            oldVersionNum.put(tuple3.var1, versionNum);
        }
        bucketInfo.setVersionNum(VersionUtil.getVersionNum(isSyncSwitchOff));
        return updateRocksKey(StoragePoolFactory.getMetaStoragePool(bucketInfo.bucketName), oldVersionNum,
                key, Json.encode(bucketInfo), PUT_BUCKET_INFO, ERROR_PUT_BUCKET_INFO, nodeList, null);
    }


    /**
     * getObjectMeta修复时使用。
     */
    public static Mono<Integer> updateMetaData(String key, MetaData metaData, List<Tuple3<String, String, String>> nodeList,
                                               MsHttpRequest request, Tuple2<PayloadMetaType, MetaData>[] res, boolean... isUnLimit) {
        return updateMetaData(key, metaData, nodeList, request, res, null, isUnLimit);
    }

    /**
     * getObjectMeta修复时使用。
     * 桶快照版--修复时会根据snapshotLink将快照创建前上传的同名对象进行逻辑删除
     */
    public static Mono<Integer> updateMetaData(String key, MetaData metaData, List<Tuple3<String, String, String>> nodeList,
                                               MsHttpRequest request, Tuple2<PayloadMetaType, MetaData>[] res, String snapshotLink, boolean... isUnLimit) {

        PayloadMetaType fixType = UPDATE_OBJECT_META;
        if (isUnLimit.length > 0) {
            fixType = UPDATE_OBJECT_META_UNLIMIT;
        }

        //updateMeta的依据
        Map<String, String> oldVersionNum = new HashMap<>();
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple2<PayloadMetaType, MetaData> tuple2 = res[i];
            if (null != tuple2) {
                if (NOT_FOUND.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                } else if (SUCCESS.equals(tuple2.var1)) {
                    if (!Objects.equals(tuple2.var2.snapshotMark, metaData.snapshotMark)) {
                        // get到的数据和待更新数据不是同一快照下，则说明当前快照下元数据为not found
                        oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                    } else {
                        oldVersionNum.put(nodeList.get(i).var1, tuple2.var2.versionNum);
                    }
                } else if (ERROR.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_ERROR.name());
                }
            }
        }

        PayloadMetaType finalFixType = fixType;
        return Mono.just(metaData.inode > 0)
                .flatMap(b -> {
                    if (!b) {
                        // 对象的meta修复时versionNum从(bucket+obj).hashCode() % 1024 % ipNum 获取
                        return VersionUtil.getLastVersionNum(metaData.getVersionNum(), metaData.bucket, metaData.key);
                    } else {
                        // 文件的meta修复时versionNum从nfs vnode主获取
                        return Node.getInstance().getVersion(metaData.inode, metaData.bucket, false, metaData.getVersionNum())
                                .map(Inode::getVersionNum);
                    }
                })
                .doOnNext(lastVersionNum -> {
                    metaData.setVersionNum(lastVersionNum);
                    for (Tuple2<PayloadMetaType, MetaData> re : res) {
                        if (re.var1.equals(SUCCESS)) {
                            re.var2.setVersionNum(lastVersionNum);
                        }
                    }
                })
                .flatMap(lastVersionNum -> MsObjVersionUtils.versionStatusReactive(metaData.bucket).flatMap(status -> updateRocksKey(StoragePoolFactory.getMetaStoragePool(metaData.bucket),
                        oldVersionNum, key, Json.encode(metaData), finalFixType, ERROR_PUT_OBJECT_META, nodeList, request, status, snapshotLink)));

    }

    /**
     * 文件数据在deleteInode调用的getFsMetaVerUnlimited修复时使用。
     */
    public static Mono<Integer> updateFsMeta(String key, MetaData metaData, List<Tuple3<String, String, String>> nodeList,
                                             MsHttpRequest request, Tuple2<PayloadMetaType, MetaData>[] res, String snapshotLink, boolean... isUnLimit) {
        PayloadMetaType fixType = UPDATE_OBJECT_META;
        if (isUnLimit.length > 0) {
            fixType = UPDATE_OBJECT_META_UNLIMIT;
        }

        //updateMeta的依据
        Map<String, String> oldVersionNum = new HashMap<>();
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple2<PayloadMetaType, MetaData> tuple2 = res[i];
            if (null != tuple2) {
                if (NOT_FOUND.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                } else if (SUCCESS.equals(tuple2.var1)) {
                    if (!Objects.equals(tuple2.var2.snapshotMark, metaData.snapshotMark)) {
                        // get到的数据和待更新数据不是同一快照下，则说明当前快照下元数据为not found
                        oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                    } else {
                        oldVersionNum.put(nodeList.get(i).var1, tuple2.var2.versionNum);
                    }
                } else if (ERROR.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_ERROR.name());
                }
            }
        }

        PayloadMetaType finalFixType = fixType;
        return Mono.just(metaData.inode > 0)
                .flatMap(b -> {
                    if (!b) {
                        // 对象的meta修复时versionNum从(bucket+obj).hashCode() % 1024 % ipNum 获取
                        return VersionUtil.getLastVersionNum(metaData.getVersionNum(), metaData.bucket, metaData.key);
                    } else {
                        // 删除时的vnode主即当前节点本身，因此直接调用lastVersionNum生成版本号
                        return Mono.just(VersionUtil.getLastVersionNum(metaData.getVersionNum()));
                    }
                })
                .doOnNext(lastVersionNum -> {
                    metaData.setVersionNum(lastVersionNum);
                    for (Tuple2<PayloadMetaType, MetaData> re : res) {
                        if (re.var1.equals(SUCCESS)) {
                            re.var2.setVersionNum(lastVersionNum);
                        }
                    }
                })
                .flatMap(lastVersionNum -> MsObjVersionUtils.versionStatusReactive(metaData.bucket).flatMap(status -> updateRocksKey(StoragePoolFactory.getMetaStoragePool(metaData.bucket),
                        oldVersionNum, key, Json.encode(metaData), finalFixType, ERROR_PUT_OBJECT_META, nodeList, request, status, snapshotLink)));
    }

    /**
     * getObjectMeta修复时使用。 TODO 生命周期专用，QoS单独判定权重
     */
    public static Mono<Integer> lifecycleMetaData(String key, MetaData metaData, List<Tuple3<String, String, String>> nodeList,
                                                  MsHttpRequest request, Tuple2<PayloadMetaType, MetaData>[] res) {
        //updateMeta的依据
        Map<String, String> oldVersionNum = new HashMap<>();
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple2<PayloadMetaType, MetaData> tuple2 = res[i];
            if (null != tuple2) {
                if (NOT_FOUND.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                } else if (SUCCESS.equals(tuple2.var1)) {
                    if (!Objects.equals(tuple2.var2.snapshotMark, metaData.snapshotMark)) {
                        // get到的数据和待更新数据不是同一快照下，则说明当前快照下元数据为not found
                        oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                    } else {
                        oldVersionNum.put(nodeList.get(i).var1, tuple2.var2.versionNum);
                    }
                } else if (ERROR.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_ERROR.name());
                }
            }
        }

        return VersionUtil.getLastVersionNum(metaData.getVersionNum(), metaData.bucket, metaData.key)
                .doOnNext(lastVersionNum -> {
                    metaData.setVersionNum(lastVersionNum);
                    for (Tuple2<PayloadMetaType, MetaData> re : res) {
                        if (re.var1.equals(SUCCESS)) {
                            re.var2.setVersionNum(lastVersionNum);
                        }
                    }
                })
                .flatMap(lastVersionNum -> MsObjVersionUtils.versionStatusReactive(metaData.bucket).flatMap(status -> updateRocksKey(StoragePoolFactory.getMetaStoragePool(metaData.bucket),
                        oldVersionNum, key, Json.encode(metaData), LIFECYCLE_OBJECT_META, ERROR_PUT_OBJECT_META, nodeList, request, status)));
    }

    /**
     * getObjectMeta修复时使用。
     */
    public static Mono<Integer> updateDedupMetaData(String key, DedupMeta metaData, List<Tuple3<String, String, String>> nodeList,
                                                    MsHttpRequest request, Tuple2<PayloadMetaType, DedupMeta>[] res) {
        //updateMeta的依据
        Map<String, String> oldVersionNum = new HashMap<>();
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple2<PayloadMetaType, DedupMeta> tuple2 = res[i];
            if (null != tuple2) {
                if (NOT_FOUND.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                } else if (SUCCESS.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, tuple2.var2.versionNum);
                } else if (ERROR.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_ERROR.name());
                }
            }
        }

        String lastVersionNum = VersionUtil.getLastVersionNum(metaData.getVersionNum());
//        log.info("bbbbb {}", key);
        metaData.setVersionNum(lastVersionNum);
        for (Tuple2<PayloadMetaType, DedupMeta> re : res) {
            if (re.var1.equals(SUCCESS)) {
                re.var2.setVersionNum(lastVersionNum);
            }
        }

        if (metaData.equals(NOT_FOUND_DEDUP_META)) {
            return Mono.just(1);
        }
        return updateRocksKey(StoragePoolFactory.getMetaStoragePool(metaData.getEtag()), oldVersionNum, key, Json.encode(metaData)
                , UPDATE_DEDUPLICATE_META, ERROR_PUT_DEDUPLICATE_META, nodeList, request, "NULL");

    }


    public static Mono<Integer> updateMetaDataAcl(String key, MetaData metaData, List<Tuple3<String, String, String>> nodeList,
                                                  MsHttpRequest request, Tuple2<PayloadMetaType, MetaData>[] res) {

        if (metaData.inode > 0) {
            return Node.getInstance().updateObjACL(metaData.bucket, metaData.inode, metaData.getObjectAcl())
                    .doOnNext(i -> {
                        if (null != i.getObjName()) {
                            InodeUtils.updateParentDirTime(i.getObjName(), metaData.bucket);
                        }
                    })
                    .map(inode -> {
                        return InodeUtils.isError(inode) ? 0 : 1;
                    });
        }

        return VersionUtil.getLastVersionNum(metaData.versionNum, metaData.bucket, metaData.key)
                .flatMap(versionNum -> updateMetaDataAcl(key, metaData, nodeList, request, res, versionNum));
    }

    public static Mono<Tuple2<Integer, Inode>> updateMetaDataAcl(String key, MetaData metaData, List<Tuple3<String, String, String>> nodeList,
                                                                 MsHttpRequest request, Tuple2<PayloadMetaType, MetaData>[] res, Inode inode) {
        return Node.getInstance().getVersion(inode.getNodeId(), inode.getBucket(), false, metaData.getVersionNum())
                .flatMap(versionInode -> {
                    if (InodeUtils.isError(versionInode)) {
                        log.info("get inode version {} fail", Json.encode(inode));
                        return Mono.just(new Tuple2<>(0, ERROR_INODE));
                    } else {
                        // 避免oldVersionNum与更新的versionNum相等导致metaData更新失败
                        String updateVersionNum = versionInode.getVersionNum();
                        if (metaData.getVersionNum().compareTo(updateVersionNum) >= 0) {
                            updateVersionNum += "0";
                        }
                        inode.setVersionNum(updateVersionNum);
                        metaData.setTmpInodeStr(Json.encode(inode));
                        return updateMetaDataAclInode(key, metaData, nodeList, request, res, updateVersionNum);
                    }
                });
    }

    /**
     * 设置对象ACL时更新元数据
     */
    private static Mono<Integer> updateMetaDataAcl(String key, MetaData metaData, List<Tuple3<String, String, String>> nodeList,
                                                   MsHttpRequest request, Tuple2<PayloadMetaType, MetaData>[] res, String versionNum) {

        //updateMeta的依据
        Map<String, String> oldVersionNum = new HashMap<>();
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple2<PayloadMetaType, MetaData> tuple2 = res[i];
            if (null != tuple2) {
                if (NOT_FOUND.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                } else if (SUCCESS.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, tuple2.var2.versionNum);
                } else if (ERROR.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_ERROR.name());
                }
            }
        }

        //实际写入db的metaData
        metaData.setVersionNum(versionNum);
        for (Tuple2<PayloadMetaType, MetaData> re : res) {
            if (re.var1.equals(SUCCESS)) {
                re.var2.setVersionNum(versionNum);
            }
        }

        return MsObjVersionUtils.versionStatusReactive(metaData.bucket)
                .flatMap(status -> updateRocksKey(StoragePoolFactory.getMetaStoragePool(metaData.bucket), oldVersionNum, key, Json.encode(metaData), UPDATE_OBJECT_META, ERROR_PUT_OBJECT_META,
                        nodeList, request, status));
    }

    private static Mono<Tuple2<Integer, Inode>> updateMetaDataAclInode(String key, MetaData metaData, List<Tuple3<String, String, String>> nodeList,
                                                                       MsHttpRequest request, Tuple2<PayloadMetaType, MetaData>[] res, String versionNum) {

        //updateMeta的依据
        Map<String, String> oldVersionNum = new HashMap<>();
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple2<PayloadMetaType, MetaData> tuple2 = res[i];
            if (null != tuple2) {
                if (NOT_FOUND.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                } else if (SUCCESS.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, tuple2.var2.versionNum);
                } else if (ERROR.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_ERROR.name());
                }
            }
        }

        //实际写入db的metaData
        metaData.setVersionNum(versionNum);
        for (Tuple2<PayloadMetaType, MetaData> re : res) {
            if (re.var1.equals(SUCCESS)) {
                re.var2.setVersionNum(versionNum);
            }
        }

        return MsObjVersionUtils.versionStatusReactive(metaData.bucket)
                .flatMap(status -> updateRocksKeyInode(StoragePoolFactory.getMetaStoragePool(metaData.bucket), oldVersionNum, key, Json.encode(metaData), UPDATE_OBJECT_META_UNLIMIT, ERROR_PUT_OBJECT_META,
                        nodeList, request, status));
    }


    public static Mono<Integer> lifecycleMetaDataAcl(String key, MetaData metaData, List<Tuple3<String, String, String>> nodeList,
                                                     MsHttpRequest request, Tuple2<PayloadMetaType, MetaData>[] res) {
        return VersionUtil.getLastVersionNum(metaData.versionNum, metaData.bucket, metaData.key)
                .flatMap(versionNum -> lifecycleMetaDataAcl(key, metaData, nodeList, request, res, versionNum));
    }

    /**
     * 设置对象ACL时更新元数据 TODO Lifecycle专用，QoS单独判定权重
     */
    private static Mono<Integer> lifecycleMetaDataAcl(String key, MetaData metaData, List<Tuple3<String, String, String>> nodeList,
                                                      MsHttpRequest request, Tuple2<PayloadMetaType, MetaData>[] res, String versionNum) {

        //updateMeta的依据
        Map<String, String> oldVersionNum = new HashMap<>();
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple2<PayloadMetaType, MetaData> tuple2 = res[i];
            if (null != tuple2) {
                if (NOT_FOUND.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                } else if (SUCCESS.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, tuple2.var2.versionNum);
                } else if (ERROR.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_ERROR.name());
                }
            }
        }

        //实际写入db的metaData
        metaData.setVersionNum(versionNum);
        for (Tuple2<PayloadMetaType, MetaData> re : res) {
            if (re.var1.equals(SUCCESS)) {
                re.var2.setVersionNum(versionNum);
            }
        }

        return MsObjVersionUtils.versionStatusReactive(metaData.bucket)
                .flatMap(status -> updateRocksKey(StoragePoolFactory.getMetaStoragePool(metaData.bucket), oldVersionNum, key, Json.encode(metaData), LIFECYCLE_OBJECT_META, ERROR_PUT_OBJECT_META,
                        nodeList, request, status));
    }

    /**
     * getPartInfo修复时使用。
     */
    public static Mono<Integer> updatePartInfo(String key, PartInfo partInfo, List<Tuple3<String, String, String>> nodeList,
                                               MsHttpRequest request, Tuple2<PayloadMetaType, PartInfo>[] res, String snapshotLink) {
        //updateMeta的依据
        Map<String, String> oldVersionNum = new HashMap<>();
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple2<PayloadMetaType, PartInfo> tuple2 = res[i];
            if (null != tuple2) {
                if (NOT_FOUND.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                } else if (SUCCESS.equals(tuple2.var1)) {
                    if (!Objects.equals(tuple2.var2.snapshotMark, partInfo.snapshotMark)) {
                        // get到的数据和待更新数据不是同一快照下，则说明当前快照下元数据为not found
                        oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_NOT_FOUND.name());
                    } else {
                        oldVersionNum.put(nodeList.get(i).var1, tuple2.var2.versionNum);
                    }
                } else if (ERROR.equals(tuple2.var1)) {
                    oldVersionNum.put(nodeList.get(i).var1, GetMetaResEnum.GET_ERROR.name());
                }
            }
        }

        return VersionUtil.getLastVersionNum(partInfo.getVersionNum(), partInfo.bucket, partInfo.object)
                .doOnNext(partInfo::setVersionNum)
                .flatMap(lastVersionNum -> updateRocksKey(StoragePoolFactory.getMetaStoragePool(partInfo.bucket), oldVersionNum,
                        key, Json.encode(partInfo), PART_UPLOAD_META, ERROR_PART_UPLOAD_META, nodeList, request, null, snapshotLink));
    }

    public static Mono<Boolean> putMetaData(String key, MetaData metaData, List<Tuple3<String, String, String>> nodeList, EsMeta esMeta, String mda) {
        return putMetaData(key, metaData, nodeList, null, null, mda, esMeta, null);
    }

    public static Mono<Boolean> putMetaData(String key, MetaData metaData, List<Tuple3<String, String, String>> nodeList,
                                            MonoProcessor<Boolean> recoverDataProcessor, MsHttpRequest request, String mda, EsMeta esMeta, String snapshotLink) {
        return MsObjVersionUtils.versionStatusReactive(metaData.bucket)
                .flatMap(status -> putMetaData(key, metaData, nodeList, recoverDataProcessor, request, mda, status, esMeta, false, snapshotLink));
    }

    public static Mono<Boolean> putMetaData(String key, MetaData metaData, List<Tuple3<String, String, String>> nodeList, boolean isMigrate, String snapshotLink) {
        return MsObjVersionUtils.versionStatusReactive(metaData.bucket)
                .flatMap(status -> putMetaData(key, metaData, nodeList, null, null, "", status, null, isMigrate, snapshotLink));
    }

    public static Mono<Boolean> putMetaData(String key, MetaData metaData, List<Tuple3<String, String, String>> nodeList,
                                            MonoProcessor<Boolean> recoverDataProcessor, MsHttpRequest request, String mda, String versionStatus, EsMeta esMeta, boolean isMigrate, String snapshotLink) {
        Set<String> overWriteFiles = new HashSet<>(6);
        Set<String> dupFiles = new HashSet<>(6);
        Set<String> aggregateFiles = new HashSet<>(6);
        boolean[] firstWrite = {false};
        MetaData[] lastMeta = {null};
        Inode[] lastInode = {null};
        // 是否所有节点都 覆盖元数据成功
        boolean[] allOverWriteMetaSuccess = {true};

        Consumer<Tuple3<Integer, PayloadMetaType, String>> responseHandler = s -> {
            if (s.var2 == SUCCESS) {
                if (StringUtils.isNotBlank(s.var3)) {
                    MetaData oldMeta = Json.decodeValue(s.var3, MetaData.class);
                    if (oldMeta.versionNum.equals(metaData.versionNum)) {
                        // 返回的oldMeta为本次上传的元数据，则说明覆盖失败
                        allOverWriteMetaSuccess[0] = false;
                    }
                    Inode oldInode = null;
                    if (null != oldMeta.tmpInodeStr) {
                        oldInode = Json.decodeValue(oldMeta.tmpInodeStr, Inode.class);
                    }

                    if (null == oldInode) {
                        // 覆盖的数据为对象
                        if (oldMeta.partUploadId == null) {
                            if (oldMeta.fileName != null) {
                                if (!oldMeta.fileName.equals(metaData.fileName)) {
                                    if (StringUtils.isNotEmpty(oldMeta.aggregationKey)) {
                                        aggregateFiles.add(oldMeta.aggregationKey);
                                    } else if (StringUtils.isNotEmpty(oldMeta.duplicateKey)) {
                                        dupFiles.add(oldMeta.duplicateKey);
                                    } else {
                                        overWriteFiles.add(oldMeta.fileName);
                                    }
                                } else if (StringUtils.isNotEmpty(oldMeta.duplicateKey) && !oldMeta.duplicateKey.equals(metaData.duplicateKey)) {
                                    dupFiles.add(oldMeta.duplicateKey);
                                }
                            }
                        } else {
                            if (!oldMeta.partUploadId.equals(metaData.partUploadId)) {
                                for (PartInfo partInfo : oldMeta.getPartInfos()) {
                                    if (StringUtils.isNotEmpty(partInfo.deduplicateKey)) {
                                        dupFiles.add(partInfo.deduplicateKey);
                                    } else {
                                        overWriteFiles.add(partInfo.fileName);
                                    }
                                }
                            } else if (metaData.referencedBucket.equals(metaData.bucket) &&
                                    metaData.referencedKey.equals(metaData.key) &&//源对象覆盖
                                    metaData.referencedVersionId.equals(metaData.versionId)) {
                                for (PartInfo partInfo : oldMeta.getPartInfos()) {
                                    if (StringUtils.isNotEmpty(partInfo.deduplicateKey)) {
                                        dupFiles.add(partInfo.deduplicateKey);
                                    } else {
                                        overWriteFiles.add(partInfo.fileName);
                                    }
                                }
                            }
                        }
                        if (oldMeta.isAvailable()) {
                            if (lastMeta[0] == null || oldMeta.versionNum.compareTo(lastMeta[0].versionNum) > 0) {
                                lastMeta[0] = oldMeta;
                            }
                        }
                    } else {
                        // 覆盖的数据为文件
                        if (oldInode.getNodeId() > 0) {
                            for (Inode.InodeData data : oldInode.getInodeData()) {
                                overWriteFiles.add(data.getFileName());
                            }
                        }
                        if (!InodeUtils.isError(oldInode) && !oldInode.isDeleteMark()) {
                            if (lastInode[0] == null || oldInode.getVersionNum().compareTo(lastInode[0].getVersionNum()) > 0) {
                                lastInode[0] = oldInode;
                            }
                        }
                        //同名覆盖目录的情况异步删除原目录文件配额信息。
                        if (oldInode.getNodeId() > 0 && !oldInode.isDeleteMark() && (oldInode.getMode() & S_IFMT) == S_IFDIR || metaData.key.endsWith("/")) {
                            String finalBucketName = oldInode.getBucket();
                            FSQuotaRealService.FS_QUOTA_EXECUTOR.schedule(() -> {
                                FSQuotaUtils.delQuotas(finalBucketName, metaData.key);
                            });
                        }
                    }

                } else {
                    firstWrite[0] = true;
                }
            } else {
                allOverWriteMetaSuccess[0] = false;
            }
        };

        return putRocksKey(StoragePoolFactory.getMetaStoragePool(metaData.bucket), key, Json.encode(metaData),
                PUT_OBJECT_META, ERROR_PUT_OBJECT_META, responseHandler, nodeList, recoverDataProcessor, request, mda, versionStatus, esMeta, isMigrate, snapshotLink)
                .flatMap(b -> {
                    if (!b || allOverWriteMetaSuccess[0] || (dupFiles.isEmpty() && overWriteFiles.isEmpty() && aggregateFiles.isEmpty())) {
                        return Mono.just(b);
                    }
                    if (!firstWrite[0] || (lastMeta[0] != null && lastMeta[0].getVersionNum().compareTo(metaData.getVersionNum()) < 0) || (lastInode[0] != null && lastInode[0].getVersionNum().compareTo(metaData.getVersionNum()) < 0)) {
                        return getObjectMetaVersionRes(metaData.getBucket(), metaData.getKey(), metaData.getVersionId(), nodeList, request)
                                .doOnNext(res -> {
                                    MetaData currentMeta = res.var1;
                                    if (currentMeta.equals(ERROR_META)) {
                                        // 获取元数据失败，则清空被覆盖数据块的集合，防止数据误删
                                        dupFiles.clear();
                                        overWriteFiles.clear();
                                    }
                                    if (currentMeta.equals(NOT_FOUND_META) || currentMeta.isDeleteMark() || currentMeta.isDeleteMarker()) {
                                        return;
                                    }
                                    Inode inode = null;
                                    if (null != currentMeta.tmpInodeStr) {
                                        inode = Json.decodeValue(currentMeta.tmpInodeStr, Inode.class);
                                    }
                                    // 从之前记录的被覆盖数据块的集合中移除当前最新元数据中的数据块
                                    if (inode == null) {
                                        if (currentMeta.partUploadId == null) {
                                            if (StringUtils.isNotEmpty(currentMeta.aggregationKey)) {
                                                aggregateFiles.remove(currentMeta.aggregationKey);
                                            } else if (StringUtils.isNotEmpty(currentMeta.duplicateKey)) {
                                                dupFiles.remove(currentMeta.duplicateKey);
                                            } else {
                                                overWriteFiles.remove(currentMeta.fileName);
                                            }
                                        } else {
                                            for (PartInfo partInfo : currentMeta.getPartInfos()) {
                                                if (StringUtils.isNotEmpty(partInfo.deduplicateKey)) {
                                                    dupFiles.remove(partInfo.deduplicateKey);
                                                } else {
                                                    overWriteFiles.remove(partInfo.fileName);
                                                }
                                            }
                                        }
                                    } else {
                                        for (Inode.InodeData data : inode.getInodeData()) {
                                            overWriteFiles.remove(data.getFileName());
                                        }
                                    }
                                })
                                .thenReturn(true);
                    }
                    return Mono.just(true);
                })
                .doOnNext(b -> {
                    if (b) {
                        int flags;
                        if (metaData.key.endsWith("/")) {
                            flags = FILE_NOTIFY_CHANGE_DIR_NAME;
                        } else {
                            flags = FILE_NOTIFY_CHANGE_FILE_NAME;
                        }

                        NotifyAction action;
                        if (metaData.isDeleteMarker() || metaData.isDeleteMark()) {
                            action = NotifyAction.FILE_ACTION_REMOVED;
                        } else {
                            action = NotifyAction.FILE_ACTION_ADDED;
                        }

                        NotifyServer.getInstance().maybeNotify(metaData.bucket, metaData.key, flags, action);

                        if (!firstWrite[0] || (lastMeta[0] != null && lastMeta[0].getVersionNum().compareTo(metaData.getVersionNum()) < 0) || (lastInode[0] != null && lastInode[0].getVersionNum().compareTo(metaData.getVersionNum()) < 0)) {
                            if (null == lastInode[0]) {
                                // 覆盖的是对象
                                if (!metaData.isDeleteMarker() && lastMeta[0] != null && !overWriteFiles.isEmpty()) {
                                    if (metaData.partUploadId == null) {
                                        overWriteFiles.remove(metaData.getFileName());
                                    } else {
                                        for (PartInfo partInfo : metaData.getPartInfos()) {
                                            overWriteFiles.remove(partInfo.fileName);
                                        }
                                    }
                                    if (metaData.getPartInfos() != null) {
                                        for (PartInfo partInfo : metaData.getPartInfos()) {
                                            dupFiles.remove(partInfo.deduplicateKey);
                                        }
                                    }
                                    StoragePool storagePool = StoragePoolFactory.getStoragePool(lastMeta[0]);
                                    StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(metaData.bucket);
                                    Disposable subscribe = ErasureClient.deleteObjectFile(storagePool, overWriteFiles.toArray(new String[0]), request)
                                            .flatMap(a -> deleteDedupObjectFile(metaStoragePool, dupFiles.toArray(new String[0]), request, false))
                                            .subscribe(s -> {
                                            }, e -> log.error("delete overwrite data error!", e));
                                    Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));

                                } else if (!dupFiles.isEmpty()) {
                                    if (metaData.getPartInfos() != null) {
                                        for (PartInfo partInfo : metaData.getPartInfos()) {
                                            dupFiles.remove(partInfo.deduplicateKey);
                                        }
                                    }
                                    StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(metaData.bucket);
                                    Disposable subscribe = ErasureClient.deleteDedupObjectFile(metaStoragePool, dupFiles.toArray(new String[0]), request, false)
                                            .subscribe(s -> {
                                            }, e -> log.error("delete overwrite data error!", e));
                                    Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
                                } else if (!aggregateFiles.isEmpty()) {
                                    Flux.fromIterable(aggregateFiles)
                                            .flatMap(file -> {
                                                StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool("");
                                                String vnode = Utils.getVnode(file);
                                                return metaStoragePool.mapToNodeInfo(vnode)
                                                        .flatMap(aNodeList -> AggregateFileClient.freeAggregationSpace(file, aNodeList));
                                            }).subscribe(s -> {
                                            }, e -> log.error("delete overwrite data error!", e));
                                }
                            } else {
                                // 覆盖的是文件
                                if (!metaData.isDeleteMarker() && lastInode[0] != null && !overWriteFiles.isEmpty()) {
                                    StoragePool storagePool = StoragePoolFactory.getStoragePool(lastInode[0].getStorage(), lastInode[0].getBucket());
                                    Disposable subscribe = Mono.just(lastInode[0].getLinkN() > 1)
                                            .flatMap(hasLink -> {
                                                if (hasLink) {
                                                    return Node.getInstance().updateInodeLinkN(lastInode[0].getNodeId(), lastInode[0].getBucket(), 0);
                                                } else {
                                                    return ErasureClient.deleteObjectFile(storagePool, overWriteFiles.toArray(new String[0]), request)
                                                            .flatMap(b0 -> Node.getInstance().getInodeAndUpdateCache(lastInode[0].getBucket(), lastInode[0].getNodeId()));
                                                }
                                            })
                                            .subscribe(s -> {
                                            }, e -> log.error("delete overwrite data error!", e));
                                    Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
                                }
                            }

                        }
                    }
                });
    }


    //更新重删引用并返回重删信息
    public static Mono<Boolean> getAndUpdateDeduplicate(StoragePool storagePool, String key, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request
            , MonoProcessor<Boolean> recoverData, MetaData metaData, String etag) {
        Set<String> overWriteFiles = new HashSet<>(6);
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(etag);
        //记录上传对象后文件的真实位置,重删处理时删除
        String[] sourceFile = new String[2];
        sourceFile[0] = metaData.fileName;

        Consumer<Tuple3<Integer, PayloadMetaType, String>> responseHandler = s -> {
            if (s.var2 == SUCCESS) {
                if (StringUtils.isNotBlank(s.var3)) {
                    /** 只有数据池里的对象才会在重删时删除重复的数据,缓存池的重复数据在下刷成功后会删除*/
                    if (!sourceFile[0].equals(metaData.fileName) && metaData.storage.startsWith("data")) {
                        overWriteFiles.add(sourceFile[0]);
                    }
                }
            }
        };

        return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(etag))
                .flatMap(nodeList1 -> getDeduplicateMeta(etag, metaData.storage, nodeList1, request))
                .flatMap(dedupMeta -> {
                    if (dedupMeta.equals(DedupMeta.ERROR_DEDUP_META)) {
                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get Object Deduplicate Meta Data fail");
                    }

                    if (StringUtils.isEmpty(dedupMeta.fileName)) {
                        dedupMeta = new DedupMeta().setBucket(metaData.bucket)
                                .setCount(0)
                                .setEtag(etag)
                                .setKey(metaData.key)
                                .setFileName(metaData.fileName)
                                .setStorage(metaData.storage)
                                .setVersionId(metaData.versionId)
                                .setVersionNum(VersionUtil.getLastVersionNum(""));


                    } else {
                        metaData.setFileName(dedupMeta.fileName);
                    }
                    return putRocksKey(StoragePoolFactory.getMetaStoragePool(etag), key, Json.encode(dedupMeta),
                            PUT_DEDUPLICATE_META, ERROR_PUT_DEDUPLICATE_META, responseHandler, nodeList, recoverData, request, null, null, null, false, null)
                            .doOnNext(b -> {
                                if (b) {
                                    //删除覆盖对象文件lastMeta[0]
                                    if (!metaData.isDeleteMarker() && !overWriteFiles.isEmpty()) {

                                        StoragePool storagePools = StoragePoolFactory.getStoragePool(metaData);

                                        Disposable subscribe = ErasureClient.deleteObjectFile(storagePools, overWriteFiles.toArray(new String[0]), request)
                                                .subscribe(s -> {
                                                }, e -> log.error("delete overwrite data error!", e));
                                        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
                                    }
                                }
                            });
                });
    }

    /**
     * 分段对象的重删
     *
     * @param storagePool
     * @param key
     * @param nodeList
     * @param request
     * @param recoverData
     * @param partInfo
     * @return
     */
    public static Mono<Boolean> putPartDeduplicate(StoragePool storagePool, String key, List<Tuple3<String, String, String>> nodeList
            , MsHttpRequest request, MonoProcessor<Boolean> recoverData, PartInfo partInfo) {
        Set<String> overWriteFiles = new HashSet<>(6);
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(partInfo.etag);
        //记录上传对象后文件的真实位置,重删处理时删除
        String[] sourceFile = new String[2];
        sourceFile[0] = partInfo.fileName;

        Consumer<Tuple3<Integer, PayloadMetaType, String>> responseHandler = s -> {
            if (s.var2 == SUCCESS) {
                if (StringUtils.isNotBlank(s.var3)) {
                    DedupMeta oldMeta = Json.decodeValue(s.var3, DedupMeta.class);
                    if (!sourceFile[0].equals(partInfo.fileName)) {
                        overWriteFiles.add(sourceFile[0]);
                    }
                }
            }
        };

        return bucketPool.mapToNodeInfo(bucketPool.getBucketVnodeId(partInfo.etag))
                .flatMap(nodeList1 -> getDeduplicateMeta(partInfo.etag, partInfo.storage, nodeList1, request))
                .flatMap(dedupMeta -> {
                    if (dedupMeta.equals(DedupMeta.ERROR_DEDUP_META)) {
                        throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get Object Deduplicate Meta Data fail");
                    }
                    if (StringUtils.isEmpty(dedupMeta.fileName)) {
                        dedupMeta = new DedupMeta().setBucket(partInfo.bucket)
                                .setCount(0)
                                .setEtag(partInfo.etag)
                                .setKey(partInfo.object)
                                .setFileName(partInfo.fileName)
                                .setStorage(partInfo.storage)
                                .setVersionId(partInfo.versionId)
                                .setVersionNum(VersionUtil.getLastVersionNum(""));
                    } else {
                        partInfo.setFileName(dedupMeta.fileName);
                    }
                    return putRocksKey(StoragePoolFactory.getMetaStoragePool(partInfo.etag), key, Json.encode(dedupMeta),
                            PUT_DEDUPLICATE_META, ERROR_PUT_DEDUPLICATE_META, responseHandler, nodeList, recoverData, request, null, null, null, false, null)
                            .doOnNext(b -> {
                                if (b) {
                                    //删除覆盖对象文件lastMeta[0]
                                    if (!partInfo.delete && !overWriteFiles.isEmpty()) {

                                        StoragePool storagePools = StoragePoolFactory.getStoragePool(partInfo);

                                        Disposable subscribe = ErasureClient.deleteObjectFile(storagePools, overWriteFiles.toArray(new String[0]), request)
                                                .subscribe(s -> {
                                                }, e -> log.error("delete overwrite data error!", e));
                                        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
                                    }
                                }
                            });
                });
    }

    public static Mono<String> putObjectParts(StoragePool storagePool, MsHttpRequest request, MetaData meta, MonoProcessor<Boolean> recoverData, JsonObject sysMetaMap) {
        long objSize = meta.endIndex + 1;
        int partNum = (int) (objSize / storagePool.getDivisionSize());
        partNum += objSize % storagePool.getDivisionSize() == 0 ? 0 : 1;
        String uploadID = "partNum:" + partNum;
        meta.setFileName(null);
        meta.setPartUploadId(uploadID);
        PartInfo[] partInfos = new PartInfo[partNum];
        meta.setPartInfos(partInfos);
        String requestID = getRequestId();
        Boolean[] dedup = new Boolean[1];

        for (int i = 0; i < partInfos.length; i++) {
            String partStr = String.valueOf(i + 1);
            partInfos[i] = new PartInfo()
                    .setUploadId(uploadID)
                    .setBucket(meta.bucket)
                    .setObject(meta.key)
                    .setPartNum(partStr)
                    .setFileName(Utils.getPartFileName(storagePool, meta.bucket, meta.key, uploadID, partStr, requestID))
                    .setPartSize(i == partInfos.length - 1 ? objSize - i * storagePool.getDivisionSize() : storagePool.getDivisionSize())
                    .setDelete(false)
                    .setStorage(storagePool.getVnodePrefix())
                    .setVersionId(meta.versionId)
                    .setSnapshotMark(meta.snapshotMark);
        }

        MonoProcessor<String> processor = MonoProcessor.create();
        LimitEncoder handler = storagePool.getLimitEncoder(request, true);
        DataDivision dataDivision = new DataDivision(handler, partInfos, storagePool, meta, recoverData);
        PartMd5Digest digest = new PartMd5Digest(storagePool.getDivisionSize(), request);
        request.addResponseEndHandler(v -> {
            if (request.getDataHandlerCompleted().get()) {
                digest.returnToPool();
            } else {
                digest.invalidateToPool();
            }
        });

        String[] md5 = new String[1];
        Long[] actualObjectSize = new Long[1];

        return processHttpBody(request, handler, digest, md5, actualObjectSize, processor, sysMetaMap)
                .doOnNext(b -> {
                    //计算缓存bye[]
                    request.exceptionHandler(e -> {
                        if (e instanceof HttpPostRequestDecoder.ErrorDataDecoderException) {
                            log.debug("", e);
                        } else {
                            log.error("", e);
                        }
                    });

                    if (request.headers().contains("Expect")) {
                        request.response().writeContinue();
                    }
                })
                .flatMap(b -> getMetaDataNodeState(meta))
                .flatMap(b -> StoragePoolFactory.getDeduplicateByBucketName(meta.bucket).zipWith(Mono.just(b)))
                .doOnNext(tuple2 -> dedup[0] = tuple2.getT1())
                .flatMap(b -> {
                    if (b.getT2()) {
                        //EC计算后上传
                        return dataDivision.putParts(request, recoverData, digest, requestID, dedup[0]);
                    }
                    return Mono.just(false);
                })
                .flatMap(b -> {
                    if (b) {
                        int i = 0;
                        //获取每一段MD5
                        String[] partMd51 = digest.getPartMd5();
                        for (String partMd5 : partMd51) {
                            partInfos[i++].setEtag(partMd5);

                            if (i == partMd51.length && dedup[0]) {
                                StoragePool dedupPool = StoragePoolFactory.getMetaStoragePool(partMd5);
                                String dedupVnode = dedupPool.getBucketVnodeId(partMd5);
                                String suffix = Utils.getDeduplicatMetaKey(dedupVnode, partMd5, meta.storage, requestID);
                                int num = i - 1;
                                partInfos[num].deduplicateKey = suffix;
                                return dedupPool.mapToNodeInfo(dedupVnode)
                                        .flatMap(nodeList -> putPartDeduplicate(dedupPool, suffix,
                                                nodeList, request, recoverData, partInfos[num]))
                                        .flatMap(bool -> {
                                            if (!bool) {
                                                throw new MsException(UNKNOWN_ERROR, "put deduplicate meta failed");
                                            }
                                            partInfos[num].deduplicateKey = suffix;
                                            return Mono.just(true);
                                        });
                            }
                        }
                        return Mono.just(true);
                    } else {
                        return Mono.just(false);
                    }
                })
                .flatMap(b -> {
                    if (b) {
                        processor.onNext(md5[0]);
                    } else {
                        processor.onNext("");
                    }

                    return processor;
                });
    }

    public static Mono<String> putObject(StoragePool storagePool, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request,
                                         MetaData meta, MonoProcessor<Boolean> recoverData, boolean isChunked, JsonObject sysMetaMap) {
        //对象长度为0，无需落盘数据，直接返回固定的etag
        MonoProcessor<String> processor = MonoProcessor.create();
        long objSize = meta.endIndex + 1;
        LimitEncoder handler;
        if (!isChunked) {
            if (objSize == 0) {
                processor.onNext(SysConstants.ZERO_ETAG);
                return processor;
            } else if (objSize > storagePool.getDivisionSize()) {
                return putObjectParts(storagePool, request, meta, recoverData, sysMetaMap);
            } else if (objSize < storagePool.getPackageSize()) {
                handler = new NoLimitEncoder(storagePool.getEncoder(objSize), request);
            } else {
                handler = storagePool.getLimitEncoder(request, true);
            }
        } else {
            if (HttpMethod.POST == request.method() && request.headers().contains(CONTENT_TYPE) && request.getHeader(CONTENT_TYPE).contains("multipart/form-data")) {
                handler = new NoLimitEncoder(storagePool.getEncoder(), request);
            } else {
                handler = storagePool.getLimitEncoder(request);
            }
        }

        Digest digest;
        try {
            if (objSize >= storagePool.getPackageSize() && FastMd5Digest.isAvailable() && StringUtils.isBlank(request.getHeader(SYNC_COMPRESS))) {
                digest = FastMd5DigestPool.getInstance().borrow();
                Digest finalDigest = digest;
                request.addResponseEndHandler(v -> {
                    if (request.getDataHandlerCompleted().get()) {
                        FastMd5DigestPool.getInstance().returnDigest(finalDigest);
                    } else {
                        FastMd5DigestPool.getInstance().invalidateDigest(finalDigest);
                    }
                });
            } else {
                digest = new Md5Digest();
            }
        } catch (Exception e) {
            digest = new Md5Digest();
        }
        String[] md5 = new String[1];
        Long[] actualObjectSize = new Long[1];
        return processHttpBody(request, handler, digest, md5, actualObjectSize, processor, sysMetaMap)
                .doOnNext(b -> {
                    request.exceptionHandler(e -> {
                        if (e instanceof HttpPostRequestDecoder.ErrorDataDecoderException) {
                            log.debug("", e);
                        } else {
                            log.error("", e);
                        }
                    });

                    if (request.headers().contains("Expect")) {
                        request.response().writeContinue();
                    }
                })
                .flatMap(b -> getMetaDataNodeState(meta))
                .flatMap(b -> {
                    if (b) {
                        return putObject(storagePool, nodeList, handler.data(), meta, handler, request, recoverData);
                    }
                    return Mono.just(false);
                })
                .flatMap(b -> {
                    if (b) {
                        meta.setEndIndex(actualObjectSize[0] - 1)
                                .setSmallFile(actualObjectSize[0] < MAX_SMALL_SIZE);
                        processor.onNext(md5[0]);
                    } else {
                        processor.onNext("");
                    }
                    return processor;
                });
    }

    public static Mono<Boolean> putCredential(Credential credential, String credentialKey, List<Tuple3<String, String, String>> nodeList) {
        //获取索引池
        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(credential.accessKey);
        return putCredentialToken(metaStoragePool, credentialKey, Json.encode(credential), PUT_STS_TOKEN, ERROR_PUT_STS_TOKEN, nodeList, null);

    }

    //todo 看下这里验证传递的是啥
    public static Mono<Credential> getCredential(String accessKey) {
        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(accessKey);
        String vnode = metaStoragePool.getBucketVnodeId(accessKey);
        String credentialKey = getCredentialKey(vnode, accessKey);
        return metaStoragePool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> getSTSTokenRes(metaStoragePool, credentialKey, Credential.class, GET_STS_TOKEN, NOT_FOUND_STS_TOKEN, ERROR_STS_TOKEN,
                        (key, credential, nodeList0, res) -> putCredential(credential, credentialKey, nodeList), nodeList, null))
                .flatMap(tuple -> Mono.just(tuple.var1));

    }

    public static Mono<Integer> setDelMarkWithFsQuota(DelMarkParams params, String currentSnapshotMark, String snapshotLink, String updateQuotaKeyStr) {
        if (StringUtils.isNotBlank(updateQuotaKeyStr)) {
            params.setUpdateQuotaKeyStr(updateQuotaKeyStr);
        }
        return setDelMark(params, currentSnapshotMark, snapshotLink);
    }

    public static Mono<Integer> setDelMark(DelMarkParams params, String currentSnapshotMark, String snapshotLink) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(params.bucket);
        String vnode = params.vnodeList.get(0).var3;
        String key = Utils.getVersionMetaDataKey(vnode, params.bucket, params.object, params.versionId, params.pendingMark == null ? params.snapshotMark : params.pendingMark.getSnapshotMark());

        MetaData metaData;
        boolean markUnView = params.pendingMark != null && !params.getPendingMark().isCurrentSnapshotObject(currentSnapshotMark);
        if (markUnView) {
            // 快照创建后删除快照创建前的对象，则将其置为unView
            metaData = params.pendingMark.clone();
            metaData.setVersionNum(params.versionNum);
            metaData.addUnViewSnapshotMark(currentSnapshotMark);
            metaData.setWeakUnView(null);
        } else {
            metaData = new MetaData()
                    .setKey(params.object)
                    .setBucket(params.bucket)
                    .setVersionNum(params.versionNum)
                    .setDeleteMark(true)
                    .setStamp(params.pendingMark == null ? String.valueOf(System.currentTimeMillis()) : params.pendingMark.stamp)
                    .setVersionId(params.versionId);
            metaData.setSnapshotMark(params.pendingMark == null ? params.snapshotMark : params.pendingMark.getSnapshotMark());
        }

        if (ERROR_DELETE_OBJECT_ALL_META.equals(params.type)) {
            metaData.setShardingStamp(VersionUtil.getVersionNum());
            metaData.setDiscard(true);
        } else if (params.pendingMark != null) {
            // 记录删除标记对应的数据块，用于恰好满足k时进行异步删除
            if (params.pendingMark.partUploadId != null) {
                if (params.pendingMark.inode <= 0 || params.needDeleteInode) {
                    metaData.setPartUploadId(params.pendingMark.partUploadId);
                    metaData.setPartInfos(params.pendingMark.partInfos);
                }
            } else if (StringUtils.isNotEmpty(params.pendingMark.fileName)) {
                metaData.setFileName(params.pendingMark.fileName);
            }
            if (StringUtils.isNotEmpty(params.pendingMark.duplicateKey)) {
                metaData.setDuplicateKey(params.pendingMark.duplicateKey);
            }
            if (StringUtils.isNotEmpty(params.pendingMark.aggregationKey)) {
                metaData.setAggregationKey(params.pendingMark.aggregationKey);
            }
            metaData.setStorage(params.pendingMark.storage);
        }

        String value = Json.encode(metaData);
        MetaData inodeDeleteMark = null;
        if (params.inodeId > 0) {
            inodeDeleteMark = Json.decodeValue(value, MetaData.class);
            inodeDeleteMark.setInode(params.inodeId);
            inodeDeleteMark.setCookie(params.fileCookie);
        }
        List<SocketReqMsg> msgs = mapToMsg(key, value, params.vnodeList);
        msgs.forEach(msg -> {
            msg.put("status", params.versionStatus);
            if (params.isMigrate()) {
                msg.put("migrate", "1");
            }
            if (params.needDeleteInode) {
                msg.put("needDeleteInode", "1");
            }
            if (!metaData.deleteMark) {
                msg.put("currentSnapshotMark", currentSnapshotMark);
            }
            if (StringUtils.isNotBlank(params.updateQuotaKeyStr)) {
                msg.put("updateQuotaKeyStr", params.updateQuotaKeyStr);
            }
            Optional.ofNullable(snapshotLink).ifPresent(v -> msg.put("snapshotLink", v));
        });

        MonoProcessor<Integer> res = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, MARK_DELETE_OBJECT, String.class, params.vnodeList);
        MetaData finalInodeDeleteMark = inodeDeleteMark;
        Set<String> overwrite = new HashSet<>();
        Disposable[] disposables = new Disposable[2];
        disposables[0] = responseInfo.responses.subscribe(s -> {
                    if (s.var2.equals(SUCCESS) && StringUtils.isNotEmpty(s.var3)) {
                        overwrite.add(s.var3);
                    }
                },
                e -> log.error("", e),
                () -> {
                    if (responseInfo.successNum == storagePool.getK() + storagePool.getM()) {
                        if (params.inodeId > 0) {
                            DelDeleteMark.putDeleteKey(key, Json.encode(finalInodeDeleteMark));
                        } else if (!markUnView) {
                            DelDeleteMark.putDeleteKey(key, value);
                        }
                        if (!overwrite.isEmpty()) {
                            Flux<Boolean> overwriteRes = Flux.empty();
                            for (String s : overwrite) {
                                String[] split = s.split("=", 2);
                                String storage = split[0];
                                String fileName = split[1];
                                if (fileName.startsWith(ROCKS_AGGREGATION_META_PREFIX)) {
                                    StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(metaData.bucket);
                                    overwriteRes = overwriteRes.mergeWith(metaStoragePool.mapToNodeInfo(Utils.getVnode(fileName))
                                            .flatMap(nodeList -> AggregateFileClient.freeAggregationSpace(fileName, nodeList)));
                                } else {
                                    StoragePool storagePool1 = StoragePoolFactory.getStoragePool(storage, metaData.bucket);
                                    overwriteRes = overwriteRes.mergeWith(ErasureClient.deleteObjectFile(storagePool1, new String[]{fileName}, null));
                                }
                            }
                            overwriteRes.defaultIfEmpty(false).collectList().subscribe();
                        }
                        res.onNext(1);
                        QuotaRecorder.addCheckBucket(params.bucket);
                    } else if (responseInfo.successNum == 0) {
                        res.onNext(0);
                    } else if (responseInfo.successNum >= storagePool.getK()) {
                        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                .put("poolQueueTag", poolQueueTag);
                        Optional.ofNullable(currentSnapshotMark).ifPresent(v -> errorMsg.put("currentSnapshotMark", v));
                        Optional.ofNullable(snapshotLink).ifPresent(v -> errorMsg.put("snapshotLink", v));
                        if (ERROR_MARK_DELETE_META.equals(params.type)) {
                            errorMsg
                                    .put("bucket", params.bucket)
                                    .put("object", params.object)
                                    .put("versionNum", params.versionNum)
                                    .put("vnode", vnode)
                                    .put("versionId", params.versionId)
                                    .put("pendingMark", Json.encode(metaData));
                            Optional.ofNullable(params.updateQuotaKeyStr).ifPresent(v -> errorMsg.put("updateQuotaKeyStr", v));
                        } else if (ERROR_DELETE_OBJECT_ALL_META.equals(params.type)) {
                            errorMsg
                                    .put("bucket", params.bucket)
                                    .put("key", key)
                                    .put("value", value)
                                    .put("vnode", vnode);
                        } else if (ERROR_DEL_INODE_META.equals(params.type)) {
                            errorMsg
                                    .put("bucket", params.bucket)
                                    .put("object", params.object)
                                    .put("versionNum", params.versionNum)
                                    .put("versionId", params.versionId)
                                    .put("needDeleteInode", params.needDeleteInode ? "1" : "0")
                                    .put("fileCookie", String.valueOf(params.fileCookie))
                                    .put("value", String.valueOf(params.inodeId > 0 ? Json.encode(finalInodeDeleteMark) : null));
                            Optional.ofNullable(params.updateQuotaKeyStr).ifPresent(v -> errorMsg.put("updateQuotaKeyStr", v));
                        }
                        publishEcError(responseInfo.res, params.vnodeList, errorMsg, params.type);
                        if (responseInfo.writedNum > 0) {
                            res.onNext(2);
                        } else {
                            res.onNext(1);
                        }
                    } else {
                        //未成功，但有部分节点成功，不返回结果

                        // 桶散列迁移删除数据直接返回false
                        if (ERROR_DELETE_OBJECT_ALL_META.equals(params.type)) {
                            res.onNext(0);
                        }
                    }
                });
        Optional.ofNullable(params.request).ifPresent(r -> r.addResponseCloseHandler(v -> {
            for (Disposable disposable : disposables) {
                if (disposable != null) {
                    disposable.dispose();
                }
            }
        }));
        return res.doOnNext(i -> {
            if (i > 0) {
                int flags;
                if (metaData.key.endsWith("/")) {
                    flags = FILE_NOTIFY_CHANGE_DIR_NAME;
                } else {
                    flags = FILE_NOTIFY_CHANGE_FILE_NAME;
                }

                NotifyAction action = NotifyAction.FILE_ACTION_REMOVED;
                NotifyServer.getInstance().maybeNotify(metaData.bucket, metaData.key, flags, action);
            }
        });
    }

    public static Mono<Integer> setDelMark(DelMarkParams params) {
        return setDelMark(params, null, null);
    }

    public static Mono<Boolean> deleteObjectMeta(String key, String bucket, String object, String versionId, String versionNum,
                                                 List<Tuple3<String, String, String>> vnodeList, MsHttpRequest request, long nodeId, String uploadIdOrFileName, String snapshotMark, long fileCookie) {
        return MsObjVersionUtils.versionStatusReactive(bucket)
                .flatMap(status -> deleteObjectMeta(key, bucket, object, versionId, versionNum, vnodeList, request, status, nodeId, uploadIdOrFileName, snapshotMark, fileCookie));
    }

    public static Mono<Boolean> deleteObjectMeta(String key, String bucket, String object, String versionId, String versionNum,
                                                 List<Tuple3<String, String, String>> vnodeList, MsHttpRequest request, String versionStatus, long nodeId, String uploadIdOrFileName, String snapshotMark, long fileCookie) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        List<SocketReqMsg> msgs = ECUtils.mapToMsg(key, versionNum, vnodeList);
        msgs.forEach(msg -> {
            msg.put("status", versionStatus);
            msg.put("nodeId", String.valueOf(nodeId));
            msg.put("bucket", bucket);
            msg.put("fileCookie", String.valueOf(fileCookie));
            Optional.ofNullable(snapshotMark).ifPresent(v -> msg.put("snapshotMark", v));
            Optional.ofNullable(uploadIdOrFileName).ifPresent(v -> msg.put("uploadIdOrFileName", v));
        });
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, DELETE_OBJECT_META, String.class, vnodeList);

        Disposable subscribe = responseInfo.responses.subscribe(s -> {
        }, e -> log.error("", e), () -> {
            //除了全部成功的情况，都要重新发布。
            if (responseInfo.successNum == storagePool.getK() + storagePool.getM()) {
                res.onNext(true);
            } else {
                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                        .put("bucket", bucket)
                        .put("object", object)
                        //setdeleteMark时生成的versionNum
                        .put("versionNum", versionNum)
                        .put("versionId", versionId)
                        .put("poolQueueTag", poolQueueTag)
                        .put("nodeId", String.valueOf(nodeId));
                Optional.ofNullable(snapshotMark).ifPresent(v -> errorMsg.put("snapshotMark", v));
                Optional.ofNullable(uploadIdOrFileName).ifPresent(v -> errorMsg.put("uploadIdOrFileName", v));
                errorMsg.put("fileCookie", String.valueOf(fileCookie));
                publishEcError(responseInfo.res, vnodeList, errorMsg, ERROR_DELETE_OBJECT_META);
                res.onNext(false);
            }
        });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return res;
    }

    public static Mono<Boolean> deleteRocketsValue(String bucket, String key, List<Tuple3<String, String, String>> vnodeList) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        List<SocketReqMsg> msgs = ECUtils.mapToMsg(key, null, vnodeList);
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, DELETE_ROCKETS_VALUE, String.class, vnodeList);
        responseInfo.responses.subscribe(s -> {
                },
                e -> log.error("", e), () -> {
                    if (responseInfo.successNum == storagePool.getK() + storagePool.getM()) {
                        res.onNext(true);
                    } else if (responseInfo.successNum >= storagePool.getK()) {
                        res.onNext(true);
//                        String storageName = "storage_" + storagePool.getVnodePrefix();
//                        String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
                        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
//                        if (StringUtils.isEmpty(poolQueueTag)) {
//                            String strategyName = "storage_" + storagePool.getVnodePrefix();
//                            poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                        }
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                .put("bucket", bucket)
                                .put("key", key)
                                .put("vnode", vnodeList.get(0).var3)
                                .put("poolQueueTag", poolQueueTag);
                        publishEcError(responseInfo.res, vnodeList, errorMsg, ERROR_DELETE_ROCKETS_VALUE);
                    } else {
                        res.onNext(false);
                    }
                });
        return res;
    }

    public static Mono<Boolean> deleteDedupMeta(StoragePool storagePool, String key, DedupMeta value, List<Tuple3<String, String, String>> nodeList) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("dedupKey", Json.encode(Collections.singletonList(key)));
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(t -> {
                    SocketReqMsg msg0 = msg.copy()
                            .put("lun", t.var2)
                            .put("curVnode", t.var3);
                    return msg0;
                }).collect(Collectors.toList());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, DELONE_DEDUPLICATE_META, String.class, nodeList);
        responseInfo.responses.subscribe(s -> {
        }, e -> log.error("", e), () -> {
            if (responseInfo.successNum != storagePool.getK() + storagePool.getM()) {
                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                        .put("realKey", key)
                        .put("value", Json.encode(value))
                        .put("poolQueueTag", poolQueueTag);
                publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_DELETE_DEDUPLICATE_META);
            }
            res.onNext(true);
        });
        return res;
    }

    public static Mono<Boolean> deleteUnsyncRocketsValue(String bucket, String key, UnSynchronizedRecord toUpdateAsyncRecord, List<Tuple3<String, String, String>> vnodeList, String asyncOperate) {
        return deleteUnsyncRocketsValue(bucket, key, toUpdateAsyncRecord, vnodeList, asyncOperate, null)
                .map(res -> res != 0);
    }

    /**
     * 删除差异记录，根据asyncOperate的不同，会对async站点的记录做额外的处理。
     *
     * @param toUpdateAsyncRecord asyncOperate等于UPDATE_ASYNC_RECORD时生效，删除差异记录后将相关的async记录更新
     * @param asyncOperate        见{@link SysConstants#ASYNC_CLUSTER_SIGNAL_SET}，对async记录的处理，有删除和更新两种。
     * @return
     */
    public static Mono<Integer> deleteUnsyncRocketsValue(String bucket, String key, UnSynchronizedRecord toUpdateAsyncRecord,
                                                         List<Tuple3<String, String, String>> vnodeList, String asyncOperate, MsHttpRequest request) {
        if (toUpdateAsyncRecord != null) {
            toUpdateAsyncRecord.setVersionNum(VersionUtil.getVersionNum(false));
        }
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        List<SocketReqMsg> msgs = vnodeList.stream()
                .map(tuple -> {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("key", key);
                    if (UnSynchronizedRecord.isOldPath(key)) {
                        msg.put("lun", tuple.var2);
                    } else {
                        msg.put("lun", MSRocksDB.getSyncRecordLun(tuple.var2));
                    }
                    if ((WRITE_ASYNC_RECORD && ASYNC_CLUSTER_SIGNAL_SET.contains(asyncOperate))
                            || ARCHIVE_ANALYZER_KEY.equals(asyncOperate)) {
                        msg.put(ASYNC_CLUSTER_SIGNAL, asyncOperate);
                        if (toUpdateAsyncRecord != null) {
                            msg.put("value", Json.encode(toUpdateAsyncRecord))
                                    .put("oldVersion", toUpdateAsyncRecord.versionNum);
                        }
                    }
                    return msg;
                })
                .collect(Collectors.toList());
        MonoProcessor<Integer> res = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, DELETE_UNSYNC_ROCKETS_VALUE, String.class, vnodeList);
        Disposable subscribe = responseInfo.responses.subscribe(s -> {
                },
                e -> log.error("", e), () -> {
                    if (responseInfo.successNum == storagePool.getK() + storagePool.getM()) {
                        res.onNext(1);
                    } else {
//                        String storageName = "storage_" + storagePool.getVnodePrefix();
//                        String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
                        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
//                        if (StringUtils.isEmpty(poolQueueTag)) {
//                            String strategyName = "storage_" + storagePool.getVnodePrefix();
//                            poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                        }
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                .put("bucket", bucket)
                                .put("key", key)
                                .put("poolQueueTag", poolQueueTag);
                        if ((WRITE_ASYNC_RECORD && ASYNC_CLUSTER_SIGNAL_SET.contains(asyncOperate))
                                || ARCHIVE_ANALYZER_KEY.equals(asyncOperate)) {
                            errorMsg.put(ASYNC_CLUSTER_SIGNAL, asyncOperate);
                            if (toUpdateAsyncRecord != null) {
                                errorMsg.put("value", Json.encode(toUpdateAsyncRecord))
                                        .put("oldVersion", toUpdateAsyncRecord.versionNum);
                            }
                        }
                        publishEcError(responseInfo.res, vnodeList, errorMsg, ERROR_DELETE_UNSYNC_ROCKETS_VALUE);
                        res.onNext(responseInfo.successNum >= storagePool.getK() ? 2 : 0);
                    }
                });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return res;
    }

    /**
     * 生命周期专用删除对象，QoS限制
     */
    public static Mono<Boolean> lifecycleDeleteFile(StoragePool storagePool, String[] fileNames, MsHttpRequest request) {
        List<Disposable> disposableList = new LinkedList<>();

        if (request != null) {
            request.addResponseCloseHandler(v -> {
                for (Disposable disposable : disposableList) {
                    disposable.dispose();
                }
            });
        }

        return Flux.fromArray(fileNames)
                .flatMap(f -> storagePool.mapToNodeInfo(storagePool.getObjectVnodeId(f))
                        .flatMap(list -> {
                            Tuple2<Mono<Boolean>, Disposable> tuple = lifecycleDeleteFile(storagePool, f, list);
                            disposableList.add(tuple.var2);
                            return tuple.var1;
                        }))
                .collectList()
                .map(list -> {
                    for (Boolean res : list) {
                        if (!res) {
                            return false;
                        }
                    }
                    return true;
                });
    }

    /**
     * 生命周期专用删除对象，QoS限制
     */
    public static Tuple2<Mono<Boolean>, Disposable> lifecycleDeleteFile(StoragePool storagePool, String fileName,
                                                                        List<Tuple3<String, String, String>> curList) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("fileName", Json.encode(Collections.singletonList(fileName)));

        List<SocketReqMsg> msgs = curList.stream()
                .map(t -> {
                    SocketReqMsg msg0 = msg.copy()
                            .put("lun", t.var2)
                            .put("curVnode", t.var3)
                            .put("recover", "1")
                            .put("lifecycle_move", "1");
                    return msg0;
                }).collect(Collectors.toList());
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, DELETE_FILE, String.class, curList);
        MonoProcessor<Boolean> processor = MonoProcessor.create();

        Disposable subscribe = responseInfo.responses.subscribe(p -> {
        }, e -> log.error("", e), () -> {
            //进行到删除文件时，用户一定会获得删除成功的响应。
            if (responseInfo.writedNum > 0) {
                processor.onNext(false);
            } else {
                processor.onNext(true);
                if (responseInfo.successNum != storagePool.getK() + storagePool.getM()) {
//                    String storageName = "storage_" + storagePool.getVnodePrefix();
//                    String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
                    String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
//                    if (StringUtils.isEmpty(poolQueueTag)) {
//                        String strategyName = "storage_" + storagePool.getVnodePrefix();
//                        poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                    }
                    SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                            .put("fileName", Json.encode(new String[]{fileName}))
                            .put("storage", storagePool.getVnodePrefix())
                            .put("nodeList", Json.encode(curList))
                            .put("poolQueueTag", poolQueueTag);
                    publishEcError(responseInfo.res, curList, errorMsg, ERROR_LIFECYCLE_DELETE_OBJECT_FILE);
                }
            }
        });

        return new Tuple2<>(processor, subscribe);
    }

    /**
     * 修改ec删除旧对象数据使用
     *
     * @param storagePool
     * @param fileNames
     * @param request
     * @return
     */
    public static Mono<Boolean> restoreDeleteObjectFile(StoragePool storagePool, String[] fileNames, MsHttpRequest request) {
        //这里考虑先发消息判断当前对象是否有数据块在被读取，然后根据结果判断是否继续进行删除还是先不删除发布消息之后删除
        List<Disposable> disposableList = new LinkedList<>();

        if (request != null) {
            request.addResponseCloseHandler(v -> {
                for (Disposable disposable : disposableList) {
                    disposable.dispose();
                }
            });
        }

        //首先删除前发送消息查询对象的旧数据块是否在被读取
        return Flux.fromArray(fileNames)
                .flatMap(f -> storagePool.mapToNodeInfo(storagePool.getObjectVnodeId(f))
                        .flatMap(list -> {
                            Tuple2<Mono<Boolean>, Disposable> tuple = checkObjectBeRead(storagePool, f, list);
                            disposableList.add(tuple.var2);
                            return tuple.var1;
                        }))
                .collectList()
                .flatMap(list -> {
                    for (boolean s : list) {
                        if (s) {//若有返回数据块被读取，则该对象数据块删除失败，发布消息等待下一次删除
                            return Mono.just(false);
                        }
                    }
                    return Mono.just(true);
                })
                .flatMap(b -> {
                    if (b) {
                        return Flux.fromArray(fileNames)//分段对象删除时，这里包含了所有段的fileName
                                .flatMap(f -> storagePool.mapToNodeInfo(storagePool.getObjectVnodeId(f))
                                        .flatMap(list -> {
                                            Tuple2<Mono<Boolean>, Disposable> tuple = restoreDeleteObjectFile(storagePool, f, list);
                                            disposableList.add(tuple.var2);
                                            return tuple.var1;
                                        }))
                                .collectList()
                                .map(list -> list.get(0));
                    } else {
                        return Mono.just(false);
                    }
                });

//        return Flux.fromArray(fileNames)
//                .flatMap(f -> storagePool.mapToNodeInfo(storagePool.getObjectVnodeId(f))
//                        .flatMap(list -> {
//                            Tuple2<Mono<Boolean>, Disposable> tuple = restoreDeleteObjectFile(storagePool, f, list);
//                            disposableList.add(tuple.var2);
//                            return tuple.var1;
//                        }))
//                .collectList()
//                .map(list -> list.get(0));
    }

    public static Tuple2<Mono<Boolean>, Disposable> checkObjectBeRead(StoragePool storagePool, String fileName, List<Tuple3<String, String, String>> curList) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("fileName", Json.encode(Collections.singletonList(fileName)));//只能存放一个元素的List

        List<SocketReqMsg> msgs = curList.stream()
                .map(t -> {
                    SocketReqMsg msg0 = msg.copy()
                            .put("lun", t.var2)
                            .put("curVnode", t.var3)
                            .put("recover", "1")
                            .put("restore", "1");
                    return msg0;
                }).collect(Collectors.toList());
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, CHECK_OBJECT_FILE_READ, String.class, curList);
        MonoProcessor<Boolean> processor = MonoProcessor.create();

        Disposable subscribe = responseInfo.responses.subscribe(p -> {
        }, e -> log.error("", e), () -> {
            if (responseInfo.writedNum > 0) {
                //只要某个节点上在读取文件，那么就返回ture，表示文件在被读取
                processor.onNext(true);
            } else if (responseInfo.successNum != storagePool.getK() + storagePool.getM()) {
                //有节点未正常返回检查结果，先不删除文件
                processor.onNext(true);
            } else {
                processor.onNext(false);
            }
//            //进行到删除文件时，用户一定会获得删除成功的响应。当要删除的对象正在被读取时，writedNum增加，此次删除失败并发布消息
//            processor.onNext(true);
//            if (responseInfo.writedNum > 0 || (responseInfo.successNum != storagePool.getK() + storagePool.getM())) {
//                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
//                        .put("fileName", Json.encode(new String[]{fileName}))
//                        .put("storage", storagePool.getVnodePrefix())
//                        .put("nodeList", Json.encode(curList));
//                publishEcError(responseInfo.res, curList, errorMsg, ERROR_RESTORE_DELETE_OBJECT_FILE);
//            }
        });

        return new Tuple2<>(processor, subscribe);
    }

    /**
     * 删除对象。
     */
    public static Tuple2<Mono<Boolean>, Disposable> restoreDeleteObjectFile(StoragePool storagePool, String fileName,
                                                                            List<Tuple3<String, String, String>> curList) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("fileName", Json.encode(Collections.singletonList(fileName)));

        List<SocketReqMsg> msgs = curList.stream()
                .map(t -> {
                    SocketReqMsg msg0 = msg.copy()
                            .put("lun", t.var2)
                            .put("curVnode", t.var3)
                            .put("recover", "1")
                            .put("restore", "1");
                    return msg0;
                }).collect(Collectors.toList());
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, DELETE_FILE, String.class, curList);
        MonoProcessor<Boolean> processor = MonoProcessor.create();

        Disposable subscribe = responseInfo.responses.subscribe(p -> {
        }, e -> log.error("", e), () -> {
            //进行到删除文件时，用户一定会获得删除成功的响应。当要删除的对象正在被读取时，writedNum增加，此次删除失败并发布消息
            if (responseInfo.writedNum > 0) {
                processor.onNext(false);
            } else {
                processor.onNext(true);
                if (responseInfo.successNum != storagePool.getK() + storagePool.getM()) {
//                    String strategyName = "storage_" + storagePool.getVnodePrefix();
//                    String poolQueueTag = redisConnPool.getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
                    String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
//                    if (StringUtils.isEmpty(poolQueueTag)) {
//                        String strategyName = "storage_" + storagePool.getVnodePrefix();
//                        poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                    }
                    SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                            .put("fileName", Json.encode(new String[]{fileName}))
                            .put("storage", storagePool.getVnodePrefix())
                            .put("nodeList", Json.encode(curList))
                            .put("poolQueueTag", poolQueueTag);
                    publishEcError(responseInfo.res, curList, errorMsg, ERROR_RESTORE_DELETE_OBJECT_FILE);
                }
            }
        });

        return new Tuple2<>(processor, subscribe);
    }

    public static Mono<Boolean> deleteObjectFile(StoragePool storagePool, String[] fileNames, MsHttpRequest request) {
        List<Disposable> disposableList = new LinkedList<>();
        if (storagePool == null) {
            return Mono.just(true);
        }
        if (request != null) {
            request.addResponseCloseHandler(v -> {
                for (Disposable disposable : disposableList) {
                    disposable.dispose();
                }
            });
        }

        return Flux.fromArray(fileNames)
                .flatMap(f -> {
                    if (StringUtils.isBlank(f)) {
                        return Mono.just(true);
                    } else if (f.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                        return Node.getInstance().getChunk(f)
                                .flatMap(chunk -> {
                                    // 处理不同池（缓存池、数据池）文件
                                    Map<String, List<String>> map = new HashMap<>();
                                    chunk.getChunkList().stream().forEach(inodeData0 ->
                                            map.computeIfAbsent(inodeData0.storage, k -> new ArrayList<>()).add(inodeData0.fileName)
                                    );

                                    Flux<Boolean> deleteResults = Flux.fromIterable(map.entrySet())
                                            .flatMap(entry -> {
                                                StoragePool storagePool0 = StoragePoolFactory.getStoragePool(entry.getKey(), chunk.getBucket());
                                                String[] fs = entry.getValue().toArray(new String[0]);
                                                return ErasureClient.deleteObjectFile(storagePool0, fs, null);
                                            });

                                    return deleteResults
                                            .collectList()
                                            .map(results -> results.stream().allMatch(Boolean::booleanValue));
                                })
                                .flatMap(b -> FsUtils.deleteChunkMeta(f));
                    } else {
                        return storagePool.mapToNodeInfo(storagePool.getObjectVnodeId(f))
                                .flatMap(list -> {
                                    Tuple2<Mono<Boolean>, Disposable> tuple = deleteObjectFile(storagePool, f, list);
                                    disposableList.add(tuple.var2);
                                    return tuple.var1;
                                });
                    }
                })
                .collectList()
                .map(l -> true);
    }

    /**
     * 删除对象。
     */
    public static Tuple2<Mono<Boolean>, Disposable> deleteObjectFile(StoragePool storagePool, String fileName,
                                                                     List<Tuple3<String, String, String>> curList) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("fileName", Json.encode(Collections.singletonList(fileName)));

        List<SocketReqMsg> msgs = curList.stream()
                .map(t -> {
                    SocketReqMsg msg0 = msg.copy()
                            .put("lun", t.var2)
                            .put("curVnode", t.var3);
                    return msg0;
                }).collect(Collectors.toList());
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, DELETE_FILE, String.class, curList);
        MonoProcessor<Boolean> processor = MonoProcessor.create();

        Disposable subscribe = responseInfo.responses.subscribe(p -> {
        }, e -> log.error("", e), () -> {
            //进行到删除文件时，用户一定会获得删除成功的响应。
            processor.onNext(true);
            if (responseInfo.successNum != storagePool.getK() + storagePool.getM()) {
//                String storageName = "storage_" + storagePool.getVnodePrefix();
//                String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
//                if (StringUtils.isEmpty(poolQueueTag)) {
//                    String strategyName = "storage_" + storagePool.getVnodePrefix();
//                    poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                }
                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                        .put("fileName", Json.encode(new String[]{fileName}))
                        .put("storage", storagePool.getVnodePrefix())
                        .put("nodeList", Json.encode(curList))
                        .put("poolQueueTag", poolQueueTag);
                publishEcError(responseInfo.res, curList, errorMsg, ERROR_DELETE_OBJECT_FILE);
            }
        });

        return new Tuple2<>(processor, subscribe);
    }

    //删除多个文件
    public static Mono<Boolean> deleteDedupObjectFile(StoragePool storagePool, String[] keys, MsHttpRequest request, boolean isLifecycleClear) {
        List<Disposable> disposableList = new CopyOnWriteArrayList<>();

        if (request != null) {
            request.addResponseCloseHandler(v -> {
                for (int j = 0; j < disposableList.size(); j++) {
                    disposableList.get(j).dispose();
                }
            });
        }

        return Flux.fromArray(keys)
                .delayElements(Duration.ofMillis(1))
                .flatMap(f -> {
                    if (!f.startsWith("^")) {
                        return Mono.just(true);
                    }
                    String vnode = f.split("\\/")[0];
                    vnode = vnode.substring(1);

                    return storagePool.mapToNodeInfo(vnode)
                            .flatMap(list -> {
                                Tuple2<Mono<Boolean>, Disposable> tuple = deleteDedupObjectFile(storagePool, f, list, null, isLifecycleClear);
                                disposableList.add(tuple.var2);
                                return tuple.var1;
                            });
                })
                .doOnError(e -> {
                    if (request != null) {
                        dealException(request, e);
                    }
                })
                .collectList()
                .map(l -> true);
    }


    /**
     * 删除重删信息
     *
     * @param storagePool 索引池
     * @param key         重删信息key
     * @return
     */
    public static Tuple2<Mono<Boolean>, Disposable> deleteDedupObjectFile(StoragePool storagePool, String key, List<Tuple3<String, String, String>> nodeList, String needDeleteFileName, boolean isLifecycleClear) {
        //获取重删路径,删除
        String sourceKey = key.split(ROCKS_FILE_META_PREFIX)[0];
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("dedupKey", Json.encode(Collections.singletonList(key)));
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(t -> {
                    SocketReqMsg msg0 = msg.copy()
                            .put("lun", t.var2)
                            .put("curVnode", t.var3);
                    return msg0;
                }).collect(Collectors.toList());
        long[] size = new long[2];
        size[0] = nodeList.size();
        // 删除重删信息时各节点返回的重删信息
        String[] respDedupInfo = new String[1];
        // 根据重删key获取数据池前缀---当所有节点返回的重删信息都为空时使用
        String storage = Utils.getStorageByDeduplicateKey(key);
        // 存在重删信息已经全部删除的节点
        boolean[] retryDeleteDedup = new boolean[]{false};
        // 存在重删信息还未全部删除的节点
        boolean[] isAllDeleteDedup = new boolean[]{true};

        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
//        if (StringUtils.isEmpty(poolName)) {
//            String strategyName = "storage_" + storagePool.getVnodePrefix();
//            poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//        }
//        String poolQueueTag = poolName;

        ResponseInfo<Tuple2> responseInfo = ClientTemplate.oneResponse(msgs, DELONE_DEDUPLICATE_META, Tuple2.class, nodeList);
        MonoProcessor<Boolean> processor = MonoProcessor.create();

        Disposable subscribe = responseInfo.responses
                .doOnNext(p -> {

                })
                .subscribe(p -> {
                    //Todo 根据返回信息判断是否需要删除真实的文件和重删信息
                    if (ERROR.equals(p.var2)) {
                        size[0]--;
                        return;
                    }
                    //返回为空,表示该节点上文件已删除,count-1
                    if (StringUtils.isEmpty((String) p.var3.var1)) {
//                        log.info("no info");
                        size[0]--;
                        return;
                    }
                    if (p.var3.var1 != null) {
                        respDedupInfo[0] = (String) p.var3.var1;
                    }

                    long count = (Integer) p.var3.var2;
                    log.debug("cc = {}", p.var3.var2);
                    //返回的数目少于2一次,size-1,
                    if (count < 2) {
                        size[0]--;
                        retryDeleteDedup[0] = true;
                    } else {
                        isAllDeleteDedup[0] = false;
                    }
                }, e -> log.error("", e), () -> {

                    // 有节点异常，并且正常节点重删信息已全部删除 1x1  2x2 0x0 2x0 1x0 2xx
                    if (responseInfo.successNum != storagePool.getK() + storagePool.getM() && !(retryDeleteDedup[0] && !isAllDeleteDedup[0])) {
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                .put("dedupKey", Json.encode(new String[]{key}))
                                .put("storage", storagePool.getVnodePrefix())
                                .put("nodeList", Json.encode(nodeList))
                                .put("isLifecycleClear", String.valueOf(isLifecycleClear))
                                .put("poolQueueTag", poolQueueTag);
                        publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_DELETE_DEDUP_OBJECT);
                    }
                    if (size[0] == 0 && responseInfo.successNum >= storagePool.getK()) {// 111  000  00x 1x1 1x0 110 100
                        DedupMeta dedupInfo = StringUtils.isNotEmpty(respDedupInfo[0]) ? Json.decodeValue(respDedupInfo[0], DedupMeta.class) : new DedupMeta().setFileName(needDeleteFileName).setStorage(storage);
                        if (StringUtils.isEmpty(dedupInfo.fileName)) {
                            processor.onNext(true);
                            return;
                        }
                        StoragePool storageDataPool = StoragePoolFactory.getStoragePool(dedupInfo.storage, null);
                        getDeduplicateMeta("", storagePool.getVnodePrefix(), key, nodeList, null)
                                .flatMap(dedupMeta -> {
                                    String fileName = dedupMeta.fileName;
                                    //删除前获取重删数据,如果不存在,删除源文件;如果存在且和要删除的fileName不同,删除源文件
                                    if (StringUtils.isNotEmpty(fileName) && fileName.equals(dedupInfo.fileName)) {
                                        //需要发送到error的节点
                                        if (responseInfo.successNum == storagePool.getK() + storagePool.getM()) {
                                            SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                                    .put("dedupKey", Json.encode(new String[]{key}))
                                                    .put("storage", storagePool.getVnodePrefix())
                                                    .put("nodeList", Json.encode(nodeList))
                                                    .put("isLifecycleClear", String.valueOf(isLifecycleClear))
                                                    .put("poolQueueTag", poolQueueTag);
                                            Tuple2<PayloadMetaType, Tuple2>[] res = responseInfo.res;
                                            Tuple2 var2 = res[0].var2;
                                            res[0] = new Tuple2<>(ERROR, var2);
                                            publishEcError(res, nodeList, errorMsg, ERROR_DELETE_DEDUP_OBJECT);
                                        }
                                        return Mono.empty();
                                    } else {
                                        log.debug("storage pref {}", dedupInfo.fileName);
                                        if (StringUtils.isEmpty(dedupInfo.fileName)) {
                                            return Mono.just(true);
                                        } else {
                                            return isLifecycleClear ? ErasureClient.lifecycleDeleteFile(storageDataPool, Collections.singletonList(dedupInfo.fileName).toArray(new String[0]), null)
                                                    : deleteObjectFile(storageDataPool, new String[]{dedupInfo.fileName}, null);
                                        }
                                    }
                                }).subscribe(b -> processor.onNext(true));

                    } else if ((retryDeleteDedup[0] && !isAllDeleteDedup[0])) { //部分节点重删信息已全部删除 122 112  1x2  012 110
                        DedupMeta dedupInfo = Json.decodeValue(respDedupInfo[0], DedupMeta.class);
                        String[] lun = new String[1];
                        nodeList.forEach(t -> {
                            if (CURRENT_IP.equals(t.var1)) {
                                lun[0] = t.var2;
                            }
                        });
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                .put("dedupKey", Json.encode(new String[]{key}))
                                .put("storage", storagePool.getVnodePrefix())
                                .put("nodeList", Json.encode(nodeList))
                                .put("poolQueueTag", poolQueueTag)
                                .put("isLifecycleClear", String.valueOf(isLifecycleClear))
                                .put("fileName", dedupInfo.fileName)
                                .put("lun", lun[0]);
                        ObjectPublisher.publish(CURRENT_IP, errorMsg, ERROR_DELETE_DEDUP_OBJECT);
                        processor.onNext(true);
                    } else { // 220 200   1xx 0xx xxx
                        processor.onNext(true);
                    }
                });

        return new Tuple2<>(processor, subscribe);

    }

    public static Flux<byte[]> getObject(StoragePool storagePool, MetaData metaData, long start, long end,
                                         List<Tuple3<String, String, String>> nodeList,
                                         UnicastProcessor<Long> streamController, MsHttpRequest request, MsClientRequest clientRequest) {
        if (metaData.partUploadId == null) {
            // 聚合文件需要加上偏移
            if (StringUtils.isNotEmpty(metaData.aggregationKey)) {
                start = metaData.offset + start;
                end = metaData.offset + end;
            }
            //缓存池没有刷盘仍然按照之前的流程下载
            if (StringUtils.isNotEmpty(metaData.duplicateKey) && !metaData.storage.startsWith("cache")) {
                Map<String, String> sysMetaMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
                });
                String etag = sysMetaMap.get(ETAG);
                String vnode = metaData.fileName.split(File.separator)[1].split("_")[0];
                long finalStart = start;
                long finalEnd = end;
                return storagePool.mapToNodeInfo(vnode)
                        .flatMapMany(nodeList1 -> ECUtils.getObject(storagePool, metaData.fileName, metaData.smallFile, finalStart,
                                finalEnd, metaData.endIndex + 1, nodeList1, streamController, request, clientRequest));
            }
            return ECUtils.getObject(storagePool, metaData.fileName, metaData.smallFile, start, end, metaData.endIndex + 1, nodeList, streamController, request, clientRequest);
        } else {
            GetMultiPartObjectClientHandler getMultiPartObjectClientHandler =
                    new GetMultiPartObjectClientHandler(storagePool, start, end, metaData.endIndex + 1, metaData.partInfos, streamController, request, clientRequest);

            return getMultiPartObjectClientHandler.getBytes();
        }
    }

    public static Mono<Boolean> updateFileMeta(MetaData metaData, String keySuffix,
                                               MonoProcessor<Boolean> recoverData, MsHttpRequest request, Map<String, ChunkFile> chunkFileMap) {
        StoragePool storagePool = StoragePoolFactory.getStoragePool(metaData);
        if (metaData.endIndex + 1 - metaData.startIndex == 0) {
            return Mono.just(true);
        }
        StoragePool bucketPool = StoragePoolFactory.getStoragePool(StorageOperate.META);
        List<Tuple3<String, List<Tuple3<String, String, String>>, ChunkFile>> task = new LinkedList<>();
        Queue<Disposable> disposableQueue = new ConcurrentLinkedQueue<>();
        Queue<String> needDelete = new LinkedList<>();
        Queue<Tuple3<Tuple2<PayloadMetaType, String>[], List<Tuple3<String, String, String>>,
                SocketReqMsg>> recoverQueue = new ConcurrentLinkedQueue<>();
        String[] vnodePrefix = new String[1];
        vnodePrefix[0] = storagePool.getVnodePrefix();
        if (metaData.partUploadId == null) {
            needDelete.add(metaData.fileName.split(ROCKS_FILE_META_PREFIX)[0] + keySuffix);
            if (StringUtils.isNotEmpty(metaData.duplicateKey)) {
                List<Tuple3<String, String, String>> nodeList = bucketPool.mapToNodeInfo(bucketPool.getDedupVnode(metaData.duplicateKey)).block();
                task.add(new Tuple3<>(Json.encode(metaData), nodeList));
            } else {
                List<Tuple3<String, String, String>> nodeList = storagePool.mapToNodeInfo(storagePool.getObjectVnodeId(metaData.fileName)).block();
                task.add(new Tuple3<>(Json.encode(metaData), nodeList));
            }
        } else {
            String uploadId = metaData.partUploadId;
            PartInfo[] partInfos = metaData.partInfos;
            long start = metaData.startIndex;
            long end = metaData.endIndex;
            metaData.partUploadId = null;
            metaData.partInfos = null;
            for (PartInfo partInfo : partInfos) {
                if (partInfo.partSize == 0 || StringUtils.isEmpty(partInfo.fileName)) {
                    continue;
                }
                metaData.startIndex = 0;
                metaData.endIndex = partInfo.partSize - 1;
                metaData.fileName = partInfo.fileName;
                if (StringUtils.isNotEmpty(partInfo.deduplicateKey)) {
                    metaData.setDuplicateKey(partInfo.deduplicateKey);
                    List<Tuple3<String, String, String>> nodeList = bucketPool.mapToNodeInfo(bucketPool.getDedupVnode(partInfo.deduplicateKey)).block();
                    task.add(new Tuple3<>(Json.encode(metaData), nodeList));
                } else if (metaData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                    String vnode = bucketPool.getBucketVnodeId(metaData.bucket);
                    List<Tuple3<String, String, String>> chunkNodeList = bucketPool.mapToNodeInfo(vnode).block();
                    if (chunkFileMap != null && chunkFileMap.get(metaData.fileName) != null) {
                        ChunkFile chunkFile = chunkFileMap.get(metaData.fileName);
                        task.add(new Tuple3<>(Json.encode(metaData), chunkNodeList, chunkFile));
                    } else {
                        task.add(new Tuple3<>(Json.encode(metaData), chunkNodeList));
                    }
                    vnodePrefix[0] = bucketPool.getVnodePrefix();
                } else {
                    if (!metaData.storage.equals(partInfo.storage)) {
                        storagePool = StoragePoolFactory.getStoragePool(partInfo);
                        vnodePrefix[0] = storagePool.getVnodePrefix();
                        metaData.storage = partInfo.storage;
                    }
                    String metaJson = Json.encode(metaData);
                    List<Tuple3<String, String, String>> nodeList = storagePool.mapToNodeInfo(storagePool.getObjectVnodeId(metaData.fileName)).block();
                    task.add(new Tuple3<>(metaJson, nodeList));
                }
                needDelete.add(metaData.fileName.split(ROCKS_FILE_META_PREFIX)[0] + keySuffix);
            }

            metaData.startIndex = start;
            metaData.endIndex = end;
            metaData.partUploadId = uploadId;
            metaData.partInfos = partInfos;
            metaData.setDuplicateKey(null);
            metaData.setFileName(null);
        }

        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> {
            for (Disposable disposable : disposableQueue) {
                if (disposable != null) {
                    disposable.dispose();
                }
            }
        }));

        StoragePool finalStoragePool = storagePool;
        return Flux.fromStream(task.stream())
                .flatMap(t -> {
                    MetaData data = Json.decodeValue(t.var1, MetaData.class);
                    if (StringUtils.isNotEmpty(data.duplicateKey)) {
                        String etag = data.duplicateKey.split(File.separator)[1];
                        data.setFileName(data.fileName.split(ROCKS_FILE_META_PREFIX)[0] + keySuffix);
                        return getAndUpdateDeduplicate(bucketPool, data.duplicateKey, t.var2, request, recoverData, data, etag);
                    } else if (data.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                        return updateFileMeta(bucketPool, keySuffix, t.var2, disposableQueue, t.var1, recoverQueue, t.var3);
                    } else {
                        return updateFileMeta(finalStoragePool, keySuffix, t.var2, disposableQueue, t.var1, recoverQueue, null);
                    }
                })
                .collectList()
                .map(l -> l.stream().allMatch(b -> b))
                .doOnNext(b -> {
//                    String storageName = "storage_" + storagePool.getVnodePrefix();
//                    String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
                    String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(vnodePrefix[0]);
//                    if (StringUtils.isEmpty(poolName)) {
//                        String strategyName = "storage_" + vnodePrefix[0];
//                        poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                    }
//                    String poolQueueTag = poolName;
                    if (b) {
                        if (!recoverQueue.isEmpty()) {
                            recoverData.subscribe(recover -> {
                                if (recover) {
                                    for (Tuple3<Tuple2<PayloadMetaType, String>[], List<Tuple3<String, String, String>>,
                                            SocketReqMsg> tuple3 : recoverQueue) {
                                        MetaData metaNew = Json.decodeValue(tuple3.var3.get("value"), MetaData.class);
                                        String suffix = tuple3.var3.get("key");

                                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                                .put("errorChunksList", tuple3.var3.get("errorChunksList"))
                                                .put("suffix", suffix)
                                                .put("bucket", metaData.bucket)
                                                .put("object", metaData.key)
                                                .put("fileName", metaNew.fileName)
                                                .put("versionId", metaData.versionId)
                                                .put("storage", vnodePrefix[0])
                                                .put("stamp", metaData.stamp)
                                                .put("endIndex", String.valueOf(metaNew.endIndex))
                                                .put("value", tuple3.var3.get("value"))
                                                .put("poolQueueTag", poolQueueTag);
//                                        CryptoUtils.generateKeyPutToMsg(metaNew.crypto, errorMsg);
                                        Optional.ofNullable(metaData.snapshotMark).ifPresent(v -> errorMsg.put("snapshotMark", v));
                                        tuple3.var3.put("endIndex", String.valueOf(metaData.endIndex));
                                        publishEcError(tuple3.var1, tuple3.var2, errorMsg, ERROR_PUT_FILE_META);
                                    }
                                }
                            });
                        }
                    } else {
                        //回退
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0);
                        errorMsg.put("fileName", Json.encode(needDelete.toArray(new String[0])))
                                .put("storage", vnodePrefix[0])
                                .put("poolQueueTag", poolQueueTag);
                        ObjectPublisher.publish(CURRENT_IP, errorMsg, ERROR_DELETE_OBJECT_FILE);
                    }
                });
    }

    private static Mono<Boolean> updateFileMeta(StoragePool storagePool, String keySuffix, List<Tuple3<String, String, String>> nodeList,
                                                Queue<Disposable> disposable, String metaJson,
                                                Queue<Tuple3<Tuple2<PayloadMetaType, String>[], List<Tuple3<String, String, String>>, SocketReqMsg>> recoverQueue,
                                                ChunkFile chunkFile) {
        List<SocketReqMsg> msgs = ECUtils.mapToMsg(keySuffix, metaJson, nodeList);
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, UPDATE_FILE_META, String.class, nodeList);
        List<Integer> errorChunksList = new ArrayList<>(storagePool.getM());
        if (chunkFile != null) {
            msgs = msgs.stream().map(msg -> msg.put("chunkFile", Json.encode(chunkFile))).collect(Collectors.toList());
        }
        List<SocketReqMsg> finalMsgs = msgs;
        Map<String, Integer> map = new HashMap<>(1);
        Disposable subscribe = responseInfo.responses.subscribe(s -> {
            int c = map.getOrDefault("emptyNum", 0);
            if (s.var2.equals(SUCCESS)) {
                if ("0".equals(s.var3)) {
                    map.put("emptyNum", c + 1);
                    errorChunksList.add(s.var1);
                }
            }
            if (s.var2.equals(ERROR)) {
                errorChunksList.add(s.var1);
            }
        }, e -> log.error("", e), () -> {
            responseInfo.successNum -= map.getOrDefault("emptyNum", 0);
            if (responseInfo.successNum == storagePool.getK() + storagePool.getM()) {
                res.onNext(true);
            } else if (responseInfo.successNum >= storagePool.getK()) {
                if (responseInfo.successNum > 0) {
                    finalMsgs.get(0).put("errorChunksList", Json.encode(errorChunksList));
                    for (Tuple2<PayloadMetaType, String> re : responseInfo.res) {
                        if ("0".equals(re.var2) && SUCCESS.equals(re.var1)) {
                            re.var1 = ERROR;
                        }
                    }
                    recoverQueue.add(new Tuple3<>(responseInfo.res, nodeList, finalMsgs.get(0)));
                }
                res.onNext(true);
            } else {
                res.onNext(false);
            }
        });

        if (disposable != null) {
            disposable.add(subscribe);
        }

        return res;
    }

    public static Mono<MetaData> getObjectMetaByUploadId(String bucket, String key, String uploadId,
                                                         List<Tuple3<String, String, String>> nodeList) {
        String vnode = nodeList.get(0).var3;
        String rocksKey = Utils.getMetaDataKey(vnode, bucket, key, null);
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("key", rocksKey)
                        .put("object", key)
                        .put("uploadId", uploadId)
                        .put("lun", tuple.var2))
                .collect(Collectors.toList());

        MonoProcessor<MetaData> processor = MonoProcessor.create();
        MetaData[] real = (MetaData[]) Array.newInstance(MetaData.class, 1);
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        ResponseInfo<MetaData> responseInfo = ClientTemplate.oneResponse(msgs, GET_METADATA_BY_UPLOADID, MetaData.class, nodeList);
        Disposable[] disposables = new Disposable[2];
        //不释放该disposable，在2000并发10ms socketTimeout时会堆积
        disposables[1] = responseInfo.responses
//                .timeout(Duration.ofSeconds(15))
                .doOnCancel(() -> {
                    log.info("close disposables.");
                    for (Disposable disposable : disposables) {
                        if (disposable != null) {
                            disposable.dispose();
                        }
                    }
                })
                .subscribe(tuple -> {
                    if (tuple.var2.equals(SUCCESS)) {
                        real[0] = tuple.var3;
                    } else if (tuple.var2.equals(NOT_FOUND) && real[0] == null) {
                        real[0] = tuple.var3;
                    }
                }, e -> log.error("", e), () -> {
                    try {
                        if (real[0] != null && !real[0].equals(NOT_FOUND_META)) {
                            processor.onNext(real[0]);
                            return;
                        }
                        if (responseInfo.errorNum > pool.getM()) {
                            processor.onNext(ERROR_META);
                            return;
                        }

                        if (responseInfo.errorNum == 0) {
                            processor.onNext(NOT_FOUND_META);
                        }
                        processor.onNext(real[0]);
                    } catch (Exception e) {
                        //捕获processor中抛出的错误
                    }
                });
        return processor.onErrorReturn(ERROR_META);
    }

    public static Mono<Boolean> updateBucketCapacity(String bucket, String num, String cap, List<Tuple3<String, String, String>> nodeList) {
        String vnode = nodeList.get(0).var3;
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("lun", tuple.var2)
                        .put("bucket", bucket)
                        .put("objNum", num)
                        .put("objCap", cap)
                        .put("vnode", vnode))
                .collect(Collectors.toList());

        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, UPDATE_BUCKET_CAPACITY, String.class, nodeList);

        responseInfo.responses.subscribe(s -> {
                },
                e -> log.error("", e),
                () -> {
                    if (responseInfo.successNum >= storagePool.getK()) {
                        res.onNext(true);
                    } else {
                        res.onNext(false);
                    }
                });
        return res;
    }


    public static Mono<Boolean> copyCryptoData(StoragePool sourcePool, StoragePool targetPool, MetaData sourceMeta, MetaData targetMeta,
                                               MsHttpRequest request, MonoProcessor<Boolean> recoverDataProcessor, String requestId, String metaKey) {
        if (sourceMeta.getPartInfos() != null) {
            return partCopyCryptoData(sourcePool, targetPool, sourceMeta, targetMeta, request, recoverDataProcessor, metaKey);
        }
        String newFileName = Utils.getObjFileName(targetPool, targetMeta.getBucket(), targetMeta.getKey(), requestId);
        targetMeta.setFileName(newFileName);
        List<Tuple3<String, String, String>> sourceNodeList = sourcePool.mapToNodeInfo(sourcePool.getObjectVnodeId(sourceMeta)).block();
        String newObjectVnodeId = targetPool.getObjectVnodeId(newFileName);
        List<Tuple3<String, String, String>> targetNodeList = targetPool.mapToNodeInfo(newObjectVnodeId).block();

        Encoder ecEncodeHandler = targetPool.getEncoder();
        UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        int ontPutBytes = targetPool.getK() * targetPool.getPackageSize();
        UnicastProcessor<Tuple2<Integer, Integer>> next = UnicastProcessor.create(Queues.<Tuple2<Integer, Integer>>unboundedMultiproducer().get());
        AtomicInteger exceptGetNum = new AtomicInteger(sourcePool.getK());
        AtomicInteger waitEncodeBytes = new AtomicInteger(0);
        AtomicInteger exceptPutNum = new AtomicInteger(0);
        next.subscribe(t -> {
            if (t.var1 > 0) {
                exceptGetNum.decrementAndGet();
                waitEncodeBytes.addAndGet(t.var2);
                int n = waitEncodeBytes.get() / ontPutBytes;
                exceptPutNum.addAndGet(n);
                waitEncodeBytes.addAndGet(-n * ontPutBytes);
            } else {
                exceptPutNum.decrementAndGet();
            }

            while (exceptPutNum.get() == 0 && exceptGetNum.get() <= 0) {
                for (int j = 0; j < sourcePool.getK(); j++) {
                    streamController.onNext(-1L);
                }
                exceptGetNum.addAndGet(sourcePool.getK());
            }
        });
        ECUtils.getObject(sourcePool, sourceMeta.getFileName(), sourceMeta.isSmallFile(), sourceMeta.offset + sourceMeta.getStartIndex(), sourceMeta.offset + sourceMeta.getEndIndex(), sourceMeta.getEndIndex() + 1, sourceNodeList,
                        streamController, request, null)
                .doOnError(e -> {
                    for (int i = 0; i < ecEncodeHandler.data().length; i++) {
                        ecEncodeHandler.data()[i].onError(e);
                    }
                })
                .doOnComplete(ecEncodeHandler::complete)
                .subscribe(bytes -> {
                    next.onNext(new Tuple2<>(1, bytes.length));
                    ecEncodeHandler.put(bytes);
                }, e -> log.error("", e));
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("fileName", newFileName)
                .put("metaKey", metaKey)
                .put("compression", targetPool.getCompression());
        if (isEnableCacheOrderFlush(targetPool)) {
            msg.put("flushStamp", targetMeta.getStamp());
        }
        CryptoUtils.generateKeyPutToMsg(targetMeta.crypto, msg);
        List<UnicastProcessor<Payload>> processors = new ArrayList<>();
        for (int i = 0; i < targetNodeList.size(); i++) {
            Tuple3<String, String, String> tuple = targetNodeList.get(i);
            SocketReqMsg msg0 = msg.copy().put("vnode", tuple.var3).put("lun", tuple.var2);

            UnicastProcessor<Payload> processor = UnicastProcessor.create();
            processor.onNext(DefaultPayload.create(Json.encode(msg0), START_PUT_OBJECT.name()));


            ecEncodeHandler.data()[i].subscribe(bytes -> {
                        processor.onNext(DefaultPayload.create(bytes, PUT_OBJECT.name().getBytes()));
                    },
                    e -> {
                        log.error("", e);
                        processor.onNext(DefaultPayload.create("", ERROR.name()));
                        processor.onComplete();
                    },
                    () -> {
                        processor.onNext(DefaultPayload.create("", COMPLETE_PUT_OBJECT.name()));
                        processor.onComplete();
                    });

            processors.add(processor);
        }
        ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(processors, String.class, targetNodeList);
        List<Integer> errorChunksList = new ArrayList<>(targetPool.getM());
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(targetPool.getVnodePrefix());
        AtomicInteger putNum = new AtomicInteger();
        Disposable disposable = responseInfo.responses.subscribe(s -> {
                    if (s.var2.equals(ERROR)) {
                        errorChunksList.add(s.var1);
                    }

                    if (putNum.incrementAndGet() == targetNodeList.size()) {
                        next.onNext(new Tuple2<>(-1, 0));
                        putNum.set(errorChunksList.size());
                    }
                },
                e -> log.error("", e),
                () -> {
                    if (responseInfo.successNum == targetPool.getK() + targetPool.getM()) {
                        res.onNext(true);
                    } else if (responseInfo.successNum >= targetPool.getK()) {
                        res.onNext(true);
                        recoverDataProcessor.subscribe(s -> {
                            //若至少成功写了一个partInfo，发送修复数据的消息
                            if (s) {
                                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                        .put("errorChunksList", Json.encode(errorChunksList))
                                        .put("bucket", targetMeta.bucket)
                                        .put("object", targetMeta.key)
                                        .put("fileName", targetMeta.fileName)
                                        .put("versionId", targetMeta.versionId)
                                        .put("storage", targetPool.getVnodePrefix())
                                        .put("stamp", targetMeta.stamp)
                                        .put("fileSize", String.valueOf(ecEncodeHandler.size()))
                                        .put("poolQueueTag", poolQueueTag)
                                        .put("fileOffset", "");
                                Optional.ofNullable(targetMeta.snapshotMark).ifPresent(v -> errorMsg.put("snapshotMark", v));
                                Optional.ofNullable(msg.get("flushStamp")).ifPresent(v -> errorMsg.put("flushStamp", v));
                                CryptoUtils.putCryptoInfoToMsg(targetMeta.crypto, msg.get("secretKey"), errorMsg);

                                publishEcError(responseInfo.res, targetNodeList, errorMsg, ERROR_PUT_OBJECT_FILE);
                            }
                        });
                    } else {
                        res.onNext(false);
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                .put("errorChunksList", Json.encode(errorChunksList))
                                .put("bucket", targetMeta.bucket)
                                .put("object", targetMeta.key)
                                .put("fileName", targetMeta.fileName)
                                .put("versionId", targetMeta.versionId)
                                .put("storage", targetPool.getVnodePrefix())
                                .put("stamp", targetMeta.stamp)
                                .put("poolQueueTag", poolQueueTag);
                        ECUtils.publishEcError(responseInfo.res, targetNodeList, errorMsg, ERROR_ROLL_BACK_FILE);
                    }
                });

        Optional.ofNullable(request)
                .ifPresent(r -> {
                    r.addResponseCloseHandler(v -> {
                        for (UnicastProcessor<Payload> processor : processors) {
                            processor.onNext(ERROR_PAYLOAD);
                            processor.onComplete();
                        }
                        disposable.dispose();
                    });
                });
        return res;
    }

    public static Mono<Boolean> partCopyCryptoData(StoragePool sourcePool, StoragePool targetPool, MetaData sourceMeta, MetaData targetMeta,
                                                   MsHttpRequest request, MonoProcessor<Boolean> recoverDataProcessor, String metaKey) {
        if (sourceMeta.inode > 0) {
            if (targetPool.getVnodePrefix().startsWith("cache")) {
                return processFilePartCopyToSingle(sourcePool, targetPool, sourceMeta, targetMeta, request, recoverDataProcessor, metaKey, 2);
            }
            return processFilePartCopy(sourcePool, targetPool, sourceMeta, targetMeta, request, recoverDataProcessor, metaKey);
        }
        MonoProcessor<Boolean> res = MonoProcessor.create();
        int ontPutBytes = targetPool.getK() * targetPool.getPackageSize();

        PartInfo[] partInfos = sourceMeta.getPartInfos();
        List<UnicastProcessor<Payload>> processors = new ArrayList<>();
        List<Disposable> disposables = new ArrayList<>();
        AtomicInteger successNum = new AtomicInteger(0);
        AtomicInteger totalNum = new AtomicInteger(partInfos.length);

        AtomicLong fileOffset = new AtomicLong(0);
        Flux.fromArray(partInfos)
                .publishOn(DISK_SCHEDULER)
                .index()
                .flatMap(tuple2 -> {
                    PartInfo partInfo = tuple2.getT2();
                    Long index = tuple2.getT1();
                    StoragePool finalStoragePool = sourcePool;
                    if (!sourcePool.getVnodePrefix().equals(partInfo.storage)) {
                        finalStoragePool = StoragePoolFactory.getStoragePool(partInfo);
                    }
                    String partName = Utils.getPartFileName(targetPool, targetMeta.getBucket(), targetMeta.getKey(), targetMeta.getPartUploadId(), String.valueOf(partInfo.getPartNum()),
                            RequestBuilder.getRequestId());
                    List<Tuple3<String, String, String>> sourceNodeList = finalStoragePool.mapToNodeInfo(finalStoragePool.getObjectVnodeId(partInfo.getFileName())).block();
                    String newObjectVnodeId = targetPool.getObjectVnodeId(partName);
                    List<Tuple3<String, String, String>> targetNodeList = targetPool.mapToNodeInfo(newObjectVnodeId).block();

                    List<UnicastProcessor<Payload>> partProcessors = new ArrayList<>();
                    UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
                    Encoder ecEncodeHandler = targetPool.getEncoder();
                    UnicastProcessor<Tuple2<Integer, Integer>> next = UnicastProcessor.create(Queues.<Tuple2<Integer, Integer>>unboundedMultiproducer().get());
                    AtomicInteger exceptGetNum = new AtomicInteger(finalStoragePool.getK());
                    AtomicInteger waitEncodeBytes = new AtomicInteger(0);
                    AtomicInteger exceptPutNum = new AtomicInteger(0);
                    MonoProcessor<Boolean> partRes = MonoProcessor.create();
                    StoragePool finalStoragePool0 = finalStoragePool;
                    next.subscribe(t -> {
                        if (t.var1 > 0) {
                            exceptGetNum.decrementAndGet();
                            waitEncodeBytes.addAndGet(t.var2);
                            int n = waitEncodeBytes.get() / ontPutBytes;
                            exceptPutNum.addAndGet(n);
                            waitEncodeBytes.addAndGet(-n * ontPutBytes);
                        } else {
                            exceptPutNum.decrementAndGet();
                        }

                        while (exceptPutNum.get() == 0 && exceptGetNum.get() <= 0) {
                            for (int j = 0; j < finalStoragePool0.getK(); j++) {
                                streamController.onNext(-1L);
                            }
                            exceptGetNum.addAndGet(finalStoragePool0.getK());
                        }
                    });

                    ECUtils.getObject(finalStoragePool, partInfo.getFileName(), false, 0, partInfo.getPartSize() - 1, partInfo.getPartSize(), sourceNodeList, streamController, request, null)
                            .doOnError(e -> {
                                for (int i = 0; i < ecEncodeHandler.data().length; i++) {
                                    ecEncodeHandler.data()[i].onError(e);
                                }
                            })
                            .doOnComplete(ecEncodeHandler::complete)
                            .subscribe(bytes -> {
                                next.onNext(new Tuple2<>(1, bytes.length));
                                ecEncodeHandler.put(bytes);
                            }, e -> log.error("", e));

                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("fileName", partName)
                            .put("compression", targetPool.getCompression())
                            .put("metaKey", metaKey);

                    if (targetPool.getVnodePrefix().startsWith("cache")) {
                        msg.put("fileOffset", String.valueOf(fileOffset.getAndAdd(partInfo.getPartSize())));
                    }

                    CryptoUtils.generateKeyPutToMsg(targetMeta.crypto, msg);


                    for (int i = 0; i < targetNodeList.size(); i++) {
                        Tuple3<String, String, String> tuple = targetNodeList.get(i);
                        SocketReqMsg msg0 = msg.copy().put("vnode", tuple.var3).put("lun", tuple.var2);

                        UnicastProcessor<Payload> processor = UnicastProcessor.create();
                        processor.onNext(DefaultPayload.create(Json.encode(msg0), START_PART_UPLOAD.name()));


                        ecEncodeHandler.data()[i].subscribe(bytes -> processor.onNext(DefaultPayload.create(bytes, PART_UPLOAD.name().getBytes())),
                                e -> {
                                    log.error("", e);
                                    processor.onNext(DefaultPayload.create("", ERROR.name()));
                                    processor.onComplete();
                                },
                                () -> {
                                    processor.onNext(DefaultPayload.create("", COMPLETE_PART_UPLOAD.name()));
                                    processor.onComplete();
                                });

                        partProcessors.add(processor);
                    }
                    processors.addAll(partProcessors);
                    ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(partProcessors, String.class, targetNodeList);
                    List<Integer> errorChunksList = new ArrayList<>(targetPool.getM());
//                    String storageName = "storage_" + targetPool.getVnodePrefix();
//                    String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
                    String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(targetPool.getVnodePrefix());
//                    if (StringUtils.isEmpty(poolName)) {
//                        String strategyName = "storage_" + targetPool.getVnodePrefix();
//                        poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                    }
//                    String poolQueueTag = poolName;
                    AtomicInteger putNum = new AtomicInteger();

                    Disposable disposable = responseInfo.responses
                            .subscribe(s -> {
                                        if (s.var2.equals(ERROR)) {
                                            errorChunksList.add(s.var1);
                                        }

                                        if (putNum.incrementAndGet() == targetNodeList.size()) {
                                            next.onNext(new Tuple2<>(-1, 0));
                                            putNum.set(errorChunksList.size());
                                        }
                                    },
                                    e -> {
                                        log.error("", e);
                                        partRes.onNext(false);
                                    },
                                    () -> {
                                        PartInfo partInfo0 = targetMeta.getPartInfos()[index.intValue()].setFileName(partName);
                                        if (responseInfo.successNum == targetPool.getK() + targetPool.getM()) {
                                            partRes.onNext(true);
                                        } else if (responseInfo.successNum >= targetPool.getK()) {
                                            partRes.onNext(true);
                                            SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                                    .put("storage", targetPool.getVnodePrefix())
                                                    .put("bucket", partInfo0.bucket)
                                                    .put("object", partInfo0.object)
                                                    .put("uploadId", partInfo0.uploadId)
                                                    .put("partNum", partInfo0.partNum)
                                                    .put("fileName", partInfo0.fileName)
                                                    .put("endIndex", String.valueOf(partInfo0.partSize - 1))
                                                    .put("errorChunksList", Json.encode(errorChunksList))
                                                    .put("versionId", partInfo0.versionId)
                                                    .put("poolQueueTag", poolQueueTag);
                                            Optional.ofNullable(targetMeta.snapshotMark).ifPresent(v -> errorMsg.put("snapshotMark", v));
                                            CryptoUtils.putCryptoInfoToMsg(targetMeta.crypto, msg.get("secretKey"), errorMsg);

                                            publishEcError(responseInfo.res, targetNodeList, errorMsg, ERROR_PART_UPLOAD_FILE);
                                        } else {
                                            partRes.onNext(false);
                                            SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                                    .put("bucket", partInfo0.getBucket())
                                                    .put("object", partInfo0.getObject())
                                                    .put("fileName", partInfo0.getFileName())
                                                    .put("storage", targetPool.getVnodePrefix())
                                                    .put("poolQueueTag", poolQueueTag);
                                            ECUtils.publishEcError(responseInfo.res, targetNodeList, errorMsg, ERROR_ROLL_BACK_FILE);
                                        }
                                    });
                    disposables.add(disposable);
                    return partRes;
                }, 1, 1).subscribe(b -> {
                    if (b) {
                        successNum.getAndIncrement();
                    }
                }, e -> {
                    log.error("", e);
                    res.onNext(false);
                }, () -> {
//                    targetMeta.setPartInfos(partInfos);
                    if (successNum.get() == totalNum.get()) {
                        res.onNext(true);
                    } else {
                        res.onNext(false);
                    }
                });
        try {
            request.addResponseCloseHandler(v -> {
                for (UnicastProcessor<Payload> processor : processors) {
                    processor.onNext(ERROR_PAYLOAD);
                    processor.onComplete();
                }
                for (Disposable disposable : disposables) {
                    disposable.dispose();
                }
            });
        } catch (Exception e) {
            log.error("", e);
            res.onNext(false);
        }

        return res;
    }


    public static Mono<Boolean> processFilePartCopy(StoragePool sourcePool, StoragePool targetPool, MetaData sourceMeta, MetaData targetMeta,
                                                    MsHttpRequest request, MonoProcessor<Boolean> recoverDataProcessor, String metaKey) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        int ontPutBytes = targetPool.getK() * targetPool.getPackageSize();

        PartInfo[] partInfos = sourceMeta.getPartInfos();
        List<UnicastProcessor<Payload>> processors = new ArrayList<>();
        List<Disposable> disposables = new ArrayList<>();
        AtomicInteger successNum = new AtomicInteger(0);
        AtomicInteger totalNum = new AtomicInteger(partInfos.length);

        AtomicLong fileOffset = new AtomicLong(0);
        Flux.fromArray(partInfos)
                .publishOn(DISK_SCHEDULER)
                .index()
                .flatMap(tuple2 -> {
                    PartInfo partInfo = tuple2.getT2();
                    Long index = tuple2.getT1();
                    StoragePool finalStoragePool = sourcePool;
                    if (!sourcePool.getVnodePrefix().equals(partInfo.storage)) {
                        finalStoragePool = StoragePoolFactory.getStoragePool(partInfo);
                    }
                    AtomicLong putBytesLen = new AtomicLong();
                    String partName = Utils.getPartFileName(targetPool, targetMeta.getBucket(), targetMeta.getKey(), targetMeta.getPartUploadId(), String.valueOf(partInfo.getPartNum()),
                            RequestBuilder.getRequestId());
                    String tempFileName = partInfo.fileName;
                    if (StringUtils.isBlank(tempFileName)) {
                        StorageOperate operate = new StorageOperate(StorageOperate.PoolType.DATA, "", Long.MAX_VALUE);
                        StoragePool dataPool = StoragePoolFactory.getStoragePool(operate, sourceMeta.bucket);
                        tempFileName = Utils.getPartFileName(dataPool, sourceMeta.bucket, sourceMeta.getKey(), sourceMeta.getPartUploadId(),String.valueOf(partInfo.getPartNum()),
                                RequestBuilder.getRequestId());
                    }
                    List<Tuple3<String, String, String>> sourceNodeList = finalStoragePool.mapToNodeInfo(finalStoragePool.getObjectVnodeId(tempFileName)).block();
                    String newObjectVnodeId = targetPool.getObjectVnodeId(partName);
                    List<Tuple3<String, String, String>> targetNodeList = targetPool.mapToNodeInfo(newObjectVnodeId).block();

                    List<UnicastProcessor<Payload>> partProcessors = new ArrayList<>();
                    UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
                    Encoder ecEncodeHandler = targetPool.getEncoder();
                    UnicastProcessor<Tuple2<Integer, Integer>> next = UnicastProcessor.create(Queues.<Tuple2<Integer, Integer>>unboundedMultiproducer().get());
                    AtomicInteger exceptGetNum = new AtomicInteger(0);
                    AtomicInteger waitEncodeBytes = new AtomicInteger(0);
                    AtomicInteger exceptPutNum = new AtomicInteger(0);
                    MonoProcessor<Boolean> partRes = MonoProcessor.create();
                    StoragePool finalStoragePool0 = finalStoragePool;
                    next.subscribe(t -> {
                        if (t.var1 > 0) {
                            putBytesLen.addAndGet(t.var2);
                            exceptGetNum.incrementAndGet();
                            waitEncodeBytes.addAndGet(t.var2);
                            int n = waitEncodeBytes.get() / ontPutBytes;
                            exceptPutNum.addAndGet(n);
                            waitEncodeBytes.addAndGet(-n * ontPutBytes);
                        } else {
                            exceptPutNum.decrementAndGet();
                        }
                        while (exceptPutNum.get() == 0 && exceptGetNum.get() > 0) {
                            streamController.onNext(-1L);
                            exceptGetNum.decrementAndGet();
                        }
                    });

                    ECUtils.getObject(finalStoragePool, partInfo.getFileName(), false, partInfo.offset, partInfo.getPartSize() - 1 + partInfo.offset, partInfo.getPartSize(), sourceNodeList, streamController, request, null)
                            .doOnError(e -> {
                                for (int i = 0; i < ecEncodeHandler.data().length; i++) {
                                    ecEncodeHandler.data()[i].onError(e);
                                }
                            })
                            .doOnComplete(ecEncodeHandler::complete)
                            .subscribe(bytes -> {
                                next.onNext(new Tuple2<>(1, bytes.length));
                                ecEncodeHandler.put(bytes);
                            }, e -> log.error("", e));

                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("fileName", partName)
                            .put("compression", targetPool.getCompression())
                            .put("metaKey", metaKey);

                    if (targetPool.getVnodePrefix().startsWith("cache")) {
                        msg.put("fileOffset", String.valueOf(fileOffset.getAndAdd(partInfo.getPartSize())));
                    }

                    CryptoUtils.generateKeyPutToMsg(targetMeta.crypto, msg);


                    for (int i = 0; i < targetNodeList.size(); i++) {
                        Tuple3<String, String, String> tuple = targetNodeList.get(i);
                        SocketReqMsg msg0 = msg.copy().put("vnode", tuple.var3).put("lun", tuple.var2);

                        UnicastProcessor<Payload> processor = UnicastProcessor.create();
                        processor.onNext(DefaultPayload.create(Json.encode(msg0), START_PART_UPLOAD.name()));


                        ecEncodeHandler.data()[i].subscribe(bytes -> processor.onNext(DefaultPayload.create(bytes, PART_UPLOAD.name().getBytes())),
                                e -> {
                                    log.error("", e);
                                    processor.onNext(DefaultPayload.create("", ERROR.name()));
                                    processor.onComplete();
                                },
                                () -> {
                                    processor.onNext(DefaultPayload.create("", COMPLETE_PART_UPLOAD.name()));
                                    processor.onComplete();
                                });

                        partProcessors.add(processor);
                    }
                    processors.addAll(partProcessors);
                    ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(partProcessors, String.class, targetNodeList);
                    List<Integer> errorChunksList = new ArrayList<>(targetPool.getM());
//                    String storageName = "storage_" + targetPool.getVnodePrefix();
//                    String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
                    String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(targetPool.getVnodePrefix());
//                    if (StringUtils.isEmpty(poolName)) {
//                        String strategyName = "storage_" + targetPool.getVnodePrefix();
//                        poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                    }
//                    String poolQueueTag = poolName;
                    AtomicInteger putNum = new AtomicInteger();

                    Disposable disposable = responseInfo.responses
                            .subscribe(s -> {
                                        if (s.var2.equals(ERROR)) {
                                            errorChunksList.add(s.var1);
                                        }

                                        if (putNum.incrementAndGet() == targetNodeList.size()) {
                                            next.onNext(new Tuple2<>(-1, 0));
                                            putNum.set(errorChunksList.size());
                                        }
                                    },
                                    e -> {
                                        log.error("", e);
                                        partRes.onNext(false);
                                    },
                                    () -> {
                                        PartInfo partInfo0 = targetMeta.getPartInfos()[index.intValue()].setFileName(partName);
                                        if (putBytesLen.get() != partInfo0.partSize){
                                            partRes.onNext(false);
                                            return;
                                        }
                                        if (responseInfo.successNum == targetPool.getK() + targetPool.getM()) {
                                            partRes.onNext(true);
                                        } else if (responseInfo.successNum >= targetPool.getK()) {
                                            partRes.onNext(true);
                                            SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                                    .put("storage", targetPool.getVnodePrefix())
                                                    .put("bucket", partInfo0.bucket)
                                                    .put("object", partInfo0.object)
                                                    .put("uploadId", partInfo0.uploadId)
                                                    .put("partNum", partInfo0.partNum)
                                                    .put("fileName", partInfo0.fileName)
                                                    .put("endIndex", String.valueOf(partInfo0.partSize - 1))
                                                    .put("errorChunksList", Json.encode(errorChunksList))
                                                    .put("versionId", partInfo0.versionId)
                                                    .put("poolQueueTag", poolQueueTag);
                                            Optional.ofNullable(targetMeta.snapshotMark).ifPresent(v -> errorMsg.put("snapshotMark", v));
                                            CryptoUtils.putCryptoInfoToMsg(targetMeta.crypto, msg.get("secretKey"), errorMsg);

                                            publishEcError(responseInfo.res, targetNodeList, errorMsg, ERROR_PART_UPLOAD_FILE);
                                        } else {
                                            partRes.onNext(false);
                                            SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                                    .put("bucket", partInfo0.getBucket())
                                                    .put("object", partInfo0.getObject())
                                                    .put("fileName", partInfo0.getFileName())
                                                    .put("storage", targetPool.getVnodePrefix())
                                                    .put("poolQueueTag", poolQueueTag);
                                            ECUtils.publishEcError(responseInfo.res, targetNodeList, errorMsg, ERROR_ROLL_BACK_FILE);
                                        }
                                    });
                    disposables.add(disposable);
                    return partRes;
                }, 1, 1).subscribe(b -> {
                    if (b) {
                        successNum.getAndIncrement();
                    }
                }, e -> {
                    log.error("", e);
                    res.onNext(false);
                }, () -> {
//                    targetMeta.setPartInfos(partInfos);
                    if (successNum.get() == totalNum.get()) {
                        res.onNext(true);
                    } else {
                        res.onNext(false);
                    }
                });
        try {
            request.addResponseCloseHandler(v -> {
                for (UnicastProcessor<Payload> processor : processors) {
                    processor.onNext(ERROR_PAYLOAD);
                    processor.onComplete();
                }
                for (Disposable disposable : disposables) {
                    disposable.dispose();
                }
            });
        } catch (Exception e) {
            log.error("", e);
            res.onNext(false);
        }

        return res;
    }

    private static Mono<Boolean> processFilePartCopyToSingle(StoragePool sourcePool, StoragePool targetPool,
                                                             MetaData sourceMeta, MetaData targetMeta,
                                                             MsHttpRequest request, MonoProcessor<Boolean> recoverDataProcessor,
                                                             String metaKey, int tryNum) {
        AtomicInteger retryCount = new AtomicInteger(0);
        int maxRetries = 2;
        return processFilePartCopyToSingle(sourcePool, targetPool, sourceMeta, targetMeta, request, recoverDataProcessor, metaKey)
                .flatMap(result -> {
                    if (!result) {
                        if (retryCount.getAndDecrement() < maxRetries) {
                            return Mono.error(new RuntimeException("Retry needed"));
                        }
                        return Mono.just(false);
                    }
                    return Mono.just(true);
                })
                .retry(maxRetries)
                .onErrorReturn(false);

    }

    // 合并成一个数据块，适配S3的缓存池下刷
    private static Mono<Boolean> processFilePartCopyToSingle(StoragePool sourcePool, StoragePool targetPool,
                                                             MetaData sourceMeta, MetaData targetMeta,
                                                             MsHttpRequest request, MonoProcessor<Boolean> recoverDataProcessor,
                                                             String metaKey) {
        targetMeta.setPartInfos(null);
        targetMeta.setPartUploadId(null);

        String newFileName = Utils.getObjFileName(targetPool, targetMeta.getBucket(), targetMeta.getKey(), RequestBuilder.getRequestId());
        targetMeta.setFileName(newFileName);
        List<Tuple3<String, String, String>> sourceNodeList = sourcePool.mapToNodeInfo(sourcePool.getObjectVnodeId(sourceMeta)).block();
        String newObjectVnodeId = targetPool.getObjectVnodeId(newFileName);
        List<Tuple3<String, String, String>> targetNodeList = targetPool.mapToNodeInfo(newObjectVnodeId).block();
        Encoder ecEncodeHandler = targetPool.getEncoder();
        UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        int ontPutBytes = targetPool.getK() * targetPool.getPackageSize();
        UnicastProcessor<Tuple2<Integer, Integer>> next = UnicastProcessor.create(Queues.<Tuple2<Integer, Integer>>unboundedMultiproducer().get());
        AtomicInteger exceptGetNum = new AtomicInteger(0);
        AtomicInteger waitEncodeBytes = new AtomicInteger(0);
        AtomicInteger exceptPutNum = new AtomicInteger(0);
        next.subscribe(t -> {
            if (t.var1 > 0) {
                exceptGetNum.incrementAndGet();
                waitEncodeBytes.addAndGet(t.var2);
                int n = waitEncodeBytes.get() / ontPutBytes;
                exceptPutNum.addAndGet(n);
                waitEncodeBytes.addAndGet(-n * ontPutBytes);
            } else {
                exceptPutNum.decrementAndGet();
            }
            while (exceptPutNum.get() == 0 && exceptGetNum.get() > 0) {
                streamController.onNext(-1L);
                exceptGetNum.decrementAndGet();
            }
        });
        ErasureClient.getObject(sourcePool, sourceMeta, 0, sourceMeta.endIndex, sourceNodeList, streamController, null, null)
                .doOnError(e -> {
                    for (int i = 0; i < ecEncodeHandler.data().length; i++) {
                        ecEncodeHandler.data()[i].onError(e);
                    }
                })
                .doOnComplete(ecEncodeHandler::complete)
                .subscribe(bytes -> {
                    next.onNext(new Tuple2<>(1, bytes.length));
                    ecEncodeHandler.put(bytes);
                }, e -> log.error("", e));

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("fileName", newFileName)
                .put("metaKey", metaKey)
                .put("compression", targetPool.getCompression());
        if (isEnableCacheOrderFlush(targetPool)) {
            msg.put("flushStamp", targetMeta.getStamp());
        }
        CryptoUtils.generateKeyPutToMsg(targetMeta.crypto, msg);
        List<UnicastProcessor<Payload>> processors = new ArrayList<>();
        for (int i = 0; i < targetNodeList.size(); i++) {
            Tuple3<String, String, String> tuple = targetNodeList.get(i);
            SocketReqMsg msg0 = msg.copy().put("vnode", tuple.var3).put("lun", tuple.var2);

            UnicastProcessor<Payload> processor = UnicastProcessor.create();
            processor.onNext(DefaultPayload.create(Json.encode(msg0), START_PUT_OBJECT.name()));

            ecEncodeHandler.data()[i].subscribe(bytes -> {
                        processor.onNext(DefaultPayload.create(bytes, PUT_OBJECT.name().getBytes()));
                    },
                    e -> {
                        log.error("", e);
                        processor.onNext(DefaultPayload.create("", ERROR.name()));
                        processor.onComplete();
                    },
                    () -> {
                        processor.onNext(DefaultPayload.create("", COMPLETE_PUT_OBJECT.name()));
                        processor.onComplete();
                    });

            processors.add(processor);
        }
        ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(processors, String.class, targetNodeList);
        List<Integer> errorChunksList = new ArrayList<>(targetPool.getM());
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(targetPool.getVnodePrefix());
        AtomicInteger putNum = new AtomicInteger();
        Disposable disposable = responseInfo.responses.subscribe(s -> {
                    if (s.var2.equals(ERROR)) {
                        errorChunksList.add(s.var1);
                    }

                    if (putNum.incrementAndGet() == targetNodeList.size()) {
                        next.onNext(new Tuple2<>(-1, 0));
                        putNum.set(errorChunksList.size());
                    }
                },
                e -> log.error("", e),
                () -> {
                    if (responseInfo.successNum == targetPool.getK() + targetPool.getM()) {
                        res.onNext(true);
                    } else if (responseInfo.successNum >= targetPool.getK()) {
                        res.onNext(true);
                        recoverDataProcessor.subscribe(s -> {
                            //若至少成功写了一个partInfo，发送修复数据的消息
                            if (s) {
                                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                        .put("errorChunksList", Json.encode(errorChunksList))
                                        .put("bucket", targetMeta.bucket)
                                        .put("object", targetMeta.key)
                                        .put("fileName", targetMeta.fileName)
                                        .put("versionId", targetMeta.versionId)
                                        .put("storage", targetPool.getVnodePrefix())
                                        .put("stamp", targetMeta.stamp)
                                        .put("fileSize", String.valueOf(ecEncodeHandler.size()))
                                        .put("poolQueueTag", poolQueueTag)
                                        .put("fileOffset", "");
                                Optional.ofNullable(targetMeta.snapshotMark).ifPresent(v -> errorMsg.put("snapshotMark", v));
                                Optional.ofNullable(msg.get("flushStamp")).ifPresent(v -> errorMsg.put("flushStamp", v));
                                CryptoUtils.putCryptoInfoToMsg(targetMeta.crypto, msg.get("secretKey"), errorMsg);

                                publishEcError(responseInfo.res, targetNodeList, errorMsg, ERROR_PUT_OBJECT_FILE);
                            }
                        });
                    } else {
                        res.onNext(false);
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                .put("errorChunksList", Json.encode(errorChunksList))
                                .put("bucket", targetMeta.bucket)
                                .put("object", targetMeta.key)
                                .put("fileName", targetMeta.fileName)
                                .put("versionId", targetMeta.versionId)
                                .put("storage", targetPool.getVnodePrefix())
                                .put("stamp", targetMeta.stamp)
                                .put("poolQueueTag", poolQueueTag);
                        ECUtils.publishEcError(responseInfo.res, targetNodeList, errorMsg, ERROR_ROLL_BACK_FILE);
                    }
                });

        Optional.ofNullable(request)
                .ifPresent(r -> {
                    r.addResponseCloseHandler(v -> {
                        for (UnicastProcessor<Payload> processor : processors) {
                            processor.onNext(ERROR_PAYLOAD);
                            processor.onComplete();
                        }
                        disposable.dispose();
                    });
                });
        return res;
    }

    /**
     * 通知其它节点配置QoS策略
     *
     * @param metaType 发送的消息类型
     */
    public static boolean notifyNodes(PayloadMetaType metaType) {

        // 需要发送的socketReqMsg
        SocketReqMsg msg = new SocketReqMsg("", 0);
        AtomicBoolean res = new AtomicBoolean(true);
        AtomicInteger num = new AtomicInteger(0);
        List<String> ipList = RabbitMqUtils.HEART_IP_LIST.stream().filter(s -> !s.equals(CURRENT_IP)).collect(Collectors.toList());
        String curIp = ServerConfig.getInstance().getHeartIp1();

        for (int i = 0; i < ipList.size(); i++) {

            if (ipList.get(i).equalsIgnoreCase(curIp)) {
                continue;
            }
            Mono.just(ipList.get(i))
                    .flatMap(ip -> RSocketClient.getRSocket(ip, BACK_END_PORT))
                    .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), metaType.name())))
                    .timeout(Duration.ofSeconds(30))
                    .map(payload -> {
                        try {
                            num.incrementAndGet();
                            String metaDataPayload = payload.getMetadataUtf8();
                            String dataPayload = payload.getDataUtf8();
                            if (metaDataPayload.equalsIgnoreCase(SUCCESS.name())) {
                                log.info(dataPayload);
                                return true;
                            }
                            log.error(dataPayload);
                            return false;
                        } finally {
                            payload.release();
                        }
                    }).subscribe(aBoolean -> {
                        log.debug("num: " + num.get());  // 仅调试使用
                    }, e -> log.error("Failed to notify other nodes due to RSocket exception" + num.get(), e));

        }

        return res.get();
    }

    public static Mono<Boolean> updateAfterInitRecord(StoragePool storagePool, String key, String value, List<Tuple3<String, String, String>> nodeList) {
//        String storageName = "storage_" + storagePool.getVnodePrefix();
//        String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
//        if (StringUtils.isEmpty(poolName)) {
//            String strategyName = "storage_" + storagePool.getVnodePrefix();
//            poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//        }
//        String poolQueueTag = poolName;
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("key", key)
                            .put("value", value)
                            .put("poolQueueTag", poolQueueTag);
                    if (UnSynchronizedRecord.isOldPath(key)) {
                        msg.put("lun", tuple.var2);
                    } else {
                        msg.put("lun", MSRocksDB.getSyncRecordLun(tuple.var2));
                    }
                    return msg;
                })
                .collect(Collectors.toList());
        return putRecord(storagePool, msgs, UPDATE_AFTER_INIT_RECORD, ERROR_UPDATE_AFTER_INIT_RECORD, nodeList, null);
    }

    //  因为每次rewrite会生成新的记录，因此有一个节点成功就返回true，删掉原记录，防止原纪录和m个新纪录同时存在导致重复计数
    public static Mono<Boolean> rewriteRecord(StoragePool storagePool, String key, UnSynchronizedRecord record, List<Tuple3<String, String, String>> nodeList) {
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("key", key)
                            .put("value", Json.encode(record))
                            .put("poolQueueTag", poolQueueTag);
                    if (UnSynchronizedRecord.isOldPath(key)) {
                        msg.put("lun", tuple.var2);
                    } else {
                        msg.put("lun", MSRocksDB.getSyncRecordLun(tuple.var2));
                    }
                    return msg;
                })
                .collect(Collectors.toList());

        MonoProcessor<Boolean> res = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, UPDATE_AFTER_INIT_RECORD, String.class, nodeList);

        Disposable subscribe = responseInfo.responses.subscribe(s -> {
        }, e -> log.error("", e), () -> {
            if (responseInfo.successNum == 0) {
                res.onNext(false);
                return;
            }
            // 写成功1个节点就返回true，可能存在的问题：唯一存在record的节点掉了，导致同步状态为已同步
            if (responseInfo.successNum != nodeList.size()) {
                // ERROR_UPDATE_AFTER_INIT_RECORD处理rabbitmq消息时如果已经不存在该差异记录则不会修复，因此不会重复计数
                publishEcError(responseInfo.res, nodeList, msgs.get(0), ERROR_UPDATE_AFTER_INIT_RECORD);
            }
            res.onNext(true);
        });

        return res;
    }

    public static Mono<String> getTargetUploadId(String bucket, String recordKey) {
        return getUnsyncRecord(bucket, recordKey)
                .flatMap(afterInitRecord -> {
                    if (afterInitRecord.equals(ERROR_UNSYNC_RECORD)) {
                        return Mono.just("-1");
                    }
                    if (afterInitRecord.equals(NOT_FOUND_UNSYNC_RECORD)) {
                        return Mono.just("-2");
                    }

                    return Mono.just(afterInitRecord.headers.get(UPLOAD_ID));
                });
    }

    public static Mono<UnSynchronizedRecord> getUnsyncRecord(String bucket, String recordKey) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);
        return storagePool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> ECUtils.getRocksKey(StoragePoolFactory.getMetaStoragePool(bucket),
                        recordKey, UnSynchronizedRecord.class, GET_UNSYNC_RECORD, NOT_FOUND_UNSYNC_RECORD, ERROR_UNSYNC_RECORD, null, UnSynchronizedRecord::getVersionNum,
                        Comparator.comparing(a -> a.versionNum), (a, b, c, d) -> Mono.just(1), nodeList, null))
                .flatMap(tuple -> Mono.just(tuple.var1));
    }

    public static Mono<Boolean> deleteObjectAllMeta(String bucket, String key, String value, List<Tuple3<String, String, String>> nodeList) {
        return MsObjVersionUtils.versionStatusReactive(bucket)
                .flatMap(status -> deleteObjectAllMeta(bucket, key, value, nodeList, status));
    }

    private static Mono<Boolean> deleteObjectAllMeta(String bucket, String key, String value, List<Tuple3<String, String, String>> nodeList, String versionStatus) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        List<SocketReqMsg> msgs = mapToMsg(key, value, nodeList);
        msgs.forEach(msg -> {
            if (!StringUtils.isEmpty(versionStatus)) {
                msg.put("status", versionStatus);
            }
        });
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, DELETE_OBJECT_ALL_META, String.class, nodeList);
        responseInfo.responses.subscribe(s -> {
        }, e -> log.error("", e), () -> {
            if (responseInfo.successNum == storagePool.getK() + storagePool.getM()) {
                res.onNext(true);
            } else if (responseInfo.successNum >= storagePool.getK()) {
                MetaData metaData = Json.decodeValue(value, MetaData.class);
                String oldVersion = metaData.versionNum;
                VersionUtil.getVersionNum(bucket, metaData.key)
                        .flatMap(versionNum -> Mono.just(StringUtils.isEmpty(oldVersion) || versionNum.compareTo(oldVersion) > 0 ? versionNum : oldVersion + "0"))
                        .flatMap(versionNum -> deleteObjectAllMetaRepair(metaData.bucket, metaData.key, versionNum, nodeList, metaData.versionId, versionStatus, metaData.snapshotMark))
                        .subscribe(res::onNext);
            } else {
                res.onNext(false);
            }
        });
        return res;
    }

    public static Mono<Boolean> deleteObjectAllMetaRepair(String bucket, String object, String versionNum, List<Tuple3<String, String, String>> vnodeList, String versionId, String versionStatus, String snapshotMark) {
        DelMarkParams delMarkParams = new DelMarkParams(bucket, object, versionId, null, vnodeList, versionStatus, versionNum).type(ERROR_DELETE_OBJECT_ALL_META).snapshotMark(snapshotMark);
        return setDelMark(delMarkParams).map(r -> r != 0);
    }

    public static Mono<Boolean> discardObjectAllMeta(String bucket, String key, String value, List<Tuple3<String, String, String>> nodeList) {
        return MsObjVersionUtils.versionStatusReactive(bucket)
                .flatMap(versionStatus -> {
                    List<SocketReqMsg> msgs = mapToMsg(key, value, nodeList);
                    msgs.forEach(msg -> {
                        if (!StringUtils.isEmpty(versionStatus)) {
                            msg.put("status", versionStatus);
                        }
                    });
                    MetaData metaData = Json.decodeValue(value, MetaData.class);
                    String oldVersion = metaData.versionNum;
                    return VersionUtil.getVersionNum(bucket, metaData.key)
                            .flatMap(versionNum -> Mono.just(StringUtils.isEmpty(oldVersion) || versionNum.compareTo(oldVersion) > 0 ? versionNum : oldVersion + "0"))
                            .flatMap(versionNum -> deleteObjectAllMetaRepair(metaData.bucket, metaData.key, versionNum, nodeList, metaData.versionId, versionStatus, metaData.snapshotMark));
                });

    }

    public static Mono<Boolean> deleteRocksKey(String key, MSRocksDB.IndexDBEnum indexDBEnum, String columnFamily, List<Tuple3<String, String, String>> nodeList) {
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("key", key)
                        .put("cfName", StringUtils.isEmpty(columnFamily) ? "default" : columnFamily)
                        .put("lun", MSRocksDB.getIndexRocksDBLun(tuple.var2, indexDBEnum)))
                .collect(Collectors.toList());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, DELETE_ROCKS_KEY, String.class, nodeList);
        responseInfo.responses.subscribe(s -> {
        }, e -> log.error("", e), () -> res.onNext(true));
        return res;
    }

    public static Mono<Boolean> mergeTempBucketInfo(String vnode, String bucket) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        return storagePool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> ErasureClient.mergeTempBucketInfo(vnode, bucket, nodeList));
    }

    public static Mono<Boolean> mergeTempBucketInfo(String vnode, String bucket, List<Tuple3<String, String, String>> nodeList) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("vnode", vnode)
                        .put("bucket", bucket)
                        .put("lun", tuple.var2))
                .collect(Collectors.toList());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, MERGE_TMP_BUCKET_INFO, String.class, nodeList);
        responseInfo.responses.subscribe(s -> {
        }, e -> {
            log.error("", e);
            res.onNext(false);
        }, () -> {
            if (responseInfo.successNum == pool.getK() + pool.getM()) {
                QuotaRecorder.addCheckBucket(bucket);
                res.onNext(true);
            } else if (responseInfo.successNum >= pool.getK()) {
                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                        .put("vnode", vnode)
                        .put("bucket", bucket);
                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());
                errorMsg.put("poolQueueTag", poolQueueTag);
                ECUtils.publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_MERGE_TMP_BUCKET_INFO);
                res.onNext(true);
            } else {
                res.onNext(false);
            }
        });
        return res;
    }

    public static Mono<Boolean> deleteFilesInRange(List<String> rangeList, List<Tuple3<String, String, String>> nodeList) {
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("range", Json.encode(rangeList))
                        .put("lun", tuple.var2))
                .collect(Collectors.toList());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, DELETE_FILES_IN_RANGE, String.class, nodeList);
        responseInfo.responses.subscribe(s -> {
        }, e -> {
            log.error("", e);
            res.onNext(false);
        }, () -> res.onNext(true));
        return res;
    }
}



