package com.macrosan.ec;

import com.macrosan.component.ComponentUtils;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.doubleActive.MsClientRequest;
import com.macrosan.doubleActive.compress.SyncUncompressProcessor;
import com.macrosan.ec.error.ErrorConstant.ECErrorType;
import com.macrosan.ec.server.ErasureServer.PayloadMetaType;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.storage.client.GetMultiPartObjectClientHandler;
import com.macrosan.storage.client.GetObjectClientHandler;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.utils.authorize.*;
import com.macrosan.utils.cache.ReadCache;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.*;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.md5.Digest;
import com.macrosan.utils.perf.AccountPerfLimiter;
import com.macrosan.utils.perf.BucketPerfLimiter;
import com.macrosan.utils.perf.DataSyncPerfLimiter;
import io.netty.buffer.ByteBuf;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.mvel2.util.FastList;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.macrosan.action.core.BaseService.isCurrentTimeWithinRange;
import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.archive.ArchieveUtils.ARCHIVE_SYNC_REC_MARK;
import static com.macrosan.doubleActive.deployment.AddClusterHandler.HIS_SYNC_REC_MARK;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.NOT_FOUND;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.filesystem.FsConstants.ACLConstants.MERGE_META;
import static com.macrosan.filesystem.utils.ChunkFileUtils.mapToPartInfo;
import static com.macrosan.httpserver.MossHttpClient.*;
import static com.macrosan.rabbitmq.ObjectPublisher.publish;
import static com.macrosan.rabbitmq.RabbitMqUtils.*;
import static com.macrosan.storage.StoragePoolFactory.DEFAULT_PACKAGE_SIZE;
import static com.macrosan.utils.cache.ReadCache.MAX_CACHE_FILE_SIZE;


@Log4j2
public class ECUtils {

    protected static RedisConnPool pool = RedisConnPool.getInstance();

    public static MetaData createMeta(StoragePool pool, String bucketName, String objName, String requestId) {
        MetaData metaData = new MetaData()
                .setStorage(pool.getVnodePrefix())
                .setFileName(Utils.getObjFileName(pool, bucketName, objName, requestId))
                .setBucket(bucketName)
                .setKey(objName)
                .setDeleteMark(false)
                .setStartIndex(0)
                .setDeleteMarker(false)
                .setReferencedBucket(bucketName)
                .setReferencedKey(objName);
        return metaData;
    }

    public static LongLongTuple dealRange(String range, long objSize) {
        try {
            String[] array = range.split("=");
            if (array.length != 2) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "");
            }

            long startIndex;
            long endIndex = objSize == 0 ? objSize : objSize - 1;
            array = array[1].split("-");

            if (array.length == 2) {
                if (StringUtils.isEmpty(array[0])) {
                    final long rightVal = Long.parseLong(array[1]);
                    startIndex = objSize - rightVal;
                } else {
                    endIndex = Long.parseLong(array[1]);
                    startIndex = Long.parseLong(array[0]);
                }
            } else if (array.length == 1) {
                startIndex = Long.parseLong(array[0]);
            } else {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "");
            }

            if (objSize == 0) {
                if (startIndex > endIndex) {
                    throw new MsException(ErrorNo.INVALID_RANGE, "");
                }
            } else {
                if (startIndex > endIndex) {
                    throw new MsException(ErrorNo.INVALID_RANGE, "");
                }
            }

            startIndex = startIndex < 0 ? 0 : startIndex;
            endIndex = endIndex < 0 || endIndex >= objSize ? objSize - 1 : endIndex;
            return new LongLongTuple(startIndex, endIndex);
        } catch (NumberFormatException e) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "");
        }
    }

    /**
     * 處理copypart接口中的range字段
     *
     * @param range
     * @param objSize
     * @return
     */
    public static LongLongTuple dealCopyRange(String range, long objSize) {
        try {
            String[] array = range.split("=");
            if (!array[0].equals("bytes")) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "The x-amz-copy-source-range value must be of the " +
                        "form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy");
            }
            if (array.length != 2) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "");
            }

            long startIndex;
            long endIndex = objSize == 0 ? objSize : objSize - 1;
            array = array[1].split("-");

            if (array.length == 2) {
                if (StringUtils.isEmpty(array[0])) {
                    throw new MsException(ErrorNo.INVALID_ARGUMENT, "The x-amz-copy-source-range value must be of the " +
                            "form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy");
                } else {
                    endIndex = Long.parseLong(array[1]);
                    startIndex = Long.parseLong(array[0]);
                }
            } else {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "The x-amz-copy-source-range value must be of the " +
                        "form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy");
            }

            if (objSize == 0) {
                if (startIndex >= objSize) {
                    throw new MsException(ErrorNo.INVALID_REQUEST, "The specified copy range is invalid for the source object size");
                }

                if (startIndex > endIndex || endIndex > objSize) {
                    throw new MsException(ErrorNo.INVALID_ARGUMENT, "The x-amz-copy-source-range value must be of the form bytes=first-last where " +
                            "first and last are the zero-based offsets of the first and last bytes to copy ");
                }
            } else {
                if (startIndex >= objSize) {
                    throw new MsException(ErrorNo.INVALID_REQUEST, "The specified copy range is invalid for the source object size");
                }

                if (startIndex > endIndex || endIndex >= objSize) {
                    throw new MsException(ErrorNo.INVALID_ARGUMENT, "The x-amz-copy-source-range value must be of the form bytes=first-last where " +
                            "first and last are the zero-based offsets of the first and last bytes to copy ");
                }

            }
            startIndex = startIndex < 0 ? 0 : startIndex;
            endIndex = endIndex < 0 || endIndex >= objSize ? objSize - 1 : endIndex;

            if (endIndex - startIndex >= MAX_PUT_SIZE) {
                throw new MsException(SOURCE_TOO_LARGE, "");
            }
            return new LongLongTuple(startIndex, endIndex);
        } catch (NumberFormatException e) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "");
        }
    }

    public static Mono<Boolean> hadObject(String bucket) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        List<String> bucketVnodeList = storagePool.getBucketVnodeList(bucket);
        MonoProcessor<Boolean> res = MonoProcessor.create();
        UnicastProcessor<String> hadObjectProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        hadObjectProcessor.onNext(bucketVnodeList.get(0));
        AtomicInteger nextRunNumber = new AtomicInteger(1);
        AtomicBoolean hadObject = new AtomicBoolean(false);
        AtomicBoolean error = new AtomicBoolean(false);
        hadObjectProcessor.subscribe(bucketVnode -> storagePool.mapToNodeInfo(bucketVnode)
                .zipWith(RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hget(bucket, SNAPSHOT_SWITCH).switchIfEmpty(Mono.just("off")))
                .subscribe(tuple2 -> {
                    List<Tuple3<String, String, String>> list = tuple2.getT1();
                    List<SocketReqMsg> msgs = list.stream()
                            .map(t -> new SocketReqMsg("", 0)
                                    .put("bucket", bucket)
                                    .put("vnode", bucketVnode)
                                    .put("lun", t.var2)
                                    .put("snapshotSwitch", tuple2.getT2()))
                            .collect(Collectors.toList());
                    ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, HAD_OBJECT, String.class, list);
                    responseInfo.responses.subscribe(s -> {
                    }, e -> log.error("", e), () -> {
                        if (responseInfo.errorNum > storagePool.getM()) {
                            //res.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "had bucket " + bucket + " fail"));
                            error.set(true);
                            hadObjectProcessor.onComplete();
                        } else if (responseInfo.successNum > 0) {
                            hadObject.set(true);
                            hadObjectProcessor.onComplete();
                        } else {
                            int next = nextRunNumber.getAndIncrement();
                            if (next < bucketVnodeList.size()) {
                                hadObjectProcessor.onNext(bucketVnodeList.get(next));
                            } else {
                                hadObject.set(false);
                                hadObjectProcessor.onComplete();
                            }
                        }
                    });
                }), log::error, () -> {

            if (error.get()) {
                res.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "had bucket " + bucket + " fail"));
                return;
            }
            if (!error.get() && hadObject.get()) {
                res.onNext(true);
                return;
            }
            if (!error.get() && !hadObject.get()) {
                res.onNext(false);
            }

        });
        return res;
    }

    public static Mono<Boolean> hadObject(String bucket, String bucketVnode) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        MonoProcessor<Boolean> res = MonoProcessor.create();
        pool.mapToNodeInfo(bucketVnode)
                .zipWith(RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hget(bucket, SNAPSHOT_SWITCH).switchIfEmpty(Mono.just("off")))
                .subscribe(tuple2 -> {
                    List<Tuple3<String, String, String>> list = tuple2.getT1();
                    List<SocketReqMsg> msgs = list.stream()
                            .map(t -> new SocketReqMsg("", 0)
                                    .put("bucket", bucket)
                                    .put("vnode", bucketVnode)
                                    .put("lun", t.var2)
                                    .put("snapshotSwitch", tuple2.getT2()))
                            .collect(Collectors.toList());
                    ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, HAD_OBJECT, String.class, list);
                    responseInfo.responses.subscribe(s -> {
                    }, e -> log.error("", e), () -> {
                        if (responseInfo.errorNum > pool.getM()) {
                            res.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "had bucket " + bucket + " fail"));
                        } else if (responseInfo.successNum > 0) {
                            res.onNext(true);
                        } else {
                            res.onNext(false);
                        }
                    });
                });

        return res;
    }

    public static Mono<Boolean> putRocksKey(StoragePool pool, String key, String value, PayloadMetaType type, ECErrorType errorType,
                                            List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        return putRocksKey(pool, key, value, type, errorType, s -> {
        }, nodeList, null, request, "", "", null, false, null);
    }

    public static Mono<Boolean> putRocksKey(StoragePool pool, String key, String value, PayloadMetaType type, ECErrorType errorType,
                                            List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String snapshotLink) {
        return putRocksKey(pool, key, value, type, errorType, s -> {
        }, nodeList, null, request, "", "", null, false, snapshotLink);
    }

    public static Mono<Boolean> putRocksKey(StoragePool pool, String key, String value, PayloadMetaType type, ECErrorType errorType,
                                            List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, boolean isMigrate, String snapshotLink) {
        return putRocksKey(pool, key, value, type, errorType, s -> {
        }, nodeList, null, request, "", "", null, isMigrate, snapshotLink);
    }

    public static Mono<Boolean> putRocksKey(StoragePool pool, String key, String value, PayloadMetaType type, ECErrorType errorType,
                                            Consumer<Tuple3<Integer, PayloadMetaType, String>> responseHandler,
                                            List<Tuple3<String, String, String>> nodeList, MonoProcessor<Boolean> recoverDataProcessor,
                                            MsHttpRequest request, String mda, String versionStatus, EsMeta esMeta, boolean isMigrate, String snapshotLink) {
        List<SocketReqMsg> msgs = mapToMsg(key, value, nodeList);
        msgs.forEach(msg1 -> {
            if (!StringUtils.isEmpty(versionStatus)) {
                msg1.put("status", versionStatus);
            }
            if (isMigrate) {
                msg1.put("migrate", "1");
            }
            // snapshotLink用于将快照创建前上传的同名对象置为不可见
            Optional.ofNullable(snapshotLink).ifPresent(v -> msg1.put("snapshotLink", v));
        });
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, type, String.class, nodeList);

        Disposable subscribe = responseInfo.responses.subscribe(responseHandler, e -> log.error("", e), () -> {
            if (responseInfo.successNum == 0) {
                if (recoverDataProcessor != null) {
                    recoverDataProcessor.onNext(false);
                }
                res.onNext(false);
            } else {
                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());
                if (recoverDataProcessor != null) {
                    recoverDataProcessor.onNext(true);
                }
                if (responseInfo.successNum == pool.getK() + pool.getM()) {
                    res.onNext(true);
                } else if (responseInfo.successNum >= pool.getK()) {
                    if (errorType.equals(ERROR_PUT_DEDUPLICATE_META)) {
                        msgs.get(0).put("realKey", key);
                    }
                    msgs.get(0).put("poolQueueTag", poolQueueTag);
                    msgs.get(0).put("vnode", nodeList.get(0).var3);
                    publishEcError(responseInfo.res, nodeList, msgs.get(0), errorType);
                    res.onNext(true);
                } else {
                    if (errorType.equals(ERROR_PUT_DEDUPLICATE_META)) {
                        msgs.get(0).put("realKey", key);
                        msgs.get(0).put("poolQueueTag", poolQueueTag);
                        // 超冗余，删除成功节点上的重删记录
                        publishEcError(responseInfo.res, nodeList, msgs.get(0), ERROR_DELETE_DEDUPLICATE_META);
                    }
                    //未成功，但有部分节点成功，不返回结果。
                    //数据恢复消息正常发出。
                    if (null != esMeta && "on".equals(mda)) {
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0);
                        errorMsg.put("metaData", value);
                        errorMsg.put("mda", mda);
                        errorMsg.put("esMeta", Json.encode(esMeta));
                        errorMsg.put("poolQueueTag", poolQueueTag);
                        log.error("es meta minor success  ! esMeta is {}", esMeta);
//                        publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_PUT_OBJECT_META_WITH_ES_META);
                        boolean isOverWrite = false;
                        Inode[] inode = new Inode[]{null};
                        MetaData metaData = Json.decodeValue(value, MetaData.class);
                        if (metaData.inode > 0 && StringUtils.isNotBlank(metaData.tmpInodeStr)) {
                            esMeta.inode = metaData.inode;
                            inode[0] = Json.decodeValue(metaData.tmpInodeStr, Inode.class);
                            isOverWrite = true;
                        }
                        if (isOverWrite) {
                            Mono.just(1).publishOn(DISK_SCHEDULER).subscribe(l -> EsMetaTask.overWriteEsMeta(esMeta, inode[0], true).subscribe());
                        } else {
                            Mono.just(1).publishOn(DISK_SCHEDULER).subscribe(l -> EsMetaTask.putEsMeta(esMeta, true).subscribe());
                        }
                    }

                    if (isMigrate) {
                        res.onNext(false);
                    }
                }
            }
        });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));

        return res;
    }

    public static Mono<Boolean> putSynchronizedRecord(StoragePool storagePool, String key, String value, List<Tuple3<String, String, String>> nodeList, boolean writeAsync) {
        return putSynchronizedRecord(storagePool, key, value, nodeList, writeAsync, null);
    }

    public static Mono<Boolean> putSynchronizedRecord(StoragePool storagePool, String key, String value, List<Tuple3<String, String, String>> nodeList, boolean writeAsync, MsHttpRequest r) {
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
                    if (writeAsync) {
                        msg.put(ASYNC_CLUSTER_SIGNAL, ASYNC_CLUSTERS);
                    }
                    return msg;
                })
                .collect(Collectors.toList());
        return putRecord(storagePool, msgs, PUT_SYNC_RECORD, ERROR_PUT_SYNC_RECORD, nodeList, r);
    }

    public static Mono<Boolean> rewritePartCopyRecord(StoragePool storagePool, String key, String recordValue, String daVersion,
                                                      String metaValue, List<Tuple3<String, String, String>> nodeList) {
//        String storageName = "storage_" + storagePool.getVnodePrefix();
//        String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(storagePool.getVnodePrefix());
//        if (StringUtils.isEmpty(poolName)) {
//            String strategyName = "storage_" + storagePool.getVnodePrefix();
//            poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//        }
//        String poolQueueTag = poolName;
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("key", key)
                        .put("recordValue", recordValue)
                        .put("DaVersion", daVersion)
                        .put("metaValue", metaValue)
                        .put("oldVersion", null)
                        .put("lun", MSRocksDB.getSyncRecordLun(tuple.var2))
                        .put("poolQueueTag", poolQueueTag))
                .collect(Collectors.toList());

        return putRecord(storagePool, msgs, REWRITE_PART_COPY_RECORD, ERROR_REWRITE_PART_COPY_RECORD, nodeList, null);
    }

    public static Mono<Boolean> putRecord(StoragePool storagePool, List<SocketReqMsg> msgs, PayloadMetaType type, ECErrorType errorType,
                                          List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, type, String.class, nodeList);

        Disposable subscribe = responseInfo.responses.subscribe(s -> {
        }, e -> log.error("", e), () -> {
            if (responseInfo.successNum < storagePool.getK()) {
                res.onNext(false);
                return;
            }

            if (responseInfo.successNum != nodeList.size()) {
                //只要写不成功，均写入消息队列
                publishEcError(responseInfo.res, nodeList, msgs.get(0), errorType);
            }
            res.onNext(true);
        });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));

        return res;
    }

    public static List<SocketReqMsg> mapToRecordMsg(String oldVersion, String key, String recordValue, String metaValue, List<Tuple3<String, String, String>> nodeList) {
        return nodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("key", key)
                        .put("recordValue", recordValue)
                        .put("metaValue", metaValue)
                        .put("oldVersion", oldVersion)
                        .put("lun", tuple.var2))
                .collect(Collectors.toList());
    }

    /**
     * 更新rocksDB中的双活record
     */
    public static Mono<Integer> updateSyncRecord(UnSynchronizedRecord record, List<Tuple3<String, String, String>> nodeList, boolean writeAsync) {
        return updateSyncRecord(record, nodeList, writeAsync, null);
    }

    public static Mono<Integer> updateSyncRecord(UnSynchronizedRecord record, List<Tuple3<String, String, String>> nodeList, boolean writeAsync, MsHttpRequest request) {
        if (record == null) {
            return Mono.just(1);
        }
        record.setVersionNum(VersionUtil.getVersionNum(false));
        MonoProcessor<Integer> res = MonoProcessor.create();
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("key", record.rocksKey())
                            .put("value", Json.encode(record))
                            .put("oldVersion", record.versionNum);
                    if (UnSynchronizedRecord.isOldPath(record.rocksKey())) {
                        msg.put("lun", tuple.var2);
                    } else {
                        msg.put("lun", MSRocksDB.getSyncRecordLun(tuple.var2));
                    }
                    if (record.headers.containsKey(HIS_SYNC_REC_MARK) || record.headers.containsKey(ARCHIVE_SYNC_REC_MARK)) {
                        msg.put(HIS_SYNC_SIGNAL, "1");
                    }
                    if (writeAsync) {
                        if (IS_THREE_SYNC) {
                            msg.put(ASYNC_CLUSTER_SIGNAL, THREE_SYNC_INDEX.toString());
                        } else {
                            msg.put(ASYNC_CLUSTER_SIGNAL, ASYNC_CLUSTERS);
                        }
                    }
                    return msg;
                })
                .collect(Collectors.toList());
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, UPDATE_SYNC_RECORD, String.class, nodeList);
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(record.bucket);
        Disposable subscribe = responseInfo.responses.subscribe(s -> {
        }, e -> log.error("", e), () -> {
            if (responseInfo.successNum == pool.getK() + pool.getM()) {
                res.onNext(1);
            } else if (responseInfo.writedNum == pool.getK() + pool.getM()) {
                res.onNext(2);
            } else if (responseInfo.errorNum == pool.getK() + pool.getM()) {
                res.onNext(0);
            } else if (responseInfo.successNum >= pool.getK()) {
//                String strategyName = "storage_" + pool.getVnodePrefix();
//                String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());
//                if (StringUtils.isEmpty(poolQueueTag)) {
//                    String strategyName = "storage_" + pool.getVnodePrefix();
//                    poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                }
                msgs.get(0).put("poolQueueTag", poolQueueTag);
                publishEcError(responseInfo.res, nodeList, msgs.get(0), ERROR_PUT_SYNC_RECORD);
                res.onNext(1);
            } else if (responseInfo.errorNum < pool.getK() && responseInfo.writedNum >= 1) {
                res.onNext(2);
            } else {
                //未成功，但有部分节点成功，不返回结果
            }
        });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return res;
    }

    public static Mono<Integer> updateRocksKey(StoragePool pool, Map<String, String> oldVersion, String key, String value, PayloadMetaType type, ECErrorType errorType,
                                               List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        return updateRocksKey(pool, oldVersion, key, value, type, errorType, nodeList, request, null);
    }

    public static Mono<Integer> updateRocksKey(StoragePool pool, Map<String, String> oldVersion, String key, String value,
                                               PayloadMetaType type, ECErrorType errorType,
                                               List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String status) {
        return updateRocksKey(pool, oldVersion, key, value, type, errorType, nodeList, request, status, null);
    }

    public static Mono<Integer> updateRocksKey(StoragePool pool, Map<String, String> oldVersion, String key, String value,
                                               PayloadMetaType type, ECErrorType errorType,
                                               List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String status, String snapshotLink) {
        List<SocketReqMsg> msgs = mapToMsgWithVersion(oldVersion, key, value, nodeList);
        MonoProcessor<Integer> res = MonoProcessor.create();
        if (!StringUtils.isEmpty(status)) {
            msgs.forEach(msg -> msg.put("status", status));
        }
        msgs.forEach(msg -> {
            if (ComponentUtils.isComponentRecordLun(key)) {
                msg.put("lun", MSRocksDB.getComponentRecordLun(msg.get("lun")));
            }
        });
        Optional.ofNullable(snapshotLink).ifPresent(v -> msgs.forEach(msg -> msg.put("snapshotLink", v)));
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, type, String.class, nodeList);

        Disposable subscribe = responseInfo.responses.subscribe(s -> {
        }, e -> {
            log.error("", e);
            res.onNext(0);
        }, () -> {
            InitPartInfo initPart = null;
            if (responseInfo.successNum != pool.getK() + pool.getM()
                    && responseInfo.writedNum != pool.getK() + pool.getM()
                    && responseInfo.successNum + responseInfo.writedNum == pool.getK() + pool.getM()) {
                // 并发修复时可能出现 successNum + writedNum = k+m，此时也需要putDeleteKey
                if (Utils.isMetaJson(value.getBytes())) {
                    MetaData metaData = Json.decodeValue(value, MetaData.class);
                    if (metaData.deleteMark) {
                        DelDeleteMark.putDeleteKey(key, value);
                    }
                } else if ((initPart = Utils.isInitPartJson(value.getBytes())) != null) {
                    if (initPart.delete) {
                        DelDeleteMark.putDeleteKey(initPart.getPartKey(nodeList.get(0).var3), value);
                    }
                }
            }
            if (responseInfo.successNum == pool.getK() + pool.getM()) {
                if (Utils.isMetaJson(value.getBytes())) {
                    MetaData metaData = Json.decodeValue(value, MetaData.class);
                    if (metaData.deleteMark) {
                        DelDeleteMark.putDeleteKey(key, value);
                    }
                } else if ((initPart = Utils.isInitPartJson(value.getBytes())) != null) {
                    if (initPart.delete) {
                        DelDeleteMark.putDeleteKey(initPart.getPartKey(nodeList.get(0).var3), value);
                    }
                }
                res.onNext(1);
            } else if (responseInfo.writedNum == pool.getK() + pool.getM()) {
                res.onNext(2);
            } else if (responseInfo.errorNum == pool.getK() + pool.getM()) {
                res.onNext(0);
            } else if (responseInfo.successNum >= pool.getK()) {
                if (errorType != ECErrorType.ERROR_PUT_BUCKET_INFO) {
                    if (errorType.equals(ERROR_PUT_DEDUPLICATE_META)) {
                        msgs.get(0).put("realKey", key);
                    }
                    if (errorType.equals(ERROR_PUT_OBJECT_META)) {
                        msgs.get(0).put("vnode", Utils.getVnode(key));
                    }
                    String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());
                    msgs.get(0).put("poolQueueTag", poolQueueTag);
                    publishEcError(responseInfo.res, nodeList, msgs.get(0), errorType);
                }
                if (responseInfo.writedNum >= 1) {
                    res.onNext(2);
                } else {
                    res.onNext(1);
                }
            } else if (responseInfo.errorNum < pool.getK() && responseInfo.writedNum >= 1) {
                res.onNext(2);
            } else {
                //未成功，但有部分节点成功，不返回结果
                //文件相关操作需返错，否则业务会卡住
                if (PUT_INODE.equals(type)
                        || PUT_CHUNK.equals(type)
                        || PUT_COOKIE.equals(type)
                ) {
                    res.onNext(0);
                }
            }
        });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return res;
    }

    public static Mono<Boolean> putCredentialToken(StoragePool pool, String key, String value, PayloadMetaType type, ECErrorType errorType,
                                                   List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        return putCredentialToken(pool, key, value, type, errorType, s -> {
        }, nodeList, null, request);
    }

    public static Mono<Boolean> putCredentialToken(StoragePool pool, String key, String value, PayloadMetaType type, ECErrorType errorType,
                                                   Consumer<Tuple3<Integer, PayloadMetaType, String>> responseHandler,
                                                   List<Tuple3<String, String, String>> nodeList, MonoProcessor<Boolean> recoverDataProcessor,
                                                   MsHttpRequest request) {
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("key", key)
                        .put("value", value)
                        .put("lun", MSRocksDB.getSTSTokenLun(tuple.var2)))
                .collect(Collectors.toList());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, type, String.class, nodeList);

        Disposable subscribe = responseInfo.responses.subscribe(responseHandler, e -> {
            log.error("", e);
            res.onNext(false);
        }, () -> {
            if (responseInfo.successNum == 0) {
                if (recoverDataProcessor != null) {
                    recoverDataProcessor.onNext(false);
                }
                res.onNext(false);
            } else {
//                String storageName = "storage_" + pool.getVnodePrefix();
//                String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
                String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());
//                if (StringUtils.isEmpty(poolQueueTag)) {
//                    String strategyName = "storage_" + pool.getVnodePrefix();
//                    poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                }
                if (recoverDataProcessor != null) {
                    recoverDataProcessor.onNext(true);
                }
                if (responseInfo.successNum == pool.getK() + pool.getM()) {
                    res.onNext(true);
                } else if (responseInfo.successNum >= pool.getK()) {
                    msgs.get(0).put("poolQueueTag", poolQueueTag);
                    publishEcError(responseInfo.res, nodeList, msgs.get(0), errorType);
                    res.onNext(true);
                } else {
                    res.onNext(false);
                }
            }
        });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));

        return res;
    }

    public static <T> Mono<Tuple2<T, Tuple2<PayloadMetaType, T>[]>> getSTSTokenRes(StoragePool pool, String key, Class<T> tClass, PayloadMetaType type, T notFoundRes, T errorRes,
                                                                                   Function4<String, T, List<Tuple3<String, String, String>>, Tuple2<PayloadMetaType, T>[], Mono<Boolean>> repairFunction,
                                                                                   List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        MonoProcessor<Tuple2<T, Tuple2<PayloadMetaType, T>[]>> processor = MonoProcessor.create();//返回得到的结果

        PayloadMetaType finalType = type;//GET_STS_TOKEN
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("key", key)
                            .put("lun", tuple.var2);
                    if (key.startsWith(ROCKS_STS_TOKEN_KEY)) {
                        msg.put("lun", MSRocksDB.getSTSTokenLun(tuple.var2));
                    }
                    return msg;
                })
                .collect(Collectors.toList());

        ResponseInfo<T> responseInfo = ClientTemplate.oneResponse(msgs, finalType, tClass, nodeList);

        //保存相应中meta的versionNum
        Map<String, Integer> map = new HashMap<>(1);
        //保存versionNum为最新的metadata
        T[] real = (T[]) Array.newInstance(tClass, 1);
        //保存最新的versionNum
        String[] realVersionNum = new String[]{null};

        Disposable[] disposables = new Disposable[2];
        disposables[1] = responseInfo.responses
                .timeout(Duration.ofSeconds(15))
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
                }, e -> {
                    log.error("key: {}, type: {}, error: {}", key, type.name(), e);
                    processor.onNext(new Tuple2<>(errorRes, responseInfo.res));
                }, () -> {
                    try {
                        if (responseInfo.errorNum > pool.getM()) {
                            processor.onNext(new Tuple2<>(errorRes, responseInfo.res));
                            return;
                        }

                        if (responseInfo.errorNum == 0) {
                            for (Tuple3<String, String, String> tuple3 : nodeList) {
                                String diskName = getDiskName(tuple3.var1, tuple3.var2);
                                if (!diskIsAvailable(diskName)) {
                                    Mono.just(true).publishOn(DISK_SCHEDULER)
                                            .subscribe(b -> {
                                                updateDiskStatus(diskName, DISK_CONSUMER_STATUS.AVAILABLE);
                                            });
                                }
                            }

                            if (real[0].equals(notFoundRes)) {//只要有一个成功就不会是notFound
                                processor.onNext(new Tuple2<>(notFoundRes, responseInfo.res));
                                return;
                            } else if (responseInfo.successNum == pool.getK() + pool.getM()) {//都成功
                                processor.onNext(new Tuple2<>(real[0], responseInfo.res));
                                return;
                            } else {//errornum为0，三个不全是notfound，至少有一个success，需要修复
                                disposables[0] = repairFunction.apply(key, real[0], nodeList, responseInfo.res).subscribe(b -> {
                                    if (b) {
                                        processor.onNext(new Tuple2<>(real[0], responseInfo.res));
                                    } else {
                                        processor.onNext(new Tuple2<>(errorRes, responseInfo.res));
                                    }
                                });
                                return;
                            }
                        }

                        //存在error的返回。
                        T t;
                        //不直接删除是因为down掉的节点可能保存有完好的metadata，节点重启时若其他节点没有记录会重新修复
                        if (real[0].equals(notFoundRes)) {//如果返回notfound，说明没有success，不修复
                            t = real[0];
                            processor.onNext(new Tuple2<>(real[0], responseInfo.res));
                        } else {
                            t = real[0];
                            disposables[0] = repairFunction.apply(key, t, nodeList, responseInfo.res).subscribe(b -> {
                                if (b) {
                                    processor.onNext(new Tuple2<>(real[0], responseInfo.res));
                                } else {
                                    processor.onNext(new Tuple2<>(errorRes, responseInfo.res));
                                }
                            });
                        }
                    } catch (Exception e) {
                        //捕获processor中抛出的错误
                        log.error("getRocksKey error", e);
                        processor.onNext(new Tuple2<>(errorRes, responseInfo.res));
                    }
                });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> {
            for (Disposable disposable : disposables) {
                if (disposable != null) {
                    disposable.dispose();
                }
            }
        }));

        return processor.onErrorReturn(new Tuple2<>(errorRes, responseInfo.res));
    }

    public static Mono<Tuple2<Integer, Inode>> updateRocksKeyInode(StoragePool pool, Map<String, String> oldVersion, String key, String value,
                                                                   PayloadMetaType type, ECErrorType errorType,
                                                                   List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String status) {
        List<SocketReqMsg> msgs = mapToMsgWithVersion(oldVersion, key, value, nodeList);
        MonoProcessor<Tuple2<Integer, Inode>> res = MonoProcessor.create();
        msgs.forEach(msg -> msg.put("createS3Inode", "1"));
        if (!StringUtils.isEmpty(status)) {
            msgs.forEach(msg -> msg.put("status", status));
        }
        ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, type, String.class, nodeList);
        Inode resInode[] = new Inode[1];
        Disposable subscribe = responseInfo.responses.subscribe(s -> {
            if (s.var1 == 2 && StringUtils.isNotEmpty(s.var3)) {
                Inode tmp = Json.decodeValue(s.var3, Inode.class);
                if (resInode[0] == null || tmp.getVersionNum().compareTo(resInode[0].getVersionNum()) > 0) {
                    resInode[0] = tmp;
                }
            }
        }, e -> {
            log.error("", e);
            res.onNext(new Tuple2<>(0, null));
        }, () -> {
            if (responseInfo.successNum == pool.getK() + pool.getM()) {
                res.onNext(new Tuple2<>(1, null));
            } else if (responseInfo.writedNum == pool.getK() + pool.getM()) {
                res.onNext(new Tuple2<>(2, resInode[0]));
            } else if (responseInfo.errorNum == pool.getK() + pool.getM()) {
                res.onNext(new Tuple2<>(0, null));
            } else if (responseInfo.successNum >= pool.getK()) {
                if (errorType != ECErrorType.ERROR_PUT_BUCKET_INFO) {
                    if (errorType.equals(ERROR_PUT_DEDUPLICATE_META)) {
                        msgs.get(0).put("realKey", key);
                    }
//                    String storageName = "storage_" + pool.getVnodePrefix();
//                    String poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storageName, "pool");
                    String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(pool.getVnodePrefix());
//                    if (StringUtils.isEmpty(poolQueueTag)) {
//                        String strategyName = "storage_" + pool.getVnodePrefix();
//                        poolQueueTag = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(strategyName, "pool");
//                    }
                    msgs.get(0).put("poolQueueTag", poolQueueTag);
                    publishEcError(responseInfo.res, nodeList, msgs.get(0), errorType);
                }
                res.onNext(new Tuple2<>(1, null));
            } else if (responseInfo.errorNum < pool.getK() && responseInfo.writedNum >= 1) {
                res.onNext(new Tuple2<>(2, resInode[0]));
            } else {
                res.onNext(new Tuple2<>(0, null));
            }
        });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return res;
    }

    public static Mono<Tuple3<Boolean, MetaData, Tuple2<PayloadMetaType, MetaData>[]>> getRocksKey(StoragePool pool, String key, PayloadMetaType type, MetaData notFoundRes,
                                                                                                   MetaData errorRes, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request,
                                                                                                   String currentSnapshotMark, String snapshotLink) {
        MonoProcessor<Tuple3<Boolean, MetaData, Tuple2<PayloadMetaType, MetaData>[]>> processor = MonoProcessor.create();

        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("key", key)
                            .put("lun", tuple.var2);
                    Optional.ofNullable(snapshotLink).ifPresent(s -> msg.put("snapshotLink", s));
                    Optional.ofNullable(currentSnapshotMark).ifPresent(s -> msg.put("currentSnapshotMark", s));
                    return msg;
                })
                .collect(Collectors.toList());

        ResponseInfo<MetaData> responseInfo = ClientTemplate.oneResponse(msgs, type, MetaData.class, nodeList);
        //保存相应中meta的versionNum
        Map<String, Integer> map = new HashMap<>(1);
        //保存versionNum为最新的metadata
        MetaData[] real = (MetaData[]) Array.newInstance(MetaData.class, 1);
        //保存最新的versionNum
        String[] realVersionNum = new String[]{null};

        Disposable[] disposables = new Disposable[2];
        //不释放该disposable，在2000并发10ms socketTimeout时会堆积
        disposables[1] = responseInfo.responses
                .timeout(Duration.ofSeconds(15))
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
                        String versionNum = tuple.var3.versionNum;
                        int c = map.getOrDefault(versionNum, 0);
                        map.put(versionNum, c + 1);

                        if (realVersionNum[0] == null) {
                            real[0] = tuple.var3;
                            realVersionNum[0] = versionNum;
                        } else if (tuple.var3.versionId.compareTo(real[0].versionId) == 0) {
                            if (tuple.var3.versionNum.compareTo(real[0].versionNum) > 0) {
                                real[0] = tuple.var3;
                                realVersionNum[0] = versionNum;
                            }
                        } else if (tuple.var3.stamp.compareTo(real[0].stamp) > 0) {
                            real[0] = tuple.var3;
                            realVersionNum[0] = versionNum;
                            map.put(versionNum, 1);
                        } else if (tuple.var3.stamp.compareTo(real[0].stamp) == 0 && tuple.var3.versionId.compareTo(real[0].versionId) > 0) {
                            real[0] = tuple.var3;
                            realVersionNum[0] = versionNum;
                            map.put(versionNum, 1);
                        } else {
                            map.put(versionNum, 1);
                        }

                    } else if (tuple.var2.equals(NOT_FOUND) && real[0] == null) {
                        real[0] = tuple.var3;
                    }
                }, e -> {
                    log.error("", e);
                    processor.onNext(new Tuple3<>(false, errorRes, responseInfo.res));
                }, () -> {
                    try {
                        if (responseInfo.errorNum > pool.getM()) {
                            processor.onNext(new Tuple3<>(false, errorRes, responseInfo.res));
                            return;
                        }

                        if (responseInfo.errorNum == 0) {
                            for (Tuple3<String, String, String> tuple3 : nodeList) {
                                String diskName = getDiskName(tuple3.var1, tuple3.var2);
                                if (!diskIsAvailable(diskName)) {
                                    Mono.just(true).publishOn(DISK_SCHEDULER)
                                            .subscribe(b -> {
                                                updateDiskStatus(diskName, DISK_CONSUMER_STATUS.AVAILABLE);
                                            });
                                }
                            }

                            if (real[0].equals(notFoundRes)) {
                                processor.onNext(new Tuple3<>(false, notFoundRes, responseInfo.res));
                                return;
                            } else if (map.get(realVersionNum[0]) == pool.getK() + pool.getM()) {
                                processor.onNext(new Tuple3<>(false, real[0], responseInfo.res));
                                return;
                            }
                        }
                        processor.onNext(new Tuple3<>(true, real[0], responseInfo.res));
                    } catch (Exception e) {
                        //捕获processor中抛出的错误
                    }
                });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> {
            for (Disposable disposable : disposables) {
                if (disposable != null) {
                    disposable.dispose();
                }
            }
        }));

        return processor.onErrorReturn(new Tuple3<>(false, errorRes, responseInfo.res));

    }

    public static <T> Mono<Tuple2<T, Tuple2<PayloadMetaType, T>[]>> getRocksKey(StoragePool pool, String key, Class<T> tClass, PayloadMetaType type, T notFoundRes, T errorRes,
                                                                                T deleteMark, Function<T, String> getVersionFunction, Comparator<T> comparator,
                                                                                Function4<String, T, List<Tuple3<String, String, String>>, Tuple2<PayloadMetaType, T>[], Mono<Integer>> repairFunction,
                                                                                List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        return getRocksKey(pool, key, tClass, type, notFoundRes, errorRes, deleteMark, getVersionFunction, comparator, repairFunction, nodeList, request, new HashMap<>(), null);
    }

    public static <T> Mono<Tuple2<T, Tuple2<PayloadMetaType, T>[]>> getRocksKey(StoragePool pool, String key, Class<T> tClass, PayloadMetaType type, T notFoundRes, T errorRes,
                                                                                T deleteMark, Function<T, String> getVersionFunction, Comparator<T> comparator,
                                                                                Function4<String, T, List<Tuple3<String, String, String>>, Tuple2<PayloadMetaType, T>[], Mono<Integer>> repairFunction,
                                                                                List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String currentSnapshotMark, String snapshotLink) {
        Map<String, String> extraMap = null;
        if (currentSnapshotMark != null || snapshotLink != null) {
            extraMap = new HashMap<>();
            if (currentSnapshotMark != null) {
                extraMap.put("currentSnapshotMark", currentSnapshotMark);
            }

            if (snapshotLink != null) {
                extraMap.put("snapshotLink", snapshotLink);
            }
        }
        return getRocksKey(pool, key, tClass, type, notFoundRes, errorRes, deleteMark, getVersionFunction, comparator, repairFunction, nodeList, request, extraMap, null);
    }

    public static <T> Mono<Tuple2<T, Tuple2<PayloadMetaType, T>[]>> getRocksKey(StoragePool pool, String key, Class<T> tClass, PayloadMetaType type, T notFoundRes, T errorRes,
                                                                                T deleteMark, Function<T, String> getVersionFunction, Comparator<T> comparator,
                                                                                Function4<String, T, List<Tuple3<String, String, String>>, Tuple2<PayloadMetaType, T>[], Mono<Integer>> repairFunction,
                                                                                List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, Map<String, String> extraMap,
                                                                                Function2<T, T, T> mergeFunction) {
        MonoProcessor<Tuple2<T, Tuple2<PayloadMetaType, T>[]>> processor = MonoProcessor.create();

        PayloadMetaType finalType = type;
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("key", key)
                            .put("lun", tuple.var2);
                    if (extraMap != null && !extraMap.isEmpty()) {
                        msg.dataMap.putAll(extraMap);
                    }

                    if (GET_INODE.equals(type)) {
                        boolean isACLStart = ACLUtils.NFS_ACL_START || ACLUtils.CIFS_ACL_START;
                        if (isACLStart) {
                            msg.put(MERGE_META, "1");
                        }
                    }

                    if (key.startsWith(ROCKS_UNSYNCHRONIZED_KEY)) {
                        msg.put("lun", MSRocksDB.getSyncRecordLun(tuple.var2));
                    }
                    if (ComponentUtils.isComponentRecordLun(key)) {
                        msg.put("lun", MSRocksDB.getComponentRecordLun(tuple.var2));
                    }
                    if (key.startsWith(ROCKS_ES_KEY)) {
                        msg.put("lun", MSRocksDB.getRabbitmqRecordLun(tuple.var2));
                    }

                    return msg;
                })
                .collect(Collectors.toList());


        ResponseInfo<T> responseInfo = ClientTemplate.oneResponse(msgs, finalType, tClass, nodeList);

        //保存相应中meta的versionNum
        Map<String, Integer> map = new HashMap<>(1);
        //保存versionNum为最新的metadata
        T[] real = (T[]) Array.newInstance(tClass, 1);
        //保存最新的versionNum
        String[] realVersionNum = new String[]{null};

        Disposable[] disposables = new Disposable[2];
        //不释放该disposable，在2000并发10ms socketTimeout时会堆积
        disposables[1] = responseInfo.responses
                .timeout(Duration.ofSeconds(15))
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
                        String versionNum = getVersionFunction.apply(tuple.var3);
                        int c = map.getOrDefault(versionNum, 0);
                        map.put(versionNum, c + 1);

                        if (realVersionNum[0] == null || comparator.compare(tuple.var3, real[0]) > 0
                                || (MetaData.class.getName().equals(tClass.getName()) && InodeUtils.needChangeGetRes((MetaData) real[0], (MetaData) tuple.var3))) {
                            real[0] = tuple.var3;
                            realVersionNum[0] = versionNum;
                        }
                        if (mergeFunction != null) {
                            real[0] = mergeFunction.apply(real[0], tuple.var3);
                        }
                    } else if (tuple.var2.equals(NOT_FOUND) && real[0] == null) {
                        real[0] = tuple.var3;
                    }
                }, e -> {
                    log.error("key: {}, type: {}, error: {}", key, type.name(), e);
                    processor.onNext(new Tuple2<>(errorRes, responseInfo.res));
                }, () -> {
                    try {
                        if (responseInfo.errorNum > pool.getM()) {
                            if (GET_ES_META.equals(type) && responseInfo.successNum > 0) {
                                processor.onNext(new Tuple2<>(real[0], responseInfo.res));
                                return;
                            }
                            processor.onNext(new Tuple2<>(errorRes, responseInfo.res));
                            return;
                        }

                        if (responseInfo.errorNum == 0) {
                            for (Tuple3<String, String, String> tuple3 : nodeList) {
                                String diskName = getDiskName(tuple3.var1, tuple3.var2);
                                if (!diskIsAvailable(diskName)) {
                                    Mono.just(true).publishOn(DISK_SCHEDULER)
                                            .subscribe(b -> {
                                                updateDiskStatus(diskName, DISK_CONSUMER_STATUS.AVAILABLE);
                                            });
                                }
                            }

                            if (real[0].equals(notFoundRes)) {
                                processor.onNext(new Tuple2<>(notFoundRes, responseInfo.res));
                                return;
                            } else if (map.get(realVersionNum[0]) == pool.getK() + pool.getM()) {
                                processor.onNext(new Tuple2<>(real[0], responseInfo.res));
                                return;
                            }
                        }

                        //存在error的返回。
                        T t;
                        //待更新的metadata为not_found，将各节点元数据更新为deleteMark
                        //不直接删除是因为down掉的节点可能保存有完好的metadata，节点重启时若其他节点没有记录会重新修复
                        if (real[0].equals(notFoundRes)) {
                            t = deleteMark;
                            if (DedupMeta.class.getName().equals(tClass.getName())) {
                                t = real[0];
                            }
                        } else {
                            t = real[0];
                        }

                        disposables[0] = repairFunction.apply(key, t, nodeList, responseInfo.res)
                                .timeout(Duration.ofSeconds(30))
                                .onErrorReturn(0)
                                .subscribe(b -> {
                                    if (b != 0) {
                                        processor.onNext(new Tuple2<>(real[0], responseInfo.res));
                                    } else {
                                        processor.onNext(new Tuple2<>(errorRes, responseInfo.res));
                                    }
                                });
                    } catch (Exception e) {
                        //捕获processor中抛出的错误
                        log.error("getRocksKey error", e);
                        processor.onNext(new Tuple2<>(errorRes, responseInfo.res));
                    }
                });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> {
            for (Disposable disposable : disposables) {
                if (disposable != null) {
                    disposable.dispose();
                }
            }
        }));

        return processor.onErrorReturn(new Tuple2<>(errorRes, responseInfo.res));

    }

    public static <T> Mono<Tuple2<T, Tuple2<PayloadMetaType, T>[]>> getAccessRecordRes(StoragePool pool, String key, Class<T> tClass, PayloadMetaType type, T notFoundRes, T errorRes,
                                                                                       List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        MonoProcessor<Tuple2<T, Tuple2<PayloadMetaType, T>[]>> processor = MonoProcessor.create();//返回得到的结果
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("key", key)
                            .put("lun", tuple.var2);
                    return msg;
                })
                .collect(Collectors.toList());

        ResponseInfo<T> responseInfo = ClientTemplate.oneResponse(msgs, type, tClass, nodeList);

        T[] real = (T[]) Array.newInstance(tClass, 1);

        Disposable[] disposables = new Disposable[2];
        disposables[1] = responseInfo.responses
                .timeout(Duration.ofSeconds(30))
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
                }, e -> {
                    log.error("key: {}, type: {}, error: {}", key, type.name(), e);
                    processor.onNext(new Tuple2<>(errorRes, responseInfo.res));
                }, () -> {
                    try {
                        if (responseInfo.errorNum > pool.getM()) {
                            processor.onNext(new Tuple2<>(errorRes, responseInfo.res));
                            return;
                        }

                        if (responseInfo.errorNum == 0) {
                            for (Tuple3<String, String, String> tuple3 : nodeList) {
                                String diskName = getDiskName(tuple3.var1, tuple3.var2);
                                if (!diskIsAvailable(diskName)) {
                                    Mono.just(true).publishOn(DISK_SCHEDULER)
                                            .subscribe(b -> {
                                                updateDiskStatus(diskName, DISK_CONSUMER_STATUS.AVAILABLE);
                                            });
                                }
                            }

                            if (real[0].equals(notFoundRes)) {//只要有一个成功就不会是notFound
                                processor.onNext(new Tuple2<>(notFoundRes, responseInfo.res));
                                return;
                            } else if (responseInfo.successNum == pool.getK() + pool.getM() || responseInfo.successNum >= pool.getK()) {//都成功
                                processor.onNext(new Tuple2<>(real[0], responseInfo.res));
                                return;
                            } else {//errornum为0，三个不全是notfound，至少有一个success，需要修复//todo 这里不修复
                                processor.onNext(new Tuple2<>(notFoundRes, responseInfo.res));
                                return;
                            }
                        }

                        //存在error的返回。
                        T t;
                        //不直接删除是因为down掉的节点可能保存有完好的metadata，节点重启时若其他节点没有记录会重新修复
                        if (real[0].equals(notFoundRes)) {//如果返回notfound，说明没有success，不修复
                            t = real[0];
                            processor.onNext(new Tuple2<>(real[0], responseInfo.res));
                        } else {
                            if (responseInfo.successNum == pool.getK() + pool.getM() || responseInfo.successNum >= pool.getK()) {//都成功
                                processor.onNext(new Tuple2<>(real[0], responseInfo.res));
                            } else {
                                processor.onNext(new Tuple2<>(errorRes, responseInfo.res));
                            }
                        }
                    } catch (Exception e) {
                        //捕获processor中抛出的错误
                        log.error("getRocksKey error", e);
                        processor.onNext(new Tuple2<>(errorRes, responseInfo.res));
                    }
                });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> {
            for (Disposable disposable : disposables) {
                if (disposable != null) {
                    disposable.dispose();
                }
            }
        }));

        return processor.onErrorReturn(new Tuple2<>(errorRes, responseInfo.res));

    }

    public static List<SocketReqMsg> mapToMsg(String key, String value, List<Tuple3<String, String, String>> nodeList) {
        return nodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("key", key)
                        .put("value", value)
                        .put("lun", tuple.var2))
                .collect(Collectors.toList());
    }

    private static List<SocketReqMsg> mapToMsgWithVersion(Map<String, String> oldVersion, String key, String value, List<Tuple3<String, String, String>> nodeList) {
        return nodeList.stream()
                .map(tuple -> new SocketReqMsg("", 0)
                        .put("key", key)
                        .put("value", value)
                        .put("lun", tuple.var2)
                        .put("oldVersion", oldVersion.getOrDefault(tuple.var1, null)))
                .collect(Collectors.toList());
    }

    /**
     * 错误发送至第一个ERROR对应节点，lun中记录所有error磁盘，
     * consumer中需对所有磁盘移除状态检测，决定是否不再消费。<br>
     * msg.get("lun")有可能的格式：“0001@fs-SP0-1#@001@fs-SP0-2”、“0001@fs-SP0-1”
     *
     * @注意 与对象或者存储池相关的修复消息请在消息内容中携带poolQueueTag信息(value为相关的存储池名称)用于队列的分发，不携带则不会分发至存储池相关队列
     */
    public static <T> void publishEcError(Tuple2<PayloadMetaType, T>[] res, List<Tuple3<String, String, String>> nodeList,
                                          SocketReqMsg msg, ECErrorType errorType) {
        String errorNode = "";
        StringBuilder lunList = new StringBuilder();
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple2<PayloadMetaType, T> tuple2 = res[i];
            if (null != tuple2 && ERROR.equals(tuple2.var1)) {
                if (lunList.toString().isEmpty()) {
                    errorNode = nodeList.get(i).var1;
                }
                String diskName = getDiskName(nodeList.get(i).var1, nodeList.get(i).var2);
                lunList.append(lunList.toString().isEmpty() ? diskName : "#" + diskName);
            }
        }
        //lun用于记录所有error磁盘名称，处理异常时需判断磁盘状态
        msg.put("lun", lunList.toString());
        publish(errorNode, msg, errorType);
    }

//    public static Mono<List<Tuple3<String, String, String>>> mapToNodeInfo(String vnode) {
//        return VnodeCache.hgetVnodeInfo(vnode, "link")
//                .map(link -> link.substring(1, link.length() - 1).split(","))
//                .flatMapMany(Flux::fromArray)
//                .index()
//                .flatMap(t -> Mono.just(t.getT1())
//                        .zipWith(getTargetInfoReactive(t.getT2())
//                                .flatMap(ECUtils::mapToNodeInfo)))
//                .collectMap(reactor.util.function.Tuple2::getT1)
//                .map(resMap -> {
//                    List<Tuple3<String, String, String>> res = new ArrayList<>(k + m);
//                    for (long i = 0; i < k + m; i++) {
//                        res.add(resMap.get(i).getT2());
//                    }
//                    return res;
//                });
//    }

    public static Mono<Tuple3<String, String, String>> mapToNodeInfo(Tuple2<Map<String, String>, FastList<String>> tuple) {
        String lunName = tuple.var1.get(VNODE_LUN_NAME);
        String ip = tuple.var2.get(0);
        String vnode = tuple.var1.get("v_num");
        return Mono.just(new Tuple3<>(ip, lunName, vnode));
    }


    /**
     * 获取当前的lun在nodeList中的索引。如果nodeList中没有该lun，返回-1；
     *
     * @param ip  lun所处的ip。
     * @param lun 如果不止一个lun，取第一个。有可能的格式：“0001@fs-SP0-1#@001@fs-SP0-2”、“fs-SP0-1”
     */
    public static int getIndexByLunAndIp(List<Tuple3<String, String, String>> nodeList, String ip, String lun) {
        lun = lun.split("#")[0];
        if (lun.contains("@")) {
            lun = lun.split("@")[1];
        }
        int index = -1;
        for (int i = 0; i < nodeList.size(); i++) {
            if (nodeList.get(i).var2.equals(lun) && nodeList.get(i).var1.equals(ip)) {
                index = i;
                break;
            }
        }
        return index;
    }

    public static Handler<Buffer> bufferHandler(MsHttpRequest request, Consumer<Buffer> bufferConsumer, Consumer<byte[]> bytesConsumer, Consumer<Throwable> throwableConsumer) {
        String authorization = request.getHeader(AUTHORIZATION);
        boolean isContainTrailer = request.headers().contains(AuthorizeV4.X_AMZ_TRAILER);
        if (authorization != null && authorization.startsWith(AuthorizeV4.AWS4_HMAC_SHA256) && !isContainTrailer) {
            return new V4DataStreamHandler(request, bytesConsumer, throwableConsumer);
        } else if (isContainTrailer) {
            if (AuthorizeV4.CONTENT_SHA256_SIGNED_PAYLOAD_TRAILER.equals(request.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256))) {
                return new V4DataStreamTrailerHandler(request, bytesConsumer, throwableConsumer);
            } else if (AuthorizeV4.CONTENT_SHA256_UNSIGNED_PAYLOAD_TRAILER.equals(request.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256))) {
                return new V2DataStreamTrailerHandler(request, bytesConsumer, throwableConsumer);
            } else {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "A header you provided implies functionality that is not implemented");
            }
        } else {
            if (bufferConsumer != null) {
                return bufferConsumer::accept;
            }
            return buffer -> bytesConsumer.accept(buffer.getBytes());
        }
    }

    /**
     * 获取元数据所在节点node的状态信息
     *
     * @param meta 元数据
     * @return 节点node在线的数量>=k，返回true；反之返回false
     */
    public static Mono<Boolean> getMetaDataNodeState(MetaData meta) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(meta.bucket);

        return storagePool.mapToNodeInfo(storagePool.getBucketVnodeId(meta.bucket, meta.key))
                .flatMapMany(listNodes -> Flux.fromStream(listNodes.stream()))
                .map(tuple3 -> NodeCache.mapIPToNode(tuple3.var1))
                .flatMap(uuid -> pool.getReactive(REDIS_NODEINFO_INDEX).hget(uuid, NODE_SERVER_STATE))
                .collectList()
                .flatMap(stateList -> {
                    int onlineCount = 0;
                    for (String state : stateList) {
                        if ("1".equals(state)) {
                            onlineCount++;
                        }
                    }
                    return Mono.just(onlineCount >= storagePool.getK());
                });
    }

    private static AccountPerfLimiter accountPerfLimiter = AccountPerfLimiter.getInstance();
    private static BucketPerfLimiter bucketPerfLimiter = BucketPerfLimiter.getInstance();

    private static void syncCompressProcessHttpBody(MsHttpRequest request, Encoder handler, Digest digest, String[] md5, Long[] objectSize,
                                                    MonoProcessor<String> processor) {

        AtomicLong originalDataLength = new AtomicLong(0);
        String syncCompress = request.getHeader(SYNC_COMPRESS);

        // request.endhandler里使用，表示上游流结束
        MonoProcessor<Boolean> completeSignal = MonoProcessor.create();
        SyncUncompressProcessor uncompressProcessor = new SyncUncompressProcessor(request, syncCompress);
        completeSignal.subscribe(b -> {
            uncompressProcessor.outputProcessor.onComplete();
        });

        uncompressProcessor.outputProcessor
                .publishOn(DISK_SCHEDULER)
                .map(uncompressProcessor::uncompressBytes)
                .doOnNext(dstBytes -> {
                    handler.put(dstBytes);
                    originalDataLength.addAndGet(dstBytes.length);
                    digest.update(dstBytes);
                })
                .doOnComplete(() -> {
                    md5[0] = Hex.encodeHexString(digest.digest());
                    objectSize[0] = originalDataLength.get();
                    handler.complete();
                })
                .doOnError(e -> {
                    if (e instanceof CancellationException) {
                        log.debug("outputProcessor error, ", e);
                        return;
                    }
                    log.error("outputProcessor error, ", e);
                })
                .subscribe();

        Consumer<byte[]> bytesConsumer = uncompressProcessor::parse;

        Consumer<Throwable> throwableConsumer = throwable -> {
            for (int i = 0; i < handler.data().length; i++) {
                handler.data()[i].onError(throwable);
            }
            if (!processor.isError()) {
                processor.onError(throwable);
            }
            log.error("", throwable);
        };

        Handler<Buffer> bufferHandler = bufferHandler(request, null, bytesConsumer, throwableConsumer);
        request.handler(bufferHandler);

        request.endHandler(v -> {
            AuthorizeV4.checkDecodedContentLength(request, originalDataLength.get(), r -> {
                log.error("The Authorization was wrong. decoded content length provided in header:["
                        + r + "] is not right");
                MsException exception = new MsException(MISSING_DECODED_CONTENT_LENGTH, "");
                throwableConsumer.accept(exception);
            });
            AuthorizeV4.checkSingleChunkObjectSHA256(request, bufferHandler, s -> {
                log.error("The Authorization was wrong. content sha256 provided in header:["
                        + request.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256) + "] payload:[" + s + "]");
                MsException exception = new MsException(X_AMZ_CONTENT_SHA_256_MISMATCH, "");
                throwableConsumer.accept(exception);
            });

            completeSignal.onNext(true);
        });

        request.addResponseCloseHandler(v -> uncompressProcessor.close());
    }

    private static void normalProcessHttpBody(MsHttpRequest request, Encoder handler, Digest digest, String[] md5, Long[] objectSize,
                                              MonoProcessor<String> processor, JsonObject sysMetaMap) {

        AtomicLong originalDataLength = new AtomicLong(0);
        Consumer<Buffer> bufferConsumer = buffer -> {
            ByteBuf byteBuf = buffer.getByteBuf();
            if (originalDataLength.addAndGet(buffer.length()) > DEFAULT_PACKAGE_SIZE && request.getContext() != null) {
                request.getContext().executeBlocking(future -> {
                    digest.update(byteBuf.nioBuffer());
                }, true, null);
            } else {
                digest.update(byteBuf.nioBuffer());
            }
            handler.put(buffer);
        };

        Consumer<byte[]> bytesConsumer = bytes -> {
            if (originalDataLength.addAndGet(bytes.length) > DEFAULT_PACKAGE_SIZE && request.getContext() != null) {
                request.getContext().executeBlocking(future -> {
                    digest.update(bytes);
                }, true, null);
            } else {
                digest.update(bytes);
            }
            handler.put(bytes);
        };

        Consumer<Throwable> throwableConsumer = throwable -> {
            for (int i = 0; i < handler.data().length; i++) {
                handler.data()[i].onError(throwable);
            }
            if (!processor.isError()) {
                processor.onError(throwable);
            }
            log.error("", throwable);
        };

        Handler<Buffer> bufferHandler = bufferHandler(request, bufferConsumer, bytesConsumer, throwableConsumer);
        if (HttpMethod.POST == request.method() && request.headers().contains(CONTENT_TYPE) && request.getHeader(CONTENT_TYPE).contains("multipart/form-data")) {
            request.uploadHandler(upload -> {
                //也可在这里获取文件的content-type
                sysMetaMap.remove(CONTENT_TYPE);
                sysMetaMap.put(CONTENT_TYPE, upload.contentType());
                upload.handler(bufferHandler);
            });

        } else {
            request.handler(bufferHandler);
        }

        request.endHandler(v -> {
            AuthorizeV4.checkDecodedContentLength(request, originalDataLength.get(), r -> {
                log.error("The Authorization was wrong. decoded content length provided in header:["
                        + r + "] is not right");
                MsException exception = new MsException(MISSING_DECODED_CONTENT_LENGTH, "");
                throwableConsumer.accept(exception);
            });
            AuthorizeV4.checkSingleChunkObjectSHA256(request, bufferHandler, s -> {
                log.error("The Authorization was wrong. content sha256 provided in header:["
                        + request.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256) + "] payload:[" + s + "]");
                MsException exception = new MsException(X_AMZ_CONTENT_SHA_256_MISMATCH, "");
                throwableConsumer.accept(exception);
            });
            if (originalDataLength.get() > DEFAULT_PACKAGE_SIZE && request.getContext() != null) {
                request.getContext().executeBlocking(future -> {
                    md5[0] = Hex.encodeHexString(digest.digest());
                    request.getDataHandlerCompleted().set(true);
                    objectSize[0] = originalDataLength.get();
                    handler.complete();
                }, true, null);
            } else {
                md5[0] = Hex.encodeHexString(digest.digest());
                request.getDataHandlerCompleted().set(true);
                objectSize[0] = originalDataLength.get();
                handler.complete();
            }

        });
    }

    private static void limitProcessHttpBody(MsHttpRequest request, Encoder handler, Digest digest, String[] md5, Long[] objectSize,
                                             MonoProcessor<String> processor, JsonObject sysMetaMap) {
        UnicastProcessor<byte[]> processor0 = UnicastProcessor.create();
        Subscription[] subscriptions = new Subscription[]{null};
        Queue<Disposable> disposableList = new ConcurrentLinkedDeque<>();
        AtomicLong originalDataLength = new AtomicLong(0);

        AtomicBoolean hasTerminated = new AtomicBoolean();
        Handler<Void> terminateHandler = v -> {
            if (hasTerminated.compareAndSet(false, true)) {
                try {
                    for (Disposable disposable : disposableList) {
                        Optional.ofNullable(disposable).ifPresent(Disposable::dispose);
                    }

                    processor0.cancel();
                    subscriptions[0].cancel();
                } catch (Exception e) {
                    log.error("terminate error, ", e);
                }
            }

        };

        Consumer<byte[]> bytesConsumer = bytes -> {
            try {
                processor0.onNext(bytes);
                Disposable subscribe = AccountPerfLimiter.getInstance().limits(request.getUserId(), BAND_WIDTH_QUOTA, bytes.length)
                        .flatMap(w -> BucketPerfLimiter.getInstance().limits(request.getBucketName(), BAND_WIDTH_QUOTA, bytes.length)
                                .map(millis -> millis + w))
                        .subscribe(waitMillis -> {
                            try {
                                // HttpClientRequest类型的连接发到非本地节点，close后无法被监听到，无法触发两种closeHandler
                                if (waitMillis > 60_000 && request.headers().contains(CLUSTER_ALIVE_HEADER)) {
                                    Disposable disposable = DISK_SCHEDULER.schedule(() -> {
                                        request.connection().close();
                                    }, 60_000, TimeUnit.MILLISECONDS);
                                    disposableList.add(disposable);
                                    return;
                                }
                                if (waitMillis > 0) {
                                    Disposable disposable = DISK_SCHEDULER.schedule(() -> {
                                        subscriptions[0].request(1);
                                    }, waitMillis, TimeUnit.MILLISECONDS);
                                    disposableList.add(disposable);
                                } else {
                                    subscriptions[0].request(1);
                                }
                            } catch (Exception e) {
                                log.error("limit waitMillis, ", e);
                            }
                        }, log::error);
                disposableList.add(subscribe);
            } catch (Exception e) {
                processor.onError(e);
            }
        };
        Consumer<Throwable> throwableConsumer = throwable -> {
            for (int i = 0; i < handler.data().length; i++) {
                handler.data()[i].onError(throwable);
            }
            if (!processor.isError()) {
                processor.onError(throwable);
            }
            log.error("", throwable);
        };

        Handler<Buffer> bufferHandler = bufferHandler(request, null, bytesConsumer, throwableConsumer);

        if (HttpMethod.POST == request.method() && request.headers().contains(CONTENT_TYPE) && request.getHeader(CONTENT_TYPE).contains("multipart/form-data")) {
            request.uploadHandler(upload -> {
                sysMetaMap.remove(CONTENT_TYPE);
                sysMetaMap.put(CONTENT_TYPE, upload.contentType());
                upload.handler(bufferHandler);
            });
        } else {
            request.handler(buffer -> {
                bufferHandler.handle(buffer);
            });
        }

        // 有可能在限流情况下，request的数据读完触发endHandler，但由于bytes在byteConsumer中是延时写入的，processor0不能complete，可能会有md5不匹配等问题。
        request.endHandler(v -> processor0.onComplete());

        CoreSubscriber<byte[]> coreSubscriber = new CoreSubscriber<byte[]>() {
            @Override
            public void onSubscribe(Subscription s) {
                // 整个请求只调用一次
                subscriptions[0] = s;
            }

            @Override
            public void onNext(byte[] bytes) {
                try {
                    handler.put(bytes);
                    originalDataLength.addAndGet(bytes.length);
                    digest.update(bytes);
                } catch (Exception e) {
                    log.error("limit write exception, ", e);
                    subscriptions[0].cancel();
                }

            }

            @Override
            public void onError(Throwable t) {
                log.error("", t);
            }

            @Override
            public void onComplete() {
                AuthorizeV4.checkDecodedContentLength(request, originalDataLength.get(), r -> {
                    log.error("The Authorization was wrong. decoded content length provided in header:["
                            + r + "] is not right");
                    MsException exception = new MsException(MISSING_DECODED_CONTENT_LENGTH, "");
                    throwableConsumer.accept(exception);
                });
                AuthorizeV4.checkSingleChunkObjectSHA256(request, bufferHandler, s -> {
                    log.error("The Authorization was wrong. content sha256 provided in header:["
                            + request.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256) + "] payload:[" + s + "]");
                    MsException exception = new MsException(X_AMZ_CONTENT_SHA_256_MISMATCH, "");
                    throwableConsumer.accept(exception);
                });

                md5[0] = Hex.encodeHexString(digest.digest());
                request.getDataHandlerCompleted().set(true);
                objectSize[0] = originalDataLength.get();
                handler.complete();
            }
        };
        processor0.publishOn(DISK_SCHEDULER).subscribe(coreSubscriber);

        request.addResponseCloseHandler(terminateHandler);

        request.connection().closeHandler(terminateHandler);
    }


    public static Mono<Boolean> processHttpBody(MsHttpRequest request, Encoder handler, Digest digest, String[] md5, Long[] objectSize,
                                                MonoProcessor<String> processor, JsonObject sysMetaMap) {
        if (StringUtils.isNotBlank(request.getHeader(SYNC_COMPRESS))) {
            syncCompressProcessHttpBody(request, handler, digest, md5, objectSize, processor);
            return Mono.just(true);
        }
        Mono<Long> mono;
        if (PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            mono = Mono.just(0L);
        } else {
            mono = accountPerfLimiter.getQuota(request.getUserId(), BAND_WIDTH_QUOTA)
                    .flatMap(l -> bucketPerfLimiter.getQuota(request.getBucketName(), BAND_WIDTH_QUOTA).map(b -> l + b));
        }
        return mono
                .map(quota -> {
                    if (quota == 0L) {
                        normalProcessHttpBody(request, handler, digest, md5, objectSize, processor, sysMetaMap);
                        return true;
                    } else {
                        limitProcessHttpBody(request, handler, digest, md5, objectSize, processor, sysMetaMap);
                        return true;
                    }
                });
    }

    public static Flux<byte[]> getObject(StoragePool storagePool, String fileName, boolean smallFile, long start, long end, long fileSize,
                                         List<Tuple3<String, String, String>> nodeList,
                                         UnicastProcessor<Long> streamController, MsHttpRequest request, MsClientRequest clientRequest) {
        if (start <= end && (end - start) < MAX_CACHE_FILE_SIZE) {
            List<byte[]> res = ReadCache.getInstance().getCache(fileName, start, end);
            if (null == res) {
                Flux<byte[]> flux = getObject0(storagePool, fileName, smallFile, start, end, fileSize, nodeList,
                        streamController, request, clientRequest);
                List<byte[]> list = new LinkedList<>();
                return flux.doOnNext(list::add)
                        .doOnComplete(() -> {
                            ReadCache.getInstance().addCache(fileName, start, end, list);
                        });

            } else {
                ReadCache.getInstance().addCache(fileName, start, end, res);
                return Flux.fromStream(res.stream());
            }
        }

        return getObject0(storagePool, fileName, smallFile, start, end, fileSize, nodeList, streamController, request, clientRequest);
    }

    private static Flux<byte[]> getObject0(StoragePool storagePool, String fileName, boolean smallFile, long start, long end, long fileSize,
                                           List<Tuple3<String, String, String>> nodeList,
                                           UnicastProcessor<Long> streamController, MsHttpRequest request, MsClientRequest clientRequest) {
        if (fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
            return getObjectChunkFile(storagePool, fileName, smallFile, start, end, fileSize, nodeList,
                    streamController, request, clientRequest);
        } else {
            return getObjectSingleFile(storagePool, fileName, smallFile, start, end, fileSize, nodeList,
                    streamController, request, clientRequest);
        }
    }

    private static Flux<byte[]> getObjectChunkFile(StoragePool storagePool, String fileName, boolean smallFile, long start, long end, long fileSize,
                                                   List<Tuple3<String, String, String>> nodeList,
                                                   UnicastProcessor<Long> streamController, MsHttpRequest request, MsClientRequest clientRequest) {
        return Node.getInstance().getChunk(fileName)
                .map(chunkFile -> {
                    if (chunkFile.size == ChunkFile.ERROR_CHUNK.size) {
                        throw new NFSException(FsConstants.NfsErrorNo.NFS3ERR_I0, "getObjectChunkFile error.fileName:" + fileName);
                    }
                    return chunkFile;
                })
                .flatMapMany(chunk -> {
                    GetMultiPartObjectClientHandler getMultiPartObjectClientHandler =
                            new GetMultiPartObjectClientHandler(storagePool, start, end, fileSize, mapToPartInfo(chunk), streamController, request, clientRequest);

                    return getMultiPartObjectClientHandler.getBytes();
                });
    }

    private static Flux<byte[]> getObjectSingleFile(StoragePool storagePool, String fileName, boolean smallFile, long start, long end, long fileSize,
                                                    List<Tuple3<String, String, String>> nodeList,
                                                    UnicastProcessor<Long> streamController, MsHttpRequest request, MsClientRequest clientRequest) {
        if (start > end) {
            return Flux.just(new byte[0]);
        }

        if (StringUtils.isBlank(fileName)){
            return FsUtils.getHoleFileBytes(start, end, storagePool, streamController);
        }

        MsHttpRequest msHttpRequest = request;
        if (request != null && (StringUtils.isNotEmpty(request.getSyncTag()) && request.getSyncTag().equals(IS_SYNCING))) {
            msHttpRequest = null;
        }

        GetObjectClientHandler clientHandler = new GetObjectClientHandler(storagePool, start, end, fileName, fileSize,
                nodeList, streamController, msHttpRequest, clientRequest);
        if (request == null) {
            //异常处理流程不限流
            return clientHandler.getBytes();
        }

        if (request.getDelegate() != null && PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
            // 后台校验的getObject请求不限流。
            return clientHandler.getBytes();
        }

        final int[] count = {0};
        final int[] byteslength = {0};

        // 异步复制限流流程
        if (StringUtils.isNotEmpty(request.getSyncTag()) && request.getSyncTag().equals(IS_SYNCING)) {
            DataSyncPerfLimiter dataSyncPerfLimiter = DataSyncPerfLimiter.getInstance();
            String syncClusterIndex = Optional.ofNullable(request.getMember("target-cluster")).orElse("-1");
            return pool.getReactive(REDIS_SYSINFO_INDEX).hgetall(SYNC_QOS_RULE)
                    .flatMap(map -> {
                        if (isCurrentTimeWithinRange(map.get(START_TIME), map.get(END_TIME))) {
                            return dataSyncPerfLimiter.getQuota(DATA_SYNC_QUOTA, BAND_WIDTH_QUOTA)
                                    .flatMap(quota -> dataSyncPerfLimiter.getQuota(DATA_SYNC_QUOTA, BAND_WIDTH_QUOTA + "_" + syncClusterIndex).map(bucketQuota -> quota + bucketQuota))
                                    .flatMap(quota -> bucketPerfLimiter.getQuota(request.getBucketName(), DATASYNC_BAND_WIDTH_QUOTA).map(bucketQuota -> quota + bucketQuota))
                                    .flatMap(quota -> bucketPerfLimiter.getQuota(request.getBucketName(), DATASYNC_BAND_WIDTH_QUOTA + "_" + syncClusterIndex).map(bucketQuota -> quota + bucketQuota));
                        } else {
                            return Mono.just(0L);
                        }
                    })
                    .flatMapMany(quota -> {
                        if (quota == 0L) {
                            return clientHandler.getBytes();
                        } else {
                            return clientHandler.getBytes()
                                    .doOnNext(buf -> {
                                        count[0]++;
                                        byteslength[0] += buf.length;
                                    })
                                    .flatMap(buf -> {
                                        if (count[0] >= storagePool.getK()) {
                                            int length = byteslength[0];
                                            count[0] = 0;
                                            byteslength[0] = 0;
                                            return dataSyncPerfLimiter.limits(DATA_SYNC_QUOTA, BAND_WIDTH_QUOTA, length)
                                                    .flatMap(waitTime0 -> dataSyncPerfLimiter.limits(DATA_SYNC_QUOTA, BAND_WIDTH_QUOTA + "_" + syncClusterIndex, length)
                                                            .map(waitTime -> waitTime + waitTime0))
                                                    .flatMap(waitTime0 -> bucketPerfLimiter
                                                            .limits(request.getBucketName(), DATASYNC_BAND_WIDTH_QUOTA, length)
                                                            .map(waitTime -> waitTime + waitTime0))
                                                    .flatMap(waitTime0 -> bucketPerfLimiter
                                                            .limits(request.getBucketName(), DATASYNC_BAND_WIDTH_QUOTA + "_" + syncClusterIndex, length)
                                                            .map(waitTime -> waitTime + waitTime0))
                                                    .flatMap(waitMillis -> waitMillis == 0 ? Mono.just(buf) : Mono.just(buf).delayElement(Duration.ofMillis(waitMillis)));
                                        } else {
                                            return Mono.just(buf);
                                        }
                                    });
                        }
                    });
        }

        // 正常限流流程
        AccountPerfLimiter accountPerfLimiter = AccountPerfLimiter.getInstance();
        BucketPerfLimiter bucketPerfLimiter = BucketPerfLimiter.getInstance();
        return accountPerfLimiter.getQuota(request.getUserId(), BAND_WIDTH_QUOTA)
                .flatMap(l -> bucketPerfLimiter.getQuota(request.getBucketName(), BAND_WIDTH_QUOTA).map(b -> l + b))
                .flatMapMany(quota -> {
                    if (quota == 0L) {
                        return clientHandler.getBytes();
                    } else {
                        return clientHandler.getBytes()
                                .doOnNext(buf -> {
                                    count[0]++;
                                    byteslength[0] += buf.length;
                                })
                                .flatMap(buf -> {
                                            if (count[0] >= storagePool.getK()) {
                                                int length = byteslength[0];
                                                count[0] = 0;
                                                byteslength[0] = 0;
                                                return accountPerfLimiter
                                                        .limits(request.getUserId(), BAND_WIDTH_QUOTA, length)
                                                        .flatMap(waitMillis -> bucketPerfLimiter
                                                                .limits(request.getBucketName(), BAND_WIDTH_QUOTA, length)
                                                                .map(millis -> millis + waitMillis))
                                                        .flatMap(waitMillis -> waitMillis == 0 ? Mono.just(buf) : Mono.just(buf).delayElement(Duration.ofMillis(waitMillis)));
                                            } else {
                                                return Mono.just(buf);
                                            }
                                        }
                                );
                    }
                });
    }

    public static long bytes2long(byte[] b) {
        long s = 0;
        long s0 = b[0] & 0xff;// 最低位
        long s1 = b[1] & 0xff;
        long s2 = b[2] & 0xff;
        long s3 = b[3] & 0xff;
        long s4 = b[4] & 0xff;// 最低位
        long s5 = b[5] & 0xff;
        long s6 = b[6] & 0xff;
        long s7 = b[7] & 0xff;

        // s0不变
        s1 <<= 8;
        s2 <<= 16;
        s3 <<= 24;
        s4 <<= 8 * 4;
        s5 <<= 8 * 5;
        s6 <<= 8 * 6;
        s7 <<= 8 * 7;
        s = s0 | s1 | s2 | s3 | s4 | s5 | s6 | s7;
        return s;
    }

    public static PartInfo[] appendPartInfo(PartInfo[] partInfos, PartInfo partInfo) {
        if (partInfos != null) {
            int len1 = partInfos.length;
            PartInfo[] mergeArray = new PartInfo[len1 + 1];
            System.arraycopy(partInfos, 0, mergeArray, 0, partInfos.length);
            mergeArray[len1] = partInfo;
            return mergeArray;
        }
        return new PartInfo[]{partInfo};
    }

    public static void checkAppendHeader(MsHttpRequest request) {
        if (!request.headers().contains(CONTENT_LENGTH)) {
            throw new MsException(ErrorNo.MISSING_CONTENT_LENGTH, "append object error, no content-length param.");
        }
        if (!StringUtils.isNumeric(request.getHeader(CONTENT_LENGTH))) {
            throw new MsException(INVALID_ARGUMENT, "content length is not number.");
        }
        if (Long.parseLong(request.getHeader(CONTENT_LENGTH)) > MAX_PUT_SIZE) {
            throw new MsException(OBJECT_SIZE_OVERFLOW, "object size too large.");
        }

        if (!request.params().contains(POSITION)) {
            throw new MsException(MISSING_POSITION, "append object error, no position param.");
        }
        if (!StringUtils.isNumeric(request.getParam(POSITION))) {
            throw new MsException(INVALID_ARGUMENT, "position is not number.");
        }
    }

    public static boolean isAppendableObject(MetaData meta) {
        return meta != null && "appendObject".equals(meta.getPartUploadId());
    }
}
