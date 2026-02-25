package com.macrosan.lifecycle;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.deployment.AddClusterUtils;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.lifecycle.mq.LifecycleMessageBroker;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.aggregation.AggregateFileClient;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.params.DelMarkParams;
import com.macrosan.utils.quota.QuotaRecorder;
import com.macrosan.utils.ratelimiter.RecoverLimiter;
import com.macrosan.utils.serialize.JsonUtils;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.DoubleActiveUtil.datasyncIsEnabled;
import static com.macrosan.doubleActive.DoubleActiveUtil.getOtherSiteIndex;
import static com.macrosan.doubleActive.HeartBeatChecker.isMultiAliveStarted;
import static com.macrosan.doubleActive.HeartBeatChecker.syncPolicy;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.SWITCH_ON;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.SWITCH_SUSPEND;
import static com.macrosan.ec.ECUtils.createMeta;
import static com.macrosan.ec.ErasureClient.*;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.OVER_WRITE;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.filesystem.utils.InodeUtils.isError;
import static com.macrosan.httpserver.MossHttpClient.WRITE_ASYNC_RECORD;
import static com.macrosan.lifecycle.LifecycleService.getEndStamp;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.message.jsonmsg.Inode.NOT_FOUND_INODE;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.macrosan.storage.StorageOperate.PoolType.DATA;
import static com.macrosan.utils.worm.WormUtils.*;


public class LifecycleCommandConsumer extends DefaultConsumer {
    private static final Logger delObjLogger = LogManager.getLogger("DeleteObjLog.LifecycleCommandConsumer");
    public static final Logger logger = LogManager.getLogger(LifecycleCommandConsumer.class.getName());
    private static final ThreadPoolExecutor commandConsumerExecutor = LifecycleService.getCommandConsumerExecutor();
    public static final Scheduler LIFECYCLE_SCHEDULER = Schedulers.fromExecutor(commandConsumerExecutor);
    private static final RedisConnPool pool = RedisConnPool.getInstance();
    public static final Integer ACK_FALSE = 0;
    public static final Integer ACK_TURE = 1;
    // 对大文件做限流
    public static final Integer ACK_LIMIT = 2;
    public static LifecycleMessageBroker messageBroker;
    public static boolean isDelLimit = true;

    public LifecycleCommandConsumer(Channel channel) {
        super(channel);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

        super.handleDelivery(consumerTag, envelope, properties, body);
        try {
            Tuple2<Envelope, byte[]> tuple2 = new Tuple2<>(envelope, body);
            messageBroker.processor.onNext(tuple2);
        } catch (Exception e) {
            logger.error("Consume lifecycle command ", e);
        }
    }


    public Mono<Boolean> dealMsg(Envelope envelope, byte[] body) {

        MonoProcessor<Boolean> res = MonoProcessor.create();
        try {
            JSONObject jsonObject = JSONObject.parseObject(new String(body));
            if (System.currentTimeMillis() < getEndStamp()) {
                execute(jsonObject)
                        .subscribe(i -> dealQueueAck(i, envelope, jsonObject, res),
                                e -> dealQueueAck(ACK_FALSE, envelope, jsonObject, res));
            } else {
                dealQueueAck(ACK_TURE, envelope, jsonObject, res);
            }
        } catch (Exception e) {
            dealQueueAck(ACK_FALSE, envelope, null, res);
            logger.error("Deal with failed, reject and requeue!!! ", e);
        }

        return res;
    }


    private Mono<Integer> execute(JSONObject jsonObject) {
        if (StringUtils.isNotEmpty(jsonObject.getString("targetStorageStrategy"))) {
            String bucket = jsonObject.getString("bucket");
            String object = jsonObject.getString("object");
            String versionId = jsonObject.getString("versionId");
            String storageStrategy = jsonObject.getString("targetStorageStrategy");
            return LifecycleMoveTask.move(bucket, object, versionId, storageStrategy)
                    .doOnNext(i -> {
                        if (i.equals(ACK_TURE)) {
                            logger.debug("move bucket:{},object:{},versionId:{} to storageStrategy:{} successfully.", bucket, object, versionId, storageStrategy);
                        }
                    });
        } else {
            return RecoverLimiter.getInstance().acquireMeta(isDelLimit)
                    .flatMap(b -> expirationOperation(jsonObject));
        }
    }

    public void dealQueueAck(Integer ack, Envelope envelope, JSONObject jsonObject, MonoProcessor<Boolean> res) {
        queueAck(ack, envelope, jsonObject).subscribe(b -> {
            if (b) {
                res.onNext(true);
            } else {
                Mono.delay(Duration.ofSeconds(1)).publishOn(SCAN_SCHEDULER).subscribe(s -> dealQueueAck(ack, envelope, jsonObject, res));
            }
        });
    }

    private Mono<Boolean> queueAck(Integer ack, Envelope envelope, JSONObject jsonObject) {
        try {
            if (envelope == null) {
                LifecycleCommandProducer.semaphore.release();
                return Mono.just(true);
            }
            if (ack.equals(ACK_TURE)) {
                getChannel().basicAck(envelope.getDeliveryTag(), false);
            } else if (ack.equals(ACK_FALSE)) {
                if (jsonObject != null) {
                    String bucketName = jsonObject.containsKey("bucket") ? jsonObject.getString("bucket") : "";
                    String objName = jsonObject.containsKey("object") ? jsonObject.getString("object") : "";
                    String versionId = jsonObject.containsKey("versionId") ? jsonObject.getString("versionId") : "";
                    String strategy = jsonObject.containsKey("targetStorageStrategy") ? jsonObject.getString("targetStorageStrategy") : "";
                    String snapshotMark = jsonObject.containsKey("snapshotMark") ? jsonObject.getString("snapshotMark") : "";
                    if (StringUtils.isNotEmpty(strategy)) {
                        logger.error("Deal with failed, reject and requeue!!! bucket: {} objName:{} versionId:{} snapshotMark:{} move:{}",
                                bucketName, objName, versionId, snapshotMark, strategy);
                    } else {
                        logger.error("Deal with failed, reject and requeue!!! bucket: {} objName:{} versionId:{} snapshotMark:{}",
                                bucketName, objName, versionId, snapshotMark);
                    }
                } else {
                    logger.error("Deal with failed, reject and requeue!!! ");
                }
                getChannel().basicReject(envelope.getDeliveryTag(), true);
            } else {
                getChannel().basicReject(envelope.getDeliveryTag(), true);
            }
            return Mono.just(true);
        } catch (Exception e) {
            logger.error("Lifecycle consumer ack {} failed.", jsonObject, e);
            return Mono.just(false);
        }
    }

    private static UnSynchronizedRecord getSyncRecord(String versionId, String newVersionId, MetaData metaData) {
//        metaData.setSyncStamp(VersionUtil.getVersionNum(false)).setStamp(String.valueOf(System.currentTimeMillis()));
        UnSynchronizedRecord record = AddClusterUtils.buildSyncRecord(getOtherSiteIndex(), HttpMethod.DELETE, metaData);
        if (StringUtils.isEmpty(versionId)) {
            record.headers.put(NEW_VERSION_ID, newVersionId);
            record.versionId = null;
            record.headers.remove(VERSIONID);
        } else {
            record.setUri(record.uri + "?versionId=" + metaData.versionId);
        }
        return record;
    }

    public static Mono<Integer> expirationOperation(JSONObject infoMap) {
        final String bucketName = infoMap.getString("bucket");
        final String objName = infoMap.getString("object");
        final String versionId = infoMap.getString("versionId");
        final String status = infoMap.getString("status");
        final boolean mediaDeleteSource = infoMap.containsKey("mediaDeleteSource");
        // 扫描的截止时间戳
        final long timestamps = Optional.ofNullable(infoMap.getLong("timestamps")).orElse(Long.MAX_VALUE);
        // 待删除对象put时的快照标记
        final String snapshotMark = infoMap.getString("snapshotMark");
        final String from = infoMap.getString("from");
        // 当前最新的快照标记
        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        Tuple2<String, String> bucketVnodeIdTuple = storagePool.getBucketVnodeIdTuple(bucketName, objName);
        String bucketVnodeId = storagePool.getBucketVnodeId(bucketName);
        String bucketVnode = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        final String[] mda = new String[1];
        final String[] newVersionId = new String[1];
        String deleteStamp = String.valueOf(System.currentTimeMillis());
        MonoProcessor<Boolean> res = MonoProcessor.create();
        List<String> needDeleteFile = new LinkedList<>();
        MetaData[] metaData = new MetaData[]{null};
        UnSynchronizedRecord[] syncRecord = new UnSynchronizedRecord[]{null};
        long[] currentWormStamp = new long[1];
        String[] versionState = new String[1];
        String[] trashDir = new String[1];
        AtomicBoolean trashTag = new AtomicBoolean();
        // 判断对象是否在生命周期扫描到队列后进行修改
        AtomicBoolean metaUpdate = new AtomicBoolean(false);
        AtomicBoolean isIff = new AtomicBoolean(false);
        String[] updateDirStr = new String[1];
        currentWormTimeMillis()
                .doOnNext(wormTime -> currentWormStamp[0] = wormTime)
                .flatMap(wormTime -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName))
                .flatMap(bucketInfo -> {
                    if (bucketInfo != null && !bucketInfo.isEmpty() && bucketInfo.containsKey("fsid")) {
                        return FSQuotaUtils.existQuotaInfoS3(bucketName, objName, System.currentTimeMillis(), "")
                                .flatMap(t2 -> {
                                    if (t2.var1) {
                                        updateDirStr[0] = Json.encode(t2.var2);
                                    }
                                    return Mono.just(bucketInfo);
                                })
                                .onErrorResume(e -> Mono.just(bucketInfo));
                    }
                    return Mono.just(bucketInfo);
                })
                .flatMap(bucketInfo -> {
                    mda[0] = bucketInfo.get("mda");
                    String versionStatus = bucketInfo.getOrDefault(BUCKET_VERSION_STATUS, "NULL");
                    versionState[0] = versionStatus;
                    newVersionId[0] = "NULL".equals(versionStatus) || VERSION_SUSPENDED.equals(versionStatus) ?
                            "null" : RandomStringUtils.randomAlphanumeric(32);
                    trashDir[0] = bucketInfo.getOrDefault("trashDir", null);
                    if (!StringUtils.isBlank(trashDir[0])) {
                        if (!objName.startsWith(trashDir[0])) {
                            trashTag.set(true);
                        }
                    }
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                    return storagePool.mapToNodeInfo(bucketVnode);
                })
                //1、检查对象元数据
                .flatMap(list -> {
                    if (StringUtils.isNotBlank(updateDirStr[0])) {
                        return getObjectMetaVersionFsQuotaRecover(bucketName, objName, StringUtils.isNotEmpty(versionId) ? versionId : "NULL".equals(status) ? "null" : "", list, null,
                                snapshotMark, null, updateDirStr[0], true).zipWith(storagePool.mapToNodeInfo(bucketVnodeId));
                    }
                    return getObjectMetaVersion(bucketName, objName, StringUtils.isNotEmpty(versionId)
                            ? versionId : "NULL".equals(status) ? "null" : "", list, null, snapshotMark, null).zipWith(storagePool.mapToNodeInfo(bucketVnodeId));
                })
                .doOnNext(tuple -> {
                    MetaData meta = tuple.getT1();
                    if (meta.equals(ERROR_META)) {
                        throw new MsException(UNKNOWN_ERROR, "Get Object Meta Data fail");
                    } else if (!meta.equals(NOT_FOUND_META) && !meta.deleteMark && meta.isViewable(currentSnapshotMark[0])) {
                        if (meta.partUploadId == null) {
                            if (meta.isCurrentSnapshotObject(currentSnapshotMark[0])) {// 只删除当前快照下的数据块
                                needDeleteFile.add(meta.getFileName());
                            }
                        } else {
                            for (PartInfo partInfo : meta.partInfos) {
                                if (partInfo.isCurrentSnapshotObject(currentSnapshotMark[0])) {// 只删除当前快照下的数据块
                                    needDeleteFile.add(partInfo.fileName);
                                }
                            }
                        }
                        Map<String, String> sysMap = Json.decodeValue(meta.getSysMetaData(), new TypeReference<Map<String, String>>() {
                        });
                        if (sysMap.containsKey(NO_SYNCHRONIZATION_KEY) && sysMap.get(NO_SYNCHRONIZATION_KEY).equals(NO_SYNCHRONIZATION_VALUE)) {
                            isIff.set(true);
                        }
                    }

                    metaData[0] = meta;

                    if (infoMap.containsKey("no_check")) {
                        metaUpdate.set(false);
                    } else if (metaData[0].inode == 0) {
                        if (Long.parseLong(metaData[0].stamp) >= timestamps) metaUpdate.set(true);
                    } else {
                        Node.getInstance().getInode(metaData[0].bucket, metaData[0].inode)
                                .subscribe(inode -> {
                                    long mtimeMs = inode.getMtime() * 1000L;
                                    if (NOT_FOUND_INODE.equals(inode) || isError(inode) || mtimeMs >= timestamps)
                                        metaUpdate.set(true);
                                });
                    }
                })
                .doOnNext(tuple2 -> {
                    if (infoMap.containsKey("no_check")) {
                        return;
                    }
                    MetaData meta = tuple2.getT1();
                    // 只有真删的时候进行worm保护并且不保护deleteMarker
                    if (!metaUpdate.get() && !meta.equals(NOT_FOUND_META) && !meta.deleteMark && !meta.deleteMarker && (versionId != null || (!"Enabled".equals(versionState[0]))) && meta.isViewable(currentSnapshotMark[0])) {
                        checkWormRetention(null, getWormExpire(meta), currentWormStamp[0]);
                    }
                })
                .zipWith(datasyncIsEnabled(bucketName))
                .flatMap(tuple -> {
                    if (isMultiAliveStarted && !metaUpdate.get() && ("0".equals(syncPolicy) || mediaDeleteSource) && (SWITCH_ON.equals(tuple.getT2()) || SWITCH_SUSPEND.equals(tuple.getT2())) && !tuple.getT1().getT1().equals(NOT_FOUND_META) && !tuple.getT1().getT1().deleteMark && !isIff.get()) {
                        syncRecord[0] = getSyncRecord(versionId, newVersionId[0], metaData[0]).setCommited(false);
                        syncRecord[0].headers.replace("stamp", deleteStamp);
                        if (trashTag.get()) {
                            syncRecord[0].headers.put("trashDir", trashDir[0]);
                        }
                        return ECUtils.putSynchronizedRecord(storagePool, syncRecord[0].rocksKey(), Json.encode(syncRecord[0]), tuple.getT1().getT2(), mediaDeleteSource ? WRITE_ASYNC_RECORD : false);
                    }
                    return Mono.just(true);
                })
                .flatMap(b -> {
                    if (!b) {
                        return Mono.just(b);
                    }
                    if (metaData[0].equals(NOT_FOUND_META) || metaData[0].deleteMark || metaData[0].isUnView(currentSnapshotMark[0])) {
                        return Mono.just(true);
                    }
                    //检测是否开启桶回收站功能
                    if (!metaUpdate.get() && trashTag.get()) {
                        return moveTrash(bucketName, objName, trashDir[0], metaData[0], mda[0], currentSnapshotMark[0], snapshotLink[0]);
                    }
                    return Mono.just(b);
                })
                .flatMap(r -> {
                    if (!r) {
                        return Mono.just(r);
                    }
                    if (metaUpdate.get()) {
                        return Mono.just(true);
                    }
                    MetaData meta = metaData[0];
                    if (meta.equals(NOT_FOUND_META) || meta.deleteMark || meta.isUnView(currentSnapshotMark[0])) {
                        if (StringUtils.isNotEmpty(mda[0]) && "on".equals(mda[0]) && StringUtils.isNotEmpty(versionId)) {
                            return deleteEs(bucketName, objName, versionId);
                        } else {
                            return Mono.just(true);
                        }
                    }
                    //历史版本会传版本号过来(真删)
                    if (versionId != null) {
                        //如果该版本号对应的是删除标记，删除该删除标记
                        if (meta.deleteMarker) {
                            String[] versionNum = new String[]{null};
                            // 双写，写入新的vnode中
                            return VersionUtil.getVersionNum(bucketName, objName)
                                    .doOnNext(currentVersionNum -> versionNum[0] = currentVersionNum)
                                    .flatMap(currentVersionNum -> {
                                        if (migrateVnode != null) {
                                            return storagePool.mapToNodeInfo(migrateVnode)
                                                    .flatMap(migrateVnodeList -> setDelMark(new DelMarkParams(bucketName, objName, versionId, meta, migrateVnodeList, versionState[0], currentVersionNum).migrate(true), currentSnapshotMark[0], snapshotLink[0]))
                                                    .doOnNext(b1 -> {
                                                        if (b1 == 0) {
                                                            logger.error("delete object deletemarker from new mapping internal error");
                                                        }
                                                    });

                                        }
                                        return Mono.just(1);
                                    })
                                    // 写入旧的vnode中
                                    .flatMap(b -> {
                                        if (b > 0) {
                                            return storagePool.mapToNodeInfo(bucketVnode)
                                                    .flatMap(bucketVnodeList -> setDelMarkWithFsQuota(new DelMarkParams(bucketName, objName, versionId, meta, bucketVnodeList, versionState[0], versionNum[0]), currentSnapshotMark[0], snapshotLink[0], updateDirStr[0]))
                                                    .doOnNext(b1 -> {
                                                        if (b1 == 0) {
                                                            logger.error("delete object deletemarker internal error");
                                                        }
                                                    }).map(r1 -> r1 > 0);
                                        }
                                        return Mono.just(false);
                                    });
                        } else {
                            boolean[] syncDelete = new boolean[]{true};
                            String[] versionNum = new String[]{null};
                            return VersionUtil.getVersionNum(bucketName, objName)
                                    .doOnNext(currentVersionNum -> versionNum[0] = currentVersionNum)
                                    .flatMap(currentVersionNum -> {
                                        if (migrateVnode != null) {
                                            return storagePool.mapToNodeInfo(migrateVnode)
                                                    .flatMap(migrateVnodeList -> setDelMark(new DelMarkParams(bucketName, objName, versionId, meta, migrateVnodeList, versionState[0], currentVersionNum).migrate(true), currentSnapshotMark[0], snapshotLink[0]))
                                                    .doOnNext(b1 -> {
                                                        if (b1 == 0) {
                                                            logger.error("delete object deletemarker from new mapping internal error");
                                                        }
                                                    });

                                        }
                                        return Mono.just(1);
                                    })
                                    .doOnNext(r1 -> syncDelete[0] = syncDelete[0] && r1 == 1)
                                    // 写入旧的vnode中
                                    .flatMap(b -> {
                                        if (b > 0) {
                                            return storagePool.mapToNodeInfo(bucketVnode)
                                                    .flatMap(bucketVnodeList -> {
                                                        if (metaData[0].inode == 0) {
                                                            return setDelMarkWithFsQuota(new DelMarkParams(bucketName, objName, versionId, meta, bucketVnodeList, versionState[0], versionNum[0]), currentSnapshotMark[0], snapshotLink[0], updateDirStr[0]);
                                                        }
                                                        return Mono.just(1);
                                                    })
                                                    .doOnNext(b1 -> {
                                                        if (b1 == 0) {
                                                            logger.error("delete object deletemarker internal error");
                                                        }
                                                    });
                                        }
                                        return Mono.just(0);
                                    })
                                    .doOnNext(r1 -> syncDelete[0] = syncDelete[0] && r1 == 1)
                                    .flatMap(b -> {
                                        if (b == 0) {
                                            logger.info("setDelMark Error ! ! ! ! ! !");
                                            return Mono.just(false);
                                        }

                                        return deleteObjectOrFile(bucketName, objName, needDeleteFile, mda[0], metaData, currentSnapshotMark[0], timestamps, syncDelete[0], metaUpdate);
                                    });
                        }
                    }
                    //未开启多版本和最新版本不带版本号
                    else {
                        if ("Enabled".equals(status)) {
                            if (!meta.deleteMarker) {
                                return addVersionDeleteMarker(bucketName, objName, bucketVnode, migrateVnode, meta.setVersionId(newVersionId[0]).setStamp(deleteStamp), status, currentSnapshotMark[0], snapshotLink[0]);
                            }
                            return Mono.just(true);
                        } else if ("Suspended".equals(status)) {
                            if (meta.deleteMarker) {
                                return Mono.just(true);
                            }
                            List<String> nullNeedDeleteFile = new LinkedList<>();
                            return storagePool.mapToNodeInfo(bucketVnode)
                                    .flatMap(list -> getObjectMetaVersion(bucketName, objName, "null", list, null, currentSnapshotMark[0], snapshotLink[0]))
                                    .flatMap(nullMeta -> {
                                        //删除版本号为null的对象
                                        if (nullMeta.isAvailable()) {
                                            if (nullMeta.partUploadId == null) {
                                                if (nullMeta.isCurrentSnapshotObject(currentSnapshotMark[0])) {
                                                    nullNeedDeleteFile.add(nullMeta.getFileName());
                                                }
                                            } else {
                                                for (PartInfo partInfo : nullMeta.partInfos) {
                                                    if (partInfo.isCurrentSnapshotObject(currentSnapshotMark[0])) {
                                                        nullNeedDeleteFile.add(partInfo.fileName);
                                                    }
                                                }
                                            }
                                            metaData[0] = nullMeta;

                                            return deleteObjectOrFile(bucketName, objName, needDeleteFile, mda[0], metaData, currentSnapshotMark[0], timestamps, true, metaUpdate);

                                        } else if (nullMeta.equals(ERROR_META)) {
                                            logger.error("Get Object Meta Data fail");
                                            return Mono.just(false);
                                        } else {
                                            return Mono.just(true);
                                        }
                                    })
                                    .flatMap(b -> {
                                        if (!b) {
                                            return Mono.just(false);
                                        }
                                        return addVersionDeleteMarker(bucketName, objName, bucketVnode, migrateVnode, meta.setVersionId(newVersionId[0]).setStamp(deleteStamp), status, currentSnapshotMark[0], snapshotLink[0]);
                                    });
                        }
                        //未开启多版本
                        else {
                            boolean[] syncDelete = new boolean[]{true};
                            String[] versionNum = new String[]{null};
                            return VersionUtil.getVersionNum(bucketName, objName)
                                    .doOnNext(currentVersionNum -> versionNum[0] = currentVersionNum)
                                    .flatMap(currentVersionNum -> {
                                        if (StringUtils.isNotBlank(migrateVnode)) {
                                            return storagePool.mapToNodeInfo(migrateVnode)
                                                    .flatMap(migrateVnodeList -> setDelMark(new DelMarkParams(bucketName, objName, "null", meta, migrateVnodeList, versionState[0], currentVersionNum).migrate(true), currentSnapshotMark[0], snapshotLink[0]))
                                                    .doOnNext(b1 -> {
                                                        if (b1 == 0) {
                                                            logger.error("delete object deletemarker from new mapping internal error");
                                                        }
                                                    });

                                        }
                                        return Mono.just(1);
                                    })
                                    .doOnNext(r1 -> syncDelete[0] = syncDelete[0] && r1 == 1)
                                    // 写入旧的vnode中
                                    .flatMap(b -> {
                                        if (b > 0) {
                                            return storagePool.mapToNodeInfo(bucketVnode)
                                                    .flatMap(bucketVnodeList -> {
                                                        if (metaData[0].inode == 0) {
                                                            return setDelMarkWithFsQuota(new DelMarkParams(bucketName, objName, "null", meta, bucketVnodeList, versionState[0], versionNum[0]), currentSnapshotMark[0], snapshotLink[0], updateDirStr[0]);
                                                        }
                                                        return Mono.just(1);
                                                    })
                                                    .doOnNext(b1 -> {
                                                        if (b1 == 0) {
                                                            logger.error("delete object deletemarker internal error");
                                                        }
                                                    });
                                        }
                                        return Mono.just(0);
                                    })
                                    .doOnNext(r1 -> syncDelete[0] = syncDelete[0] && r1 == 1)
                                    .flatMap(b -> {
                                        if (b == 0) {
                                            logger.info("setDelMark Error ! ! ! ! ! !");
                                            return Mono.just(false);
                                        }

                                        return deleteObjectOrFile(bucketName, objName, needDeleteFile, mda[0], metaData, currentSnapshotMark[0], timestamps, syncDelete[0], metaUpdate);
                                    });
                        }
                    }
                })
                .flatMap(b -> {
                    if (b && isMultiAliveStarted && !metaUpdate.get() && syncRecord[0] != null) {
                        return storagePool.mapToNodeInfo(bucketVnodeId)
                                .flatMap(nodeList -> ECUtils.updateSyncRecord(syncRecord[0].setCommited(true), nodeList, mediaDeleteSource ? WRITE_ASYNC_RECORD : false).map(resInt -> resInt != 0));
                    } else {
                        return Mono.just(b);
                    }
                })
                .subscribe(b -> {
                    if (b) {
                        res.onNext(true);
                        //5、删除配额
                        if (metaUpdate.get()) {
                            delObjLogger.info("Object metadata has been modified, bucketName:{} , objName: {} , versionId: {}", bucketName, objName, versionId);
                            return;
                        }
                        QuotaRecorder.addCheckBucket(bucketName);
                        if (StringUtils.isNotEmpty(from)) {
                            delObjLogger.info("clear model deleteObject success,bucketName:{} , objName: {} , versionId: {}", bucketName, objName, versionId);
                        } else {
                            delObjLogger.info("lifecycle deleteObject success,bucketName:{} , objName: {} , versionId: {}", bucketName, objName, versionId);
                        }
                    } else {
                        res.onNext(false);
                    }
                }, e -> {
                    if (e != null && e.getMessage() != null && e.getMessage().contains("protected by WORM")) {
                        logger.error("", e);
                        res.onNext(true);
                    } else {
                        res.onNext(false);
                    }
                });
        return res.timeout(Duration.ofSeconds(120)).doOnError(logger::error).onErrorReturn(false).flatMap(b -> {
            if (b) {
                return Mono.just(ACK_TURE);
            } else {
                return Mono.just(ACK_FALSE);
            }
        });
    }

    private static Mono<Boolean> addVersionDeleteMarker(String bucketName, String objName, String bucketVnode, String migrateVnode, MetaData meta, String status,
                                                        String currentSnapshotMark, String snapshotLink) {
        final String[] metaKey = new String[1];
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String sysMetaData = meta.getSysMetaData();
        Map<String, String> sysMetaMap = Json.decodeValue(sysMetaData, new TypeReference<Map<String, String>>() {
        });
        String gmt = MsDateUtils.stampToGMT(Long.parseLong(meta.stamp));
        sysMetaMap.put("Last-Modified", gmt);
        meta.setSysMetaData(JsonUtils.toString(sysMetaMap, HashMap.class));
        metaKey[0] = Utils.getMetaDataKey(bucketVnode, bucketName, objName, meta.versionId, meta.stamp, currentSnapshotMark);
        meta.setReferencedVersionId(meta.versionId);
        meta.setDeleteMarker(true);
//        meta.setVersionNum(VersionUtil.getVersionNum(bucketName));
        meta.setFileName(null);
        meta.setAggregationKey(null);
        meta.setPartUploadId(null);
        meta.setPartInfos(null);
        meta.setShardingStamp(VersionUtil.getVersionNum());
        meta.setSnapshotMark(currentSnapshotMark);

        return VersionUtil.getVersionNum(bucketName, objName)
                .doOnNext(meta::setVersionNum)
                .flatMap(b -> {
                    if (StringUtils.isNotEmpty(migrateVnode)) {
                        return storagePool.mapToNodeInfo(migrateVnode)
                                .flatMap(migrateVnodeList -> putMetaData(Utils.getMetaDataKey(migrateVnode, bucketName, objName, meta.versionId, meta.stamp, currentSnapshotMark), meta, migrateVnodeList, null, null, "", status, null, true, snapshotLink))
                                .doOnNext(r -> {
                                    if (!r) {
                                        logger.error("insert object deletemarker to new mapping internal error");
                                    }
                                });
                    }
                    return Mono.just(true);
                })
                .flatMap(b -> {
                    if (b) {
                        return storagePool.mapToNodeInfo(bucketVnode)
                                .flatMap(bucketVnodeList -> putMetaData(metaKey[0], meta, bucketVnodeList, null, null, "", status, null, false, snapshotLink))
                                .doOnNext(r -> {
                                    if (!r) {
                                        logger.error("insert object deletemarker internal error");
                                    }
                                });
                    }
                    return Mono.just(false);
                });
    }

    //分段对象没有duplicateKey
    private static Mono<Boolean> deleteVersionObj(List<String> needDeleteFile, String mda, MetaData[] metaData, String currentSnapshotMark) {
        StoragePool storagePool = StoragePoolFactory.getStoragePool(metaData[0]);
        List<String> dupFile = new LinkedList<>();
        List<String> allFile = new LinkedList<>();
        if (StringUtils.isNotEmpty(metaData[0].aggregationKey)) {
            StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(metaData[0].bucket);
            String vnode = Utils.getVnode(metaData[0].aggregationKey);
            return metaStoragePool.mapToNodeInfo(vnode)
                    .flatMap(nodeList -> AggregateFileClient.freeAggregationSpace(metaData[0].aggregationKey, nodeList));
        } else if (StringUtils.isEmpty(metaData[0].duplicateKey)) {
            if (metaData[0].partUploadId != null) {
                PartInfo[] partInfos = metaData[0].partInfos;
                for (PartInfo partInfo : partInfos) {
                    if (partInfo.isCurrentSnapshotObject(currentSnapshotMark)) {
                        if (StringUtils.isNotEmpty(partInfo.deduplicateKey)) {
                            dupFile.add(partInfo.deduplicateKey);
                        } else {
                            allFile.add(partInfo.fileName);
                        }
                    }
                }
            }
        } else {
            if (metaData[0].isCurrentSnapshotObject(currentSnapshotMark)) {
                dupFile.add(metaData[0].duplicateKey);
            }
        }
        return Mono.just("")
                .flatMap(l -> {
                    if (dupFile.isEmpty()) {
                        try {
                            if (!needDeleteFile.isEmpty()) {
                                return ErasureClient.deleteObjectFile(storagePool, needDeleteFile.toArray(new String[0]), null);
                            }
                            return Mono.just(true);
                        } catch (Exception e) {
                            logger.error("deleteObjectFile error !!! " + e);
                            return Mono.just(false);
                        }
                    } else {
                        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(metaData[0].bucket);
                        return ErasureClient.deleteDedupObjectFile(metaStoragePool, dupFile.toArray(new String[0]), null, false)
                                .flatMap(b -> {
                                    if (b && allFile.size() > 0) {
                                        return ErasureClient.deleteObjectFile(storagePool, allFile.toArray(new String[0]), null);
                                    }
                                    return Mono.just(b);
                                });
                    }
                })
                .flatMap(b -> {
                            //4、如果元数据检索开关为on的状态，则删除es
                            if (StringUtils.isNotEmpty(mda) && "on".equals(mda)) {
                                return deleteEs(metaData[0].bucket, metaData[0].key, metaData[0].versionId);
                            } else {
                                return Mono.just(true);
                            }
                        }
                );
    }

    /**
     * 根据 inodeId 判断是对象还是文件，调用相应删除路径：
     * - 文件（metaData[0].inode > 0）：
     * - 文件：直接通过 inode 删除
     * - 对象：回退到原有对象物理删除 deleteVersionObj
     */
    private static Mono<Boolean> deleteObjectOrFile(String bucket, String objName, List<String> needDeleteFile, String mda, MetaData[] metaData, String currentSnapshotMark, long timestamps, boolean syncDelete, AtomicBoolean metaUpdate) {
        if (metaData[0] == null || metaData[0].inode <= 0) {
            if (metaData[0] != null && Long.parseLong(metaData[0].stamp) > timestamps) {
                return Mono.just(true);
            }
            if (!syncDelete) {
                return Mono.just(true);
            }
            return deleteVersionObj(needDeleteFile, mda, metaData, currentSnapshotMark);
        }

        return Node.getInstance().getInode(bucket, metaData[0].inode)
                .flatMap(inode -> {
                    if (InodeUtils.isError(inode) || inode.getNodeId() <= 0) {
                        return Mono.just(true);
                    }

                    // 文件的最后一次修改时间 Mtime 在本次扫描的截止时间范围之外，直接认定它已经被处理过了，跳过它。
                    long mtimeMs = inode.getMtime() * 1000L;
                    if (mtimeMs >= timestamps) {
                        metaUpdate.set(true);
                        return Mono.just(true);
                    }

                    Mono<Boolean> canDeleteMono = Mono.just(true);;

                    if (objName.endsWith("/")) {
                        // 如果是目录，查询列表检查是否为空
                        canDeleteMono = isDirectoryEmpty(bucket, objName);
                    }

                    return canDeleteMono.flatMap(canDelete -> {
                        // 目录非空，直接跳过
                        if (!canDelete) {
                            return Mono.just(true);
                        }
                        return Node.getInstance().deleteInode(inode.getNodeId(), bucket, objName)
                                .flatMap(deletedInode -> {
                                    if (isError(deletedInode)) {
                                        return Mono.just(false);
                                    }
                                    if (StringUtils.isNotEmpty(mda) && "on".equals(mda)) {
                                        return EsMetaTask.delEsMeta(deletedInode.clone().setObjName(objName), inode.clone().setObjName(objName), bucket, objName, inode.getNodeId(), true);
                                    }
                                    return Mono.just(true);
                                });

                    });
                });
    }

    /**
     * 检查目录是否为空
     *
     * @param bucket 存储桶
     * @param dirObjName 目录对象名
     * @return Mono<Boolean> true  = 目录为空, false = 目录非空
     */
    private static Mono<Boolean>  isDirectoryEmpty(String bucket, String dirObjName) {
        // 确保前缀以 "/" 结尾
        String prefix = dirObjName.endsWith("/") ? dirObjName : dirObjName + "/";

        return FsUtils.listObjectNoRepair(bucket, prefix, null, 3, "*", -1)
                .map(inodeList -> {
                    if (inodeList == null || inodeList.isEmpty()) {
                        return true;
                    }

                    for (Inode inode : inodeList) {
                        // 如果发现任何一个对象的 key 不等于目录本身
                        // 说明存在子文件，返回 false (非空)
                        if (!inode.isDeleteMark() && !inode.getObjName().equals(prefix)) {
                            return false;
                        }
                    }

                    return true;
                })
                .defaultIfEmpty(true);
    }

    private static Mono<Boolean> moveTrash(String bucket, String object, String dir, MetaData meta, String mda, String currentSnapshotMark, String snapshotLink) {
        StoragePool bucketPool = StoragePoolFactory.getMetaStoragePool(bucket);
        MonoProcessor<Boolean> recoverDataProcessor = MonoProcessor.create();
        String newObjectName = "";
        if (object.startsWith(dir)) {
            newObjectName = object.substring(dir.length());
        } else {
            newObjectName = dir + object;
        }
        String requestId = getRequestId();
        final Map<String, String> sysMetaMap = new HashMap<>();
        String stamp1 = String.valueOf(System.currentTimeMillis());
        String lastModify = MsDateUtils.stampToGMT(Long.parseLong(stamp1));

        if (StringUtils.isEmpty(mda)) {
            mda = "off";
        }

        com.macrosan.utils.functional.Tuple2<String, String> bucketVnodeIdTuple = bucketPool.getBucketVnodeIdTuple(bucket, newObjectName);
        final String targetBucketVnode = bucketVnodeIdTuple.var1;
        final String targetBucketMigrateVnode = bucketVnodeIdTuple.var2;
        AtomicBoolean needCopyWrite = new AtomicBoolean(StringUtils.isNotEmpty(meta.aggregationKey));
        String finalNewObjectName = newObjectName;
        final String[] stamp = {null};
        StoragePool dataPool = StoragePoolFactory.getStoragePool(meta);
        StoragePool targetPool;
        if (needCopyWrite.get()) {
            StorageOperate operate = new StorageOperate(DATA, newObjectName, meta.endIndex + 1);
            targetPool = StoragePoolFactory.getStoragePool(operate, bucket);
        } else {
            targetPool = dataPool;
        }
        String[] targetObjSuffix = new String[]{ROCKS_FILE_META_PREFIX + dataPool.getObjectVnodeId(bucket, newObjectName).var2};
        MetaData metaData = createMeta(targetPool, bucket, newObjectName, requestId);
        JsonObject sysMeta = new JsonObject(meta.sysMetaData);
        // 对象的worm属性不复制
        sysMeta.fieldNames()
                .stream()
                .filter(key -> !EXPIRATION.equals(key))
                .forEach(key -> sysMetaMap.put(key, String.valueOf(sysMeta.getValue(key))));

        String finalNewObjectName1 = newObjectName;
        String finalMda = mda;
        return Mono.just("null")
                .zipWith(VersionUtil.getVersionNum(bucket, object))
                .doOnNext(tuple2 -> {
                    String version = tuple2.getT1();
                    String versionNum = tuple2.getT2();
                    targetObjSuffix[0] = targetObjSuffix[0] + version + requestId;
                    stamp[0] = Utils.getMetaDataKey(targetBucketVnode, bucket, finalNewObjectName, version, stamp1, currentSnapshotMark);
                    sysMetaMap.put(LAST_MODIFY, lastModify);

                    metaData.setVersionId(version).setVersionNum(versionNum).setStamp(stamp1)
                            .setFileName(meta.fileName).setEndIndex(meta.endIndex)
                            .setObjectAcl(meta.getObjectAcl())
                            .setReferencedBucket(meta.referencedBucket)
                            .setReferencedKey(meta.referencedKey)
                            .setSyncStamp(meta.syncStamp)
                            .setShardingStamp(VersionUtil.getVersionNum())
                            .setReferencedVersionId(meta.referencedVersionId)
                            .setSnapshotMark(currentSnapshotMark);
                    if (meta.getPartInfos() != null) {
                        String uploadId = RandomStringUtils.randomAlphabetic(32);
                        PartInfo[] metaPartInfos = meta.getPartInfos();
                        int size = metaPartInfos.length;
                        PartInfo[] updatePartInfos = new PartInfo[size];
                        for (int i = 0; i < size; i++) {
                            PartInfo sourceInfo = metaPartInfos[i];
                            PartInfo info = new PartInfo();
                            info.setBucket(bucket);
                            info.setObject(finalNewObjectName);
                            info.setUploadId(uploadId);
                            info.setFileName(sourceInfo.fileName)
                                    .setEtag(sourceInfo.etag)
                                    .setStorage(sourceInfo.storage)
                                    .setVersionId(sourceInfo.versionId)
                                    .setLastModified(sourceInfo.lastModified)
                                    .setPartNum(sourceInfo.partNum)
                                    .setVersionNum(sourceInfo.versionNum)
                                    .setPartSize(sourceInfo.partSize)
                                    .setDelete(sourceInfo.delete)
                                    .setSyncStamp(sourceInfo.syncStamp)
                                    .setSnapshotMark(currentSnapshotMark);
                            updatePartInfos[i] = info;
                        }

                        metaData.setPartInfos(updatePartInfos);
                        metaData.setPartUploadId(uploadId);
                    }

                    metaData.setUserMetaData(meta.userMetaData);
                    metaData.setSysMetaData(JsonUtils.toString(sysMetaMap, HashMap.class));
                    if (meta.referencedKey.equals(finalNewObjectName) && meta.referencedBucket.equals(bucket) && meta.referencedVersionId.equals(version)) {
                        targetObjSuffix[0] = "#" + requestId;
                    }
                }).flatMap(b -> {
                    if (needCopyWrite.get()) {
                        String fileMetaKey = Utils.getVersionMetaDataKey(targetBucketVnode, bucket, metaData.key, metaData.versionId, metaData.snapshotMark);
                        return ErasureClient.copyCryptoData(dataPool, targetPool, meta, metaData, null, recoverDataProcessor, requestId, fileMetaKey);
                    }
                    String objVnode = dataPool.getObjectVnodeId(metaData);
                    return dataPool.mapToNodeInfo(objVnode)
                            .flatMap(list -> updateFileMeta(metaData, targetObjSuffix[0], recoverDataProcessor, null, null));
                }).doOnNext(b -> {
                    if (!b) {
                        throw new MsException(UNKNOWN_ERROR, "move object internal error");
                    }
                })
                .flatMap(b -> bucketPool.mapToNodeInfo(targetBucketVnode))
                .flatMap(bucketList -> {
                            if (metaData.getPartInfos() != null) {
                                PartInfo[] metaPartInfos = metaData.getPartInfos();
                                for (PartInfo info : metaPartInfos) {
                                    info.setFileName(info.fileName.split(ROCKS_FILE_META_PREFIX)[0] + targetObjSuffix[0]);
                                }
                                metaData.setPartInfos(metaPartInfos);
                            } else if (!needCopyWrite.get()) {
                                metaData.setFileName(metaData.fileName.split(ROCKS_FILE_META_PREFIX)[0] + targetObjSuffix[0]);
                            }
                            return Mono.just(targetBucketMigrateVnode != null)
                                    .flatMap(b -> {
                                        if (b) {
                                            if (targetBucketMigrateVnode.equals(targetBucketVnode)) {
                                                return Mono.just(true);
                                            }
                                            return bucketPool.mapToNodeInfo(targetBucketMigrateVnode)
                                                    .flatMap(nodeList -> putMetaData(Utils.getMetaDataKey(targetBucketMigrateVnode, bucket,
                                                            finalNewObjectName1, metaData.versionId, metaData.stamp, metaData.snapshotMark), metaData, nodeList, recoverDataProcessor, null, finalMda, null, snapshotLink));
                                        }
                                        return Mono.just(true);
                                    })
                                    .doOnNext(b -> {
                                        if (!b) {
                                            throw new MsException(UNKNOWN_ERROR, "put object meta to new mapping error!");
                                        }
                                    }).flatMap(b -> putMetaData(stamp[0], metaData, bucketList, recoverDataProcessor, null, finalMda, null, snapshotLink)
                                            .doOnNext(res -> {
                                                if (!res) {
                                                    Set<String> overWriteFileNames = new HashSet<>();
                                                    if (metaData.partUploadId != null) {
                                                        for (PartInfo info : metaData.partInfos) {
                                                            overWriteFileNames.add(info.fileName);
                                                        }
                                                    } else if (StringUtils.isNotEmpty(metaData.aggregationKey)) {
                                                        overWriteFileNames.add(metaData.aggregationKey);
                                                    } else {
                                                        overWriteFileNames.add(metaData.fileName);
                                                    }
                                                    String[] overWriteFileName = overWriteFileNames.toArray(new String[0]);
                                                    String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(dataPool.getVnodePrefix());
                                                    SocketReqMsg overWriteMsg = new SocketReqMsg("", 0)
                                                            .put("bucket", metaData.referencedBucket)
                                                            .put("storage", dataPool.getVnodePrefix())
                                                            .put("object", metaData.referencedKey)
                                                            .put("overWrite0", Json.encode(overWriteFileName))
                                                            .put("poolQueueTag", poolQueueTag);
                                                    ObjectPublisher.basicPublish(bucketList.get(0).var1, overWriteMsg, OVER_WRITE);
                                                    throw new MsException(UNKNOWN_ERROR, "put meta data error!");
                                                }
                                            }));
                        }
                )
                .flatMap(b -> {
                    if ("on".equals(finalMda)) {
                        MonoProcessor<Boolean> esRes = MonoProcessor.create();
                        String userid = new JsonObject(meta.sysMetaData).getString("owner");
                        EsMeta esMeta = new EsMeta()
                                .setUserId(userid)
                                .setSysMetaData(JsonUtils.toString(sysMetaMap, HashMap.class))
                                .setUserMetaData(meta.userMetaData)
                                .setBucketName(bucket)
                                .setObjName(finalNewObjectName)
                                .setVersionId(metaData.versionId)
                                .setStamp(stamp1)
                                .setObjSize(String.valueOf(metaData.endIndex + 1))
                                .setInode(metaData.inode);
                        return Mono.just(1).publishOn(DISK_SCHEDULER).flatMap(l -> EsMetaTask.putEsMeta(esMeta, false)).doOnNext(esRes::onNext).flatMap(f -> esRes);
                    } else {
                        return Mono.just(b);
                    }
                }).flatMap(b -> {
                    try {
                        QuotaRecorder.addCheckBucket(bucket);
                        return Mono.just(true);
                    } catch (Exception e) {
                        logger.error(e);
                        return Mono.just(false);
                    }
                });
    }

    private static Mono<? extends Boolean> deleteEs(String bucket, String object, String versionId) {
        EsMeta esMeta = new EsMeta()
                .setBucketName(bucket)
                .setObjName(object)
                .setVersionId(versionId);

        return Mono.just(1).publishOn(LifecycleService.getScheduler()).flatMap(l -> EsMetaTask.delEsMetaAsync(esMeta));
    }

}
