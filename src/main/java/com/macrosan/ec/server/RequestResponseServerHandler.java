package com.macrosan.ec.server;


import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.database.rocksdb.MossMergeOperator;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.database.rocksdb.batch.BatchRocksDB.RequestConsumer;
import com.macrosan.database.rocksdb.batch.WriteBatch;
import com.macrosan.doubleActive.DataSynChecker;
import com.macrosan.doubleActive.arbitration.Arbitrator;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.doubleActive.arbitration.DAVersionUtils;
import com.macrosan.ec.GetMetaResEnum;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.error.overwrite.OverWriteHandler;
import com.macrosan.ec.rebuild.*;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.cache.Vnode;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.fs.BlockDevice;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.LocalPayload;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.rsocket.server.MsPayload;
import com.macrosan.snapshot.service.SnapshotGetObjectService;
import com.macrosan.snapshot.service.SnapshotListPartInfoService;
import com.macrosan.snapshot.service.SnapshotMergeService;
import com.macrosan.storage.PoolHealth;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.compressor.CompressorUtils;
import com.macrosan.storage.crypto.rootKey.RootSecretKeyManager;
import com.macrosan.storage.metaserver.BucketShardCache;
import com.macrosan.storage.metaserver.ObjectSplitTree;
import com.macrosan.storage.metaserver.ShardingWorker;
import com.macrosan.storage.strategy.StorageStrategy;
import com.macrosan.utils.aggregation.AggregationUtils;
import com.macrosan.utils.bucketLog.LogRecorder;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.quota.StatisticsRecorder.RecordType;
import com.macrosan.utils.quota.StatisticsRecorder.StatisticRecord;
import com.macrosan.utils.ratelimiter.JobScheduler;
import com.macrosan.utils.ratelimiter.LimitTimeSet;
import com.macrosan.utils.ratelimiter.RecoverLimiter;
import com.macrosan.utils.sts.StsCredentialSyncTask;
import io.netty.channel.EventLoop;
import io.netty.util.internal.StringUtil;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.quartz.SchedulerException;
import org.rocksdb.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.database.rocksdb.MSRocksDB.*;
import static com.macrosan.database.rocksdb.MossMergeOperator.SPACE_SIZE;
import static com.macrosan.database.rocksdb.batch.BatchRocksDB.toByte;
import static com.macrosan.doubleActive.HeartBeatChecker.isMultiAliveStarted;
import static com.macrosan.doubleActive.arbitration.Arbitrator.MASTER_INDEX;
import static com.macrosan.doubleActive.arbitration.Arbitrator.TERM;
import static com.macrosan.doubleActive.arbitration.DAVersionUtils.*;
import static com.macrosan.doubleActive.archive.ArchieveUtils.ARCHIVE_SYNC_REC_MARK;
import static com.macrosan.doubleActive.archive.ArchiveAnalyzer.ARCHIVE_ANALYZER_KEY;
import static com.macrosan.ec.ECUtils.bytes2long;
import static com.macrosan.ec.ECUtils.isAppendableObject;
import static com.macrosan.ec.Utils.*;
import static com.macrosan.ec.migrate.Migrate.ADD_NODE_SCHEDULER;
import static com.macrosan.ec.rebuild.RebuildCheckpointManager.CHECK_POINT_STATUS_PREFIX;
import static com.macrosan.ec.rebuild.RebuildCheckpointManager.CHECK_POINT_SUCCESS;
import static com.macrosan.ec.server.ErasureServer.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.NOT_FOUND;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.ec.server.ListServerHandler.executeListOperation;
import static com.macrosan.filesystem.FsConstants.ACLConstants.*;
import static com.macrosan.filesystem.FsConstants.S_IFLNK;
import static com.macrosan.filesystem.FsConstants.S_IFMT;
import static com.macrosan.filesystem.nfs.call.ReNameCall.RENAME_NOT_FOUND;
import static com.macrosan.filesystem.quota.FSQuotaConstants.*;
import static com.macrosan.filesystem.utils.FSQuotaUtils.*;
import static com.macrosan.fs.BlockDevice.*;
import static com.macrosan.httpserver.MossHttpClient.*;
import static com.macrosan.httpserver.RestfulVerticle.requestVersionNumMap;
import static com.macrosan.inventory.InventoryService.LOCAL_VM_UUID;
import static com.macrosan.lifecycle.LifecycleService.setEndStamp;
import static com.macrosan.message.jsonmsg.BucketInfo.*;
import static com.macrosan.message.jsonmsg.Credential.ERROR_STS_TOKEN;
import static com.macrosan.message.jsonmsg.Credential.NOT_FOUND_STS_TOKEN;
import static com.macrosan.message.jsonmsg.Inode.*;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.Type.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.snapshot.SnapshotMarkGenerator.MAX_SNAPSHOT_MARK;
import static com.macrosan.snapshot.SnapshotMarkGenerator.MIN_SNAPSHOT_MARK;
import static com.macrosan.snapshot.utils.SnapshotUtil.*;
import static com.macrosan.storage.metaserver.ObjectSplitTree.SEPARATOR_LINE_PREFIX;
import static com.macrosan.utils.quota.StatisticsRecorder.JOINER;
import static com.macrosan.utils.ratelimiter.LimitTimeSet.DEFAULT_STRATEGY;
import static javax.management.timer.Timer.ONE_DAY;

/**
 * ErasureServer中对requestResponse相关请求的处理方法
 *
 * @author fanjunxi
 */
@Log4j2
public class RequestResponseServerHandler {
    public static Mono<Payload> rename(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String vnode = msg.get("vnode");

        String oldObj = msg.get("oldObj");
        String bucket = msg.get("bucket");
        String newObj = msg.get("newObj");

        long stamp = Long.parseLong(msg.get("stamp"));
        int stampNano = Integer.parseInt(msg.get("stampNano"));
        String versionNum = msg.get("versionNum");
        MetaData deleteMeta = Json.decodeValue(msg.get("deleteMark"), MetaData.class);

        String oldKey = Utils.getVersionMetaDataKey(vnode, bucket, oldObj, "null");
        String newKey = Utils.getVersionMetaDataKey(vnode, bucket, newObj, "null");
        String oldLatestKey = Utils.getLatestMetaKey(vnode, bucket, oldObj);
        MonoProcessor<Payload> res = MonoProcessor.create();
        RequestConsumer consumer = (db, writeBatch, request) -> {
            try {
                byte[] oldMetaBytes = writeBatch.getFromBatchAndDB(db, oldKey.getBytes());
                if (oldMetaBytes != null) {
                    MetaData oldMeta = Json.decodeValue(new String(oldMetaBytes), MetaData.class);
                    if (oldMeta.deleteMark || oldMeta.deleteMarker) {
                        res.onNext(DefaultPayload.create(RENAME_NOT_FOUND, PayloadMetaType.SUCCESS.name()));
                        return;
                    }
                    String inodeKey = Inode.getKey(vnode, oldMeta.bucket, oldMeta.inode);
                    byte[] oldInodeBytes = writeBatch.getFromBatchAndDB(db, inodeKey.getBytes());
                    if (null == oldInodeBytes) {
                        res.onNext(DefaultPayload.create(RENAME_NOT_FOUND, ERROR.name()));
                        return;
                    }

                    Inode oldInode = Json.decodeValue(new String(oldInodeBytes), Inode.class);
                    if (oldInode.isDeleteMark()) {
                        res.onNext(DefaultPayload.create(RENAME_NOT_FOUND, PayloadMetaType.SUCCESS.name()));
                        return;
                    }


                    byte[] newMetaBytes = writeBatch.getFromBatchAndDB(db, newKey.getBytes());
                    if (newMetaBytes != null) {
                        MetaData newMeta = Json.decodeValue(new String(newMetaBytes), MetaData.class);
                        long oldDeltaCapacity;
                        if (!newMeta.deleteMark) {
                            if (newMeta.inode > 0) {
                                String newMetaOldInodeKey = Inode.getKey(vnode, newMeta.bucket, newMeta.inode);
                                byte[] newMetaOldInodeValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, newMetaOldInodeKey.getBytes());
                                if (null != newMetaOldInodeValue) {
                                    String inodeValueStr = new String(newMetaOldInodeValue);
                                    Inode newMetaOldInode = Json.decodeValue(inodeValueStr, Inode.class);
                                    if (newMetaOldInode.getLinkN() == 1 && newMetaOldInode.getObjName().equals(newMeta.key)) {
                                        writeBatch.delete(newMetaOldInodeKey.substring(1).getBytes());
                                        writeBatch.delete(newMetaOldInodeKey.getBytes());
                                    }
                                    oldDeltaCapacity = newMetaOldInode.isDeleteMark() || newMetaOldInode.getLinkN() > 1 ? 0 : newMetaOldInode.getSize();
                                    updateCapacityInfo(writeBatch, newMeta.getBucket(), newMeta.getKey(), vnode, -1, -oldDeltaCapacity);
//                                updateAllKeyCap(updateQuotaKeyStr, writeBatch, newMeta.getBucket(), vnode, -1, oldDeltaCapacity);
                                    long newMtimeMs = newMetaOldInode.getMtime() * 1000L + (newMetaOldInode.getMtimensec() / 1_000_000L);
                                    String newLifecycleKey = Utils.getLifeCycleMetaKey(vnode, newMetaOldInode.getBucket(), newMetaOldInode.getObjName(), newMetaOldInode.getVersionId(), String.valueOf(newMtimeMs), newMetaOldInode.getNodeId());
                                    writeBatch.delete(newLifecycleKey.getBytes());
                                    newMeta.setStamp(String.valueOf(newMtimeMs));
                                }
                                String cookieKey = getCookieKey(vnode, newMeta.bucket, newMeta.cookie);
                                writeBatch.delete(cookieKey.getBytes());
                            } else {
                                oldDeltaCapacity = newMeta.endIndex - newMeta.startIndex + 1;
                                updateCapacityInfo(writeBatch, newMeta.getBucket(), newMeta.getKey(), vnode, -1, -oldDeltaCapacity);
                            }

                        }


                        Tuple3<String, String, String> oldTuple = Utils.getAllMetaDataKey(vnode, newMeta.bucket, newMeta.key, newMeta.versionId, newMeta.stamp, newMeta.snapshotMark, newMeta.inode);
                        writeBatch.delete(oldTuple.var1.getBytes());
                        writeBatch.delete(oldTuple.var3.getBytes());
                        if (NULL.equals(newMeta.versionId)) {
                            writeBatch.delete(Utils.getMetaDataKey(vnode, newMeta.bucket, newMeta.key, newMeta.versionId, "0", newMeta.snapshotMark).getBytes());
                        }
                    }

                    String oldObjName = oldMeta.getKey();

                    oldInode.setCtime(stamp)
                            .setCtimensec(stampNano);
                    oldInode.setObjName(newObj);
                    oldInode.setVersionNum(versionNum);
                    oldInode.setCifsMode(CifsUtils.changeToHiddenCifsMode(newObj, oldInode.getCifsMode(), true));

                    oldMeta.setKey(newObj);
                    oldMeta.setVersionNum(versionNum);

                    //软连接重命名时，不修改引用名称
                    if ((oldInode.getMode() & S_IFMT) != S_IFLNK) {
                        oldMeta.setReferencedKey(newObj);
                        oldInode.setReference(newObj);
                    }
                    String oldLifecycleStamp = String.valueOf(oldInode.getMtime() * 1000L + oldInode.getMtimensec() / 1_000_000L);
                    if (oldMeta.inode > 0) {
                        oldMeta.tmpInodeStr = null;
                        String oldLifecycleKey = Utils.getLifeCycleMetaKey(vnode, oldMeta.bucket, oldObjName, oldMeta.versionId, oldLifecycleStamp, oldMeta.inode);
                        writeBatch.delete(oldLifecycleKey.getBytes());
                    }

                    writeBatch.put(inodeKey.getBytes(), Json.encode(oldInode).getBytes());

                    byte[] simplifyValue = Utils.simplifyMetaJson(oldMeta);

                    Tuple3<String, String, String> tuple = Utils.getAllMetaDataKey(vnode, oldMeta.bucket, newObj, oldMeta.versionId, oldMeta.stamp, oldMeta.inode);
                    tuple.var1 = Utils.getMetaDataKey(vnode, oldMeta.bucket, newObj, oldMeta.versionId, "0");
                    String latestKey = Utils.getLatestMetaKey(vnode, oldMeta.bucket, newObj);

                    writeBatch.put(latestKey.getBytes(), simplifyValue);
                    writeBatch.put(tuple.var1.getBytes(), simplifyValue);
                    writeBatch.put(tuple.var2.getBytes(), Json.encode(oldMeta).getBytes());
                    String newLifecycleKey = Utils.getLifeCycleMetaKey(vnode, oldMeta.bucket, newObj, oldMeta.versionId, oldLifecycleStamp, oldMeta.inode);
                    writeBatch.put(newLifecycleKey.getBytes(), new byte[]{0});

                    // cookieKey更新文件名
                    String cookieKey = Inode.getCookieKey(vnode, oldMeta.getBucket(), oldMeta.getCookie());
                    byte[] cookieBytes = writeBatch.getFromBatchAndDB(db, cookieKey.getBytes());
                    Inode cookieInode = null;
                    if (cookieBytes != null) {
                        cookieInode = Json.decodeValue(new String(writeBatch.getFromBatchAndDB(db, cookieKey.getBytes())), Inode.class);
                    } else {
                        cookieInode = Inode.defaultCookieInode(oldInode).setCookie(oldMeta.getCookie());
                    }
                    cookieInode.setObjName(newObj);
                    cookieInode.setVersionNum(versionNum);
                    writeBatch.put(cookieKey.getBytes(), Json.encode(cookieInode).getBytes());

                    tuple = Utils.getAllMetaDataKey(vnode, oldMeta.bucket, oldObjName, oldMeta.versionId, oldMeta.stamp, oldMeta.inode);
                    tuple.var1 = Utils.getMetaDataKey(vnode, oldMeta.bucket, oldObjName, oldMeta.versionId, "0");
                    latestKey = Utils.getLatestMetaKey(vnode, oldMeta.bucket, oldObjName);

                    deleteMeta.setStamp(oldMeta.stamp);
                    String deleteMark = Json.encode(deleteMeta);
                    writeBatch.delete(latestKey.getBytes());
                    writeBatch.put(tuple.var1.getBytes(), deleteMark.getBytes());
                    writeBatch.put(tuple.var2.getBytes(), deleteMark.getBytes());
                    writeBatch.put(tuple.var3.getBytes(), deleteMark.getBytes());
                    FSQuotaUtils.quotaConfigRename(bucket, oldMeta.inode, newObj);
                    res.onNext(SUCCESS_PAYLOAD);
                } else {
                    res.onNext(DefaultPayload.create(RENAME_NOT_FOUND, ERROR.name()));
                }
            } catch (Exception e) {
                log.error("rename error..oldKey:{}", oldKey, e);
                res.onNext(SUCCESS_PAYLOAD);
            }
        };
        // 由锁 old LatestKey 改为 new LatestKey
        String newLatestKey = getLatestMetaKey(vnode, bucket, newObj);
        return BatchRocksDB.customizeOperate(lun, newLatestKey.hashCode(), consumer, res)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> lookup(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String vnode = Utils.getVnode(key);

        boolean isReName = "1".equalsIgnoreCase(msg.get("isReName"));
        boolean noCaseSensitive = "0".equalsIgnoreCase(msg.get("caseSensitive"));
        AtomicBoolean caseRetry = new AtomicBoolean(false);

        return FsServerHandler.lookup0(lun, key, vnode, isReName, caseRetry)
                .flatMap(resPayload -> {
                    //需要处理大小写不匹配
                    if (noCaseSensitive && caseRetry.get()) {
                        long dirInode = Long.parseLong(msg.get("dirInode"));
                        //lookup dir
                        if (dirInode == -1) {
                            return FsServerHandler.smbLookupDir(lun, vnode, key, isReName);
                        } else {
                            return FsServerHandler.smbLookupFile(lun, vnode, key, isReName);
                        }

                    } else {
                        return Mono.just(resPayload);
                    }
                });
    }

    public static Mono<Payload> getInode(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        boolean mergeMeta = "1".equals(msg.dataMap.getOrDefault(MERGE_META, "0"));

        try {
            MSRocksDB db = MSRocksDB.getRocksDB(lun);
            if (db == null) {
                return Mono.just(DefaultPayload.create(Json.encode(ERROR_INODE), ERROR.name()));
            }
            byte[] value = db.get(key.getBytes());

            if (null == value) {
                return Mono.just(DefaultPayload.create(Json.encode(NOT_FOUND_INODE), NOT_FOUND.name()));
            } else {
                Inode inode = Json.decodeValue(new String(value), Inode.class);
                if (mergeMeta && inode.getNodeId() > 1 && !inode.isDeleteMark() && inode.getObjAcl() == null) {
                    String vnode = Utils.getVnode(key);
                    String versionKey = Utils.getVersionMetaDataKey(vnode, inode.getBucket(), inode.getObjName(), inode.getVersionId(), null);
                    byte[] metaBytes = db.get(versionKey.getBytes());
                    if (metaBytes != null) {
                        MetaData metaData = Json.decodeValue(new String(metaBytes), MetaData.class);
                        Inode.mergeObjAcl(inode, metaData);
                        value = Json.encode(inode).getBytes();
                    }
                }

                return Mono.just(DefaultPayload.create(value, SUCCESS.name().getBytes()));
            }

        } catch (Exception e) {
            log.error("", e);
            return Mono.just(DefaultPayload.create(Json.encode(ERROR_INODE), ERROR.name()));
        }
    }

    public static Mono<Payload> putInode(Payload payload) {
        boolean local = payload instanceof LocalPayload;
        SocketReqMsg msg = local ? ((LocalPayload<SocketReqMsg>) payload).data : SocketReqMsg.toSocketReqMsg(payload.data());
        String lun = msg.get("lun");
        String inodeKey = msg.get("key");
        String inodeValue = msg.get("value");
        boolean isRecover = "1".equals(msg.get("isRecover"));
        boolean[] repairCookieAndInode = {false};
        if (null != msg.get("repairCookieAndInode")) {
            repairCookieAndInode[0] = "1".equals(msg.get("repairCookieAndInode"));
        }
        Inode inode = Json.decodeValue(inodeValue, Inode.class);

        RequestConsumer consumer = (db, writeBatch, request) -> {

            // 更新桶容量: setDelMark中减少容量，putInode中增加容量
            // (1) 文件端上传文件，putObjectMeta单次，putInode多次，因此在putInode时更新桶容量
            // (2) 文件端创建硬链接，无putObjectMeta，会putInode更新linkN，此时会先获取旧的size，因为硬链接的size和旧的size是一样的，所以不会增加容量
            // (3) 文件端删除文件，先setDelMark，再删除数据块
            // (4) 文件端删除硬链接，先setDelMark，再putInode
            // (5) moss端删除文件，用setDelMark，通过endIndex和startIndex更新容量，此时若是对象，则endIndex和startIndex有值；此时若是文件，则读取inodeSize更新
            // (6) moss端删除硬链接，先setDelMark删除所有元数据包括Inode记录，再putInode创建回link-1的inode记录
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, inodeKey.getBytes());
            long oldSize = 0L;
            Inode oldInode = null;
            boolean updateCreateTime = false;
            if (null != oldValue) {
                oldInode = Json.decodeValue(new String(oldValue), Inode.class);
                oldSize = oldInode.getSize();
                // 默认inode createTime=0 时 cookieInode不存在，deleteMark除外；若 oldInode createTime=0，即当前元数据为旧版本创建，应增添createTime和cookieInode
                if (repairCookieAndInode[0] && !oldInode.isDeleteMark() && oldInode.getCreateTime() == 0 && inode.getCreateTime() > 0) {
                    updateCreateTime = true;
                    String cookieKey = Inode.getCookieKey(getVnode(inodeKey), inode.getBucket(), inode.getCookie());
                    Inode cookieInode = Inode.defaultCookieInode(inode);
                    writeBatch.put(cookieKey.getBytes(), Json.encode(cookieInode).getBytes());
                }
            }
            //1)写入inode元数据时，若oldInode==null 或者是删除标记则不写入
            //2)oldInode的版本号大于等于新inode的版本号也不写入
            if (oldInode != null && (
                    (!isRecover && oldInode.isDeleteMark())
                            || (inode.getVersionNum().compareTo(oldInode.getVersionNum()) <= 0)
            )) {
                // 如果是情况2)，此时需判断是否更新createTime；更新createTime时versionNum不更新，不会继续往后走
                if (updateCreateTime && (oldInode != null && inode.getVersionNum().compareTo(oldInode.getVersionNum()) <= 0)) {
                    oldInode.setCreateTime(inode.getCreateTime());
                    writeBatch.put(inodeKey.getBytes(), Json.encode(oldInode).getBytes());
                }

                return;
            }
            String updateS3InodeStr = inode.getXAttrMap().get(QUOTA_KEY_CREATE_S3_INODE);
            if (oldInode == null
                    && StringUtils.isNotBlank(inode.getXAttrMap().get(CREATE_S3_INODE_TIME))
                    && inode.getUid() > 0
                    && StringUtils.isBlank(updateS3InodeStr)
            ) {
                Tuple2<Boolean, List<String>> quotaInfoByInode = getQuotaInfoByInode(inode);
                if (quotaInfoByInode.var1) {
                    updateS3InodeStr = Json.encode(quotaInfoByInode.var2);
                }
            }

            long capacity = inode.getSize() - oldSize;
            //updateMeta中会对capacity进行更新
            if ((inode.getMode() & S_IFMT) == S_IFLNK) {
                capacity = 0;
            }
            updateCapacityInfo(writeBatch, inode.getBucket(), inode.getObjName(), getVnode(inodeKey), 0, capacity);
            String updateKeyStr = inode.getXAttrMap().get(QUOTA_KEY);
            if (StringUtils.isBlank(updateKeyStr)) {
                updateKeyStr = FSQuotaUtils.getQuotaKeys(inode.getBucket(), inode.getObjName(), System.currentTimeMillis(), inode.getUid(), inode.getGid());
            }
            FSQuotaUtils.updateAllKeyCap(updateKeyStr, writeBatch, inode.getBucket(), getVnode(inodeKey), 0, capacity);
            //对于文件更新了uid和gid，相应的配额需要更新容量和文件数信息
            if (oldInode != null && (oldInode.getUid() != inode.getUid() || oldInode.getGid() != inode.getGid())) {
                if (StringUtils.isBlank(updateKeyStr)) {
                    List<Integer> uidList = new ArrayList<>();
                    List<Integer> gidList = new ArrayList<>();
                    if (oldInode.getUid() != inode.getUid()) {
                        uidList.add(inode.getUid());
                        uidList.add(oldInode.getUid());
                    }
                    if (oldInode.getGid() != inode.getGid()) {
                        gidList.add(inode.getGid());
                        gidList.add(oldInode.getGid());
                    }
                    Tuple2<Boolean, List<String>> tuple2 = FSQuotaUtils.existQuotaInfoListUserOrGroup(inode.getBucket(), inode.getObjName(), System.currentTimeMillis(), uidList, gidList).block();
                    if (tuple2.var1) {
                        updateKeyStr = Json.encode(tuple2.var2);
                    }
                }
                FSQuotaUtils.updateCapAfterSetAttr(updateKeyStr, writeBatch, inode.getBucket(), getVnode(inodeKey), 1, inode.getSize(), oldInode, inode);
            }
            if (StringUtils.isNotBlank(updateS3InodeStr)) {
                FSQuotaUtils.updateAllKeyOnCreateS3Inode(updateS3InodeStr, writeBatch, inode.getBucket(), getVnode(inodeKey), 1, inode.getSize());
            }
            inode.getXAttrMap().remove(QUOTA_KEY_CREATE_S3_INODE);
            inode.getXAttrMap().remove(QUOTA_KEY);
            byte[] inodeBytes;
            if (inode.getChunkFileMap() != null) {
                Map<String, ChunkFile> updateChunk = inode.getChunkFileMap();
                for (String chunkName : updateChunk.keySet()) {
                    ChunkFile chunkFile = updateChunk.get(chunkName);
                    writeBatch.put(chunkName.getBytes(), Json.encode(chunkFile).getBytes());
                }

                inode.setChunkFileMap(null);
                inodeBytes = Json.encode(inode).getBytes();
            } else {
                inodeBytes = inodeValue.getBytes();
            }
            if (updateInodeSetQuotaDirSize(updateKeyStr, inode, oldInode, oldSize)) {
                inodeBytes = Json.encode(inode).getBytes();
            }
            writeBatch.put(inodeKey.getBytes(), inodeBytes);

            // 当 inode 的 MTime（修改时间）发生变化时，更新生命周期键
            if (oldInode != null) {
                String vnode = getVnode(inodeKey);
                long oldMtimeMs = oldInode.getMtime() * 1000L + (oldInode.getMtimensec() / 1_000_000L);
                long newMtimeMs = inode.getMtime() * 1000L + (inode.getMtimensec() / 1_000_000L);

                String newLifecycleKey = Utils.getLifeCycleMetaKey(vnode, inode.getBucket(), inode.getObjName(), inode.getVersionId(), String.valueOf(newMtimeMs), inode.getNodeId());
                String oldLifecycleKey = Utils.getLifeCycleMetaKey(vnode, oldInode.getBucket(), oldInode.getObjName(), oldInode.getVersionId(), String.valueOf(oldMtimeMs), oldInode.getNodeId());

                if (!newLifecycleKey.equals(oldLifecycleKey) && inode.getVersionId().equals(oldInode.getVersionId())) {
                    // 立即删除旧生命周期键
                    writeBatch.delete(oldLifecycleKey.getBytes());
                    // 添加新生命周期键
                    writeBatch.put(newLifecycleKey.getBytes(), new byte[]{0});
                }
            }
        };

        return BatchRocksDB.customizeOperateMeta(lun, Long.hashCode(inode.getNodeId()), consumer)
                .map(str -> SUCCESS_PAYLOAD)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> putChunk(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String chunkKey = msg.get("key");
        String chunkValue = msg.get("value");
        ChunkFile chunkFile = Json.decodeValue(chunkValue, ChunkFile.class);

        RequestConsumer consumer = (db, writeBatch, request) -> {
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, chunkKey.getBytes());

            if (null != oldValue) {
                ChunkFile oldChunk = Json.decodeValue(new String(oldValue), ChunkFile.class);
                if (oldChunk.versionNum.compareTo(chunkFile.versionNum) <= 0) {
                    writeBatch.put(chunkKey.getBytes(), chunkValue.getBytes());
                }
            } else {
                writeBatch.put(chunkKey.getBytes(), chunkValue.getBytes());
            }
        };

        return BatchRocksDB.customizeOperateMeta(lun, Long.hashCode(chunkFile.getNodeId()), consumer)
                .map(str -> SUCCESS_PAYLOAD)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> delChunk(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        Tuple3<Long, String, String> key = ChunkFile.getChunkFromFileName(msg.get("key"));
        String chunkKey = key.var3;
        String bucket = msg.get("value");
        RequestConsumer consumer = (db, writeBatch, request) -> {
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, chunkKey.getBytes());
            if (oldValue != null) {
                writeBatch.delete(chunkKey.getBytes());
            }
        };
        return BatchRocksDB.customizeOperateMeta(lun, Long.hashCode(key.var1), consumer)
                .map(str -> SUCCESS_PAYLOAD)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> delInode(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String inodeKey = msg.get("key");
        String vnode = msg.get("vnode");
        String metaData = msg.get("deleteMark");
        Inode inode = Json.decodeValue(msg.get("value"), Inode.class);
        RequestConsumer consumer = (db, writeBatch, request) -> {
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, inodeKey.getBytes());
            if (oldValue != null) {
                writeBatch.transferAndPut(inodeKey.getBytes(), metaData.getBytes());
                long inodeSize = inode.getSize();
                if ((inode.getMode() & S_IFMT) == S_IFLNK) {
                    inodeSize = 0;
                }
                updateCapacityInfo(writeBatch, inode.getBucket(), inode.getObjName(), vnode, 0, -inodeSize);
            }
        };
        return BatchRocksDB.customizeOperateMeta(lun, Long.hashCode(inode.getNodeId()), consumer)
                .map(str -> SUCCESS_PAYLOAD)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> findCookie(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");

        try {
            MSRocksDB db = MSRocksDB.getRocksDB(lun);
            if (db == null) {
                return Mono.just(DefaultPayload.create(Json.encode(ERROR_INODE), ERROR.name()));
            }
            byte[] value = db.get(key.getBytes());

            if (null == value) {
                return Mono.just(DefaultPayload.create(Json.encode(NOT_FOUND_INODE), NOT_FOUND.name()));
            } else {
                return Mono.just(DefaultPayload.create(value, SUCCESS.name().getBytes()));
            }

        } catch (Exception e) {
            log.error("", e);
            return Mono.just(DefaultPayload.create(Json.encode(ERROR_INODE), ERROR.name()));
        }
    }

    public static Mono<Payload> putCookie(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String cookieKey = msg.get("key");
        String cookieValue = msg.get("value");
        Inode cookieInode = Json.decodeValue(cookieValue, Inode.class);

        RequestConsumer consumer = (db, writeBatch, request) -> {
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, cookieKey.getBytes());
            if (null != oldValue) {
                Inode oldCookieInode = Json.decodeValue(new String(oldValue), Inode.class);
                if (cookieInode.getVersionNum().compareTo(oldCookieInode.getVersionNum()) > 0) {
                    writeBatch.put(cookieKey.getBytes(), cookieValue.getBytes());
                }
            } else {
                writeBatch.put(cookieKey.getBytes(), cookieValue.getBytes());
            }
        };

        return BatchRocksDB.customizeOperateMeta(lun, Long.hashCode(cookieInode.getNodeId()), consumer)
                .map(str -> SUCCESS_PAYLOAD)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> heartFail(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        long nodeId = Long.parseLong(msg.get("nodeId"));
        Vnode vnode = Node.getInstance().getInodeV(nodeId);
        vnode.heartFail0();
        return Mono.just(DefaultPayload.create("", SUCCESS.name()));
    }

    /**
     * 删除对象前，检查objectACL，给object的metadata中写入删除标记。
     */
    public static Mono<Payload> markDeleteObject(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        MonoProcessor<Payload> res = MonoProcessor.create();
        String lun = msg.get("lun");
        String key = msg.get("key");
        String versionStatus = msg.get("status");
        String snapshotLink = msg.get("snapshotLink");
        String currentSnapshotMark = msg.get("currentSnapshotMark");
        String vnode = key.replace(ROCKS_VERSION_PREFIX, "").split(File.separator)[0];
        MetaData meta = Json.decodeValue(msg.get("value"), MetaData.class);

        String latestKey = Utils.getLatestMetaKey(vnode, meta.bucket, meta.key, meta.snapshotMark);
        boolean versionEnabled = "Enabled".equals(versionStatus) || "Suspended".equals(versionStatus);
        boolean isMigrate = "1".equals(msg.get("migrate"));
        boolean needDeleteInode = "1".equals(msg.get("needDeleteInode"));
        String updateQuotaKeyStr = msg.get("updateQuotaKeyStr");
        AtomicReference<String> overwriteFiles = new AtomicReference<>("");
        RequestConsumer consumer = (db, writeBatch, request) -> {
            //根据latestKey值获取rocksDB中保存data值
            byte[] latestValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, latestKey.getBytes());
            MetaData latestMeta = null;
            byte[] metaBytes = latestValue;
            boolean isTempLatestMeta = false;
            if (latestValue != null) {
                //data值不为空
                latestMeta = Json.decodeValue(new String(latestValue), MetaData.class);
                if (!latestMeta.versionId.equals(meta.versionId)) {
                    if (versionEnabled && latestMeta.isUnView(currentSnapshotMark) && !meta.deleteMark) {
                        // 桶快照逻辑删除，判断是否存在临时latestKey
                        String tempLatestKey = replaceSnapshotMark(latestKey, currentSnapshotMark);
                        byte[] temptLatestValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, tempLatestKey.getBytes());
                        if (temptLatestValue != null) {
                            MetaData metaData = Json.decodeValue(new String(temptLatestValue), MetaData.class);
                            isTempLatestMeta = metaData.snapshotMark.equals(meta.snapshotMark);
                            if (isTempLatestMeta) {
                                latestValue = temptLatestValue;
                                latestMeta = metaData;
                                metaBytes = temptLatestValue;
                                if (!latestMeta.versionId.equals(meta.versionId)) {
                                    metaBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, key.getBytes());
                                }
                            } else {
                                metaBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, key.getBytes());
                            }
                        } else {
                            metaBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, key.getBytes());
                        }
                    } else {
                        //根据key值来获取rocksDB中的meta数据
                        metaBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, key.getBytes());
                    }
                }
            } else if (!meta.deleteMark) {
                // 桶快照逻辑删除，判断是否存在tempLatestKey
                String tempLatestKey = replaceSnapshotMark(latestKey, currentSnapshotMark);
                byte[] temptLatestValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, tempLatestKey.getBytes());
                if (temptLatestValue != null) {
                    MetaData metaData = Json.decodeValue(new String(temptLatestValue), MetaData.class);
                    isTempLatestMeta = metaData.snapshotMark.equals(meta.snapshotMark);
                    if (isTempLatestMeta) {
                        latestValue = temptLatestValue;
                        latestMeta = metaData;
                        metaBytes = temptLatestValue;
                        if (!latestMeta.versionId.equals(meta.versionId)) {
                            metaBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, key.getBytes());
                        }
                    }
                } else {
                    metaBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, key.getBytes());
                }
            } else {
                metaBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, key.getBytes());
            }

            if (metaBytes != null) {
                MetaData objMeta = Json.decodeValue(new String(metaBytes), MetaData.class);
                String storedVersionNum = objMeta.versionNum;
                // 待写入的版本号小于当前版本号，直接视为已成功写入删除标记
                // 说明元数据在未删除前已经发生改变(如重新上传同名对象)，无需再删除
                if (storedVersionNum.compareTo(meta.versionNum) >= 0) {
                    if (storedVersionNum.compareTo(meta.versionNum) > 0) {
                        boolean overwrite = false;
                        MetaData oldMeta = objMeta;
                        if (oldMeta.partUploadId == null && oldMeta.fileName == null) {
                            // 两者都为null，则说明objMeta是latestkey的value，latestkey的value中不包含这两个值，因此重新get一次
                            byte[] bytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, key.getBytes());
                            if (bytes != null) {
                                oldMeta = Json.decodeValue(new String(bytes), MetaData.class);
                            }
                        }
                        // 当前对象的元数据若不为删除标记并且数据块与删除标记需要删除的数据块一致的话，则不返成功，返回overwrite
                        if (meta.partUploadId != null) {
                            overwrite = meta.partUploadId.equals(oldMeta.partUploadId) && !oldMeta.deleteMark;
                        } else if (meta.fileName != null) {
                            overwrite = meta.fileName.equals(oldMeta.fileName) && !oldMeta.deleteMark;
                        }
                        if (overwrite) {
                            res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                        } else {
                            res.onNext(SUCCESS_PAYLOAD);
                        }
                    } else {
                        res.onNext(SUCCESS_PAYLOAD);
                    }
                    return;
                } else {
                    MetaData oldMeta = objMeta;
                    if (oldMeta.partUploadId == null && oldMeta.fileName == null) {
                        // 两者都为null，则说明objMeta是latestkey的value，latestkey的value中不包含这两个值，因此重新get一次
                        byte[] bytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, key.getBytes());
                        if (bytes != null) {
                            oldMeta = Json.decodeValue(new String(bytes), MetaData.class);
                        }
                    }
                    if (StringUtils.isNotEmpty(oldMeta.fileName) && StringUtils.isNotEmpty(meta.fileName) && (!oldMeta.fileName.equals(meta.fileName) || !oldMeta.storage.equals(meta.storage))) {
                        // todo 设置删除标记时，如果发现对象的数据块或者其位置发生变化，可能会导致漏删，需要将该数据块删除。比如缓存池下刷、生命周期分层、小文件聚合，生命周期迁移等。此种情况暂不将对象元数据标记为删除标记
                        if (StringUtils.isNotEmpty(oldMeta.aggregationKey)) {
                            overwriteFiles.set(oldMeta.storage + "=" + oldMeta.aggregationKey);
                        } else {
                            overwriteFiles.set(oldMeta.storage + "=" + oldMeta.fileName + "=" + (StringUtils.isNotEmpty(oldMeta.lastAccessStamp) ? oldMeta.lastAccessStamp : ""));
                        }
                    }
                }
                Tuple3<String, String, String> tuple = Utils.getAllMetaDataKey(vnode, objMeta.bucket, objMeta.key, objMeta.versionId, meta.stamp, objMeta.snapshotMark, objMeta.inode);
                String oldStampKey = tuple.var1;
                if (!versionEnabled) {
                    tuple.var1 = Utils.getMetaDataKey(vnode, objMeta.bucket, objMeta.key, objMeta.versionId, "0", objMeta.snapshotMark);
                }
                boolean hasSameNameObject = false;
                boolean deleteLatest = false;
                if (latestValue != null) {
                    //判断数据池中的版本与传来的 版本是否一致
                    if (latestMeta.versionId.equals(objMeta.versionId) && meta.deleteMark) {
                        writeBatch.delete(latestKey.getBytes());
                        deleteLatest = true;
                        try (Slice lowerSlice = new Slice(latestKey.substring(1).getBytes());
                             Slice upperSlice = new Slice((tuple.var1 + Utils.ONE_STR).getBytes());
                             ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperSlice).setIterateLowerBound(lowerSlice);
                             RocksIterator iterator = db.newIterator(readOptions);
                             RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)) {

                            keyIterator.seek(tuple.var1.getBytes());
                            keyIterator.prev();
                            if (keyIterator.isValid() && new String(keyIterator.key()).startsWith(latestKey.substring(1))) {
                                MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                                boolean insert = !meta.versionId.equals(metaData.versionId);
                                if (insert && metaData.key.equals(objMeta.key) && metaData.bucket.equals(objMeta.bucket) && !metaData.deleteMarker && !metaData.deleteMark) {
                                    writeBatch.put(true, latestKey.getBytes(), keyIterator.value());
                                }
                                if (snapshotLink != null && metaData.key.equals(objMeta.key) && metaData.bucket.equals(objMeta.bucket) && metaData.snapshotMark.equals(objMeta.snapshotMark) && !metaData.deleteMark) {
                                    hasSameNameObject = true;
                                }
                            }
                        }
                    } else if (latestMeta.versionId.equals(objMeta.versionId) && versionEnabled) {
                        // 桶快照逻辑删除处理--- 为快照前上传的对象 put一个临时的latestKey
                        putTempLatestKey(db, writeBatch, latestKey, currentSnapshotMark, vnode, meta, tuple, objMeta);
                    }
                } else {
                    if (meta.deleteMark) {
                        //最新版本为deleteMarker
                        try (Slice lowerSlice = new Slice(latestKey.substring(1).getBytes());
                             Slice upperSlice = new Slice((latestKey.substring(1) + Utils.ONE_STR).getBytes());
                             ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperSlice).setIterateLowerBound(lowerSlice);
                             RocksIterator iterator = db.newIterator(readOptions);
                             RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)) {

                            keyIterator.seek((tuple.var1).getBytes());
                            keyIterator.next();
                            if (!keyIterator.isValid() || !new String(keyIterator.key()).startsWith(latestKey.substring(1))) {
                                keyIterator.seek((tuple.var1).getBytes());
                                keyIterator.prev();
                                if (keyIterator.isValid() && new String(keyIterator.key()).startsWith(latestKey.substring(1))) {
                                    MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                                    boolean b = meta.versionId.equals(metaData.versionId);
                                    if (!b && metaData.key.equals(objMeta.key) && metaData.bucket.equals(objMeta.bucket) && !metaData.deleteMarker && !metaData.deleteMark) {
                                        writeBatch.put(true, latestKey.getBytes(), keyIterator.value());
                                    }
                                    if (snapshotLink != null && metaData.key.equals(objMeta.key) && metaData.bucket.equals(objMeta.bucket) && metaData.snapshotMark.equals(objMeta.snapshotMark) && !metaData.deleteMark) {
                                        hasSameNameObject = true;
                                    }
                                }
                                deleteLatest = true;
                            }
                        }
                    } else if (meta.deleteMarker) {
                        // 删除快照前的快照标记，则put一个临时的latestKey
                        putTempLatestKey(db, writeBatch, latestKey, currentSnapshotMark, vnode, meta, tuple, objMeta);
                    }
                }

                if (objMeta.inode == 0) {
                    meta.setStamp(objMeta.stamp);
                }

                if (objMeta.inode > 0 && needDeleteInode) {
                    meta.inode = objMeta.inode;
                    meta.cookie = objMeta.cookie;
                }

                if (meta.deleteMark) {
                    if (objMeta.inode > 0 && !needDeleteInode) {
                        meta.inode = objMeta.inode;
                    }
                    writeBatch.put(tuple.var1.getBytes(), Json.encode(meta).getBytes());
                    writeBatch.put(tuple.var2.getBytes(), Json.encode(meta).getBytes());
                    writeBatch.put(tuple.var3.getBytes(), Json.encode(meta).getBytes());
                    // 如果是旧版本（3.0.6P03之前）的元数据，还需删除多余的、未适配新格式的文件生命周期键
                    if (objMeta.inode > 0 && !VersionUtil.hasVersionMagicPrefix(metaBytes)) {
                        // 构造旧版本的生命周期元数据 key
                        String legacyCtimeLifecycleKey = Utils.getLifeCycleMetaKey(vnode, objMeta.bucket, objMeta.key, objMeta.versionId, objMeta.stamp, objMeta.snapshotMark);
                        writeBatch.put(legacyCtimeLifecycleKey.getBytes(), Json.encode(meta).getBytes());
                    }
                } else {
                    // 桶快照逻辑删除
                    writeBatch.put(true, tuple.var1.getBytes(), Json.encode(meta).getBytes());
                    writeBatch.put(tuple.var2.getBytes(), Json.encode(meta).getBytes());
                    if (latestMeta != null && latestMeta.isViewable(currentSnapshotMark) && !isTempLatestMeta) {
                        writeBatch.put(true, latestKey.getBytes(), Json.encode(meta).getBytes());
                    }
                }

                // 桶快照处理
                if (snapshotLink != null && deleteLatest && !hasSameNameObject && meta.deleteMark && versionEnabled) {
                    cancelWeakUnView(db, writeBatch, snapshotLink, vnode, meta);
                }
                Inode inode = new Inode();
                long inodeSize = 0L;
                if (objMeta.inode > 0) {
                    String inodeKey = Inode.getKey(vnode, objMeta.bucket, objMeta.inode);
                    byte[] inodeValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, inodeKey.getBytes());
                    if (null != inodeValue) {
                        inode = Json.decodeValue(new String(inodeValue), Inode.class);
                        if (needDeleteInode) {//删除硬链接时，不进行容量更新，也不删除inode元数据
                            inodeSize = inode.getSize();
                            MetaData inodeMeta = meta.clone();
                            inodeMeta.inode = objMeta.inode;
                            writeBatch.transferAndPut(inodeKey.getBytes(), Json.encode(inodeMeta).getBytes());
                            Inode markDeleteInode = INODE_DELETE_MARK.clone().setVersionNum(meta.getVersionNum())
                                    .setNodeId(objMeta.inode)
                                    .setBucket(meta.bucket)
                                    .setUid(inode.getUid())
                                    .setGid(inode.getGid())
                                    .setCookie(objMeta.cookie)
                                    .setVersionId(meta.versionId == null ? "null" : meta.versionId)
                                    .setMtime(inode.getMtime())
                                    .setMtimensec(inode.getMtimensec())
                                    .setObjName(meta.key);
                            writeBatch.put(inodeKey.getBytes(), Json.encode(markDeleteInode).getBytes());
                        }

                        if ((inode.getMode() & S_IFMT) == S_IFLNK || inode.isDeleteMark()) {
                            inodeSize = 0;
                        }
                    }

                    // cookie表置成deleteMark; cookie表跟随metaData删而不跟随inode删;只要objMeta中有nodeId就说明应将cookie置成删除标记
                    Inode cookieDelMark = INODE_DELETE_MARK.clone()
                            .setVersionNum(meta.getVersionNum())
                            .setNodeId(objMeta.inode)
                            .setBucket(meta.bucket)
                            .setObjName(meta.key)
                            .setCookie(meta.cookie);
                    String cookieKey = Inode.getCookieKey(vnode, objMeta.bucket, objMeta.cookie);
                    writeBatch.put(cookieKey.getBytes(), Json.encode(cookieDelMark).getBytes());
                }
                // 处理升级
                if (!oldStampKey.equals(tuple.var1) && meta.deleteMark) {
                    writeBatch.put(oldStampKey.getBytes(), Json.encode(meta).getBytes());
                }

                // 在多版本状态下(Suspended Enabled) 操作过去版本号为null的对象
                if (NULL.equals(objMeta.versionId) && versionEnabled || versionEnabled && isMultiAliveStarted) {
                    writeBatch.put(Utils.getMetaDataKey(vnode, objMeta.bucket, objMeta.key, objMeta.versionId, "0", objMeta.snapshotMark).getBytes(), Json.encode(meta).getBytes());
                }

                long num = objMeta.deleteMarker || objMeta.deleteMark || !meta.deleteMark ? 0 : -1;
                long deltaAvailableCapacity = 0L;
                if (objMeta.inode > 0) {
                    // 创建硬链接本身会使容量加一，因此减去时多减一
                    deltaAvailableCapacity = objMeta.deleteMarker || objMeta.deleteMark ? 0 : -(inodeSize);
                } else {
                    deltaAvailableCapacity = objMeta.deleteMarker || objMeta.deleteMark || !meta.deleteMark ? 0 : -Utils.getObjectSize(objMeta);
                }

                updateCapacityInfo(writeBatch, objMeta.getBucket(), objMeta.getKey(), vnode, num, deltaAvailableCapacity);
                updateAllKeyCap(updateQuotaKeyStr, writeBatch, objMeta.getBucket(), vnode, num, deltaAvailableCapacity);
            } else if (isMigrate) {
                String metaKey = Utils.getMetaDataKey(vnode, meta.bucket, meta.key, meta.versionId, !versionEnabled ? "0" : meta.stamp, meta.snapshotMark);
                writeBatch.put(metaKey.getBytes(), Json.encode(meta).getBytes());
                writeBatch.put(key.getBytes(), Json.encode(meta).getBytes());
            } else if (meta.isDiscard()) {
                String metaKey = Utils.getMetaDataKey(vnode, meta.bucket, meta.key, meta.versionId, !versionEnabled ? "0" : meta.stamp, meta.snapshotMark);
                writeBatch.put(key.getBytes(), Json.encode(meta).getBytes());
                writeBatch.put(metaKey.getBytes(), Json.encode(meta).getBytes());
            }
            res.onNext(DefaultPayload.create(overwriteFiles.get(), SUCCESS.name()));
        };
        int hashCode = getFinalHashCode(latestKey, meta.snapshotMark != null);
        return BatchRocksDB.customizeOperate(lun, hashCode, consumer, res)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> deleteMeta(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String status = msg.get("status");
        String versionNum = msg.get("value");
        String snapshotMark = msg.get("snapshotMark");
        long nodeId = 0L;
        if (null != msg.get("nodeId")) {
            nodeId = Long.parseLong(msg.get("nodeId"));
        }
        long fileCookie = StringUtils.isBlank(msg.get("fileCookie")) ? 0L : Long.parseLong(msg.get("fileCookie"));
        String bucket = msg.get("bucket");
        boolean versionEnabled = "Enabled".equals(status) || "Suspended".equals(status);
        String uploadIdOrFileName = msg.get("uploadIdOrFileName");

        long finalNodeId = nodeId;
        String finalKey = key;
        RequestConsumer consumer = (db, writeBatch, request) -> {
            byte[] metaBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, finalKey.getBytes());
            if (metaBytes == null) {
                // 仅当meta不存在且nodeId>0时，查询到的inode deleteMark不为空且oldVersionNum相比versionNum<=0，删除已无用的inode deleteMark
                if (finalNodeId > 0) {
                    deleteInodeAndCookieDeleteMark(db, writeBatch, bucket, finalNodeId, finalKey, versionNum, fileCookie);
                }
                return;
            }
            MetaData objMeta = Json.decodeValue(new String(metaBytes), MetaData.class);
            if (finalNodeId > 0 && objMeta.inode != finalNodeId) {
                // 传入的nodeId与元数据中的inode不一致,说明metadata删除标记已被覆盖，需要直接删除inode 元数据和 cookie元数据
                deleteInodeAndCookieDeleteMark(db, writeBatch, bucket, finalNodeId, finalKey, versionNum, fileCookie);
                return;
            }

            // 文件的versionNum采用inode中记录的versionNum
            String objStamp = objMeta.stamp;
            Inode inode = null;
            if (objMeta.inode > 0) {
                String vnode = Utils.getVnode(finalKey);
                String inodeKey = Inode.getKey(vnode, objMeta.bucket, objMeta.inode);
                byte[] inodeBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, inodeKey.getBytes());
                if (null != inodeBytes) {
                    Inode tmpInode = Json.decodeValue(new String(inodeBytes), Inode.class);
                    boolean needDeleteInode = tmpInode.isDeleteMark() && StringUtils.isNotBlank(tmpInode.getObjName())
                            && StringUtils.isNotBlank(objMeta.key) && tmpInode.getObjName().equals(objMeta.key);
                    if (needDeleteInode) {
                        inode = tmpInode;
                        objMeta.setVersionNum(inode.getVersionNum());
                        objStamp = String.valueOf(inode.getMtime() * 1000L + inode.getMtimensec() / 1_000_000L);
//                        objMeta.setStamp(String.valueOf(inode.getMtime() * 1000L + inode.getMtimensec() / 1_000_000L));
                    }
                }
            }

            String oldVersionNum = objMeta.versionNum;
            // 待写入的版本号小于当前版本号，直接视为已成功写入删除标记
            // 说明元数据在未删除前已经发生改变(如重新上传同名对象)，无需再删除
            if (oldVersionNum.compareTo(versionNum) > 0) {
                String oldMetaUploadIdOrFileName = objMeta.fileName != null ? objMeta.fileName : objMeta.partUploadId;
                // 比较文件名是否一致，如果一致，则继续走删除流程
                if (uploadIdOrFileName == null || !uploadIdOrFileName.equals(oldMetaUploadIdOrFileName)) {
                    return;
                }
            }
            String vnode = finalKey.replace(ROCKS_VERSION_PREFIX, "").split(File.separator)[0];
            if (objMeta.deleteMark) {
                Tuple3<String, String, String> tuple = Utils.getAllMetaDataKey(vnode, objMeta.bucket, objMeta.key, objMeta.versionId, objMeta.stamp, objMeta.snapshotMark, objMeta.inode);
                String oldStampKey = tuple.var1;
                if (!versionEnabled) {
                    writeBatch.delete(oldStampKey.getBytes());
                    tuple.var1 = Utils.getMetaDataKey(vnode, objMeta.bucket, objMeta.key, objMeta.versionId, "0", objMeta.snapshotMark);
                }
                if (objMeta.inode > 0 && inode != null) {
                    tuple.var3 = Utils.getLifeCycleMetaKey(vnode, objMeta.bucket, objMeta.key, objMeta.versionId, objStamp, objMeta.inode);
                }
                writeBatch.delete(tuple.var1.getBytes());
                writeBatch.delete(tuple.var2.getBytes());
                writeBatch.delete(tuple.var3.getBytes());
                // 删除文件系统前缀记录
                if (objMeta.inode > 0) {
                    long deleteCookie = fileCookie == 0 ? objMeta.cookie : fileCookie;
                    if (null != inode) {
                        if (inode.isDeleteMark()) {
                            String inodeKey = Inode.getKey(vnode, objMeta.bucket, objMeta.inode);
                            writeBatch.delete(inodeKey.substring(1).getBytes());
                            writeBatch.delete(inodeKey.getBytes());
                            if (deleteCookie == 0) {
                                deleteCookie = inode.getCookie();
                            }
                        }
                    }
                    String cookieKey = Inode.getCookieKey(vnode, objMeta.bucket, deleteCookie);
                    writeBatch.delete(cookieKey.getBytes());
                    // 如果是旧版本（3.0.6P03之前）的元数据，还需删除多余的、未适配新格式的文件生命周期键
                    if (!VersionUtil.hasVersionMagicPrefix(metaBytes)) {
                        // 构造旧版本的生命周期元数据 key
                        String legacyCtimeLifecycleKey = Utils.getLifeCycleMetaKey(vnode, objMeta.bucket, objMeta.key, objMeta.versionId, objMeta.stamp, objMeta.snapshotMark);
                        writeBatch.delete(legacyCtimeLifecycleKey.getBytes());
                    }
                }

                // 由于putObjectMeta将get去除后，为了解决lsVersions出现相同版本的同名对象问题将stampKey中的时间戳改为固定值0的缘故
                // 在桶开启多版本时需要额外删除时间戳为0的元数据
                if (NULL.equals(objMeta.versionId) && versionEnabled || versionEnabled && isMultiAliveStarted) {
                    writeBatch.delete(Utils.getMetaDataKey(vnode, objMeta.bucket, objMeta.key, objMeta.versionId, "0", objMeta.snapshotMark).getBytes());
                }
            }
            if (!objMeta.deleteMark && fileCookie > 0) {
                String cookieKey = Inode.getCookieKey(vnode, objMeta.bucket, fileCookie);
                byte[] cookieBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, cookieKey.getBytes());
                if (null != cookieBytes) {
                    Inode cookieInode = Json.decodeValue(new String(cookieBytes), Inode.class);
                    if (cookieInode.isDeleteMark() && cookieInode.getVersionNum().compareTo(versionNum) <= 0) {
                        writeBatch.delete(cookieKey.getBytes());
                    }
                }
            }
        };

        // 和putObject等接口保持一致，用ROCKS_LATEST_KEY计算hashcode
        if (key.startsWith(ROCKS_VERSION_PREFIX)) {
            int end = key.indexOf(ZERO_STR);
            key = ROCKS_LATEST_KEY + key.substring(1, end);
        }
        int hashCode = getFinalHashCode(key, snapshotMark != null);
        return BatchRocksDB.customizeOperateMeta(lun, hashCode, consumer)
                .map(b -> SUCCESS_PAYLOAD)
//                .doOnError(e -> log.error("", e))
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static void deleteInodeAndCookieDeleteMark(RocksDB db, WriteBatch writeBatch, String bucket, long nodeId, String key, String versionNum, long fileCookie) throws RocksDBException {
        String vnode = Utils.getVnode(key);
        String inodeKey = Inode.getKey(vnode, bucket, nodeId);
        byte[] inodeBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, inodeKey.getBytes());
        if (null != inodeBytes) {
            Inode inode = Json.decodeValue(new String(inodeBytes), Inode.class);
            if (inode.isDeleteMark() && inode.getVersionNum().compareTo(versionNum) <= 0) {
                writeBatch.delete(inodeKey.substring(1).getBytes());
                writeBatch.delete(inodeKey.getBytes());
                if (fileCookie == 0L && inode.getCookie() > 0) {
                    fileCookie = inode.getCookie();
                }
            }
        }
        if (fileCookie > 0) {
            String cookieKey = Inode.getCookieKey(vnode, bucket, fileCookie);
            writeBatch.delete(cookieKey.getBytes());
        }
    }

    public static RequestConsumer getDeleteFileConsumer(String lun, String... fileNames) {
        return (db, writeBatch, request) -> {
            try {
                for (String fileName : fileNames) {
                    if (StringUtils.isEmpty(fileName)) {
                        continue;
                    }
                    String fileMetaKey = FileMeta.getKey(fileName);
                    byte[] fileMetaBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, fileMetaKey.getBytes());
                    if (fileMetaBytes == null) {
                        continue;
                    }
                    FileMeta fileMeta = Json.decodeValue(new String(fileMetaBytes), FileMeta.class);
                    if (fileMeta.getFlushStamp() != null) {
                        writeBatch.delete(Utils.getCacheOrderKey(fileMeta.getFlushStamp(), fileName).getBytes());
                    }
                    if (fileMeta.getLastAccessStamp() != null) {//缓存盘上的数据下刷到数据池后删除时需要把访问记录的key删除
                        log.debug("delete file access time key: {}", Utils.getAccessTimeKey(fileMeta.getLastAccessStamp(), fileName));
                        writeBatch.delete(Utils.getAccessTimeKey(fileMeta.getLastAccessStamp(), fileName).getBytes());
                    }
                    writeBatch.delete(fileMetaKey.getBytes());

                    if (!fileName.replace("/split/", "").endsWith("/")) {
                        String sourceFileKey = FileMeta.getKey(fileName.split(ROCKS_FILE_META_PREFIX)[0]);
                        byte[] low = sourceFileKey.getBytes();
                        byte[] high = sourceFileKey.getBytes();
                        high[high.length - 1]++;

                        try (Slice lowerSlice = new Slice(low);
                             Slice upperSlice = new Slice(high);
                             ReadOptions readOptions = new ReadOptions().setIterateLowerBound(lowerSlice)
                                     .setIterateUpperBound(upperSlice);
                             RocksIterator iterator = db.newIterator(readOptions);
                             RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)) {

                            keyIterator.seek(sourceFileKey.getBytes());
                            boolean skip = false;
                            while (keyIterator.isValid() && new String(keyIterator.key()).startsWith(sourceFileKey)) {
                                String key = new String(keyIterator.key());
                                byte[] meteBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, key.getBytes());
                                if (meteBytes != null) {
                                    FileMeta fileMeta0 = Json.decodeValue(new String(meteBytes), FileMeta.class);
                                    long[] offset = fileMeta.getOffset();
                                    long[] offset0 = fileMeta0.getOffset();
                                    if (offset.length > 0 && offset0.length > 0 && offset[0] == offset0[0]) {
                                        skip = true;
                                        break;
                                    }
                                }
                                keyIterator.next();
                            }
                            if (skip) {
                                continue;
                            }
                        }
                    }

                    long[] offsets = fileMeta.getOffset();
                    long[] lens = fileMeta.getLen();
                    long totalLen = 0L;
                    for (int i = 0; i < offsets.length; i++) {
                        //计算删除文件占用空间大小
                        totalLen += lens[i];
                        List<byte[]> list = BlockInfo.getUpdateValue(offsets[i], lens[i], "delete");
                        for (int j = 0; j < list.size(); j++) {
                            String key = BlockInfo.getFamilySpaceKey((offsets[i] / SPACE_SIZE) + j);
                            writeBatch.merge(MSRocksDB.getColumnFamily(lun), key.getBytes(), list.get(j));
                        }
                    }

                    //将索引盘中有关要删除的索引合并
                    writeBatch.merge(ROCKS_FILE_SYSTEM_FILE_NUM, toByte(-1L));
                    writeBatch.merge(ROCKS_FILE_SYSTEM_FILE_SIZE, toByte(-fileMeta.getSize()));
                    writeBatch.merge(ROCKS_FILE_SYSTEM_USED_SIZE, toByte(-totalLen));

                    if (CompressorUtils.checkCompressEnable(fileMeta.getCompression())) {
                        long[] compressBeforeLen = fileMeta.getCompressBeforeLen();
                        long beforeCompressionLen = 0;
                        for (int i = 0; i < compressBeforeLen.length; i++) {
                            if (compressBeforeLen[i] != -1) {
                                long beforeLen = compressBeforeLen[i] % 4096 == 0 ? compressBeforeLen[i] : (compressBeforeLen[i] / 4096 * 4096 + 4096);
                                beforeCompressionLen += beforeLen;
                            }
                        }
                        long compressionFileSize = Arrays.stream(fileMeta.getCompressAfterLen()).sum();
                        writeBatch.merge(ROCKS_COMPRESSION_FILE_SYSTEM_FILE_SIZE, toByte(compressionFileSize - fileMeta.getSize()));
                        writeBatch.merge(ROCKS_BEFORE_COMPRESSION_FILE_SYSTEM_FILE_SIZE, toByte(-beforeCompressionLen));
                    } else {
                        writeBatch.merge(ROCKS_BEFORE_COMPRESSION_FILE_SYSTEM_FILE_SIZE, toByte(-totalLen));
                    }

                    // 准备释放逻辑空间
                    addToReleaseMap(offsets, lens, lun);
                }
            } catch (Exception e) {
                log.error("delfiles error", e);
            }
        };
    }

    public static Mono<Payload> checkObjectFileRead(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String[] fileNames = Json.decodeValue(msg.get("fileName"), String[].class);
        String lun = msg.get("lun");
        for (String fileName : fileNames) {//这里实际只遍历了一个fileName
            if (StringUtils.isEmpty(fileName)) {
                continue;
            }
            // 读对象过程中不能删除数据块
            String fileMetaKey = FileMeta.getKey(fileName);
            int hash = Math.abs(fileMetaKey.hashCode() % GetServerHandler.RUNNING_MAX_NUM);
            if (GetServerHandler.RUNNING[hash].get() > 0) {
                log.debug("file {} in {} is reading", fileMetaKey, lun);
                return Mono.just(WAITING_PAYLOAD);
            }
        }
        return Mono.just(SUCCESS_PAYLOAD);
    }

    /**
     * 删除带有删除标记的对象元数据和文件。
     */
    public static Mono<Payload> deleteFile(Payload payload) {
        boolean local = payload instanceof LocalPayload;
        SocketReqMsg msg = local ? ((LocalPayload<SocketReqMsg>) payload).data : SocketReqMsg.toSocketReqMsg(payload.data());

        String[] fileNames = Json.decodeValue(msg.get("fileName"), String[].class);
        String lun = msg.get("lun");
        boolean flag = StringUtils.isNotEmpty(msg.get("lifecycle_move"));
        boolean restoreDelete = StringUtils.isNotEmpty(msg.get("restore"));
        // 判断流量是否来自生命周期迁移或者修改EC删除旧对象数据
        if (flag || restoreDelete) {
            for (String fileName : fileNames) {
                if (StringUtils.isEmpty(fileName)) {
                    continue;
                }
                // 读对象过程中不能删除数据块
                String fileMetaKey = FileMeta.getKey(fileName);
                int hash = Math.abs(fileMetaKey.hashCode() % GetServerHandler.RUNNING_MAX_NUM);
                if (GetServerHandler.RUNNING[hash].get() > 0) {
                    log.debug("file {} in {} is reading, can't be deleted.", fileMetaKey, lun);
                    return Mono.just(WAITING_PAYLOAD);
                }
            }
        }

        RequestConsumer consumer = getDeleteFileConsumer(lun, fileNames);
        return BatchRocksDB.customizeOperateData(lun, consumer)
                .map(b -> local ? LocalPayload.SUCCESS_PAYLOAD : SUCCESS_PAYLOAD)
//                .doOnError(e -> log.error("", e))
                .onErrorReturn(local ? LocalPayload.SUCCESS_PAYLOAD : SUCCESS_PAYLOAD);
    }

    public static Mono<Payload> putPartInfo(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String[] value = new String[]{msg.get("value")};
        String oldVersion = msg.dataMap.getOrDefault("oldVersion", null);
        String snapshotLink = msg.get("snapshotLink");// 用于覆盖快照创建前上传的分段时使用
        MonoProcessor<Payload> res = MonoProcessor.create();
        PartInfo partInfo0 = Json.decodeValue(value[0], PartInfo.class);
        String vnode = key.split(File.separator)[0].replace(ROCKS_PART_META_PREFIX, "");
        String latestKey = Utils.getLatestMetaKey(vnode, partInfo0.bucket, partInfo0.object, partInfo0.snapshotMark);
//        String[] tmpUpdateQuotaKeyStr = new String[]{partInfo0.getTmpUpdateQuotaKeyStr()};
//        if (StringUtils.isBlank(tmpUpdateQuotaKeyStr[0])){
//            tmpUpdateQuotaKeyStr[0] = FSQuotaUtils.getQuotaKeys(partInfo0.bucket, partInfo0.object, System.currentTimeMillis(),0,0);
//        }
        value[0] = Json.encode(partInfo0.setTmpUpdateQuotaKeyStr(null));
        RequestConsumer consumer = (db, writeBatch, request) -> {
            if (StringUtils.isNotEmpty(oldVersion) && GetMetaResEnum.GET_ERROR.name().equals(oldVersion)) {
                res.onNext(ERROR_PAYLOAD);
                return;
            }
            String overWriteFileName = "";
            long deltaCapacity;
            byte[] partValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, key.getBytes());
            deltaCapacity = partInfo0.getPartSize();
            if (null != partValue) {
                PartInfo partInfo = Json.decodeValue(new String(partValue), PartInfo.class);

                if (StringUtils.isNotEmpty(oldVersion)) {
                    boolean oldVersionDelete = GetMetaResEnum.GET_NOT_FOUND.name().equals(oldVersion);
                    if ((oldVersionDelete && !partInfo.delete) || (!oldVersionDelete && partInfo.delete)) {
                        res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                        return;
                    } else if (!oldVersionDelete && !oldVersion.equals(partInfo.versionNum)) {
                        res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                        return;
                    }
                }
                deltaCapacity = deltaCapacity - partInfo.getPartSize();
                // 代码至此，代表修复元数据相关的判断已不用考虑。而修复是本地站点的行为，也不用考虑syncStamp的先后
                // 要更新的partInfo0，syncStamp有两个-，说明开了双活，判断先后以syncStamp为准而非versionNum
                if (DAVersionUtils.countHyphen(partInfo0.syncStamp) == 2) {
                    // 现有的partInfo syncStamp更大，不需要更新partInfo。
                    // 旧版本的syncStamp必定小于新版本。
                    int result = partInfo.syncStamp.compareTo(partInfo0.syncStamp);
                    if (result > 0) {
                        res.onNext(SUCCESS_PAYLOAD);
                        return;
                    } else if (result == 0) {
                        if (partInfo.versionNum.compareTo(partInfo0.versionNum) >= 0) {
                            res.onNext(SUCCESS_PAYLOAD);
                            return;
                        }
                    }
                } else {
                    if (partInfo.versionNum.compareTo(partInfo0.versionNum) >= 0) {
                        res.onNext(SUCCESS_PAYLOAD);
                        return;
                    }
                }
                if (partInfo.fileName != null && !partInfo.fileName.equals(partInfo0.fileName)) {
                    overWriteFileName = partInfo.fileName;
                }
            } else if (StringUtils.isNotEmpty(oldVersion) && !GetMetaResEnum.GET_NOT_FOUND.name().equals(oldVersion)) {
                res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                return;
            }
            if (snapshotLink != null) {
                TreeSet<String> snapshotLinks = Json.decodeValue(snapshotLink, new TypeReference<TreeSet<String>>() {
                });
                String prevSnapMark = snapshotLinks.first();
                String prevSnapKey = replaceSnapshotMark(key, prevSnapMark);
                byte[] prevSnapPartValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, prevSnapKey.getBytes());
                if (prevSnapPartValue != null) {
                    // 快照创建后上传了 快照创建前已经存在的分段
                    PartInfo partInfo = Json.decodeValue(new String(prevSnapPartValue), PartInfo.class);
                    if (partInfo.isViewable(partInfo0.snapshotMark)) {
                        // 进行逻辑删除
                        partInfo.addUnViewSnapshotMark(partInfo0.snapshotMark);
                        partInfo.setVersionNum(getVersionIncrement(partInfo.versionNum));
                        writeBatch.put(prevSnapKey.getBytes(), Json.encode(partInfo).getBytes());
                    }
                }
            }
            updateCapacityInfo(writeBatch, partInfo0.getBucket(), partInfo0.getObject(), vnode, 0, deltaCapacity);
//            updateAllKeyCap(tmpUpdateQuotaKeyStr[0], writeBatch, partInfo0.getBucket(), vnode, 0, deltaCapacity);
            writeBatch.put(key.getBytes(), value[0].getBytes());
            res.onNext(DefaultPayload.create(Json.encode(new Tuple2<>(overWriteFileName, partValue == null ? "" : new String(partValue))), SUCCESS.name()));
        };
        int hashCode = getFinalHashCode(latestKey, partInfo0.snapshotMark != null);
        return BatchRocksDB.customizeOperate(lun, hashCode, consumer, res)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> initPartUpload(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String initPartInfo = msg.get("value");
        String partKey = msg.get("key");
        String vnode = partKey.split(File.separator)[0].replace(ROCKS_PART_PREFIX, "");
        boolean isMigrate = "1".equals(msg.get("migrate"));
        InitPartInfo initInfo = Json.decodeValue(initPartInfo, InitPartInfo.class);
        String latestKey = Utils.getLatestMetaKey(vnode, initInfo.bucket, initInfo.object, initInfo.snapshotMark);
        MonoProcessor<Payload> res = MonoProcessor.create();
        RequestConsumer consumer = (db, writeBatch, request) -> {
            if (isMigrate) {
                byte[] bytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, partKey.getBytes());
                if (bytes != null) {
                    InitPartInfo oldInitPartInfo = Json.decodeValue(new String(bytes), InitPartInfo.class);
                    if (oldInitPartInfo.uploadId != null && initInfo.uploadId != null && oldInitPartInfo.uploadId.equals(initInfo.uploadId)
                            && oldInitPartInfo.versionNum.compareTo(initInfo.versionNum) >= 0) {
                        res.onNext(SUCCESS_PAYLOAD);
                        return;
                    }
                }
            } else if (isMultiAliveStarted) {
                //解决多站点环境下，分段差异记录可能会执行多次，导致有碎片
                if (!initInfo.delete) {
                    String completeKey = Utils.getVersionMetaDataKey(vnode, initInfo.bucket, initInfo.object, initInfo.metaData.versionId);
                    byte[] completeValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, completeKey.getBytes());
                    if (null != completeValue) {
                        MetaData completeMeta = Json.decodeValue(new String(completeValue), MetaData.class);
                        if (!completeMeta.isDeleteMark() && !completeMeta.isDeleteMarker() && initInfo.uploadId.equals(completeMeta.partUploadId)) {
                            res.onNext(SUCCESS_PAYLOAD);
                            return;
                        }
                    }
                }
            }
            // 删除分段任务时，检查分段是否已经合并,若已经合并则返回writed 状态，不删除数据块
            if (initInfo.delete) {
                String completeKey = Utils.getVersionMetaDataKey(vnode, initInfo.bucket, initInfo.object, initInfo.metaData.versionId);
                byte[] completeValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, completeKey.getBytes());
                if (null != completeValue) {
                    MetaData completeMeta = Json.decodeValue(new String(completeValue), MetaData.class);
                    if (!completeMeta.isDeleteMark() && !completeMeta.isDeleteMarker() && initInfo.uploadId.equals(completeMeta.partUploadId)) {
                        writeBatch.put(partKey.getBytes(), initPartInfo.getBytes());
                        res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                        return;
                    }
                }
            }
            writeBatch.put(partKey.getBytes(), initPartInfo.getBytes());
            res.onNext(SUCCESS_PAYLOAD);
        };
        int hashCode = getFinalHashCode(latestKey, initInfo.snapshotMark != null);
        return BatchRocksDB.customizeOperate(lun, hashCode, consumer, res)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> deletePart(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String initPartKey = msg.get("key");
        String vnode = msg.get("vnode");

        String partKey = msg.get("value");
        String isAbortPart = msg.get("isAbortPart");
        String bucketName = msg.get("bucket");
        String partInfoNumsStr = msg.get("partInfoNums");
        String latestKey = ROCKS_LATEST_KEY + initPartKey.substring(1, initPartKey.lastIndexOf(ZERO_STR));
        String initPartValue = msg.get("initPartInfo");
        String initPartSnapshotMark = msg.get("initPartSnapshotMark");
        String currentSnapshotMark = msg.get("currentSnapshotMark");

        Set<String> partInfoNums = new HashSet<>();
        if (!StringUtil.isNullOrEmpty(partInfoNumsStr)) {
            partInfoNums.addAll(Arrays.asList(partInfoNumsStr.split(",")));
        }
        RequestConsumer consumer = (db, writeBatch, request) -> {
            boolean markDelete = false;
            boolean isLogicalDelete = false;
            String tmpUpdateQuotaKeyStr = null;
            if (initPartValue != null) {
                InitPartInfo initPartInfo = Json.decodeValue(initPartValue, InitPartInfo.class);
                if (StringUtils.isNotBlank(initPartInfo.metaData.tmpInodeStr)) {
                    Inode tmpInode = Json.decodeValue(initPartInfo.metaData.tmpInodeStr, Inode.class);
                    tmpUpdateQuotaKeyStr = tmpInode.getXAttrMap().get(QUOTA_KEY);
                }
                if (initPartInfo.delete) {
                    markDelete = true;
                    if (currentSnapshotMark != null && !Objects.equals(currentSnapshotMark, initPartSnapshotMark)) {
                        // 快照创建后合并快照创建前上传的initPart
                        String prevInitPartKey = replaceSnapshotMark(initPartKey, initPartSnapshotMark);
                        byte[] prevSnapInitPartBytes = writeBatch.getFromBatchAndDB(db, prevInitPartKey.getBytes());
                        if (prevSnapInitPartBytes != null) {
                            InitPartInfo prevSnapInitPart = Json.decodeValue(new String(prevSnapInitPartBytes), InitPartInfo.class);
                            if (prevSnapInitPart.isViewable(currentSnapshotMark)) {
                                Set<String> unView = new HashSet<>(1);
                                unView.add(currentSnapshotMark);
                                prevSnapInitPart.setUnView(unView);
                                prevSnapInitPart.setVersionNum(initPartInfo.versionNum);
                                String prevSnapInitPartValue = Json.encode(prevSnapInitPart);
                                // 将快照创建前的initPart置为不可见
                                writeBatch.put(prevInitPartKey.getBytes(), prevSnapInitPartValue.getBytes());
                                isLogicalDelete = true;
                            }
                        }
                    }
                }
            }
            // 删除初始化分段任务
            if (markDelete) {
                if (!isLogicalDelete) {
                    writeBatch.put(initPartKey.getBytes(), initPartValue.getBytes());
                }
                return;
            } else {
                writeBatch.delete(initPartKey.getBytes());
            }

            // 删除分段碎片
            try (RocksIterator iterator = db.newIterator(READ_OPTIONS);
                 RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)) {
                keyIterator.seek(partKey.getBytes());
                while (keyIterator.isValid() && new String(keyIterator.key()).startsWith(partKey)) {
                    long deltaAvailableCapacity = 0;
                    PartInfo partInfo = Json.decodeValue(new String(keyIterator.value()), PartInfo.class);
                    if ("1".equals(isAbortPart)) {
                        //abortPart需删除文件大小
                        deltaAvailableCapacity = partInfoNums.contains(partInfo.getPartNum())
                                ? deltaAvailableCapacity : deltaAvailableCapacity - partInfo.getPartSize();
                    }
//                    long versionStamp = FSQuotaUtils.getStampFromVersion(partInfo.versionNum);
//                    tmpUpdateQuotaKeyStr = FSQuotaUtils.getQuotaKeys(partInfo.bucket, partInfo.object, versionStamp, 0, 0);
                    updateCapacityInfo(writeBatch, bucketName, partInfo.getObject(), vnode, 0, deltaAvailableCapacity);
//                    updateAllKeyCap(tmpUpdateQuotaKeyStr, writeBatch, bucketName, vnode, 0, deltaAvailableCapacity);
                    writeBatch.delete(keyIterator.key());
                    keyIterator.next();
                }
            }
        };
        int hashCode = getFinalHashCode(latestKey, initPartSnapshotMark != null);
        return BatchRocksDB.customizeOperateMeta(lun, hashCode, consumer)
                .map(b -> SUCCESS_PAYLOAD)
//                .doOnError(e -> log.error("", e))
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> listPartInfoFileNames(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String partKey = msg.get("key");
        String ignoreDeletePartNums = msg.get("ignoreDeletePartNums");
        Set<String> ignoreDeleteSet;
        if (ignoreDeletePartNums == null) {
            ignoreDeleteSet = new HashSet<>();
        } else {
            ignoreDeleteSet = Json.decodeValue(ignoreDeletePartNums, new TypeReference<Set<String>>() {
            });
        }
        List<String> res = new LinkedList<>();

        try (MSRocksIterator iterator = MSRocksDB.getRocksDB(lun).newIterator()) {
            iterator.seek(partKey.getBytes());
            while (iterator.isValid() && new String(iterator.key()).startsWith(partKey)) {
                PartInfo partInfo = Json.decodeValue(new String(iterator.value()), PartInfo.class);
                if (!ignoreDeleteSet.contains(partInfo.getPartNum())) {
                    if (partInfo.deduplicateKey != null && StringUtils.isNotEmpty(partInfo.deduplicateKey)) {
                        res.add(partInfo.deduplicateKey);
                    } else {
                        res.add(partInfo.fileName);
                    }
                }
                iterator.next();
            }
        }

        return Mono.just(DefaultPayload.create(Json.encode(res.stream().toArray(String[]::new)), SUCCESS.name()));
    }

    public static <T> Mono<Payload> getRocksValue(Payload payload, T errorResponse, T notFoundResponse) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String currentSnapshotMark = msg.get("currentSnapshotMark");
        String snapshotLink = msg.get("snapshotLink");

        try {
            MSRocksDB db = MSRocksDB.getRocksDB(lun);
            if (db == null) {
                return Mono.just(DefaultPayload.create(Json.encode(errorResponse), ERROR.name()));
            }
            Function<String, byte[]> getRocksValueFunction = (snapshotMark) -> {
                try {
                    String realKey = key;
                    if (snapshotMark != null && !snapshotMark.equals(currentSnapshotMark)) {
                        realKey = Utils.replaceSnapshotMark(key, snapshotMark);
                    }
                    byte[] value = null;
                    if (!realKey.startsWith(ROCKS_SPECIAL_KEY)) {
                        value = db.get(realKey.getBytes());
                    } else {
                        // 当开启多版本后不指定版本号, 从latestKey获取元数据
                        realKey = realKey.substring(1);

                        String vnode = realKey.split(File.separator)[0];
                        byte[] latestValue = db.get((ROCKS_LATEST_KEY + realKey).getBytes());
                        if (latestValue != null) {
                            MetaData latestMeta = Json.decodeValue(new String(latestValue), MetaData.class);
                            String versionKey = Utils.getVersionMetaDataKey(vnode, latestMeta.bucket, latestMeta.key, latestMeta.versionId, snapshotMark);
                            value = db.get(versionKey.getBytes());
                        }

                        if (value == null) {
                            try (Slice lowerSlice = new Slice(realKey.getBytes());
                                 ReadOptions readOptions = new ReadOptions().setIterateLowerBound(lowerSlice);
                                 MSRocksIterator keyIterator = db.newIterator(readOptions)) {
                                keyIterator.seek((realKey + Utils.ONE_STR).getBytes());
                                if (keyIterator.isValid()) {
                                    keyIterator.prev();
                                } else {
                                    keyIterator.seekToLast();
                                }
                                if (keyIterator.isValid() && new String(keyIterator.key()).startsWith(realKey)) {
                                    MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                                    String versionKey = Utils.getVersionMetaDataKey(vnode, metaData.bucket, metaData.key, metaData.versionId, snapshotMark);
                                    value = db.get(versionKey.getBytes());
                                }
                            }
                        }
                    }
                    return value;
                } catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
            };

            byte[] value;
            if (StringUtils.isAnyBlank(currentSnapshotMark, snapshotLink)) {
                // 未开桶快照，或开启桶快照但为创建快照
                value = getRocksValueFunction.apply(currentSnapshotMark);
            } else {
                // 开桶快照，并且创建快照，则需要进行数据合并
                SnapshotGetObjectService snapshotObjectService = new SnapshotGetObjectService();
                TreeSet<String> snapshotMarks = Json.decodeValue(snapshotLink, new TypeReference<TreeSet<String>>() {
                });
                if (snapshotMarks.contains(currentSnapshotMark)) {
                    value = getRocksValueFunction.apply(currentSnapshotMark);
                } else {
                    value = snapshotObjectService.getMerge(snapshotMarks, currentSnapshotMark, getRocksValueFunction);
                }
            }
            if (null == value) {
                return Mono.just(DefaultPayload.create(Json.encode(notFoundResponse), NOT_FOUND.name()));
            } else {
                byte start = msg.get("key").getBytes()[0];
                if (start == '*' || start == '%') {
                    MetaData realValue = Json.decodeValue(new String(value), MetaData.class);
                    if (realValue.inode > 0) {
                        //fuse meta
                        String vnode = Utils.getVnode(msg.get("key"));
                        String inodeKey = Inode.getKey(vnode, realValue.bucket, realValue.inode);
                        byte[] inodeBytes = db.get(inodeKey.getBytes());
                        if (inodeBytes != null) {
                            Inode inode = Json.decodeValue(new String(inodeBytes), Inode.class);
                            CifsUtils.setDefaultCifsMode(inode);
                            Inode.mergeMeta(realValue, inode);
                            //处理软连接
                            if ((inode.getMode() & S_IFMT) == S_IFLNK) {
                                realValue.endIndex = -1;
                            }
                        }
                        return Mono.just(DefaultPayload.create(Json.encode(realValue), SUCCESS.name()));
                    }
                }
                if (start == '?') {
                    AggregateFileMetadata fileMetadata = Json.decodeValue(new String(value), AggregateFileMetadata.class);
                    if (!fileMetadata.deleteMark) {
                        String bitmapKey = AggregationUtils.getAggregateFileBitmapKey(getVnode(key), fileMetadata.namespace, fileMetadata.aggregationId);
                        byte[] bytes = db.get(bitmapKey.getBytes());
                        if (bytes != null) {
                            fileMetadata.bitmap = Base64.getEncoder().encodeToString(bytes);
                            return Mono.just(DefaultPayload.create(Json.encode(fileMetadata), SUCCESS.name()));
                        }
                    }
                }
                return Mono.just(DefaultPayload.create(value, SUCCESS.name().getBytes()));
            }

        } catch (Exception e) {
            log.error("", e);
            return Mono.just(DefaultPayload.create(Json.encode(errorResponse), ERROR.name()));
        }
    }


    //仅记录获取桶信息时错误的lun
    private static final Set<String> ERROR_LUN_SET = ConcurrentHashMap.newKeySet();

    public static Mono<Payload> getBucketInfo(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String capacityKey = key + CAPACITY_SUFFIX;
        String objNumKey = key + OBJNUM_SUFFIX;
        try {
            MSRocksDB db = MSRocksDB.getRocksDB(lun);
            BucketInfo info = new BucketInfo();
            info.setVersionNum("0");
            byte[] value = db.get(capacityKey.getBytes());
            if (value != null) {
                info.setBucketStorage(String.valueOf(bytes2long(value)));
            } else {
                return Mono.just(DefaultPayload.create(Json.encode(NOT_FOUND_BUCKET_INFO), NOT_FOUND.name()));
            }
            if ((value = db.get(objNumKey.getBytes())) != null) {
                info.setObjectNum(String.valueOf(bytes2long(value)));
            }
            if (null == value) {
                return Mono.just(DefaultPayload.create(Json.encode(NOT_FOUND_BUCKET_INFO), NOT_FOUND.name()));
            } else {
                return Mono.just(DefaultPayload.create(Json.encode(info), SUCCESS.name()));
            }

        } catch (NullPointerException e) {
            return Mono.just(DefaultPayload.create(Json.encode(ERROR_BUCKET_INFO), ERROR.name()));
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("rocks db fail")) {
                if (!ERROR_LUN_SET.contains(lun)) {
                    ERROR_LUN_SET.add(lun);
                    log.error("", e);
                }
            } else {
                log.error("", e);
            }
            return Mono.just(DefaultPayload.create(Json.encode(ERROR_BUCKET_INFO), ERROR.name()));
        }
    }

    public static Mono<Payload> mergeTmpBucketInfo(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String vnode = msg.get("vnode");
        String bucket = msg.get("bucket");
        ShardingWorker.SHARDING_LOCK_SET.remove(bucket);
        RequestConsumer consumer = (db, w, request) -> {
            String tmpCapacityKey = getTmpCapacityKey(vnode, bucket);
            String tmpObjNumKey = getTmpObjNumKey(vnode, bucket);
            byte[] capacityBytes = db.get(tmpCapacityKey.getBytes());
            if (capacityBytes != null) {
                w.merge(BucketInfo.getCpacityKey(vnode, bucket).getBytes(), capacityBytes);
                w.delete(tmpCapacityKey.getBytes());
            }
            byte[] objNumBytes = db.get(tmpObjNumKey.getBytes());
            if (objNumBytes != null) {
                w.merge(BucketInfo.getObjNumKey(vnode, bucket).getBytes(), objNumBytes);
                w.delete(tmpObjNumKey.getBytes());
            }
        };

        return BatchRocksDB.customizeOperateMetaLowPriority(lun, bucket.hashCode(), consumer)
                .map(str -> SUCCESS_PAYLOAD)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    /**
     * 设置RocksDB中对象元数据
     *
     * @param payload 包含元数据信息payload
     * @return 旧的元数据，需要删除对应的文件
     * @throws Exception
     */
    public static Mono<Payload> putObjectMeta(Payload payload) {
        boolean local = payload instanceof LocalPayload;
        SocketReqMsg msg = local ? ((LocalPayload<SocketReqMsg>) payload).data : SocketReqMsg.toSocketReqMsg(payload.data());
        String status = msg.get("status");
        String[] value = new String[]{msg.get("value")};
        String lun = msg.get("lun");
        String stampKey = msg.get("key");
        String snapshotLink = msg.get("snapshotLink");// 添加删除标记时使用，用于将快照创建前对象置为不可见
        String vnode = stampKey.split(File.separator)[0];
        MetaData newMeta = Json.decodeValue(value[0], MetaData.class);

        String[] inode = new String[1];
        Inode[] newInode = new Inode[1];
        long[] deltaCap = new long[1];
        /**
         * 用以区分控制台重命名NFS上传的文件和修改元数据的情况
         * 控制台重命名 容量不需改变
         * S3修改元数据，容量需改变。
         */
        boolean[] needPut = new boolean[]{true};
        String[] updateQuotaKeyStr = new String[1];
        if (newMeta.inode > 0) {
            if (StringUtils.isNotEmpty(newMeta.getTmpInodeStr())) {
                inode[0] = newMeta.getTmpInodeStr();
                newInode[0] = Json.decodeValue(inode[0], Inode.class);
                if (newInode[0].getSize() > 0
                        && (newInode[0].getInodeData() == null || newInode[0].getInodeData().size() == 0)
                        && (newInode[0].getMode() & S_IFMT) != S_IFLNK
                ) {
                    needPut[0] = false;
                }
                updateQuotaKeyStr[0] = newInode[0].getXAttrMap().get(QUOTA_KEY);
                newInode[0].getXAttrMap().remove(QUOTA_KEY);
                inode[0] = Json.encode(newInode[0]);
            }
            deltaCap[0] = newMeta.deleteMarker ? 0 : (newMeta.getEndIndex() - newMeta.getStartIndex() + 1);
            newMeta.tmpInodeStr = null;
            Inode.reduceMeta(newMeta);
        } else {
            inode[0] = "";
            //S3上传对象，适配更新文件配额
            if (StringUtils.isNotEmpty(newMeta.getTmpInodeStr())) {
                Inode tmpInode = Json.decodeValue(newMeta.getTmpInodeStr(), Inode.class);
                updateQuotaKeyStr[0] = tmpInode.getXAttrMap().get(QUOTA_KEY);
                newMeta.tmpInodeStr = null;
            }
        }
        value[0] = Json.encode(newMeta);
        Tuple3<String, String, String> tuple = Utils.getAllMetaDataKey(vnode, newMeta.bucket, newMeta.key, newMeta.versionId, newMeta.stamp, newMeta.snapshotMark, newMeta.inode);
        MonoProcessor<String> res = MonoProcessor.create();
        String latestKey = Utils.getLatestMetaKey(vnode, newMeta.bucket, newMeta.key, newMeta.snapshotMark);
        boolean versionEnabled = "Enabled".equals(status) || "Suspended".equals(status);
        boolean isMigrate = "1".equals(msg.get("migrate"));
        final byte[][] simplifyValue = new byte[1][1];

        if (!versionEnabled) {
            simplifyValue[0] = Utils.simplifyMetaJson(newMeta);
        } else {
            simplifyValue[0] = null;
        }

        RequestConsumer consumer = (db, writeBatch, request) -> {
            byte[] oldValue;
            if (!versionEnabled && !isMigrate && !isAppendableObject(newMeta)) {
                if (OverWriteHandler.ASYNC_OVER_WRITE) {
                    oldValue = null;
                } else {
                    oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, tuple.var2.getBytes());
                }
            } else {
                oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, tuple.var2.getBytes());
            }

            long deltaAvailableCapacity = newMeta.deleteMarker ? 0 : Utils.getObjectSize(newMeta);
            if (snapshotLink != null && newMeta.partUploadId != null) {
                long tempCap = 0L;
                for (PartInfo partInfo : newMeta.partInfos) {
                    if (newMeta.snapshotMark.equals(partInfo.snapshotMark)) {
                        tempCap += partInfo.partSize;
                    }
                }
                deltaAvailableCapacity = tempCap;
            }
            long objNum = 0;
            long oldDeltaCapacity = 0;
            boolean oldValueDeleteMark = true;
            boolean oldValueDeleteMarker = true;
            boolean versionIsEquals = false;
            Inode oldTmpInode = null;
            if (null != oldValue) {
                MetaData oldMeta = Json.decodeValue(new String(oldValue), MetaData.class);
                versionIsEquals = newMeta.versionId.equals(oldMeta.versionId);
                if (!oldMeta.deleteMark) {
                    oldValueDeleteMark = false;
                    objNum = oldMeta.deleteMarker ? 1 : 0;
                    oldDeltaCapacity = oldMeta.deleteMarker ? 0 : Utils.getObjectSize(oldMeta);
                } else {
                    objNum = 1;
                }
                if (!oldMeta.deleteMarker) {
                    oldValueDeleteMarker = false;
                }

                // 元数据已经为最新无需更新
                if (!oldMeta.isDiscard() && (!oldMeta.deleteMark || isMigrate)) {
                    if (countHyphen(newMeta.syncStamp) == 2 && oldMeta.syncStamp != null && newMeta.syncStamp.compareTo(oldMeta.syncStamp) < 0) {
                        res.onNext(value[0]);
                        return;
                    } else {
                        if (oldMeta.getVersionNum().compareTo(newMeta.getVersionNum()) >= 0) {
                            res.onNext(oldMeta.getVersionNum().compareTo(newMeta.getVersionNum()) == 0 ? "" : value[0]);
                            return;
                        }
                    }
                }

                // 【文件】新版本执行：如果被覆盖的数据是文件，且当前由s3端操作，文件的inode>0，且不为删除标记，读取当前已存的inode放入oldValue的tmpInodeStr中，用于同步删除旧的数据块
                if (oldMeta.inode > 0 && !oldMeta.deleteMark && newMeta.inode == 0) {
                    String inodeKey = Inode.getKey(vnode, oldMeta.bucket, oldMeta.inode);
                    byte[] inodeValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, inodeKey.getBytes());
                    if (null != inodeValue) {
                        String inodeValueStr = new String(inodeValue);
                        Inode oldInode = Json.decodeValue(inodeValueStr, Inode.class);
                        int linkN = oldInode.getLinkN();
                        oldMeta.tmpInodeStr = inodeValueStr;
                        if (linkN > 1) {
                            //存在硬链接，链接数减一
                            oldInode.setLinkN(linkN - 1);
                            oldValue = Json.encode(oldMeta).getBytes();
                            oldDeltaCapacity = 0;
                            writeBatch.delete(inodeKey.getBytes());
                            writeBatch.put(inodeKey.getBytes(), Json.encode(oldInode).getBytes());
                        } else {
                            //不存在硬链接，删除inode
                            oldValue = Json.encode(oldMeta).getBytes();
                            oldDeltaCapacity = oldInode.isDeleteMark() ? 0 : oldInode.getSize();
                            writeBatch.delete(inodeKey.getBytes());

                        }

                        oldTmpInode = oldInode.clone();

                        long oldMtimeMs = oldInode.getMtime() * 1000L + (oldInode.getMtimensec() / 1_000_000L);
                        String oldLifecycleKey = Utils.getLifeCycleMetaKey(vnode, oldInode.getBucket(), oldInode.getObjName(), oldInode.getVersionId(), String.valueOf(oldMtimeMs), oldInode.getNodeId());
                        writeBatch.delete(oldLifecycleKey.getBytes());
                    }
                    String cookieKey = getCookieKey(vnode, oldMeta.bucket, oldMeta.inode);
                    writeBatch.delete(cookieKey.getBytes());

                }

                //【文件】旧版本执行
                if (oldMeta.inode > 0 && !oldMeta.deleteMark && newMeta.inode > 0 && !OverWriteHandler.ASYNC_OVER_WRITE) {
                    String inodeKey = Inode.getKey(vnode, oldMeta.bucket, oldMeta.inode);
                    byte[] inodeValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, inodeKey.getBytes());
                    deltaAvailableCapacity = deltaCap[0];
                    if (null != inodeValue) {
                        String inodeValueStr = new String(inodeValue);
                        Inode oldInode = Json.decodeValue(inodeValueStr, Inode.class);
                        //处理修改元数据的情况
                        if (oldInode.getLinkN() == 1 || (oldMeta.inode == newMeta.inode)) {
                            if (oldMeta.inode == newMeta.inode && StringUtils.isBlank(inode[0])) {
                                if (oldInode.getSize() == deltaCap[0]) {
                                    oldDeltaCapacity = oldInode.isDeleteMark() ? 0 : oldInode.getSize();
                                } else if (deltaCap[0] != 0) {
                                    oldDeltaCapacity = deltaCap[0];
                                }
                            }
                            if (oldMeta.inode != newMeta.inode) {
                                if (oldInode.getObjName().equals(oldMeta.key)) {
                                    writeBatch.delete(inodeKey.substring(1).getBytes());
                                    writeBatch.delete(inodeKey.getBytes());
                                    String cookieKey = Inode.getCookieKey(vnode, oldMeta.bucket, oldMeta.cookie);
                                    writeBatch.delete(cookieKey.getBytes());
                                }
                                oldDeltaCapacity = oldInode.isDeleteMark() ? 0 : oldInode.getSize();
                            }
                            oldTmpInode = oldInode.clone();
                        }
                        long oldMtimeMs = oldInode.getMtime() * 1000L + (oldInode.getMtimensec() / 1_000_000L);
                        String oldLifecycleKey = Utils.getLifeCycleMetaKey(vnode, oldInode.getBucket(), oldInode.getObjName(), oldInode.getVersionId(), String.valueOf(oldMtimeMs), oldInode.getNodeId());
                        writeBatch.delete(oldLifecycleKey.getBytes());
                    }
                }


                Tuple3<String, String, String> oldTuple = Utils.getAllMetaDataKey(vnode, oldMeta.bucket, oldMeta.key, oldMeta.versionId, oldMeta.stamp, newMeta.snapshotMark, oldMeta.inode);

                if (!tuple.var1.equals(oldTuple.var1) || oldMeta.inode > 0) {
                    writeBatch.delete(oldTuple.var1.getBytes());
                }

                if (!tuple.var3.equals(oldTuple.var3)) {
                    writeBatch.delete(oldTuple.var3.getBytes());
                }

                if (NULL.equals(newMeta.versionId) || versionEnabled && isMultiAliveStarted) {
                    writeBatch.delete(Utils.getMetaDataKey(vnode, newMeta.bucket, newMeta.key, newMeta.versionId, "0", newMeta.snapshotMark).getBytes());
                }
            } else {
                objNum = newMeta.deleteMarker ? 0 : 1;
                if (deltaCap[0] != 0) {
                    deltaAvailableCapacity = deltaCap[0];
                }
            }
            // 当未开启过多版本状态时
            if (!versionEnabled) {
                // 防止listVersions时出现多个版本号相同的同名对象，故将stamp一致化
                tuple.var1 = Utils.getMetaDataKey(vnode, newMeta.bucket, newMeta.key, newMeta.versionId, "0", newMeta.snapshotMark);
                if (OverWriteHandler.ASYNC_OVER_WRITE && !isMigrate && !isAppendableObject(newMeta)) {
                    writeBatch.merge(tuple.var1.getBytes(), simplifyValue[0]);
                    writeBatch.merge(tuple.var2.getBytes(), value[0].getBytes());
                } else {
                    writeBatch.put(tuple.var1.getBytes(), simplifyValue[0]);
                    writeBatch.put(tuple.var2.getBytes(), value[0].getBytes());
                }
                writeBatch.put((tuple.var3).getBytes(), new byte[]{0});
            } else {
                writeBatch.put(true, tuple.var1.getBytes(), value[0].getBytes());
                writeBatch.put(tuple.var2.getBytes(), value[0].getBytes());
                writeBatch.put(tuple.var3.getBytes(), new byte[]{0});
            }
            boolean addCapacity = false;
            if (newMeta.inode > 0 && StringUtils.isNotEmpty(inode[0]) && needPut[0]) {
                //创建硬链接inode不存在计算容量
                if (newInode[0].getInodeData().size() > 0 && newInode[0].getLinkN() > 1
                        && deltaCap[0] == 0 && deltaCap[0] != newInode[0].getSize()) {
                    byte[] oldInodeBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, Inode.getKey(vnode, newMeta.bucket, newMeta.inode).getBytes());
                    if (oldInodeBytes == null) {
                        addCapacity = true;
                        deltaCap[0] = newInode[0].getSize();
                    }
                }
                writeBatch.put(Inode.getKey(vnode, newMeta.bucket, newMeta.inode).getBytes(), inode[0].getBytes());
                // 创建cookie-key表
                Inode cookieInode = Json.decodeValue(inode[0], Inode.class);
                String cookieValue = Json.encode(Inode.defaultCookieInode(cookieInode).setCookie(newMeta.getCookie()));
                writeBatch.put(Inode.getCookieKey(vnode, newMeta.bucket, newMeta.cookie).getBytes(), cookieValue.getBytes());
            }

            if (NULL.equals(newMeta.versionId)) {
                if (newMeta.deleteMarker) {
                    writeBatch.delete(latestKey.getBytes());
                    if ("Suspended".equals(status) && oldValue != null && versionIsEquals) {
                        objNum += -1;
                    }
                } else {
                    if (!versionEnabled && !isMigrate) {
                        if (OverWriteHandler.ASYNC_OVER_WRITE) {
                            writeBatch.merge(latestKey.getBytes(), simplifyValue[0]);
                        } else {
                            writeBatch.put(latestKey.getBytes(), simplifyValue[0]);
                        }
                    } else {
                        if (isMultiAliveStarted) {
                            // 此时桶启用多版本，对象为暂停时或一开始上传的对象(无versionId)，需要查找所有metaKey确认latestkey是否为自己
                            try (Slice lowerSlice = new Slice(latestKey.substring(1).getBytes());
                                 Slice upperSlice = new Slice((latestKey.substring(1) + Utils.ONE_STR).getBytes());
                                 ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperSlice).setIterateLowerBound(lowerSlice);
                                 RocksIterator iterator = db.newIterator(readOptions);
                                 RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)) {
                                keyIterator.seek(tuple.var1.getBytes());
                                if (keyIterator.isValid() && new String(keyIterator.key()).equals(tuple.var1)) {
                                    keyIterator.next();
                                }
                                if (keyIterator.isValid() && new String(keyIterator.key()).startsWith(latestKey.substring(1))) {
                                    MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                                    if (!metaData.key.equals(newMeta.key) || !metaData.bucket.equals(newMeta.bucket)) {
                                        if (newMeta.deleteMarker) {
                                            writeBatch.delete(latestKey.getBytes());
                                        } else {
                                            writeBatch.put(true, latestKey.getBytes(), value[0].getBytes());
                                        }
                                    }
                                } else {
                                    if (newMeta.deleteMarker) {
                                        writeBatch.delete(latestKey.getBytes());
                                    } else {
                                        writeBatch.put(true, latestKey.getBytes(), value[0].getBytes());
                                    }
                                }
                            }

                        } else {
                            writeBatch.put(true, latestKey.getBytes(), value[0].getBytes());
                        }
                    }
                }
            } else {
                try (Slice lowerSlice = new Slice(latestKey.substring(1).getBytes());
                     Slice upperSlice = new Slice((latestKey.substring(1) + Utils.ONE_STR).getBytes());
                     ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperSlice).setIterateLowerBound(lowerSlice);
                     RocksIterator iterator = db.newIterator(readOptions);
                     RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)) {
                    keyIterator.seek(tuple.var1.getBytes());
                    if (keyIterator.isValid() && new String(keyIterator.key()).equals(tuple.var1)) {
                        keyIterator.next();
                    }
                    if (keyIterator.isValid() && new String(keyIterator.key()).startsWith(latestKey.substring(1))) {
                        MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                        if (!metaData.key.equals(newMeta.key) || !metaData.bucket.equals(newMeta.bucket)) {
                            if (newMeta.deleteMarker) {
                                writeBatch.delete(latestKey.getBytes());
                            } else {
                                writeBatch.put(true, latestKey.getBytes(), value[0].getBytes());
                            }
                        }
                    } else {
                        if (newMeta.deleteMarker) {
                            writeBatch.delete(latestKey.getBytes());
                        } else {
                            writeBatch.put(true, latestKey.getBytes(), value[0].getBytes());
                        }
                    }
                }
            }
            // 桶快照处理
            if (snapshotLink != null) {
                snapshotLogicalDeleteProcessing(db, writeBatch, snapshotLink, newMeta, vnode, versionEnabled);
            }

            objNum = newMeta.isDeleteMarker() && !"Suspended".equals(status) ? 0 : objNum;
            long capacity;
            if (oldValueDeleteMark) {
                capacity = deltaAvailableCapacity;
            } else if (oldValueDeleteMarker) {
                capacity = deltaAvailableCapacity;
            } else {
                capacity = deltaAvailableCapacity - oldDeltaCapacity;
            }
            if (newMeta.inode > 0 && StringUtils.isEmpty(inode[0]) && OverWriteHandler.ASYNC_OVER_WRITE) {
                capacity = deltaCap[0];
            }
            //控制台重命名NFS上传的文件容量不进行更新
            if (newInode[0] != null && newInode[0].getNodeId() > 0 && OverWriteHandler.ASYNC_OVER_WRITE) {
                if (newInode[0].getInodeData().size() == 0) {
                    capacity = 0;
                } else {
                    //控制台同名覆盖硬链接
                    capacity = deltaCap[0];
                }
            }
            if (newInode[0] != null && addCapacity) {
                capacity = newInode[0].getSize();
            }
            if (isMigrate) {
                updateTempCapacityInfo(writeBatch, newMeta.getBucket(), vnode, objNum, capacity);
            } else {
                updateCapacityInfo(writeBatch, newMeta.getBucket(), newMeta.getKey(), vnode, objNum, capacity);
            }
            if (oldTmpInode != null && FSQuotaUtils.isNotRootUserOrGroup(oldTmpInode) && !OverWriteHandler.ASYNC_OVER_WRITE) {
                FSQuotaUtils.updateCapInfoSyncOverWrite(writeBatch, newMeta.getBucket(), newMeta.key, vnode, 1, oldDeltaCapacity, deltaAvailableCapacity, oldTmpInode);
            } else {
                updateAllKeyCap(updateQuotaKeyStr[0], writeBatch, newMeta.getBucket(), vnode, objNum, capacity);
            }

            res.onNext(oldValue == null ? "" : new String(oldValue));
        };

        int hashCode = getFinalHashCode(latestKey, newMeta.snapshotMark != null);
        Mono<Boolean> mono;
        if (!versionEnabled && !isMigrate && OverWriteHandler.ASYNC_OVER_WRITE) {
            mono = BatchRocksDB.customizeOperateMetaLowPriority(lun, hashCode, consumer);
        } else if (payload instanceof MsPayload && ((MsPayload) payload).loopIndex != -1) {
            EventLoop loop = RSocketClient.loops[((MsPayload) payload).loopIndex];
            mono = BatchRocksDB.customizeOperateMeta(lun, hashCode, consumer, loop);
        } else {
            mono = BatchRocksDB.customizeOperateMeta(lun, hashCode, consumer);
        }

        return mono.flatMap(b -> res)
                .map(str -> DefaultPayload.create(str, SUCCESS.name()))
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> updateLifecycle(Payload payload) {
        return updateMeta(payload);
    }

    public static Mono<Payload> updateMeta(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        final String[] value = {msg.get("value")};
        String lun = msg.get("lun");
        String key = msg.get("key");
        String status = msg.get("status");
        String oldVersion = msg.dataMap.getOrDefault("oldVersion", null);
        MonoProcessor<Payload> res = MonoProcessor.create();
        boolean versionEnabled = "Enabled".equals(status) || "Suspended".equals(status);
        boolean isCreateS3Inode = "1".equals(msg.get("createS3Inode"));
        MetaData metaData = Json.decodeValue(value[0], MetaData.class);
        String snapshotLink = msg.get("snapshotLink");
        String[] inode = new String[1];
        String[] tmpInode = new String[1];
        String[] updateCapKeys = new String[1];
        String[] updateS3InodeCapKeys = new String[1];
        long[] s3InodeSize = {0};
        if (metaData.inode > 0) {
            if (StringUtils.isNotBlank(metaData.tmpInodeStr)) {
                inode[0] = metaData.tmpInodeStr;
                metaData.tmpInodeStr = null;
                Inode inodeVal = Json.decodeValue(inode[0], Inode.class);
                tmpInode[0] = inode[0];
                /**
                 *  metadata 恢复时，get到的metadata中会存在tmpInodeStr，但tmpInodeStr中不存在inodeData
                 *  此时inode不需要进行put（软连接除外）
                 */

                if (inodeVal.getSize() > 0
                        && (null == inodeVal.getInodeData() || inodeVal.getInodeData().size() == 0)
                        && (inodeVal.getMode() & S_IFMT) != S_IFLNK
                ) {
                    inode[0] = "";
                }
                updateCapKeys[0] = inodeVal.getXAttrMap().get(QUOTA_KEY);
                updateS3InodeCapKeys[0] = inodeVal.getXAttrMap().get(QUOTA_KEY_CREATE_S3_INODE);
                s3InodeSize[0] = inodeVal.getSize();
                if (StringUtils.isNotBlank(inode[0])) {
                    inodeVal.getXAttrMap().remove(QUOTA_KEY);
                    inodeVal.getXAttrMap().remove(QUOTA_KEY_CREATE_S3_INODE);
                    inode[0] = Json.encode(inodeVal);
                }

            } else {
                inode[0] = "";
            }
            boolean renamedMeta = metaData.deleteMark && metaData.partInfos != null && metaData.partInfos.length == 0 && "inode".equals(metaData.partUploadId);
            Inode.reduceMeta(metaData);
            if (renamedMeta) {
                metaData.partInfos = new PartInfo[0];
                metaData.partUploadId = "inode";
            }
            value[0] = Json.encode(metaData);
        } else {
            inode[0] = "";
            //S3上传对象，适配更新文件配额
            if (StringUtils.isNotEmpty(metaData.getTmpInodeStr())) {
                Inode tmpInode0 = Json.decodeValue(metaData.getTmpInodeStr(), Inode.class);
                updateCapKeys[0] = tmpInode0.getXAttrMap().get(QUOTA_KEY);
                value[0] = Json.encode(metaData.setTmpInodeStr(null));
                metaData.tmpInodeStr = null;
            }
        }

        String vnode = key.replace(ROCKS_VERSION_PREFIX, "").replace(ROCKS_SPECIAL_KEY, "").split(File.separator)[0];
        String latestKey = Utils.getLatestMetaKey(vnode, metaData.bucket, metaData.key, metaData.snapshotMark);
        Tuple3<String, String, String> tuple = Utils.getAllMetaDataKey(vnode, metaData.bucket, metaData.key, metaData.versionId, metaData.stamp, metaData.snapshotMark, metaData.inode);
        String lifeCycleKeyWithOldStamp = Utils.getLifeCycleMetaKey(vnode, metaData.bucket, metaData.key, metaData.versionId, metaData.stamp, metaData.snapshotMark);

        RequestConsumer consumer = (db, writeBatch, request) -> {
            MetaData oldMeta = null;
            Inode oldMetaInode = null;
            String oldInodeMetaKey = null;
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, tuple.var2.getBytes());
            if (StringUtils.isEmpty(oldVersion) || GetMetaResEnum.GET_ERROR.name().equals(oldVersion)) {
                res.onNext(ERROR_PAYLOAD);
                return;
            }
            if (oldValue != null) {
                oldMeta = Json.decodeValue(new String(oldValue), MetaData.class);
                if (GetMetaResEnum.GET_NOT_FOUND.name().equals(oldVersion) && !oldMeta.deleteMark) {
                    res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                    return;
                } else if (!oldMeta.versionNum.equals(oldVersion) && (metaData.inode == 0 || StringUtils.isEmpty(inode[0]) || isCreateS3Inode)) {
                    if (isCreateS3Inode) {
                        //创建S3 Inode时，如果metadata被修改过，则直接返错重试。
                        res.onNext(ERROR_PAYLOAD);
                    } else {
                        res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                    }
                    return;
                }
            } else if (!GetMetaResEnum.GET_NOT_FOUND.name().equals(oldVersion)) {
                res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                return;
            }

            if (metaData.deleteMark && oldValue != null) {
                if (!oldMeta.deleteMark && oldMeta.versionNum.compareTo(oldVersion) <= 0) {
                    String seekKey = Utils.getMetaDataKey(vnode, metaData.bucket, metaData.key, metaData.versionId, versionEnabled ? oldMeta.stamp : "0", oldMeta.snapshotMark);
                    boolean hasSameNameObject = false;
                    boolean deleteLatest = false;
                    try (Slice lowerSlice = new Slice(latestKey.substring(1).getBytes());
                         Slice upperSlice = new Slice((latestKey.substring(1) + Utils.ONE_STR).getBytes());
                         ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperSlice).setIterateLowerBound(lowerSlice);
                         RocksIterator iterator = db.newIterator(readOptions);
                         RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)) {
                        keyIterator.seek((seekKey).getBytes());
                        if (keyIterator.isValid()) {
                            if (new String(keyIterator.key()).equals(seekKey)) {
                                keyIterator.next();
                                boolean b = false;
                                if (!keyIterator.isValid() || !new String(keyIterator.key()).startsWith(latestKey.substring(1))) {
                                    b = true;
                                } else {
                                    MetaData nextMeta = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                                    if (!nextMeta.key.equals(metaData.key)) {
                                        b = true;
                                    }
                                }
                                if (b) {
                                    deleteLatest = true;
                                    keyIterator.seek((seekKey).getBytes());
                                    keyIterator.prev();
                                    if (keyIterator.isValid() && new String(keyIterator.key()).startsWith(latestKey.substring(1))) {
                                        MetaData meta = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                                        boolean insert = !metaData.versionId.equals(meta.versionId);
                                        if (insert && !meta.deleteMark && !meta.deleteMarker && metaData.key.equals(meta.key) && metaData.bucket.equals(meta.bucket)) {
                                            writeBatch.put(true, latestKey.getBytes(), keyIterator.value());
                                        } else {
                                            writeBatch.delete(latestKey.getBytes());
                                        }
                                        if (snapshotLink != null && insert && metaData.key.equals(meta.key) && metaData.bucket.equals(meta.bucket) && metaData.snapshotMark.equals(meta.snapshotMark) && !meta.deleteMark) {
                                            hasSameNameObject = true;
                                        }
                                    } else {
                                        writeBatch.delete(latestKey.getBytes());
                                    }
                                }
                            }
                        } else {
                            writeBatch.delete(latestKey.getBytes());
                        }
                    }
                    // 桶快照处理
                    if (metaData.deleteMark && snapshotLink != null && deleteLatest && !hasSameNameObject && versionEnabled) {
                        cancelWeakUnView(db, writeBatch, snapshotLink, vnode, metaData);
                    }
                }
            }
            boolean putFlag = true;
            if (oldValue == null) {
                String stampKey = tuple.var1;
                String oldStampKey = tuple.var1;
                if (!versionEnabled) {
                    writeBatch.delete(oldStampKey.getBytes());
                    stampKey = Utils.getMetaDataKey(vnode, metaData.bucket, metaData.key, metaData.versionId, "0", metaData.snapshotMark);
                }
                writeBatch.put(true, stampKey.getBytes(), value[0].getBytes());
                writeBatch.put(tuple.var2.getBytes(), value[0].getBytes());
                writeBatch.put(tuple.var3.getBytes(), new byte[]{0});
                // 如果不一样，表示对象转换成了文件，需要删除旧的对象生命周期键。
                if (!lifeCycleKeyWithOldStamp.equals(tuple.var3)) {
                    writeBatch.delete(lifeCycleKeyWithOldStamp.getBytes());
                }

                if (!metaData.isDeleteMark() && !metaData.isDeleteMarker()) {
                    long capacity = Utils.getObjectSize(metaData);
                    if (metaData.inode > 0) {
                        if (StringUtils.isNotBlank(tmpInode[0])) {
                            Inode curInode = Json.decodeValue(tmpInode[0], Inode.class);
                            if (StringUtils.isBlank(updateCapKeys[0])) {
                                updateCapKeys[0] = FSQuotaUtils.getQuotaKeys(curInode.getBucket(), metaData.key, System.currentTimeMillis(), curInode.getUid(), curInode.getGid());
                            }
                        }
                        capacity = 0;
                    } else {
                        if (StringUtils.isBlank(updateCapKeys[0])) {
                            updateCapKeys[0] = FSQuotaUtils.getQuotaKeys(metaData.getBucket(), metaData.key, System.currentTimeMillis(), 0, 0);
                        }
                    }
                    updateCapacityInfo(writeBatch, metaData.getBucket(), metaData.getKey(), vnode, 1, capacity);
                    FSQuotaUtils.updateAllKeyCap(updateCapKeys[0], writeBatch, metaData.getBucket(), vnode, 1, capacity);
                }
            } else {
                JsonObject currentValue = new JsonObject(new String(oldValue));
                String currentVersion = currentValue.getString("versionNum");
                currentValue.remove("versionNum");
                JsonObject updateValue = new JsonObject(value[0]);
                String updateVersion = updateValue.getString("versionNum");
                updateValue.remove("versionNum");
                if (oldMeta.inode > 0 && metaData.inode > 0 && metaData.inode != oldMeta.inode) {
                    oldInodeMetaKey = Inode.getKey(vnode, oldMeta.getBucket(), oldMeta.inode);
                    byte[] oldMetaInodeBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, oldInodeMetaKey.getBytes());
                    if (oldMetaInodeBytes != null) {
                        oldMetaInode = Json.decodeValue(new String(oldMetaInodeBytes), Inode.class);
                    }
                }
                //判断除versionNum外要更新的元数据内容上是否有变化
                if (currentValue.equals(updateValue)) {
                    //比较新旧元数据中的versionNum
                    if (updateVersion.compareTo(currentVersion) > 0) {
                        String stampKey = tuple.var1;
                        String oldStampKey = tuple.var1;
                        if (!versionEnabled) {
                            writeBatch.delete(oldStampKey.getBytes());
                            stampKey = Utils.getMetaDataKey(vnode, metaData.bucket, metaData.key, metaData.versionId, "0", metaData.snapshotMark);
                        }
                        writeBatch.put(true, stampKey.getBytes(), value[0].getBytes());
                        writeBatch.put(tuple.var2.getBytes(), value[0].getBytes());
                        writeBatch.put(tuple.var3.getBytes(), new byte[]{0});
                        if (!lifeCycleKeyWithOldStamp.equals(tuple.var3)) {
                            writeBatch.delete(lifeCycleKeyWithOldStamp.getBytes());
                        }

                        if (NULL.equals(metaData.versionId) && versionEnabled || versionEnabled && isMultiAliveStarted) {
                            writeBatch.delete(Utils.getMetaDataKey(vnode, metaData.bucket, metaData.key, metaData.versionId, "0", metaData.snapshotMark).getBytes());
                        }
                    } else {
                        putFlag = false;
                    }
                } else {
                    //元数据内容有更新时，比较现在db中的versionNum和原请求的versionNum（非新元数据中的versionNum）
                    if ((currentVersion.compareTo(oldVersion) <= 0 && !oldVersion.equalsIgnoreCase(updateVersion))
                            || (metaData.inode > 0 && updateVersion.compareTo(currentVersion) > 0)) {
                        //更新容量统计
                        if (!metaData.stamp.equals(oldMeta.stamp)) {
                            Tuple3<String, String, String> oldTuple = Utils.getAllMetaDataKey(vnode, oldMeta.bucket, oldMeta.key, oldMeta.versionId, oldMeta.stamp, oldMeta.snapshotMark, oldMeta.inode);
                            writeBatch.delete(oldTuple.var1.getBytes());
                            writeBatch.delete(oldTuple.var3.getBytes());
                        }
                        String stampKey = tuple.var1;
                        String oldStampKey = tuple.var1;
                        if (!versionEnabled) {
                            writeBatch.delete(oldStampKey.getBytes());
                            stampKey = Utils.getMetaDataKey(vnode, metaData.bucket, metaData.key, metaData.versionId, "0", metaData.snapshotMark);
                        }
                        writeBatch.put(true, stampKey.getBytes(), value[0].getBytes());
                        writeBatch.put(tuple.var2.getBytes(), value[0].getBytes());
                        writeBatch.put(tuple.var3.getBytes(), new byte[]{0});
                        if (!lifeCycleKeyWithOldStamp.equals(tuple.var3)) {
                            writeBatch.delete(lifeCycleKeyWithOldStamp.getBytes());
                        }

                        if (NULL.equals(metaData.versionId) && versionEnabled || versionEnabled && isMultiAliveStarted) {
                            writeBatch.delete(Utils.getMetaDataKey(vnode, metaData.bucket, metaData.key, metaData.versionId, "0", metaData.snapshotMark).getBytes());
                        }

                        String bucket = currentValue.containsKey("bucket") ? currentValue.getString("bucket") : currentValue.getString("a");
                        if (StringUtils.isBlank(updateCapKeys[0])) {
                            FSQuotaUtils.getQuotaKeyByMetadata(metaData, oldMeta, updateCapKeys, writeBatch, db, vnode);
                        }
                        if (metaData.deleteMark && !oldMeta.deleteMark) {
                            if (!oldMeta.deleteMarker) {
                                long capacity = Utils.getObjectSize(oldMeta);
                                updateCapacityInfo(writeBatch, bucket, metaData.key, vnode, -1, -capacity);
                                FSQuotaUtils.updateAllKeyCap(updateCapKeys[0], writeBatch, bucket, vnode, -1, -capacity);
                            }
                        } else if (!metaData.deleteMark && oldMeta.deleteMark) {
                            long capacity = metaData.deleteMarker ? 0 : Utils.getObjectSize(metaData);
                            long num = metaData.deleteMarker ? 0 : 1;

                            if (metaData.inode > 0) {
                                capacity = 0;
                            }
                            updateCapacityInfo(writeBatch, bucket, currentValue.getString("key"), vnode, num, capacity);
                            FSQuotaUtils.updateAllKeyCap(updateCapKeys[0], writeBatch, currentValue.getString("bucket"), vnode, num, capacity);
                        } else {
                            long capacity = 0;
                            String currentStorage = currentValue.getString("storage");
                            String updateStorage = updateValue.getString("storage");
                            long oldDataCapacity = 0;
                            long updateDataCapacity = 0;
                            Inode oldInode = null;
                            if (OverWriteHandler.ASYNC_OVER_WRITE && ((currentStorage != null && updateStorage != null && !updateStorage.equals(currentStorage)) || (isAppendableObject(oldMeta) && metaData.inode <= 0))) {
                                oldDataCapacity = oldMeta.deleteMarker ? 0 : Utils.getObjectSize(oldMeta);
                                updateDataCapacity = metaData.deleteMarker ? 0 : Utils.getObjectSize(metaData);
                                capacity = updateDataCapacity - oldDataCapacity;
                            }
                            if (!OverWriteHandler.ASYNC_OVER_WRITE) {
                                oldDataCapacity = oldMeta.deleteMarker ? 0 : Utils.getObjectSize(oldMeta);
                                if (metaData.inode <= 0 && oldMeta.inode > 0) {
                                    String oldInodeKey = getKey(vnode, oldMeta.bucket, oldMeta.inode);
                                    byte[] oldInodeBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, oldInodeKey.getBytes());
                                    if (oldInodeBytes != null) {
                                        oldInode = Json.decodeValue(new String(oldInodeBytes), Inode.class);
                                        if (oldInode.getLinkN() == 1 && (oldInode.getMode() & S_IFMT) != S_IFLNK) {
                                            oldDataCapacity = oldInode.getSize();
                                        }
                                    }
                                }
                                updateDataCapacity = metaData.deleteMarker ? 0 : Utils.getObjectSize(metaData);
                                capacity = updateDataCapacity - oldDataCapacity;
                            }
                            if ((metaData.deleteMark && oldMeta.deleteMark) || metaData.inode > 0) {
                                capacity = 0;
                                updateDataCapacity = 0;
                                oldDataCapacity = 0;
                            }
                            updateCapacityInfo(writeBatch, bucket, metaData.key, vnode, 0, capacity);
                            if (oldInode != null && FSQuotaUtils.isNotRootUserOrGroup(oldInode) && !OverWriteHandler.ASYNC_OVER_WRITE) {
                                FSQuotaUtils.updateCapInfoSyncOverWrite(writeBatch, metaData.getBucket(), metaData.key, vnode, 1, oldDataCapacity, updateDataCapacity, oldInode);
                            } else {
                                updateAllKeyCap(updateCapKeys[0], writeBatch, metaData.getBucket(), vnode, 0, capacity);
                            }
                        }

                        if (isCreateS3Inode && StringUtils.isNotBlank(updateS3InodeCapKeys[0])) {
                            updateAllKeyOnCreateS3Inode(updateS3InodeCapKeys[0], writeBatch, metaData.getBucket(), vnode, 1, s3InodeSize[0]);
                        }

                    } else {
                        //元数据被覆盖
                        res.onNext(DefaultPayload.create(oldMetaInode == null ? "" : Json.encode(oldMetaInode), META_WRITEED.name()));
                        return;
                    }
                }
            }
            // 桶快照处理
            if (snapshotLink != null && !metaData.deleteMark) {
                snapshotLogicalDeleteProcessing(db, writeBatch, snapshotLink, metaData, vnode, versionEnabled);
            }

            long inodeSize = 0;
            boolean updateDeleteInode = StringUtils.isBlank(inode[0]) && metaData.deleteMark && metaData.cookie > 0;
            boolean isUpdateInode = true;
            if (putFlag && metaData.inode > 0 && oldMetaInode != null && oldMetaInode.getNodeId() != metaData.inode && StringUtils.isNotBlank(tmpInode[0])) {
                Inode curInode = Json.decodeValue(tmpInode[0], Inode.class);
                String oldMetaInodeVersionNum = oldMetaInode.getVersionNum();
                String curInodeVersionNum = curInode.getVersionNum();
                if (StringUtils.isNotBlank(oldMetaInodeVersionNum) && oldMetaInodeVersionNum.compareTo(curInodeVersionNum) < 0) {
                    //处理S3上传同名对象，覆盖NFS上传的对象，根据inode.size，以及inodeData中文件名是否一致来进行判断
                    //如果oldInode的size与新的inode 中的size不相等，则不需要删除这个旧的inode，在overwrite的时候会进行删除
                    boolean needDelete = true;
                    if (OverWriteHandler.ASYNC_OVER_WRITE) {
                        if (oldMetaInode.getSize() != curInode.getSize()) {
                            needDelete = false;
                        } else if (!oldMetaInode.getInodeData().equals(curInode.getInodeData())) {
                            needDelete = false;
                        }
                    }
                    if (oldMetaInode.getLinkN() > 1 || !oldMetaInode.getObjName().equals(metaData.key)) {
                        needDelete = false;
                    }
                    if (!OverWriteHandler.ASYNC_OVER_WRITE) {
                        if (needDelete && !oldMetaInode.isDeleteMark()) {
                            writeBatch.delete(oldInodeMetaKey.substring(1).getBytes());
                            writeBatch.delete(oldInodeMetaKey.getBytes());
                            inodeSize += oldMetaInode.getSize();
                        }
                        String cookieKey = Inode.getCookieKey(vnode, oldMeta.bucket, oldMeta.cookie);
                        writeBatch.delete(cookieKey.getBytes());
                    }
                } else {
                    isUpdateInode = false;
                }
            }
            if (putFlag && metaData.inode > 0 && (StringUtils.isNotBlank(inode[0]) || updateDeleteInode)) {
                String inodeKey = Inode.getKey(vnode, metaData.getBucket(), metaData.inode);
                if (updateDeleteInode) {
                    Inode markInode = INODE_DELETE_MARK.clone().setVersionNum(metaData.getVersionNum())
                            .setNodeId(metaData.inode)
                            .setBucket(metaData.bucket)
                            .setObjName(metaData.key)
                            .setCookie(metaData.cookie)
                            .setVersionId(metaData.versionId == null ? "null" : metaData.versionId);
                    byte[] oldInodeBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, inodeKey.getBytes());
                    if (null != oldInodeBytes) {
                        Inode oldInode0 = Json.decodeValue(new String(oldInodeBytes), Inode.class);
                        markInode.setUid(oldInode0.getUid());
                        markInode.setGid(oldInode0.getGid());
                    }
                    inode[0] = Json.encode(markInode);
                }
                Inode curInode = Json.decodeValue(inode[0], Inode.class);
                // 判断当前是否已经存在inode记录，如果存在则获取Inode记录中的版本号，如果该版本号比当前metaData.inode记录中的新就不put
                byte[] oldInodeBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, inodeKey.getBytes());
                if (null != oldInodeBytes) {
                    Inode oldInode = Json.decodeValue(new String(oldInodeBytes), Inode.class);
                    String oldInodeVersionNum = oldInode.getVersionNum();
                    String curInodeVersionNum = curInode.getVersionNum();
                    if (updateDeleteInode) {
                        inodeSize = oldInode.getSize();
                        if ((oldInode.getMode() & S_IFMT) == S_IFLNK) {
                            inodeSize = 0;
                        }
                    }
                    if (StringUtils.isNotBlank(oldInodeVersionNum) && oldInodeVersionNum.compareTo(curInodeVersionNum) >= 0 || !updateDeleteInode) {
                        isUpdateInode = false;
                    }
                }

                // put inode
                if (isUpdateInode) {
                    if (StringUtils.isBlank(updateCapKeys[0])) {
                        FSQuotaUtils.getQuotaKeyByMetadata(metaData, oldMeta, updateCapKeys, writeBatch, db, vnode);
                    }
                    writeBatch.put(inodeKey.getBytes(), inode[0].getBytes());
                    if (inodeSize != 0) {
                        updateCapacityInfo(writeBatch, metaData.bucket, metaData.key, vnode, 0, -inodeSize);
                        FSQuotaUtils.updateAllKeyCap(updateCapKeys[0], writeBatch, metaData.bucket, vnode, 0, -inodeSize);
                    }
                }
            }

            // update cookie
            boolean notUpdateCookie = true;
            if (putFlag && metaData.inode > 0 && metaData.cookie > 0) {
                String cookieKey = Inode.getCookieKey(vnode, metaData.getBucket(), metaData.getCookie());

                // 如果要更新的成的metaData不是deleteMark
                notUpdateCookie = false;
                if (!metaData.isDeleteMark()) {
                    Inode cookieInode = new Inode()
                            .setNodeId(metaData.inode)
                            .setObjName(metaData.key)
                            .setBucket(metaData.bucket)
                            .setCookie(metaData.cookie)
                            .setVersionNum(metaData.versionNum);
                    writeBatch.put(cookieKey.getBytes(), Json.encode(cookieInode).getBytes());
                } else {
                    byte[] cookieBytes = writeBatch.getFromBatchAndDB(db, cookieKey.getBytes());
                    boolean needDeleteMark = true;
                    Inode cookieInode0;
                    if (cookieBytes != null) {
                        cookieInode0 = Json.decodeValue(new String(writeBatch.getFromBatchAndDB(db, cookieKey.getBytes())), Inode.class);
                        if (!metaData.key.equals(cookieInode0.getObjName()) && !cookieInode0.isDeleteMark()) {
                            needDeleteMark = false;
                        }
                    }
                    // 如果要更新成的metaData是deleteMark，则将cookieInode设置为deleteMark
                    if (needDeleteMark) {
                        Inode cookieInode = new Inode()
                                .setNodeId(metaData.inode)
                                .setVersionNum(metaData.versionNum)
                                .setDeleteMark(true)
                                .setObjName(metaData.key)
                                .setBucket(metaData.bucket)
                                .setCookie(metaData.cookie);
                        writeBatch.put(cookieKey.getBytes(), Json.encode(cookieInode).getBytes());
                    }
                }
            }
            //异常情况下,硬链接cookie修复成删除标记
            if (putFlag && notUpdateCookie && metaData.inode > 0 && metaData.deleteMark && oldMeta != null && !oldMeta.deleteMark && oldMeta.cookie > 0) {
                String cookieKey = Inode.getCookieKey(vnode, oldMeta.getBucket(), oldMeta.getCookie());
                // 如果要更新成的metaData是deleteMark，则将cookieInode设置为deleteMark
                byte[] cookieBytes = writeBatch.getFromBatchAndDB(db, cookieKey.getBytes());
                boolean needDeleteMark = true;
                Inode cookieInode0;
                if (cookieBytes != null) {
                    cookieInode0 = Json.decodeValue(new String(writeBatch.getFromBatchAndDB(db, cookieKey.getBytes())), Inode.class);
                    if ((!metaData.key.equals(cookieInode0.getObjName()) || metaData.inode != cookieInode0.getNodeId()) && !cookieInode0.isDeleteMark()) {
                        needDeleteMark = false;
                    }
                }
                if (needDeleteMark) {
                    needDeleteMark = !(metaData.partInfos != null && metaData.partInfos.length == 0 && "inode".equals(metaData.partUploadId));
                }
                if (needDeleteMark) {
                    Inode cookieInode = new Inode()
                            .setNodeId(metaData.inode)
                            .setVersionNum(metaData.versionNum)
                            .setDeleteMark(true)
                            .setObjName(metaData.key)
                            .setBucket(metaData.bucket)
                            .setCookie(oldMeta.cookie);
                    writeBatch.put(cookieKey.getBytes(), Json.encode(cookieInode).getBytes());
                }
            }

            if (!metaData.deleteMark) {
                try (Slice lowerSlice = new Slice(latestKey.substring(1).getBytes());
                     Slice upperSlice = new Slice((latestKey.substring(1) + Utils.ONE_STR).getBytes());
                     ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperSlice).setIterateLowerBound(lowerSlice);
                     RocksIterator iterator = db.newIterator(readOptions);
                     RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)) {
                    String seekKey = tuple.var1;
                    if (!versionEnabled) {
                        seekKey = Utils.getMetaDataKey(vnode, metaData.bucket, metaData.key, metaData.versionId, "0", metaData.snapshotMark);
                    }
                    keyIterator.seek(seekKey.getBytes());
                    boolean b = false;
                    if (!keyIterator.isValid() || !new String(keyIterator.key()).startsWith(latestKey.substring(1))) {
                        b = true;
                    } else {
                        keyIterator.next();
                        if (!keyIterator.isValid() || !new String(keyIterator.key()).startsWith(latestKey.substring(1))) {
                            b = true;
                        } else {
                            MetaData nextMeta = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                            if (!nextMeta.key.equals(metaData.key)) {
                                b = true;
                            }
                        }
                    }
                    if (b) {
                        if (metaData.deleteMarker) {
                            writeBatch.delete(latestKey.getBytes());
                        } else {
                            if (putFlag) {
                                writeBatch.put(true, latestKey.getBytes(), value[0].getBytes());
                            }
                        }
                    }
                }
            }
            res.onNext(SUCCESS_PAYLOAD);
        };
        int hashCode = getFinalHashCode(latestKey, metaData.snapshotMark != null);
        return BatchRocksDB.customizeOperate(lun, hashCode, consumer, res)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    /**
     * 更新重删对象的元数据
     */
    public static Mono<Payload> putDedupMeta(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String key = msg.get("key");
        String[] value = new String[]{msg.get("value")};
        String lun = msg.get("lun");

        String metakey = key.split(ROCKS_FILE_META_PREFIX)[0];

        DedupMeta meta = Json.decodeValue(value[0], DedupMeta.class);
        MonoProcessor<String> res = MonoProcessor.create();
        BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
            DedupMeta oldMeta = null;
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, metakey.getBytes());

            if (oldValue == null) {
                if (meta.fileName != null) {
                    writeBatch.put(metakey.getBytes(), value[0].getBytes());
                    writeBatch.put(key.getBytes(), value[0].getBytes());
                }
            } else {
                oldMeta = Json.decodeValue(new String(oldValue), DedupMeta.class);
                if (StringUtils.isNotEmpty(oldMeta.fileName)) {
                    if (StringUtils.isEmpty(oldMeta.versionNum)) {
                        oldMeta.setVersionNum(VersionUtil.getLastVersionNum(""));
                    }
                    writeBatch.put(key.getBytes(), Json.encode(oldMeta).getBytes());
                } else {
                    if (meta.fileName != null) {
                        writeBatch.put(metakey.getBytes(), value[0].getBytes());
                        writeBatch.put(key.getBytes(), value[0].getBytes());
                    }
                }

            }
            res.onNext(oldValue == null ? "" : new String(oldValue));
        };

        Mono<Boolean> mono;
        if (OverWriteHandler.ASYNC_OVER_WRITE) {
            mono = BatchRocksDB.customizeOperateMetaLowPriority(lun, metakey.hashCode(), consumer);
        } else {
            mono = BatchRocksDB.customizeOperateMeta(lun, metakey.hashCode(), consumer);
        }

        return mono.flatMap(b -> res)
                .map(str -> DefaultPayload.create(str, SUCCESS.name()))
                .doOnError(e -> log.error("", e))
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> delDedupMeta(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);

        String[] fileNames = Json.decodeValue(msg.get("dedupKey"), String[].class);
        String lun = msg.get("lun");

        MonoProcessor<Tuple2<String, Long>> res = MonoProcessor.create();
        String key = fileNames[0];
        String sourceKey = key.split(ROCKS_FILE_META_PREFIX)[0];
        BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
            DedupMeta oldMeta = null;
            try {
                byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, key.getBytes());
                byte[] sourceKeyBytes = sourceKey.getBytes();
                byte[] sourceValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, sourceKeyBytes);


                writeBatch.delete(key.getBytes());
//                log.info("delete  key   {}", key);
                Tuple2<String, Long> resultTuple = new Tuple2<>();

                //判断是否超过2个
                long count = 1;

                if (sourceValue != null) {
                    // sourceKey存在的情况下在进行seek
                    try (RocksIterator iterator = db.newIterator(READ_OPTIONS);
                         RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)
                    ) {
                        keyIterator.seek(sourceKeyBytes);
                        while (keyIterator.isValid() && new String(keyIterator.key()).startsWith(sourceKey)) {

                            if (count >= 2) {
                                break;
                            }

                            if (new String(keyIterator.key()).startsWith(sourceKey + ROCKS_FILE_META_PREFIX)) {
                                count++;
                            }

                            keyIterator.next();
                        }
                    }
                }

                resultTuple.var2 = count;

                if (oldValue == null) {
                    if (sourceValue == null) {
                        resultTuple.var1 = "";
                    } else {
                        resultTuple.var1 = new String(sourceValue);
                    }
                    count++;
                    resultTuple.var2++;
                } else {
                    if (sourceValue == null) {
                        resultTuple.var1 = new String(oldValue);
                    } else {
                        resultTuple.var1 = new String(sourceValue);
                    }
                }

                //如果个数小于2,将原始的也删除
                if (count < 2) {
                    writeBatch.delete(sourceKey.getBytes());
//                    log.info("delete  all key   {}", key);
                }

                res.onNext(resultTuple);
            } catch (Exception e) {
                log.error("delete deduplicate info error", e);
            }
        };

        return BatchRocksDB.customizeOperateMeta(lun, sourceKey.hashCode(), consumer)
                .flatMap(b -> res)
                .map(str -> DefaultPayload.create(Json.encode(str), SUCCESS.name()))
                .doOnError(e -> log.error("", e))
                .onErrorReturn(ERROR_PAYLOAD);
    }


    /**
     * 修复重删引用数据使用
     */
    public static Mono<Payload> updateDedupMeta(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        final String[] value = {msg.get("value")};
        String lun = msg.get("lun");
        String key = msg.get("key");
//    log.info("lun = {},  key = {},  value =  {}", lun, key, value[0]);
        DedupMeta metaData = Json.decodeValue(value[0], DedupMeta.class);
        MonoProcessor<Payload> res = MonoProcessor.create();
        String sourceKey = key.split(ROCKS_FILE_META_PREFIX)[0];

        BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
            DedupMeta oldMeta = null;
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, sourceKey.getBytes());
            if (oldValue == null) {
                writeBatch.put(sourceKey.getBytes(), value[0].getBytes());
                if (!sourceKey.equals(key)) {
                    writeBatch.put(key.getBytes(), value[0].getBytes());
                }
            } else {
                writeBatch.put(sourceKey.getBytes(), value[0].getBytes());
                if (!sourceKey.equals(key)) {
                    writeBatch.put(key.getBytes(), value[0].getBytes());
                }
            }
            res.onNext(SUCCESS_PAYLOAD);
        };

        return BatchRocksDB.customizeOperate(lun, sourceKey.hashCode(), consumer, res)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    /**
     * 更新元数据信息中num值用于copy对象计数
     *
     * @param payload
     * @return
     */
    public static Mono<Payload> updateFileMeta(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String keySuffix = msg.get("key");
        String value = msg.get("value");
        String lun = msg.get("lun");
        boolean replay = Boolean.parseBoolean(msg.get("replay"));
        MetaData metaData = Json.decodeValue(value, MetaData.class);

        MonoProcessor<String> res = MonoProcessor.create();
        BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
            if (metaData.getPartInfos() != null) {
                boolean fileNotExist = false;
                for (PartInfo info : metaData.getPartInfos()) {
                    String oldKey = FileMeta.getKey(info.fileName);
                    writeBatch.copyMigrate(oldKey, keySuffix, value);
                    byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, oldKey.getBytes());
                    if (oldValue == null) {
                        fileNotExist = true;
                        continue;
                    }
                    FileMeta oldFileMeta = Json.decodeValue(new String(oldValue), FileMeta.class);
                    oldFileMeta.setFileName(oldFileMeta.getFileName().split(ROCKS_FILE_META_PREFIX)[0] + keySuffix);
                    // 分段对象的后台校验跳过fileMeta检查
                    if (StringUtils.isNotBlank(oldFileMeta.getMetaKey())) {
                        oldFileMeta.setMetaKey(null);
                    }
                    String oldFileName = info.fileName.split(ROCKS_FILE_META_PREFIX)[0];
                    if (replay && writeBatch.getFromBatchAndDB(db, READ_OPTIONS, (FileMeta.getKey(oldFileName) + keySuffix).getBytes()) != null) {
                        // 迁移 重放copy操作时，如果filemeta已经存在，则不在进行重放操作，避免假copy的filemeta 覆盖迁移过程中真copy的filemeta
                        continue;
                    }
                    writeBatch.put((FileMeta.getKey(oldFileName) + keySuffix).getBytes(), Json.encode(oldFileMeta).getBytes());
                }
                if (fileNotExist) {
                    res.onNext("0");
                } else {
                    res.onNext("1");
                }
            } else if (StringUtils.isNotEmpty(metaData.fileName) && metaData.fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                String oldChunkFileName = metaData.fileName;
                String chunkKey = ChunkFile.getChunkFromFileName(oldChunkFileName).var3;
                String chunkFileStr = msg.get("chunkFile");
                if (chunkFileStr != null) {
                    ChunkFile oldChunk = Json.decodeValue(chunkFileStr, ChunkFile.class);
                    if (!oldChunk.bucket.equals(metaData.bucket)) {
                        chunkKey = chunkKey.replaceFirst(oldChunk.bucket, metaData.bucket);
                        StoragePool pool = StoragePoolFactory.getMetaStoragePool(metaData.bucket);
                        String v = pool.getBucketVnodeId(metaData.bucket);
                        String fileName = chunkKey.substring(chunkKey.split("_")[0].length() + 1);
                        chunkKey = ROCKS_CHUNK_FILE_KEY + v + "_" + fileName;
                    }
                    oldChunk.setObjName(metaData.key);
                    oldChunk.setBucket(metaData.bucket);
                    LinkedList<Inode.InodeData> newChunkList = new LinkedList<>();
                    oldChunk.getChunkList()
                            .forEach(inodeData -> {
                                if (!StringUtils.isEmpty(inodeData.fileName)) {
                                    String oldFileKey = inodeData.fileName.split(ROCKS_FILE_META_PREFIX)[0];
                                    inodeData.setFileName(oldFileKey + keySuffix);
                                }
                                newChunkList.add(inodeData);
                            });
                    oldChunk.setChunkList(newChunkList);
                    chunkKey = chunkKey.split(ROCKS_FILE_META_PREFIX)[0];
                    if (replay && writeBatch.getFromBatchAndDB(db, READ_OPTIONS, (chunkKey + keySuffix).getBytes()) != null) {
                        // 迁移 重放copy操作时，如果filemeta已经存在，则不在进行重放操作，避免假copy的filemeta 覆盖迁移过程中真copy的filemeta
                        res.onNext("1");
                        return;
                    }
                    writeBatch.put((chunkKey + keySuffix).getBytes(), Json.encode(oldChunk).getBytes());
                    res.onNext("1");
                    return;
                } else {
                    res.onNext("0");
                    return;
                }
            } else {
                String oldFileName = metaData.fileName.split(ROCKS_FILE_META_PREFIX)[0];
                if (writeBatch.getFromBatchAndDB(db, READ_OPTIONS, (FileMeta.getKey(oldFileName) + keySuffix).getBytes()) != null) {
                    res.onNext("1");
                    return;
                }
                String oldKey = FileMeta.getKey(metaData.fileName);
                writeBatch.copyMigrate(oldKey, keySuffix, value);
                byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, oldKey.getBytes());

                if (oldValue == null) {
                    try (RocksIterator iterator = db.newIterator(READ_OPTIONS);
                         RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)
                    ) {
                        keyIterator.seek(oldFileName.getBytes());
                        while (keyIterator.isValid() && new String(keyIterator.key()).startsWith(oldFileName)) {
                            if (new String(keyIterator.key()).startsWith(oldFileName + ROCKS_FILE_META_PREFIX)) {
                                oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, keyIterator.key());
                                break;
                            }
                            keyIterator.next();
                        }
                    }
                    if (oldValue == null) {
                        res.onNext("0");
                        return;
                    }
                }
                FileMeta oldFileMeta = Json.decodeValue(new String(oldValue), FileMeta.class);
                oldFileMeta.setFileName(oldFileMeta.getFileName().split(ROCKS_FILE_META_PREFIX)[0] + keySuffix);
                if (!isPartFile(oldFileMeta.getFileName())) {
                    String vnode = StoragePoolFactory.getMetaStoragePool(metaData.getBucket()).getBucketVnodeId(metaData.getBucket(), metaData.getKey());
                    String metaDataKey = Utils.getVersionMetaDataKey(vnode, metaData.getBucket(), metaData.getKey(), metaData.getVersionId(), metaData.snapshotMark);
                    oldFileMeta.setMetaKey(metaDataKey);
                }
                if (replay && writeBatch.getFromBatchAndDB(db, READ_OPTIONS, (FileMeta.getKey(oldFileName) + keySuffix).getBytes()) != null) {
                    // 迁移 重放copy操作时，如果filemeta已经存在，则不在进行重放操作，避免假copy的filemeta 覆盖迁移过程中真copy的filemeta
                    res.onNext("1");
                    return;
                }
                writeBatch.put((FileMeta.getKey(oldFileName) + keySuffix).getBytes(), Json.encode(oldFileMeta).getBytes());
                res.onNext("1");
            }
        };
        return BatchRocksDB.customizeOperateData(lun, consumer, res)
                .map(str -> DefaultPayload.create(str, SUCCESS.name()))
                .doOnError(e -> log.error("", e))
                .onErrorReturn(ERROR_PAYLOAD);
    }

    /**
     * 更新fileMeta中的最后访问时间
     * @param payload
     * @return
     */
    public static Mono<Payload> updateFileAccessTime(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String fileName = msg.get("fileName");
        String stamp = msg.get("stamp");
        String lun = msg.get("lun");

        String accessRecord = getAccessTimeKey(stamp, fileName);

        MonoProcessor<String> res = MonoProcessor.create();
        BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
            //检查指定的fileMeta是否存在
            byte[] oldValue = db.get(FileMeta.getKey(fileName).getBytes());
            if (oldValue == null) {
                res.onNext("1");
                return;
            }

            //首先在当前缓存盘增加访问记录
            //修改增加LastAccessTime

            byte[] valueByte = ZERO_BYTES;
            FileMeta fileMeta = Json.decodeValue(new String(oldValue), FileMeta.class);
            //获取旧的时间戳对应的访问记录key
            if (fileMeta.getLastAccessStamp() != null) {
                String oldRecord = getAccessTimeKey(fileMeta.getLastAccessStamp(), fileName);
                //获取访问记录中保存的数据池前缀
                byte[] prefixByte = writeBatch.getFromBatchAndDB(db, oldRecord.getBytes());
                if (prefixByte != null && new String(prefixByte).startsWith("data")) {
                    valueByte = prefixByte;
                }
                writeBatch.delete(oldRecord.getBytes());
            }

            writeBatch.put(accessRecord.getBytes(), valueByte);
            fileMeta.setLastAccessStamp(stamp);

            writeBatch.put(FileMeta.getKey(fileName).getBytes(), Json.encode(fileMeta).getBytes());
            res.onNext("1");

        };
        return BatchRocksDB.customizeOperateData(lun, consumer, res)
                .map(str -> DefaultPayload.create(str, SUCCESS.name()))
                .doOnError(e -> log.error("", e))
                .onErrorReturn(ERROR_PAYLOAD);
    }

    // 后台校验：copy的自动分段、普通分段的对象的fileMeta不计入metaKey，是为了防止生成了fileMeta但metaData仍长时间没有生成导致的误删
    static boolean isPartFile(String fileName) {
        if (fileName.split("_").length != 6) {
            return false;
        }
        String s = fileName.split("_")[3];
        return s.startsWith("partNum") || s.length() == 32 || "appendObject".equals(s) || "append".equals(s.split(":")[0]);
    }

    public static void updateCapacityInfo(WriteBatch writeBatch, String bucket, String object, String vnode, long num, long capacity) throws RocksDBException {
        if (capacity == 0 && num == 0) {
            return;
        }
        if (StringUtils.isNotEmpty(object)) {
            ObjectSplitTree objectSplitTree = BucketShardCache.getInstance().getCache().get(bucket);
            if (objectSplitTree != null) {
                String bucketVnodeId = objectSplitTree.find(object);
                if (!vnode.equals(bucketVnodeId) && ShardingWorker.SHARDING_LOCK_SET.contains(bucket)) {
                    updateTempCapacityInfo(writeBatch, bucket, vnode, num, capacity);
                    return;
                }
            }
        }
        String capacityKey = BucketInfo.getCpacityKey(vnode, bucket);
        String objNumKey = BucketInfo.getObjNumKey(vnode, bucket);
        writeBatch.merge(objNumKey.getBytes(), toByte(num));
        writeBatch.merge(capacityKey.getBytes(), toByte(capacity));
    }

    public static void updateTempCapacityInfo(WriteBatch writeBatch, String bucket, String vnode, long num, long capacity) throws RocksDBException {
        if (capacity == 0 && num == 0) {
            return;
        }

        String capacityKey = BucketInfo.getTmpCapacityKey(vnode, bucket);
        String objNumKey = BucketInfo.getTmpObjNumKey(vnode, bucket);
        writeBatch.merge(objNumKey.getBytes(), toByte(num));
        writeBatch.merge(capacityKey.getBytes(), toByte(capacity));
    }

    private static long getCapacity(StoragePool metaPool, long metaCapacity) {
        return metaCapacity * (metaPool.getK() + metaPool.getM());
    }

    public static long getCapacity(StoragePool dataPool, StoragePool metaPool, long dataCapacity, long metaCapacity) {
        return metaCapacity * (metaPool.getK() + metaPool.getM()) + dataCapacity * (dataPool.getK() + dataPool.getM());
    }

    /**
     * 更新RocksDB中元数据
     * 元数据不一致，且当前元数据版本大于更新前旧版本，直接返回META_WRITEED
     *
     * @param payload
     * @return
     */
    public static Mono<Payload> updateRocketsValue(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String value = msg.get("value");
        String lun = msg.get("lun");
        String key = msg.get("key");
        String oldVersion = msg.dataMap.getOrDefault("oldVersion", null);
        MonoProcessor<Payload> res = MonoProcessor.create();

        RequestConsumer consumer = (db, writeBatch, request) -> {
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, key.getBytes());

            if (StringUtils.isEmpty(oldVersion) || GetMetaResEnum.GET_ERROR.name().equals(oldVersion)) {
                res.onNext(ERROR_PAYLOAD);
                return;
            }
            if (oldValue == null) {
                if (!GetMetaResEnum.GET_NOT_FOUND.name().equals(oldVersion)) {
                    res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                    return;
                }
                writeBatch.put(key.getBytes(), value.getBytes());
            } else {
                JsonObject currentValue = new JsonObject(new String(oldValue));
                String currentVersion = currentValue.getString("versionNum");
                currentValue.remove("versionNum");
                JsonObject updateValue = new JsonObject(value);
                String updateVersion = updateValue.getString("versionNum");
                updateValue.remove("versionNum");
                boolean delete = currentValue.containsKey("delete") && currentValue.getBoolean("delete");
                if (GetMetaResEnum.GET_NOT_FOUND.name().equals(oldVersion) && (!currentValue.containsKey("delete") || delete)) {
                    res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                    return;
                } else if (!currentVersion.equals(oldVersion)) {
                    res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                    return;
                }
                //判断除versionNum外要更新的元数据内容上是否有变化
                if (currentValue.equals(updateValue)) {
                    //比较新旧元数据中的versionNum
                    if (updateVersion.compareTo(currentVersion) > 0) {
                        writeBatch.put(key.getBytes(), value.getBytes());

                    }
                } else {
                    //元数据内容有更新时，比较现在db中的versionNum和原请求的versionNum（非新元数据中的versionNum）
                    if (currentVersion.compareTo(oldVersion) <= 0) {
                        writeBatch.put(key.getBytes(), value.getBytes());
                    } else {
                        //元数据被覆盖
                        res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                        return;
                    }
                }
            }

            res.onNext(SUCCESS_PAYLOAD);
        };

        return BatchRocksDB.customizeOperate(lun, key.hashCode(), consumer, res)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> updatePartInitValue(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String value = msg.get("value");
        String lun = msg.get("lun");
        String key = msg.get("key");
        InitPartInfo initInfo = Json.decodeValue(value, InitPartInfo.class);
        String lockKey = Utils.getLatestMetaKey(key.split(File.separator)[0].replace(ROCKS_PART_PREFIX, ""), initInfo.bucket, initInfo.object, initInfo.snapshotMark);
        String oldVersion = msg.dataMap.getOrDefault("oldVersion", null);
        MonoProcessor<Payload> res = MonoProcessor.create();

        RequestConsumer consumer = (db, writeBatch, request) -> {
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, key.getBytes());

            if (StringUtils.isEmpty(oldVersion) || GetMetaResEnum.GET_ERROR.name().equals(oldVersion)) {
                res.onNext(ERROR_PAYLOAD);
                return;
            }
            if (oldValue == null) {
                if (!GetMetaResEnum.GET_NOT_FOUND.name().equals(oldVersion)) {
                    res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                    return;
                }
                writeBatch.put(key.getBytes(), value.getBytes());
            } else {
                JsonObject currentValue = new JsonObject(new String(oldValue));
                String currentVersion = currentValue.getString("versionNum");
                currentValue.remove("versionNum");
                JsonObject updateValue = new JsonObject(value);
                String updateVersion = updateValue.getString("versionNum");
                updateValue.remove("versionNum");
                boolean delete = currentValue.containsKey("delete") && currentValue.getBoolean("delete");
                if (GetMetaResEnum.GET_NOT_FOUND.name().equals(oldVersion) && (!currentValue.containsKey("delete") || delete)) {
                    res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                    return;
                } else if (!currentVersion.equals(oldVersion)) {
                    res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                    return;
                }
                //判断除versionNum外要更新的元数据内容上是否有变化
                if (currentValue.equals(updateValue)) {
                    //比较新旧元数据中的versionNum
                    if (updateVersion.compareTo(currentVersion) > 0) {
                        writeBatch.put(key.getBytes(), value.getBytes());

                    }
                } else {
                    //元数据内容有更新时，比较现在db中的versionNum和原请求的versionNum（非新元数据中的versionNum）
                    if (currentVersion.compareTo(oldVersion) <= 0) {
                        writeBatch.put(key.getBytes(), value.getBytes());
                    } else {
                        //元数据被覆盖
                        res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                        return;
                    }
                }
            }

            res.onNext(SUCCESS_PAYLOAD);
        };
        int hashCode = getFinalHashCode(lockKey, initInfo.snapshotMark != null);
        return BatchRocksDB.customizeOperate(lun, hashCode, consumer, res)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    /**
     * rocksDB中写入分钟的统计信息
     */
    public static Mono<Payload> putMinuteRecord(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String key = msg.get("key");
        String lun = msg.get("lun");
        String value = msg.get("value");

        StatisticRecord record = Json.decodeValue(value, StatisticRecord.class);

        return BatchRocksDB.customizeOperateMeta(lun, key.hashCode(), (db, writeBatch, request) -> {
                    long dayTime = record.getRecordTime() / ONE_DAY * ONE_DAY;

                    String dayKey = StatisticRecord.getKey(record.getVnode(), RecordType.DAY, record.getAccountId(),
                            dayTime, record.getRequestType(), record.getBucket());
                    //未合并
                    if (writeBatch.getFromBatchAndDB(db, dayKey.getBytes()) == null) {
                        final byte[] oldValue = writeBatch.getFromBatchAndDB(db, key.getBytes());
                        if (oldValue == null) {
                            writeBatch.put(key.getBytes(), value.getBytes());
                        }
                    }
                })
                .map(b -> SUCCESS_PAYLOAD)
                .doOnError(e -> {
                    if (!PoolHealth.repairLun.contains(CUR_NODE + "@" + lun)) {
                        log.error("put minute record error,key:{}", key, e);
                    }
                })
                .onErrorReturn(ERROR_PAYLOAD);
    }

    /**
     * 获取需要被合并的record
     *
     * @param payload rsocket客户端发来的消息
     * @return 返回本节点查询到的数据
     */
    public static Mono<Payload> getRecordToMerge(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String keyPrefix = msg.get("key");
        long endTime = Long.parseLong(msg.get("value"));
        String lun = msg.get("lun");
        List<StatisticRecord> data = new LinkedList<>();

        try (MSRocksIterator iterator = MSRocksDB.getRocksDB(lun).newIterator()) {
            for (iterator.seek(keyPrefix.getBytes()); iterator.isValid(); iterator.next()) {
                String key = new String(iterator.key());
                if (!key.startsWith(keyPrefix)) {
                    break;
                }
                //查出的统计记录对应的时间
                long recordTime = Long.parseLong(key.split(JOINER)[4]);
                if (recordTime >= endTime) {
                    break;
                }

                byte[] value = iterator.value();
                StatisticRecord record = Json.decodeValue(new String(value), StatisticRecord.class);
                data.add(record);

                if (data.size() > 20000) {
                    break;
                }
            }
        }
        return Mono.just(DefaultPayload.create(Json.encode(data.toArray()), SUCCESS.name()));
    }

    /**
     * 整合统计信息
     */
    public static Mono<Payload> mergeRecord(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        long endTime = Long.parseLong(msg.get("key"));

        Map<String, StatisticRecord> mergeRecords =
                Json.decodeValue(msg.get("value"), new TypeReference<Map<String, StatisticRecord>>() {
                });

        return BatchRocksDB.customizeOperateMeta(lun, lun.hashCode(), (db, writeBatch, request) -> {
                    for (StatisticRecord dayRecord : mergeRecords.values()) {
                        byte[] oldValue = writeBatch.getFromBatchAndDB(db, dayRecord.getKey().getBytes());
                        if (oldValue == null) {
                            writeBatch.put(dayRecord.getKey().getBytes(), Json.encode(dayRecord).getBytes());
                        } else {
                            StatisticRecord old = Json.decodeValue(new String(oldValue), StatisticRecord.class);
                            if (dayRecord.getVersionNum().compareTo(old.getVersionNum()) >= 0) {
                                writeBatch.put(dayRecord.getKey().getBytes(), Json.encode(dayRecord).getBytes());
                            }
                        }
                    }

                    if (mergeRecords.size() > 0) {
                        StatisticRecord dayRecord = mergeRecords.values().iterator().next();
                        try (RocksIterator baseIterator = db.newIterator();
                             RocksIterator iterator = writeBatch.getRocksIterator(baseIterator)) {
                            String keyPrefix = JOINER + dayRecord.getVnode() + JOINER + RecordType.MINUTE.typePrefix
                                    + JOINER + dayRecord.getAccountId() + JOINER;
                            int count = 0;
                            for (iterator.seek(keyPrefix.getBytes()); iterator.isValid(); iterator.next()) {
                                String key = new String(iterator.key());

                                if (!key.startsWith(keyPrefix) || count >= 1000) {
                                    break;
                                }

                                long recordTime = Long.parseLong(key.split(JOINER)[4]);
                                if (recordTime >= endTime) {
                                    break;
                                }
                                count++;

                                writeBatch.delete(iterator.key());
                            }

                        }
                    }

                }).map(b -> SUCCESS_PAYLOAD)
                .doOnError(e -> {
                    log.error("put day record error {}", endTime, e);
                })
                .onErrorReturn(ERROR_PAYLOAD);
    }

    /**
     * 查询指定时间段、指定类型的统计信息
     *
     * @param payload
     * @return
     */
    public static Mono<Payload> getTrafficStatistics(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String maker = msg.get("maker");
        HashMap<String, Object> dataMap = Json.decodeValue(msg.get("value"), new TypeReference<HashMap<String, Object>>() {
        });
        long startStamp = (long) dataMap.get(MGT_STAT_START_STAMP);
        final long endStamp = (long) dataMap.get(MGT_STAT_END_STAMP);
        String requestsType = (String) dataMap.get(MGT_STAT_REQUEST_TYPE);
        String bucket = (String) dataMap.get(MGT_STAT_BUCKET);
        String vNode = (String) dataMap.get("vnode");
        /*
        	给定时间段的起始时间距离当前时间在五分钟到一个小时以内（包含一个小时），可以查询精确到五分钟的数据。
        	给定时间段的起始时间距离当前时间在一小时到一天以内（包含一天），可以查询精确到一小时的数据。
        	给定时间段的起始时间距离当前时间在一天到一个月（包含一个月），可以查询精确到一天的数据。
        	给定时间段的起始时间距离当前时间超过一个月，可以查询精确到一月的数据。
         */
        MSRocksDB rocksDB = MSRocksDB.getRocksDB(lun);
        List<StatisticRecord> records;
        List<StatisticRecord> dayRecord;
        List<StatisticRecord> minuteRecord = null;

        try (MSRocksIterator rocksIterator = rocksDB.newIterator()) {
//            String startKey = JOINER + vNode + JOINER ;
            if (StringUtils.isBlank(maker)) {
                String startKey = JOINER + vNode + JOINER + RecordType.DAY.typePrefix + JOINER + dataMap.get(MGT_STAT_ACCOUNT) + JOINER;
                dayRecord = rocksGetList(rocksIterator, startKey, requestsType, bucket, startStamp, endStamp);
                if (dayRecord.size() < 1000) {
                    startKey = JOINER + vNode + JOINER + RecordType.MINUTE.typePrefix + JOINER + dataMap.get(MGT_STAT_ACCOUNT) + JOINER;
                    minuteRecord = rocksGetList(rocksIterator, startKey, requestsType, bucket, startStamp, endStamp);
                } else {
                    minuteRecord = new ArrayList<>();
                }
//                records = rocksGetList(rocksIterator, startKey, requestsType, bucket, startStamp, endStamp);
            } else {
                if (maker.split(JOINER)[2].equals(RecordType.DAY.typePrefix)) {
                    dayRecord = rocksGetList(rocksIterator, maker, requestsType, bucket, startStamp, endStamp);
                    minuteRecord = new ArrayList<>();
                } else {
                    minuteRecord = rocksGetList(rocksIterator, maker, requestsType, bucket, startStamp, endStamp);
                    dayRecord = new ArrayList<>();
                }
            }
//            String startKey = JOINER + vNode + JOINER + RecordType.DAY.typePrefix + JOINER + dataMap.get(MGT_STAT_ACCOUNT) + JOINER;
//            int count = 0;
//            dayRecord = rocksGetList(rocksIterator, startKey, requestsType, bucket, startStamp, endStamp, count);
//            startKey = JOINER + vNode + JOINER + RecordType.MINUTE.typePrefix + JOINER + dataMap.get(MGT_STAT_ACCOUNT) + JOINER;
//            minuteRecord = rocksGetList(rocksIterator, startKey, requestsType, bucket, startStamp, endStamp, count);
        }
        dayRecord.addAll(minuteRecord);
        return Mono.just(DefaultPayload.create(Json.encode(dayRecord), SUCCESS.name()));
    }

    /**
     * 查询指定开头的key和时间区间的所有StatisticRecord
     *
     * @param iterator
     * @param beginKey
     * @param type
     * @param endStamp
     */
    private static List<StatisticRecord> rocksGetList(MSRocksIterator iterator, String beginKey, String type, String bucket,
                                                      long startStamp, long endStamp) {
        ArrayList<StatisticRecord> statisticRecords = new ArrayList<>();
        String prefix = beginKey;
        if (prefix.split(JOINER).length > 4) {
            prefix = beginKey.split(JOINER)[0] + JOINER + beginKey.split(JOINER)[1] + JOINER + beginKey.split(JOINER)[2] + JOINER + beginKey.split(JOINER)[3] + JOINER;
        }
        for (iterator.seek(beginKey.getBytes()); iterator.isValid(); iterator.next()) {
            String key = new String(iterator.key());
            if (key.equals(beginKey)) {
                continue;
            }
            if (!key.startsWith(prefix)) {
                break;
            }
            //&12323&hour&mkk&20200611000000&READ
            String[] split = key.split(JOINER);
            long parseTime = Long.parseLong(split[4]);
            //如果这条记录的时间小于等于终止时间大于起始时间
            if (parseTime <= endStamp && parseTime > startStamp) {
                String value = new String(iterator.value());
                StatisticRecord statisticRecord = Json.decodeValue(value, StatisticRecord.class);
                if (StringUtils.isNoneBlank(bucket) && !bucket.equals(statisticRecord.getBucket())) {
                    continue;
                }
                switch (type) {
                    case MGT_STAT_REQ_TYPE_TOTAL:
                        statisticRecords.add(statisticRecord);
                        break;
                    case MGT_STAT_REQ_TYPE_READ:
                        if ("READ".equalsIgnoreCase(split[5])) {
                            statisticRecords.add(statisticRecord);
                        }
                        break;
                    case MGT_STAT_REQ_TYPE_WRITE:
                        if ("WRITE".equalsIgnoreCase(split[5])) {
                            statisticRecords.add(statisticRecord);
                        }
                        break;
                }
                if (statisticRecords.size() >= 1000) {
                    break;
                }
            }
        }
        return statisticRecords;
    }

    public static Mono<Payload> hadObject(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String bucket = msg.get("bucket");
        String vnode = msg.get("vnode");
        String snapshotSwitch = msg.get("snapshotSwitch");
        ObjectSplitTree objectSplitTree = StoragePoolFactory.getMetaStoragePool(bucket).getBucketShardCache().get(bucket);
        Tuple2<String, String> upperBoundAndLowerBound = objectSplitTree.queryNodeUpperBoundAndLowerBound(vnode);
        String bucketKey = Utils.getMetaDataKey(vnode, bucket, "", null);
        String nextBucketKey = Utils.getMetaDataKey(vnode, bucket + ROCKS_SMALLEST_KEY, "", null);

        String partInitPrefix = ROCKS_PART_PREFIX + vnode + File.separator + bucket;
        String nextPartInitPrefix = ROCKS_PART_PREFIX + vnode + File.separator + bucket + ROCKS_SMALLEST_KEY;

        String lowerKeyPrefix = bucketKey;
        String upperKeyPrefix = nextBucketKey;
        String lowerPartPrefix = partInitPrefix;
        String upperPartPrefix = nextPartInitPrefix;
        if (upperBoundAndLowerBound != null) {
            if (StringUtils.isNotBlank(upperBoundAndLowerBound.var1)) {
                lowerKeyPrefix = Utils.getMetaDataKey(vnode, bucket, upperBoundAndLowerBound.var1 + ONE_STR, "on".equals(snapshotSwitch) ? MIN_SNAPSHOT_MARK : null);
                lowerPartPrefix = InitPartInfo.getPartKeyPrefix(vnode, bucket, upperBoundAndLowerBound.var1 + ONE_STR, "on".equals(snapshotSwitch) ? MIN_SNAPSHOT_MARK : null);
            }
            if (StringUtils.isNotBlank(upperBoundAndLowerBound.var2)) {
                upperKeyPrefix = Utils.getMetaDataKey(vnode, bucket, upperBoundAndLowerBound.var2 + ONE_STR, "on".equals(snapshotSwitch) ? MAX_SNAPSHOT_MARK : null);
                upperPartPrefix = InitPartInfo.getPartKeyPrefix(vnode, bucket, upperBoundAndLowerBound.var2 + ONE_STR, "on".equals(snapshotSwitch) ? MAX_SNAPSHOT_MARK : null);
            }
        }

        try (Slice lowerSlice = new Slice(lowerKeyPrefix.getBytes());
             Slice upperSlice = new Slice(upperKeyPrefix.getBytes());
             ReadOptions readOptions = new ReadOptions().setIterateLowerBound(lowerSlice).setIterateUpperBound(upperSlice);
             MSRocksIterator iterator = MSRocksDB.getRocksDB(lun).newIterator(readOptions)) {
            iterator.seek(bucketKey.getBytes());
            while (iterator.isValid() && new String(iterator.key()).startsWith(bucketKey)) {
                String value = new String(iterator.value());
                if (!Json.decodeValue(value, MetaData.class).deleteMark) {
                    return Mono.just(SUCCESS_PAYLOAD);
                }
                iterator.next();
            }
        }

        try (Slice lowerSlice = new Slice(lowerPartPrefix.getBytes());
             Slice upperSlice = new Slice(upperPartPrefix.getBytes());
             ReadOptions readOptions = new ReadOptions().setIterateLowerBound(lowerSlice).setIterateUpperBound(upperSlice);
             MSRocksIterator iterator = MSRocksDB.getRocksDB(lun).newIterator(readOptions)) {
            iterator.seek(partInitPrefix.getBytes());
            while (iterator.isValid() && new String(iterator.key()).startsWith(partInitPrefix)) {
                String value = new String(iterator.value());
                if (!Json.decodeValue(value, InitPartInfo.class).delete) {
                    return Mono.just(SUCCESS_PAYLOAD);
                }
                iterator.next();
            }
        }

        return Mono.just(NOT_FOUND_PAYLOAD);
    }

    public static Mono<Payload> completeMultiPart(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String vnode = msg.get("vnode");
        String lun = msg.get("lun");
        String initValue = msg.get("initPartInfo");
        String status = msg.get("status");
        String snapshotLink = msg.get("snapshotLink");
        boolean versionEnabled = "Enabled".equals(status) || "Suspended".equals(status);
        InitPartInfo initPartInfo = Json.decodeValue(initValue, InitPartInfo.class);
        String bucket = initPartInfo.bucket;
        String object = initPartInfo.object;
        String uploadId = initPartInfo.uploadId;
        MetaData metaData = initPartInfo.metaData;
        String latestKey = Utils.getLatestMetaKey(vnode, bucket, object, metaData.snapshotMark);
        byte[][] value = new byte[1][];
        value[0] = Json.encode(metaData).getBytes();
        String versionId = metaData.versionId;
        Set<String> partFileNames = new HashSet<>();
        for (PartInfo partInfo : initPartInfo.metaData.partInfos) {
            partFileNames.add(partInfo.fileName);
        }

        LinkedList<String> extraPartFileNames = new LinkedList<>();
        MonoProcessor<String> res = MonoProcessor.create();
        String[] updateCapKeys = new String[1];
//        if (StringUtils.isNotBlank(metaData.tmpInodeStr)) {
//            Inode tmpInode = Json.decodeValue(metaData.tmpInodeStr, Inode.class);
//            updateCapKeys[0] = tmpInode.getXAttrMap().get(QUOTA_KEY);
//        }
        updateCapKeys[0] = FSQuotaUtils.getQuotaKeys(metaData.getBucket(), metaData.key, System.currentTimeMillis(), 0, 0);
        RequestConsumer consumer = (db, writeBatch, request) -> {
            List<String> overWriteFileName = new LinkedList<>();
            // 旧对象数据块所在存储池
            String overWriteStorage = metaData.storage;
            long deltaCapacity = 0;
            byte[] bytes = uploadId.getBytes();
            bytes[bytes.length - 1] += 1;
            String upperUploadId = new String(bytes);
            try (Slice lowerSlice = new Slice(PartInfo.getPartKey(vnode, bucket, object, uploadId, null, metaData.snapshotMark).getBytes());
                 Slice upperSlice = new Slice(PartInfo.getPartKey(vnode, bucket, object, upperUploadId, null, metaData.snapshotMark).getBytes());
                 ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperSlice).setIterateLowerBound(lowerSlice);
                 RocksIterator iterator = db.newIterator(readOptions);
                 RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)) {
                int startNum = 1;
                String partKey = PartInfo.getPartKey(vnode, bucket, object, uploadId, "", metaData.snapshotMark);
                keyIterator.seek(partKey.getBytes());

                //遍历rocksDB中相关的partInfo记录
                //partInfo记录的fileName不包含在initPartInfo中，要删除。这些是要被覆盖的同名对象的分段。
                while (keyIterator.isValid() && new String(keyIterator.key()).startsWith(partKey)) {
                    PartInfo partInfo = Json.decodeValue(new String(keyIterator.value()), PartInfo.class);

                    //找出多余的分段文件
                    if (!partFileNames.contains(partInfo.fileName)) {
                        if (StringUtils.isNotEmpty(partInfo.deduplicateKey)) {
                            extraPartFileNames.add(partInfo.deduplicateKey);
                        } else if (partInfo.fileName != null) {
                            extraPartFileNames.add(partInfo.fileName);
                        }
                    }
                    keyIterator.next();
                }
            }

            //更新元数据
            Tuple3<String, String, String> tuple = Utils.getAllMetaDataKey(vnode, metaData.bucket, metaData.key, metaData.versionId, metaData.stamp, metaData.snapshotMark, metaData.inode);
            if (!versionEnabled) {
                tuple.var1 = Utils.getMetaDataKey(vnode, bucket, object, versionId, "0", metaData.snapshotMark);
            }
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, tuple.var2.getBytes());
            boolean isDeleteMark = false;
            MetaData newMeta = metaData.clone();
            long fsQuotaCapacity = newMeta.endIndex - newMeta.startIndex + 1;
            Inode oldTmpInode = null;
            long oldCap = 0;
            long newCap = 0;
            if (null != oldValue) {
                MetaData oldMeta = Json.decodeValue(new String(oldValue), MetaData.class);
                //【文件】
                if (oldMeta.inode > 0) {
                    String inodeKey = Inode.getKey(vnode, oldMeta.bucket, oldMeta.inode);
                    byte[] inodeBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, inodeKey.getBytes());
                    if (inodeBytes != null) {
                        Inode oldInode = Json.decodeValue(new String(inodeBytes), Inode.class);
                        oldTmpInode = oldInode.clone();
                        if (oldInode.getLinkN() <= 1) {
                            Inode deleteMarkInode = Inode.INODE_DELETE_MARK.clone();
                            deleteMarkInode.setGid(oldInode.getGid());
                            deleteMarkInode.setUid(oldInode.getUid());
                            deleteMarkInode.setNodeId(oldInode.getNodeId());
                            deleteMarkInode.setBucket(oldInode.getBucket());
                            deleteMarkInode.setObjName(oldMeta.key);
                            deleteMarkInode.setVersionNum(metaData.versionNum + "0");

                            if (metaData.inode > 0 && StringUtils.isNotEmpty(metaData.tmpInodeStr)) {
                                Inode inode = Json.decodeValue(metaData.tmpInodeStr, Inode.class);
                                if (!(metaData.inode == oldMeta.inode
                                        && oldInode.getVersionNum().equals(inode.getVersionNum()))) {
                                    Inode.mergeMeta(oldMeta, oldInode);
                                    oldMeta.tmpInodeStr = null;
                                    writeBatch.put(inodeKey.getBytes(), Json.encode(deleteMarkInode).getBytes());
                                }
                            } else {
                                Inode.mergeMeta(oldMeta, oldInode);
                                oldMeta.tmpInodeStr = null;
                                writeBatch.put(inodeKey.getBytes(), Json.encode(deleteMarkInode).getBytes());
                            }
                        } else {
                            //新版本执行
                            if (newMeta.inode == 0) {
                                int linkN = oldInode.getLinkN();
                                oldInode.setLinkN(linkN - 1);
                                writeBatch.delete(inodeKey.getBytes());
                                writeBatch.put(inodeKey.getBytes(), Json.encode(oldInode).getBytes());
                            }

                            //旧版本执行
                            if (newMeta.inode > 0 && StringUtils.isNotEmpty(newMeta.tmpInodeStr)) {
                                Inode inode = Json.decodeValue(newMeta.tmpInodeStr, Inode.class);
                                if (inode.getLinkN() > 1 && (inode.getSize() > 0 && inode.getInodeData().size() > 0)) {
                                    String newInodeKey = Inode.getKey(vnode, newMeta.bucket, newMeta.inode);
                                    writeBatch.put(newInodeKey.getBytes(), newMeta.tmpInodeStr.getBytes());
                                }
                                Inode.mergeMeta(oldMeta, oldInode);
                                Inode.reduceMeta(newMeta);
                                newMeta.tmpInodeStr = null;
                                value[0] = Json.encode(newMeta).getBytes();
                                oldMeta.tmpInodeStr = null;
                            }
                        }
                    }
                } else {
                    newMeta.tmpInodeStr = null;
                    value[0] = Json.encode(newMeta).getBytes();
                }
                isDeleteMark = oldMeta.deleteMark || oldMeta.deleteMarker;

                // 要更新的metaData，syncStamp有两个-，说明开了双活，判断先后以syncStamp为准而非versionNum
                if (DAVersionUtils.countHyphen(metaData.syncStamp) == 2) {
                    // 旧的partInfo syncStamp更大，不需要更新partInfo。
                    // 旧版本的syncStamp必定小于新版本。
                    if (!oldMeta.deleteMark && oldMeta.syncStamp.compareTo(metaData.syncStamp) >= 0) {
                        String[] overWrite = overWriteFileName.toArray(new String[0]);
                        String[] extraPart = extraPartFileNames.toArray(new String[0]);
                        res.onNext(Json.encode(new Tuple2<>(overWrite, extraPart)));
                        return;
                    }
                } else {
                    // 旧metadata的versionNum更大，此次partMerge不执行，只是去删除要覆盖的file
                    if (!oldMeta.deleteMark && oldMeta.getVersionNum().compareTo(metaData.getVersionNum()) >= 0) {
                        String[] overWrite = overWriteFileName.toArray(new String[0]);
                        String[] extraPart = extraPartFileNames.toArray(new String[0]);
                        res.onNext(Json.encode(new Tuple2<>(overWrite, extraPart)));
                        return;
                    }
                }
                writeBatch.put(tuple.var2.getBytes(), value[0]);
                deltaCapacity = deltaCapacity - (oldMeta.deleteMarker || oldMeta.deleteMark ? 0 : Utils.getObjectSize(oldMeta));
                if (oldTmpInode != null) {
                    if (oldTmpInode.getLinkN() == 1) {
                        oldCap = oldTmpInode.getSize();
                    }
                } else {
                    oldCap = (oldMeta.deleteMarker || oldMeta.deleteMark ? 0 : oldMeta.endIndex - oldMeta.startIndex + 1);
                }

                newCap = newMeta.endIndex - newMeta.startIndex + 1;
                writeBatch.put(true, tuple.var1.getBytes(), value[0]);
                writeBatch.put(tuple.var3.getBytes(), new byte[]{0});

                // 删除已有的元数据和fileMeta
                //  新的metadata.stamp有变化，将旧的metadata全部删除
                if (!metaData.stamp.equals(oldMeta.stamp) || metaData.versionNum.compareTo(oldMeta.versionNum) > 0) {
                    Tuple3<String, String, String> oldTuple = Utils.getAllMetaDataKey(vnode, bucket, object, versionId, oldMeta.stamp, oldMeta.snapshotMark, oldMeta.inode);
                    writeBatch.delete(oldTuple.var1.getBytes());
                    if (NULL.equals(oldMeta.versionId) && versionEnabled || versionEnabled && isMultiAliveStarted) {
                        writeBatch.delete(Utils.getMetaDataKey(vnode, bucket, object, versionId, "0", oldMeta.snapshotMark).getBytes());
                    }
                    writeBatch.delete(oldTuple.var3.getBytes());
                }

                //覆盖为分段对象后要将原文件删除，原文件有一般对象和分段对象两种情况
                if (oldMeta.partUploadId == null) {
                    if (!oldMeta.deleteMark && !oldMeta.deleteMarker) {
                        if (StringUtils.isNotEmpty(oldMeta.aggregationKey)) {
                            overWriteFileName.add(oldMeta.aggregationKey);
                        } else if (StringUtils.isNotEmpty(oldMeta.duplicateKey)) {
                            overWriteFileName.add(oldMeta.duplicateKey);
                        } else {
                            overWriteFileName.add(oldMeta.fileName);
                        }
                    }
                } else if (!oldMeta.partUploadId.equals(metaData.partUploadId)) {
                    for (PartInfo partInfo : oldMeta.getPartInfos()) {
                        if (StringUtils.isNotEmpty(partInfo.deduplicateKey)) {
                            overWriteFileName.add(partInfo.deduplicateKey);
                        } else {
                            overWriteFileName.add(partInfo.fileName);
                        }
                    }
                }
                // 存在多个存储池时，被覆盖的数据块与当前数据块可能不在同一存储池中
                overWriteStorage = oldMeta.storage;
                fsQuotaCapacity = fsQuotaCapacity - oldCap;
            } else {
                newMeta.tmpInodeStr = null;
                value[0] = Json.encode(newMeta).getBytes();
                writeBatch.put(true, tuple.var1.getBytes(), value[0]);
                writeBatch.put(tuple.var2.getBytes(), value[0]);
                writeBatch.put(tuple.var3.getBytes(), new byte[]{0});
            }

            // 更新latestKey
            try (Slice lowerSlice = new Slice(latestKey.substring(1).getBytes());
                 Slice upperSlice = new Slice((latestKey.substring(1) + Utils.ONE_STR).getBytes());
                 ReadOptions readOptions = new ReadOptions().setIterateUpperBound(upperSlice).setIterateLowerBound(lowerSlice);
                 RocksIterator iterator = db.newIterator(readOptions);
                 RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)) {
                keyIterator.seek((tuple.var1).getBytes());
                if (!keyIterator.isValid() || !new String(keyIterator.key()).startsWith(latestKey.substring(1))) {
                    writeBatch.put(true, latestKey.getBytes(), value[0]);
                } else {
                    keyIterator.next();
                    if (!keyIterator.isValid() || !new String(keyIterator.key()).startsWith(latestKey.substring(1))) {
                        writeBatch.put(true, latestKey.getBytes(), value[0]);
                    } else {
                        MetaData next = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                        if (!metaData.key.equals(next.key) || !metaData.bucket.equals(next.bucket)) {
                            writeBatch.put(true, latestKey.getBytes(), value[0]);
                        }
                    }
                }
            }
            // 桶快照处理
            if (snapshotLink != null) {
                snapshotLogicalDeleteProcessing(db, writeBatch, snapshotLink, newMeta, vnode, versionEnabled);
            }

            //更新InitPartInfo
            String initPartKey = initPartInfo.getPartKey(vnode);
            if (initPartInfo.delete) {
                writeBatch.put(initPartKey.getBytes(), Json.encode(initPartInfo).getBytes());
            }
            String[] overWrite = overWriteFileName.toArray(new String[0]);
            String[] extraPart = extraPartFileNames.toArray(new String[0]);
            updateCapacityInfo(writeBatch, metaData.getBucket(), metaData.getKey(), vnode, (oldValue == null || isDeleteMark) ? 1 : 0, deltaCapacity);
            if (oldTmpInode != null && FSQuotaUtils.isNotRootUserOrGroup(oldTmpInode)) {
                FSQuotaUtils.updateCapInfoSyncOverWrite(writeBatch, bucket, object, vnode, 1, oldCap, newCap, oldTmpInode);
            } else {
                updateAllKeyCap(updateCapKeys[0], writeBatch, metaData.getBucket(), vnode, (oldValue == null || isDeleteMark) ? 1 : 0, fsQuotaCapacity);
            }

            res.onNext(Json.encode(new Tuple3<>(overWrite, extraPart, overWriteStorage)));
        };
        int hashCode = getFinalHashCode(latestKey, initPartInfo.snapshotMark != null);
        return BatchRocksDB.customizeOperate(lun, hashCode, consumer, res)
                .map(str -> DefaultPayload.create(str, SUCCESS.name()))
                .onErrorReturn(ERROR_PAYLOAD);
    }


    public static Mono<Payload> checkCompleteMultiPart(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String bucket = msg.get("bucket");
        String object = msg.get("object");
        String uploadId = msg.get("uploadId");
        String vnode = msg.get("vnode");
        String currentSnapshotMark = msg.get("currentSnapshotMark");
        String snapshotLink = msg.get("snapshotLink");

        Function<String, List<PartInfo>> checkCompleteMultiPartFunction = (snapshotMark) -> {
            String partKey = PartInfo.getPartKey(vnode, bucket, object, uploadId, "", snapshotMark);
            List<PartInfo> list;
            try (MSRocksIterator keyIterator = MSRocksDB.getRocksDB(lun).newIterator()) {
                keyIterator.seek(partKey.getBytes());
                list = new LinkedList<>();

                while (keyIterator.isValid() && (new String(keyIterator.key())).startsWith(partKey)) {
                    PartInfo partInfo = Json.decodeValue(new String(keyIterator.value()), PartInfo.class);
                    list.add(partInfo);
                    keyIterator.next();
                }
            }

            return list;
        };
        SnapshotMergeService<PartInfo> snapshotListPartInfoService = new SnapshotListPartInfoService();
        List<PartInfo> list = executeListOperation(checkCompleteMultiPartFunction, snapshotListPartInfoService, currentSnapshotMark, snapshotLink, 1000);
        PartInfo[] partInfos = list.toArray(new PartInfo[0]);

        return Mono.just(DefaultPayload.create(Json.encode(partInfos), SUCCESS.name()));
    }

    public static Mono<Payload> hasPartInfoOrMeta(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String metaKey = msg.get("metaKey");
        String initPartInfoKey = msg.get("initPartInfoKey");
        String partInfoKey = msg.get("partInfoKey");
        String lun = msg.get("lun");
        String fileName = msg.get("fileName");
        String storage = msg.get("storage");
        String updateEC = msg.get("updateEC");
        String partNumStr = msg.get("partNum");
        int partNum = Integer.parseInt(partNumStr);

        try {
            //initPartInfo无删除标记，表示还未被覆盖
            //metaData含任意非删除标记记录，表示已merge
            byte[] metaBytes = MSRocksDB.getRocksDB(lun).get(metaKey.getBytes());
            if (metaBytes != null) {
                MetaData metaData = Json.decodeValue(new String(metaBytes), MetaData.class);
                if (!metaData.deleteMark && metaData.partInfos != null) {
                    if (metaData.partInfos.length >= partNum) {
                        //由于可能已被同名对象覆盖，metadata中对应的partInfo.fileName要匹配
                        PartInfo partInfoInMeta = metaData.partInfos[partNum - 1];
                        if (fileName.equals(partInfoInMeta.fileName)) {
                            return Mono.just(SUCCESS_PAYLOAD);
                        } else if (!storage.equals(partInfoInMeta.storage) && StringUtils.isNotEmpty(updateEC) && updateEC.equals(partInfoInMeta.storage)) {
                            return Mono.just(SUCCESS_PAYLOAD);
                        }
                    }
                }
            }

            //partInfo含有记录:1.fileName不同，表示同样的uploadId已有对象覆盖，不修复；
            //                 2.fileName相同但是initPartInfo.delete为true ,表示分段已合并过，
            //                   但是由于之前metadata检测未通过，表示已经被同名对象覆盖，不修复。
            byte[] partInfoBytes = MSRocksDB.getRocksDB(lun).get(partInfoKey.getBytes());
            byte[] initPartInfoBytes = MSRocksDB.getRocksDB(lun).get(initPartInfoKey.getBytes());
            if (partInfoBytes != null) {
                PartInfo partInfo = Json.decodeValue(new String(partInfoBytes), PartInfo.class);
                if (fileName.equals(partInfo.fileName)) {
                    // 由于1986，可能多出partInfo但initPart已无。
                    if (initPartInfoBytes == null) {
                        return Mono.just(NOT_FOUND_PAYLOAD);
                    }

                    InitPartInfo initPartInfo = Json.decodeValue(new String(initPartInfoBytes), InitPartInfo.class);
                    //delete为false表示该分段对象并未合并，需要修复。
                    if (!initPartInfo.delete) {
                        return Mono.just(SUCCESS_PAYLOAD);
                    }
                }
            }
        } catch (RocksDBException e) {
            log.error("", e);
            return Mono.just(ERROR_PAYLOAD);
        }

        return Mono.just(NOT_FOUND_PAYLOAD);
    }


    public static Mono<Payload> deleteRocketsValue(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");

        try {
            MSRocksDB.getRocksDB(lun).delete(key.getBytes());
            return Mono.just(SUCCESS_PAYLOAD);
        } catch (Exception e) {
            log.error("delete rockets value error,key{},{}", key, e);
            return Mono.just(ERROR_PAYLOAD);
        }

    }

    public static Mono<Payload> migrateMergeRocks(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String value = msg.get("value");

        try {
            MSRocksDB db = getRocksDB(lun);
            if (db == null) {
                log.error("migrate merge value error get db is null,key{},lun:{}", key, lun);
                return Mono.just(ERROR_PAYLOAD);
            }
            db.merge(key.getBytes(), Json.decodeValue(value, byte[].class));
            return Mono.just(SUCCESS_PAYLOAD);
        } catch (Exception e) {
            log.error("migrate merge value error,key{}", key, e);
            return Mono.just(ERROR_PAYLOAD);
        }
    }

    private static final Map<String, SstFileWriter> SST_FILE_CACHE = new ConcurrentHashMap<>();
    private static final EnvOptions SST_ENV_OPTIONS = new EnvOptions();
    private static final MergeOperator SST_MERGE_OPERATOR = new MossMergeOperator();
    private static final Options SST_OPTIONS = new Options().setMergeOperator(SST_MERGE_OPERATOR).setCreateIfMissing(true);
    private static final IngestExternalFileOptions INGEST_EXTERNAL_FILE_OPTIONS = new IngestExternalFileOptions();
    private static final TypeReference<List<Tuple2<byte[], byte[]>>> SST_TYPE_REFERENCE = new TypeReference<List<Tuple2<byte[], byte[]>>>() {
    };

    private static SstFileWriter getSstWriter(String lun, String vnode, String fileName, int writeType, boolean isFirst) throws Exception {
        String sstCacheKey = getSstCacheKey(lun, vnode, fileName, writeType);
        SstFileWriter writer = SST_FILE_CACHE.get(sstCacheKey);
        String sstFilePath = getWriteSstFilePath(lun, vnode, fileName, writeType);
        if (isFirst && writer != null) {
            log.info("close {}, rewrite", sstCacheKey);
            writer.finish();
            writer.close();
            Files.delete(Paths.get(sstFilePath));
            SST_FILE_CACHE.remove(sstCacheKey);
        }
        if (!SST_FILE_CACHE.containsKey(sstCacheKey)) {
            writer = new SstFileWriter(SST_ENV_OPTIONS, SST_OPTIONS);
            writer.open(sstFilePath);
            SST_FILE_CACHE.put(sstCacheKey, writer);
        }
        return writer;
    }

    private static final Map<String, UnicastProcessor<Tuple3<String, String, Tuple2<MonoProcessor<Boolean>, Boolean>>>> VNODE_INGEST_PROCESSOR_MAP = new ConcurrentHashMap<>();
    private static final Map<String, String> VNODE_MSG_ID_MAP = new ConcurrentHashMap<>();

    public static void registerIngestFileProcessor(String lun, String vnode, String migrateSstList) {
        List<String> list = Json.decodeValue(migrateSstList, new TypeReference<List<String>>() {
        });
        List<String> list1 = new ArrayList<>();
        for (String fileName : list) {
            //存储元数据的文件
            list1.add("1-" + fileName);
            //存储删除标记的文件
            list1.add("0-" + fileName);
        }
        log.info("{} migrate sst list:{}", vnode, list);
        //用于接收这个vnode写完的sst文件，并按照顺序插入到rocksdb中
        UnicastProcessor<Tuple3<String, String, Tuple2<MonoProcessor<Boolean>, Boolean>>> processor =
                UnicastProcessor.create(Queues.<Tuple3<String, String, Tuple2<MonoProcessor<Boolean>, Boolean>>>unboundedMultiproducer().get());
        final int[] currIndex = {0};
        int maxIndex = list1.size();
        Map<Integer, String> indexAndNameMap = new ConcurrentHashMap<>();
        processor.publishOn(ADD_NODE_SCHEDULER)
                .doOnComplete(() -> {
                    VNODE_INGEST_PROCESSOR_MAP.remove(lun + '-' + vnode);
                    VNODE_MSG_ID_MAP.keySet().removeIf(k -> k.startsWith(lun + '-' + vnode + '-'));
                })
                .subscribe(tuple3 -> {
                    MonoProcessor<Boolean> res = tuple3.var3.var1;
                    try {
                        String fileName = tuple3.var1;
                        String filePath = tuple3.var2;
                        boolean needIngest = tuple3.var3.var2;
                        int index = list1.indexOf(fileName);

                        //需要按照list中的顺序插入文件，若这个sst文件的索引和待写入的不同，需要暂时将写完的sst名暂存在map中，等待指定索引的sst文件先写
                        if (index == currIndex[0]) {
                            if (needIngest) {
                                MSRocksDB.getRocksDB(lun).ingestExternalFile(Collections.singletonList(filePath), INGEST_EXTERNAL_FILE_OPTIONS);
                                Files.delete(Paths.get(filePath));
                            }
                            currIndex[0]++;
                            while (indexAndNameMap.containsKey(currIndex[0])) {
                                String removeFileName = indexAndNameMap.remove(currIndex[0]);
                                //缓存中的sst文件名为"",代表不需要插入此sst文件
                                if (!"".equals(removeFileName)) {
                                    MSRocksDB.getRocksDB(lun).ingestExternalFile(Collections.singletonList(removeFileName), INGEST_EXTERNAL_FILE_OPTIONS);
                                    Files.delete(Paths.get(removeFileName));
                                }
                                currIndex[0]++;
                            }
                        } else {
                            if (!needIngest) {
                                filePath = "";
                            }
                            indexAndNameMap.put(index, filePath);
                        }
                        if (currIndex[0] == maxIndex) {
                            processor.onComplete();
                        }
                    } catch (Exception e) {
                        log.error("", e);
                    } finally {
                        //res不为null，代表一个文件写入完成，此时需要等待文件成功插入rocksdb后才能返回成功响应
                        if (res != null) {
                            res.onNext(true);
                        }
                    }
                });
        VNODE_INGEST_PROCESSOR_MAP.put(lun + '-' + vnode, processor);
    }

    public static Mono<Payload> migratePutRocks(Payload payload) throws RocksDBException {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String value = msg.get("value");
        String dataType = msg.get("dataType");
        String migrateCopy = msg.get("migrateCopy");

        //dataType不为null，代表处理的是加节点迁移
        if (dataType != null) {
            String vnode = msg.get("objVnode");

            //待写入的元数据
            String resList = msg.get("metaList");

            //当前vnode可能需要迁移的所有sst文件名,在开始写vnode下的sst文件前，先初始化部分信息
            String migrateSstFileList = msg.get("sstFileList");
            if (migrateSstFileList != null) {
                registerIngestFileProcessor(lun, vnode, migrateSstFileList);
                return Mono.just(SUCCESS_PAYLOAD);
            }

            List<Tuple2<byte[], byte[]>> list = Json.decodeValue(resList, SST_TYPE_REFERENCE);
            if ("new".equals(dataType)) {
                RequestConsumer consumer = (db, writeBatch, request) -> list.forEach(tuple2 -> {
                    try {
                        byte[] k = tuple2.var1;
                        byte[] v = tuple2.var2;
                        //删除
                        if (v[0] == ROCKS_OBJ_META_DELETE_MARKER.getBytes()[0]) {
                            byte[] curV = writeBatch.getFromBatchAndDB(db, k);
                            if (Arrays.equals(Arrays.copyOfRange(v, 1, v.length), new byte[]{0})) {
                                if (Arrays.equals(curV, Arrays.copyOfRange(v, 1, v.length))) {
                                    writeBatch.delete(k);
                                }
                            } else {
                                //log.info("!!!key:{}, ======curV:{}, newV:{}", new String(k), new String(curV), new String(v));
                                JsonObject o = new JsonObject(new String(Arrays.copyOfRange(v, 1, v.length)));
                                if (Arrays.equals(curV, Arrays.copyOfRange(v, 1, v.length))) {
                                    writeBatch.delete(k);
                                } else if (null != o.getBoolean("deleteMark") && o.getBoolean("deleteMark")) {//新值为删除删除标记，那么直接删除目标盘的key
                                    writeBatch.delete(k);
                                }
                            }
                        } else {
                            boolean needPut = true;
                            if (new String(k).startsWith(ROCKS_AGGREGATION_RATE_PREFIX)) {
                                byte[] bytes = writeBatch.getFromBatchAndDB(db, k);
                                if (bytes != null) {
                                    BitSet current = BitSet.valueOf(bytes);
                                    BitSet bitSet = AggregationUtils.deserialize(new String(v));
                                    bitSet.and(current);
                                    writeBatch.put(k, bitSet.toByteArray());
                                }
                                needPut = false;
                            } else if (!new String(k).startsWith(ROCKS_LIFE_CYCLE_PREFIX)) {
                                JsonObject o = new JsonObject(new String(v));
                                String versionNum = o.getString("versionNum");
                                if (versionNum != null) {
                                    try {
                                        byte[] oldValue = MSRocksDB.getRocksDB(lun).get(k);
                                        if (null != oldValue) {
                                            o = new JsonObject(new String(oldValue));
                                            if (versionNum.compareTo(o.getString("versionNum")) <= 0) {
                                                needPut = false;
                                            }
                                        }
                                    } catch (Exception e) {
                                        log.error(e.getMessage());
                                    }
                                }
                            }
                            if (needPut) {
                                writeBatch.put(k, v);
                            }
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                });
                return BatchRocksDB.customizeOperateMeta(lun, 0, consumer)
                        .map(b -> SUCCESS_PAYLOAD)
//                        .doOnError(e -> log.error("", e))
                        .onErrorReturn(ERROR_PAYLOAD);
            }

            String deletionListStr = msg.get("deletionList");

            String fileName = msg.get("sstFileName");
            checkSstParentPath(lun);
            try {
                boolean isFirst = Boolean.parseBoolean(msg.get("isFirst"));

                String index = msg.dataMap.getOrDefault("index", "-1");
                String retry = msg.dataMap.getOrDefault("retry", "0");
                String curMsgId = msg.dataMap.getOrDefault("msgId", "-1");
                String msgKey = lun + '-' + vnode + '-' + index;

                if (!"0".equals(retry)) {
                    String lastMsgId = VNODE_MSG_ID_MAP.getOrDefault(msgKey, "-2");
                    if (lastMsgId.equals(curMsgId)) {
                        //已经处理过的消息不能重复处理
                        return Mono.just(DefaultPayload.create(ERROR_PAYLOAD));
                    }
                }

                //writeType用于区分put(1)和delete(0)，put用于记录元数据，delete用于记录删除标记
                String putSstFilePath = getWriteSstFilePath(lun, vnode, fileName, 1);
                String deleteSstFilePath = getWriteSstFilePath(lun, vnode, fileName, 0);
                if (deletionListStr != null) {
                    MonoProcessor<Boolean> res = MonoProcessor.create();
                    boolean firstScanDeletion = Boolean.parseBoolean(msg.get("firstScanDeletion"));
                    if (firstScanDeletion) {
                        if (list.isEmpty() && !SST_FILE_CACHE.containsKey(getSstCacheKey(lun, vnode, fileName, 1))) {
                            VNODE_INGEST_PROCESSOR_MAP.get(lun + '-' + vnode).onNext(new Tuple3<>("1-" + fileName, putSstFilePath, new Tuple2<>(null, Boolean.FALSE)));
                        } else {
                            SstFileWriter putWriter = getSstWriter(lun, vnode, fileName, 1, isFirst);
                            for (Tuple2<byte[], byte[]> tuple2 : list) {
                                if (new String(tuple2.var1).startsWith(ROCKS_AGGREGATION_RATE_PREFIX)) {
                                    BitSet deserialize = AggregationUtils.deserialize(new String(tuple2.var2));
                                    putWriter.put(tuple2.var1, deserialize.toByteArray());
                                } else {
                                    putWriter.put(tuple2.var1, tuple2.var2);
                                }
                            }
                            SST_FILE_CACHE.remove(getSstCacheKey(lun, vnode, fileName, 1));
                            putWriter.finish();
                            putWriter.close();
                            VNODE_INGEST_PROCESSOR_MAP.get(lun + '-' + vnode).onNext(new Tuple3<>("1-" + fileName, putSstFilePath, new Tuple2<>(null, Boolean.TRUE)));
                        }
                    }
                    List<byte[]> deletionList = Json.decodeValue(deletionListStr, new TypeReference<List<byte[]>>() {
                    });
                    if (deletionList.isEmpty() && !SST_FILE_CACHE.containsKey(getSstCacheKey(lun, vnode, fileName, 0))) {
                        VNODE_INGEST_PROCESSOR_MAP.get(lun + '-' + vnode).onNext(new Tuple3<>("0-" + fileName, deleteSstFilePath, new Tuple2<>(res, Boolean.FALSE)));
                        //等待sst写入完成
                        return res.flatMap(x -> Mono.just(SUCCESS_PAYLOAD));
                    } else {
                        SstFileWriter deleteWriter = getSstWriter(lun, vnode, fileName, 0, isFirst);
                        for (byte[] deleteKey : deletionList) {
                            deleteWriter.delete(deleteKey);
                        }
                        if (msg.get("readEnd") != null) {
                            SST_FILE_CACHE.remove(getSstCacheKey(lun, vnode, fileName, 0));
                            deleteWriter.finish();
                            deleteWriter.close();
                            VNODE_INGEST_PROCESSOR_MAP.get(lun + '-' + vnode).onNext(new Tuple3<>("0-" + fileName, deleteSstFilePath, new Tuple2<>(res, Boolean.TRUE)));
                            //等待sst写入完成
                            return res.flatMap(x -> Mono.just(SUCCESS_PAYLOAD));
                        }
                    }
                } else {
                    SstFileWriter putWriter = getSstWriter(lun, vnode, fileName, 1, isFirst);
                    for (Tuple2<byte[], byte[]> tuple2 : list) {
                        putWriter.put(tuple2.var1, tuple2.var2);
                    }
                }

                if (!"-1".equals(curMsgId)) {
                    VNODE_MSG_ID_MAP.put(msgKey, curMsgId);
                }

                return Mono.just(DefaultPayload.create(SUCCESS_PAYLOAD));
            } catch (Exception e) {
                log.error("", e);
                log.error("{} {} - {}", vnode, fileName, e.getMessage());
                return Mono.just(DefaultPayload.create(ERROR_PAYLOAD));
            }
        }

        if (migrateCopy != null) {
            //表示处理copy的fileMeta
            MonoProcessor<String> res = MonoProcessor.create();
            BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
                String fileName = key.split(ROCKS_FILE_META_PREFIX)[1];
                String sourceFileName = fileName.split(ROCKS_FILE_META_PREFIX)[0];
                String sourceKey = ROCKS_FILE_META_PREFIX + sourceFileName;
                byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, sourceKey.getBytes());

                if (oldValue == null) {//如果源FileMeta不存在
                    //源FileMeta不存在，则查找是否有已迁移成功的copy数据，有的话表示在源数据块被删之已经迁到了新盘，那么可以直接复制fileMeta
                    try (RocksIterator iterator = db.newIterator(READ_OPTIONS);
                         RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)) {
                        keyIterator.seek(sourceKey.getBytes());
                        while (keyIterator.isValid() && new String(keyIterator.key()).startsWith(sourceKey)) {
                            if (new String(keyIterator.key()).startsWith(sourceKey + ROCKS_FILE_META_PREFIX)) {//检查是否存在有copy的数据块
                                oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, keyIterator.key());
                                break;
                            }
                            keyIterator.next();
                        }
                    }
                    //如果当前磁盘不存在已成功迁移的copy FileMeta时，走真copy
                    if (oldValue == null) {
                        res.onNext("0");
                        return;
                    }
                }
                FileMeta oldFileMeta = Json.decodeValue(new String(oldValue), FileMeta.class);
                oldFileMeta.setFileName(replaceFirst(key));
                writeBatch.put(key.getBytes(), Json.encode(oldFileMeta).getBytes());

//                writeBatch.put(key.getBytes(), value.getBytes());
                res.onNext("1");
            };
            return BatchRocksDB.customizeOperateData(lun, consumer, res)
                    .map(str -> DefaultPayload.create(str, SUCCESS.name()))
                    .doOnError(e -> log.error("", e))
                    .onErrorReturn(ERROR_PAYLOAD);
        }
        try {
            //这里使用与本地双写迁移相同的writeBatch处理
            RequestConsumer consumer = (db, writeBatch, request) -> {
                writeBatch.put(key.getBytes(), value.getBytes());
            };

            return BatchRocksDB.customizeOperateMeta(lun, 0, consumer)
                    .map(b -> SUCCESS_PAYLOAD)
                    .doOnError(e -> log.error("", e))
                    .onErrorReturn(ERROR_PAYLOAD);
//            MSRocksDB.getRocksDB(lun).put(key.getBytes(), value.getBytes());
//            return Mono.just(SUCCESS_PAYLOAD);
        } catch (Exception e) {
            log.error("delete rockets value error,key{},{}", key, e);
            return Mono.just(ERROR_PAYLOAD);
        }
    }

    public static String replaceFirst(String input) {
        int index = input.indexOf(ROCKS_FILE_META_PREFIX);
        return File.separator + input.substring(index + 1);
    }

    private static String getWriteSstFilePath(String lun, String vnode, String fileName, int writeType) {
        return "/" + lun + "/sst_writer/" + vnode + "-" + writeType + "-" + fileName;
    }

    public static void checkSstParentPath(String lun) {
        if (Files.notExists(Paths.get("/" + lun + "/sst_writer/"))) {
            File file = new File("/" + lun + "/sst_writer/");
            file.mkdirs();
        }
    }

    public static String getSstCacheKey(String lun, String vnode, String sstFileName, int writerType) {
        return lun + "$" + vnode + "$" + writerType + "$" + sstFileName;
    }

    public static Mono<Payload> migrateMeta(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String value = msg.get("value");

        JsonObject o = new JsonObject(value);
        String versionNum = o.getString("versionNum");
        if (null != versionNum) {
            RequestConsumer consumer = (db, writeBatch, request) -> {
                boolean needPut = true;
                byte[] oldValue = writeBatch.getFromBatchAndDB(db, key.getBytes());
                if (null != oldValue) {
                    JsonObject oldJson = new JsonObject(new String(oldValue));
                    if (versionNum.compareTo(oldJson.getString("versionNum")) <= 0) {
                        needPut = false;
                    }
                }

                if (needPut) {
                    writeBatch.put(key.getBytes(), value.getBytes());
                }
            };

            return BatchRocksDB.customizeOperateData(lun, consumer)
                    .map(b -> SUCCESS_PAYLOAD)
                    .onErrorReturn(ERROR_PAYLOAD);
        }

        return Mono.just(SUCCESS_PAYLOAD);


    }

    public static Mono<Payload> replayDelete(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String value = msg.get("value");

        MonoProcessor<Payload> res = MonoProcessor.create();
        RequestConsumer consumer = (db, writeBatch, request) -> {
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, key.getBytes());
            if (null != oldValue && Arrays.equals(oldValue, value.getBytes())) {
                writeBatch.delete(key.getBytes());
            } else {
                if (null != oldValue) {
                    //如果新value是删除标记，此时如果新节点不是删除标记，那么删除失败重试
                    JsonObject o = new JsonObject(value);
                    JsonObject o1 = new JsonObject(new String(oldValue));
                    if (null != o.getBoolean("deleteMark") && o.getBoolean("deleteMark")) {
                        if (null != o1.getBoolean("deleteMark") && o1.getBoolean("deleteMark")) {
                            res.onNext(ERROR_PAYLOAD);
                            return;
                        }
                    }
                }
            }

            res.onNext(SUCCESS_PAYLOAD);
        };
        return BatchRocksDB.customizeOperateData(lun, consumer, res);
    }

    public static Mono<Payload> updateDisk(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String pool = msg.get("pool");
        String vnode = msg.get("vnode");
        String newDisk = msg.get("newDisk");
        StoragePoolFactory.getStoragePool(pool, null).updateVnodeDisk(vnode, newDisk);

        return Mono.just(SUCCESS_PAYLOAD);
    }

    public static Mono<Payload> updateNode(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String pool = msg.get("poolType");
        String vnode = msg.get("vnode");
        String newDisk = msg.get("lun");
        String node = msg.get("node");

        StoragePoolFactory.getStoragePool(pool, null).updateVnodeNode(vnode, node, newDisk);

        return Mono.just(SUCCESS_PAYLOAD);
    }

    public static Mono<Payload> createRebuildQueue(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String disk = msg.get("disk");
        ReBuildRunner.getInstance().rabbitMq.consumer(disk);

        return Mono.just(SUCCESS_PAYLOAD);
    }

    public static Mono<Payload> haveSyncRecord(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String bucket = msg.get("bucket");
        int clusterIndex = Integer.parseInt(msg.get("clusterIndex"));
        String syncPrefix = "";
        if ("1".equals(msg.get("deleteSource"))) {
            syncPrefix = UnSynchronizedRecord.getRecorderPrefixLocal(bucket);
        } else {
            syncPrefix = UnSynchronizedRecord.getRecorderPrefixAsync(bucket, clusterIndex, !lun.endsWith(UnsyncRecordDir));
        }
        String checkRecordKey = msg.get("checkRecordKey");
        if (StringUtils.isNotBlank(checkRecordKey)) {
            syncPrefix = checkRecordKey;
        }
        boolean onlyDelete = "1".equals(msg.get("onlyDelete"));
        if (onlyDelete) {
            syncPrefix = UnSynchronizedRecord.getOnlyDeletePrefix(bucket);
        }

        try (Slice lowerSlice = new Slice(syncPrefix.getBytes());
             Slice upperSlice = new Slice((syncPrefix + ROCKS_SMALLEST_KEY).getBytes());
             ReadOptions readOptions = new ReadOptions().setIterateLowerBound(lowerSlice).setIterateUpperBound(upperSlice);
             MSRocksIterator iterator = MSRocksDB.getRocksDB(lun).newIterator(readOptions)) {
            iterator.seek(syncPrefix.getBytes());
            while (iterator.isValid() && new String(iterator.key()).startsWith(syncPrefix)) {
                if (IS_THREE_SYNC) {
                    if (!LOCAL_CLUSTER_INDEX.equals(THREE_SYNC_INDEX) && clusterIndex != THREE_SYNC_INDEX
                            && UnSynchronizedRecord.isAsyncRecord(new String(iterator.key()))
                            && !onlyDelete) {
                        return Mono.just(NOT_FOUND_PAYLOAD);
                    }
                } else {
                    // 非async站点且指针已经到了async记录，说明没有该双活站点的相关记录，返回NOT_FOUND
                    if (!ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(clusterIndex)
                            && UnSynchronizedRecord.isAsyncRecord(new String(iterator.key()))
                            && !onlyDelete) {
                        return Mono.just(NOT_FOUND_PAYLOAD);
                    }
                }

                if (StringUtils.isNotBlank(checkRecordKey)) {
                    return Mono.just(SUCCESS_PAYLOAD);
                }

                String value = new String(iterator.value());
                UnSynchronizedRecord record = Json.decodeValue(value, UnSynchronizedRecord.class);

                if (!record.isDeleteMark()) {
                    return Mono.just(SUCCESS_PAYLOAD);
                }

                iterator.next();
            }
        }
        return Mono.just(NOT_FOUND_PAYLOAD);
    }

    public static Mono<Payload> putRocksValue(Payload payload) {
        if (!isMultiAliveStarted) {
            return Mono.just(ERROR_PAYLOAD);
        }
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String value = msg.get("value");
        String async = msg.get(ASYNC_CLUSTER_SIGNAL);

        MonoProcessor<Payload> res = MonoProcessor.create();
        RequestConsumer consumer = (db, writeBatch, request) -> {
            UnSynchronizedRecord newAsyncRecord = Json.decodeValue(value, UnSynchronizedRecord.class);
            final Set<Integer> syncIndexMap = BucketSyncSwitchCache.getSyncIndexMap(newAsyncRecord.bucket, LOCAL_CLUSTER_INDEX);
            if (!syncIndexMap.isEmpty()) {
                for (Integer syncIndex : syncIndexMap) {
                    if (DA_INDEX_IPS_ENTIRE_MAP.size() != 1 && DA_INDEX_IPS_ENTIRE_MAP.containsKey(syncIndex)) {
                        // 非异构站点不写差异
                        if (IS_THREE_SYNC) {
                            // 3复制环境，新加复制站点写入
                            if (LOCAL_CLUSTER_INDEX.equals(THREE_SYNC_INDEX)) {
                                if (!LOCAL_CLUSTER_INDEX.equals(syncIndex)) {
                                    UnSynchronizedRecord newSyncRecord = Json.decodeValue(value, UnSynchronizedRecord.class);
                                    String asyncKey = newSyncRecord.rocksKeyAsync(syncIndex);
                                    newSyncRecord.setIndex(syncIndex);
                                    writeBatch.put(asyncKey.getBytes(), Json.encode(newSyncRecord).getBytes());
                                }
                            } else {
                                if (!LOCAL_CLUSTER_INDEX.equals(syncIndex)) {
                                    UnSynchronizedRecord newSyncRecord = Json.decodeValue(value, UnSynchronizedRecord.class);
                                    if (THREE_SYNC_INDEX.equals(syncIndex)) {
                                        String asyncKey = newSyncRecord.rocksKeyAsync(syncIndex);
                                        newSyncRecord.setIndex(syncIndex);
                                        writeBatch.put(asyncKey.getBytes(), Json.encode(newSyncRecord).getBytes());
                                    } else {
                                        newSyncRecord.setIndex(syncIndex);
                                        writeBatch.put(key.getBytes(), Json.encode(newSyncRecord).getBytes());
                                    }
                                }
                            }
                        } else {
                            byte[] curRecValue = writeBatch.getFromBatchAndDB(db, key.getBytes());
                            if (curRecValue == null) {
                                writeBatch.put(key.getBytes(), value.getBytes());
                            } else {
                                JsonObject updateObj = new JsonObject(value);
                                String toUpdateVersion = updateObj.getString("versionNum");
                                JsonObject currObj = new JsonObject(new String(curRecValue));
                                if (currObj.getString("versionNum").compareTo(toUpdateVersion) < 0) {
                                    writeBatch.put(key.getBytes(), value.getBytes());
                                }
                            }
                        }
                    } else if (StringUtils.isNotBlank(async) && ASYNC_INDEX_IPS_ENTIRE_MAP.size() > 0 && ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(syncIndex)) {
                        String asyncKey = newAsyncRecord.rocksKeyAsync(syncIndex);
                        newAsyncRecord.setIndex(syncIndex);
                        writeBatch.put(asyncKey.getBytes(), Json.encode(newAsyncRecord).getBytes());
                    }
                }
            }
            res.onNext(SUCCESS_PAYLOAD);
        };

        return BatchRocksDB.customizeOperate(lun, key.hashCode(), consumer, res)
                .doOnError(e -> log.error("", e))
                .onErrorReturn(ERROR_PAYLOAD);
    }

    /**
     * 更新双活同步日志
     */
    public static Mono<Payload> updateSyncRecord(Payload payload) {
        if (!isMultiAliveStarted) {
            return Mono.just(ERROR_PAYLOAD);
        }
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String value = msg.get("value");
        String oldVersion = msg.get("oldVersion");
        String hisRecord = msg.get(HIS_SYNC_SIGNAL);
        String deleteSource = msg.get(DELETE_SOURCE);
        String async = msg.get(ASYNC_CLUSTER_SIGNAL);
        MonoProcessor<Payload> res = MonoProcessor.create();

        UnSynchronizedRecord newAsyncRecord = Json.decodeValue(value, UnSynchronizedRecord.class);
        Integer clusterIndex = newAsyncRecord.index;
        String bucket = newAsyncRecord.bucket;
        DataSynChecker.getInstance().addSyncBucket(clusterIndex, bucket);
        boolean archiveSign = newAsyncRecord.headers != null && newAsyncRecord.headers.containsKey(ARCHIVE_SYNC_REC_MARK);
        final Set<Integer> syncIndexMap = archiveSign ? BucketSyncSwitchCache.getArchiveIndexMap(bucket, LOCAL_CLUSTER_INDEX) : BucketSyncSwitchCache.getSyncIndexMap(bucket, LOCAL_CLUSTER_INDEX);
        RequestConsumer consumer = (db, writeBatch, request) -> {
            if (!StringUtils.isEmpty(deleteSource)) {
                byte[] currValue = writeBatch.getFromBatchAndDB(db, key.getBytes());
                if (currValue == null) {
                    // 上传时预提交修复可能将已完成的记录又重新写入。此时comit又被改为false
                    writeBatch.put(key.getBytes(), value.getBytes());
                } else {
                    JsonObject currentValue = new JsonObject(new String(currValue));
                    String currentVersion = currentValue.getString("versionNum");
                    currentValue.remove("versionNum");
                    JsonObject updateValue = new JsonObject(value);
                    String updateVersion = updateValue.getString("versionNum");
                    updateValue.remove("versionNum");
                    //判断除versionNum外要更新的元数据内容上是否有变化
                    if (currentValue.equals(updateValue)) {
                        //比较新旧元数据中的versionNum
                        if (updateVersion.compareTo(currentVersion) > 0) {
                            writeBatch.put(key.getBytes(), value.getBytes());
                        }
                    } else {
                        //元数据内容有更新时，比较现在db中的versionNum和原请求的versionNum（非新元数据中的versionNum）
                        if (currentVersion.compareTo(oldVersion) <= 0) {
                            writeBatch.put(key.getBytes(), value.getBytes());
                        } else {
                            //元数据被覆盖
                            res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                            return;
                        }
                    }
                }
                res.onNext(SUCCESS_PAYLOAD);
                return;
            }
            boolean overwrite = false;
            if (!syncIndexMap.isEmpty()) {
                for (Integer syncIndex : syncIndexMap) {
                    if (StringUtils.isNotBlank(hisRecord) && !clusterIndex.equals(syncIndex)) {
                        continue;
                    }
                    if (DA_INDEX_IPS_ENTIRE_MAP.size() != 1 && DA_INDEX_IPS_ENTIRE_MAP.containsKey(syncIndex)) {
                        if (IS_THREE_SYNC) {
                            // 3复制环境，新加复制站点写入
                            if (LOCAL_CLUSTER_INDEX.equals(THREE_SYNC_INDEX)) {
                                if (syncIndex.equals(LOCAL_CLUSTER_INDEX)) {
                                    continue;
                                }
                                UnSynchronizedRecord newSyncRecord = Json.decodeValue(value, UnSynchronizedRecord.class);
                                if (StringUtils.isBlank(async) && StringUtils.isBlank(hisRecord)) {
                                    break;
                                }
                                String asyncKey = newSyncRecord.rocksKeyAsync(syncIndex);
                                byte[] curAsyncRecValue = writeBatch.getFromBatchAndDB(db, asyncKey.getBytes());
                                if (curAsyncRecValue == null) {
                                    newSyncRecord.setIndex(syncIndex);
                                    writeBatch.put(asyncKey.getBytes(), Json.encode(newSyncRecord).getBytes());
                                } else {
                                    JsonObject currentValue = new JsonObject(new String(curAsyncRecValue));
                                    String currentVersion = currentValue.getString("versionNum");
                                    currentValue.remove("versionNum");
                                    JsonObject updateValue = new JsonObject(value);
                                    String updateVersion = updateValue.getString("versionNum");
                                    updateValue.remove("versionNum");

                                    if (currentValue.equals(updateValue)) {
                                        if (updateVersion.compareTo(currentVersion) > 0) {
                                            newSyncRecord.setIndex(syncIndex);
                                            writeBatch.put(asyncKey.getBytes(), Json.encode(newSyncRecord).getBytes());
                                        }
                                    } else {
                                        //元数据内容有更新时，比较现在db中的versionNum和原请求的versionNum（非新元数据中的versionNum）
                                        if (currentVersion.compareTo(oldVersion) <= 0) {
                                            newSyncRecord.setIndex(syncIndex);
                                            writeBatch.put(asyncKey.getBytes(), Json.encode(newSyncRecord).getBytes());
                                        } else {
                                            //元数据被覆盖
                                            overwrite = true;
                                        }
                                    }
                                }
                            } else {
                                if (syncIndex.equals(LOCAL_CLUSTER_INDEX)) {
                                    continue;
                                }
                                if (StringUtils.isBlank(async) && !syncIndex.equals(clusterIndex)) {
                                    continue;
                                }
                                UnSynchronizedRecord newSyncRecord = Json.decodeValue(value, UnSynchronizedRecord.class);
                                if (THREE_SYNC_INDEX.equals(syncIndex)) {
                                    String asyncKey = newSyncRecord.rocksKeyAsync(syncIndex);
                                    byte[] curAsyncRecValue = writeBatch.getFromBatchAndDB(db, asyncKey.getBytes());
                                    if (curAsyncRecValue == null) {
                                        newSyncRecord.setIndex(syncIndex);
                                        writeBatch.put(asyncKey.getBytes(), Json.encode(newSyncRecord).getBytes());
                                    } else {
                                        JsonObject currentValue = new JsonObject(new String(curAsyncRecValue));
                                        String currentVersion = currentValue.getString("versionNum");
                                        currentValue.remove("versionNum");
                                        JsonObject updateValue = new JsonObject(value);
                                        String updateVersion = updateValue.getString("versionNum");
                                        updateValue.remove("versionNum");

                                        if (currentValue.equals(updateValue)) {
                                            if (updateVersion.compareTo(currentVersion) > 0) {
                                                newSyncRecord.setIndex(syncIndex);
                                                writeBatch.put(asyncKey.getBytes(), Json.encode(newSyncRecord).getBytes());
                                            }
                                        } else {
                                            //元数据内容有更新时，比较现在db中的versionNum和原请求的versionNum（非新元数据中的versionNum）
                                            if (currentVersion.compareTo(oldVersion) <= 0) {
                                                newSyncRecord.setIndex(syncIndex);
                                                writeBatch.put(asyncKey.getBytes(), Json.encode(newSyncRecord).getBytes());
                                            } else {
                                                //元数据被覆盖
                                                overwrite = true;
                                            }
                                        }
                                    }
                                } else {
                                    byte[] curAsyncRecValue = writeBatch.getFromBatchAndDB(db, key.getBytes());
                                    if (curAsyncRecValue == null) {
                                        newSyncRecord.setIndex(syncIndex);
                                        writeBatch.put(key.getBytes(), Json.encode(newSyncRecord).getBytes());
                                    } else {
                                        JsonObject currentValue = new JsonObject(new String(curAsyncRecValue));
                                        String currentVersion = currentValue.getString("versionNum");
                                        currentValue.remove("versionNum");
                                        JsonObject updateValue = new JsonObject(value);
                                        String updateVersion = updateValue.getString("versionNum");
                                        updateValue.remove("versionNum");

                                        if (currentValue.equals(updateValue)) {
                                            if (updateVersion.compareTo(currentVersion) > 0) {
                                                newSyncRecord.setIndex(syncIndex);
                                                writeBatch.put(key.getBytes(), Json.encode(newSyncRecord).getBytes());
                                            }
                                        } else {
                                            //元数据内容有更新时，比较现在db中的versionNum和原请求的versionNum（非新元数据中的versionNum）
                                            if (currentVersion.compareTo(oldVersion) <= 0) {
                                                newSyncRecord.setIndex(syncIndex);
                                                writeBatch.put(key.getBytes(), Json.encode(newSyncRecord).getBytes());
                                            } else {
                                                //元数据被覆盖
                                                overwrite = true;
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            // index不存在时不写差异记录，如单站点只有复制站点时index为-1，
                            byte[] currValue = writeBatch.getFromBatchAndDB(db, key.getBytes());

                            if (currValue == null) {
                                // 上传时预提交修复可能将已完成的记录又重新写入。此时comit又被改为false
                                writeBatch.put(key.getBytes(), value.getBytes());
                            } else {
                                JsonObject currentValue = new JsonObject(new String(currValue));
                                String currentVersion = currentValue.getString("versionNum");
                                currentValue.remove("versionNum");
                                JsonObject updateValue = new JsonObject(value);
                                String updateVersion = updateValue.getString("versionNum");
                                updateValue.remove("versionNum");
                                //判断除versionNum外要更新的元数据内容上是否有变化
                                if (currentValue.equals(updateValue)) {
                                    //比较新旧元数据中的versionNum
                                    if (updateVersion.compareTo(currentVersion) > 0) {
                                        writeBatch.put(key.getBytes(), value.getBytes());
                                    }
                                } else {
                                    //元数据内容有更新时，比较现在db中的versionNum和原请求的versionNum（非新元数据中的versionNum）
                                    if (currentVersion.compareTo(oldVersion) <= 0) {
                                        writeBatch.put(key.getBytes(), value.getBytes());
                                    } else {
                                        //元数据被覆盖
                                        res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                                        return;
                                    }
                                }
                            }
                        }
                    } else if ((StringUtils.isNotBlank(async) || StringUtils.isNotBlank(hisRecord)) && ASYNC_INDEX_IPS_ENTIRE_MAP.size() > 0 && ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(syncIndex)) {
                        String asyncKey = newAsyncRecord.rocksKeyAsync(syncIndex);
                        byte[] curAsyncRecValue = writeBatch.getFromBatchAndDB(db, asyncKey.getBytes());
                        if (curAsyncRecValue == null) {
                            newAsyncRecord.setIndex(syncIndex);
                            writeBatch.put(asyncKey.getBytes(), Json.encode(newAsyncRecord).getBytes());
                        } else {
                            JsonObject currentValue = new JsonObject(new String(curAsyncRecValue));
                            String currentVersion = currentValue.getString("versionNum");
                            currentValue.remove("versionNum");
                            JsonObject updateValue = new JsonObject(value);
                            String updateVersion = updateValue.getString("versionNum");
                            updateValue.remove("versionNum");

                            if (currentValue.equals(updateValue)) {
                                if (updateVersion.compareTo(currentVersion) > 0) {
                                    newAsyncRecord.setIndex(syncIndex);
                                    writeBatch.put(asyncKey.getBytes(), Json.encode(newAsyncRecord).getBytes());
                                }
                            } else {
                                //元数据内容有更新时，比较现在db中的versionNum和原请求的versionNum（非新元数据中的versionNum）
                                if (currentVersion.compareTo(oldVersion) <= 0) {
                                    newAsyncRecord.setIndex(syncIndex);
                                    writeBatch.put(asyncKey.getBytes(), Json.encode(newAsyncRecord).getBytes());
                                } else {
                                    //元数据被覆盖
                                    overwrite = true;
                                }
                            }
                        }
                    }
                }
            }
            if (overwrite) {
                res.onNext(DefaultPayload.create("", META_WRITEED.name()));
            } else {
                res.onNext(SUCCESS_PAYLOAD);
            }
        };

        return BatchRocksDB.customizeOperate(lun, key.hashCode(), consumer, res)
                .doOnError(e -> log.error("", e))
                .onErrorReturn(ERROR_PAYLOAD);
    }


    public static Mono<Payload> deleteUnsyncRecord(Payload payload) {
        if (!isMultiAliveStarted) {
            return Mono.just(ERROR_PAYLOAD);
        }
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        RequestConsumer consumer = (db, writeBatch, request) -> {

            String asyncSignal = msg.get(ASYNC_CLUSTER_SIGNAL);
            if (DELETE_SYNC_RECORD.equals(asyncSignal)) {
                if (LOCAL_CLUSTER_INDEX.equals(THREE_SYNC_INDEX)) {
                    for (int index : DA_INDEX_IPS_ENTIRE_MAP.keySet()) {
                        if (index != LOCAL_CLUSTER_INDEX) {
                            String asyncKey = UnSynchronizedRecord.getRocksKeyAsync(key, index);
                            writeBatch.delete(asyncKey.getBytes());
                        }
                    }
                } else {
                    for (int index : DA_INDEX_IPS_ENTIRE_MAP.keySet()) {
                        if (index != LOCAL_CLUSTER_INDEX) {
                            if (THREE_SYNC_INDEX.equals(index)) {
                                String asyncKey = UnSynchronizedRecord.getRocksKeyAsync(key, index);
                                writeBatch.delete(asyncKey.getBytes());
                            } else {
                                writeBatch.delete(key.getBytes());
                            }
                        }
                    }
                }
                return;
            }

            writeBatch.delete(key.getBytes());

            if (!ASYNC_CLUSTER_SIGNAL_SET.contains(asyncSignal)) {
                return;
            }

            if (DELETE_ASYNC_RECORD.equals(asyncSignal)) {
                for (int index : ASYNC_INDEX_IPS_ENTIRE_MAP.keySet()) {
                    String asyncKey = UnSynchronizedRecord.getRocksKeyAsync(key, index);
                    writeBatch.delete(asyncKey.getBytes());
                }
                return;
            }

            String value = msg.get("value");
            if (value == null) {
                return;
            }
            String oldVersion = msg.get("oldVersion");
            UnSynchronizedRecord newAsyncRecord = Json.decodeValue(value, UnSynchronizedRecord.class);
            if (UPDATE_ASYNC_RECORD.equals(asyncSignal)) {
                for (int index : ASYNC_INDEX_IPS_ENTIRE_MAP.keySet()) {
                    String asyncKey = newAsyncRecord.rocksKeyAsync(index);
                    byte[] curAsyncRecValue = writeBatch.getFromBatchAndDB(db, asyncKey.getBytes());
                    if (curAsyncRecValue != null) {
                        JsonObject currentValue = new JsonObject(new String(curAsyncRecValue));
                        String currentVersion = currentValue.getString("versionNum");
                        currentValue.remove("versionNum");
                        JsonObject updateValue = new JsonObject(value);
                        String updateVersion = updateValue.getString("versionNum");
                        updateValue.remove("versionNum");

                        if (currentValue.equals(updateValue)) {
                            if (updateVersion.compareTo(currentVersion) > 0) {
                                newAsyncRecord.setIndex(index);
                                writeBatch.put(asyncKey.getBytes(), Json.encode(newAsyncRecord).getBytes());
                            }
                        } else {
                            //元数据内容有更新时，比较现在db中的versionNum和原请求的versionNum（非新元数据中的versionNum）
                            if (currentVersion.compareTo(oldVersion) <= 0) {
                                newAsyncRecord.setIndex(index);
                                writeBatch.put(asyncKey.getBytes(), Json.encode(newAsyncRecord).getBytes());
                            } else {
                                //元数据被覆盖
//                            res.onNext(DefaultPayload.create("", META_WRITEED.name()));
//                                return;
                            }
                        }
                    }
                }
            }

        };
        return BatchRocksDB.customizeOperateMeta(lun, key.hashCode(), consumer)
                .map(b -> SUCCESS_PAYLOAD)
//                .doOnError(e -> log.error("", e))
                .onErrorReturn(ERROR_PAYLOAD);
    }

    /**
     * put拆分copy的part对象到record
     *
     * @param payload
     * @return
     */
    public static Mono<Payload> putPartCopyRecord(Payload payload) {
        if (!isMultiAliveStarted) {
            return Mono.just(ERROR_PAYLOAD);
        }
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String recordValue = msg.get("recordValue");
        String metaValue = msg.get("metaValue");
        UnSynchronizedRecord record = Json.decodeValue(recordValue, UnSynchronizedRecord.class);
        String daVersion = record.syncStamp;
        if (StringUtils.isNotBlank(msg.get("DaVersion"))) {
            daVersion = msg.get("DaVersion");
        }
        String archiveBackupKey = record.headers.get(ARCHIVE_ANALYZER_KEY);
        if (StringUtils.isNotBlank(archiveBackupKey)) {
            record.headers.remove(ARCHIVE_ANALYZER_KEY);
        }
        MetaData metaData = Json.decodeValue(metaValue, MetaData.class);
        MonoProcessor<Payload> res = MonoProcessor.create();
        String finalDaVersion = daVersion;
        RequestConsumer consumer = (db, writeBatch, request) -> {
            String keyPrefix;
            if (IS_THREE_SYNC) {
                if (LOCAL_CLUSTER_INDEX.equals(THREE_SYNC_INDEX)) {
                    if (!lun.endsWith(UnsyncRecordDir)) {
                        keyPrefix =
                                record.recordKey.split(File.separator)[0] + File.separator + record.bucket + File.separator + "async" + File.separator + record.index + File.separator + finalDaVersion;
                    } else {
                        keyPrefix =
                                record.recordKey.split(File.separator)[0] + File.separator + record.bucket + File.separator + UnsyncRecordDir + File.separator + "async" + File.separator + record.index + File.separator + finalDaVersion;
                    }
                } else {
                    if (THREE_SYNC_INDEX.equals(record.index)) {
                        if (!lun.endsWith(UnsyncRecordDir)) {
                            keyPrefix =
                                    record.recordKey.split(File.separator)[0] + File.separator + record.bucket + File.separator + "async" + File.separator + record.index + File.separator + finalDaVersion;
                        } else {
                            keyPrefix =
                                    record.recordKey.split(File.separator)[0] + File.separator + record.bucket + File.separator + UnsyncRecordDir + File.separator + "async" + File.separator + record.index + File.separator + finalDaVersion;
                        }
                    } else {
                        if (!lun.endsWith(UnsyncRecordDir)) {
                            keyPrefix = record.recordKey.split(File.separator)[0] + File.separator + record.bucket + File.separator + finalDaVersion;
                        } else {
                            keyPrefix = record.recordKey.split(File.separator)[0] + File.separator + record.bucket + File.separator + UnsyncRecordDir + File.separator + finalDaVersion;
                        }
                    }
                }
            } else {
                if (!ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(record.index)) {
                    if (!lun.endsWith(UnsyncRecordDir)) {
                        keyPrefix = record.recordKey.split(File.separator)[0] + File.separator + record.bucket + File.separator + finalDaVersion;
                    } else {
                        keyPrefix = record.recordKey.split(File.separator)[0] + File.separator + record.bucket + File.separator + UnsyncRecordDir + File.separator + finalDaVersion;
                    }
                } else {
                    if (!lun.endsWith(UnsyncRecordDir)) {
                        keyPrefix =
                                record.recordKey.split(File.separator)[0] + File.separator + record.bucket + File.separator + "async" + File.separator + record.index + File.separator + finalDaVersion;
                    } else {
                        keyPrefix =
                                record.recordKey.split(File.separator)[0] + File.separator + record.bucket + File.separator + UnsyncRecordDir + File.separator + "async" + File.separator + record.index + File.separator + finalDaVersion;
                    }
                }
            }

            String partInitUri = key + "?uploads";
            String partInitKey = keyPrefix + File.separator + ERROR_INIT_PART_UPLOAD + partInitUri;

            byte[] oldValue = writeBatch.getFromBatchAndDB(db, partInitKey.getBytes());
            if (oldValue != null) {
                res.onNext(SUCCESS_PAYLOAD);
                return;
            }

            record.setUri(partInitUri);
            record.setMethod(HttpMethod.POST);
            record.setRecordKey(partInitKey);
            //固定大小
            record.getHeaders().put(CONTENT_LENGTH, String.valueOf(0));
            writeBatch.put(partInitKey.getBytes(), Json.encode(record).getBytes());

            for (PartInfo partInfo : metaData.partInfos) {
                String partUploadUri = key + "?partNumber=" + partInfo.partNum + "&uploadId=" + metaData.partUploadId;
                String partUploadKey = keyPrefix + "0" + File.separator + ERROR_PART_UPLOAD + partUploadUri;
                record.getHeaders().put(CONTENT_LENGTH, partInfo.getPartSize() + "");
                record.setUri(partUploadUri);
                record.setMethod(HttpMethod.PUT);
                record.setRecordKey(partUploadKey);
                writeBatch.put(partUploadKey.getBytes(), Json.encode(record).getBytes());
            }

            if (StringUtils.isNotBlank(archiveBackupKey)) {
                record.headers.put(ARCHIVE_ANALYZER_KEY, archiveBackupKey);
            }
            String partMergeUri = key + "?uploadId=" + metaData.partUploadId;
            String partMergeKey = keyPrefix + "1" + File.separator + ERROR_COMPLETE_PART + partMergeUri;
            record.setUri(partMergeUri);
            record.setMethod(HttpMethod.POST);
            record.setRecordKey(partMergeKey);
            //固定大小
            record.getHeaders().put(CONTENT_LENGTH, String.valueOf(1024 * 1024));
            writeBatch.put(partMergeKey.getBytes(), Json.encode(record).getBytes());
            res.onNext(SUCCESS_PAYLOAD);
        };

        return BatchRocksDB.customizeOperate(lun, key.hashCode(), consumer, res)
                .doOnError(e -> log.error("", e))
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> getUsedSize() {
        String node = ServerConfig.getInstance().getHostUuid();

        return Flux.fromStream(map.keySet().stream())
                .publishOn(DISK_SCHEDULER)
                .flatMap(lun -> {
                    String key = node + '@' + lun;
                    String firstPartInfoKey = "firstPart_" + key;
                    if (!RemovedDisk.getInstance().contains(key)) {
                        try {
                            BlockDevice device = map.get(lun);
                            Tuple2<byte[], byte[]> t1 =
                                    new Tuple2<>(("total_" + key).getBytes(), toByte(device.getSize()));
                            byte[] size = MSRocksDB.getRocksDB(lun).get(ROCKS_FILE_SYSTEM_USED_SIZE);

                            Tuple2<Long, Long> firstPartInfo = getFirstPartInfo(lun);
                            List<Tuple2<byte[], byte[]>> extras = new ArrayList<>();
                            extras.add(new Tuple2<>(("total_" + firstPartInfoKey).getBytes(), toByte(firstPartInfo.var1())));
                            extras.add(new Tuple2<>((firstPartInfoKey).getBytes(), toByte(firstPartInfo.var2())));

                            if (null != size) {
                                return Flux.just(t1, new Tuple2<>(key.getBytes(), size)).concatWith(Flux.fromIterable(extras));
                            } else {
                                return Flux.just(t1).concatWith(Flux.fromIterable(extras));
                            }
                        } catch (Exception e) {
                        }
                    }

                    return Flux.empty();
                })
                .collectList()
                .map(list -> DefaultPayload.create(Json.encode(list.toArray()), SUCCESS.name()));
    }

    public static Mono<Payload> getMetaDataByUploadId(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String objName = msg.get("object");
        String uploadId = msg.get("uploadId");

        try {
            MSRocksDB db = MSRocksDB.getRocksDB(lun);
            if (db == null) {
                return Mono.just(DefaultPayload.create(Json.encode(MetaData.ERROR_META), ERROR.name()));
            }
            byte[] value = null;
            try (Slice lowerSlice = new Slice(key);
                 ReadOptions readOptions = new ReadOptions().setIterateLowerBound(lowerSlice);
                 MSRocksIterator keyIterator = db.newIterator(readOptions)) {
                keyIterator.seek((key).getBytes());
                while (keyIterator.isValid() && new String(keyIterator.key()).startsWith(key)) {
                    MetaData metaData = Json.decodeValue(new String(keyIterator.value()), MetaData.class);
                    if (!objName.equals(metaData.key)) {
                        break;
                    }
                    String versionKey = ROCKS_VERSION_PREFIX + key + Utils.ZERO_STR + metaData.versionId;
                    byte[] bytes = db.get(versionKey.getBytes());
                    if (bytes != null) {
                        metaData = Json.decodeValue(new String(bytes), MetaData.class);
                        if (StringUtils.isNotEmpty(metaData.partUploadId) && uploadId.equals(metaData.partUploadId)) {
                            value = keyIterator.value();
                            break;
                        }
                    }
                    keyIterator.next();
                }
            }
            if (null == value) {
                return Mono.just(DefaultPayload.create(Json.encode(MetaData.NOT_FOUND_META), NOT_FOUND.name()));
            } else {
                return Mono.just(DefaultPayload.create(value, SUCCESS.name().getBytes()));
            }

        } catch (Exception e) {
            log.error("", e);
            return Mono.just(DefaultPayload.create(Json.encode(MetaData.ERROR_META), ERROR.name()));
        }
    }

    public static Mono<Payload> updateBucketCap(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String bucket = msg.get("bucket");
        String vnode = msg.get("vnode");
        long objNum = Long.parseLong(msg.get("objNum"));
        long objCap = Long.parseLong(msg.get("objCap"));

        MonoProcessor<Payload> res = MonoProcessor.create();
        RequestConsumer consumer = (db, writeBatch, request) -> {

            updateCapacityInfo(writeBatch, bucket, "", vnode, objNum, objCap);
            res.onNext(SUCCESS_PAYLOAD);
        };

        return BatchRocksDB.customizeOperate(lun, lun.hashCode(), consumer, res)
                .doOnError(e -> log.error("", e))
                .onErrorReturn(ERROR_PAYLOAD);
    }

    /**
     * 将本节点的数据转换成Payload传输到主节点
     *
     * @return
     */

    public static Mono<Payload> getLogging() {
        Map<String, JsonObject> msg = LogRecorder.getLogging();
        return Mono.just(DefaultPayload.create(Json.encode(msg), SUCCESS.name()));
    }

    public static Mono<Payload> hasSSRecord(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        Integer clusterIndex = Integer.parseInt(msg.get("clusterIndex"));

        String syncPrefix = SingleSyncRecord.getRecordPrefix(clusterIndex);
        String syncPrefixUpper = SingleSyncRecord.getRecordPrefix(clusterIndex + 1);
        try (Slice lowerSlice = new Slice(syncPrefix);
             Slice upperSlice = new Slice(syncPrefixUpper);
             ReadOptions readOptions = new ReadOptions().setIterateLowerBound(lowerSlice).setIterateUpperBound(upperSlice);
             MSRocksIterator iterator = MSRocksDB.getRocksDB(lun).newIterator(readOptions)) {
            iterator.seek(syncPrefix.getBytes());
            while (iterator.isValid()) {
                if (new String(iterator.key()).startsWith(syncPrefix)) {
                    return Mono.just(SUCCESS_PAYLOAD);
                } else {
                    iterator.next();
                }
            }
        }
        return Mono.just(NOT_FOUND_PAYLOAD);
    }

    public static Mono<Payload> updateBucketStrategy(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        String strategy = msg.get("strategy");
        String newVnode = msg.get("newVnode");
        String serialize = msg.get("serialize");
        StorageStrategy.BUCKET_STRATEGY_NAME_MAP.put(bucket, strategy);
        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucket);
        ObjectSplitTree objectSplitTree;
        if (StringUtils.isNotEmpty(serialize)) {
            objectSplitTree = new ObjectSplitTree(bucket, serialize);
            objectSplitTree.print();
        } else {
            // 初始化桶的初始分片
            objectSplitTree = new ObjectSplitTree(bucket, null);
            // 对桶的初始分片进行散列
            if (newVnode != null) {
                log.info("new Vnode:" + newVnode);
                List<String> allLeafNode = objectSplitTree.getAllLeafNode();
                objectSplitTree.expansionNode(allLeafNode.get(0), newVnode, SEPARATOR_LINE_PREFIX + "m", true);
            }
        }
        // 缓存散列信息
        metaStoragePool.getBucketShardCache().put(bucket, objectSplitTree);
        BucketSyncSwitchCache.getInstance().add(bucket, msg.get(DATA_SYNC_SWITCH));
        log.info("update {} strategy {}", bucket, strategy);
        return Mono.just(SUCCESS_PAYLOAD);
    }

    public static Mono<Payload> startArbitrator(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String s = msg.get("init");
        if ("1".equals(s)) {
            Arbitrator.getInstance().init();
            DAVersionUtils.getInstance().init();
            Arbitrator.getInstance().firstCheckMaster();
        }
        return Mono.just(SUCCESS_PAYLOAD);
    }

    /**
     * 用于查询本节点的数据、缓存盘文件总量
     */
    public static Mono<Payload> queryNodeFileNum(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String poolListStr = msg.get("poolListStr");
        String[] poolArray = poolListStr.substring(1, poolListStr.length() - 1).split(", ");

        long totalFileNum = 0L;
        for (String curDisk : poolArray) {
            // 查询本节点的磁盘
            if (curDisk.startsWith(LOCAL_VM_UUID)) {
                try {
                    byte[] curDiskFileNumBytes = MSRocksDB.getRocksDB(curDisk.split("@")[1]).get(BlockDevice.ROCKS_FILE_SYSTEM_FILE_NUM);
                    if (curDiskFileNumBytes != null) {
                        long curDiskObjNum = bytes2long(curDiskFileNumBytes);
                        totalFileNum += curDiskObjNum;
                    }
                } catch (RocksDBException e) {
                    log.error("calculate file num error", e);
                }
            }
        }
        return Mono.just(DefaultPayload.create(String.valueOf(totalFileNum), SUCCESS.name()));
    }

    /**
     * 用于接收其它节点传来的QoS配置通知，成功返回元数据success
     */
    public static Mono<Payload> notifyQosSetting(Payload payload) {

        String uuid = ServerConfig.getInstance().getHostUuid();
        try {
            Mono.just("1")
                    .publishOn(DISK_SCHEDULER)
                    .subscribe(s -> {
                        LimitTimeSet.init();  // 开启非默认策略的生效定时
                    });

        } catch (Exception e) {
            log.error("", e);
            return Mono.just(DefaultPayload.create("Node " + uuid + " " + CURRENT_IP + " set QoS strategies error.", ERROR.name()));
        }

        log.info(" This node received the QoS setting message and set strategies successfully.");
        return Mono.just(DefaultPayload.create("Node " + uuid + " " + CURRENT_IP + " set QoS strategies successfully.", SUCCESS.name()));
    }


    /**
     * 用于通知其它节点删除设置的QoS，恢复为业务优先策略；如果是正在生效的策略，立即恢复为业务优先策略；如果是还未生效的策略，则删除定时器
     */
    public static Mono<Payload> notifyQosDelete(Payload payload) {

        String uuid = ServerConfig.getInstance().getHostUuid();
        try {
            JobScheduler.removeJobs();  // 移除系统存在的所有数据恢复QoS定时配置
            RecoverLimiter.updateStrategy(DEFAULT_STRATEGY);
        } catch (SchedulerException e) {
            log.error("", e);
            return Mono.just(DefaultPayload.create("Node " + uuid + " " + CURRENT_IP + " deleted QoS strategies error.", ERROR.name()));
        }

        log.info(" This node received the QoS delete message and deleted strategies successfully.");
        return Mono.just(DefaultPayload.create("Node " + uuid + " " + CURRENT_IP + " deleted QoS strategies successfully.", SUCCESS.name()));
    }

    public static Mono<Payload> simplePutRocks(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String value = msg.get("value");

        MonoProcessor<Payload> res = MonoProcessor.create();
        RequestConsumer consumer = (db, writeBatch, request) -> {
            // 单站点只有复制站点时不写差异记录，
            byte[] curRecValue = writeBatch.getFromBatchAndDB(db, key.getBytes());

            if (curRecValue == null) {
                writeBatch.put(key.getBytes(), value.getBytes());
            } else {
                JsonObject updateObj = new JsonObject(value);
                String toUpdateVersion = updateObj.getString("versionNum");
                JsonObject currObj = new JsonObject(new String(curRecValue));
                if (currObj.getString("versionNum").compareTo(toUpdateVersion) < 0) {
                    writeBatch.put(key.getBytes(), value.getBytes());
                }
            }
            res.onNext(SUCCESS_PAYLOAD);
        };

        return BatchRocksDB.customizeOperate(lun, key.hashCode(), consumer, res)
                .doOnError(e -> log.error("", e))
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> putEsRocks(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String value = msg.get("value");
        boolean isReName = "1".equalsIgnoreCase(msg.get("isReName"));
        boolean removeLink = "1".equalsIgnoreCase(msg.get("removeLink"));
        boolean addLink = "1".equalsIgnoreCase(msg.get("addLink"));
        String linkObjNames = msg.get("linkObjNames");
        String stamp = msg.get("stamp");
        String inodeStamp = msg.get("inodeStamp");
        MonoProcessor<Payload> res = MonoProcessor.create();
        EsMeta esMeta = Json.decodeValue(value, EsMeta.class);
        String esMetaKey = EsMeta.getKey(esMeta);
        RequestConsumer consumer = (db, writeBatch, request) -> {
            byte[] curRecValue = writeBatch.getFromBatchAndDB(db, key.getBytes());
            JsonObject updateObj = new JsonObject(value);
            if (curRecValue == null) {
                writeBatch.put(key.getBytes(), value.getBytes());
            } else {
                String toUpdateVersion = updateObj.getString("versionNum");
                JsonObject currObj = new JsonObject(new String(curRecValue));
                if (key.endsWith(EsMeta.LINK_SUFFIX)) {
                    String objName = currObj.getString("objName");
                    String[] objNames = new String[0];
                    if (StringUtils.isNotBlank(objName)) {
                        objNames = Json.decodeValue(objName, String[].class);
                    }
                    String[] names = Json.decodeValue(esMeta.objName, String[].class);
                    if (removeLink) {
                        Set<String> removeSet = new HashSet<>(Arrays.asList(names));
                        String[] newObjNames = Arrays.stream(objNames)
                                .filter(name -> !removeSet.contains(name))
                                .toArray(String[]::new);
                        esMeta.objName = Json.encode(newObjNames);
                        writeBatch.put(key.getBytes(), Json.encode(esMeta).getBytes());
                    } else {
                        Set<String> objNamesSet = Sets.newHashSet(objNames);
                        Set<String> namesSet = Sets.newHashSet(names);
                        Set<String> difference = Sets.difference(objNamesSet, namesSet);
                        for (String obj : difference) {
                            String esStamp = esMeta.stamp;
                            if (StringUtils.isNotBlank(inodeStamp)) {
                                esStamp = inodeStamp;
                            }
                            EsMeta esMeta0 = EsMeta.mapEsMeta(esMeta, obj);
                            esMeta0.stamp = esStamp;
                            String key0 = esMeta0.rocksKey(true, false, false, false);
                            writeBatch.put(key0.getBytes(), Json.encode(esMeta0).getBytes());
                        }
                    }
                } else if ((currObj.getString("versionNum").compareTo(toUpdateVersion) < 0)
                        || (currObj.getString("versionNum").compareTo(toUpdateVersion) == 0
                        && currObj.getString("stamp") != null && currObj.getString("stamp").compareTo(esMeta.stamp) < 0)) {
                    writeBatch.put(key.getBytes(), value.getBytes());
                }
            }
            if (StringUtils.isNotBlank(linkObjNames)) {
                String linkKey = EsMeta.getLinkKey(esMeta);
                EsMeta newLinkMeta = EsMeta.mapLinkMeta(esMeta, linkObjNames);
                newLinkMeta.stamp = stamp;
                byte[] linkBytes = writeBatch.getFromBatchAndDB(db, linkKey.getBytes());
                if (linkBytes == null) {
                    writeBatch.put(linkKey.getBytes(), Json.encode(newLinkMeta).getBytes());
                } else {
                    EsMeta linkMeta = Json.decodeValue(new String(linkBytes), EsMeta.class);
                    String[] objNames = new String[0];
                    if (StringUtils.isNotBlank(linkMeta.objName)) {
                        objNames = Json.decodeValue(linkMeta.objName, String[].class);
                    }
                    String[] newObjNames = Json.decodeValue(linkObjNames, String[].class);
                    Set<String> tmpSet = new HashSet<>();
                    Collections.addAll(tmpSet, objNames);
                    Collections.addAll(tmpSet, newObjNames);
                    String[] lastObjNames = tmpSet.toArray(new String[0]);
                    linkMeta.versionNum = esMeta.versionNum;
                    linkMeta.objName = Json.encode(lastObjNames);
                    writeBatch.put(linkKey.getBytes(), Json.encode(linkMeta).getBytes());
                }
            }
            if (EsMeta.needPut(key, esMeta) && !isReName) {
                String inodeKey = EsMeta.getInodeKey(esMeta);
                EsMeta newInodeMeta = EsMeta.mapInodeMeta(esMeta);
                newInodeMeta.stamp = stamp;
                byte[] inodeBytes = writeBatch.getFromBatchAndDB(db, inodeKey.getBytes());
                if (inodeBytes == null) {
                    writeBatch.put(inodeKey.getBytes(), Json.encode(newInodeMeta).getBytes());
                } else {
                    EsMeta inodeMeta = Json.decodeValue(new String(inodeBytes), EsMeta.class);
                    if (addLink) {
                        inodeMeta.stamp = stamp;
                        writeBatch.put(inodeKey.getBytes(), Json.encode(inodeMeta).getBytes());
                    } else if (StringUtils.isNotBlank(inodeMeta.versionNum)
                            && inodeMeta.versionNum.compareTo(esMeta.versionNum) < 0) {
                        writeBatch.put(inodeKey.getBytes(), Json.encode(newInodeMeta).getBytes());
                    }
                }
            }
            res.onNext(SUCCESS_PAYLOAD);
        };
        return BatchRocksDB.customizeOperate(lun, esMetaKey.hashCode(), consumer, res)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> delEsRocks(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String value = msg.get("value");
        List<String> esMetaList = JSON.parseObject(value, new com.alibaba.fastjson.TypeReference<List<String>>() {
        });
        MonoProcessor<Payload> res = MonoProcessor.create();
        RequestConsumer consumer = (db, writeBatch, request) -> {
            for (String rocksKey : esMetaList) {
                if (rocksKey.startsWith(EsMeta.SPLIT_FLAG)) {
                    String[] split = rocksKey.split(EsMeta.SPLIT_FLAG);
                    if (split.length == 4) {
                        rocksKey = split[1];
                        String versionNum = split[2];
                        String stamp = split[3];
                        byte[] curRecValue = writeBatch.getFromBatchAndDB(db, rocksKey.getBytes());
                        if (curRecValue != null) {
                            EsMeta inodeMeta = Json.decodeValue(new String(curRecValue), EsMeta.class);
                            if (StringUtils.isNotBlank(inodeMeta.versionNum)
                                    && inodeMeta.versionNum.compareTo(versionNum) <= 0
                                    && StringUtils.isNotBlank(inodeMeta.stamp) &&
                                    Long.parseLong(inodeMeta.stamp) <= Long.parseLong(stamp)) {
                                writeBatch.delete(rocksKey.getBytes());
                            }
                        }
                    }
                } else {
                    byte[] curRecValue = writeBatch.getFromBatchAndDB(db, rocksKey.getBytes());
                    if (curRecValue != null) {
                        writeBatch.delete(rocksKey.getBytes());
                    }
                }
            }
            res.onNext(SUCCESS_PAYLOAD);
        };
        return BatchRocksDB.customizeOperate(lun, key.hashCode(), consumer, res)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> putSTSToken(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String value = msg.get("value");
        RequestConsumer consumer = (db, writeBatch, request) -> {
            writeBatch.put(key.getBytes(), value.getBytes());
            Credential credential = Json.decodeValue(value, Credential.class);
            StsCredentialSyncTask.credentialCache.put(credential.accessKey, credential);
        };

        return BatchRocksDB.customizeOperateMeta(lun, key.hashCode(), consumer)
                .map(b -> SUCCESS_PAYLOAD)
                .onErrorReturn(ERROR_PAYLOAD);

    }

    public static Mono<Payload> getSTSToken(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");

        try {
            MSRocksDB db = MSRocksDB.getRocksDB(lun);
            if (db == null) {
                return Mono.just(DefaultPayload.create(Json.encode(ERROR_STS_TOKEN), ERROR.name()));
            }
            if (!key.startsWith(ROCKS_STS_TOKEN_KEY)) {
                return Mono.just(DefaultPayload.create(Json.encode(ERROR_STS_TOKEN), ERROR.name()));
            } else {
                byte[] value = db.get(key.getBytes());
                if (value != null) {
                    Credential credential = Json.decodeValue(new String(value), Credential.class);
                    long cur = System.currentTimeMillis() / 1000;
                    if (cur > credential.deadline) {
                        return Mono.just(DefaultPayload.create(Json.encode(NOT_FOUND_STS_TOKEN), NOT_FOUND.name()));
                    } else {
                        return Mono.just(DefaultPayload.create(Json.encode(credential), SUCCESS.name()));
                    }
                } else {
                    return Mono.just(DefaultPayload.create(Json.encode(NOT_FOUND_STS_TOKEN), NOT_FOUND.name()));
                }
            }
        } catch (RocksDBException e) {
            log.error("", e);
            return Mono.just(DefaultPayload.create(Json.encode(ERROR_STS_TOKEN), ERROR.name()));
        }
    }

    public static Mono<Payload> deleteObjectAllMeta(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String value = msg.get("value");
        boolean versionEnabled = "Enabled".equals(msg.get("status")) || "Suspended".equals(msg.get("status"));
        String vnode = key.substring(0, key.indexOf(File.separator));
        MetaData metaData = Json.decodeValue(value, MetaData.class);
        String latestKey = Utils.getLatestMetaKey(vnode, metaData.bucket, metaData.key, metaData.snapshotMark);

        RequestConsumer consumer = (db, writeBatch, request) -> {

            Tuple3<String, String, String> tuple3 = Utils.getAllMetaDataKey(vnode, metaData.bucket, metaData.key, metaData.versionId, metaData.stamp, metaData.snapshotMark, metaData.inode);
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, tuple3.var2.getBytes());
            if (oldValue != null) {
                if (!versionEnabled) {
                    tuple3.var1 = Utils.getMetaDataKey(vnode, metaData.bucket, metaData.key, metaData.versionId, "0", metaData.snapshotMark);
                } else if ("null".equals(metaData.versionId)) {
                    writeBatch.delete(Utils.getMetaDataKey(vnode, metaData.bucket, metaData.key, metaData.versionId, "0", metaData.snapshotMark).getBytes());
                }
                MetaData oldMeta = Json.decodeValue(new String(oldValue), MetaData.class);
//                if (oldMeta.discard) {
//                    return;
//                }
                long capacity = 0;
                if (oldMeta.snapshotMark == null || oldMeta.partUploadId == null) {
                    capacity = oldMeta.deleteMarker || oldMeta.deleteMark ? 0 : Utils.getObjectSize(oldMeta);
                } else {
                    long tempCap = 0;
                    for (PartInfo partInfo : oldMeta.partInfos) {
                        tempCap += partInfo.getSnapshotMark().equals(oldMeta.snapshotMark) ? partInfo.partSize : 0;
                    }
                    capacity = oldMeta.deleteMarker || oldMeta.deleteMark ? 0 : tempCap;
                }
                long objNum = oldMeta.deleteMarker || oldMeta.deleteMark ? 0 : -1;
                writeBatch.delete(latestKey.getBytes());
                writeBatch.delete(tuple3.var1.getBytes());
                writeBatch.delete(tuple3.var2.getBytes());
                writeBatch.delete(tuple3.var3.getBytes());
                long finalObjNum = metaData.deleteMark && (oldMeta.deleteMark || oldMeta.deleteMarker) ? 0 : objNum;
                long finalCapacity = metaData.deleteMark && (oldMeta.deleteMark || oldMeta.deleteMarker) ? 0 : -capacity;
                updateCapacityInfo(writeBatch, metaData.bucket, metaData.key, vnode, finalObjNum, finalCapacity);
            }
        };
        int hashCode = getFinalHashCode(latestKey, metaData.snapshotMark != null);
        return BatchRocksDB.customizeOperateMetaForDelete(lun, hashCode, consumer)
                .map(b -> SUCCESS_PAYLOAD)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> bucketStartDoubleWrite(Payload payload) {
        try {
            SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
            String bucketName = msg.get("bucket");
            String sourceNode = msg.get("sourceNode");
            String goalNode = msg.get("goalNode");
            log.info("bucket {} vnode {} start double write -> {}", bucketName, sourceNode, goalNode);
            ObjectSplitTree objectSplitTree = StoragePoolFactory.getMetaStoragePool(bucketName).getBucketShardCache().get(bucketName);
            ShardingWorker.SHARDING_LOCK_SET.add(bucketName);
            boolean b = objectSplitTree.updateNodeState(sourceNode, 1, goalNode);
            long startId = id.incrementAndGet();
            return b ? Mono.just(DefaultPayload.create(String.valueOf(startId), SUCCESS.name())) : Mono.just(ERROR_PAYLOAD);
        } catch (Exception e) {
            return Mono.just(ERROR_PAYLOAD);
        }
    }

    public static Mono<Payload> updateBucketIndexTree(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String type = msg.get("type");
        if (msg.dataMap.containsKey("strategy")) {
            String strategy = msg.get("strategy");
            StorageStrategy.BUCKET_STRATEGY_NAME_MAP.put(msg.get("bucket"), strategy);
        }
        if ("0".equals(type)) {
            String bucketName = msg.get("bucket");
            String sourceNode = msg.get("sourceNode");
            String goalNode = msg.get("goalNode");
            String divider = msg.get("divider");
            ObjectSplitTree objectSplitTree = StoragePoolFactory.getMetaStoragePool(bucketName).getBucketShardCache().get(bucketName);
            boolean expansionNode = objectSplitTree.expansionNode(sourceNode, goalNode, divider, true);
            return expansionNode ? Mono.just(SUCCESS_PAYLOAD) : Mono.just(ERROR_PAYLOAD);
        } else if ("1".equals(type)) {
            String bucket = msg.get("bucket");
            String oldDivider = msg.get("oldDivider");
            String newDivider = msg.get("newDivider");
            String sourceNode = msg.get("sourceNode");
            String targetNode = msg.get("targetNode");
            ObjectSplitTree objectSplitTree = StoragePoolFactory.getMetaStoragePool(bucket).getBucketShardCache().get(bucket);
            boolean b = objectSplitTree.updateIntersectionSeparator(sourceNode, targetNode, oldDivider, newDivider) && objectSplitTree.updateNodeState(sourceNode, 0, null);
            return b ? Mono.just(SUCCESS_PAYLOAD) : Mono.just(ERROR_PAYLOAD);
        } else if ("2".equals(type)) {
            String bucket = msg.get("bucket");
            ObjectSplitTree objectSplitTree = new ObjectSplitTree(bucket, null);
            StoragePoolFactory.getMetaStoragePool(bucket).getBucketShardCache().put(bucket, objectSplitTree);
        } else if ("3".equals(type)) {
            String bucket = msg.get("bucket");
            StoragePoolFactory.getMetaStoragePool(bucket).getBucketShardCache().remove(bucket);
            BucketSyncSwitchCache.getInstance().remove(bucket);
        } else if ("4".equals(type)) {
            String bucket = msg.get("bucket");
            String sourceNode = msg.get("sourceNode");
            ObjectSplitTree objectSplitTree = StoragePoolFactory.getMetaStoragePool(bucket).getBucketShardCache().get(bucket);
            boolean b = objectSplitTree.updateNodeState(sourceNode, 0, null);
            return b ? Mono.just(SUCCESS_PAYLOAD) : Mono.just(ERROR_PAYLOAD);
        } else if ("5".equals(type)) {
            String bucket = msg.get("bucket");
            String sourceNode = msg.get("sourceNode");
            String targetNode = msg.get("targetNode");
            ObjectSplitTree objectSplitTree = StoragePoolFactory.getMetaStoragePool(bucket).getBucketShardCache().get(bucket);
            boolean b = objectSplitTree.merge(sourceNode, targetNode);
            return b ? Mono.just(SUCCESS_PAYLOAD) : Mono.just(ERROR_PAYLOAD);
        } else if ("6".equals(type)) {
            String bucketName = msg.get("bucket");
            String sourceNode = msg.get("sourceNode");
            String goalNode = msg.get("goalNode");
            String divider = msg.get("divider");
            ObjectSplitTree objectSplitTree = StoragePoolFactory.getMetaStoragePool(bucketName).getBucketShardCache().get(bucketName);
            boolean expansionNode = objectSplitTree.expansionNode(sourceNode, goalNode, divider, false);
            return expansionNode ? Mono.just(SUCCESS_PAYLOAD) : Mono.just(ERROR_PAYLOAD);
        }
        return Mono.just(SUCCESS_PAYLOAD);
    }

    public static Mono<Payload> addRootSecretKey(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String versionNum = msg.get("versionNum");
        String rootKey = msg.get("rootKey");
        String endTime = msg.get("endTime");
        boolean b = RootSecretKeyManager.addRootSecretKey(versionNum, rootKey, endTime);
        return b ? Mono.just(SUCCESS_PAYLOAD) : Mono.just(ERROR_PAYLOAD);
    }

    public static Mono<Payload> getRootKeyList() {
        if (RootSecretKeyManager.INIT_FLAG.get()) {
            Map<String, String> map = RootSecretKeyManager.getRootKeyList();
            return Mono.just(DefaultPayload.create(Json.encode(map), SUCCESS.name()));
        }
        //本节点的根密钥模块还未初始化完成，缓存中记录的密钥可能不全，直接返回失败
        return Mono.just(ERROR_PAYLOAD);
    }

    public static Mono<Payload> localIsRunningStop(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        Long curId = ErasureServer.id.incrementAndGet();
        if (msg.get("curId") != null) {
            curId = Long.parseLong(msg.get("curId"));
            log.info("receive curId:{}", curId);
        }
        if (runningMap.floorKey(curId) == null) {
            log.info("curId {} running is stopped!", curId);
            curId = 0L;
        }
        return Mono.just(DefaultPayload.create(String.valueOf(curId), SUCCESS.name()));
    }

    public static Mono<Payload> deleteComponentRecord(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        RequestConsumer consumer = (db, writeBatch, request) -> {
            writeBatch.delete(key.getBytes());
        };

        return BatchRocksDB.customizeOperateMeta(lun, key.hashCode(), consumer)
                .map(b -> SUCCESS_PAYLOAD)
                .doOnError(e -> log.error("", e))
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> getVersionNum(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");
        boolean isLast = Boolean.parseBoolean(msg.get("isLast"));
        String oldVersion = msg.get("oldVersion");
        long vnode = Long.parseLong(msg.get("vnode"));
        VersionUtil.updateState(vnode);

        return BucketSyncSwitchCache.isSyncSwitchOffMono(bucket)
                .map(isSyncSwitchOff -> {
                    String versionNum = isLast ? VersionUtil.getLastVersionNum(oldVersion, isSyncSwitchOff) : VersionUtil.getVersionNumMaybeUpdate(isSyncSwitchOff, vnode);
                    return DefaultPayload.create(versionNum, SUCCESS.name());
                });
    }

    public static Mono<Payload> inodeCacheHeart(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        int priority = Integer.parseInt(msg.get("priority"));
        int vNum = Integer.parseInt(msg.get("vNum"));
        boolean getState = "1".equals(msg.dataMap.getOrDefault("getState", "0"));
        Node node0 = Node.getInstance();
        if (node0 == null) {
            return Mono.just(ERROR_PAYLOAD);
        }

        //返回当前节点的CachedList和实时的list
        if (getState) {
            String res = Node.getInstance().getVnode(vNum).getVnodeStateAndList();
            return Mono.just(DefaultPayload.create(res, SUCCESS.name()));
        }

        //slave msg
        if (priority == -1) {
            if (Node.getInstance().getVnode(vNum).isMaster()) {
                return Mono.just(DefaultPayload.create(ServerConfig.getInstance().getHostUuid(), SUCCESS.name()));
            } else {
                return Mono.just(ERROR_PAYLOAD);
            }
        }

        long lastHeart = Long.parseLong(msg.get("lastHeart"));
        String node = msg.get("node");

        String heart = Node.getInstance().getVnode(vNum).getHeart(lastHeart, priority, node);

        return Mono.just(DefaultPayload.create(heart, SUCCESS.name()));
    }

    public static Mono<Payload> freshDAMasterInfo(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String code = msg.get("code");
        if (code.equals("0")) {
            // 更新主站点信息
            refreshMasterInfo();
        } else {
            log.info("freshDAMasterInfo failed, node:{}", code);
            return Mono.just(ERROR_PAYLOAD);
        }
//        int newMasterIndex = Integer.parseInt(msg.get(ArbitratorUtils.MASTER_CLUSTER_INDEX_HEADER));
//        long newTerm = Long.parseLong(msg.get(ArbitratorUtils.DA_TERM_HEADER));
//        synchronized (TERM) {
//            termChanged.compareAndSet(false, true);
//            MASTER_INDEX = newMasterIndex;
//            TERM.set(newTerm);
//        }

        log.info("freshDAMasterInfo success, newMasterIndex:{}, newTerm:{}", MASTER_INDEX, TERM.get());
        return Mono.just(SUCCESS_PAYLOAD);
    }

    public static Mono<Payload> ifPutDone(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String getMin = msg.get("getMinStamp");

        if ("1".equals(getMin)) {
            Map<String, String> resMap = new HashMap<>();
            for (Map.Entry<String, ConcurrentSkipListMap<String, MsHttpRequest>> entry : requestVersionNumMap.entrySet()) {
                String bucket = entry.getKey();
                ConcurrentSkipListMap<String, MsHttpRequest> listMap = entry.getValue();
                try {
                    String minVersionNum = listMap.firstKey();
                    String stamp = verNum2timeStamp(minVersionNum);
                    // record.header里的stamp在buildRecord时就初始化，因此会比接口中生成的更小一些，所以手动减去一个值，使其小于record里的stamp
                    long l = Long.parseLong(stamp) - 60L * 1000;
                    resMap.put(bucket, String.valueOf(l));
                } catch (NoSuchElementException e) {
                    resMap.remove(bucket);
                }
            }
            return Mono.just(DefaultPayload.create(Json.encode(resMap), PayloadMetaType.SUCCESS.name()));
        } else {
            String bucket = msg.get("bucket");
            String versionNum = msg.get("versionNum");
            ConcurrentSkipListMap<String, MsHttpRequest> listMap = requestVersionNumMap.get(bucket);
            // 是否还存在比开始同步扫描的versionNum之前进入接口并且还未完成的请求
            boolean res = true;
            if (listMap != null) {
                String lastVersionNum = listMap.keySet().floor(versionNum);
                if (lastVersionNum != null) {
                    log.info("requestVersionNumMap {}, {} , ver {}", bucket, requestVersionNumMap.get(bucket), versionNum);
                    res = false;
                }
            }
            return Mono.just(DefaultPayload.create(String.valueOf(res), PayloadMetaType.SUCCESS.name()));
        }
    }

    public static Mono<Payload> updateTime(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        long endStamp = Long.parseLong(msg.get("lifecycleEndStamp"));
        setEndStamp(endStamp);
        return Mono.just(DefaultPayload.create("true", PayloadMetaType.SUCCESS.name()));
    }

    static ReentrantReadWriteLock checkPointLock = new ReentrantReadWriteLock();

    public static Mono<Payload> createCheckPoint(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String poolQueueTag = msg.get("poolQueueTag");
        String uuid = msg.get("uuid");
        String disk = uuid + "@" + lun;
        String runningKey = "running_" + poolQueueTag;
        try {
            checkPointLock.writeLock().lock();
            if (CHECK_POINT_SUCCESS.equals(RebuildRabbitMq.getMaster().hget(runningKey, CHECK_POINT_STATUS_PREFIX + disk))) {
                return Mono.just(DefaultPayload.create(Boolean.TRUE.toString(), PayloadMetaType.SUCCESS.name()));
            }
            RebuildCheckpointUtil.createCheckPoint(lun);
            RebuildCheckpointUtil.createCheckPoint(getSyncRecordLun(lun));
            RebuildCheckpointUtil.createCheckPoint(getComponentRecordLun(lun));
            RebuildCheckpointUtil.createCheckPoint(getSTSTokenLun(lun));
            RebuildCheckpointUtil.createCheckPoint(getAggregateLun(lun));
            RebuildRabbitMq.getMaster().hset(runningKey, CHECK_POINT_STATUS_PREFIX + disk, CHECK_POINT_SUCCESS);
        } catch (Exception e) {
            log.error("create check point fail,lun:{}", lun, e);
            return Mono.just(DefaultPayload.create(ERROR_PAYLOAD));
        } finally {
            checkPointLock.writeLock().unlock();
        }
        return Mono.just(DefaultPayload.create(Boolean.TRUE.toString(), PayloadMetaType.SUCCESS.name()));
    }

    public static Mono<Payload> deleteCheckPoint(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String needCloseLun = msg.get("needCloseLun");
        String runningKey = msg.get("runningKey");
        String removeDisk = msg.get("removeDisk");//重平衡相关盘
        Set<String> needCloseLunSet = Json.decodeValue(needCloseLun, new TypeReference<Set<String>>() {
        });
        try {
            checkPointLock.writeLock().lock();
            Map<String, String> runnnigMap = RebuildRabbitMq.getMaster().hgetall(runningKey);
            String newRemoveDisk = runnnigMap.get("diskName");
            if (StringUtils.isNotBlank(newRemoveDisk) && !removeDisk.equals(newRemoveDisk)) {
                // 说明已经开始新的重构任务
                Set<String> createdCheckPoint = runnnigMap.keySet().stream()
                        .filter(kv -> kv.startsWith(RebuildCheckpointManager.CHECK_POINT_STATUS_PREFIX))
                        .map(key -> key.substring(RebuildCheckpointManager.CHECK_POINT_STATUS_PREFIX.length()))
                        .filter(lun -> lun.startsWith(ServerConfig.getInstance().getHostUuid()))
                        .map(lun -> lun.split("@")[1])
                        .collect(Collectors.toSet());
                if (!createdCheckPoint.isEmpty()) {
                    // 已经开始其他盘的重构
                    needCloseLunSet.removeIf(createdCheckPoint::contains);
                }
            }
            for (String lun : needCloseLunSet) {
                if (BatchRocksDB.errorLun.contains(lun)) {
                    continue;
                }
                RebuildCheckpointUtil.closeCheckPoint(lun);
                RebuildCheckpointUtil.closeCheckPoint(getSyncRecordLun(lun));
                RebuildCheckpointUtil.closeCheckPoint(getComponentRecordLun(lun));
                RebuildCheckpointUtil.closeCheckPoint(getSTSTokenLun(lun));
                RebuildCheckpointUtil.closeCheckPoint(getAggregateLun(lun));
            }
            return Mono.just(DefaultPayload.create(Boolean.TRUE.toString(), PayloadMetaType.SUCCESS.name()));
        } catch (Exception e) {
            return Mono.just(DefaultPayload.create(ERROR_PAYLOAD));
        } finally {
            checkPointLock.writeLock().unlock();
        }
    }

    public static Mono<Payload> deleteRocksKey(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String columnFamily = msg.get("cfName");
        ColumnFamilyHandle familyHandle = getColumnFamily(lun, columnFamily);
        RequestConsumer consumer = (db, writeBatch, request) -> {
            if (familyHandle != null) {
                writeBatch.delete(familyHandle, key.getBytes());
            } else {
                writeBatch.delete(key.getBytes());
            }
        };
        return BatchRocksDB.customizeOperateMeta(lun, key.hashCode(), consumer)
                .map(b -> SUCCESS_PAYLOAD)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> putAggregationMeta(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key"); // ?25187/SS_strategy_B/c78d4e86-7220-4239-9584-7138908e1ae3-0001
        String value = msg.get("value");
        RequestConsumer consumer = getPutAggregationMetaRequestConsumer(key, value);

        return BatchRocksDB.customizeOperateMeta(lun, key.hashCode(), consumer)
                .map(b -> SUCCESS_PAYLOAD)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    private static RequestConsumer getPutAggregationMetaRequestConsumer(String key, String value) {
        String vnode = key.split("/")[0].substring(1);
        String nameSpace = key.split("/")[1];
        String aggregationId = key.split("/")[2];
        String bitmapKey = AggregationUtils.getAggregateFileBitmapKey(vnode, nameSpace, aggregationId);
        AggregateFileMetadata fileMetadata = Json.decodeValue(value, AggregateFileMetadata.class);
        return (db, writeBatch, request) -> {
            writeBatch.put(key.getBytes(), value.getBytes());
            if (fileMetadata.deleteMark) {
                writeBatch.delete(bitmapKey.getBytes());
                return;
            }
            int size = fileMetadata.getSegmentKeys().length;
            BitSet bitSet = new BitSet(size);
            bitSet.set(0, size);
            byte[] byteArray = bitSet.toByteArray();
            writeBatch.put(bitmapKey.getBytes(), byteArray);
        };
    }

    public static Mono<Payload> freeAggregationSpace(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key"); // ?34889/SS_strategy_B/02ada39b-679c-44de-b3a5-1e641db8586d-0002:10000:40
        String bitmap = msg.get("value");
        String[] strings = key.split(":");
        String realKey = strings[0];
        String mapKey = ROCKS_SPECIAL_KEY + realKey.substring(1);
        RequestConsumer consumer = (db, writeBatch, request) -> {
            byte[] value = writeBatch.getFromBatchAndDB(db, realKey.getBytes());
            if (value != null) {
                AggregateFileMetadata fileMetadata = Json.decodeValue(new String(value), AggregateFileMetadata.class);
                if (!fileMetadata.deleteMark) {
                    byte[] mapBytes = writeBatch.getFromBatchAndDB(db, mapKey.getBytes());
                    BitSet oldBitSet = BitSet.valueOf(mapBytes);
                    if (strings.length == 3) {
                        int totalParts = Integer.parseInt(strings[1]);
                        int partNum = Integer.parseInt(strings[2]);
                        BitSet bitSet = new BitSet(totalParts);
                        bitSet.set(0, totalParts);
                        bitSet.clear(partNum);
                        oldBitSet.and(bitSet);
                    } else {
                        BitSet newBitSet = AggregationUtils.deserialize(bitmap == null ? "" : bitmap);
                        oldBitSet.and(newBitSet);
                    }
                    writeBatch.put(mapKey.getBytes(), oldBitSet.toByteArray());
                    String strategy = fileMetadata.getNamespace().substring(fileMetadata.getNamespace().indexOf("_") + 1);
                    double maxHoleRate = StorageStrategy.STRATEGY_NAMESPACE_MAP.get(strategy).getAggregateConfig().getMaxHoleRate();
                    // 空洞率超过阈值
                    if (AggregationUtils.calculateHoleRatio(fileMetadata, oldBitSet) >= maxHoleRate) {
                        Tuple3<String, String, String> info = AggregationUtils.getInfoFromAggregationKey(realKey);
                        String aggregationGcKey = AggregationUtils.getAggregateGcKey(info.var1, info.var2, info.var3);
                        // 写入mq中等待二次合并处理
                        writeBatch.put(MSRocksDB.getColumnFamily(lun, ColumnFamilyEnum.AGGREGATION_GC_FAMILY.getName()),
                                aggregationGcKey.getBytes(), AggregationUtils.serialize(oldBitSet).getBytes());
                    }
                }
            }
        };
        return BatchRocksDB.customizeOperateMeta(lun, realKey.hashCode(), consumer)
                .map(b -> SUCCESS_PAYLOAD)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> updateAggregationMeta(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String value = msg.get("value");
        String lun = msg.get("lun");
        String key = msg.get("key");
        String vnode = key.split("/")[0].substring(1);
        String nameSpace = key.split("/")[1];
        String aggregationId = key.split("/")[2];
        AggregateFileMetadata fileMetadata = Json.decodeValue(value, AggregateFileMetadata.class);
        BitSet bitSet;
        if (!fileMetadata.deleteMark) {
            if (fileMetadata.bitmap != null) {
                bitSet = AggregationUtils.deserialize(fileMetadata.bitmap);
            } else {
                bitSet = new BitSet(fileMetadata.getSegmentKeys().length);
                bitSet.set(0, fileMetadata.getSegmentKeys().length);
            }
        } else {
            bitSet = null;
        }
        fileMetadata.bitmap = null;
        value = Json.encode(fileMetadata);
        String oldVersion = msg.dataMap.getOrDefault("oldVersion", null);
        MonoProcessor<Payload> res = MonoProcessor.create();
        String finalValue = value;
        RequestConsumer consumer = (db, writeBatch, request) -> {
            byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, key.getBytes());
            if (StringUtils.isEmpty(oldVersion) || GetMetaResEnum.GET_ERROR.name().equals(oldVersion)) {
                res.onNext(ERROR_PAYLOAD);
                return;
            }
            if (oldValue == null) {
                if (!GetMetaResEnum.GET_NOT_FOUND.name().equals(oldVersion)) {
                    res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                    return;
                }
                writeBatch.put(key.getBytes(), finalValue.getBytes());
                if (bitSet != null) {
                    String bitmapKey = AggregationUtils.getAggregateFileBitmapKey(vnode, nameSpace, aggregationId);
                    byte[] oldBit = writeBatch.getFromBatchAndDB(db, bitmapKey.getBytes());
                    if (oldBit != null) {
                        BitSet oldBitSet = BitSet.valueOf(oldBit);
                        bitSet.and(oldBitSet);
                    }
                    writeBatch.put(AggregationUtils.getAggregateFileBitmapKey(vnode, nameSpace, aggregationId).getBytes(), bitSet.toByteArray());
                } else {
                    writeBatch.delete(AggregationUtils.getAggregateFileBitmapKey(vnode, nameSpace, aggregationId).getBytes());
                }
            } else {
                JsonObject currentValue = new JsonObject(new String(oldValue));
                String currentVersion = currentValue.getString("versionNum");
                currentValue.remove("versionNum");
                JsonObject updateValue = new JsonObject(finalValue);
                String updateVersion = updateValue.getString("versionNum");
                updateValue.remove("versionNum");
                boolean delete = currentValue.containsKey("deleteMark") && currentValue.getBoolean("deleteMark");
                if (GetMetaResEnum.GET_NOT_FOUND.name().equals(oldVersion) && !delete) {
                    res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                    return;
                } else if (!currentVersion.equals(oldVersion)) {
                    res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                    return;
                }
                //判断除versionNum外要更新的元数据内容上是否有变化
                if (currentValue.equals(updateValue)) {
                    //比较新旧元数据中的versionNum
                    if (updateVersion.compareTo(currentVersion) > 0) {
                        writeBatch.put(key.getBytes(), finalValue.getBytes());
                        if (bitSet != null) {
                            String bitmapKey = AggregationUtils.getAggregateFileBitmapKey(vnode, nameSpace, aggregationId);
                            byte[] oldBit = writeBatch.getFromBatchAndDB(db, bitmapKey.getBytes());
                            if (oldBit != null) {
                                BitSet oldBitSet = BitSet.valueOf(oldBit);
                                bitSet.and(oldBitSet);
                            }
                            writeBatch.put(AggregationUtils.getAggregateFileBitmapKey(vnode, nameSpace, aggregationId).getBytes(), bitSet.toByteArray());
                        } else {
                            writeBatch.delete(AggregationUtils.getAggregateFileBitmapKey(vnode, nameSpace, aggregationId).getBytes());
                        }
                    }
                } else {
                    //元数据内容有更新时，比较现在db中的versionNum和原请求的versionNum（非新元数据中的versionNum）
                    if (currentVersion.compareTo(oldVersion) <= 0) {
                        writeBatch.put(key.getBytes(), finalValue.getBytes());
                        if (bitSet != null) {
                            String bitmapKey = AggregationUtils.getAggregateFileBitmapKey(vnode, nameSpace, aggregationId);
                            byte[] oldBit = writeBatch.getFromBatchAndDB(db, bitmapKey.getBytes());
                            if (oldBit != null) {
                                BitSet oldBitSet = BitSet.valueOf(oldBit);
                                bitSet.and(oldBitSet);
                            }
                            writeBatch.put(AggregationUtils.getAggregateFileBitmapKey(vnode, nameSpace, aggregationId).getBytes(), bitSet.toByteArray());
                        } else {
                            writeBatch.delete(AggregationUtils.getAggregateFileBitmapKey(vnode, nameSpace, aggregationId).getBytes());
                        }
                    } else {
                        //元数据被覆盖
                        res.onNext(DefaultPayload.create("", META_WRITEED.name()));
                        return;
                    }
                }
            }

            res.onNext(SUCCESS_PAYLOAD);
        };
        int hashCode = getFinalHashCode(key, false);
        return BatchRocksDB.customizeOperateMetaLowPriority(lun, hashCode, consumer, res)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> recordAggregationUndoLog(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String lun = msg.get("lun");
        String key = msg.get("key");
        String value = msg.get("value");
        RequestConsumer consumer = (db, writeBatch, request) -> {
            writeBatch.put(key.getBytes(), value.getBytes());
        };
        return BatchRocksDB.customizeOperateMeta(lun, key.hashCode(), consumer)
                .map(b -> SUCCESS_PAYLOAD)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    public static Mono<Payload> checkLunStatus(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String node = msg.get("nodeUuid");
        String lun = msg.get("lun");
        if (null == MSRocksDB.getRocksDB(lun) || RemovedDisk.getInstance().contains(node + "@" + lun)) {
            return Mono.just(ERROR_PAYLOAD);
        }
        return Mono.just(SUCCESS_PAYLOAD);
    }

    public static Mono<Payload> deleteFilesInRange(Payload payload) {
        try {
            SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
            String lun = msg.get("lun");
            List<byte[]> bytes = Json.decodeValue(msg.get("range"), new TypeReference<List<String>>() {
            }).stream().map(String::getBytes).collect(Collectors.toList());
            MSRocksDB rocksDB = getRocksDB(lun);
            if (null == rocksDB) {
                return Mono.just(ERROR_PAYLOAD);
            }
            rocksDB.deleteFilesInRangesDefaultColumnFamily(bytes, false);
            return Mono.just(SUCCESS_PAYLOAD);
        } catch (RocksDBException e) {
            log.error("", e);
            return Mono.just(ERROR_PAYLOAD);
        }
    }

    public static Mono<Payload> updateFsIdentity(Payload payload) {
        try {
            SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
            String param = msg.get("param");
            JsonObject map = new JsonObject(param);

            String s3Id = map.getString(FsConstants.ACLConstants.S3_ID);
            String uid = map.getString(FsConstants.ACLConstants.UID);
            String masterGid = map.getString(FsConstants.ACLConstants.MASTER_GID);
            JsonArray gids = map.getJsonArray(FsConstants.ACLConstants.GIDS);

            ACLUtils.userInfo.compute(s3Id, (k, v) -> {
                if (v != null) {
                    //存在s3Id的对应信息则更新
                    int oldUid = v.getUid();
                    int oldGid = v.getGid();

                    if (StringUtils.isNotEmpty(uid)) {
                        v.setUid(Integer.parseInt(uid));
                        v.setUserSid(FSIdentity.getUserSIDByUid(Integer.parseInt(uid)));
                        synchronized (ACLUtils.uidToS3ID) {
                            if (oldGid != 0) {
                                ACLUtils.uidToS3ID.remove(oldUid);
                            }
                            ACLUtils.uidToS3ID.put(Integer.parseInt(uid), s3Id);
                        }
                    }

                    if (StringUtils.isNotEmpty(masterGid)) {
                        v.setGid(Integer.parseInt(masterGid));
                        v.setGroupSid(FSIdentity.getGroupSIDByGid(Integer.parseInt(masterGid)));
                        synchronized (ACLUtils.gidToS3ID) {
                            ACLUtils.gidToS3ID.remove(oldGid);
                            ACLUtils.gidToS3ID.put(Integer.parseInt(masterGid), s3Id);
                        }
                    }

                    if (null != gids && !gids.isEmpty()) {
                        Set<Integer> set = new HashSet<>();
                        for (Object gid : gids) {
                            set.add((Integer) gid);
                        }
                        ACLUtils.s3IDToGids.compute(s3Id, (k0, v0) -> {
                            if (!set.isEmpty()) {
                                return set;
                            } else {
                                return v0;
                            }
                        });
                    }
                } else {
                    //不存在s3Id对应的信息则创建；各参数的校验已在后端包完成
                    int newUid = 0;
                    int newGid = 0;
                    if (StringUtils.isNotEmpty(uid)) {
                        synchronized (ACLUtils.uidToS3ID) {
                            newUid = Integer.parseInt(uid);
                            ACLUtils.uidToS3ID.put(newUid, s3Id);
                        }
                    }
                    if (StringUtils.isNotEmpty(masterGid)) {
                        synchronized (ACLUtils.gidToS3ID) {
                            newGid = Integer.parseInt(masterGid);
                            ACLUtils.gidToS3ID.put(newGid, s3Id);
                        }
                    }
                    if (null != gids && !gids.isEmpty()) {
                        Set<Integer> set = new HashSet<>();
                        for (Object gid : gids) {
                            set.add((Integer) gid);
                        }

                        ACLUtils.s3IDToGids.compute(s3Id, (k0, v0) -> {
                            if (!set.isEmpty()) {
                                return set;
                            } else {
                                return v0;
                            }
                        });
                    }

                    FSIdentity i = new FSIdentity(s3Id, newUid, newGid, FSIdentity.getUserSIDByUid(newUid), FSIdentity.getGroupSIDByGid(newGid));
                    ACLUtils.userInfo.put(s3Id, i);
                }

                return v;
            });


        } catch (Exception e) {
            log.error("update fsIdentity reqMsg error, ", e);
            return Mono.just(ERROR_PAYLOAD);
        }

        return Mono.just(SUCCESS_PAYLOAD);
    }

    public static Mono<Payload> startNFSACL(Payload payload) {
        try {
            SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
            boolean start = Boolean.parseBoolean(msg.get("start"));
            String proto = msg.get(PROTO);
            switch (proto) {
                case "nfs":
                    ACLUtils.NFS_ACL_START = start;
                    log.info("switch nfs acl: {}", ACLUtils.NFS_ACL_START);
                    break;
                case "cifs":
                    ACLUtils.CIFS_ACL_START = start;
                    log.info("switch cifs acl: {}", ACLUtils.CIFS_ACL_START);
                    break;
                default:
                    log.info("There is no {} switch", proto);
                    break;
            }
        } catch (Exception e) {
            log.error("switch nfs acl error, ", e);
            return Mono.just(ERROR_PAYLOAD);
        }

        return Mono.just(SUCCESS_PAYLOAD);
    }

    public static Mono<Payload> adjustFsIdRange(Payload payload) {
        try {
            SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
            String maxIdStr = msg.get(FS_ID_MAX);
            int maxId = Integer.parseInt(maxIdStr);
            ACLUtils.MAX_ID = maxId;
            log.info("adjust fs id range min: {} -> max: {}", ACLUtils.MIN_ID, ACLUtils.MAX_ID);
        } catch (Exception e) {
            log.error("adjust fs id range error, ", e);
            return Mono.just(ERROR_PAYLOAD);
        }

        return Mono.just(SUCCESS_PAYLOAD);
    }
}