package com.macrosan.ec.server;

import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.doubleActive.arbitration.DAVersionUtils;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.rebuild.DiskStatusChecker;
import com.macrosan.filesystem.cache.InodeOperator;
import com.macrosan.filesystem.cifs.lease.LeaseServer;
import com.macrosan.filesystem.cifs.lock.CIFSLockServer;
import com.macrosan.filesystem.lock.LockServer;
import com.macrosan.filesystem.nfs.lock.NLMLockServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.LocalPayload;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.rsocket.server.MsPayload;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import com.macrosan.utils.ratelimiter.RecoverLimiter;
import io.netty.buffer.ByteBuf;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Publisher;
import org.rocksdb.IngestExternalFileOptions;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.component.pojo.ComponentRecord.ERROR_COMPONENT_RECORD;
import static com.macrosan.component.pojo.ComponentRecord.NOT_FOUND_COMPONENT_RECORD;
import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.ec.server.FsQuotaServerHandler.*;
import static com.macrosan.ec.server.ListServerHandler.*;
import static com.macrosan.ec.server.RequestResponseServerHandler.*;
import static com.macrosan.filesystem.quota.FSQuotaRealService.updateQuotaInfo;
import static com.macrosan.filesystem.utils.IpWhitelistUtils.updateNFSIpWhitelists;
import static com.macrosan.filesystem.utils.acl.ACLUtils.*;
import static com.macrosan.fs.AioChannel.AIO_SCHEDULER;
import static com.macrosan.message.jsonmsg.AggregateFileMetadata.ERROR_AGGREGATION_META;
import static com.macrosan.message.jsonmsg.AggregateFileMetadata.NOT_FOUND_AGGREGATION_META;
import static com.macrosan.message.jsonmsg.ChunkFile.ERROR_CHUNK;
import static com.macrosan.message.jsonmsg.ChunkFile.NOT_FOUND_CHUNK;
import static com.macrosan.message.jsonmsg.DedupMeta.ERROR_DEDUP_META;
import static com.macrosan.message.jsonmsg.DedupMeta.NOT_FOUND_DEDUP_META;
import static com.macrosan.message.jsonmsg.EsMeta.ERROR_ES_META;
import static com.macrosan.message.jsonmsg.EsMeta.NOT_FOUND_ES_META;
import static com.macrosan.message.jsonmsg.InitPartInfo.ERROR_INIT_PART_INFO;
import static com.macrosan.message.jsonmsg.InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.macrosan.message.jsonmsg.PartInfo.*;
import static com.macrosan.storage.PoolHealth.repairLun;

@Log4j2
public class ErasureServer extends AbstractRSocket {
    public final static Payload ERROR_PAYLOAD = DefaultPayload.create("", PayloadMetaType.ERROR.name());
    public final static Payload NOT_FOUND_PAYLOAD = DefaultPayload.create("", PayloadMetaType.NOT_FOUND.name());
    public final static Payload WAITING_PAYLOAD = DefaultPayload.create("", PayloadMetaType.META_WRITEED.name());
    public final static Payload FILE_ERROR_PAYLOAD = DefaultPayload.create("", PayloadMetaType.FILE_ERROR.name());
    public final static Payload SUCCESS_PAYLOAD = DefaultPayload.create("", PayloadMetaType.SUCCESS.name());
    public final static Payload CONTINUE_PAYLOAD = DefaultPayload.create("", PayloadMetaType.CONTINUE.name());
    public final static Payload NO_WRITE_PAYLOAD = DefaultPayload.create("", PayloadMetaType.NO_WRITE_PERMISSION.name());
    public final static Payload SYSTEM_ERROR_PAYLOAD = DefaultPayload.create("", PayloadMetaType.SYSTEM_ERROR.name());
    public final static Payload TIME_OUT_PAYLOAD = DefaultPayload.create("", PayloadMetaType.TIME_OUT.name());
    private final static ThreadFactory DISK_THREAD_FACTORY = new MsThreadFactory("disk");
    public static final MsExecutor DISK_EXECUTOR;
    public static final Scheduler DISK_SCHEDULER;
    public static Set<String> nonLunMap = new ConcurrentSkipListSet<>();

    public static void init() {

    }

    static {
        Scheduler scheduler = null;
        MsExecutor executor = null;
        try {
            if (ServerConfig.isUnify() && PROC_NUM < 8) {
                executor = new MsExecutor(PROC_NUM * 2, 1, DISK_THREAD_FACTORY);
            } else {
                executor = new MsExecutor(PROC_NUM * 2, 16, DISK_THREAD_FACTORY);
            }
            scheduler = Schedulers.fromExecutor(executor);
        } catch (Exception e) {
            log.error("", e);
        }

        DISK_EXECUTOR = executor;
        DISK_SCHEDULER = scheduler;
    }

    public enum PayloadMetaType {
        ERROR,
        NOT_FOUND,
        NO_WRITE_PERMISSION,
        SUCCESS,
        WAITING,
        META_WRITEED,
        CONTINUE,
        HAD_OBJECT,
        GET_OBJECT_META,
        START_PUT_OBJECT,
        START_BATCH_PUT,
        BATCH_PUT_OBJECT,
        PUT_OBJECT_META,
        UPDATE_OBJECT_META,
        UPDATE_OBJECT_META_UNLIMIT,  // 解除QoS限流的UPDATE_OBJECT_META
        PUT_OBJECT,
        COMPLETE_PUT_OBJECT,
        PUT_AND_COMPLETE_PUT_OBJECT,
        PUT_OBJECT_ONCE,
        PUT_EMPTY_OBJECT,
        PUT_DEDUPLICATE_META,
        GET_DEDUPLICATE_META,
        GET_FILEMETA,
        UPDATE_DEDUPLICATE_META,
        DELONE_DEDUPLICATE_META,
        MARK_DELETE_OBJECT,
        DELETE_OBJECT_META,
        DELETE_FILE,
        CHECK_OBJECT_FILE_READ,
        INIT_PART_UPLOAD,
        REPAIR_INIT_PART_UPLOAD,
        REPAIR_PART_UPLOAD,
        GET_PART_META,
        GET_PART_UPLOAD_META,
        GET_PART_INFO,
        START_PART_UPLOAD,
        PART_UPLOAD,
        COMPLETE_PART_UPLOAD,
        PART_UPLOAD_META,
        LIST_OBJECTS,
        LIST_VERSIONS,
        LIST_MULTI_PART_UPLOAD,
        LIST_PART,
        COMPLETE_PART_CHECK,
        DELETE_PART,
        LIST_PART_INFO_FILE,
        SYSTEM_ERROR,
        START_GET_OBJECT,
        GET_OBJECT,
        COMPLETE_GET_OBJECT,
        UPDATE_FILE_META,
        TIME_OUT,
        FILE_ERROR,
        MARK_ABORT_PARTS,
        MARK_COMPLETE_PARTS,
        HAS_PART_INFO_OR_META,
        GET_BUCKET_INFO,
        MERGE_TMP_BUCKET_INFO,
        PUT_BUCKET_INFO,
        DELETE_ROCKETS_VALUE,
        DELETE_UNSYNC_ROCKETS_VALUE,
        PUT_MINUTE_RECORD,
        UPLOAD_MONTH_RECORD,
        GET_RECORD_TO_MERGE,
        MERGE_RECORD,
        GET_TRAFFIC_STATISTICS,
        MIGRATE_PUT_ROCKS,
        MIGRATE_MERGE_ROCKS,
        MIGRATE_META,
        MIGRATE_REPLAY_DELETE,
        LIST_VNODE_OBJ,
        LIST_VNODE_META,
        LIST_VNODE_META_MARKER,
        UPDATE_DISK_VNODE,
        UPDATE_DISK_NODE,
        CREATE_REBUILD_QUEUE,
        WAIT_WRITE_DONE,
        LIST_LIFE_OBJECT,
        DEL_VERSION_DEL_MARKER,
        PUT_SYNC_RECORD,
        UPDATE_SYNC_RECORD,
        HAVA_SYNC_RECORD,
        LIST_SYNC_RECORD,
        LIST_SYNC_RECORD_CHECKPOINT,
        DEAL_HIS_CHECKPOINT,
        GET_DISK_USED_SIZE,
        REWRITE_PART_COPY_RECORD,
        UPDATE_REWRITE_PART_COPY_RECORD,
        GET_METADATA_BY_UPLOADID,
        UPDATE_BUCKET_CAPACITY,
        CP_SST_FILE,
        GET_LOGGING,
        PING,
        KEEPALIVE,
        HAS_SS_RECORD,
        QUERY_NODE_FILE_NUM,  // 新增用于查询节点下的数据、缓存盘文件量/容量比值
        UPDATE_BUCKET_STRATEGY,
        NOTIFY_RECOVER_QOS,   // 新增通知其它节点设置QoS限流
        NOTIFY_DELETE_QOS,    // 新增通知其它节点删除QoS限流
        START_ARBITRATOR,
        UPDATE_AFTER_INIT_RECORD,
        GET_UNSYNC_RECORD,
        GET_AFTER_INIT_RECORD,
        DELETE_OBJECT_ALL_META,
        START_DOUBLE_WRITE,
        UPDATE_BUCKET_INDEX,
        LIST_INVENTORY_VERSIONS,
        LIST_INVENTORY_CURRENT,
        LIST_INVENTORY_INCREMENT_VERSIONS,
        LIST_INVENTORY_INCREMENT_CURRENT,
        LIFECYCLE_OBJECT_META,
        ADD_ROOT_SECRET_KEY,
        GET_ROOT_KEY_LIST,
        LIST_SHARDING_META_OBJ,
        NODE_IS_RUNNING_STOP,
        LIST_COMPONENT_RECORD,
        PUT_MULTI_MEDIA_RECORD,
        DEL_COMPONENT_RECORD,
        GET_COMPONENT_RECORD,
        UPDATE_COMPONENT_RECORD,
        GET_VERSION_NUM,
        GET_VIDEO_OBJECT,
        PUT_INODE,
        GET_INODE,
        LIST_DIR_PLUS,
        LOOK_UP,
        LOOK_UP_RENAME,
        RENAME,
        CHECK_DIR,
        LOCK,
        UNLOCK,
        KEEP_LOCK,
        INODE_CACHE_OPT,
        INODE_CACHE_HEART,
        INODE_CACHE_HEART_FAIL,
        INODE_BUFFER,
        PUT_ES_META,
        GET_ES_META,
        FRESH_DA_TERM,
        IF_PUT_DONE,
        DEL_ES_META,
        GET_CHUNK,
        PUT_CHUNK,
        DEL_CHUNK,
        UPDATE_TIME,
        DEL_INODE,
        PUT_COOKIE,
        GET_COOKIE,
        PUT_STS_TOKEN,
        GET_STS_TOKEN,
        REBUILD_CREATE_CHECK_POINT,
        REBUILD_DELETE_CHECK_POINT,
        NLM_LOCK,
        NLM_UNLOCK,
        NLM_KEEP_LOCK,
        NLM_CANCEL,
        NLM_GRANTED,
        QUOTA_SCAN,
        UPDATE_QUOTA_INFO,
        SYNC_QUOTA_CACHE,
        DEL_QUOTA_INFO,
        GET_QUOTA_INFO,
        CIFS_BREAK_LEASE,
        CIFS_BREAK_LEASE_START,
        CIFS_BREAK_LEASE_END,
        CIFS_LOCK_CANCEL,
        CIFS_LOCK_GRANTED,
        DELETE_ROCKS_KEY,
        PUT_AGGREGATION_META,
        FREE_AGGREGATION_SPACE,
        UPDATE_AGGREGATION_META,
        GET_AGGREGATION_META,
        RECORD_AGGREGATION_UNDO_LOG,
        LIST_AGGREGATION_UNDO_LOG,
        UPDATE_ROOT_ACL,
        UPDATE_FS_IDENTITY,
        UPDATE_ALL_FS_IDENTITY,
        UPDATE_NFS_IP_WHITELISTS,
        START_NFS_ACL,
        ADJUST_FS_ID_RANGE,
        CHECK_LUN_STATUS,
        DELETE_FILES_IN_RANGE,
        WRITE_CACHE,
        FLUSH_WRITE_CACHE
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        try {
            //ping消息使用vertx线程响应，避免disk线程阻塞使rsocket心跳超过10秒响应而断开
            if ("PING".equals(payload.getMetadataUtf8())) {
                try {
                    return Mono.just(SUCCESS_PAYLOAD);
                } finally {
                    payload.release();
                }
            }
        } catch (Exception e) {
            log.info("process PING error, ", e);
        }

        if (payload instanceof MsPayload) {
            ((MsPayload) payload).loopIndex = RSocketClient.loopThreads.getIfAbsent(Thread.currentThread().getId(), -1);
        }
        return Mono.just(payload).publishOn(DISK_SCHEDULER).flatMap(this::realRequestResponse);
    }

    private static Set<PayloadMetaType> recoverMetaType = new HashSet<>();

    static {
        PayloadMetaType[] types = new PayloadMetaType[]{
                LIFECYCLE_OBJECT_META,
                UPDATE_OBJECT_META,
                REPAIR_INIT_PART_UPLOAD,
                REPAIR_PART_UPLOAD,
                MIGRATE_META,
                MIGRATE_PUT_ROCKS,
                MIGRATE_MERGE_ROCKS,
                MIGRATE_REPLAY_DELETE,
                LIST_VNODE_OBJ,
                LIST_VNODE_META,
                LIST_VNODE_META_MARKER,
                REWRITE_PART_COPY_RECORD,
                UPDATE_REWRITE_PART_COPY_RECORD,
        };
        recoverMetaType.addAll(Arrays.asList(types));
    }

    public Mono<Payload> realRequestResponse(Payload payload) {
        try {
            boolean local = payload instanceof LocalPayload;
            PayloadMetaType metaType = local ? ((LocalPayload) payload).type : PayloadMetaType.valueOf(payload.getMetadataUtf8());
            boolean recover = recoverMetaType.contains(metaType);
            return RecoverLimiter.getInstance().acquireMeta(recover, metaType)
                    .flatMap(v -> {
                        try {
                            switch (metaType) {
                                case GET_OBJECT_META:
                                    return getRocksValue(payload, ERROR_META, NOT_FOUND_META);
                                case MARK_DELETE_OBJECT:
                                    return markDeleteObject(payload);
                                case DELETE_FILE:
                                    return deleteFile(payload);
                                case CHECK_OBJECT_FILE_READ:
                                    return checkObjectFileRead(payload);
                                case PUT_OBJECT_META:
                                    return putObjectMeta(payload);
                                case UPDATE_OBJECT_META:
                                    return updateMeta(payload);
                                case UPDATE_OBJECT_META_UNLIMIT:
                                    return updateMeta(payload);
                                case PUT_DEDUPLICATE_META:
                                    return putDedupMeta(payload);
                                case DELONE_DEDUPLICATE_META:
                                    return delDedupMeta(payload);
                                case GET_DEDUPLICATE_META:
                                    return getRocksValue(payload, ERROR_DEDUP_META, NOT_FOUND_DEDUP_META);
                                case GET_FILEMETA:
                                    return getRocksValue(payload, "error", "not Found");
                                case UPDATE_DEDUPLICATE_META:
                                    return updateDedupMeta(payload);
                                case REPAIR_INIT_PART_UPLOAD:
                                    return updatePartInitValue(payload);
                                case REPAIR_PART_UPLOAD:
                                    return updateRocketsValue(payload);
                                case UPDATE_FILE_META:
                                    return updateFileMeta(payload);
                                case INIT_PART_UPLOAD:
                                case MARK_ABORT_PARTS:
                                    return initPartUpload(payload);
                                case GET_PART_META:
                                    return getRocksValue(payload, ERROR_INIT_PART_INFO, NO_SUCH_UPLOAD_ID_INIT_PART_INFO);
                                case GET_PART_UPLOAD_META:
                                    return getRocksValue(payload, ERROR_PART_INFO, NOT_FOUND_PART_INFO);
                                case GET_PART_INFO:
                                    return getRocksValue(payload, ERROR_PART_INFO, NO_SUCH_UPLOAD_ID_PART_INFO);
                                case GET_BUCKET_INFO:
                                    return getBucketInfo(payload);
                                case PUT_BUCKET_INFO:
                                    return updateRocketsValue(payload);
                                case MERGE_TMP_BUCKET_INFO:
                                    return mergeTmpBucketInfo(payload);
                                case DELETE_ROCKETS_VALUE:
                                    return deleteRocketsValue(payload);
                                case DELETE_UNSYNC_ROCKETS_VALUE:
                                    return deleteUnsyncRecord(payload);
                                case PART_UPLOAD_META:
                                    return putPartInfo(payload);
                                case LIST_MULTI_PART_UPLOAD:
                                    return ListServerHandler.listMultiPartUpload(payload);
                                case LIST_PART:
                                    return ListServerHandler.listPart(payload);
                                case DELETE_PART:
                                    return deletePart(payload);
                                case LIST_PART_INFO_FILE:
                                    return listPartInfoFileNames(payload);
                                case COMPLETE_PART_CHECK:
                                    return checkCompleteMultiPart(payload);
                                case MARK_COMPLETE_PARTS:
                                    return completeMultiPart(payload);
                                case LIST_OBJECTS:
                                    return ListServerHandler.listObjects(payload);
                                case LIST_LIFE_OBJECT:
                                    return ListServerHandler.listLifecycleObjects(payload);
                                case LIST_VERSIONS:
                                    return ListServerHandler.listVersions(payload);
                                case DELETE_OBJECT_META:
                                    return deleteMeta(payload);
                                case HAD_OBJECT:
                                    return hadObject(payload);
                                case HAS_PART_INFO_OR_META:
                                    return hasPartInfoOrMeta(payload);
                                case PUT_MINUTE_RECORD:
                                    return putMinuteRecord(payload);
                                case GET_RECORD_TO_MERGE:
                                    return getRecordToMerge(payload);
                                case MERGE_RECORD:
                                    return mergeRecord(payload);
                                case GET_TRAFFIC_STATISTICS:
                                    return getTrafficStatistics(payload);
                                case MIGRATE_MERGE_ROCKS:
                                    return migrateMergeRocks(payload);
                                case MIGRATE_PUT_ROCKS:
                                    return migratePutRocks(payload);
                                case MIGRATE_META:
                                    return migrateMeta(payload);
                                case MIGRATE_REPLAY_DELETE:
                                    return replayDelete(payload);
                                case LIST_VNODE_OBJ:
                                    return ListServerHandler.listVnodeObj(payload);
                                case LIST_VNODE_META:
                                    return ListServerHandler.listMetaObj(payload);
                                case LIST_VNODE_META_MARKER:
                                    return ListServerHandler.nextPrefix(payload);
                                case UPDATE_DISK_VNODE:
                                    return updateDisk(payload);
                                case UPDATE_DISK_NODE:
                                    return updateNode(payload);
                                case CREATE_REBUILD_QUEUE:
                                    return createRebuildQueue(payload);
                                case WAIT_WRITE_DONE:
                                    return waitRunningStop().map(b -> SUCCESS_PAYLOAD);
                                case PUT_SYNC_RECORD:
                                    return putRocksValue(payload);
                                case UPDATE_SYNC_RECORD:
                                    return updateSyncRecord(payload);
                                case HAVA_SYNC_RECORD:
                                    return haveSyncRecord(payload);
                                case LIST_SYNC_RECORD:
                                    return ListServerHandler.listSyncRecorder(payload);
                                case LIST_SYNC_RECORD_CHECKPOINT:
                                    return ListServerHandler.listSyncRecordCheckpoint(payload);
                                case DEAL_HIS_CHECKPOINT:
                                    return ListServerHandler.dealHisCheckPoint(payload);
                                case GET_DISK_USED_SIZE:
                                    return getUsedSize();
                                case REWRITE_PART_COPY_RECORD:
                                case UPDATE_REWRITE_PART_COPY_RECORD:
                                    return putPartCopyRecord(payload);
                                case GET_METADATA_BY_UPLOADID:
                                    return getMetaDataByUploadId(payload);
                                case UPDATE_BUCKET_CAPACITY:
                                    return updateBucketCap(payload);
                                case GET_LOGGING:
                                    return getLogging();
                                case PING:
                                    return Mono.just(SUCCESS_PAYLOAD);
                                case HAS_SS_RECORD:
                                    return hasSSRecord(payload);
                                case UPDATE_BUCKET_STRATEGY:
                                    return updateBucketStrategy(payload);
                                case QUERY_NODE_FILE_NUM:
                                    return queryNodeFileNum(payload);
                                case START_ARBITRATOR:
                                    return startArbitrator(payload);
                                case NOTIFY_RECOVER_QOS:
                                    return notifyQosSetting(payload);
                                case NOTIFY_DELETE_QOS:
                                    return notifyQosDelete(payload);
                                case UPDATE_AFTER_INIT_RECORD:
                                    return simplePutRocks(payload);
                                case GET_UNSYNC_RECORD:
                                    return getRocksValue(payload, UnSynchronizedRecord.ERROR_UNSYNC_RECORD, UnSynchronizedRecord.NOT_FOUND_UNSYNC_RECORD);
                                case DELETE_OBJECT_ALL_META:
                                    return deleteObjectAllMeta(payload);
                                case START_DOUBLE_WRITE:
                                    return bucketStartDoubleWrite(payload);
                                case UPDATE_BUCKET_INDEX:
                                    return updateBucketIndexTree(payload);
                                case LIST_INVENTORY_VERSIONS:
                                    return listInventoryVersions(payload);
                                case LIST_INVENTORY_CURRENT:
                                    return ListServerHandler.listInventoryCurrent(payload);
                                case LIST_INVENTORY_INCREMENT_VERSIONS:
                                    return listInventoryIncrementalVersions(payload);
                                case LIST_INVENTORY_INCREMENT_CURRENT:
                                    return ListServerHandler.listInventoryCurrent(payload);
                                case ADD_ROOT_SECRET_KEY:
                                    return addRootSecretKey(payload);
                                case GET_ROOT_KEY_LIST:
                                    return getRootKeyList();
                                case LIFECYCLE_OBJECT_META:
                                    return updateLifecycle(payload);
                                case LIST_SHARDING_META_OBJ:
                                    return listShardingMetaObj(payload);
                                case NODE_IS_RUNNING_STOP:
                                    return localIsRunningStop(payload);
                                case LIST_COMPONENT_RECORD:
                                    return ListServerHandler.listComponentTask(payload);
                                case PUT_MULTI_MEDIA_RECORD:
                                    return simplePutRocks(payload);
                                case DEL_COMPONENT_RECORD:
                                    return deleteComponentRecord(payload);
                                case GET_COMPONENT_RECORD:
                                    return getRocksValue(payload, ERROR_COMPONENT_RECORD, NOT_FOUND_COMPONENT_RECORD);
                                case UPDATE_COMPONENT_RECORD:
                                    return updateRocketsValue(payload);
                                case GET_VERSION_NUM:
                                    return RequestResponseServerHandler.getVersionNum(payload);
                                case GET_INODE:
                                    return getInode(payload);
                                case PUT_INODE:
                                    return putInode(payload);
                                case LIST_DIR_PLUS:
                                    return ListServerHandler.listDirPlus(payload);
                                case LOOK_UP:
                                    return lookup(payload);
                                case CHECK_DIR:
                                    return FsServerHandler.checkDir(payload);
                                case RENAME:
                                    return rename(payload);
                                case LOCK:
                                    return LockServer.lock(payload);
                                case UNLOCK:
                                    return LockServer.unlock(payload);
                                case KEEP_LOCK:
                                    return LockServer.keep(payload);
                                case INODE_CACHE_HEART:
                                    return inodeCacheHeart(payload);
                                case INODE_CACHE_OPT:
                                    return InodeOperator.mapToOperator(payload);
                                case PUT_ES_META:
                                    return putEsRocks(payload);
                                case GET_ES_META:
                                    return getRocksValue(payload, ERROR_ES_META, NOT_FOUND_ES_META);
                                case FRESH_DA_TERM:
                                    return freshDAMasterInfo(payload);
                                case IF_PUT_DONE:
                                    return ifPutDone(payload);
                                case DEL_ES_META:
                                    return delEsRocks(payload);
                                case GET_CHUNK:
                                    return getRocksValue(payload, ERROR_CHUNK, NOT_FOUND_CHUNK);
                                case PUT_CHUNK:
                                    return putChunk(payload);
                                case DEL_CHUNK:
                                    return delChunk(payload);
                                case UPDATE_TIME:
                                    return updateTime(payload);
                                case DEL_INODE:
                                    return delInode(payload);
                                case INODE_CACHE_HEART_FAIL:
                                    return heartFail(payload);
                                case GET_COOKIE:
                                    return findCookie(payload);
                                case PUT_COOKIE:
                                    return putCookie(payload);
                                case PUT_STS_TOKEN:
                                    return putSTSToken(payload);
                                case GET_STS_TOKEN:
                                    return getSTSToken(payload);
                                case REBUILD_CREATE_CHECK_POINT:
                                    return createCheckPoint(payload);
                                case REBUILD_DELETE_CHECK_POINT:
                                    return deleteCheckPoint(payload);
                                case NLM_LOCK:
                                    return NLMLockServer.lock(payload);
                                case NLM_UNLOCK:
                                    return NLMLockServer.unlock(payload);
                                case NLM_KEEP_LOCK:
                                    return NLMLockServer.keep(payload);
                                case NLM_CANCEL:
                                    return NLMLockServer.cancel(payload);
                                case NLM_GRANTED:
                                    return NLMLockServer.granted(payload);
                                case QUOTA_SCAN:
                                    return scanDir(payload);
                                case UPDATE_QUOTA_INFO:
                                    return updateQuotaInfo(payload);
                                case DEL_QUOTA_INFO:
                                    return delQuota(payload);
                                case GET_QUOTA_INFO:
                                    return getQuotaInfo(payload);
                                case SYNC_QUOTA_CACHE:
                                    return syncQuotaCache(payload);
                                case CIFS_BREAK_LEASE:
                                    return LeaseServer.breakLease(payload);
                                case CIFS_BREAK_LEASE_START:
                                    return LeaseServer.breakLeaseStart(payload);
                                case CIFS_BREAK_LEASE_END:
                                    return LeaseServer.breakLeaseEnd(payload);
                                case CIFS_LOCK_CANCEL:
                                    return CIFSLockServer.cancel(payload);
                                case CIFS_LOCK_GRANTED:
                                    return CIFSLockServer.granted(payload);
                                case DELETE_ROCKS_KEY:
                                    return deleteRocksKey(payload);
                                case FREE_AGGREGATION_SPACE:
                                    return freeAggregationSpace(payload);
                                case PUT_AGGREGATION_META:
                                    return putAggregationMeta(payload);
                                case GET_AGGREGATION_META:
                                    return getRocksValue(payload, ERROR_AGGREGATION_META, NOT_FOUND_AGGREGATION_META);
                                case UPDATE_AGGREGATION_META:
                                    return updateAggregationMeta(payload);
                                case RECORD_AGGREGATION_UNDO_LOG:
                                    return recordAggregationUndoLog(payload);
                                case LIST_AGGREGATION_UNDO_LOG:
                                    return ListServerHandler.listAggregationUndoLog(payload);
                                case GET_OBJECT:
                                    return ReadObjServer.readObj(payload);
                                case UPDATE_ROOT_ACL:
                                    return reloadRootCache(payload);
                                case UPDATE_FS_IDENTITY:
                                    return updateFsIdentity(payload);
                                case UPDATE_ALL_FS_IDENTITY:
                                    return updateAllFsIdentity0(payload);
                                case UPDATE_NFS_IP_WHITELISTS:
                                    return updateNFSIpWhitelists(payload);
                                case START_NFS_ACL:
                                    return startNFSACL(payload);
                                case CHECK_LUN_STATUS:
                                    return checkLunStatus(payload);
                                case DELETE_FILES_IN_RANGE:
                                    return deleteFilesInRange(payload);
                                case ADJUST_FS_ID_RANGE:
                                    return adjustFsIdRange(payload);
                                case WRITE_CACHE:
                                    return WriteCacheServer.writeCache(payload);
                                case FLUSH_WRITE_CACHE:
                                    return WriteCacheServer.flushWriteCache(payload);
                                case ERROR:
                                default:
                                    log.info("no such payload meta type:{}", metaType);
                                    return Mono.just(ERROR_PAYLOAD);
                            }
                        } catch (Exception e) {
                            if (!DAVersionUtils.canGet.get() && StringUtils.isNotBlank(e.getMessage()) && e.getMessage().contains("correct syncStamp")) {
                                return Mono.just(ERROR_PAYLOAD);
                            }
                            log.error("request response error.", e);
                            return Mono.just(ERROR_PAYLOAD);
                        } finally {
                            payload.release();
                        }
                    });
        } catch (Exception e) {
            log.error("request response error.", e);
            return Mono.just(ERROR_PAYLOAD);
        }
    }

    private RequestChannalHandler batchPutChannel(Flux<Payload> requestFlux, UnicastProcessor<Payload> responseFlux) {
        BatchUploadServerHandler uploadServerHandler = new BatchUploadServerHandler(responseFlux);
        boolean[] first = new boolean[]{true};

        requestFlux.subscribe(payload -> {
            try {
                if (first[0]) {
                    uploadServerHandler.start(payload);
                    first[0] = false;
                } else {
                    uploadServerHandler.put(payload);
                }
            } catch (Exception e) {
                log.error("", e);
                uploadServerHandler.timeOut();
                responseFlux.onNext(ERROR_PAYLOAD);
                responseFlux.onComplete();
            } finally {
                payload.release();
            }
        });

        return uploadServerHandler;
    }

    RequestChannalHandler putChannel(Flux<Payload> requestFlux, UnicastProcessor<Payload> responseFlux) {
        AioUploadServerHandler uploadServerHandler = new AioUploadServerHandler(responseFlux);

        requestFlux.doFinally(s -> {
            uploadServerHandler.timeOut();
        }).subscribe(payload -> {
            try {
                PayloadMetaType metaType = PayloadMetaType.valueOf(payload.getMetadataUtf8());
                switch (metaType) {
                    case START_PUT_OBJECT:
                    case START_PART_UPLOAD:
                        uploadServerHandler.start(payload);
                        break;
                    case PART_UPLOAD:
                    case PUT_OBJECT:
                        uploadServerHandler.put(payload);
                        break;
                    case COMPLETE_PART_UPLOAD:
                    case COMPLETE_PUT_OBJECT:
                        uploadServerHandler.complete();
                        break;
                    case ERROR:
                        uploadServerHandler.timeOut();
                        responseFlux.onNext(ERROR_PAYLOAD);
                        responseFlux.onComplete();
                        break;
                    case KEEPALIVE:
                        responseFlux.onNext(DefaultPayload.create("", KEEPALIVE.name()));
                        break;
                    default:
                        log.info("no such meta type");
                        responseFlux.onNext(ERROR_PAYLOAD);
                        responseFlux.onComplete();
                }
            } catch (Exception e) {
                if (e instanceof MsException && uploadServerHandler.isNullDevice) {
                    if (!nonLunMap.contains(uploadServerHandler.lun)) {
                        log.error("", e);
                        nonLunMap.add(uploadServerHandler.lun);
                    }
                } else {
                    String lun = e.getMessage().split(" ")[0];
                    if (!repairLun.contains(CUR_NODE + "@" + lun) && !DiskStatusChecker.isRebuildWaiter(lun)) {
                        log.error("", e);
                    }
                }
                uploadServerHandler.timeOut();
                responseFlux.onNext(ERROR_PAYLOAD);
                responseFlux.onComplete();
            } finally {
                payload.release();
            }
        });

        return uploadServerHandler;
    }

    private static final boolean AIO_GET;
    private static final Logger rebuildLog = LogManager.getLogger("RebuildLog.ErasureServer");
    public static final String CUR_NODE = ServerConfig.getInstance().getHostUuid();

    static {
        String aioFlag = System.getProperty("com.macrosan.aio.get");
        AIO_GET = (null == aioFlag) || Boolean.parseBoolean(aioFlag);
    }

    private GetServerHandler getChannel(Flux<Payload> requestFlux, UnicastProcessor<Payload> responseFlux) {
        GetServerHandler handler;
        if (AIO_GET) {
            handler = new AioGetServerHandler(responseFlux);
        } else {
            handler = new GetServerHandler(responseFlux);
        }
        boolean[] first = new boolean[]{true};

        GetServerHandler getServerHandler = handler;
        requestFlux.doFinally(s -> getServerHandler.timeOut()).subscribe(payload -> {
            try {
                PayloadMetaType metaType = PayloadMetaType.valueOf(payload.getMetadataUtf8());
                switch (metaType) {
//                    case START_GET_OBJECT:
//                        getServerHandler.start(payload);
//                        break;
                    case GET_OBJECT:
                        if (first[0]) {
                            getServerHandler.start(payload);
                            first[0] = false;
                        }
                        getServerHandler.read();
                        break;
                    case COMPLETE_GET_OBJECT:
                        responseFlux.onComplete();
                        break;
                    case ERROR:
                        responseFlux.onComplete();
                        break;
                    default:
                        responseFlux.onNext(ERROR_PAYLOAD);
                        responseFlux.onComplete();
                }
            } catch (Exception e) {
                if (e instanceof NoSuchFileException) {
                    rebuildLog.error(e);
                } else {
                    String lun = e.getMessage().split(" ")[1];
                    if (!repairLun.contains(CUR_NODE + "@" + lun) && !DiskStatusChecker.isRebuildWaiter(lun)) {
                        log.error("{}", e.getMessage());
                    }
                }
                responseFlux.onNext(ERROR_PAYLOAD);
                responseFlux.onComplete();
            } finally {
                payload.release();
            }
        });

        return getServerHandler;
    }

    private static LocalMigrateServer localMigrateServer = LocalMigrateServer.getInstance();
    private static MigrateServer migrateServer = MigrateServer.getInstance();
    public static AtomicLong id = new AtomicLong();
    public static ConcurrentSkipListMap<Long, Long> runningMap = new ConcurrentSkipListMap<>();

    public static Mono<Boolean> waitRunningStop() {
        long curId = ErasureServer.id.incrementAndGet();
        MonoProcessor<Boolean> processor = MonoProcessor.create();
        Disposable[] disposable = new Disposable[1];
        boolean[] stop = new boolean[]{false};
        disposable[0] = Flux.interval(Duration.ofSeconds(1L), ErasureServer.DISK_SCHEDULER)
                .subscribe(l -> {
                    if (ErasureServer.runningMap.floorKey(curId) == null) {
                        if (stop[0]) {
                            processor.onNext(true);
                            disposable[0].dispose();
                        } else {
                            stop[0] = true;
                        }
                    } else {
                        stop[0] = false;
                    }
                }, e -> {
                    log.error("", e);
                    processor.onNext(false);
                });

        return processor;
    }

    private static Flux<Payload> newRequest(SocketReqMsg msg, byte[] bytes) {
        Payload start = DefaultPayload.create(Json.encode(msg), START_PUT_OBJECT.name());
        Payload put = DefaultPayload.create(bytes, PUT_OBJECT.name().getBytes());
        Payload complete = DefaultPayload.create("", COMPLETE_PUT_OBJECT.name());
        return Flux.just(start, put, complete);
    }

    private static Flux<Payload> beforePut(SocketReqMsg msg, Flux<Payload> request) {
        return beforePut(msg, null, request);
    }

    private static Flux<Payload> beforePut(SocketReqMsg msg, byte[] bytes, Flux<Payload> request) {
        String lun = msg.get("lun");
        String fileName = msg.get("fileName");
        String metaKey = msg.get("metaKey");
        String compression = msg.get("compression");
        String flushStamp = msg.get("flushStamp");
        long fileOffset = -1;
        if (msg.getDataMap().containsKey("fileOffset")) {
            fileOffset = Long.parseLong(msg.get("fileOffset"));
        }
        int vnodeIndex = fileName.indexOf("_");
        int dirIndex = fileName.indexOf(File.separator);
        String vnode = fileName.substring(dirIndex + 1, vnodeIndex);

        if (localMigrateServer.start > 0 && localMigrateServer.getDstLun(lun, vnode) != null) {
            if (request == null) {
                request = newRequest(msg, bytes);
            }
            request = LocalMigrateServer.getInstance().putChannel(lun, fileName, metaKey, compression, flushStamp, fileOffset, request);
        } else if (migrateServer.start > 0 && migrateServer.getDstLunInfo(lun, vnode) != null) {
            if (request == null) {
                request = newRequest(msg, bytes);
            }
            //初始化request后再处理双写
            request = MigrateServer.getInstance().putChannel(lun, fileName, request);
        } else if (request != null) {
            long curId = id.incrementAndGet();
            runningMap.put(curId, 0L);
            request = request.doFinally(s -> {
                runningMap.remove(curId);
            });
        }

        return request;
    }

    private void onePutChannel(boolean local, Payload p, UnicastProcessor<Payload> responseFlux) {
        SocketReqMsg msg;
        byte[] bytes;

        if (local) {
            LocalPayload<Tuple2<SocketReqMsg, byte[]>> localPayload = (LocalPayload<Tuple2<SocketReqMsg, byte[]>>) p;
            Tuple2<SocketReqMsg, byte[]> tuple2 = localPayload.data;
            msg = tuple2.var1;
            bytes = tuple2.var2;
        } else {
            ByteBuf buf = p.data();
            msg = SocketReqMsg.toSocketReqMsg(buf);
            bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
        }

        //迁移双写暂时走旧流程
        Flux<Payload> request = beforePut(msg, bytes, null);
        if (request != null) {
            putChannel(request, responseFlux);
        } else {
            if (true) {
                OneUploadServerHandler handler = new OneUploadServerHandler(responseFlux, local);
                handler.start(msg);
                handler.complete(bytes);
            } else {

            }
        }
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        UnicastProcessor<Payload> requestFlux = UnicastProcessor.create(Queues.<Payload>unboundedMultiproducer().get());
        UnicastProcessor<Payload> responseFlux = UnicastProcessor.create(Queues.<Payload>unboundedMultiproducer().get());
        boolean[] first = new boolean[]{true};

        Disposable disposable = Flux.from(payloads)
                .publishOn(AIO_SCHEDULER)
                .subscribe(p -> {
                    if (first[0]) {
                        boolean local = p instanceof LocalPayload;
                        PayloadMetaType metaType = local ? ((LocalPayload) p).type : PayloadMetaType.valueOf(p.getMetadataUtf8());

                        //已经包含了所有数据，跳过timeout等流程
                        if (metaType == PUT_OBJECT_ONCE) {
                            try {
                                onePutChannel(local, p, responseFlux);
                            } catch (Exception e) {
                                log.error("", e);
                                responseFlux.onNext(ERROR_PAYLOAD);
                                responseFlux.onComplete();
                            } finally {
                                p.release();
                            }

                            return;
                        }

                        RequestChannalHandler[] handler = new RequestChannalHandler[]{null};
                        Flux<Payload> request = requestFlux.name("moss requestFlux").timeout(Duration.ofSeconds(60), DISK_SCHEDULER)
                                .doOnError(e -> {
                                    boolean onNextFlag = false;
                                    if (e instanceof TimeoutException) {
                                        log.error("timeout request");
                                        if (null != handler[0]) {
                                            handler[0].timeOut();
                                            if (handler[0] instanceof GetServerHandler) {
                                                responseFlux.onNext(TIME_OUT_PAYLOAD);
                                                onNextFlag = true;
                                            }
                                        }
                                    } else {
                                        log.error("", e);
                                    }
                                    if (!onNextFlag) {
                                        responseFlux.onNext(ERROR_PAYLOAD);
                                    }
                                    responseFlux.onComplete();
                                });
                        SocketReqMsg msg;
                        switch (metaType) {
                            case PUT_AND_COMPLETE_PUT_OBJECT:
                                ByteBuf byteBuf = p.sliceData();
                                byte[] lenBytes = new byte[8];
                                byteBuf.getBytes(0, lenBytes);
                                int startLen = (int) ECUtils.bytes2long(lenBytes);
                                byte[] startBytes = new byte[startLen];
                                byteBuf.getBytes(8, startBytes);
                                msg = Json.decodeValue(new String(startBytes), SocketReqMsg.class);
                                request = beforePut(msg, request);

                                int lastLen = byteBuf.readableBytes() - startBytes.length - 8;
                                byte[] bytes = new byte[lastLen];
                                byteBuf.getBytes(startBytes.length + 8, bytes);

                                requestFlux.onNext(DefaultPayload.create(startBytes, START_PUT_OBJECT.name().getBytes()));
                                requestFlux.onNext(DefaultPayload.create(bytes, PUT_OBJECT.name().getBytes()));
                                requestFlux.onNext(DefaultPayload.create("", COMPLETE_PUT_OBJECT.name()));
                                requestFlux.onComplete();
                                handler[0] = putChannel(request, responseFlux);
                                break;
                            case START_PUT_OBJECT:
                            case START_PART_UPLOAD:
                                msg = Json.decodeValue(p.getDataUtf8(), SocketReqMsg.class);
                                request = beforePut(msg, request);
                                handler[0] = putChannel(request, responseFlux);
                                break;
                            case START_BATCH_PUT:
                                handler[0] = batchPutChannel(requestFlux, responseFlux);
                                break;
                            case CP_SST_FILE:
                                putSstChannel(requestFlux, responseFlux);
                                break;
                            case PUT_EMPTY_OBJECT:
                                responseFlux.onNext(SUCCESS_PAYLOAD);
                                responseFlux.onComplete();
                                break;
                            default:
                                handler[0] = getChannel(request, responseFlux);
                        }

                        first[0] = false;
                    }

                    if (requestFlux.isDisposed()) {
                        p.release();
                    } else {
                        requestFlux.onNext(p);
                    }
                });

        return responseFlux.doFinally(a -> {
            requestFlux.onComplete();
            disposable.dispose();
        });
    }

    void putSstChannel(Flux<Payload> requestFlux, UnicastProcessor<Payload> responseFlux) {
        final FileOutputStream[] fo = {null};
        final FileChannel[] channel = new FileChannel[1];
        final String[] path = new String[1];
        final boolean[] first = {true};
        final String[] lun = new String[1];
        final String[] sstFileName = new String[1];
        requestFlux.doOnComplete(() -> {
                })
                .subscribe(p -> {
                    try {
                        if (first[0]) {
                            first[0] = false;
                            SocketReqMsg msg = Json.decodeValue(p.getDataUtf8(), SocketReqMsg.class);
                            lun[0] = msg.get("lun");
                            sstFileName[0] = msg.get("fileName");
                            path[0] = "/" + lun[0] + sstFileName[0];
                            fo[0] = new FileOutputStream(path[0]);
                            channel[0] = fo[0].getChannel();
                            return;
                        }
                        String data = p.getDataUtf8();
                        if ("end".equals(data)) {
                            fo[0].flush();
                            channel[0].close();
                            fo[0].close();
                            MSRocksDB.getRocksDB(lun[0]).ingestExternalFile(Collections.singletonList(path[0]),
                                    new IngestExternalFileOptions().setAllowBlockingFlush(false).setAllowGlobalSeqNo(false));
                            responseFlux.onComplete();
                            return;
                        }
                        p.getData().flip();
                        channel[0].write(p.getData());
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }

                });
    }
}
