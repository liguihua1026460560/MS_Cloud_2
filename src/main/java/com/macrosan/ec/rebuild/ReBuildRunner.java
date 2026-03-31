package com.macrosan.ec.rebuild;

import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.server.ListServerHandler;
import com.macrosan.message.jsonmsg.ChunkFile;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.storage.crypto.rootKey.RootSecretKeyUtils;
import com.macrosan.utils.aggregation.AggregationUtils;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.error.DiskErrorHandler.NFS_REBUILD_DEBUG;
import static com.macrosan.ec.rebuild.ReBuildTask.Type.*;

/**
 * 执行重建任务
 *
 * @author gaozhiyuan
 */
@Log4j2
public class ReBuildRunner {
    private static ReBuildRunner instance = new ReBuildRunner();
    private ThreadPoolExecutor reBuildThreadPool;
    public RebuildRabbitMq rabbitMq;
    public Scheduler publishThreads;
    public List<String> prefixList;

    private ReBuildRunner() {
        ThreadFactory reBuildThreadFactory = new ThreadFactory() {
            AtomicInteger num = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ReBuild-Thread-" + num.getAndIncrement());
            }
        };

        reBuildThreadPool = new ThreadPoolExecutor(3, 3,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                reBuildThreadFactory);
        rabbitMq = new RebuildRabbitMq(reBuildThreadPool);
        publishThreads = Schedulers.newParallel("ReBuild-Publish-Thread", 3);
        prefixList = Arrays.asList(ROCKS_VERSION_PREFIX, ROCKS_LIFE_CYCLE_PREFIX, ROCKS_LATEST_KEY, ROCKS_COOKIE_KEY, ROCKS_AGGREGATION_RATE_PREFIX, ROCKS_AGGREGATION_UNDO_LOG_PREFIX);
    }

    public static synchronized ReBuildRunner getInstance() {
        return instance;
    }

    public Mono<Boolean> addObjTask(String errorVnode, FileMeta fileMeta, StoragePool pool, String disk, String vKey, MonoProcessor<Integer> res) {
        String objVnode = fileMeta.getKey().split("_")[0].replace("#", "");
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(objVnode).block();
        int errorIndex = -1;
        for (int i = 0; i < nodeList.size(); i++) {
            if (nodeList.get(i).var3.equalsIgnoreCase(errorVnode)) {
                errorIndex = i;
            }
        }
        String nodeListStr = Json.encode(nodeList);
        String[] diskLink = pool.getLink(objVnode);

        long fileSize = fileMeta.getSize();
        long endIndex = pool.getOriginalObjSize(fileSize) - 1;

        ReBuildTask task = new ReBuildTask(OBJECT_FILE, disk, objVnode, diskLink, pool, res);
        task.map.put("errorIndex", String.valueOf(errorIndex));
        task.map.put("fileSize", String.valueOf(fileMeta.getSize()));
        task.map.put("fileName", fileMeta.getFileName());
        task.map.put("metaKey", fileMeta.getMetaKey());
        task.map.put("endIndex", String.valueOf(endIndex));
        task.map.put("nodeList", nodeListStr);
        task.map.put("lun", nodeList.get(errorIndex).var2);
        task.map.put("fileKey", fileMeta.getKey());
        task.map.put("flushStamp", fileMeta.getFlushStamp());
        task.map.put("lastAccessStamp", fileMeta.getLastAccessStamp());
        task.map.put("fileOffset", String.valueOf(fileMeta.getFileOffset()));

        if (CryptoUtils.checkCryptoEnable(fileMeta.getCrypto())) {
            task.map.put("crypto", fileMeta.getCrypto());
            String sk = RootSecretKeyUtils.rootKeyDecrypt(fileMeta.getSecretKey(), fileMeta.getCryptoVersion());
            task.map.put("secretKey", sk);
        }
        if (StringUtils.isBlank(vKey)){
            rabbitMq.publish(task);
            return Mono.just(true);
        }
        task.map.put("vKey", vKey);
        return RebuildCache.getCache(task.disk).cacheWrite(task);
    }

    public Mono<Boolean> addMetaTask(Tuple2<String, JsonObject> tuple2, StoragePool pool, String disk, String vKey, MonoProcessor<Integer> res) {
        Tuple2<String, String> vnodeTuple = ListServerHandler.getVnode(tuple2.var1);
        String objVnode = vnodeTuple.var2;
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(objVnode).block();
        String nodeListStr = Json.encode(nodeList);
        String[] diskLink = pool.getLink(objVnode);
        ReBuildTask task;
        switch (vnodeTuple.var1) {
            case ROCKS_OBJ_META_DELETE_MARKER:
            case "":
                task = new ReBuildTask(OBJECT_META, disk, objVnode, diskLink, pool,res);
                MetaData metaData = Json.decodeValue(tuple2.var2.encode(), MetaData.class);
                task.map.put("bucket", metaData.bucket);
                task.map.put("object", metaData.key);
                task.map.put("versionId", metaData.versionId == null ? "null" : metaData.versionId);
                task.map.put("nodeList", nodeListStr);
                task.map.put("fileKey", tuple2.var1);
                Optional.ofNullable(metaData.snapshotMark).ifPresent(v -> task.map.put("snapshotMark",v));
                if (StringUtils.isBlank(vKey)){
                    rabbitMq.publish(task);
                    return Mono.just(true);
                }
                task.map.put("vKey", vKey);
                return RebuildCache.getCache(task.disk).cacheWrite(task);
            case "!/":
            case ":/":
                task = new ReBuildTask(COMPONENT_RECORD, disk, objVnode, diskLink, pool,res);
                task.map.put("value", tuple2.var2.toString());
                task.map.put("nodeList", nodeListStr);
                task.map.put("fileKey", tuple2.var1);
                if (StringUtils.isBlank(vKey)){
                    rabbitMq.publish(task);
                    return Mono.just(true);
                }
                task.map.put("vKey", vKey);
                return RebuildCache.getCache(task.disk).cacheWrite(task);
            case ROCKS_PART_PREFIX:
                task = new ReBuildTask(INIT_PART_UPLOAD, disk, objVnode, diskLink, pool,res);
                task.map.put("bucket", tuple2.var2.getString("bucket"));
                task.map.put("object", tuple2.var2.getString("object"));
                task.map.put("uploadId", tuple2.var2.getString("uploadId"));
                task.map.put("nodeList", nodeListStr);
                task.map.put("fileKey", tuple2.var1);
                Optional.ofNullable(tuple2.var2.getString("snapshotMark")).ifPresent(v -> task.map.put("snapshotMark",v));
                if (StringUtils.isBlank(vKey)){
                    rabbitMq.publish(task);
                    return Mono.just(true);
                }
                task.map.put("vKey", vKey);
                return RebuildCache.getCache(task.disk).cacheWrite(task);
            case ROCKS_PART_META_PREFIX:
                task = new ReBuildTask(PART_UPLOAD, disk, objVnode, diskLink, pool,res);
                task.map.put("value", tuple2.var2.toString());
                task.map.put("nodeList", nodeListStr);
                task.map.put("fileKey", tuple2.var1);
                if (StringUtils.isBlank(vKey)){
                    rabbitMq.publish(task);
                    return Mono.just(true);
                }
                task.map.put("vKey", vKey);
                return RebuildCache.getCache(task.disk).cacheWrite(task);
            case ROCKS_STATIC_PREFIX:
                task = new ReBuildTask(STATISTIC, disk, objVnode, diskLink, pool,res);
                task.map.put("value", tuple2.var2.toString());
                task.map.put("nodeList", nodeListStr);
                task.map.put("fileKey", tuple2.var1);
                if (StringUtils.isBlank(vKey)){
                    rabbitMq.publish(task);
                    return Mono.just(true);
                }
                task.map.put("vKey", vKey);
                return RebuildCache.getCache(task.disk).cacheWrite(task);
            case ROCKS_BUCKET_META_PREFIX:
                task = new ReBuildTask(BUCKET_STORAGE, disk, objVnode, diskLink, pool,res);
                task.map.put("nodeList", nodeListStr);
                task.map.put("fileKey", tuple2.var1);
                if (StringUtils.isBlank(vKey)){
                    rabbitMq.publish(task);
                    return Mono.just(true);
                }
                task.map.put("vKey", vKey);
                return RebuildCache.getCache(task.disk).cacheWrite(task);
            case ROCKS_UNSYNCHRONIZED_KEY:
                task = new ReBuildTask(SYNC_RECORD, disk, objVnode, diskLink, pool,res);
                task.map.put("value", tuple2.var2.toString());
                task.map.put("nodeList", nodeListStr);
                task.map.put("fileKey", tuple2.var1);
                if (StringUtils.isBlank(vKey)){
                    rabbitMq.publish(task);
                    return Mono.just(true);
                }
                task.map.put("vKey", vKey);
                return RebuildCache.getCache(task.disk).cacheWrite(task);
            case ROCKS_DEDUPLICATE_KEY:
                task = new ReBuildTask(DEDUP_INFO, disk, objVnode, diskLink, pool,res);
                task.map.put("nodeList", nodeListStr);
                task.map.put("value", tuple2.var2.toString());
                task.map.put("realKey", tuple2.var1);
                task.map.put("fileKey", tuple2.var1);
                if (StringUtils.isBlank(vKey)){
                    rabbitMq.publish(task);
                    return Mono.just(true);
                }
                task.map.put("vKey", vKey);
                return RebuildCache.getCache(task.disk).cacheWrite(task);
            case ROCKS_INODE_PREFIX:
                if (NFS_REBUILD_DEBUG) {
                    log.info("type: {}, vkey: {}, key: {}, value: {}", vnodeTuple.var1, vKey, tuple2.var1, tuple2.var2.toString());
                }
                task = new ReBuildTask(NFS_INODE, disk, objVnode, diskLink, pool, res);
                task.map.put("inodeKey", tuple2.var1);
                task.map.put("inodeValue", tuple2.var2.toString());
                task.map.put("fileKey", tuple2.var1);
                if (StringUtils.isBlank(vKey)){
                    rabbitMq.publish(task);
                    return Mono.just(true);
                }
                task.map.put("vKey", vKey);
                return RebuildCache.getCache(task.disk).cacheWrite(task);
            case ROCKS_CHUNK_FILE_KEY:
                ChunkFile chunkFile = Json.decodeValue(tuple2.var2.toString(), ChunkFile.class);
                if (NFS_REBUILD_DEBUG) {
                    log.info("type: {}, vkey: {}, key: {}, value: {}", vnodeTuple.var1, vKey, tuple2.var1, ChunkFile.printChunk(chunkFile));
                }
                task = new ReBuildTask(NFS_CHUNK, disk, objVnode, diskLink, pool, res);
                task.map.put("chunkKey", tuple2.var1);
                task.map.put("chunkValue", tuple2.var2.toString());
                task.map.put("fileKey", tuple2.var1);
                if (StringUtils.isBlank(vKey)){
                    rabbitMq.publish(task);
                    return Mono.just(true);
                }
                task.map.put("vKey", vKey);
                return RebuildCache.getCache(task.disk).cacheWrite(task);
            case ROCKS_STS_TOKEN_KEY:
                task = new ReBuildTask(STS_TOKEN, disk, objVnode, diskLink, pool, res);
                task.map.put("value", tuple2.var2.toString());
                task.map.put("fileKey", tuple2.var1);
                if (StringUtils.isBlank(vKey)){
                    rabbitMq.publish(task);
                    return Mono.just(true);
                }
                task.map.put("vKey", vKey);
                return RebuildCache.getCache(task.disk).cacheWrite(task);
            case ROCKS_AGGREGATION_META_PREFIX:
                task = new ReBuildTask(AGGREGATE_META, disk, objVnode, diskLink, pool, res);
                String aggregationId = tuple2.var2.getString("aggregationId");
                String namespace = tuple2.var2.getString("namespace");
                String vnode = StoragePoolFactory.getMetaStoragePool(namespace).getBucketVnodeId(aggregationId);
                String key = AggregationUtils.getAggregationKey(vnode, namespace, aggregationId);
                task.map.put("key", key);
                task.map.put("vKey", vKey);
                task.map.put("fileKey", tuple2.var1);
                return RebuildCache.getCache(task.disk).cacheWrite(task);
            case ROCKS_VERSION_PREFIX:
            case ROCKS_LIFE_CYCLE_PREFIX:
            case ROCKS_LATEST_KEY:
            case ROCKS_FILE_META_PREFIX:
            case ROCKS_COOKIE_KEY:
            case ROCKS_AGGREGATION_RATE_PREFIX:
            case ROCKS_AGGREGATION_UNDO_LOG_PREFIX:
                break;
            default:
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "get unknown  key prefix " + vnodeTuple.var1);
        }
        return Mono.just(true);
    }
}
