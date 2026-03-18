package com.macrosan.ec.server;

import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.migrate.MigrateUtil;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.checksum.ChecksumProvider;
import io.netty.util.ReferenceCounted;
import io.rsocket.Payload;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;

/**
 * 加磁盘迁移过程中转发 写入源磁盘vnode的所有数据到目标盘
 *
 * @author gaozhiyuan
 */
@Log4j2
public class LocalMigrateServer {
    public volatile int start = 0;
    private Map<String, String> diskInMigrating = new ConcurrentHashMap<>();
    private static final AtomicIntegerFieldUpdater<LocalMigrateServer> UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(LocalMigrateServer.class, "start");

    private LocalMigrateServer() {
    }

    private static LocalMigrateServer instance = new LocalMigrateServer();

    public static LocalMigrateServer getInstance() {
        return instance;
    }

    public Mono<Boolean> startMigrate(String lun, String vnode, String dstLun) {
        if (lun.contains("index")) {
            diskInMigrating.put(MSRocksDB.getSTSTokenLun(lun) + '$' + vnode, MSRocksDB.getSTSTokenLun(dstLun));
            diskInMigrating.put(MSRocksDB.getAggregateLun(lun) + '$' + vnode, MSRocksDB.getAggregateLun(dstLun));
        }
        diskInMigrating.put(lun + '$' + vnode, dstLun);
        UPDATER.incrementAndGet(this);

        return ErasureServer.waitRunningStop();
    }

    public Flux<Tuple2<byte[], byte[]>> endMigrate(String lun, String vnode) {
        if (lun.contains("index")) {
            diskInMigrating.remove(MSRocksDB.getSTSTokenLun(lun) + '$' + vnode);
            diskInMigrating.remove(MSRocksDB.getAggregateLun(lun) + '$' + vnode);
        }
        diskInMigrating.remove(lun + '$' + vnode);
        UPDATER.decrementAndGet(this);

        String queueKey = "local-" + lun + '$' + vnode;
        return ErasureServer.waitRunningStop().flatMapMany(b -> MigrateUtil.iterator(queueKey));
    }

    public String getDstLun(String lun, String vnode) {
        return diskInMigrating.get(lun + '$' + vnode);
    }

    public void addDelete(String lun, String vnode, Tuple2<byte[], byte[]> delete) {
        String queueKey = "local-" + lun + '$' + vnode;
        MigrateUtil.add(queueKey, delete.var1, delete.var2);
    }

    public void addCopy(String lun, String vnode, Tuple2<byte[], byte[]> copy) {
        String queueKey = "local-copy-" + lun + '$' + vnode;
        MigrateUtil.add(queueKey, copy.var1, copy.var2);
    }

    public Flux<Tuple2<byte[], byte[]>> endMigrateThenCopy(String lun, String vnode) {
        String queueKey = "local-copy-" + lun + '$' + vnode;
        return ErasureServer.waitRunningStop().flatMapMany(b -> MigrateUtil.iterator(queueKey));
    }

    public Flux<Payload> putChannel(String lun, String fileName, String metaKey, String compression, String flushStamp, String lastAccessStamp, long fileOffset, Flux<Payload> requestFlux) {
        int vnodeIndex = fileName.indexOf("_");
        int dirIndex = fileName.indexOf(File.separator);
        String key = lun + '$' + fileName.substring(dirIndex + 1, vnodeIndex);
        String dstLun = diskInMigrating.get(key);

        if (null == dstLun) {
            return requestFlux;
        }

        UnicastProcessor<Payload> responseFlux = UnicastProcessor.create();
        AioUploadServerHandler uploadServerHandler = new AioUploadServerHandler(responseFlux);
        responseFlux.subscribe(ReferenceCounted::release);

        uploadServerHandler.lun = dstLun;
        uploadServerHandler.fileName = fileName;
        uploadServerHandler.path = File.separator + dstLun + File.separator + fileName;
        uploadServerHandler.checksumProvider = ChecksumProvider.create();
        uploadServerHandler.compression = compression;
        uploadServerHandler.fileMeta = new FileMeta()
                .setMetaKey(metaKey)
                .setFileName(fileName)
                .setSmallFile(false)
                .setLun(lun)
                .setFlushStamp(flushStamp);
        if (StringUtils.isNotEmpty(lastAccessStamp)) {
            uploadServerHandler.fileMeta.setLastAccessStamp(lastAccessStamp);
        }
        if (fileOffset != -1) {
            uploadServerHandler.fileMeta.setFileOffset(fileOffset);
        }

        requestFlux = requestFlux.flatMap(payload -> {
            try {
                ErasureServer.PayloadMetaType metaType = ErasureServer.PayloadMetaType.valueOf(payload.getMetadataUtf8());
                switch (metaType) {
                    case START_PUT_OBJECT:
                    case START_PART_UPLOAD:
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
                        break;
                    default:
                        log.info("no suck meta type");
                }
            } catch (Exception e) {
                log.error("", e);
                uploadServerHandler.timeOut();
                return Mono.just(ERROR_PAYLOAD);
            }
            return Mono.just(payload);
        });

        return requestFlux;
    }

    public static String getVnode(String key) {
        try {
            int vnodeLeftIndex, vnodeRightIndex;
            if (key.startsWith(ROCKS_BUCKET_META_PREFIX)) {
                vnodeLeftIndex = ROCKS_BUCKET_META_PREFIX.length();
                vnodeRightIndex = key.indexOf(File.separator);
            } else if (key.startsWith(ROCKS_FILE_META_PREFIX)) {
                vnodeLeftIndex = ROCKS_FILE_META_PREFIX.length();
                vnodeRightIndex = key.indexOf("_");
            } else if (key.startsWith(ROCKS_PART_META_PREFIX)) {
                vnodeLeftIndex = ROCKS_PART_META_PREFIX.length();
                vnodeRightIndex = key.indexOf(File.separator);
            } else if (key.startsWith(ROCKS_COMPONENT_IMAGE_KEY + File.separator)) {
                return key.split(File.separator)[1];
            } else if (key.startsWith(ROCKS_PART_PREFIX)) {
                vnodeLeftIndex = ROCKS_PART_PREFIX.length();
                vnodeRightIndex = key.indexOf(File.separator);
            } else if (key.startsWith(ROCKS_STATIC_PREFIX)) {
                vnodeLeftIndex = ROCKS_STATIC_PREFIX.length();
                vnodeRightIndex = key.substring(vnodeLeftIndex).indexOf('&') + vnodeLeftIndex;
            } else if (key.startsWith(ROCKS_VERSION_PREFIX)) {
                vnodeLeftIndex = ROCKS_VERSION_PREFIX.length();
                vnodeRightIndex = key.indexOf(File.separator);
            } else if (key.startsWith(ROCKS_LIFE_CYCLE_PREFIX)) {
                vnodeLeftIndex = ROCKS_LIFE_CYCLE_PREFIX.length();
                vnodeRightIndex = key.indexOf(File.separator);
            } else if (key.startsWith(ROCKS_LATEST_KEY)) {
                vnodeLeftIndex = ROCKS_LATEST_KEY.length();
                vnodeRightIndex = key.indexOf(File.separator);
            } else if (key.startsWith(ROCKS_COMPONENT_VIDEO_KEY)) {
                return key.split(File.separator)[1];
            } else if (key.startsWith(ROCKS_UNSYNCHRONIZED_KEY)) {
                vnodeLeftIndex = ROCKS_UNSYNCHRONIZED_KEY.length();
                vnodeRightIndex = key.indexOf(File.separator);
            } else if (key.startsWith(ROCKS_INODE_PREFIX)) {
                vnodeLeftIndex = ROCKS_INODE_PREFIX.length();
                vnodeRightIndex = key.indexOf(File.separator);
            } else if (key.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                vnodeLeftIndex = ROCKS_CHUNK_FILE_KEY.length();
                vnodeRightIndex = key.indexOf("_");
            } else if (key.startsWith(ROCKS_COOKIE_KEY)) {
                vnodeLeftIndex = ROCKS_COOKIE_KEY.length();
                vnodeRightIndex = key.indexOf(File.separator);
            } else if (key.startsWith(ROCKS_STS_TOKEN_KEY)) {
                vnodeLeftIndex = ROCKS_STS_TOKEN_KEY.length();
                vnodeRightIndex = key.indexOf(File.separator);
            } else if (key.startsWith(ROCKS_AGGREGATION_META_PREFIX)) {
                vnodeLeftIndex = ROCKS_AGGREGATION_META_PREFIX.length();
                vnodeRightIndex = key.indexOf(File.separator);
            } else if (key.startsWith(ROCKS_AGGREGATION_RATE_PREFIX)) {
                vnodeLeftIndex = ROCKS_AGGREGATION_RATE_PREFIX.length();
                vnodeRightIndex = key.indexOf(File.separator);
            } else if (key.startsWith(ROCKS_AGGREGATION_UNDO_LOG_PREFIX)) {
                vnodeLeftIndex = ROCKS_AGGREGATION_UNDO_LOG_PREFIX.length();
                vnodeRightIndex = key.indexOf(File.separator);
            } else {
                vnodeLeftIndex = 0;
                vnodeRightIndex = key.indexOf(File.separator);
            }

            return key.substring(vnodeLeftIndex, vnodeRightIndex);
        } catch (IndexOutOfBoundsException e) {
            return "-1";
        } catch (Exception e) {
            log.error("get key vnode fail {}", key, e);
            return "-1";
        }
    }
}
