package com.macrosan.ec.rebuild;

import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.storage.StoragePool;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.RocksIterator;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.constants.SysConstants.ROCKS_FILE_META_PREFIX;
import static com.macrosan.database.rocksdb.MSRocksDB.READ_OPTIONS;

/**
 * @author zhaoyang
 * @date 2025/11/10
 **/
@Log4j2
public class CopyObjectFileRebuilder {

    private String prevFileName;
    private final Set<String> rebuildingCopySourceFileNames = new ConcurrentHashSet<>();
    private final Map<String, String> rebuildCopyObjFileLock = new ConcurrentHashMap<>();

    public void trackCopySourceFile(String fileName) {
        // 判断前后两个filename，是否为copy源对象和copy目标对象
        if (prevFileName != null && (!fileName.contains("#") || !fileName.startsWith(prevFileName))) {
            // 不是copy源对象和目标对象，则从集合移除上次添加的filename
            rebuildingCopySourceFileNames.remove(prevFileName);
        }
        rebuildingCopySourceFileNames.add(fileName);
        prevFileName = fileName;
    }

    public void cancelTrackCopySourceFile(String fileName) {
        rebuildingCopySourceFileNames.remove(fileName);
    }

    public boolean isRebuildingInProgress(String fileName) {
        return rebuildingCopySourceFileNames.contains(fileName);
    }

    public Mono<Boolean> rebuildCopyObjFile(StoragePool pool, String metaKey, String lun, String errorIndex, String fileName, String endIndex, String fileSize, String crypto, String secretKey, List<Tuple3<String, String, String>> nodeList, String flushStamp, String lastAccessStamp, String fileOffset, int retry) {
        return putCopyObjFileMeta(lun, fileName, metaKey)
                .flatMap(b -> {
                    if (b) {
                        // copy 对象 fileMeta写入成功，则不再进行数据块重构
                        return Mono.just(true);
                    }
                    // copy 源对象 fileMeta写入失败，则表明 copy 源对象已不存在，则需要对数据块进行重构
                    String lockKey = fileName.split("#")[0];
                    boolean lockSuccess = rebuildCopyObjFileLock.putIfAbsent(lockKey, metaKey) == null;
                    if (lockSuccess) {
                        // 获取锁后再次尝试写入fileMeta
                        return putCopyObjFileMeta(lun, fileName, metaKey)
                                .flatMap(b0 -> {
                                    if (b0) {
                                        return Mono.just(true);
                                    }
                                    // fileMeta写入失败，说明没有前缀相同的FileMeta，则进行数据块重构
                                    return TaskHandler.rebuildObjFile(pool, metaKey, lun, errorIndex, fileName, endIndex, fileSize, crypto, secretKey, nodeList, flushStamp, lastAccessStamp, fileOffset);
                                }).doFinally(s -> rebuildCopyObjFileLock.remove(lockKey));

                    } else {
                        if (retry >= 10) {
                            log.info("filename:{} get rebuild copy obj lock fail, retry fail", fileName);
                            return Mono.just(false);
                        }
                        log.debug("filename:{} get rebuild copy obj lock fail, retry {}", fileName, retry);
                        return Mono.delay(Duration.ofMillis(200L * (retry * 10L))).flatMap(wait -> rebuildCopyObjFile(pool, metaKey, lun, errorIndex, fileName, endIndex, fileSize, crypto, secretKey, nodeList, flushStamp, lastAccessStamp, fileOffset, retry + 1));
                    }
                });
    }

    private Mono<Boolean> putCopyObjFileMeta(String lun, String fileName, String metaKey) {
        MonoProcessor<Boolean> res = MonoProcessor.create();
        BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
            String fileMetaKey = FileMeta.getKey(fileName);
            if (writeBatch.getFromBatchAndDB(db, READ_OPTIONS, fileMetaKey.getBytes()) != null) {
                // FileMeta 已经存在则直接返回
                res.onNext(true);
                return;
            }
            String sourceKey = FileMeta.getKey(fileName.split("#")[0]);
            byte[] sourceValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, sourceKey.getBytes());
            if (sourceValue == null) {
                try (RocksIterator iterator = db.newIterator(READ_OPTIONS);
                     RocksIterator keyIterator = writeBatch.getRocksIterator(iterator)) {
                    keyIterator.seek(sourceKey.getBytes());
                    while (keyIterator.isValid() && new String(keyIterator.key()).startsWith(sourceKey)) {
                        if (new String(keyIterator.key()).startsWith(sourceKey + ROCKS_FILE_META_PREFIX)) {
                            sourceValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, keyIterator.key());
                            break;
                        }
                        keyIterator.next();
                    }
                }
                if (sourceValue == null) {
                    res.onNext(false);
                    return;
                }
            }
            FileMeta sourceFileMeta = Json.decodeValue(new String(sourceValue), FileMeta.class);
            sourceFileMeta.setFileName(fileName);
            sourceFileMeta.setMetaKey(metaKey);
            writeBatch.put(fileMetaKey.getBytes(), Json.encode(sourceFileMeta).getBytes());
            res.onNext(true);
        };
        return BatchRocksDB.customizeOperateData(lun, consumer, res);
    }
}
