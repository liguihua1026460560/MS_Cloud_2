package com.macrosan.ec.rebuild;

import com.macrosan.constants.SysConstants;
import com.macrosan.ec.error.DiskErrorHandler;
import com.macrosan.ec.error.FsErrorHandler;
import com.macrosan.ec.error.PartErrorHandler;
import com.macrosan.ec.error.PutErrorHandler;
import com.macrosan.message.jsonmsg.*;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;

import static com.macrosan.ec.error.ErrorConstant.ECErrorType.RECOVER_DISK_FILE;

@Log4j2
public class TaskHandler {
    public static Mono<Boolean> rebuildObjMeta(String bucket, String object, String versionId, List<Tuple3<String, String, String>> nodeList, String snapshotMark) {
        return PutErrorHandler.recoverMeta(new MetaData().setBucket(bucket).setKey(object).setVersionId(versionId).setSnapshotMark(snapshotMark), null, null).map(b -> true);
    }

    public static Mono<Boolean> rebuildInitPartUpload(String bucket, String object, String uploadId,
                                                      List<Tuple3<String, String, String>> nodeList, String snapshotMark) {
        return PartErrorHandler.errorInitPartUpload(new InitPartInfo()
                .setBucket(bucket).setObject(object).setUploadId(uploadId).setSnapshotMark(snapshotMark), null).map(b -> true);
    }

    /**
     * 不处理
     */
    public static Mono<Boolean> rebuildPartUpload(PartInfo partInfo,
                                                  List<Tuple3<String, String, String>> nodeList) {
        return PartErrorHandler.errorPartUploadMetaData(partInfo, null).map(b -> true);
    }

    public static Mono<Boolean> rebuildObjFile(StoragePool storagePool, String metaKey, String lun, String errorIndex, String fileName, String endIndex, String fileSize, String crypto, String secretKey,
                                               List<Tuple3<String, String, String>> nodeList, String flushStamp, String lastAccessStamp, String fileOffset) {
        List<Integer> errorChunksList = Arrays.asList(Integer.parseInt(errorIndex));
        int i = Integer.parseInt(errorIndex);
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("lun", nodeList.get(i).var2)
                .put("fileName", fileName)
                .put("metaKey", metaKey)
                .put("endIndex", endIndex)
                .put("errorChunksList", Json.encode(errorChunksList))
                .put("fileSize", fileSize)
                .put("nodeList", Json.encode(nodeList));

        if (flushStamp != null) {
            msg.put("flushStamp", flushStamp);
        }
        if (StringUtils.isNotEmpty(lastAccessStamp)) {
            msg.put("lastAccessStamp", lastAccessStamp);
        }
        if (StringUtils.isNotEmpty(fileOffset)) {
            msg.put("fileOffset", fileOffset);
        }
        CryptoUtils.putCryptoInfoToMsg(crypto, secretKey, msg);

        // 聚合文件修复流程
        if (StringUtils.isNotEmpty(metaKey) && metaKey.startsWith(SysConstants.ROCKS_AGGREGATION_META_PREFIX)) {
            String[] split = metaKey.split("/");
            String namespace = split[1];
            String aggregateId = split[2];
            msg.put("namespace", namespace);
            msg.put("aggregateId", aggregateId);
            return PutErrorHandler.recoverAggregateFile(lun, namespace, aggregateId, fileName, storagePool.getVnodePrefix(), errorChunksList, String.valueOf(Long.parseLong(endIndex) + 1), msg, storagePool.getUpdateECPrefix());
        }

        return PutErrorHandler
                .recoverSpecificChunks(storagePool, metaKey, lun, errorChunksList, RECOVER_DISK_FILE, fileName,
                        Long.parseLong(endIndex), msg, nodeList);
    }

    public static Mono<Boolean> rebuildSyncRecord(String value) {
        UnSynchronizedRecord record = Json.decodeValue(value, UnSynchronizedRecord.class);
        SocketReqMsg msg = new SocketReqMsg("", 0);
        return PutErrorHandler.recoverSyncRecord(record, msg);
    }

    public static Mono<Boolean> rebuildDeduplicateInfo(String realKey, String value) {
        DedupMeta meta = Json.decodeValue(value, DedupMeta.class);
        return PutErrorHandler.recoverDedupMeta(meta, realKey).map(b -> true);
    }

    public static Mono<Boolean> rebuildComponentRecord(String value) {
        SocketReqMsg msg = new SocketReqMsg("", 0);
        msg.put("value", value);
        return PutErrorHandler.putMultiMediaRecord(msg);
    }

    public static Mono<Boolean> rebuildInode(String key, String value) {
        if (DiskErrorHandler.NFS_REBUILD_DEBUG) {
            log.info("rebuild inode: {}", key);
        }
        return FsErrorHandler.repairInode(key, value, "").map(b -> true);
    }

    public static Mono<Boolean> rebuildChunkFile(String chunkKey, String chunkFileValue) {
        if (DiskErrorHandler.NFS_REBUILD_DEBUG) {
            log.info("rebuild chunk: {}", chunkKey);
        }
        return FsErrorHandler.repairChunk(chunkKey, chunkFileValue, "").map(b -> true);
    }

    public static Mono<Boolean> rebuildSTSToken(String value) {
        SocketReqMsg msg = new SocketReqMsg("", 0).put("value", value);
        return PutErrorHandler.putCredentialToken(value, msg).map(b -> true);
    }

    public static Mono<Boolean> rebuildAggregateMeta(String key) {
        return PutErrorHandler.recoverAggregationMeta(key).map(b -> true);
    }
}
