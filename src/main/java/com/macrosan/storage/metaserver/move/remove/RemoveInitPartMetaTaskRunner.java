package com.macrosan.storage.metaserver.move.remove;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.ec.part.PartUtils;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.storage.metaserver.move.scanner.DefaultMetaDataScanner;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.io.File;

import static com.macrosan.constants.SysConstants.ROCKS_PART_PREFIX;

@Log4j2
public class RemoveInitPartMetaTaskRunner extends AbstractRemoveTaskRunner<Tuple2<byte[], byte[]>> {

    public RemoveInitPartMetaTaskRunner(String bucketName, String vnode, String separator, POSITION position) {
        super(bucketName, vnode, separator, position);
        String startMarker = ROCKS_PART_PREFIX + vnode + File.separator + bucketName;
        DefaultMetaDataScanner.SCAN_SEQUENCE sequence = DefaultMetaDataScanner.SCAN_SEQUENCE.NEXT;
        if (position.equals(POSITION.LEFT)) {
            startMarker = ROCKS_PART_PREFIX + vnode + File.separator + bucketName + File.separator + separator;
        }

        this.scanner = new DefaultMetaDataScanner(bucketName, vnode, ROCKS_PART_PREFIX, startMarker, sequence);
    }

    @Override
    protected Mono<Boolean> remove(Tuple2<byte[], byte[]> tuple2) {
        try {
            String key = new String(tuple2.var1);
            String value = new String(tuple2.var2);
            JsonObject o = new JsonObject(value);
            String versionNum = o.getString("versionNum");
            if (versionNum != null) {
                if (key.startsWith(ROCKS_PART_PREFIX)) {
                    InitPartInfo initPartInfo = Json.decodeValue(value, new TypeReference<InitPartInfo>() {
                    });

                    // 节点位于分割线的左边，分割线不删除
                    if (position.equals(POSITION.LEFT) && initPartInfo.object.compareTo(separator)== 0) {
                        return Mono.just(true);
                    }

                    if (canTryEnd(initPartInfo.object)) {
                        if (!scanner.isEnd()) {
                            scanner.tryEnd();
                        }
                        return Mono.just(true);
                    }
                    String initPartSnapshotMark = initPartInfo.snapshotMark;
                    String completePartSnapshotMark = initPartInfo.metaData == null ? initPartInfo.snapshotMark : initPartInfo.metaData.snapshotMark;
                    return storagePool.mapToNodeInfo(vnode)
                            .flatMap(nodeList -> PartUtils.deleteMultiPartUploadMeta(initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, nodeList, true,
                                    null,null, initPartSnapshotMark, completePartSnapshotMark, true));
                }
            }
            return Mono.just(true);
        } catch (Exception e) {
            log.error("", e);
            return Mono.just(false);
        }
    }
}
