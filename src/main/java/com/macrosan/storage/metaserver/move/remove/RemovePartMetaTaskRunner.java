package com.macrosan.storage.metaserver.move.remove;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.ec.part.PartUtils;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.storage.metaserver.move.copy.AbstractCopyTaskRunner;
import com.macrosan.storage.metaserver.move.scanner.DefaultMetaDataScanner;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.io.File;

import static com.macrosan.constants.SysConstants.ROCKS_PART_META_PREFIX;

@Log4j2
public class RemovePartMetaTaskRunner extends AbstractRemoveTaskRunner<Tuple2<byte[], byte[]>> {

    private final String partSeparator;

    public RemovePartMetaTaskRunner(String bucketName, String vnode, String separator, POSITION position, String partSeparator) {
        super(bucketName, vnode, separator, position);
        this.partSeparator = partSeparator;
        String startMarker = ROCKS_PART_META_PREFIX + vnode + File.separator + bucketName;
        DefaultMetaDataScanner.SCAN_SEQUENCE sequence = DefaultMetaDataScanner.SCAN_SEQUENCE.NEXT;
        if (position.equals(POSITION.LEFT)) {
            String key = StringUtils.isNotEmpty(partSeparator) && partSeparator.compareTo(separator) < 0 ? partSeparator : separator;
            startMarker = ROCKS_PART_META_PREFIX + vnode + File.separator + bucketName + File.separator + key;
        }
        log.info("startMarker:{}", startMarker);
        this.scanner = new DefaultMetaDataScanner(bucketName, vnode, ROCKS_PART_META_PREFIX, startMarker, sequence);
    }

    private boolean canTryEnd(String key, String uploadId) {
        return super.canTryEnd(key) && (StringUtils.isEmpty(partSeparator) || (key + uploadId).compareTo(partSeparator) > 0);
    }

    @Override
    protected Mono<Boolean> remove(Tuple2<byte[], byte[]> tuple2) {
        try {
            String key = new String(tuple2.var1);
            String value = new String(tuple2.var2);
            JsonObject o = new JsonObject(value);
            String versionNum = o.getString("versionNum");
            if (versionNum != null) {
                if (key.startsWith(ROCKS_PART_META_PREFIX)) {
                    PartInfo partInfo = Json.decodeValue(value, new TypeReference<PartInfo>() {
                    });

                    // 节点位于分割线的左边，分割线不删除
                    if (position.equals(POSITION.LEFT) && partInfo.object.compareTo(separator) <= 0) {
                        return Mono.just(true);
                    }

                    // 节点位于分割线的右边，大于分割线不删除
                    if (position.equals(POSITION.RIGHT) && partInfo.object.compareTo(separator) > 0) {
                        return Mono.just(true);
                    }

                    if (canTryEnd(partInfo.object, partInfo.uploadId)) {
                        if (!scanner.isEnd()) {
                            scanner.tryEnd();
                        }
                        return Mono.just(true);
                    }

                    return storagePool.mapToNodeInfo(vnode)
                            .flatMap(nodeList -> PartUtils.deleteMultiPartUploadMeta(partInfo.bucket, partInfo.object, partInfo.uploadId, nodeList, true, partInfo.snapshotMark, partInfo.snapshotMark));
                }
            }
            return Mono.just(true);
        } catch (Exception e) {
            log.error("", e);
            return Mono.just(false);
        }
    }
}
