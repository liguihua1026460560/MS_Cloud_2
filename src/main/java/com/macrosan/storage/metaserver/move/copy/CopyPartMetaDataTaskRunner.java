package com.macrosan.storage.metaserver.move.copy;

import com.macrosan.ec.part.PartClient;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.storage.metaserver.move.scanner.DefaultMetaDataScanner;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.io.File;

import static com.macrosan.constants.SysConstants.ROCKS_PART_META_PREFIX;
import static com.macrosan.constants.SysConstants.ROCKS_SMALLEST_KEY;
import static com.macrosan.storage.metaserver.move.AbstractShardingTaskRunner.Type.MERGE;
import static com.macrosan.storage.metaserver.move.AbstractShardingTaskRunner.Type.REBUILD;

@Log4j2
public class CopyPartMetaDataTaskRunner extends AbstractCopyTaskRunner<Tuple2<byte[], byte[]>> {

    private final String separator;

    private final String partSeparator;

    public CopyPartMetaDataTaskRunner(String bucketName, String sourceVnode, String startPosition, String targetVnode, Direction direction, String separator, String partSeparator) {
        super(bucketName, sourceVnode, targetVnode, direction);
        this.separator = separator;
        this.partSeparator = partSeparator;

        String startMarker = ROCKS_PART_META_PREFIX + sourceVnode + File.separator + bucketName;
        if (StringUtils.isNotBlank(startPosition)) {
            startMarker = startMarker + File.separator + startPosition;
        }
        DefaultMetaDataScanner.SCAN_SEQUENCE sequence = DefaultMetaDataScanner.SCAN_SEQUENCE.NEXT;
        if (direction.equals(Direction.RIGHT)) {
            startMarker = ROCKS_PART_META_PREFIX + sourceVnode + File.separator + bucketName + ROCKS_SMALLEST_KEY;
            sequence = DefaultMetaDataScanner.SCAN_SEQUENCE.PREV;
        }
        log.info("startMarker:" + startMarker);
        this.scanner = new DefaultMetaDataScanner(bucketName, sourceVnode, ROCKS_PART_META_PREFIX, startMarker, sequence);
    }

    private boolean canTryEnd(String key, String uploadId) {
        if (context != null
                && (REBUILD.name().equals(context.get("type")) || MERGE.name().equals(context.get("type")))
                && "false".equals(context.get("continuation"))) {
            return false;
        }
        if (direction.equals(Direction.LEFT)) {
            return StringUtils.isNotEmpty(separator) && key.compareTo(separator) > 0
                    && (StringUtils.isEmpty(partSeparator) || (key + uploadId).compareTo(partSeparator) > 0);
        } else {
            return StringUtils.isNotEmpty(separator) && key.compareTo(separator) <= 0
                    && (StringUtils.isEmpty(partSeparator) || (key + uploadId).compareTo(partSeparator) <= 0);
        }
    }

    @Override
    protected Mono<Boolean> copy(Tuple2<byte[], byte[]> tuple2) {
        try {
            String key = new String(tuple2.var1);
            String value = new String(tuple2.var2);
            JsonObject o = new JsonObject(value);
            String versionNum = o.getString("versionNum");
            if (versionNum != null) {
                PartInfo partInfo = Json.decodeValue(value, PartInfo.class);

                if (canTryEnd(partInfo.object, partInfo.uploadId)) {
                    if (!scanner.isEnd()) {
                        scanner.tryEnd();
                    }
                    return Mono.just(true);
                }

                return retryableOperation(nodeList -> PartClient.partUploadMeta(partInfo, nodeList, null, null, true, null));
            } else {
                return Mono.just(true);
            }
        } catch (Exception e) {
            log.error("", e);
            return Mono.just(false);
        }
    }
}
