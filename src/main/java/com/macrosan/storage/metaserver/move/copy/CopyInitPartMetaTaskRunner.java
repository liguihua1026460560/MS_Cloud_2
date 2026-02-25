package com.macrosan.storage.metaserver.move.copy;

import com.macrosan.ec.part.PartClient;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.storage.metaserver.move.scanner.DefaultMetaDataScanner;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.io.File;
import java.time.Duration;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.storage.metaserver.move.AbstractShardingTaskRunner.Type.MERGE;
import static com.macrosan.storage.metaserver.move.AbstractShardingTaskRunner.Type.REBUILD;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className CopyInitPartMetaTaskRunner
 * @date 2023/3/9 16:58
 */
@Log4j2
public class CopyInitPartMetaTaskRunner extends AbstractCopyTaskRunner<Tuple2<byte[], byte[]>> {

    private final String separator;

    public CopyInitPartMetaTaskRunner(String bucketName, String sourceVnode, String startPosition, String targetVnode, Direction direction, String separator) {
        super(bucketName, sourceVnode, targetVnode, direction);
        this.separator = separator;

        String startMarker = ROCKS_PART_PREFIX + sourceVnode + File.separator + bucketName;
        if (StringUtils.isNotBlank(startPosition)) {
            startMarker = startMarker + File.separator + startPosition;
        }
        DefaultMetaDataScanner.SCAN_SEQUENCE sequence = DefaultMetaDataScanner.SCAN_SEQUENCE.NEXT;
        if (direction.equals(Direction.RIGHT)) {
            if (StringUtils.isBlank(startPosition)) {
                startMarker = ROCKS_PART_PREFIX + sourceVnode + File.separator + bucketName + ROCKS_SMALLEST_KEY + "";
            } else {
                byte[] bytes = startPosition.getBytes();
                bytes[bytes.length - 1] += 1;
                String marker = new String(bytes);
                startMarker = ROCKS_PART_PREFIX + sourceVnode + File.separator + bucketName + File.separator + marker;
            }
            sequence = DefaultMetaDataScanner.SCAN_SEQUENCE.PREV;
        }
        log.info("startMarker:" + startMarker);
        this.scanner = new DefaultMetaDataScanner(bucketName, sourceVnode, ROCKS_PART_PREFIX, startMarker, sequence);
    }

    private boolean canTryEnd(String key) {
        if (context != null
                && (REBUILD.name().equals(context.get("type")) || MERGE.name().equals(context.get("type")))
                && "false".equals(context.get("continuation"))) {
            return false;
        }
        if (direction.equals(Direction.LEFT)) {
            return StringUtils.isNotEmpty(separator) && key.compareTo(separator) > 0;
        } else {
            return StringUtils.isNotEmpty(separator) && key.compareTo(separator) <= 0;
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
                InitPartInfo initPartInfo = Json.decodeValue(value, InitPartInfo.class);

                if (canTryEnd(initPartInfo.object)) {
                    if (!scanner.isEnd()) {
                        scanner.tryEnd();
                    }
                    return Mono.just(true);
                }

                String partKey = initPartInfo.object + initPartInfo.uploadId;
                partDivider.updateAndGet(s -> {
                    boolean toLeft = direction.equals(Direction.LEFT);
                    boolean condition = toLeft ? !StringUtils.isEmpty(s) && partKey.compareTo(s) <= 0 : !StringUtils.isEmpty(s) && partKey.compareTo(s) >= 0;
                    return condition ? s : partKey;
                });

                return retryableOperation(nodeList -> PartClient.initPartUpload(initPartInfo, nodeList, null, true, null));
            } else {
                return Mono.just(true);
            }
        } catch (Exception e) {
            log.error("", e);
            return Mono.just(false);
        }
    }
}
