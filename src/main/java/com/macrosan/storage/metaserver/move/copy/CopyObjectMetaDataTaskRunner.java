package com.macrosan.storage.metaserver.move.copy;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.metaserver.move.scanner.DefaultMetaDataScanner;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.SysConstants.ROCKS_SMALLEST_KEY;
import static com.macrosan.constants.SysConstants.ROCKS_VERSION_PREFIX;
import static com.macrosan.ec.Utils.ONE_STR;
import static com.macrosan.storage.metaserver.ShardingScheduler.DISABLED_BUCKET_HASH;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className CopyObjectMetaDataTaskRunner
 * @date 2023/3/9 16:07
 */
@Log4j2
public class CopyObjectMetaDataTaskRunner extends AbstractCopyTaskRunner<Tuple2<byte[], byte[]>> {

    protected final long copyLimit;
    protected final AtomicLong copyNumbers = new AtomicLong(0);

    public CopyObjectMetaDataTaskRunner(String bucketName, String sourceVnode, String startPosition, String targetVnode, long copyLimit, Direction direction) {
        super(bucketName, sourceVnode, targetVnode, direction);
        this.copyLimit = copyLimit;
        String startMarker = ROCKS_VERSION_PREFIX + sourceVnode + File.separator + bucketName;
        if (StringUtils.isNotBlank(startPosition)) {
            startMarker = startMarker + File.separator + startPosition;
        }
        DefaultMetaDataScanner.SCAN_SEQUENCE sequence = DefaultMetaDataScanner.SCAN_SEQUENCE.NEXT;
        if (direction.equals(Direction.RIGHT)) {
            if (StringUtils.isBlank(startPosition)) {
                startMarker = ROCKS_VERSION_PREFIX + sourceVnode + File.separator + bucketName + ROCKS_SMALLEST_KEY + "";
            } else {
                startMarker = ROCKS_VERSION_PREFIX + sourceVnode + File.separator + bucketName + File.separator + startPosition + ONE_STR;
            }
            sequence = DefaultMetaDataScanner.SCAN_SEQUENCE.PREV;
        }
        log.info("startMarker:" + startMarker);
        this.scanner = new DefaultMetaDataScanner(bucketName, sourceVnode, ROCKS_VERSION_PREFIX, startMarker, sequence);
    }

    @Override
    protected Mono<Boolean> copy(Tuple2<byte[], byte[]> tuple2) {
        try {
            String value = new String(tuple2.var2);
            JsonObject o = new JsonObject(value);
            String versionNum = o.getString("versionNum");
            if (versionNum != null) {
                MetaData metaData = Json.decodeValue(value, new TypeReference<MetaData>() {
                });
                if (metaData.deleteMark) {
                    return Mono.just(true);
                }
                // 当复制的对象数目已达到目标
                if ((copyNumbers.incrementAndGet() >= copyLimit || (DISABLED_BUCKET_HASH.get())) && !metaData.key.equals(divider.get())) {
                    if (!scanner.isEnd()) {
                        scanner.tryEnd();
                        if (direction.equals(Direction.RIGHT)) {
                            divider.set(metaData.key);
                        }
                        return Mono.just(true);
                    } else {
                        return Mono.just(true);
                    }
                }
                divider.set(metaData.key);
                String metaKey = Utils.getMetaDataKey(targetVnode, metaData.bucket, metaData.key, metaData.versionId, metaData.stamp, metaData.snapshotMark);
                metaData.setShardingStamp(VersionUtil.getVersionNum());
                return retryableOperation(nodeList -> ErasureClient.putMetaData(metaKey, metaData, nodeList, true, null));
            } else {
                return Mono.just(true);
            }
        } catch (Exception e) {
            log.error("", e);
            return Mono.just(false);
        }
    }
}