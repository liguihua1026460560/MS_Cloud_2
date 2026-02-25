package com.macrosan.storage.metaserver.move.remove;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.metaserver.move.scanner.DefaultMetaDataScanner;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.macrosan.constants.SysConstants.ROCKS_SMALLEST_KEY;
import static com.macrosan.constants.SysConstants.ROCKS_VERSION_PREFIX;
import static com.macrosan.ec.Utils.ONE_STR;
import static com.macrosan.ec.Utils.ZERO_STR;

@Log4j2
public class RemoveObjectMetaTaskRunner extends AbstractRemoveTaskRunner<Tuple2<byte[], byte[]>> {

    public RemoveObjectMetaTaskRunner(String bucketName, String vnode, String separator, POSITION position) {
        super(bucketName, vnode, separator, position);
        String startMarker = ROCKS_VERSION_PREFIX + vnode + File.separator + bucketName;
        DefaultMetaDataScanner.SCAN_SEQUENCE sequence = DefaultMetaDataScanner.SCAN_SEQUENCE.NEXT;
        if (position.equals(POSITION.LEFT)) {
            startMarker = ROCKS_VERSION_PREFIX + vnode + File.separator + bucketName + File.separator + separator;
        }
        log.info("vnode:{} {}, startMarker:{}" ,vnode, position.name(), startMarker);
        this.scanner = new DefaultMetaDataScanner(bucketName, vnode, ROCKS_VERSION_PREFIX, startMarker, sequence);
    }

    @Override
    public void run() {
        List<String> list = Arrays.asList("", "-");
        List<String> rangeList = new ArrayList<>();
        for (String prefix : list) {
            String s = prefix + vnode + File.separator + bucketName + File.separator + separator + (prefix.equals("-") ? ZERO_STR : ONE_STR);
            if (position.equals(POSITION.LEFT)) {
                String end = prefix + vnode + File.separator + bucketName + ROCKS_SMALLEST_KEY;
                rangeList.add(s);
                rangeList.add(end);
            } else {
                String start = prefix + vnode + File.separator + bucketName;
                rangeList.add(start);
                rangeList.add(s);
            }
        }
        storagePool.mapToNodeInfo(vnode)
                .doOnNext(l -> log.info("try delete sst files in range:{}", rangeList))
                .flatMap(nodeList -> ErasureClient.deleteFilesInRange(rangeList, nodeList))
                .flatMap(l -> Mono.delay(Duration.ofMillis(10000)))
                .doFinally(v -> super.run())
                .subscribe();
    }

    @Override
    protected Mono<Boolean> remove(Tuple2<byte[], byte[]> tuple2) {
        try {
            String key = new String(tuple2.var1);
            String value = new String(tuple2.var2);
            JsonObject o = new JsonObject(value);
            String versionNum = o.getString("versionNum");
            if (versionNum != null) {
                if (key.startsWith(ROCKS_VERSION_PREFIX) && Utils.isMetaJson(tuple2.var2)) {
                    MetaData metaData = Json.decodeValue(value, new TypeReference<MetaData>() {
                    });

                    // 节点位于分割线的左边，分割线不删除
                    if (position.equals(POSITION.LEFT) && metaData.key.compareTo(separator)== 0) {
                        return Mono.just(true);
                    }

                    if (canTryEnd(metaData.key)) {
                        if (!scanner.isEnd()) {
                            scanner.tryEnd();
                        }
                        return Mono.just(true);
                    }
                    String metaKey = Utils.getMetaDataKey(vnode, metaData.bucket, metaData.key, metaData.versionId, metaData.stamp, metaData.snapshotMark);
                    return storagePool.mapToNodeInfo(vnode)
                            .flatMap(nodeList -> ErasureClient.deleteObjectAllMeta(bucketName, metaKey, Json.encode(metaData), nodeList));
                }
            }
            return Mono.just(true);
        } catch (Exception e) {
            log.error("", e);
            return Mono.just(false);
        }
    }
}
