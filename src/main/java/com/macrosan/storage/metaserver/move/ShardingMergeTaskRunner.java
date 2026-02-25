package com.macrosan.storage.metaserver.move;


import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.metaserver.ObjectSplitTree;
import com.macrosan.storage.metaserver.move.copy.AbstractCopyTaskRunner;
import com.macrosan.storage.metaserver.move.remove.AbstractRemoveTaskRunner;
import com.macrosan.utils.functional.Tuple2;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.UPDATE_BUCKET_INDEX;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;
import static com.macrosan.storage.metaserver.ObjectSplitTree.isSeparatorLine;
import static com.macrosan.storage.metaserver.RSocketCommUtil.broadcastRequest;
import static com.macrosan.storage.metaserver.RSocketCommUtil.validateResponses;
import static com.macrosan.storage.metaserver.ShardingScheduler.MAX_COPY_KEYS;

@Log4j2
public class ShardingMergeTaskRunner extends ShardingLoadBalanceTaskRunner {

    public ShardingMergeTaskRunner(String bucketName, String sourceVnode, String targetVnode, AbstractCopyTaskRunner.Direction direction) {
        super(bucketName, sourceVnode, targetVnode, MAX_COPY_KEYS, direction, Type.MERGE);
        init(() -> true);
    }

    public ShardingMergeTaskRunner(String bucketName, String sourceVnode, String targetVnode, AbstractCopyTaskRunner.Direction direction, BooleanSupplier booleanSupplier) {
        super(bucketName, sourceVnode, targetVnode, MAX_COPY_KEYS, direction, Type.MERGE);
        init(booleanSupplier);
    }

    public void init(BooleanSupplier throwErrorIfEmpty) {
        ObjectSplitTree.Node src = storagePool.getBucketShardCache().get(bucketName).findNodeByValue(sourceVnode);
        ObjectSplitTree.Node dest = storagePool.getBucketShardCache().get(bucketName).findNodeByValue(targetVnode);
        ObjectSplitTree.Node intersection = ObjectSplitTree.intersection(src, dest);
        if (!isSeparatorLine(intersection) && throwErrorIfEmpty.getAsBoolean()) {
            throw new UnsupportedOperationException("sharding load balance task build error!" + intersection);
        }
        if (isSeparatorLine(intersection)) {
            log.info("sourceNode:{} goalNode:{} intersection:{}", sourceVnode, targetVnode, intersection);
            this.oldDivider = intersection.value;
            this.startPosition = intersection.value.substring(1);
        }
    }

    @Override
    protected Mono<Boolean> update() {
        log.info("continue:{}", continuation.get());
        if (continuation.get()) {
            return super.update();
        }
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("type", "5")
                .put("bucket", bucketName)
                .put("sourceNode", sourceVnode)
                .put("targetNode", targetVnode);
        return broadcastRequest(UPDATE_BUCKET_INDEX.name(), msg, Duration.ofSeconds(30))
                .flatMap(r -> validateResponses(bucketName, r, queue));
    }

    @Override
    protected Mono<Boolean> remove() {
        log.info("continue:{}", continuation.get());
        if (!continuation.get() && !discard.get()) {
            ObjectSplitTree objectSplitTree = storagePool.getBucketShardCache().get(bucketName);
            if (objectSplitTree.getAllLeafNode().contains(sourceVnode)) {
                return Mono.just(false);
            }
            // 删除原节点上的所有数据
            divider.set("");
            return realRemove(sourceVnode, AbstractRemoveTaskRunner.POSITION.LEFT);
        }
        return super.remove();
    }

    public Mono<Boolean> res() {
        return res
                .doOnNext(b -> {
                    log.info("continuation:{}", continuation.get());
                    //若本次任务成功，并且分片还有数据未迁移完毕，则将任务状态重新设置为START初始状态，待后续调度执行。
                    if (b && continuation.get()) {
                        phase = PHASE.START;
                        archive();
                        log.info("bucket {} migrate metadata from {} to {} continue...", bucketName, sourceVnode, targetVnode);
                    }
                })
                // 若此次分片数据未全部迁移成功，则视作失败，后续继续处理。
                .map(b -> !continuation.get() && b);
    }
}
