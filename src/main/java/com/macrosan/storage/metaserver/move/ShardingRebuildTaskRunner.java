package com.macrosan.storage.metaserver.move;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.metaserver.ObjectSplitTree;
import com.macrosan.storage.metaserver.RSocketCommUtil;
import com.macrosan.storage.metaserver.move.remove.AbstractRemoveTaskRunner;
import com.macrosan.utils.functional.Tuple2;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.UPDATE_BUCKET_INDEX;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;
import static com.macrosan.storage.metaserver.ObjectSplitTree.*;
import static com.macrosan.storage.metaserver.ObjectSplitTree.isSeparatorLine;
import static com.macrosan.storage.metaserver.ShardingScheduler.MAX_COPY_KEYS;

/**
 * @author admin
 */
@Log4j2
public class ShardingRebuildTaskRunner extends ShardingExpansionTaskRunner {

    public ShardingRebuildTaskRunner(String bucketName, String sourceVnode, String targetVnode) {
        super(bucketName, sourceVnode, targetVnode, MAX_COPY_KEYS, Type.REBUILD);
        init(() -> true);
    }

    public ShardingRebuildTaskRunner(String bucketName, String sourceVnode, String targetVnode, BooleanSupplier throwErrorIfSrcIsNone) {
        super(bucketName, sourceVnode, targetVnode, MAX_COPY_KEYS, Type.REBUILD);
        init(throwErrorIfSrcIsNone);
    }

    private void init(BooleanSupplier throwErrorIfSrcIsNone) {
        ObjectSplitTree objectSplitTree = storagePool.getBucketShardCache().get(bucketName);
        ObjectSplitTree.Node src = objectSplitTree.findNodeByValue(sourceVnode);
        ObjectSplitTree.Node dest = objectSplitTree.findNodeByValue(targetVnode);
        ObjectSplitTree.Node intersection = ObjectSplitTree.intersection(src, dest);
        if (src != null && dest != null && (!ObjectSplitTree.isSeparatorLine(intersection) || src.parent != intersection || dest.parent != intersection)) {
            throw new UnsupportedOperationException("sharding rebuild task build error. because sourceNode with targetNode have not common parent!");
        }
        if (src == null && (throwErrorIfSrcIsNone.getAsBoolean())) {
            throw new UnsupportedOperationException("sharding rebuild task build error. because sourceNode is None!");
        }
        if (src != null && src.parent != null) {
            if (isRight(src)) {
                this.startPosition = src.parent.value.substring(1);
            } else if (isLeft(src)) {
                ObjectSplitTree.Node leftNeighbor = objectSplitTree.leftNeighbor(src);
                if (leftNeighbor != null) {
                    ObjectSplitTree.Node intersection2 = ObjectSplitTree.intersection(leftNeighbor, src);
                    if (isSeparatorLine(intersection2)) {
                        this.startPosition = intersection2.value.substring(1);
                    }
                }
            }
        }
    }

    @Override
    protected Mono<Boolean> update() {
        log.info("continuation:{}", continuation.get());
        // 当分片数据没有全部迁移完毕时，更新映射采用expansion的方式更新
        SocketReqMsg msg = null;
        if (continuation.get()) {
            ObjectSplitTree objectSplitTree = storagePool.getBucketShardCache().get(bucketName);
            ObjectSplitTree.Node src = objectSplitTree.findNodeByValue(sourceVnode);
            ObjectSplitTree.Node leftNeighbor = objectSplitTree.leftNeighbor(src);
            if (leftNeighbor != null && leftNeighbor.value.equals(targetVnode)) {
                if (StringUtils.isEmpty(divider.get())) {
                    throw new UnsupportedOperationException("update mapping error, Invalid divider");
                }
                ObjectSplitTree.Node dest = storagePool.getBucketShardCache().get(bucketName).findNodeByValue(targetVnode);;
                ObjectSplitTree.Node intersection = ObjectSplitTree.intersection(src, dest);
                log.info("bucket {} sourceNode:{} targetNode:{} intersection:{}", bucketName, sourceVnode, targetVnode, intersection);
                String oldDivider = intersection.value;
                msg = new SocketReqMsg("", 0)
                        .put("type", "1")
                        .put("bucket", bucketName)
                        .put("oldDivider", oldDivider)
                        .put("sourceNode", sourceVnode)
                        .put("targetNode", targetVnode)
                        .put("newDivider", SEPARATOR_LINE_PREFIX + divider.get());
            } else {
                return super.update();
            }
        }

        // 当分片数据全部迁移完毕，直接将二叉树中的原始vnode更新成目标vnode
        msg = (msg != null) ? msg : new SocketReqMsg("", 0)
                .put("type", "5")
                .put("bucket", bucketName)
                .put("sourceNode", sourceVnode)
                .put("targetNode", targetVnode);
        return RSocketCommUtil.broadcastRequest(UPDATE_BUCKET_INDEX.name(), msg, Duration.ofSeconds(30))
                .flatMap(results -> RSocketCommUtil.validateResponses(bucketName, results, queue));
    }

    @Override
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

    @Override
    protected Mono<Boolean> remove() {
        log.info("continuation:{}", continuation.get());
        if (discard.get()) {
            ObjectSplitTree objectSplitTree = storagePool.getBucketShardCache().get(bucketName);
            ObjectSplitTree.Node src = objectSplitTree.findNodeByValue(sourceVnode);
            ObjectSplitTree.Node leftNeighbor = objectSplitTree.leftNeighbor(src);
            if (leftNeighbor != null && leftNeighbor.value.equals(targetVnode)) {
                ObjectSplitTree.Node dest = storagePool.getBucketShardCache().get(bucketName).findNodeByValue(targetVnode);;
                ObjectSplitTree.Node intersection = ObjectSplitTree.intersection(src, dest);
                log.info("recover bucket {} sourceNode:{} targetNode:{} intersection:{}", bucketName, sourceVnode, targetVnode, intersection);
                if (intersection == null) {
                    return Mono.just(true);
                }
                String oldDivider = intersection.value.substring(1);
                divider.set(oldDivider);
                log.info("bucket {} sourceVnode:{} targetVnode:{} discard abandon data oldDivider:{}", bucketName, sourceVnode, targetVnode, divider.get());
                return removeRepeat();
            } else {
                if (objectSplitTree.getAllLeafNode().contains(targetVnode)) {
                    return Mono.just(true);
                }
                divider.set("");
                log.info("bucket {} targetVnode:{} discard abandon data.", bucketName, targetVnode);
                return realRemove(targetVnode, AbstractRemoveTaskRunner.POSITION.LEFT);
            }
        }

        if (!continuation.get()) {
            // 若当前任务可以结束，则代表原始分片中的数据全部迁移到了目标分片上，则直接将原始分片上的数据全部删除。
            ObjectSplitTree objectSplitTree = storagePool.getBucketShardCache().get(bucketName);
            if (objectSplitTree.getAllLeafNode().contains(sourceVnode)) {
                return Mono.just(false);
            }
            divider.set("");
            return realRemove(sourceVnode, AbstractRemoveTaskRunner.POSITION.LEFT);
        }

        return super.remove();
    }
}
