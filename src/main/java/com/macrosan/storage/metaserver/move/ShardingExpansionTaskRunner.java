package com.macrosan.storage.metaserver.move;

import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.metaserver.ObjectSplitTree;
import com.macrosan.storage.metaserver.RSocketCommUtil;
import com.macrosan.storage.metaserver.move.copy.AbstractCopyTaskRunner;
import com.macrosan.storage.metaserver.move.remove.AbstractRemoveTaskRunner;
import com.macrosan.utils.functional.Tuple2;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.UPDATE_BUCKET_INDEX;
import static com.macrosan.storage.metaserver.ObjectSplitTree.*;

@Log4j2
public class ShardingExpansionTaskRunner extends AbstractShardingTaskRunner {

    protected ShardingExpansionTaskRunner(String bucketName, String sourceVnode, String targetVnode, long copyMaxKeys, Type type) {
        super(bucketName, sourceVnode, targetVnode, copyMaxKeys, AbstractCopyTaskRunner.Direction.LEFT, type);
    }
    public ShardingExpansionTaskRunner(String bucketName, String sourceVnode, String targetVnode, long copyMaxKeys) {
        super(bucketName, sourceVnode, targetVnode, copyMaxKeys, AbstractCopyTaskRunner.Direction.LEFT, Type.EXPANSION);
        ObjectSplitTree objectSplitTree = storagePool.getBucketShardCache().get(bucketName);
        ObjectSplitTree.Node src = objectSplitTree.findNodeByValue(sourceVnode);
        if (src.parent != null) {
            if (isRight(src)) {
                this.startPosition = src.parent.value.substring(1);
            } else if (isLeft(src)) {
                ObjectSplitTree.Node leftNeighbor = objectSplitTree.leftNeighbor(src);
                if (leftNeighbor != null) {
                    ObjectSplitTree.Node intersection = ObjectSplitTree.intersection(leftNeighbor, src);
                    if (isSeparatorLine(intersection)) {
                        this.startPosition = intersection.value.substring(1);
                    }
                }
            }
        }
    }


    @Override
    protected Mono<Boolean> copy() {
        return realCopy();
    }

    @Override
    protected Mono<Boolean> update() {
        if (StringUtils.isEmpty(divider.get())) {
            throw new UnsupportedOperationException("update mapping error, Invalid divider");
        }
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("type", "0")
                .put("bucket", bucketName)
                .put("sourceNode", sourceVnode)
                .put("goalNode", targetVnode)
                .put("divider", SEPARATOR_LINE_PREFIX + divider.get());
        return RSocketCommUtil.broadcastRequest(UPDATE_BUCKET_INDEX.name(), msg, Duration.ofSeconds(30))
                .flatMap( t -> RSocketCommUtil.validateResponses(bucketName, t, queue));
    }

    @Override
    protected Mono<Boolean> remove() {
        //若该任务需要被舍弃执行，则清除已经被迁移到新的分片上的数据
        if (discard.get()) {
            ObjectSplitTree objectSplitTree = storagePool.getBucketShardCache().get(bucketName);
            if (objectSplitTree.getAllLeafNode().contains(targetVnode)) {
                return Mono.just(true);
            }
            divider.set("");
            log.info("");
            log.info("bucket {} targetVnode:{} discard abandon data.", bucketName, targetVnode);
            return realRemove(targetVnode, AbstractRemoveTaskRunner.POSITION.LEFT);
        }
        return removeRepeat();
    }

    protected Mono<Boolean> removeRepeat() {
        MonoProcessor<Boolean> res = MonoProcessor.create();

        Tuple2<String, AbstractRemoveTaskRunner.POSITION> task1 = new Tuple2<>(targetVnode, AbstractRemoveTaskRunner.POSITION.LEFT);
        Tuple2<String, AbstractRemoveTaskRunner.POSITION> task2 = new Tuple2<>(sourceVnode, AbstractRemoveTaskRunner.POSITION.RIGHT);

        Flux<Tuple2<String, AbstractRemoveTaskRunner.POSITION>> removeProcessor = Flux.just(task1, task2);

        removeProcessor
                .flatMap(tuple2 -> {
                    log.info("Start remove repeat meta from bucket {} {}......", bucketName, tuple2.var1);
                    return realRemove(tuple2.var1, tuple2.var2)
                            .doOnNext(b -> {
                                if (b) {
                                    log.info("Bucket {} remove repeat meta from {} successfully!", bucketName, tuple2.var1);
                                } else {
                                    log.info("Bucket {} remove repeat meta from {} fail!", bucketName, tuple2.var1);
                                    throw new RuntimeException("sharding remove exception.");
                                }
                            });
                }, 2)
                .doOnError(e -> {
                    log.error("", e);
                    res.onNext(false);
                })
                .doOnComplete(() -> res.onNext(true))
                .subscribe();

        return res;
    }
}
