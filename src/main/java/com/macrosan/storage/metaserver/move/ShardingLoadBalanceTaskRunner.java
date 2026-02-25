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
import static com.macrosan.storage.metaserver.ObjectSplitTree.SEPARATOR_LINE_PREFIX;
import static com.macrosan.storage.metaserver.ObjectSplitTree.isSeparatorLine;

@Log4j2
public class ShardingLoadBalanceTaskRunner extends AbstractShardingTaskRunner {

    protected String oldDivider;

    protected ShardingLoadBalanceTaskRunner(String bucketName, String sourceVnode, String targetVnode, long copyMaxKeys, AbstractCopyTaskRunner.Direction direction, Type type) {
        super(bucketName, sourceVnode, targetVnode, copyMaxKeys, direction, type);
    }

    public ShardingLoadBalanceTaskRunner(String bucketName, String sourceVnode, String targetVnode, long copyMaxKeys, AbstractCopyTaskRunner.Direction direction) {
        super(bucketName, sourceVnode, targetVnode, copyMaxKeys, direction, Type.BALANCE);
        ObjectSplitTree.Node src = storagePool.getBucketShardCache().get(bucketName).findNodeByValue(sourceVnode);
        ObjectSplitTree.Node dest = storagePool.getBucketShardCache().get(bucketName).findNodeByValue(targetVnode);
        ObjectSplitTree.Node intersection = ObjectSplitTree.intersection(src, dest);
        if (!isSeparatorLine(intersection)) {
            throw new UnsupportedOperationException("sharding load balance task build error!" + intersection);
        }
        log.info("sourceNode:{} goalNode:{} intersection:{}", sourceVnode, targetVnode, intersection);
        this.oldDivider = intersection.value;
        this.startPosition = intersection.value.substring(1);
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
                .put("type", "1")
                .put("bucket", bucketName)
                .put("oldDivider", oldDivider)
                .put("sourceNode", sourceVnode)
                .put("targetNode", targetVnode)
                .put("newDivider", SEPARATOR_LINE_PREFIX + divider.get());
        return RSocketCommUtil.broadcastRequest(UPDATE_BUCKET_INDEX.name(), msg, Duration.ofSeconds(30))
                .flatMap( t -> RSocketCommUtil.validateResponses(bucketName, t, queue));
    }

    @Override
    protected Mono<Boolean> remove() {
        // 若该任务需要被舍弃执行，则清理不符合原始分割线的数据
        if (discard.get()) {
            divider.set(oldDivider.substring(1));
            log.info("bucket {} sourceVnode:{} targetVnode:{} discard abandon data oldDivider:{}", bucketName, sourceVnode, targetVnode, divider.get());
        }
        MonoProcessor<Boolean> res = MonoProcessor.create();
        AbstractRemoveTaskRunner.POSITION sourcePosition = direction.equals(AbstractCopyTaskRunner.Direction.LEFT) ? AbstractRemoveTaskRunner.POSITION.RIGHT : AbstractRemoveTaskRunner.POSITION.LEFT;
        AbstractRemoveTaskRunner.POSITION targetPosition = direction.equals(AbstractCopyTaskRunner.Direction.LEFT) ? AbstractRemoveTaskRunner.POSITION.LEFT : AbstractRemoveTaskRunner.POSITION.RIGHT;

        Tuple2<String, AbstractRemoveTaskRunner.POSITION> task1 = new Tuple2<>(targetVnode, targetPosition);
        Tuple2<String, AbstractRemoveTaskRunner.POSITION> task2 = new Tuple2<>(sourceVnode, sourcePosition);

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
