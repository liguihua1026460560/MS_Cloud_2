package com.macrosan.ec.rebuild;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.RequeueMQException;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.ExpiringSet;
import com.macrosan.utils.functional.Tuple3;
import io.lettuce.core.KeyValue;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanStream;
import io.vertx.core.json.Json;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.REDIS_MIGING_V_INDEX;
import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.error.DiskErrorHandler.waitWriteDone;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_REBUILD_DELETE_CHECK_POINT;
import static com.macrosan.ec.rebuild.RebuildCheckpointUtil.nodeListMapToLun;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;

/**
 * @author zhaoyang
 * @date 2024/11/12
 **/
@Log4j2
public class RebuildCheckpointManager {

    public enum RebuildCompletePhase {

        PRE_COMPLETION,

        COMPLETION;

    }

    public static final RedisConnPool POOL = RedisConnPool.getInstance();

    public static final String CHECK_POINT_SUCCESS = "1";
    public static final String VNODE_COMPLETED_SET_PREFIX = "vnode_complete_set_";
    public static final String UPDATE_VNODE_COMPLETED_SET_PREFIX = "update_vnode_complete_set_";
    public static final String CHECK_POINT_STATUS_PREFIX = "checkpoint_";
    public static final String REBUILD_COMPLETE_STATUS = "complete_status";
    public static final long FAIL_LUN_EXPIRE = 1000 * 60 * 10;

    public static final Map<String, Semaphore> CHECK_POINT_LOCK = new ConcurrentHashMap<>();
    public static final Map<String, Map<String, AtomicBoolean>> CHECK_POINT_STATUS = new ConcurrentHashMap<>();
    public static final Map<String, ExpiringSet<String>> CHECK_POINT_FAIL_CACHE = new ConcurrentHashMap<>();

    private final StoragePool storagePool;
    private final String runningKey;
    private final String objVnode;
    private final String disk;
    private final String poolQueueTag;
    @Getter
    private final boolean isRebuildIndex;

    public RebuildCheckpointManager(StoragePool storagePool, String runningKey, String objVnode, String disk, String poolQueueTag) {
        this.storagePool = storagePool;
        this.runningKey = runningKey;
        this.objVnode = objVnode;
        this.disk = disk;
        this.poolQueueTag = poolQueueTag;
        this.isRebuildIndex = disk.contains("index");
    }


    /**
     * 迁移开始前创建checkpoint
     *
     * @return 返回true，则表示checkpoint创建成功，可以进行数据恢复
     */
    public Mono<Boolean> verifyAndCreateCheckpoint() {
        if (!disk.contains("index")) {
            return Mono.just(true);
        }
        CHECK_POINT_STATUS.computeIfAbsent(runningKey, k -> new ConcurrentHashMap<>());
        CHECK_POINT_FAIL_CACHE.computeIfAbsent(runningKey, k -> new ExpiringSet<>());
        return storagePool.mapToNodeInfo(objVnode)
                .publishOn(DISK_SCHEDULER)
                .flatMap(nodeList -> {
                    // 数据恢复时的源盘
                    Set<String> recoverSrcDisk = nodeList.stream()
                            .map(RebuildCheckpointUtil::nodeListMapToLun)
                            .filter(lun -> !lun.equals(disk))
                            .collect(Collectors.toSet());
                    // 需要创建checkpoint的盘
                    Set<String> needCheckPointDisk = recoverSrcDisk.stream()
                            .filter(lun -> !CHECK_POINT_STATUS.get(runningKey).computeIfAbsent(lun, k -> new AtomicBoolean()).get()
                                    || !CHECK_POINT_FAIL_CACHE.get(runningKey).contains(lun))
                            .collect(Collectors.toSet());
                    if (needCheckPointDisk.isEmpty()) {
                        // 所有恢复数据时需要用到的盘都已经checkpoint，则直接进行数据恢复
                        return Mono.just(true);
                    }
                    // 获取锁，防止并发创建checkpoint
                    Semaphore semaphore = CHECK_POINT_LOCK.computeIfAbsent(runningKey, k -> new Semaphore(1));
                    if (!semaphore.tryAcquire()) {
                        return Mono.just(false);
                    }
                    try {
                        // 获取锁成功，再次检测是否创建checkpoint
                        needCheckPointDisk = needCheckPointDisk.stream()
                                .filter(lun -> {
                                    if (CHECK_POINT_STATUS.get(runningKey).computeIfAbsent(lun, k -> new AtomicBoolean()).get()
                                            || CHECK_POINT_FAIL_CACHE.get(runningKey).contains(lun)) {
                                        return false;
                                    }
                                    if (CHECK_POINT_SUCCESS.equals(RebuildRabbitMq.getMaster().hget(runningKey, CHECK_POINT_STATUS_PREFIX + lun))) {
                                        CHECK_POINT_STATUS.get(runningKey).computeIfAbsent(lun, k -> new AtomicBoolean()).set(true);
                                        return false;
                                    }
                                    return true;
                                })
                                .collect(Collectors.toSet());
                        if (needCheckPointDisk.isEmpty()) {
                            // 所有恢复数据时需要用到的盘都已经checkpoint，则直接进行数据恢复
                            return Mono.just(true).doOnNext(s -> semaphore.release());
                        }
                        String vnodeNumAll = getRealRemoveDiskNumAll();
                        Long updateVnodeMappingCount = RebuildRabbitMq.getMaster().scard(UPDATE_VNODE_COMPLETED_SET_PREFIX + poolQueueTag);
                        if (!vnodeNumAll.equals(updateVnodeMappingCount.toString())) {
                            return Mono.just(false).doOnNext(s -> semaphore.release());
                        }
                        // 所有vnode映射都已更新成功，则进行checkpoint
                        Set<String> finalNeedCheckPointDisk = needCheckPointDisk;
                        List<Tuple3<String, String, String>> needCheckPointNodeLisk = nodeList.stream()
                                .filter(t3 -> finalNeedCheckPointDisk.contains(nodeListMapToLun(t3)))
                                .collect(Collectors.toList());
                        log.info("notifyCreateCheckpoints:{}", needCheckPointDisk);
                        return waitWriteDone()
                                .flatMap(b -> notifyCreateCheckpoints(needCheckPointNodeLisk, poolQueueTag))
                                .flatMap(checkPointSuccessNodeList -> updateCheckPointResult(checkPointSuccessNodeList, finalNeedCheckPointDisk, recoverSrcDisk))
                                .doOnError(e -> semaphore.release())
                                .doOnNext(s -> semaphore.release());
                    } catch (Exception e) {
                        log.error("", e);
                        semaphore.release();
                        return Mono.error(new RuntimeException("handleCheckpoint fail"));
                    }
                });
    }

    public String getRealRemoveDiskNumAll() {
        String removeDiskTaskNum = RebuildRabbitMq.getMaster().hget(runningKey, "removeDiskTaskNum");
        if (StringUtils.isBlank(removeDiskTaskNum)) {
            removeDiskTaskNum = RebuildRabbitMq.getMaster().hget(runningKey, "needOperateVnode");
        }
        return removeDiskTaskNum;
    }

    /**
     * 迁移完成后，判断当前重构任务是否全部完成，如果全部完成则关闭checkpoint
     */
    public Mono<Boolean> verifyAndCloseCheckpoint() {
        // 保存处理迁移完成的vnode
        RebuildRabbitMq.getMaster().sadd(VNODE_COMPLETED_SET_PREFIX + poolQueueTag, objVnode);
        Long vnodeNumAll = Long.parseLong(getRealRemoveDiskNumAll());
        Long completeCount = RebuildRabbitMq.getMaster().scard(VNODE_COMPLETED_SET_PREFIX + poolQueueTag);
        if (completeCount < vnodeNumAll) {
            return Mono.just(true);
        }
        // 所有vnode重构完成，设置完成标记
        if (isRebuildIndex) {
            // 索引盘重构完成后先设置 预完成阶段，待快照关闭后设置完成阶段
            RebuildRabbitMq.getMaster().hset(runningKey, REBUILD_COMPLETE_STATUS, RebuildCompletePhase.PRE_COMPLETION.name());
        } else {
            // 数据盘重构直接设置 完成阶段
            return Mono.just(RebuildRabbitMq.getMaster().hset(runningKey, REBUILD_COMPLETE_STATUS, RebuildCompletePhase.COMPLETION.name()))
                    .flatMap(b -> {
                        if (b) {
                            RebuildRabbitMq.getMaster().del(VNODE_COMPLETED_SET_PREFIX + poolQueueTag);
                        }
                        return Mono.just(false);
                    });
        }
        // 所有vnode迁移完成 删除checkpoint
        String removeDisk = RebuildRabbitMq.getMaster().hget(runningKey, "diskName");
        return ScanStream.hscan(RedisConnPool.getInstance().getReactive(REDIS_MIGING_V_INDEX), runningKey, new ScanArgs().match(CHECK_POINT_STATUS_PREFIX + "*"))
                .publishOn(DISK_SCHEDULER)
                .map(KeyValue::getKey)
                .collectList()
                .map(needCloseCheckPoint -> needCloseCheckPoint.stream().map(key -> key.substring(CHECK_POINT_STATUS_PREFIX.length())).collect(Collectors.toList()))
                .flatMap(needCloseCheckPoint -> closeCheckPoint(needCloseCheckPoint, runningKey, removeDisk, poolQueueTag).zipWith(Mono.just(needCloseCheckPoint)))
                .flatMap(tuple2 -> {
                    if (!tuple2.getT1()) {
                        return Mono.error(new RequeueMQException("closeCheckPoint fail"));
                    } else {
                        CHECK_POINT_LOCK.remove(runningKey);
                        CHECK_POINT_STATUS.remove(runningKey);
                        CHECK_POINT_FAIL_CACHE.remove(runningKey);
                        String[] fields = tuple2.getT2().stream().map(lun -> CHECK_POINT_STATUS_PREFIX + lun).toArray(String[]::new);
                        RebuildRabbitMq.getMaster().hdel(runningKey, fields);
                        RebuildRabbitMq.getMaster().del(VNODE_COMPLETED_SET_PREFIX + poolQueueTag);
                        RebuildRabbitMq.getMaster().del(UPDATE_VNODE_COMPLETED_SET_PREFIX + poolQueueTag);
                        // 设置重构完成标记
                        return Mono.just(RebuildRabbitMq.getMaster().hset(runningKey, "complete_status", RebuildCompletePhase.COMPLETION.name()));
                    }
                });
    }

    /**
     * 处理checkpoint结果
     *
     * @param checkPointSuccessNodeList 本次checkpoint成功节点
     * @param finalNeedCheckPointDisk   本次需要checkpoint的节点
     * @param recoverSrcDisk            恢复vnode数据所需要的盘
     * @return 是否可以进行数据恢复
     */
    private Mono<Boolean> updateCheckPointResult(List<Tuple3<String, String, String>> checkPointSuccessNodeList, Set<String> finalNeedCheckPointDisk, Set<String> recoverSrcDisk) {
        log.info("checkPointSuccessNodeList:{}", checkPointSuccessNodeList);
        Set<String> checkPointSuccessDisk = checkPointSuccessNodeList.stream().map(RebuildCheckpointUtil::nodeListMapToLun).collect(Collectors.toSet());
        //更新checkpoint 失败状态到内存
        finalNeedCheckPointDisk.stream().filter(lun -> !checkPointSuccessDisk.contains(lun))
                .forEach(failLun -> CHECK_POINT_FAIL_CACHE.get(runningKey).add(failLun, FAIL_LUN_EXPIRE));
        if (checkPointSuccessNodeList.isEmpty()) {
            if (recoverSrcDisk.size() - finalNeedCheckPointDisk.size() > 0) {
                // 存在checkpoint成功的节点，则进行数据恢复
                return Mono.just(true);
            } else {
                return Mono.just(false);
            }
        }
        // 更新checkpoint 成功状态到redis
        Map<String, String> upodateCheckPointMap = checkPointSuccessDisk
                .stream()
                .collect(Collectors.toMap((lun -> CHECK_POINT_STATUS_PREFIX + lun), (lun -> CHECK_POINT_SUCCESS)));
        RebuildRabbitMq.getMaster().hmset(runningKey, upodateCheckPointMap);
        // 更新checkpoint 成功状态到内存
        checkPointSuccessDisk.forEach(lun -> CHECK_POINT_STATUS.get(runningKey)
                .computeIfAbsent(lun, k -> new AtomicBoolean()).compareAndSet(false, true));
        return Mono.just(true);
    }


    /**
     * 通知其他节点创建checkpoint
     *
     * @param needCheckPointNodeLisk 需要创建checkpoint的节点信息
     * @return checkpoint成功的节点
     */
    private Mono<List<Tuple3<String, String, String>>> notifyCreateCheckpoints(List<Tuple3<String, String, String>> needCheckPointNodeLisk, String poolQueueTag) {
        needCheckPointNodeLisk.removeIf(t3 -> t3.var1.equals(CURRENT_IP) && disk.contains(t3.var2));
        List<SocketReqMsg> msgs = needCheckPointNodeLisk.stream()
                .map(t -> new SocketReqMsg("", 0).put("lun", t.var2).put("poolQueueTag", poolQueueTag).put("uuid", NodeCache.mapIPToNode(t.var1)))
                .collect(Collectors.toList());
        ClientTemplate.ResponseInfo<Boolean> response = ClientTemplate.oneResponse(msgs, REBUILD_CREATE_CHECK_POINT, Boolean.class, needCheckPointNodeLisk);
        return response.responses.collectList()
                .map(res -> res.stream()
                        .filter(t3 -> t3.var2.equals(SUCCESS))
                        .map(t3 -> needCheckPointNodeLisk.get(t3.var1))
                        .collect(Collectors.toList())
                );
    }


    /**
     * vnode数据迁移完成后，发送消息给其他节点删除checkpoint文件
     *
     * @param needCloseCheckPoint 格式为 "uuidId@lun"
     * @param removeDisk
     * @return 全部失败才会返回false
     */
    public static Mono<Boolean> closeCheckPoint(List<String> needCloseCheckPoint, String runningKey, String removeDisk, String poolQueueTag) {
        // 过滤掉被移除的盘
        needCloseCheckPoint = needCloseCheckPoint.stream().filter(lun -> !RemovedDisk.getInstance().contains(lun)).collect(Collectors.toList());
        log.info("rebuild complete closeCheckPoint:{}", needCloseCheckPoint);
        if (needCloseCheckPoint.isEmpty()) {
            return Mono.just(true);
        }
        Map<String, List<String>> nodeLunMap = needCloseCheckPoint.stream()
                .collect(Collectors.groupingBy(
                        item -> NodeCache.getIP(item.split("@")[0]),
                        Collectors.mapping(
                                item -> item.split("@")[1],
                                Collectors.toList()
                        )
                ));
        List<Tuple3<String, String, String>> nodeList = nodeLunMap.keySet().stream()
                .map(ip -> new Tuple3<>(ip, nodeLunMap.get(ip).get(0), ""))
                .collect(Collectors.toList());
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple3 -> new SocketReqMsg("", 0)
                        .put("needCloseLun", Json.encode(nodeLunMap.get(tuple3.var1)))
                        .put("runningKey", runningKey)
                        .put("removeDisk", removeDisk)
                )
                .collect(Collectors.toList());

        ClientTemplate.ResponseInfo<Boolean> response = ClientTemplate.oneResponse(msgs, REBUILD_DELETE_CHECK_POINT, Boolean.class, nodeList);
        List<String> finalNeedCloseCheckPoint = needCloseCheckPoint;
        return response.responses.collectList()
                .map(b -> {
                    if (response.successNum == 0) {
                        return false;
                    } else if (response.successNum < nodeList.size()) {
                        SocketReqMsg msg = new SocketReqMsg("", 0)
                                .put("needCloseCheckPoint", Json.encode(finalNeedCloseCheckPoint.toArray(new String[0])))
                                .put("runningKey", runningKey)
                                .put("removeDisk", removeDisk)
                                .put("poolQueueTag", poolQueueTag);
                        publishEcError(response.res, nodeList, msg, ERROR_REBUILD_DELETE_CHECK_POINT);
                    }
                    return true;
                });
    }

    public boolean checkRebuildComplete() {
        String completeStatus = RebuildRabbitMq.getMaster().hget(runningKey, REBUILD_COMPLETE_STATUS);
        if (completeStatus == null) {
            return false;
        }
        try {
            RebuildCompletePhase completePhase = RebuildCompletePhase.valueOf(completeStatus);
            return completePhase == RebuildCompletePhase.COMPLETION;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean checkRebuildPreComplete() {
        if (!isRebuildIndex) {
            return false;
        }
        String completeStatus = RebuildRabbitMq.getMaster().hget(runningKey, REBUILD_COMPLETE_STATUS);
        if (completeStatus == null) {
            return false;
        }
        try {
            RebuildCompletePhase completePhase = RebuildCompletePhase.valueOf(completeStatus);
            return completePhase == RebuildCompletePhase.PRE_COMPLETION;
        } catch (Exception e) {
            return false;
        }
    }
}
