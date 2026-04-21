package com.macrosan.ec.error;

import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.migrate.LocalMigrate;
import com.macrosan.ec.migrate.Migrate;
import com.macrosan.ec.migrate.SstVnodeDataScannerUtils;
import com.macrosan.ec.rebuild.ReBuildRunner;
import com.macrosan.ec.rebuild.RebuildRabbitMq;
import com.macrosan.ec.rebuild.RemovedDisk;
import com.macrosan.ec.server.MigrateServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.ObjectConsumer;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.crypto.rootKey.RootSecretKeyManager;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.database.rocksdb.MSRocksDB.*;
import static com.macrosan.doubleActive.HeartBeatChecker.isMultiAliveStarted;
import static com.macrosan.ec.error.DiskErrorHandler.initRocksDbAndBlockDevice;
import static com.macrosan.ec.error.DiskErrorHandler.waitWriteDone;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.ec.migrate.Migrate.ADD_NODE_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.CUR_NODE;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class NodeErrorHandler {
    private static final String RUNNING_POOL_PREFIX = "running_";

    /**
     * 新加节点开启consumer
     */
    @HandleErrorFunction(value = PREPARE_ADD_NODE, timeout = 0L)
    public static Mono<Boolean> prepareAddNode(String[] uuids, String poolQueueTag) {
        return Mono.just(1L).publishOn(DISK_SCHEDULER)
                .map(b -> {
                    for (String uuid : uuids) {
                        ObjectConsumer.getInstance().init(uuid);
                        List<String> storageList = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).keys("storage_*");
                        for (String storage : storageList) {
                            String poolName = RedisConnPool.getInstance().getCommand(REDIS_POOL_INDEX).hget(storage, "pool");
                            ObjectConsumer.getInstance().initNodePoolConsumer(uuid, poolName);
                        }
                        List<String> disks = RedisConnPool.getInstance().getCommand(REDIS_LUNINFO_INDEX).keys(uuid + "@fs-SP0-*");
                        for (String disk : disks) {
                            ReBuildRunner.getInstance().rabbitMq.consumer(disk);
                            if (uuid.equals(ServerConfig.getInstance().getHostUuid())) {
                                // 新节点 初始化磁盘和rocksdb实例
                                initRocksDbAndBlockDevice(disk, null);
                            }
                        }
                        NodeCache.add(uuid);
                        RootSecretKeyManager.addNode(uuid);
                    }

                    return true;
                }).doOnNext(b -> {
                    if (StringUtils.isNotEmpty(poolQueueTag)) {//在加节点重平衡期间发布的消息
                        RebuildRabbitMq.getMaster().sadd(PREPARE_ADD_NODE_PREFIX + poolQueueTag, CUR_NODE);
                    }
                });
    }

    /**
     * 缩容移除其他节点上被缩容节点的缓存
     */
    @HandleErrorFunction(value = REMOVE_NODE_CACHE, timeout = 0L)
    public static Mono<Boolean> removeNodeCache(String uuid, String deleteQueue) {


        return Mono.just(1L).publishOn(DISK_SCHEDULER)
                .map(b -> {
                    if ("1".equals(deleteQueue)) {//仅删除队列
                        String heartIp = RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).hget(uuid, SysConstants.HEART_IP);
                        ObjectConsumer.getInstance().removeQueues(heartIp, uuid);
                    } else {
                        ObjectConsumer.getInstance().remove(uuid);
                        NodeCache.remove(uuid);
                        RootSecretKeyManager.removeNode(uuid);
                    }

                    return true;
                });
    }

    private static void refresh(AtomicBoolean end, String key) {
        if (!end.get()) {
            try {
                RedisConnPool.getInstance()
                        .getShortMasterCommand(REDIS_MIGING_V_INDEX)
                        .expire(key, 300);
            } catch (Exception e) {
                log.error("refresh lock fail", e);
            }

            ADD_NODE_SCHEDULER.schedule(() -> refresh(end, key), 100, TimeUnit.SECONDS);
        } else {
            RedisConnPool.getInstance().getShortMasterCommand(REDIS_MIGING_V_INDEX).del(key);
        }
    }

    @HandleErrorFunction(value = ADD_NODE, timeout = 0L)
    public static Mono<Boolean> addNode(String vnode, String srcDisk, String dstNode, String dstDisk,
                                        String dstNodeIp, String poolType, String poolQueueTag, String moveOther, String state) {
        return Mono.just(1L).publishOn(ADD_NODE_SCHEDULER)
                .flatMap(l -> addNode0(vnode, srcDisk, dstNode, dstDisk, dstNodeIp, poolType, poolQueueTag, moveOther, state));
    }

    /**
     *
     * @param vnode
     * @param srcDisk 不带节点信息
     * @param dstNode 目标节点uuid
     * @param dstDisk 不带节点信息
     * @param dstNodeIp 目标节点ip
     * @param poolType 存储池类型
     * @param poolQueueTag 存储池名称标识
     * @return
     */
    public static Mono<Boolean> addNode0(String vnode, String srcDisk, String dstNode, String dstDisk,
                                         String dstNodeIp, String poolType, String poolQueueTag, String moveOther, String state) {
        String runningKey = RUNNING_POOL_PREFIX + poolQueueTag;
        StoragePool pool = StoragePoolFactory.getStoragePool(poolType, null);
        String[] link = pool.getLink(vnode);
        String value = vnode + '-' + System.currentTimeMillis();
        String curNode = ServerConfig.getInstance().getHostUuid();
        boolean useAnother = false;
        boolean isRemoveNode0 = false;
        boolean nodeForRemove0 = false;
        try {
            isRemoveNode0 = REMOVE_NODE.equals(RebuildRabbitMq.getMaster().hget(runningKey, "operate"));
            nodeForRemove0 = curNode.equals(RebuildRabbitMq.getMaster().hget(runningKey, "removedNode"));
        } catch (Exception e) {
        }
        String lockRes = "";
        try {//是否可能出现加节点多个存储池同时扩容的情况，有可能会出现link锁冲突，在前面增加存储池标识
            lockRes = RedisConnPool.getInstance()
                    .getShortMasterCommand(REDIS_MIGING_V_INDEX)
                    .set(poolQueueTag + "_" + Json.encode(link), value, SetArgs.Builder.ex(300).nx());
        } catch (Exception e) {
        }

        if (!"OK".equalsIgnoreCase(lockRes)) {
            String finalSrcDisk = srcDisk;
            return Mono.delay(Duration.ofSeconds(10), ADD_NODE_SCHEDULER)
                    .map(l -> {
                        SocketReqMsg msg = new SocketReqMsg("", 0)
                                .put("vnode", vnode)
                                .put("srcDisk", finalSrcDisk)
                                .put("dstDisk", dstDisk)
                                .put("dstNode", dstNode)
                                .put("poolType", poolType)
                                .put("dstNodeIp", dstNodeIp)
                                .put("poolQueueTag", poolQueueTag)
                                .put("moveOther", moveOther)
                                .put("state", state);

                        ObjectPublisher.basicPublish(CURRENT_IP, msg, ADD_NODE);
                        log.debug("set link lock fail, retry later");
                        return true;
                    });
        }

        /**
         * TODO 关于加节点期间如果源盘出现异常或者被移除的处理
         * 1. 在消息开始处理前如果源盘被移除，则需要获取同link下其他节点的磁盘进行快照创建和扫描，发消息到其他节点
         * 2. 在消息处理期间源盘扫描数据是源盘出现异常，则产生异常重新放回消息队列中进行消费，转向第1点的处理（重点测试）主要是这个异常后能够正确捕获然后让消息重新消费，同时重新消费后也能正确的判断源盘故障转而将消息发布到其他节点去扫描处理
         *
         */
        if (null == MSRocksDB.getRocksDB(srcDisk) || RemovedDisk.getInstance().contains(curNode + "@" + srcDisk)) {
            //如果源盘被移除需要去获取同link下其他节点的磁盘进行快照创建和扫描，发消息到其他节点
            //遍历当前vnode所属的link组
            for (int i = 0 ; i < link.length; i++) {
                if (!vnode.equals(link[i])) {
                    Map<String, String> vnodeInfoMap = RedisConnPool.getInstance().getShortMasterCommand(REDIS_MAPINFO_INDEX).hgetall(poolType + link[i]);
                    log.info("=====vnodeInfo:{}",vnodeInfoMap);
                    String node = vnodeInfoMap.get("s_uuid");
                    String lunName = vnodeInfoMap.get("lun_name");
                    //如果是在本节点上的盘那么直接判断磁盘状态
                    if (ServerConfig.getInstance().getHostUuid().equals(node)) {
                        if (null != MSRocksDB.getRocksDB(lunName) && !RemovedDisk.getInstance().contains(lunName)) {
                            //那么这里就转换状态将源盘改为lunName
                            log.info("srcDisk Changed, old:{}, new:{}， vnode:{}",  srcDisk, lunName, vnode);
                            srcDisk = lunName;
                            useAnother = true;
                        }
                    } else {//如果扫到其他节点上的盘
                        //这里准备发消息至node节点确实lunName是否能够正常使用
                        //获取node的ip
                        String heartIp = NodeCache.getIP(node);
                        SocketReqMsg msg = new SocketReqMsg("", 0)
                                .put("nodeUuid", node)
                                .put("lun", lunName);
                        boolean lunState = RSocketClient.getRSocket(heartIp, BACK_END_PORT)
                                .flatMap(r -> r.requestResponse(DefaultPayload.create(Json.encode(msg), CHECK_LUN_STATUS.name())))
                                .timeout(Duration.ofSeconds(30))
                                .map(r -> SUCCESS.name().equalsIgnoreCase(r.getMetadataUtf8()))
                                .onErrorReturn(false).block();
                        if (lunState) {
                            return Mono.delay(Duration.ofSeconds(10), ADD_NODE_SCHEDULER)
                                    .map(l -> {
                                        SocketReqMsg msg1 = new SocketReqMsg("", 0)
                                                .put("vnode", vnode)
                                                .put("srcDisk", lunName)
                                                .put("dstDisk", dstDisk)
                                                .put("dstNode", dstNode)
                                                .put("poolType", poolType)
                                                .put("dstNodeIp", dstNodeIp)
                                                .put("poolQueueTag", poolQueueTag)
                                                .put("moveOther", "1")
                                                .put("state", state);

                                        ObjectPublisher.basicPublish(heartIp, msg1, ADD_NODE);
                                        log.info("send msg to node:{}, lun:{}, vnode:{}", node, lunName, vnode);
                                        return true;
                                    });
                        }
                    }
                }
            }

        }
        Flux<String> flux = Flux.empty();
        boolean migrateMeta = false;
        if (poolType.startsWith("meta")) {
            migrateMeta = true;
            for (String objVnode : link) {
                MigrateServer.getInstance().addMigrateVnode(srcDisk, objVnode);
                if (isMultiAliveStarted) {
                    MigrateServer.getInstance().addMigrateVnode(getSyncRecordLun(srcDisk), objVnode);
                }
                MigrateServer.getInstance().addMigrateVnode(getComponentRecordLun(srcDisk), objVnode);
                MigrateServer.getInstance().addMigrateVnode(getSTSTokenLun(srcDisk), objVnode);
                MigrateServer.getInstance().addMigrateVnode(getRabbitmqRecordLun(srcDisk), objVnode);
                MigrateServer.getInstance().addMigrateVnode(getAggregateLun(srcDisk), objVnode);
                flux = flux.concatWith(migrateVnodeMeta(objVnode, vnode, srcDisk, dstDisk, dstNodeIp, poolQueueTag));
            }

            synchronized (Thread.currentThread()) {
                try {
                    Thread.currentThread().wait(2_000);
                } catch (InterruptedException e) {
                    log.error("", e);
                }
            }

            try {
                SstVnodeDataScannerUtils.createCheckPoint(srcDisk, vnode, link);
                SstVnodeDataScannerUtils.createCheckPoint(getSyncRecordLun(srcDisk), vnode, link);
                SstVnodeDataScannerUtils.createCheckPoint(getComponentRecordLun(srcDisk), vnode, link);
                SstVnodeDataScannerUtils.createCheckPoint(getSTSTokenLun(srcDisk), vnode, link);
                SstVnodeDataScannerUtils.createCheckPoint(getAggregateLun(srcDisk), vnode, link);
            } catch (Exception e) {
                return Mono.error(e);
            }
        } else {
            for (String objVnode : link) {
                flux = flux.concatWith(migrateVnodeData(objVnode, vnode, srcDisk, dstDisk, dstNodeIp, poolType));
            }
        }
        AtomicBoolean end = new AtomicBoolean(false);
        refresh(end, poolQueueTag + "_" + Json.encode(link));

        boolean finalMigrateMeta = migrateMeta;
        String finalSrcDisk = srcDisk;
        boolean moveOtherMigrate = "1".equals(moveOther);
        if (useAnother || moveOtherMigrate) {
            //如果是使用当前节点其他盘或者处理其他节点转过来的加节点消息，则不需要移除被扫描盘的源数据，仅移除双写时存入migrate列族中的元数据
            return flux.publishOn(ADD_NODE_SCHEDULER).collectList()
                    .flatMap(l -> updateNode(pool, vnode, dstNode, dstDisk).map(b -> l))
                    .delayElement(Duration.ofSeconds(2))
                    .flatMap(l -> waitWriteDone().map(b -> l))
                    .flatMapMany(l -> Flux.fromStream(l.stream()))
                    .flatMap(objVnode -> endMigrateVnode(objVnode, vnode, finalSrcDisk, dstDisk, dstNodeIp, finalMigrateMeta))
                    .collectList()
                    .flatMap(l -> removeVnodeMetaForMove(link, vnode, finalSrcDisk, dstDisk, dstNodeIp, finalMigrateMeta, true))
                    .map(l -> {
                        try {
                            RebuildRabbitMq.getMaster().hincrby(runningKey, "vnodeNum", -1);
                            if (poolType.startsWith("meta")) {
                                RebuildRabbitMq.getMaster().hincrby(runningKey, "migrateNum", -1);
                                RebuildRabbitMq.getMaster().hget(runningKey, "srcDiskObjNum");
                            }
                        } catch (Exception e) {
                            log.error("", e);
                        }
                        return true;
                    })
                    .doOnNext(b -> {
                        if (StringUtils.isNotEmpty(state)) {
                            int stateInt = Integer.parseInt(state);
                            switch (stateInt) {
                                case 1:
                                    RebuildRabbitMq.getMaster().sadd(CLASH_ADD_NODE_PREFIX + poolQueueTag, vnode);
                                    break;
                                case 2:
                                    RebuildRabbitMq.getMaster().sadd(TO_NEW_ADD_NODE_PREFIX + poolQueueTag, vnode);
                                    break;
                                case 3:
                                    RebuildRabbitMq.getMaster().sadd(IN_SRC_ADD_NODE_PREFIX + poolQueueTag, vnode);
                                    break;
                                default:
                                    break;
                            }
                        }
                    })
                    .doFinally(s -> {
                        if (finalMigrateMeta) {
                            SstVnodeDataScannerUtils.removeCheckPoint(finalSrcDisk, vnode);
                            SstVnodeDataScannerUtils.removeCheckPoint(getSyncRecordLun(finalSrcDisk), vnode);
                            SstVnodeDataScannerUtils.removeCheckPoint(getComponentRecordLun(finalSrcDisk), vnode);
                            SstVnodeDataScannerUtils.removeCheckPoint(getSTSTokenLun(finalSrcDisk), vnode);
                            SstVnodeDataScannerUtils.removeCheckPoint(getAggregateLun(finalSrcDisk), vnode);
                        }
                        end.compareAndSet(false, true);
                    });
        }
        final boolean isRemoveNode = isRemoveNode0;
        final boolean nodeForRemove = nodeForRemove0;
        return flux.publishOn(ADD_NODE_SCHEDULER).collectList()
                .flatMap(l -> updateNode(pool, vnode, dstNode, dstDisk).map(b -> l))
                .delayElement(Duration.ofSeconds(2))
                .flatMap(l -> waitWriteDone().map(b -> l))
                .flatMapMany(l -> Flux.fromStream(l.stream()))
                .flatMap(objVnode -> endMigrateVnode(objVnode, vnode, finalSrcDisk, dstDisk, dstNodeIp, finalMigrateMeta))
                .collectList()
                .flatMap(l -> {
                    //如果是缩容流程且源盘在待缩容节点上，可跳过旧盘数据的扫描移除
                    if (isRemoveNode && nodeForRemove) {
                        return Mono.just(true);
                    }
                    return Mono.just(l)
                            .doOnNext(l0 -> {
                                if (finalMigrateMeta) {
                                    Set<String> objVnodes = Arrays.stream(link).collect(Collectors.toSet());
                                    LocalMigrate.removeDataFileInRanges(finalSrcDisk, objVnodes, MSRocksDB.IndexDBEnum.ROCKS_DB, true);
                                    LocalMigrate.removeDataFileInRanges(finalSrcDisk, objVnodes, MSRocksDB.IndexDBEnum.UNSYNC_RECORD_DB, true);
                                    LocalMigrate.removeDataFileInRanges(finalSrcDisk, objVnodes, MSRocksDB.IndexDBEnum.COMPONENT_RECORD_DB, true);
                                    LocalMigrate.removeDataFileInRanges(finalSrcDisk, objVnodes, MSRocksDB.IndexDBEnum.STS_TOKEN_DB, true);
                                    LocalMigrate.removeDataFileInRanges(finalSrcDisk, objVnodes, MSRocksDB.IndexDBEnum.RABBITMQ_RECORD_DB, true);
                                    LocalMigrate.removeDataFileInRanges(finalSrcDisk, objVnodes, MSRocksDB.IndexDBEnum.AGGREGATE_DB, true);
                                }
                            })
                            .flatMap(l0 -> removeVnodeMetaAndData(link, vnode, finalSrcDisk, dstDisk, dstNodeIp, finalMigrateMeta, poolType));
                })
                .map(l -> {
                    try {
                        RebuildRabbitMq.getMaster().hincrby(runningKey, "vnodeNum", -1);
                        if (poolType.startsWith("meta")) {
                            RebuildRabbitMq.getMaster().hincrby(runningKey, "migrateNum", -1);
                            RebuildRabbitMq.getMaster().hget(runningKey, "srcDiskObjNum");
                        }
                    } catch (Exception e) {
                        log.error("", e);
                    }
                    return true;
                })
                .doOnNext(b -> {
                    if (StringUtils.isNotEmpty(state)) {
                        int stateInt = Integer.parseInt(state);
                        switch (stateInt) {
                            case 1:
                                RebuildRabbitMq.getMaster().sadd(CLASH_ADD_NODE_PREFIX + poolQueueTag, vnode);
                                break;
                            case 2:
                                RebuildRabbitMq.getMaster().sadd(TO_NEW_ADD_NODE_PREFIX + poolQueueTag, vnode);
                                break;
                            case 3:
                                RebuildRabbitMq.getMaster().sadd(IN_SRC_ADD_NODE_PREFIX + poolQueueTag, vnode);
                                break;
                            default:
                                break;
                        }
                    }
                })
                .doFinally(s -> {
                    if (finalMigrateMeta) {
                        SstVnodeDataScannerUtils.removeCheckPoint(finalSrcDisk, vnode);
                        SstVnodeDataScannerUtils.removeCheckPoint(getSyncRecordLun(finalSrcDisk), vnode);
                        SstVnodeDataScannerUtils.removeCheckPoint(getComponentRecordLun(finalSrcDisk), vnode);
                        SstVnodeDataScannerUtils.removeCheckPoint(getSTSTokenLun(finalSrcDisk), vnode);
                        SstVnodeDataScannerUtils.removeCheckPoint(getAggregateLun(finalSrcDisk), vnode);
                    }
                    end.compareAndSet(false, true);
                });
    }

    private static Mono<Boolean> updateNode(StoragePool pool, String vnode, String node, String lun) {
        Map<String, String> map = new HashMap<>(2);
        map.put("lun_name", lun);
        map.put("s_uuid", node);
        log.info("start update cache! {}", vnode);

        return Mono.just(true).publishOn(ADD_NODE_SCHEDULER)
                .flatMap(b -> {
                    String masterRunId = RedisConnPool.getInstance().getMasterRunId();
                    if (masterRunId == null) {
                        return Mono.error(new IllegalStateException("master run id is null"));
                    }
                    if (vnode.charAt(0) == '-') {
                        RedisConnPool.getInstance().getShortMasterCommand(REDIS_MAPINFO_INDEX)
                                .hmset(pool.getMapVnodePrefix() + vnode.substring(1), map);
                    } else {
                        RedisConnPool.getInstance().getShortMasterCommand(REDIS_MAPINFO_INDEX)
                                .hmset(pool.getVnodePrefix() + vnode, map);

                        String mapV = RedisConnPool.getInstance().getCommand(REDIS_MAPINFO_INDEX).hget(pool.getVnodePrefix() + vnode, "map");
                        if (mapV != null) {
                            RedisConnPool.getInstance().getShortMasterCommand(REDIS_MAPINFO_INDEX)
                                    .hmset(mapV, map);
                        }
                    }
                    return RedisConnPool.getInstance().syncRedis(masterRunId, "");
                })
                .flatMap(x -> {
                    log.info("redis sync over! {}", vnode);
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("poolType", pool.getVnodePrefix())
                            .put("vnode", vnode)
                            .put("lun", lun)
                            .put("node", node);

                    Flux<Boolean> res = Flux.empty();

                    RedisCommands<String, String> command = RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX);
                    if (RabbitMqUtils.HEART_IP_LIST.size() != command.dbsize()) {
                        ScanIterator<String> iterator = ScanIterator.scan(command, new ScanArgs().match("*"));

                        while (iterator.hasNext()) {
                            String key = iterator.next();
                            ObjectConsumer.getInstance().addHeartIp(key);
                        }
                    }
                    log.info("begin update cache {}", vnode);
                    //更新各个节点缓存中的node信息（lun_name、s_uuid）
                    for (String ip : RabbitMqUtils.HEART_IP_LIST) {
                        Mono<Boolean> mono = Mono.just(true).flatMap(b -> RSocketClient.getRSocket(ip, BACK_END_PORT))
                                .flatMap(r -> r.requestResponse(DefaultPayload.create(Json.encode(msg), UPDATE_DISK_NODE.name())))
                                .timeout(Duration.ofSeconds(30))
                                .map(r -> SUCCESS.name().equalsIgnoreCase(r.getMetadataUtf8()))
                                .onErrorReturn(false);

                        UnicastProcessor<Boolean> flux = UnicastProcessor.create();
                        MonoProcessor<Boolean> processor = MonoProcessor.create();
                        flux.publishOn(ADD_NODE_SCHEDULER)
                                .flatMap(b -> mono)
                                .doOnComplete(() -> processor.onNext(true))
                                .subscribe(b -> {
                                    if (b) {
                                        flux.onComplete();
                                    } else {
                                        flux.onNext(true);
                                    }
                                });
                        flux.onNext(true);
                        res = res.mergeWith(processor);
                    }

                    return res.collectList().map(l -> true);
                });
    }

    private static Mono<Boolean> endMigrateVnode(String objVnode, String vnode, String srcDisk, String dstDisk, String dstNodeIp, boolean isMigrateMeta) {
        return Mono.just(1)
                .flatMap(l -> {
                    if (isMultiAliveStarted && isMigrateMeta) {
                        return MigrateServer.getInstance().endMigrate(getSyncRecordLun(srcDisk), objVnode).collectList();
                    } else {
                        return Mono.just(l);
                    }
                })
                .flatMap(l -> {
                    if (isMigrateMeta) {
                        return MigrateServer.getInstance().endMigrate(getComponentRecordLun(srcDisk), objVnode).collectList();
                    } else {
                        return Mono.just(l);
                    }
                })
                .flatMap(l -> {
                    if (isMigrateMeta) {
                        return MigrateServer.getInstance().endMigrate(getSTSTokenLun(srcDisk), objVnode).collectList();
                    } else {
                        return Mono.just(l);
                    }
                })
                .flatMap(l -> {
                    if (isMigrateMeta) {
                        return MigrateServer.getInstance().endMigrate(getRabbitmqRecordLun(srcDisk), objVnode).collectList();
                    } else {
                        return Mono.just(l);
                    }
                })
                .flatMap(l -> {
                    if (isMigrateMeta) {
                        return MigrateServer.getInstance().endMigrate(getAggregateLun(srcDisk), objVnode).collectList();
                    } else {
                        return Mono.just(l);
                    }
                })
                .flatMapMany(l -> MigrateServer.getInstance().endMigrate(srcDisk, objVnode))
                .publishOn(ADD_NODE_SCHEDULER)
                .flatMap(t -> Migrate.replayDeleteOperate(t, dstDisk, dstNodeIp))
                .collect(() -> true, (b, t) -> {
                })
                .flatMapMany(l -> MigrateServer.getInstance().endMigrateCopy(srcDisk, objVnode))
                .publishOn(ADD_NODE_SCHEDULER)
                .flatMap(t -> Migrate.replayCopyOperate(t, dstDisk, dstNodeIp))
                .collect(() -> true, (b, t) -> {
                });
    }

    private static Mono<Boolean> removeVnodeMetaAndData(String[] links, String vnode, String srcDisk, String dstDisk, String dstNodeIp, boolean isMigrateMeta, String poolType) {
        return Flux.fromArray(links)
                .flatMap(objVnode -> {
                    if (isMigrateMeta) {
                        return Migrate.removeVnodeMeta(objVnode, srcDisk, vnode, dstNodeIp, dstDisk, false);
                    }
                    return Mono.just(true);
                })
                .collectList()
                // 同一个link中所有vnode都执行完removeVnodeMeta后才进行removeVnodeData
                .flatMap(b -> Migrate.removeVnodeData(vnode, srcDisk, dstDisk, dstNodeIp, poolType))
                .doOnNext(b -> log.info("delete Vnode {} data from {} end", links, srcDisk))
                .filter(b -> isMigrateMeta)
                .flatMapMany(l -> Flux.fromArray(links))
                .flatMap(objVnode -> Mono.just(true)
                        .flatMap(b -> Migrate.removeVnodeMeta(objVnode, getComponentRecordLun(srcDisk), vnode, dstNodeIp, getComponentRecordLun(dstDisk), false)
                                .doOnNext(b1 -> log.info("delete Vnode {} data from {} end", objVnode, getComponentRecordLun(srcDisk))))
                        .flatMap(b -> Migrate.removeVnodeMeta(objVnode, getSTSTokenLun(srcDisk), vnode, dstNodeIp, getSTSTokenLun(dstDisk), false)
                                .doOnNext(b1 -> log.info("delete Vnode {} data from {} end", objVnode, getSTSTokenLun(srcDisk))))
                        .flatMap(b -> Migrate.removeVnodeMeta(objVnode, getRabbitmqRecordLun(srcDisk), vnode, dstNodeIp, getRabbitmqRecordLun(dstDisk), false)
                                .doOnNext(b1 -> log.info("delete Vnode {} data from {} end", objVnode, getRabbitmqRecordLun(srcDisk))))
                        .flatMap(b -> Migrate.removeVnodeMeta(objVnode, getAggregateLun(srcDisk), vnode, dstNodeIp, getAggregateLun(dstDisk), false)
                                .doOnNext(b1 -> log.info("delete Vnode {} data from {} end", objVnode, getAggregateLun(srcDisk))))
                        .filter(b -> isMultiAliveStarted)
                        .flatMap(b -> Migrate.removeVnodeMeta(objVnode, getSyncRecordLun(srcDisk), vnode, dstNodeIp, getSyncRecordLun(dstDisk), false)
                                .doOnNext(b1 -> log.info("delete Vnode {} data from {} end", objVnode, getSyncRecordLun(srcDisk))))
                )
                .collectList()
                .thenReturn(true);
    }

    private static Mono<Boolean> removeVnodeMetaForMove(String[] links, String vnode, String srcDisk, String dstDisk, String dstNodeIp, boolean isMigrateMeta, boolean moveOtherMigrate) {
        return Flux.fromArray(links)
                .flatMap(objVnode -> {
                    if (isMigrateMeta) {
                        return Migrate.removeVnodeMeta(objVnode, srcDisk, vnode, dstNodeIp, dstDisk, moveOtherMigrate);
                    }
                    return Mono.just(true);
                })
                .collectList()
                // 同一个link中所有vnode都执行完removeVnodeMeta后才进行removeVnodeData
//                .flatMap(b -> Migrate.removeVnodeData(vnode, srcDisk, dstDisk, dstNodeIp, dstNode, poolType))
//                .doOnNext(b -> log.info("delete Vnode {} data from {} end", links, srcDisk))
                .filter(b -> isMigrateMeta)
                .flatMapMany(l -> Flux.fromArray(links))
                .flatMap(objVnode -> Mono.just(true)
                        .flatMap(b -> Migrate.removeVnodeMeta(objVnode, getComponentRecordLun(srcDisk), vnode, dstNodeIp, getComponentRecordLun(dstDisk), moveOtherMigrate)
                                .doOnNext(b1 -> log.info("delete Vnode {} data from {} end", objVnode, getComponentRecordLun(srcDisk))))
                        .flatMap(b -> Migrate.removeVnodeMeta(objVnode, getSTSTokenLun(srcDisk), vnode, dstNodeIp, getSTSTokenLun(dstDisk), moveOtherMigrate)
                                .doOnNext(b1 -> log.info("delete Vnode {} data from {} end", objVnode, getSTSTokenLun(srcDisk))))
                        .flatMap(b -> Migrate.removeVnodeMeta(objVnode, getRabbitmqRecordLun(srcDisk), vnode, dstNodeIp, getRabbitmqRecordLun(dstDisk), moveOtherMigrate)
                                .doOnNext(b1 -> log.info("delete Vnode {} data from {} end", objVnode, getRabbitmqRecordLun(srcDisk))))
                        .flatMap(b -> Migrate.removeVnodeMeta(objVnode, getAggregateLun(srcDisk), vnode, dstNodeIp, getAggregateLun(dstDisk), moveOtherMigrate)
                                .doOnNext(b1 -> log.info("delete Vnode {} data from {} end", objVnode, getAggregateLun(srcDisk))))
                        .filter(b -> isMultiAliveStarted)
                        .flatMap(b -> Migrate.removeVnodeMeta(objVnode, getSyncRecordLun(srcDisk), vnode, dstNodeIp, getSyncRecordLun(dstDisk), moveOtherMigrate)
                                .doOnNext(b1 -> log.info("delete Vnode {} data from {} end", objVnode, getSyncRecordLun(srcDisk))))
                )
                .collectList()
                .thenReturn(true);
    }

    private static Mono<String> migrateVnodeMeta(String objVnode, String vnode, String srcDisk, String dstDisk, String dstNodeIp, String poolQueueTag) {
        return Mono.just(true)
                .publishOn(ADD_NODE_SCHEDULER)
                .flatMap(l -> Migrate.copyVnodeMeta(objVnode, vnode, srcDisk, dstDisk, dstNodeIp, poolQueueTag))
                .doOnNext(l -> log.info("migrate Vnode data {} {} from {} to {} {} end",
                        objVnode, vnode, srcDisk, dstNodeIp, dstDisk))
                .flatMap(b -> {
                    if (isMultiAliveStarted) {
                        return Migrate.copyVnodeMeta(objVnode, vnode, getSyncRecordLun(srcDisk), getSyncRecordLun(dstDisk), dstNodeIp, poolQueueTag)
                                .doOnNext(l -> log.info("migrate Vnode data {} {} from {} to {} {} end",
                                        objVnode, vnode, getSyncRecordLun(srcDisk), dstNodeIp, getSyncRecordLun(dstDisk)));
                    } else {
                        return Mono.just(b);
                    }
                })
                .flatMap(b -> Migrate.copyVnodeMeta(objVnode, vnode, getComponentRecordLun(srcDisk), getComponentRecordLun(dstDisk), dstNodeIp, poolQueueTag)
                        .doOnNext(l -> log.info("migrate Vnode data {} {} from {} to {} {} end",
                                objVnode, vnode, getComponentRecordLun(srcDisk), dstNodeIp, getComponentRecordLun(dstDisk))))
                .flatMap(b -> Migrate.copyVnodeMeta(objVnode, vnode, getSTSTokenLun(srcDisk), getSTSTokenLun(dstDisk), dstNodeIp, poolQueueTag)
                        .doOnNext(l -> log.info("migrate Vnode data {} {} from {} to {} {} end",
                                objVnode, vnode, getSTSTokenLun(srcDisk), dstNodeIp, getSTSTokenLun(dstDisk))))
                .flatMap(b -> Migrate.copyVnodeMeta(objVnode, vnode, getRabbitmqRecordLun(srcDisk), getRabbitmqRecordLun(dstDisk), dstNodeIp, poolQueueTag)
                        .doOnNext(l -> log.info("migrate Vnode data {} {} from {} to {} {} end",
                                objVnode, vnode, getRabbitmqRecordLun(srcDisk), dstNodeIp, getRabbitmqRecordLun(dstDisk))))
                .flatMap(b -> Migrate.copyVnodeMeta(objVnode, vnode, getAggregateLun(srcDisk), getAggregateLun(dstDisk), dstNodeIp, poolQueueTag)
                        .doOnNext(l -> log.info("migrate Vnode data {} {} from {} to {} {} end",
                                objVnode, vnode, getAggregateLun(srcDisk), dstNodeIp, getAggregateLun(dstDisk))))
                .map(b -> objVnode);
    }

    private static Mono<String> migrateVnodeData(String objVnode, String vnode, String srcDisk, String dstDisk, String dstNodeIp, String poolType) {
        return MigrateServer.getInstance().startMigrate(srcDisk, objVnode, dstDisk, dstNodeIp)
                .publishOn(ADD_NODE_SCHEDULER)
                .flatMap(l -> Migrate.copyVnodeData(objVnode, srcDisk, dstDisk, dstNodeIp, poolType))
                .doOnNext(l -> {
                    log.info("migrate Vnode data {} {} from {} to {} {} end",
                            objVnode, vnode, srcDisk, dstNodeIp, dstDisk);
                }).map(b -> objVnode);
    }
}
