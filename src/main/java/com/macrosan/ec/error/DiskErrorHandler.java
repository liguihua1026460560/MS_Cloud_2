package com.macrosan.ec.error;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.migrate.LocalMigrate;
import com.macrosan.ec.migrate.MigrateUtil;
import com.macrosan.ec.migrate.ScannerConfig;
import com.macrosan.ec.part.PartClient;
import com.macrosan.ec.rebuild.*;
import com.macrosan.ec.restore.RestoreObjectTask;
import com.macrosan.ec.server.LocalMigrateServer;
import com.macrosan.fs.BlockDevice;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.*;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.snapshot.utils.SnapshotUtil;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.*;
import com.macrosan.storage.strategy.StorageStrategy;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.sync.RedisCommands;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.ETAG;
import static com.macrosan.constants.ServerConstants.REMOVE_NODE;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.database.rocksdb.MSRocksDB.*;
import static com.macrosan.doubleActive.HeartBeatChecker.isMultiAliveStarted;
import static com.macrosan.ec.Utils.getLifeCycleStamp;
import static com.macrosan.ec.Utils.getVersionMetaDataKey;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.*;
import static com.macrosan.ec.migrate.Migrate.ADD_NODE_SCHEDULER;
import static com.macrosan.ec.part.PartClient.LIST_MULTI_PART_UPLOADS_RESPONSE_TYPE_REFERENCE;
import static com.macrosan.ec.rebuild.RebuildCheckpointManager.*;
import static com.macrosan.ec.rebuild.RebuildRabbitMq.REBUILD_QUEUE_NAME_PREFIX;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.rabbitmq.RabbitMqUtils.HEART_IP_LIST;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;
import static com.macrosan.storage.client.ListMetaVnode.SCAN_MIGRATE_POSITION_PREFIX;
import static com.macrosan.storage.client.ListMetaVnode.migrateVKeyMap;
import static com.macrosan.storage.strategy.StorageStrategy.POOL_STRATEGY_MAP;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class DiskErrorHandler {
    private static final RedisConnPool redisConnPool = RedisConnPool.getInstance();
    private static final String RUNNING_POOL_PREFIX = "running_";
    public static final String ADD_DISK_COMPLETED_SET_PREFIX = "add_disk_complete_set_";
    public static final String ADD_DISK_COMPLETE_STATUS = "add_disk_complete_status";
    private static final Set<String> msgSet = new ConcurrentHashSet<>();
    public static boolean NFS_REBUILD_DEBUG = false;

    @HandleErrorFunction(value = PREPARE_REMOVE_DISK, timeout = 0L)
    public static Mono<Boolean> prepareRemoveDisk(String disk, String poolQueueTag) throws RocksDBException {
        final List<Address> addressList = RabbitMqUtils.HEART_IP_LIST.stream().map(Address::new).collect(Collectors.toList());
        final String uuid = ServerConfig.getInstance().getHostUuid();
        final String queueName = REBUILD_QUEUE_NAME_PREFIX + "-" + uuid + "@" + disk + "-" + poolQueueTag + END_STR;
        final String runningKey = RUNNING_POOL_PREFIX + poolQueueTag;

        for (Address address : addressList) {
            try {
                final Channel channel = new ConnectionFactory().newConnection(Arrays.asList(address)).createChannel();
                channel.queueDelete(queueName);
                channel.getConnection().close();
            } catch (IOException | TimeoutException e) {
                log.error("delete queue error.", e);
            }
        }

        try {
            // 根据磁盘中文件量与容量按比例均衡的特性，通过其它磁盘求取待重构文件数的平均近似值
            String poolType = RebuildRabbitMq.getMaster().hget(runningKey, "poolType");
            String poolName = RebuildRabbitMq.getMaster().hget(runningKey, "poolName");
            String poolListStr = redisConnPool.getCommand(SysConstants.REDIS_POOL_INDEX).hget(poolName, "file_system");
            String[] diskArray = poolListStr.substring(1, poolListStr.length() - 1).split(", ");
            // 换盘的目标盘不参与容量计算
            String replaceTargetDisk = RebuildRabbitMq.getMaster().hget(runningKey, "replaceFlag");
            if (poolType.equals("data") || poolType.equals("cache")) {
                long removedSize = -1L;
                long resSize = 0L;
                long resFileNum = 0L;
                for (String curDisk : diskArray) {
                    if (curDisk.split("@")[0].equals(uuid) && !RemovedDisk.getInstance().contains(curDisk) && !curDisk.equals(replaceTargetDisk)) {
                        long curDiskSize = Long.parseLong(redisConnPool.getCommand(SysConstants.REDIS_LUNINFO_INDEX).hget(curDisk, "lun_size"));
                        resSize = resSize + curDiskSize;
                        try {
                            byte[] curDiskObjNumBytes = MSRocksDB.getRocksDB(curDisk.split("@")[1]).get(BlockDevice.ROCKS_FILE_SYSTEM_FILE_NUM);
                            if (curDiskObjNumBytes != null) {
                                long curDiskObjNum = bytes2long(curDiskObjNumBytes);
                                resFileNum = resFileNum + curDiskObjNum;
                            }
                        } catch (RocksDBException e) {
                            log.error("", e);
                        }
                    }
                }

                removedSize = Long.parseLong(RebuildRabbitMq.getMaster().hget(runningKey, "removedSize"));
                // 移除盘文件量=其他盘包含的文件总量/所有盘容量*移除盘容量
                double srcDiskObjNum = (double) resFileNum / resSize * removedSize;
                RebuildRabbitMq.getMaster().hincrby(runningKey, "migrateNum", (long) Math.ceil(srcDiskObjNum));
                RebuildRabbitMq.getMaster().hincrby(runningKey, "srcDiskObjNum", (long) Math.ceil(srcDiskObjNum));
                log.info("file system disk array size: {}, fileNum: {}", diskArray.length, srcDiskObjNum);

                // 如果是缓存池重构，且计算到的待迁移重构数为0，则需判断是否是1块盘拔盘重构的情况
                // (1) 1块盘拔盘，触发前未进行加盘操作，则无需计算进度值，此时不可能重构恢复
                // (2) 1块盘拔盘，触发前进行过加盘操作，则需要计算进度值，此时会把数据恢复至新加盘中
                if (poolType.equals("cache") && resFileNum == 0L) {
                    boolean isOnly = true;
                    // 检查fileSystem中是否有除了uuid@disk之外的本节点的磁盘
                    HashSet<String> resDisks = new HashSet<>();
                    LinkedList<String> totalDisks = new LinkedList<>(Arrays.asList(diskArray));
                    Iterator<String> iterator = totalDisks.iterator();
                    while (iterator.hasNext()) {
                        String key = iterator.next();
                        String node = key.split("@")[0];
                        String fileDisk = key.split("@")[1];
                        if (node.equalsIgnoreCase(uuid)) {
                            // 如果当前盘不等于重构盘；同时当前盘不在被移除盘中
                            if (!fileDisk.equalsIgnoreCase(disk) && !RemovedDisk.getInstance().contains(fileDisk)) {
                                resDisks.add(fileDisk);
                            }
                        }
                    }

                    if (resDisks.isEmpty()) {
                        isOnly = false;
                    }
                    log.info("totalDisks: {}, resDisks: {}, size: {}", totalDisks, resDisks, resDisks.size());

                    if (isOnly) {
                        queryNodeFileNum(poolListStr, poolName);
                    }
                }
            }
            // 删除上一次重构任务使用的key，防止上一次异常情况下没删除掉
            RebuildRabbitMq.getMaster().del(VNODE_COMPLETED_SET_PREFIX + poolQueueTag);
            RebuildRabbitMq.getMaster().del(ADD_DISK_COMPLETE_STATUS + poolQueueTag);
            RebuildRabbitMq.getMaster().del(UPDATE_VNODE_COMPLETED_SET_PREFIX + poolQueueTag);
        } catch (Exception e) {
            log.error("Calculate migrateNum or srcDiskObjNum error ", e);
        }

        MSRocksDB.remove(disk);
        if (disk.contains("index")) {
            MSRocksDB.remove(getSyncRecordLun(disk));
            MSRocksDB.remove(getComponentRecordLun(disk));
            MSRocksDB.remove(getRabbitmqRecordLun(disk));
            MSRocksDB.remove(getSTSTokenLun(disk));
            MSRocksDB.remove(getAggregateLun(disk));
        }
        return Mono.just(true);
    }

    @HandleErrorFunction(value = RELOAD_STORAGE_POOLS, timeout = 0L)
    public static Mono<Boolean> reloadStoragePools() {
        return StoragePoolFactory.forceReload();
    }

    /**
     * @param vnode        数字 vnode
     * @param dstDisk      目标盘，不带节点信息，fs-SP0-xxx
     * @param poolType     存储池前缀，如 dataa
     * @param poolQueueTag 存储池队列标识
     * @function 函数作用：(1) 将移除盘 srcDisk 的 vnode 更新至 dstDisk
     * (2) 更新表6中的映射并完成所有节点缓存中vnode的映射更新
     **/
    @HandleErrorFunction(value = REMOVE_DISK, timeout = 0L)
    public static Mono<Boolean> removeDisk(String vnode, String srcDisk, String dstDisk, String poolType, String poolQueueTag) {
        log.info("rebuild in {} pool", poolType);
        String runningKey = RUNNING_POOL_PREFIX + poolQueueTag;
        String state = null;
        String operate = null;
        String removeNode = null;
        try {
            state = RebuildRabbitMq.getMaster().hget(runningKey, "state");
            operate = RebuildRabbitMq.getMaster().hget(runningKey, "operate");
            removeNode = "";
            if (REMOVE_NODE.equals(operate)) {
                removeNode = RebuildRabbitMq.getMaster().hget(runningKey, "removedNode");
            }
        } catch (Exception e) {
            log.error("", e);
            return Mono.error(e);
        }
        if (!"running".equals(state)) {
            // 本次重构任务所有的remove_disk消息还未全部发布到mq中，则不进行后续操作
            return Mono.just(true).delayElement(Duration.ofSeconds(1))
                    .flatMap(v -> Mono.error(new NoPrintLogException()));
        }
        StoragePool pool = StoragePoolFactory.getStoragePool(poolType, null);
        Boolean replaceFlag = null;
        try {
            replaceFlag = RebuildRabbitMq.getMaster().hexists(runningKey, "replaceFlag");
        } catch (Exception e) {
            log.error("", e);
            return Mono.error(e);
        }
        if (replaceFlag) {
            // 磁盘更换，则初始化新盘
            MSRocksDB.getRocksDB(dstDisk);
            if (dstDisk.contains("index")) {
                MSRocksDB.getRocksDB(getSyncRecordLun(dstDisk));
                MSRocksDB.getRocksDB(getComponentRecordLun(dstDisk));
                MSRocksDB.getRocksDB(getSTSTokenLun(dstDisk));
                MSRocksDB.getRocksDB(getRabbitmqRecordLun(dstDisk));
                MSRocksDB.getRocksDB(getAggregateLun(dstDisk));
            }
            BlockDevice.get(dstDisk);
            pool.addLun(ServerConfig.getInstance().getHostUuid(), dstDisk);
        }
        RebuildLog.log(pool, vnode, "removeDisk start");
        if (!REMOVE_NODE.equals(operate)) {
            pool.updateVnodeDisk(vnode, dstDisk);
        }

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("disk", ServerConfig.getInstance().getHostUuid() + '@' + dstDisk)
                .put("objVnode", String.valueOf(vnode))
                .put("pool", pool.getVnodePrefix())
                .put("poolQueueTag", poolQueueTag);//RECOVER_VNODE_DATA消息增加存储池队列的标识，便于重发时发到存储池对应的队列

        RebuildLog.log(pool, vnode, "start updateDisk");
        return updateDisk(pool, vnode, dstDisk, removeNode).publishOn(DISK_SCHEDULER).doOnNext(b -> {
            RebuildLog.log(pool, vnode, "end updateDisk");
            ObjectPublisher.basicPublish(CURRENT_IP, msg, RECOVER_VNODE_DATA);
            RebuildRabbitMq.getMaster().hincrby(runningKey, "scanNum", 1);
            RebuildRabbitMq.getMaster().hincrby(runningKey, "vnodeNum", -1);
            if (srcDisk.contains("index")) {
                // 记录映射更新成功的vnode
                RebuildRabbitMq.getMaster().sadd(UPDATE_VNODE_COMPLETED_SET_PREFIX + poolQueueTag, vnode);
            }
        });
    }

    /**
     * 增加一个副本的数据
     */
    @HandleErrorFunction(value = EXPAND_VNODE_DATA, timeout = 0L)
    public static Mono<Boolean> expandVnodeData(String objVnode, String pool, String disk, String poolQueueTag, SocketReqMsg msg) {
        Flux<Object> flux = Flux.empty();
        String runningKey = RUNNING_POOL_PREFIX + poolQueueTag;
        StoragePool storagePool = StoragePoolFactory.getStoragePool(pool, null);
        List<Tuple3<String, String, String>> nodeList0 = storagePool.mapToNodeInfo(objVnode).block();

        RebuildRabbitMq.getMaster().hset(runningKey, "QoSFlag", "1");  // 一旦开启expand，即设置标志位

        String errorVnode = null;
        String lun = disk.split("@")[1];

        for (Tuple3<String, String, String> tuple3 : nodeList0) {
            if (tuple3.var1.equals(ServerConfig.getInstance().getHeartIp1()) && tuple3.var2.equalsIgnoreCase(lun)) {
                errorVnode = tuple3.var3;
            }
        }

        log.info("expand {} {}", objVnode, errorVnode);
        String finalErrorVnode = errorVnode;
        Flux<?> curFlux = waitWriteDone()
                .flatMap(b -> storagePool.mapToNodeInfo(objVnode))
                .publishOn(DISK_SCHEDULER)
                .flatMapMany(nodeList -> ListObjVnode.listVnodeObj(ScannerConfig.builder().pool(storagePool).vnode(objVnode).nodeList(nodeList).listLink(false).dstDisk(disk).build()))
                .publishOn(DISK_SCHEDULER)
                .doOnNext(meta -> {
                    ReBuildRunner.getInstance().addObjTask(finalErrorVnode, meta, storagePool, disk, "", null).subscribe();
                });

        flux = flux.mergeWith(curFlux);
        for (IndexDBEnum indexDBEnum : NEED_REBUILD_INDEX_DB) {
            if (indexDBEnum == IndexDBEnum.ROCKS_DB || disk.contains("index")) {
                if (indexDBEnum == IndexDBEnum.UNSYNC_RECORD_DB && !isMultiAliveStarted) {
                    continue;
                }
                curFlux = storagePool.mapToNodeInfo(objVnode)
                        .publishOn(DISK_SCHEDULER)
                        .flatMapMany(nodeList -> ListMetaVnode.listVnodeMeta(ScannerConfig.builder().pool(storagePool).vnode(objVnode).nodeList(nodeList).indexDBEnum(indexDBEnum).listLink(false).dstDisk(disk).build()))
                        .publishOn(DISK_SCHEDULER)
                        .doOnNext(tuple2 -> {
                            ReBuildRunner.getInstance().addMetaTask(tuple2, storagePool, disk, "", null).subscribe();
                        });

                flux = flux.mergeWith(curFlux);
            }
        }

        return flux.count()
                .map(l -> {
                    if (l >= 0) {
                        RebuildRabbitMq.getMaster().hincrby(runningKey, "taskNum", l);
                    }

                    RebuildRabbitMq.getMaster().hincrby(runningKey, "vnodeNum", -1);
                    return true;
                }).doOnError(e -> {
                    ObjectPublisher.basicPublish(CURRENT_IP, msg, EXPAND_VNODE_DATA);
                });
    }

    /**
     * 移除盘，更新完vnode后（前端更新缓存，redis中的vnode link由后端更新）
     *
     * @param objVnode     数字vnode
     * @param pool         存储池
     * @param disk         带节点信息的目标盘
     * @param poolQueueTag 存储池队列标识
     * @param msg          携带以上内容的消息
     * @function 扫描并重构
     */
    @HandleErrorFunction(value = RECOVER_VNODE_DATA, timeout = 0L)
    public static Mono<Boolean> recoverVnodeData(String objVnode, String pool, String disk, String poolQueueTag, SocketReqMsg msg) {
        log.info("recoverVnodeData {} {}", objVnode, disk);
        String runningKey = RUNNING_POOL_PREFIX + poolQueueTag;
        StoragePool storagePool = StoragePoolFactory.getStoragePool(pool, null);
        RebuildLog.log(storagePool, objVnode, "start recoverVnodeData");
        String vKey = DigestUtils.md5Hex(Json.encode(msg)) + "_" + poolQueueTag;
        if (msgSet.contains(vKey)) {
            return Mono.just(true);
        }
        String operate0 = null;
        try {
            operate0 = RebuildRabbitMq.getMaster().hget(runningKey, "operate");
        } catch (Exception e) {
            log.error("", e);
            return Mono.error(e);
        }
        RebuildCheckpointManager rebuildCheckpointManager = new RebuildCheckpointManager(storagePool, runningKey, objVnode, disk, poolQueueTag);
        if (rebuildCheckpointManager.checkRebuildComplete()) {
            log.info("rebuild running is completed {} {}", objVnode, disk);
            // 重构已完成
            return Mono.just(true);
        }
        final String operate = operate0;
        return rebuildCheckpointManager.verifyAndCreateCheckpoint()
                .flatMap(canRecover -> {
                    if (!canRecover) {
                        return Mono.delay(Duration.ofSeconds(ThreadLocalRandom.current().nextInt(3, 10)))
                                .doOnNext(s -> ObjectPublisher.basicPublish(CURRENT_IP, msg, RECOVER_VNODE_DATA))
                                .thenReturn(true);
                    }

                    Flux<Object> flux = Flux.empty();
                    msgSet.add(vKey);
                    if (rebuildCheckpointManager.checkRebuildPreComplete()) {
                        // 重构 预完成阶段，直接走结束流程
                        return rebuildCheckpointManager.verifyAndCloseCheckpoint()
                                .onErrorResume(e -> {
                                    if (e instanceof RequeueMQException) {
                                        log.error("need rePublish mq,:vnode:{},pool:{},disk:{},poolTag:{},error message:{}", objVnode, pool, disk, poolQueueTag, e.getMessage());
                                        return Mono.delay(Duration.ofSeconds(ThreadLocalRandom.current().nextInt(3, 10)))
                                                .doOnNext(s -> ObjectPublisher.basicPublish(CURRENT_IP, msg, RECOVER_VNODE_DATA))
                                                .thenReturn(true);
                                    }
                                    return Mono.error(e);
                                });
                    }
                    Flux<?> curFlux = waitWriteDone()
                            .doOnNext(b -> {
                                RebuildLog.log(storagePool, objVnode, "end waitWriteDone");
                            })
                            .flatMap(b -> storagePool.mapToNodeInfo(objVnode))
                            .publishOn(DISK_SCHEDULER)
                            .flatMapMany(nodeList -> ListObjVnode.listVnodeObj(ScannerConfig.builder()
                                    .pool(storagePool).vnode(objVnode).nodeList(nodeList).dstDisk(disk).vKey(vKey).operator(operate).build())
                            )
                            .doFinally(s -> {
                                RebuildLog.log(storagePool, objVnode, "end listVnodeObj");
                            });

                    flux = flux.mergeWith(curFlux);
                    for (IndexDBEnum indexDBEnum : NEED_REBUILD_INDEX_DB) {
                        if (indexDBEnum == IndexDBEnum.ROCKS_DB || disk.contains("index")) {
                            if (indexDBEnum == IndexDBEnum.UNSYNC_RECORD_DB && !isMultiAliveStarted) {
                                continue;
                            }
                            curFlux = storagePool.mapToNodeInfo(objVnode)
                                    .publishOn(DISK_SCHEDULER)
                                    .flatMapMany(nodeList -> ListMetaVnode.listVnodeMeta(ScannerConfig.builder().pool(storagePool).vnode(objVnode).nodeList(nodeList).indexDBEnum(indexDBEnum).dstDisk(disk).vKey(vKey).runningKey(runningKey).operator(operate).build()))
                                    .doFinally(s -> {
                                        RebuildLog.log(storagePool, objVnode, "end listVnodeMeta dir:" + indexDBEnum.getDir());
                                    });

                            flux = flux.mergeWith(curFlux);
                        }
                    }

                    return flux.count()
                            .flatMap(l -> {
                                RebuildLog.log(storagePool, objVnode, "end recoverVnodeData");
                                if (l >= 0) {
                                    RebuildRabbitMq.getMaster().hincrby(runningKey, "taskNum", l);
                                }

                                RebuildRabbitMq.getMaster().hincrby(runningKey, "scanNum", -1);
                                RebuildRabbitMq.getMaster().hincrby(runningKey, "migrateNum", -1);  // 所有盘初始化迁移文件数时都要加vnode数

                                return rebuildCheckpointManager.verifyAndCloseCheckpoint();
                            })
                            .onErrorResume(e -> {
                                if (e instanceof RequeueMQException) {
                                    log.error("need rePublish mq,:vnode:{},pool:{},disk:{},poolTag:{},error message:{}", objVnode, pool, disk, poolQueueTag, e.getMessage());
                                    return Mono.delay(Duration.ofSeconds(ThreadLocalRandom.current().nextInt(30, 60)))
                                            .doOnNext(s -> ObjectPublisher.basicPublish(CURRENT_IP, msg, RECOVER_VNODE_DATA))
                                            .thenReturn(true);
                                }
                                if (e != null) {
                                    return Mono.error(e);
                                }
                                return Mono.just(true);
                            });
                })
                .doFinally(s -> msgSet.remove(vKey));
    }

    @HandleErrorFunction(value = RECOVER_DISK_FILE, timeout = 0L)
    public static Mono<Boolean> recoverDiskFile(String lun, String metaKey, String fileName, String endIndex, List<Integer> errorChunksList,
                                                String fileSize, SocketReqMsg msg,
                                                List<Tuple3<String, String, String>> nodeList) {
        StoragePool storagePool = null;

        return PutErrorHandler
                .recoverSpecificChunks(storagePool, metaKey, lun, errorChunksList, RECOVER_DISK_FILE, fileName, Long.parseLong(endIndex), msg, nodeList)
                .doOnNext(b -> {
                    if (b) {
                        RebuildSpeed.add(Long.parseLong(fileSize));
                    }
                });
    }

    /**
     * @param vnode     数字vnode
     * @param objVnodes 更新vnode后的关联组
     * @param srcDisk   源盘
     * @param dstDisk   目标盘
     * @param vKey
     * @function 将当前vnode下的数据从源盘迁移至目标盘
     **/
    private static Mono<Boolean> migrateVnode(String vnode, Set<String> objVnodes, String srcDisk, String dstDisk, String poolType, String vKey, AtomicBoolean hasMigrateData) {
        RebuildLog.log(null, vnode, "start migrateVnode");
        Flux<Boolean> flux = Flux.empty();
        for (String objVnode : objVnodes) {
            flux = flux.mergeWith(LocalMigrateServer.getInstance().startMigrate(srcDisk, objVnode, dstDisk)
                    .doOnNext(b -> RebuildLog.log(null, vnode, objVnode + " start migrateVnode")));
        }

        return flux.publishOn(DISK_SCHEDULER).collectList()
                .doOnNext(b -> RebuildLog.log(null, vnode, " start copyVnodeData"))
                .flatMap(l -> LocalMigrate.copyVnodeData(vnode, srcDisk, dstDisk, poolType, MSRocksDB.IndexDBEnum.ROCKS_DB, vKey, hasMigrateData))
                .doOnNext(b -> {
                    log.info("migrate Vnode date {} from {} to {} end", vnode, srcDisk, dstDisk);
                })
                .flatMap(b -> {
                    if (isMultiAliveStarted && srcDisk.contains("index")) {
                        return LocalMigrate.copyVnodeData(vnode, srcDisk, dstDisk, poolType, MSRocksDB.IndexDBEnum.UNSYNC_RECORD_DB, vKey, hasMigrateData)
                                .doOnNext(s -> log.info("migrate DA Vnode date {} from {} to {} end", vnode, getSyncRecordLun(srcDisk), getSyncRecordLun(dstDisk)));
                    }
                    return Mono.just(b);
                }).flatMap(b -> {
                    if (srcDisk.contains("index")) {
                        return LocalMigrate.copyVnodeData(vnode, srcDisk, dstDisk, poolType, MSRocksDB.IndexDBEnum.COMPONENT_RECORD_DB, vKey, hasMigrateData)
                                .doOnNext(s -> log.info("migrate  Vnode date {} from {} to {} end", vnode, getComponentRecordLun(srcDisk), getComponentRecordLun(dstDisk)));
                    }
                    return Mono.just(b);
                })
                .flatMap(b -> {
                    if (srcDisk.contains("index")) {
                        return LocalMigrate.copyVnodeData(vnode, srcDisk, dstDisk, poolType, IndexDBEnum.STS_TOKEN_DB, vKey, hasMigrateData)
                                .doOnNext(s -> log.info("migrate  Vnode date {} from {} to {} end", vnode, getSTSTokenLun(srcDisk), getSTSTokenLun(dstDisk)));
                    } else {
                        return Mono.just(b);
                    }
                }).flatMap(b -> {
                    if (srcDisk.contains("index")) {
                        return LocalMigrate.copyVnodeData(vnode, srcDisk, dstDisk, poolType, IndexDBEnum.RABBITMQ_RECORD_DB, vKey, hasMigrateData)
                                .doOnNext(s -> log.info("migrate  Vnode date {} from {} to {} end", vnode, getRabbitmqRecordLun(srcDisk), getRabbitmqRecordLun(dstDisk)));
                    }
                    return Mono.just(b);
                }).flatMap(b -> {
                    if (srcDisk.contains("index")) {
                        return LocalMigrate.copyVnodeData(vnode, srcDisk, dstDisk, poolType, IndexDBEnum.AGGREGATE_DB, vKey, hasMigrateData)
                                .doOnNext(s -> log.info("migrate  Vnode date {} from {} to {} end", vnode, getAggregateLun(srcDisk), getAggregateLun(dstDisk)));
                    }
                    return Mono.just(b);
                });
    }

    /**
     * @param vnode     数字vnode
     * @param objVnodes link关联组
     * @param srcDisk   不带节点信息的源盘
     * @param dstDisk   不带节点信息的目标盘
     **/
    private static Mono<Boolean> endMigrateVnode(String vnode, Set<String> objVnodes, String srcDisk, String dstDisk, String poolType, boolean hasMigrateData) {
        return Mono.just(1).publishOn(DISK_SCHEDULER)
                .flatMapMany(l -> Flux.fromStream(objVnodes.stream()))
                .flatMap(objVnode -> LocalMigrateServer.getInstance().endMigrate(srcDisk, objVnode))
                .flatMap(t -> LocalMigrate.replayDeleteOperate(t, dstDisk))
                .collect(() -> true, (b, t) -> {
                })
                .flatMapMany(l -> Flux.fromStream(objVnodes.stream()))
                .flatMap(objVnode -> LocalMigrateServer.getInstance().endMigrateThenCopy(srcDisk, objVnode))
                .doOnNext(t -> LocalMigrate.replayCopyOperate(t, dstDisk))
                .collect(() -> true, (b, t) -> {
                })
                .flatMapMany(l -> Flux.fromStream(objVnodes.stream()))
                .collect(() -> true, (b, t) -> {
                })
                // 只有实际迁移了数据的vnode才需要删除数据
                .filter(b -> hasMigrateData)
                .doOnNext(b -> {
                    if (srcDisk.contains("index")) {
                        LocalMigrate.removeDataFileInRanges(srcDisk, objVnodes, MSRocksDB.IndexDBEnum.ROCKS_DB, false);
                        LocalMigrate.removeDataFileInRanges(srcDisk, objVnodes, MSRocksDB.IndexDBEnum.UNSYNC_RECORD_DB, false);
                        LocalMigrate.removeDataFileInRanges(srcDisk, objVnodes, MSRocksDB.IndexDBEnum.COMPONENT_RECORD_DB, false);
                        LocalMigrate.removeDataFileInRanges(srcDisk, objVnodes, MSRocksDB.IndexDBEnum.STS_TOKEN_DB, false);
                        LocalMigrate.removeDataFileInRanges(srcDisk, objVnodes, MSRocksDB.IndexDBEnum.RABBITMQ_RECORD_DB, false);
                        LocalMigrate.removeDataFileInRanges(srcDisk, objVnodes, MSRocksDB.IndexDBEnum.AGGREGATE_DB, false);
                    }
                })
                .flatMap(b -> LocalMigrate.removeVnodeDate(vnode, srcDisk, dstDisk, MSRocksDB.IndexDBEnum.ROCKS_DB, poolType))
                .doOnNext(b -> log.info("delete Vnode {} date from {} end", vnode, srcDisk))
                .flatMap(b -> {
                    if (isMultiAliveStarted && srcDisk.contains("index")) {
                        return LocalMigrate.removeVnodeDate(vnode, srcDisk, dstDisk, MSRocksDB.IndexDBEnum.UNSYNC_RECORD_DB, poolType)
                                .doOnNext(b2 -> log.info("delete Vnode {} date from {} end", vnode, getSyncRecordLun(srcDisk)));
                    }
                    return Mono.just(b);
                })
                .flatMap(b -> {
                    if (srcDisk.contains("index")) {
                        LocalMigrate.removeVnodeDate(vnode, srcDisk, dstDisk, MSRocksDB.IndexDBEnum.COMPONENT_RECORD_DB, poolType)
                                .doOnNext(b2 -> log.info("delete Vnode {} date from {} end", vnode, getComponentRecordLun(srcDisk)));
                    }
                    return Mono.just(b);
                })
                .flatMap(b -> {
                    if (srcDisk.contains("index")) {
                        return LocalMigrate.removeVnodeDate(vnode, srcDisk, dstDisk, IndexDBEnum.STS_TOKEN_DB, poolType)
                                .doOnNext(b2 -> log.info("delete Vnode {} date from {} end", vnode, getSTSTokenLun(srcDisk)));
                    } else {
                        return Mono.just(b);
                    }
                }).flatMap(b -> {
                    if (srcDisk.contains("index")) {
                        return LocalMigrate.removeVnodeDate(vnode, srcDisk, dstDisk, IndexDBEnum.RABBITMQ_RECORD_DB, poolType)
                                .doOnNext(b2 -> log.info("delete Vnode {} date from {} end", vnode, getRabbitmqRecordLun(srcDisk)));
                    } else {
                        return Mono.just(b);
                    }
                }).flatMap(b -> {
                    if (srcDisk.contains("index")) {
                        return LocalMigrate.removeVnodeDate(vnode, srcDisk, dstDisk, IndexDBEnum.AGGREGATE_DB, poolType)
                                .doOnNext(b2 -> log.info("delete Vnode {} date from {} end", vnode, getAggregateLun(srcDisk)));
                    } else {
                        return Mono.just(b);
                    }
                })
                .thenReturn(true);
    }

    /**
     * @param pool    重构存储池
     * @param vnode   要迁移的 vnode
     * @param dstDisk 要迁移到的新盘
     * @function 将redis表6中的映射更新
     **/
    private static Mono<Boolean> updateDisk(StoragePool pool, String vnode, String dstDisk, String removeNode) {
        return Mono.just(1L).publishOn(DISK_SCHEDULER)
                .flatMap(t -> {
                    String disk;
                    String masterRunId = RedisConnPool.getInstance().getMasterRunId();
                    if (masterRunId == null) {
                        return Mono.error(new IllegalStateException("master run id is null"));
                    }

                    if (StringUtils.isNotEmpty(removeNode)) {
                        Map<String, String> map = new HashMap<>(2);
                        map.put("lun_name", dstDisk);
                        map.put("s_uuid", ServerConfig.getInstance().getHostUuid());
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
                        return redisConnPool.syncRedis(masterRunId, removeNode);
                    } else {
                        if (vnode.charAt(0) == '-') {
                            disk = redisConnPool.getCommand(REDIS_MAPINFO_INDEX).hget(pool.getMapVnodePrefix() + vnode.substring(1), "lun_name");
                        } else {
                            disk = redisConnPool.getCommand(REDIS_MAPINFO_INDEX).hget(pool.getVnodePrefix() + vnode, "lun_name");
                        }

                        if (!dstDisk.equalsIgnoreCase(disk)) {
                            if (vnode.charAt(0) == '-') {
                                redisConnPool.getShortMasterCommand(REDIS_MAPINFO_INDEX)
                                        .hset(pool.getMapVnodePrefix() + vnode.substring(1), "lun_name", dstDisk);
                            } else {
                                String mapV = redisConnPool.getCommand(REDIS_MAPINFO_INDEX).hget(pool.getVnodePrefix() + vnode, "map");
                                redisConnPool.getShortMasterCommand(REDIS_MAPINFO_INDEX)
                                        .hset(pool.getVnodePrefix() + vnode, "lun_name", dstDisk);
                                if (mapV != null) {
                                    redisConnPool.getShortMasterCommand(REDIS_MAPINFO_INDEX)
                                            .hset(mapV, "lun_name", dstDisk);
                                }

                            }

                            return redisConnPool.syncRedis(masterRunId, removeNode);
                        }
                    }


                    return Mono.just(true);
                })
                .flatMap(sync -> {
                    if (StringUtils.isNotEmpty(removeNode)) {
                        SocketReqMsg msg = new SocketReqMsg("", 0)
                                .put("poolType", pool.getVnodePrefix())
                                .put("vnode", vnode)
                                .put("lun", dstDisk)
                                .put("node", ServerConfig.getInstance().getHostUuid());

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
                            if (removeNode.equals(RabbitMqUtils.getUUID(ip))) {
                                continue;
                            }
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
                    } else {
                        SocketReqMsg msg = new SocketReqMsg("", 0)
                                .put("pool", pool.getVnodePrefix())
                                .put("vnode", vnode)
                                .put("newDisk", dstDisk);

                        Flux<Boolean> res = Flux.empty();
                        RedisCommands<String, String> command = redisConnPool.getCommand(REDIS_NODEINFO_INDEX);
                        if (RabbitMqUtils.HEART_IP_LIST.size() != command.dbsize()) {
                            ScanIterator<String> iterator = ScanIterator.scan(command, new ScanArgs().match("*"));

                            while (iterator.hasNext()) {
                                String key = iterator.next();
                                ObjectConsumer.getInstance().addHeartIp(key);
                            }
                        }

                        for (String ip : RabbitMqUtils.HEART_IP_LIST) {
                            Mono<Boolean> mono = Mono.just(true).flatMap(b -> RSocketClient.getRSocket(ip, BACK_END_PORT))
                                    .flatMap(r -> r.requestResponse(DefaultPayload.create(Json.encode(msg), UPDATE_DISK_VNODE.name())))
                                    .timeout(Duration.ofSeconds(30))
                                    .map(r -> SUCCESS.name().equalsIgnoreCase(r.getMetadataUtf8()))
                                    .onErrorReturn(false);

                            UnicastProcessor<Boolean> flux = UnicastProcessor.create();
                            MonoProcessor<Boolean> processor = MonoProcessor.create();
                            flux.publishOn(DISK_SCHEDULER)
                                    .flatMap(b -> mono)
                                    .doOnComplete(() -> processor.onNext(true))
                                    .subscribe(b -> {
                                        if (b) {
                                            flux.onComplete();
                                        } else {
                                            DISK_SCHEDULER.schedule(() -> flux.onNext(true), 10, TimeUnit.SECONDS);
                                        }
                                    });
                            flux.onNext(true);
                            res = res.mergeWith(processor);
                        }

                        return res.collectList().map(l -> true);
                    }
                });
    }

    @HandleErrorFunction(value = CREATE_REBUILD_CONSUMER, timeout = 0L)
    public static Mono<Boolean> createRebuildConsumer(String newDisk) {
        ReBuildRunner.getInstance().rabbitMq.consumer(newDisk);
        return Mono.just(true);
    }

    @HandleErrorFunction(value = PREPARE_ADD_DISK, timeout = 0L)
    public static Mono<Boolean> prepareAddDisk(String disk, String poolQueueTag) {
        String runningKey = RUNNING_POOL_PREFIX + poolQueueTag;
        RebuildRabbitMq.getMaster().del(ADD_DISK_COMPLETED_SET_PREFIX + poolQueueTag);
        JSONArray diskArray = JSONArray.parseArray(disk);
        for (int i = 0; i < diskArray.size(); i++) {
            String diskKey = ServerConfig.getInstance().getHostUuid() + "@" + diskArray.getString(i);
            RabbitMqUtils.HEART_IP_LIST.forEach(ip -> ObjectPublisher.basicPublish(ip,
                    new SocketReqMsg("", 0)
                            .put("newDisk", diskKey).put("poolQueueTag", poolQueueTag), CREATE_REBUILD_CONSUMER));
        }

        try {

            String uuid = RebuildRabbitMq.getMaster().hget(runningKey, "node");
            String poolName = RebuildRabbitMq.getMaster().hget(runningKey, "poolName");
            String poolListStr = redisConnPool.getCommand(SysConstants.REDIS_POOL_INDEX).hget(poolName, "file_system");
            String poolPrefix = redisConnPool.getCommand(SysConstants.REDIS_POOL_INDEX).hget(poolName, "prefix");
            String[] poolArray = poolListStr.substring(1, poolListStr.length() - 1).split(", ");
            String poolType = RebuildRabbitMq.getMaster().hget(runningKey, "poolType");
            StoragePool pool = StoragePoolFactory.getStoragePool(poolPrefix, null);
            //初始化磁盘实例和rocksdb
            for (int i = 0; i < diskArray.size(); i++) {
                initRocksDbAndBlockDevice(diskArray.getString(i), pool);
            }

            long objSumAllDisk = 0L;
            String[] disks = disk.substring(1, disk.length() - 1).split(",");
            int lunNum = 0;  // 当前节点在触发addDisk后的磁盘数量
            for (int i = 0; i < disks.length; i++) {
                String diskName = disks[i].substring(1, disks[i].length() - 1);
                long objSumCalculate = 0L;      // 其他盘文件总量初始值
                double srcDiskObjNum = 0.0;     // 当前待迁移磁盘文件数量
                if (!poolType.equals("meta")) {
                    long totalSize = 0L;        // 总容量初始值
                    long addDiskSize = -1;      // 移除盘容量初始值

                    for (String curDisk : poolArray) {
                        if (curDisk.startsWith(uuid)) {
                            // 如果当前磁盘为新加盘
                            if (curDisk.split("@")[1].equalsIgnoreCase(diskName)) {
                                addDiskSize = Long.parseLong(redisConnPool.getCommand(SysConstants.REDIS_LUNINFO_INDEX).hget(curDisk, "lun_size"));
                                totalSize = totalSize + addDiskSize;
                                lunNum = lunNum + 1;
                            } else {
                                long curDiskSize = Long.parseLong(redisConnPool.getCommand(SysConstants.REDIS_LUNINFO_INDEX).hget(curDisk, "lun_size"));
                                totalSize = totalSize + curDiskSize;
                                try {
                                    // 一次加入两块盘的情况下，新加盘的rocksDB无实际对象
                                    byte[] curDiskObjNumBytes = MSRocksDB.getRocksDB(curDisk.split("@")[1]).get(BlockDevice.ROCKS_FILE_SYSTEM_FILE_NUM);
                                    if (curDiskObjNumBytes != null) {
                                        lunNum = lunNum + 1;
                                        long curDiskObjNum = bytes2long(curDiskObjNumBytes);
                                        objSumCalculate += curDiskObjNum;
                                    }
                                } catch (RocksDBException e) {
                                    log.error("", e);
                                }
                            }
                        }
                    }
                    // 添加盘文件量=其他盘包含的文件总量/所有盘容量*添加盘容量
                    srcDiskObjNum = (double) objSumCalculate / totalSize * addDiskSize;
                }
                objSumAllDisk = objSumAllDisk + (long) Math.ceil(srcDiskObjNum);
            }

            if (!poolType.equals("meta")) {
                RebuildRabbitMq.getMaster().hincrby(runningKey, "migrateNum", objSumAllDisk);
                RebuildRabbitMq.getMaster().hincrby(runningKey, "srcDiskObjNum", objSumAllDisk);
                log.info("file system disk array size: {}, fileNum: {}", poolArray.length, objSumAllDisk);
            }

            // 缓存池可能存在仅存一块盘被拔后加回的情况
            // (1) 当前存储池fileSystem中，本节点只有新加盘，而没有其它磁盘，则从其他节点查询文件量
            // (2) 当前存储池fileSystem中，本节点除本次addDisk新加盘，还有task任务中的新加盘，则从其它节点查询文件量
            // (3) 当前存储池fileSystem中，本节点除新加盘、task新加盘外还有其它存在的盘，则无需再从其它节点查询
            if (poolType.equals("cache") && objSumAllDisk == 0L) {
                boolean isOnly = true;
                if (lunNum != disks.length) {
                    // 检查表15存储池fileSystem中当前节点是否仅存在新加盘
                    HashSet<String> diskSet = new HashSet<>();
                    LinkedList<String> addDisks = new LinkedList<>(Arrays.asList(disks));
                    LinkedList<String> totalDisks = new LinkedList<>(Arrays.asList(poolArray));
                    Iterator<String> iterator = totalDisks.iterator();
                    try {
                        while (iterator.hasNext()) {
                            String key = iterator.next();
                            String node = key.split("@")[0];
                            String fileDisk = key.split("@")[1];
                            if (node.equalsIgnoreCase(uuid)) {
                                // 是否存在除了新加盘以外的磁盘，同时该磁盘不能是被移除的磁盘
                                if (!addDisks.contains(fileDisk) && !RemovedDisk.getInstance().contains(fileDisk)) {
                                    diskSet.add(fileDisk);
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("check addDisk error", e);
                    }

                    // 检查fileSystem中多出来的盘是否都为task任务中的新加盘；如果是则从其它节点查文件量，如果不是则不需再查
                    if (!diskSet.isEmpty()) {
                        try {
                            RedisCommands<String, String> command = redisConnPool.getCommand(REDIS_MIGING_V_INDEX);
                            ScanIterator<String> redisIterator = ScanIterator.scan(command, new ScanArgs().match("task*"));

                            while (redisIterator.hasNext()) {
                                String key = redisIterator.next();
                                if (key.startsWith("task")) {
                                    String operate = redisConnPool.getCommand(REDIS_MIGING_V_INDEX).hget(key, "operate");
                                    String nodeId = redisConnPool.getCommand(REDIS_MIGING_V_INDEX).hget(key, "node");
                                    if (nodeId.equals(uuid) && operate.equalsIgnoreCase("add_disk")) {
                                        String taskDisk = redisConnPool.getCommand(REDIS_MIGING_V_INDEX).hget(key, "diskName");
                                        if (taskDisk.startsWith("[") && taskDisk.endsWith("]")) {
                                            String[] taskDisks = taskDisk.substring(1, taskDisk.length() - 1).split(",");
                                            for (int k = 0; k < taskDisks.length; k++) {
                                                String curTaskDisk = taskDisks[k].substring(1, taskDisks[k].length() - 1);
                                                if (diskSet.contains(curTaskDisk)) {
                                                    diskSet.remove(curTaskDisk);
                                                }
                                            }

                                            if (diskSet.isEmpty()) {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }

                            // 如果多出来的盘中有部分不是task中的，也不是本次add的，则无需再计算文件量
                            if (!diskSet.isEmpty()) {
                                isOnly = false;
                                log.info("diskSet: {}", diskSet);
                            }
                        } catch (Exception e) {
                            log.error("scan redis table 12 tasks error", e);
                        }
                    }
                }

                // 仅当完成以上两次检查，确定是当前无盘的情况下新加盘
                if (isOnly) {
                    queryNodeFileNum(poolListStr, poolName);
                }
            }
        } catch (Exception e) {
            log.error("Calculate migrateNum or srcDiskObjNum error ", e);
        }

        return Mono.just(true);
    }

    /**
     * @param vnode        数字vnode
     * @param srcDisk      不带节点信息的源盘，形如 fs-SP0-1
     * @param dstDisk      不带节点信息的目标盘，形如 fs-SP0-2
     * @param poolType     存储池前缀
     * @param poolQueueTag 存储池队列标识
     **/
    @HandleErrorFunction(value = ADD_DISK, timeout = 0L)
    public static Mono<Boolean> addDisk(String vnode, String srcDisk, String dstDisk, String poolType, String poolQueueTag) {
        StoragePool pool = StoragePoolFactory.getStoragePool(poolType, null);
        String runningKey = RUNNING_POOL_PREFIX + poolQueueTag;
        log.info("migrate {} in {} pool", vnode, poolType);
        RebuildLog.log(pool, vnode, "start addDisk");
        //初始化新盘各rocksdb实例及批处理对象
        initRocksDbAndBlockDevice(dstDisk, pool);
        String vKey = SCAN_MIGRATE_POSITION_PREFIX + DigestUtils.md5Hex(Json.encode(vnode + srcDisk + dstDisk + poolType)) + "_" + poolQueueTag;

        String removeNode;
        String operate;
        try {
            operate = RebuildRabbitMq.getMaster().hget(runningKey, "operate");
            if (REMOVE_NODE.equals(operate)) {
                removeNode = RebuildRabbitMq.getMaster().hget(runningKey, "removedNode");
            } else {
                removeNode = "";
            }
        } catch (Exception e) {
            log.error("", e);
            return Mono.error(e);
        }
        String[] link = pool.getLink(vnode);
        Set<String> objVnodes = new HashSet<>();
        Collections.addAll(objVnodes, link);
        AtomicBoolean hasMigrateData = new AtomicBoolean(false);
        return migrateVnode(vnode, objVnodes, srcDisk, dstDisk, poolType, vKey, hasMigrateData)
                .publishOn(DISK_SCHEDULER)
                .flatMap(l -> updateDisk(pool, vnode, dstDisk, removeNode))
                .delayElement(Duration.ofSeconds(2))
                .flatMap(l -> waitWriteDone().map(b -> l))
                .doOnNext(b -> RebuildLog.log(pool, vnode, " start endMigrateVnode"))
                .flatMap(l -> endMigrateVnode(vnode, objVnodes, srcDisk, dstDisk, poolType, hasMigrateData.get()))
                .map(l -> {
                    RebuildRabbitMq.getMaster().del(vKey);
                    Optional.ofNullable(migrateVKeyMap.remove(vKey)).ifPresent(Map::clear);
                    RebuildLog.log(pool, vnode, "end addDisk");
                    MigrateUtil.checkAndHandleAddDiskCompletion(poolQueueTag, vnode, runningKey, operate);
                    return true;
                });
    }

    /**
     * 初始化磁盘rocksdb实例和磁盘块设备实例
     *
     * @param dstDisk 磁盘
     * @param pool    存储池
     */
    public static void initRocksDbAndBlockDevice(String dstDisk, StoragePool pool) {
        MSRocksDB.getRocksDB(dstDisk);
        if (dstDisk.contains("index")) {
            MSRocksDB.getRocksDB(getSyncRecordLun(dstDisk));
            MSRocksDB.getRocksDB(getComponentRecordLun(dstDisk));
            MSRocksDB.getRocksDB(getSTSTokenLun(dstDisk));
            MSRocksDB.getRocksDB(getRabbitmqRecordLun(dstDisk));
            MSRocksDB.getRocksDB(getAggregateLun(dstDisk));
        } else {
            // 数据盘、缓存盘 需要进行磁盘加载
            BlockDevice.get(dstDisk);
        }
        if (pool != null) {
            pool.addLun(ServerConfig.getInstance().getHostUuid(), dstDisk);
        }
    }


    /**
     * 修改ec重新存储数据池中的对象
     *
     * @param poolNameArray
     * @param poolTypes
     * @param updateTime
     * @param msg
     * @return
     */
    @HandleErrorFunction(value = RESTORE_POOL_DATA, timeout = 0L)
    public static Mono<Boolean> prepareRestorePoolData(String[] poolNameArray, Map<String, String> poolTypes, String updateTime, String poolQueueTag, SocketReqMsg msg) {
        log.info("start to restore pool data!!!");
        MonoProcessor<Long> timeRes = MonoProcessor.create();
        long updateTimeStamp = Long.parseLong(updateTime);
        Flux<Object> flux = Flux.empty();
        Map<String, String> bucketMap = new HashMap<>();
        Set<String> storageStrategySet = new HashSet<>();
        Map<String, String> poolInStrategy = new HashMap<>();
        Map<String, String> poolNameMap = new HashMap<>();

        try {
            //获取要修改的存储池所关联的存储策略
            for (String poolName : poolNameArray) {
                String strategyList = redisConnPool.getCommand(REDIS_POOL_INDEX).hget(poolName, "storage_strategy");
                String prefix = redisConnPool.getCommand(REDIS_POOL_INDEX).hget(poolName, "prefix");
                JSONArray strategyArray = JSONArray.parseArray(strategyList);
                String strategy = strategyArray.getString(0);
                storageStrategySet.add(strategy);
                poolInStrategy.put(prefix, strategy);
                poolNameMap.put(prefix, poolName);
            }

            //遍历redis中表7，获取到存储策略下所有的桶
            RedisCommands<String, String> command = redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX);
            ScanIterator<String> iterator = ScanIterator.scan(command, new ScanArgs().match("*"));

            while (iterator.hasNext()) {
                String key = iterator.next();
                if (!"hash".equals(command.type(key))) {
                    continue;
                }
                if (storageStrategySet.contains(command.hget(key, "storage_strategy"))) {
                    String status = Optional.ofNullable(redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX).hget(key, BUCKET_VERSION_STATUS))
                            .orElse("NULL");
                    bucketMap.put(key, status);
                }
                if (command.hexists(key, "transition_strategy")) {
                    JSONArray transitionStrategy = JSONArray.parseArray(command.hget(key, "transition_strategy"));
                    for (String strategy : transitionStrategy.toArray(new String[0])) {
                        //若当前遍历到的桶中有对象通过生命周期迁移到本次修改ec数据池相关联的存储策略中，则该桶中对象加入扫描
                        if (storageStrategySet.contains(strategy)) {
                            String status = Optional.ofNullable(redisConnPool.getCommand(REDIS_BUCKETINFO_INDEX).hget(key, BUCKET_VERSION_STATUS))
                                    .orElse("NULL");
                            bucketMap.put(key, status);
                            break;
                        }
                    }
                }
            }
            for (String poolName : poolNameArray) {
                String runningKey = RUNNING_POOL_PREFIX + poolName;
                RebuildRabbitMq.getMaster().hincrby(runningKey, "scanNum", bucketMap.size());//对于原running中的处理多数据池修改ec时每个数据池的running均作相同的处理
            }
        } catch (Exception e) {
            log.error("", e);
            return Mono.error(e);
        }
        log.info("get bucketMap：{}", bucketMap);


        //获取这些桶中存放在该存储池的所有的对象
        Flux<?> curFlux = waitWriteDone()
                .flatMap(b -> timeRes)
                .publishOn(DISK_SCHEDULER)
                .flatMapMany(time -> listPoolObj(poolNameArray, poolTypes, time, bucketMap.keySet()))
                .publishOn(DISK_SCHEDULER)
                .doOnNext(metaData -> {
                    restoreObjectPublish(metaData, poolTypes, bucketMap, poolInStrategy, poolNameMap, poolNameArray);
                });

        flux = flux.mergeWith(curFlux);

        curFlux = waitWriteDone()
                .publishOn(DISK_SCHEDULER)
                .flatMapMany(b -> listPoolInitPart(poolNameArray, poolTypes, bucketMap.keySet()))
                .publishOn(DISK_SCHEDULER)
                .doOnNext(initPartInfo -> {
                    restoreInitPartPublish(initPartInfo, poolTypes, poolInStrategy, poolNameMap);
                })
                .doOnComplete(() -> {
                    long completeTime = System.currentTimeMillis();
                    timeRes.onNext(completeTime);
                    log.info("-------ScanInitPart complete time: {}", completeTime);
                });

        flux = flux.mergeWith(curFlux);

        return flux.count()
                .map(l -> {
                    if (l >= 0) {
                        log.info("the value of l : {}", l);
                    }
                    return true;
                })
                .doOnError(e -> {
                    ObjectPublisher.basicPublish(CURRENT_IP, msg, RESTORE_POOL_DATA);
                });
    }

    public static void restoreObjectPublish(MetaData metaData, Map<String, String> poolTypes, Map<String, String> bucketMap,
                                            Map<String, String> poolInStrategy, Map<String, String> poolNameMap, String[] poolNameArray) {
//        log.info("publish msg about restore object data {},{}", metaData.bucket, metaData.key);
        String objectName = metaData.getKey();
        String bucketName = metaData.getBucket();
        String versionId = metaData.getVersionId();
        String oldPoolType = metaData.getStorage();
        String newPoolType = poolTypes.get(oldPoolType);
        String strategy = poolInStrategy.get(newPoolType);
        String status = bucketMap.get(bucketName);
        String poolQueueTag = poolNameMap.get(newPoolType);//发布到该任务对应的存储池队列中

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("oldPoolType", oldPoolType)
                .put("newPoolType", newPoolType)
                .put("strategy", strategy)
                .put("bucketName", bucketName)
                .put("objectName", objectName)
                .put("status", status)
                .put("versionId", versionId)
                .put("poolNameArray", JSON.toJSONString(poolNameArray))
                .put("poolQueueTag", poolQueueTag);
        Optional.ofNullable(metaData.getSnapshotMark()).ifPresent(v -> msg.put("snapshotMark", v));

        String ip = CURRENT_IP;
        final int currentIndex = ThreadLocalRandom.current().nextInt(0, HEART_IP_LIST.size());
        ip = HEART_IP_LIST.get(currentIndex);

        ObjectPublisher.basicPublish(ip, msg, RESTORE_OBJECT_DATA);
    }

    public static void restoreInitPartPublish(InitPartInfo initPartInfo, Map<String, String> poolTypes, Map<String, String> poolInStrategy, Map<String, String> poolNameMap) {
        String oldPoolType = initPartInfo.getStorage();
        String newPoolType = poolTypes.get(oldPoolType);
        String strategy = poolInStrategy.get(newPoolType);
        String bucketName = initPartInfo.getBucket();
        String objectName = initPartInfo.getObject();
        String uploadId = initPartInfo.getUploadId();
        String versionId = null != initPartInfo.metaData ? initPartInfo.metaData.getVersionId() : null;
        String poolQueueTag = poolNameMap.get(newPoolType);//发布到该任务对应的存储池队列中

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("oldPoolType", oldPoolType)
                .put("newPoolType", newPoolType)
                .put("strategy", strategy)
                .put("bucketName", bucketName)
                .put("objectName", objectName)
                .put("uploadId", uploadId)
                .put("versionId", versionId)
                .put("poolQueueTag", poolQueueTag);
        Optional.ofNullable(initPartInfo.getSnapshotMark()).ifPresent(v -> msg.put("snapshotMark", v));

        String ip = CURRENT_IP;
        final int currentIndex = ThreadLocalRandom.current().nextInt(0, HEART_IP_LIST.size());
        ip = HEART_IP_LIST.get(currentIndex);

        ObjectPublisher.basicPublish(ip, msg, RESTORE_INIT_PART_DATA);
    }

    /**
     * 列出需要修改ec的存储池下的对象元数据
     *
     * @param poolNameArray
     * @param poolTypes
     * @param updateTimeStamp
     * @param bucketSet
     * @return
     */
    public static Flux<MetaData> listPoolObj(String[] poolNameArray, Map<String, String> poolTypes, long updateTimeStamp,
                                             Set<String> bucketSet) {
        UnicastProcessor<MetaData> res = UnicastProcessor.create();
        log.info("updateTimeStamp: {}", updateTimeStamp);
        if (bucketSet.size() == 0) {
            res.onComplete();
            return res;
        }
        Set<String> oldPoolTypeSet = poolTypes.keySet();

        return Flux.fromStream(bucketSet.stream())
                .flatMap(bucket -> scanObject(bucket, updateTimeStamp, oldPoolTypeSet, poolNameArray));
//                .filter(metaData -> (!metaData.deleteMark) && oldPoolTypeSet.contains(metaData.getStorage()));

    }

    public static Flux<InitPartInfo> listPoolInitPart(String[] poolNameArray, Map<String, String> poolTypes,
                                                      Set<String> bucketSet) {
        UnicastProcessor<InitPartInfo> res = UnicastProcessor.create();
        if (bucketSet.size() == 0) {
            res.onComplete();
            return res;
        }
        Set<String> oldPoolTypeSet = poolTypes.keySet();

        return Flux.fromStream(bucketSet.stream())
                .flatMap(bucket -> scanInitPartInfo(bucket, oldPoolTypeSet));
//                .filter(metaData -> (!metaData.deleteMark) && oldPoolTypeSet.contains(metaData.getStorage()));


    }

    public static Flux<MetaData> scanObject(String bucketName, long updateTimeStamp, Set<String> oldPoolTypeSet, String[] poolNameArray) {
        UnicastProcessor<MetaData> res = UnicastProcessor.create();
        StoragePool bucketStoragePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        List<String> bucketVnodeList = bucketStoragePool.getBucketVnodeList(bucketName);
        AtomicInteger nextRunNumber = new AtomicInteger(0);
        AtomicLong objectNum = new AtomicLong(0);

        AtomicLong bucketVnodeCompleteFlag = new AtomicLong(0);
        UnicastProcessor<String> shardProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        shardProcessor.subscribe(vnode -> {
            int maxKey = 1000;
            String stampMarker = updateTimeStamp + 1 + "";
            SocketReqMsg reqMsg = new SocketReqMsg("", 0);
            String beginPrefix = getLifeCycleStamp(vnode, bucketName, "0");
            UnicastProcessor<SocketReqMsg> listController = UnicastProcessor.create(Queues.<SocketReqMsg>unboundedMultiproducer().get());
            listController.onNext(reqMsg.put("bucket", bucketName)
                    .put("maxKeys", String.valueOf(maxKey))
                    .put("stamp", stampMarker)
                    .put("retryTimes", "0")
                    .put("beginPrefix", beginPrefix));

            listController.subscribe(msg -> {
                if ("error".equals(msg.getMsgType())) {
                    throw new MsException(UNKNOWN_ERROR, "");
                }
                bucketStoragePool.mapToNodeInfo(vnode)
                        .flatMap(infoList -> SnapshotUtil.fetchBucketSnapshotInfo(bucketName, msg).thenReturn(infoList))
                        .subscribe(infoList -> {
                            String[] nodeArr = infoList.stream().map(info -> info.var3).toArray(String[]::new);
                            msg.put("vnode", nodeArr[0]);
                            List<SocketReqMsg> msgs = infoList.stream().map(info -> msg.copy().put("lun", info.var2)).collect(Collectors.toList());
                            //获取截止到修改ec时间前桶中存在的对象
                            ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo = ClientTemplate.oneResponse(msgs, LIST_LIFE_OBJECT, new TypeReference<Tuple3<Boolean,
                                    String, MetaData>[]>() {
                            }, infoList);
                            ListRestoreObjClientHandler listRestoreObjClientHandler = new ListRestoreObjClientHandler(bucketStoragePool, listController, responseInfo, nodeArr[0], reqMsg);
                            responseInfo.responses.publishOn(DISK_SCHEDULER).subscribe(listRestoreObjClientHandler::handleResponse,
                                    e -> log.error("", e),
                                    listRestoreObjClientHandler::completeResponse);
                            listRestoreObjClientHandler.res.subscribe(metaDataList -> {
                                metaDataList.forEach(counter -> {
                                    if ((!counter.getMetaData().deleteMark) && oldPoolTypeSet.contains(counter.getMetaData().getStorage())) {
                                        res.onNext(counter.getMetaData());
                                        objectNum.incrementAndGet();//统计对象数时过滤掉带有删除标记和在缓存池以及在新存储池中的对象
                                    }
                                });
//                                objectNum.addAndGet(metaDataList.size());
                            });
                        });
            }, log::error, () -> {
                bucketVnodeCompleteFlag.incrementAndGet();
                if (bucketVnodeCompleteFlag.get() == bucketVnodeList.size()) {
                    log.info("bucket:{} processed Object count: {}", bucketName, objectNum);
                    res.onComplete();
                    for (String poolName : poolNameArray) {//每个数据池的running同步设置进度值
                        String runningKey = RUNNING_POOL_PREFIX + poolName;
                        long scanNum = RebuildRabbitMq.getMaster().hincrby(runningKey, "scanNum", -1);
                        long getNum = RebuildRabbitMq.getMaster().hincrby(runningKey, "getNum", objectNum.get());
                        if (0 == scanNum) {
                            RebuildRabbitMq.getMaster().hincrby(runningKey, "taskNum", getNum);
                        }
                    }
                }
            });
        }, log::error);
        for (int i = 0; i < bucketVnodeList.size(); ++i) {
            int next = nextRunNumber.getAndIncrement();
            shardProcessor.onNext(bucketVnodeList.get(next));
        }
        return res;
    }

    public static Flux<InitPartInfo> scanInitPartInfo(String bucketName, Set<String> oldPoolTypeSet) {
        UnicastProcessor<InitPartInfo> res = UnicastProcessor.create();
        StoragePool bucketStoragePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        List<String> bucketVnodeList = bucketStoragePool.getBucketVnodeList(bucketName);
        AtomicInteger nextRunNumber = new AtomicInteger(0);
        AtomicLong objectNum = new AtomicLong(0);

        AtomicLong bucketVnodeCompleteFlag = new AtomicLong(0);
        UnicastProcessor<String> shardProcessor = UnicastProcessor.create(Queues.<String>unboundedMultiproducer().get());
        shardProcessor.subscribe(vnode -> {
            int maxUploadsInt = 1000;
            SocketReqMsg reqMsg = new SocketReqMsg("", 0);
            UnicastProcessor<SocketReqMsg> listController = UnicastProcessor.create(Queues.<SocketReqMsg>unboundedMultiproducer().get());
            listController.onNext(reqMsg.put("bucket", bucketName)
                    .put("maxUploads", String.valueOf(maxUploadsInt))
                    .put("prefix", "")
                    .put("retryTimes", "0")
                    .put("marker", "")
                    .put("delimiter", "")
                    .put("uploadIdMarker", ""));

            listController.subscribe(msg -> {
                if ("error".equals(msg.getMsgType())) {
                    throw new MsException(UNKNOWN_ERROR, "");
                }
                bucketStoragePool.mapToNodeInfo(vnode)
                        .flatMap(infoList -> SnapshotUtil.fetchBucketSnapshotInfo(bucketName, msg).thenReturn(infoList))
                        .subscribe(infoList -> {
                            String[] nodeArr = infoList.stream().map(info -> info.var3).toArray(String[]::new);
                            msg.put("vnode", nodeArr[0]);
                            List<SocketReqMsg> msgs = infoList.stream().map(info -> msg.copy().put("lun", info.var2)).collect(Collectors.toList());
                            //获取截止到修改ec时间前桶中存在的对象
                            ClientTemplate.ResponseInfo<Tuple3<Boolean, String, InitPartInfo>[]> responseInfo = ClientTemplate.oneResponse(msgs, LIST_MULTI_PART_UPLOAD, LIST_MULTI_PART_UPLOADS_RESPONSE_TYPE_REFERENCE, infoList);
                            ListInitPartInfoClientHandler listInitPartInfoClientHandler = new ListInitPartInfoClientHandler(bucketStoragePool, listController, responseInfo, nodeArr[0], reqMsg);
                            responseInfo.responses.publishOn(DISK_SCHEDULER).subscribe(listInitPartInfoClientHandler::handleResponse,
                                    e -> log.error("", e),
                                    listInitPartInfoClientHandler::completeResponse);
                            listInitPartInfoClientHandler.res.subscribe(initPartInfoList -> {
                                initPartInfoList.forEach(counter -> {
                                    if ((!counter.getInitPartInfo().delete) && oldPoolTypeSet.contains(counter.getInitPartInfo().getStorage())) {
                                        res.onNext(counter.getInitPartInfo());
                                        objectNum.incrementAndGet();//统计对象数时过滤掉带有删除标记以及storage为新数据池的初始化分段信息
                                    }
                                });
                            });
                        });
            }, log::error, () -> {
                bucketVnodeCompleteFlag.incrementAndGet();
                if (bucketVnodeCompleteFlag.get() == bucketVnodeList.size()) {
                    log.info("bucket:{} processed initPartInfo count: {}", bucketName, objectNum);
                    res.onComplete();

                }
            });
        }, log::error);
        for (int i = 0; i < bucketVnodeList.size(); ++i) {
            int next = nextRunNumber.getAndIncrement();
            shardProcessor.onNext(bucketVnodeList.get(next));
        }
        return res;
    }

    @HandleErrorFunction(value = RESTORE_OBJECT_DATA, timeout = 0L)
    public static Mono<Boolean> restoreObjData(String oldPoolType, String newPoolType, String strategy, String bucketName, String objectName, String versionId, String poolQueueTag, String[] poolNameArray, SocketReqMsg msg, String snapshotMark) {
        log.debug("begin restoring object from {} to {} , bucket:{}, object:{}, versionId:{}", oldPoolType, newPoolType, bucketName, objectName, versionId);
        StoragePool newPool = StoragePoolFactory.getStoragePool(newPoolType, bucketName);
        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        Tuple2<String, String> bucketVnodeIdTuple = metaStoragePool.getBucketVnodeIdTuple(bucketName, objectName);
        String bucketVnode = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        List<String> delFileName = new ArrayList<>();
        StorageStrategy storageStrategy = POOL_STRATEGY_MAP.get(strategy);
        String dataStr = storageStrategy.redisMap.get("data");
        List<String> storageList = Arrays.asList(Json.decodeValue(dataStr, String[].class));//获取到当前存储策略下关联的数据池
        AtomicBoolean notOperate = new AtomicBoolean(false);
        boolean[] isDup = new boolean[2];
        List<String> oldDedupFileName = new ArrayList<>();

        return metaStoragePool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> ErasureClient.getObjectMetaVersionRes(bucketName, objectName, versionId, nodeList, null, snapshotMark, null))
                .timeout(Duration.ofSeconds(30))
                .flatMap(tuple2 -> {
                    MetaData metaData = tuple2.var1;
                    if (metaData.equals(MetaData.ERROR_META)) {
                        throw new MsException(UNKNOWN_ERROR, "Get Object Meta Data fail");
                    }
                    if (metaData.equals(MetaData.NOT_FOUND_META) || metaData.deleteMark || metaData.deleteMarker || newPoolType.equals(metaData.storage)
                            || (!oldPoolType.equals(metaData.storage) && !storageList.contains(metaData.storage)) || (!oldPoolType.equals(metaData.storage) && storageList.contains(metaData.storage))) {//如果对象已经迁移到了另一个策略中，则该对象不再进行重新存储处理
                        notOperate.set(true);
                        return Mono.just(true);
                    } else {
                        Map<String, String> sysMetaMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
                        });
                        StoragePool oldPool = StoragePoolFactory.getNoUsedStoragePool(oldPoolType);
                        String requestId = getRequestId();
                        return StoragePoolFactory.getDeduplicateByStrategy(strategy)
                                .doOnNext(dup -> isDup[0] = dup)
                                .doOnNext(dup -> {
                                    if (metaData.partUploadId != null) {
                                        for (PartInfo partInfo : metaData.partInfos) {
                                            oldDedupFileName.add(partInfo.fileName);
                                        }
                                    } else {
                                        oldDedupFileName.add(metaData.fileName);
                                    }
                                })
                                .flatMap(dup -> new RestoreObjectTask(oldPool, newPool, requestId, dup).restoreObject(metaData)
                                        .flatMap(tuple -> {
                                            if (tuple.getT1()) {//重新存储完整后修改元数据中的storage,并更新对象元数据,最后删除原ec存储的对象数据
                                                metaData.setStorage(newPool.getVnodePrefix());
                                                if (metaData.partUploadId != null) {
                                                    int index = 0;
                                                    for (PartInfo partInfo : metaData.partInfos) {
                                                        if (StringUtils.isNotEmpty(partInfo.deduplicateKey)) {
                                                            delFileName.add(partInfo.deduplicateKey);
                                                            if (!isDup[0]) {
                                                                partInfo.setDeduplicateKey(null);
                                                                partInfo.setFileName(tuple.getT2()[Integer.parseInt(partInfo.partNum) - 1]);
                                                            }
                                                        } else {
                                                            if (!isDup[0]) {
                                                                delFileName.add(partInfo.fileName);
                                                                partInfo.setFileName(tuple.getT2()[Integer.parseInt(partInfo.partNum) - 1]);
                                                            } else {
                                                                delFileName.add(oldDedupFileName.get(index));
                                                                index++;
                                                            }
                                                        }
                                                        partInfo.setStorage(newPool.getVnodePrefix());
                                                        if (isDup[0]) {
                                                            String vnodeId = metaStoragePool.getBucketVnodeId(partInfo.etag);
                                                            String dedupKey = Utils.getDeduplicatMetaKey(vnodeId, partInfo.etag, newPool.getVnodePrefix(), requestId);
                                                            partInfo.setDeduplicateKey(dedupKey);
                                                        }
                                                    }
                                                } else {
                                                    if (StringUtils.isNotEmpty(metaData.duplicateKey)) {
                                                        delFileName.add(metaData.duplicateKey);
                                                        if (!isDup[0]) {
                                                            metaData.setDuplicateKey(null);
                                                            metaData.setFileName(tuple.getT2()[0]);
                                                        }
                                                    } else {
                                                        if (!isDup[0]) {
                                                            metaData.setDuplicateKey(null);
                                                            delFileName.add(metaData.fileName);
                                                            metaData.setFileName(tuple.getT2()[0]);
                                                        } else {
                                                            delFileName.add(oldDedupFileName.get(0));
                                                        }
                                                    }
                                                    if (isDup[0]) {
                                                        String md5 = sysMetaMap.get(ETAG);
                                                        String vnodeId = metaStoragePool.getBucketVnodeId(md5);
                                                        String dedupKey = Utils.getDeduplicatMetaKey(vnodeId, md5, newPool.getVnodePrefix(), requestId);
                                                        metaData.setDuplicateKey(dedupKey);
                                                    }
                                                }
                                                return Mono.just(StringUtils.isNotEmpty(migrateVnode))
                                                        .flatMap(notEmpty -> {
                                                            if (notEmpty) {
                                                                return metaStoragePool.mapToNodeInfo(migrateVnode)
                                                                        .flatMap(migrateVnodeList -> ErasureClient.getObjectMetaVersionResOnlyRead(metaData.bucket, metaData.key, metaData.versionId, migrateVnodeList, null, metaData.snapshotMark, null).zipWith(Mono.just(migrateVnodeList)))
                                                                        .flatMap(resTuple -> ErasureClient.updateMetaDataAcl(getVersionMetaDataKey(migrateVnode, metaData.bucket, metaData.key, metaData.versionId, metaData.snapshotMark), metaData.clone(), resTuple.getT2(), null, resTuple.getT1().var2))
                                                                        .timeout(Duration.ofSeconds(30))
                                                                        .doOnNext(i -> {
                                                                            if (i != 1) {
                                                                                log.error("update new mapping meta storage error! {}", i);
                                                                            }
                                                                        })
                                                                        .map(i -> i == 1)
                                                                        .doOnError(e -> log.error("", e))
                                                                        .onErrorReturn(false);
                                                            }
                                                            return Mono.just(true);
                                                        })
                                                        .flatMap(writeSuccess -> {
                                                            if (writeSuccess) {
                                                                return metaStoragePool.mapToNodeInfo(bucketVnode)
                                                                        .flatMap(nodeList -> ErasureClient.updateMetaDataAcl(getVersionMetaDataKey(bucketVnode, metaData.bucket, metaData.key, metaData.versionId, metaData.snapshotMark), metaData, nodeList, null, tuple2.var2))
                                                                        .timeout(Duration.ofSeconds(30))
                                                                        .flatMap(m -> {
                                                                            if (m != 2) {
                                                                                if (m != 1) {
                                                                                    log.error("update meta storage error! {}", m);
                                                                                }
                                                                                return Mono.just(m == 1);
                                                                            }
                                                                            log.info("update result: {}", m);
                                                                            return metaStoragePool.mapToNodeInfo(bucketVnode)
                                                                                    .flatMap(nodeList -> ErasureClient.getObjectMetaVersionRes(bucketName, objectName, versionId, nodeList, null, metaData.snapshotMark, null).zipWith(Mono.just(nodeList)))
                                                                                    .flatMap(resTuple -> ErasureClient.updateMetaDataAcl(getVersionMetaDataKey(bucketVnode, metaData.bucket, metaData.key, metaData.versionId, metaData.snapshotMark), metaData, resTuple.getT2(), null, resTuple.getT1().var2))
                                                                                    .flatMap(s -> Mono.just(s == 1));
                                                                        })
//                                                                        .map(i -> i == 1)
                                                                        .doOnError(e -> log.error("", e))
                                                                        .onErrorReturn(false);
                                                            }
                                                            return Mono.just(false);
                                                        })
                                                        .flatMap(res -> {
                                                            if (res) {
                                                                //开始删除原ec存储的对象数据
                                                                log.debug("delete file {} {}", Arrays.toString(delFileName.toArray(new String[0])), metaData.versionNum);
                                                                boolean deDup = false;
                                                                Set<String> allFile = new HashSet<>();
                                                                for (String name : delFileName) {
                                                                    if (name.startsWith(ROCKS_DEDUPLICATE_KEY)) {
                                                                        deDup = true;
                                                                        break;
                                                                    }
                                                                }
                                                                for (String files : delFileName) {
                                                                    if (!files.startsWith(ROCKS_DEDUPLICATE_KEY)) {
                                                                        allFile.add(files);
                                                                    }
                                                                }
                                                                if (deDup) {
                                                                    return ErasureClient.deleteDedupObjectFile(metaStoragePool, delFileName.toArray(new String[0]), null, false)
                                                                            .flatMap(b1 -> {
                                                                                if (b1 && !allFile.isEmpty()) {
                                                                                    return ErasureClient.restoreDeleteObjectFile(oldPool, allFile.toArray(new String[0]), null)
                                                                                            .flatMap(r -> {
                                                                                                if (!r) {//返false表示有文件在被读取无法进行删除
                                                                                                    SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                                                                                            .put("fileName", Json.encode(allFile.toArray(new String[0])))
                                                                                                            .put("storage", oldPool.getVnodePrefix())
                                                                                                            .put("poolQueueTag", poolQueueTag);
                                                                                                    ObjectPublisher.basicPublish(CURRENT_IP, errorMsg, ERROR_RESTORE_DELETE_OBJECT_FILE);
                                                                                                }
                                                                                                return Mono.just(true);
                                                                                            });
                                                                                }
                                                                                return Mono.just(b1);
                                                                            });
                                                                }
                                                                return ErasureClient.restoreDeleteObjectFile(oldPool, delFileName.toArray(new String[0]), null)
                                                                        .flatMap(r -> {
                                                                            if (!r) {//返false表示有文件在被读取无法进行删除
                                                                                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                                                                        .put("fileName", Json.encode(delFileName.toArray(new String[0])))
                                                                                        .put("storage", oldPool.getVnodePrefix())
                                                                                        .put("poolQueueTag", poolQueueTag);
                                                                                ObjectPublisher.basicPublish(CURRENT_IP, errorMsg, ERROR_RESTORE_DELETE_OBJECT_FILE);
                                                                            }
                                                                            return Mono.just(true);
                                                                        });
                                                            } else {
                                                                log.error("update meta storage error {} {} {} {}", metaData.bucket, metaData.key, metaData.versionId, metaData.storage);
                                                                return Mono.just(false);
                                                            }
                                                        });
                                            } else {
                                                log.error("restore object from {} to {} error, bucket:{}, object:{}, versionId:{}", oldPoolType, newPoolType, bucketName, objectName, versionId);
                                                return Mono.just(false);
                                            }
                                        }));
                    }

                })
                .doOnNext(b -> {
                    if (!b) {//这里如果重新存储失败，抛出异常将消息重新放入队列
                        throw new MsException(UNKNOWN_ERROR, "restore object error!");
                    } else {
                        if (notOperate.get()) {
                            log.info("process successfully, the object do not need to restore, bucket:{}, object:{}, versionId:{}", bucketName, objectName, versionId);
                        } else {
                            log.info("restore object from {} to {} successfully, bucket:{}, object:{}, versionId:{}", oldPoolType, newPoolType, bucketName, objectName, versionId);
                        }
                        for (String poolName : poolNameArray) {
                            String runningKey = RUNNING_POOL_PREFIX + poolName;
                            RebuildRabbitMq.getMaster().hincrby(runningKey, "restoredObjectNum", 1);
                        }
//                        RebuildRabbitMq.getMaster().hincrby("running", "restoredObjectNum", 1);
                    }
                })
                .doOnError(e -> {
                    log.error("", e);
                    ObjectPublisher.basicPublish(CURRENT_IP, msg, RESTORE_OBJECT_DATA);
                });//这里如果抛出的是MsException异常且错误码不为REPEATED_KEY时消息不会由于异常而进行重发, 所以需要手动重新发布消息到ECerror队列中
    }

    @HandleErrorFunction(value = RESTORE_INIT_PART_DATA, timeout = 0L)
    public static Mono<Boolean> restoreInitPartData(String oldPoolType, String newPoolType, String strategy, String bucketName, String objectName, String uploadId, String versionId, String poolQueueTag, SocketReqMsg msg, String snapshotMark) {//此时传递的是InitPartInfo中的信息
        log.debug("begin restoring object from {} to {} , bucket:{}, object:{}, uploadId:{}", oldPoolType, newPoolType, bucketName, objectName, uploadId);

        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        StoragePool newPool = StoragePoolFactory.getStoragePool(newPoolType, bucketName);
        Tuple2<String, String> bucketVnodeIdTuple = metaStoragePool.getBucketVnodeIdTuple(bucketName, objectName);
        String bucketVnode = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        AtomicBoolean notOperate = new AtomicBoolean(false);
        boolean[] isDup = new boolean[2];

        return metaStoragePool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> PartClient.getInitPartInfoRes(bucketName, objectName, uploadId, nodeList, null, snapshotMark))
                .timeout(Duration.ofSeconds(30))
                .flatMap(tuple2 -> {
                    InitPartInfo initPartInfo = tuple2.var1;
                    if (initPartInfo.equals(InitPartInfo.ERROR_INIT_PART_INFO)) {
                        throw new MsException(UNKNOWN_ERROR, "Get Object init part info fail!");
                    }
                    if (initPartInfo.equals(InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO) || initPartInfo.delete) {
                        log.info("initPartInfo：{}" + initPartInfo.toString());
                        if (initPartInfo.equals(InitPartInfo.NO_SUCH_UPLOAD_ID_INIT_PART_INFO) || initPartInfo.delete) {
                            //如果在修改ec处理该分段任务之前就合并分段，则发送消息将合并完成的对象进行重新存储处理
                            SocketReqMsg msg0 = new SocketReqMsg("", 0)
                                    .put("oldPoolType", oldPoolType)
                                    .put("newPoolType", newPoolType)
                                    .put("strategy", strategy)
                                    .put("bucketName", bucketName)
                                    .put("objectName", objectName)
                                    .put("versionId", versionId)
                                    .put("poolQueueTag", poolQueueTag);
                            Optional.ofNullable(snapshotMark).ifPresent(v -> msg0.put("snapshotMark", v));

                            ObjectPublisher.basicPublish(CURRENT_IP, msg0, RESTORE_OBJECT_DATA);
                        }
                        notOperate.set(true);
                        return Mono.just(true);
                    } else {
                        StoragePool oldPool = StoragePoolFactory.getNoUsedStoragePool(oldPoolType);
                        String requestId = getRequestId();
                        if (newPoolType.equals(initPartInfo.storage)) {
                            return StoragePoolFactory.getDeduplicateByStrategy(strategy)
                                    .doOnNext(dup -> isDup[0] = dup)
                                    .flatMap(dup -> {
                                        return new RestoreObjectTask(oldPool, newPool, requestId, dup).restoreInitPartObj(initPartInfo);
                                    });
                        }
                        //首先更新InitPartInfo中的storage
                        initPartInfo.setStorage(newPoolType);
                        initPartInfo.metaData.setStorage(newPoolType);
                        return Mono.just(StringUtils.isNotEmpty(migrateVnode))
                                .flatMap(notEmpty -> {
                                    if (notEmpty) {
                                        return metaStoragePool.mapToNodeInfo(migrateVnode)
                                                .flatMap(migrateVnodeList -> PartClient.getInitPartInfoRes(bucketName, objectName, uploadId, migrateVnodeList, null, snapshotMark).zipWith(Mono.just(migrateVnodeList)))
                                                .flatMap(resTuple -> PartClient.repairInitPartInfo(InitPartInfo.getPartKey(migrateVnode, bucketName, objectName, uploadId, snapshotMark), initPartInfo, resTuple.getT2(), null, resTuple.getT1().var2))
                                                .timeout(Duration.ofSeconds(30))
                                                .doOnNext(i -> {
                                                    if (i != 1) {
                                                        log.error("update new mapping init part info error! {}", i);
                                                    }
                                                })
                                                .map(i -> i == 1)
                                                .doOnError(e -> log.error("", e))
                                                .onErrorReturn(false);
                                    }
                                    return Mono.just(true);
                                })
                                .flatMap(writeSuccess -> {
                                    if (writeSuccess) {
                                        return metaStoragePool.mapToNodeInfo(bucketVnode)
                                                .flatMap(nodeList -> PartClient.repairInitPartInfo(InitPartInfo.getPartKey(bucketVnode, bucketName, objectName, uploadId, snapshotMark), initPartInfo, nodeList, null, tuple2.var2))
                                                .timeout(Duration.ofSeconds(30))
                                                .flatMap(m -> {
                                                    if (m != 2) {
                                                        if (m != 1) {
                                                            log.error("update meta storage error! {}", m);
                                                        }
                                                        return Mono.just(m == 1);
                                                    }
                                                    log.info("update result: {}", m);
                                                    return metaStoragePool.mapToNodeInfo(bucketVnode)
                                                            .flatMap(nodeList -> PartClient.getInitPartInfoRes(bucketName, objectName, uploadId, nodeList, null, snapshotMark).zipWith(Mono.just(nodeList)))
                                                            .flatMap(resTuple -> PartClient.repairInitPartInfo(InitPartInfo.getPartKey(bucketVnode, bucketName, objectName, uploadId, snapshotMark), initPartInfo, resTuple.getT2(), null, resTuple.getT1().var2))
                                                            .flatMap(s -> Mono.just(s == 1));
                                                })
//                                                .map(i -> i == 1)
                                                .doOnError(e -> log.error("", e))
                                                .onErrorReturn(false);
                                    }
                                    return Mono.just(false);
                                })
                                .flatMap(res -> {
                                    if (res) {//InitPartInfo更新成功，开始获取partInfo进行重新存储
                                        //首先根据InitPartInfo获取到所有的part，可以调用complete_part获取
                                        return StoragePoolFactory.getDeduplicateByStrategy(strategy)
                                                .doOnNext(dup -> isDup[0] = dup)
                                                .flatMap(dup -> {
                                                    return new RestoreObjectTask(oldPool, newPool, requestId, dup).restoreInitPartObj(initPartInfo);
                                                });
                                    } else {
                                        log.error("update init part info error {} {} {} {}", initPartInfo.bucket, initPartInfo.object, initPartInfo.uploadId, initPartInfo.storage);
                                        return Mono.just(false);
                                    }
                                });
                    }
                })
                .doOnNext(b -> {
                    if (!b) {//这里如果重新存储失败，抛出异常将消息重新放入队列
                        throw new MsException(UNKNOWN_ERROR, "restore init part error!");
                    } else {
                        if (notOperate.get()) {
                            log.info("process successfully, the object do not need to restore, bucket:{}, object:{}, uploadId:{}", bucketName, objectName, uploadId);
                        } else {
                            log.info("restore object from {} to {} successfully, bucket:{}, object:{}, uploadId:{}", oldPoolType, newPoolType, bucketName, objectName, uploadId);
                        }
                    }
                })
                .doOnError(e -> {
                    log.error("", e);
                    ObjectPublisher.basicPublish(CURRENT_IP, msg, RESTORE_INIT_PART_DATA);
                });

    }


    /**
     * 等待所有节点截止当前时间点前数据写入完成
     *
     * @return
     */
    public static Mono<Boolean> waitWriteDone() {
        Flux<Boolean> res = Flux.empty();
        for (String ip : HEART_IP_LIST) {
            Mono<Boolean> mono = RSocketClient.getRSocket(ip, BACK_END_PORT)
                    .flatMap(r -> r.requestResponse(DefaultPayload.create("", WAIT_WRITE_DONE.name())))
                    .timeout(Duration.ofSeconds(30))
                    .map(r -> SUCCESS.name().equalsIgnoreCase(r.getMetadataUtf8()))
                    .onErrorReturn(false);
            res = res.mergeWith(mono);
        }
        return res.collectList().map(l -> true);
    }

    /**
     * 加盘时如果当前节点无盘，则从其它节点获取迁移文件总量
     *
     * @param poolListStr 存储池中的磁盘磁盘列表
     */
    public static void queryNodeFileNum(String poolListStr, String poolName) {
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("poolListStr", poolListStr);
        AtomicLong fileNum = new AtomicLong(0L);
        String runningKey = RUNNING_POOL_PREFIX + poolName;

        List<String> ipList = RabbitMqUtils.HEART_IP_LIST.stream().filter(s -> !s.equals(CURRENT_IP)).collect(Collectors.toList());
        String curIp = ServerConfig.getInstance().getHeartIp1();
        Flux<Tuple2<String, Long>> res = Flux.empty();

        for (int i = 0; i < ipList.size(); i++) {
            // 本节点不再查询
            String nodeIp = ipList.get(i);
            if (!nodeIp.equalsIgnoreCase(curIp)) {
                Mono<Tuple2<String, Long>> mono = RSocketClient.getRSocket(nodeIp, BACK_END_PORT)
                        .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), QUERY_NODE_FILE_NUM.name())))
                        .timeout(Duration.ofSeconds(30))
                        .map(payload -> {
                            try {
                                String metaDataPayload = payload.getMetadataUtf8();
                                if (metaDataPayload.equalsIgnoreCase(SUCCESS.name())) {
                                    return new Tuple2<String, Long>(nodeIp, Long.parseLong(payload.getDataUtf8()));
                                }
                                return new Tuple2<String, Long>(nodeIp, 0L);
                            } finally {
                                payload.release();
                            }
                        })
                        .onErrorReturn(new Tuple2<String, Long>(nodeIp, 0L))
                        .doOnError(e -> log.info("query node {} file num error", nodeIp, e));

                res = res.mergeWith(mono);
            }
        }

        res.subscribe(t -> {
            if (t.var2 > fileNum.get()) {
                fileNum.set(t.var2);
            }
            log.info("ip: {}, fileNum: {}", t.var1, t.var2);
        }, e -> log.info("set rebalanced progress error", e), () -> {
            log.info("runningKey: {}, fileNum: {}", runningKey, fileNum.get());
            RebuildRabbitMq.getMaster().hincrby(runningKey, "migrateNum", fileNum.get());
            RebuildRabbitMq.getMaster().hincrby(runningKey, "srcDiskObjNum", fileNum.get());
        });

    }

    public static long bytes2long(byte[] b) {
        long s = 0;
        long s0 = b[0] & 0xff;// 最低位
        long s1 = b[1] & 0xff;
        long s2 = b[2] & 0xff;
        long s3 = b[3] & 0xff;
        long s4 = b[4] & 0xff;// 最低位
        long s5 = b[5] & 0xff;
        long s6 = b[6] & 0xff;
        long s7 = b[7] & 0xff;

        // s0不变
        s1 <<= 8;
        s2 <<= 16;
        s3 <<= 24;
        s4 <<= 8 * 4;
        s5 <<= 8 * 5;
        s6 <<= 8 * 6;
        s7 <<= 8 * 7;
        s = s0 | s1 | s2 | s3 | s4 | s5 | s6 | s7;
        return s;
    }

    @HandleErrorFunction(ERROR_REBUILD_DELETE_CHECK_POINT)
    public static Mono<Boolean> deleteCheckPoint(String[] needCloseCheckPoint, String runningKey, String removeDisk, String poolQueueTag) {
        return closeCheckPoint(new ArrayList<>(Arrays.asList(needCloseCheckPoint)), runningKey, removeDisk, poolQueueTag)
                .doOnNext(b -> {
                    if (!b) {
                        throw new RequeueMQException();
                    }
                });
    }

}
