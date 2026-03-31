package com.macrosan.filesystem.async;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.doubleActive.HeartBeatChecker;
import com.macrosan.doubleActive.arbitration.Arbitrator;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.doubleActive.arbitration.DAVersionUtils;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.HeartBeatChecker.heartBeatIsNormal;
import static com.macrosan.doubleActive.arbitration.Arbitrator.MASTER_INDEX;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.*;
import static com.macrosan.filesystem.async.AsyncUtils.SYNC_TYPE.LOCAL;
import static com.macrosan.httpserver.MossHttpClient.*;

@Log4j2
public class AsyncUtils {
    static RedisConnPool pool = RedisConnPool.getInstance();

    public static final String MOUNT_CLUSTER = "mountCluster";
    // opt码
    public static IntHashSet set = new IntHashSet();

    /**
     * 对应InodeOperator中的操作
     */
    static {
        /* updateInodeData */
        set.add(1);
        /* updateInodeACL */
        set.add(2);
        /* deleteInode */
        set.add(3);
        /* createHardLink */
        set.add(4);
        /* renameFile */
        set.add(5);
        /* setAttr */
        set.add(6);
        /* updateInodeTime */
        set.add(7);
        /* updateInodeLinkN，覆盖或删除文件时更新元数据 */
        set.add(10);
        /* updateObjACL */
        set.add(16);
        /* updateCIFSACL */
        set.add(17);
        /* createInode */
        set.add(50);
    }

    public enum SYNC_TYPE {
        // 复制
        ASYNC,
        // 双活
        SYNC,
        // 转发本地节点
        REDIRECT,
        // 转发其他站点
        REDIRECT2,
        LOCAL,
        REJECT
    }

    public static boolean checkOptForAsync(int opt, SocketReqMsg msg) {
        return HeartBeatChecker.isMultiAliveStarted && set.contains(opt) && !"1".equals(msg.get("async"));
    }


    public static Mono<Inode> async(String bucket, long nodeID, int opt, SocketReqMsg msg, Mono<Inode> localMono) {
        return getSyncType(bucket)
                .flatMap(sync_type -> {
                    log.debug("opt5 : {}, {}, {}", nodeID, opt, sync_type);
                    switch (sync_type) {
                        case ASYNC: {
                            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
                            String bucketVnode = storagePool.getBucketVnodeId(bucket);
                            return beforeService(bucket, nodeID, opt, msg.dataMap)
                                    .flatMap(tuple2 -> {
                                        Boolean b = tuple2.getT1();
                                        UnSynchronizedRecord record = tuple2.getT2();
                                        log.debug("opt6 : {}, {}, {}", nodeID, opt, record.syncStamp);
                                        if (!b) { // 写差异记录失败
                                            log.error("write first record err. {} {} {}", opt, bucket, nodeID);
                                            return Mono.error(new RuntimeException("write first record err."));
                                        }
                                        return localMono.flatMap(i -> {
                                            if (i.getLinkN() >= 0) {
                                                record.setCommited(true);
                                                return storagePool.mapToNodeInfo(bucketVnode)
                                                        .flatMap(nodeList -> ECUtils.updateSyncRecord(record, nodeList, WRITE_ASYNC_RECORD))
                                                        .doOnNext(i0 -> log.debug("opt7 : {}, {}, {}", nodeID, opt, record.syncStamp))
                                                        .map(s -> i);
                                            } else {
                                                log.debug("opt8 : {}, {}, {}, {}", nodeID, opt, record.syncStamp, i);
                                                return storagePool.mapToNodeInfo(bucketVnode)
                                                        .flatMap(nodeList -> {
                                                            if (IS_THREE_SYNC) {
                                                                return DoubleActiveUtil.deleteUnsyncRecordMono(bucket, record.rocksKey(), null, DELETE_SYNC_RECORD);
                                                            } else {
                                                                return DoubleActiveUtil.deleteUnsyncRecordMono(bucket, record.rocksKey(), null, DELETE_ASYNC_RECORD);
                                                            }
                                                        })
                                                        .map(s -> i);
                                            }
                                        });
                                    });
                        }
                        case LOCAL: {
                            return localMono;
                        }
                        case REJECT:
                            log.info("reject this nfsPutObjSync, bucket: {}, inode: {}", bucket, nodeID);
                        default: {
                            return Mono.error(new RuntimeException("no such sync type: " + sync_type));
                        }

                    }
                });
    }

    /**
     * 适配InodeCache批量处理。差异记录的预提交写失败一次就视为整体失败。
     */
    public static Mono<Inode> asyncBatch(String bucket, long nodeID, int opt, List<SocketReqMsg> msgs, Mono<Inode> localMono) {
        return getSyncType(bucket)
                .flatMap(sync_type -> {
                    // todo del
                    log.debug("opt5 : {} {}, {}, {}", bucket, nodeID, opt, sync_type);
                    switch (sync_type) {
                        case ASYNC: {
                            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
                            String bucketVnode = storagePool.getBucketVnodeId(bucket);

                            // 改进：先处理所有 msgs 的 beforeService
                            return Flux.fromIterable(msgs)
                                    // 过滤缓存池下刷请求
                                    // opt7可能会出现oldInodeData
                                    .filter(msg -> StringUtils.isBlank(msg.get("oldInodeData")))
                                    // concatMap保证执行顺序
                                    .concatMap(msg -> beforeService(bucket, nodeID, opt, msg.dataMap))
                                    .doOnNext(tuple2 -> {
                                        if (!tuple2.getT1()) {
                                            log.error("Some beforeService operations failed. {} {} {}", opt, bucket, nodeID);
                                        }
                                    })
                                    .collectList()  // 收集所有 beforeService 结果
                                    .flatMap(beforeResults -> {
                                        // 检查所有 beforeService 是否都成功
                                        boolean allSuccess = beforeResults.stream()
                                                .allMatch(Tuple2::getT1);

                                        if (!allSuccess) {
                                            return Mono.error(new RuntimeException("Some beforeService operations failed."));
                                        }

                                        // 提取所有记录
                                        List<UnSynchronizedRecord> records = beforeResults.stream()
                                                .map(Tuple2::getT2)
                                                .collect(Collectors.toList());

                                        // 执行 localMono
                                        return localMono.flatMap(inode -> {
                                            // 根据 localMono 结果处理每个记录
                                            if (inode.getLinkN() >= 0) {
                                                // 所有记录都更新
                                                return storagePool.mapToNodeInfo(bucketVnode)
                                                        .flatMap(nodeList ->
                                                                Flux.fromIterable(records)
                                                                        .flatMap(record -> {
                                                                            record.setCommited(true);
                                                                            return ECUtils.updateSyncRecord(record, nodeList, WRITE_ASYNC_RECORD)
                                                                                    .doOnNext(i0 -> {
                                                                                        if (i0 == 0) {
                                                                                            log.error("updateSyncRecord err. {} {} {}", opt, bucket, nodeID);
                                                                                        }
                                                                                    });
                                                                        })
                                                                        .collectList()
                                                        )
                                                        .map(results -> inode);
                                            } else {
                                                // 所有记录都删除
                                                log.info("opt8 : {}, {}, {}, {}", nodeID, opt, records.size(), inode);
                                                return Flux.fromIterable(records)
                                                        .flatMap(record -> {
                                                            if (IS_THREE_SYNC) {
                                                                return DoubleActiveUtil.deleteUnsyncRecordMono(bucket, record.rocksKey(), null, DELETE_SYNC_RECORD);
                                                            } else {
                                                                return DoubleActiveUtil.deleteUnsyncRecordMono(bucket, record.rocksKey(), null, DELETE_ASYNC_RECORD);
                                                            }
                                                        })
                                                        .collectList()
                                                        .map(results -> inode);
                                            }
                                        });
                                    });
                        }
                        case LOCAL: {
                            return localMono;
                        }
                        case REJECT:
                            log.info("reject this nfsPutObjSync, bucket: {}, inode: {}", bucket, nodeID);
                            return Mono.error(new RuntimeException("Operation rejected for bucket: " + bucket));
                        default: {
                            return Mono.error(new RuntimeException("no such sync type: " + sync_type));
                        }
                    }
                });
    }

    public static Mono<SYNC_TYPE> getSyncType(String bucket) {
        return pool.getReactive(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, SYNC_POLICY)
                .zipWith(pool.getReactive(REDIS_BUCKETINFO_INDEX)
                        .hgetall(bucket)
                        .defaultIfEmpty(new HashMap<>())
                        .doOnNext(info -> {
                            BucketSyncSwitchCache.getInstance().check(bucket, info);
                        }))
                .flatMap(tuple2 -> {
                    if ("0".equals(tuple2.getT1())) {
                        // 双活环境暂不支持文件
                        log.warn("fs is not suitable for sync clusters!");
                        return Mono.just(LOCAL);
                    }

                    if (!checkFSProtocol(tuple2.getT2())) {
                        return Mono.just(LOCAL);
                    }

                    String syncPolicy = tuple2.getT1();
                    String bucketSyncSwitch = tuple2.getT2().getOrDefault(DATA_SYNC_SWITCH, SWITCH_OFF);
                    if (StringUtils.isBlank(syncPolicy) || !isSwitchOn(bucketSyncSwitch)) {
                        return Mono.just(LOCAL);
                    }
                    if (Arbitrator.isEvaluatingMaster.get()) {
                        return Mono.error(new RuntimeException("Master Index has not checked complete. "));
                    }
                    if (IS_ASYNC_CLUSTER) {
                        return Mono.just(LOCAL);
                    }
                    if ("1".equals(syncPolicy) || SWITCH_SUSPEND.equals(bucketSyncSwitch)) {
                        //1为异步复制。双活的桶如果同步为暂停也走异步复制流程。
                        String[] localClusterIps = INDEX_IPS_MAP.get(LOCAL_CLUSTER_INDEX);
                        // 可用ip里无本地eth12 ip，说明本地eth12 ping不通
                        if (!Arrays.asList(localClusterIps).contains(LOCAL_NODE_IP)) {
                            if (DAVersionUtils.isStrictConsis() && !LOCAL_CLUSTER_INDEX.equals(MASTER_INDEX)) {
                                //如果开启了双活优化，从节点的复制链路不通会获取不到DASyncStamp，此时会轮询eth4可用的节点转发
                                ////MossHttpClient.getInstance().redirectHttpRequest(REDIRECTABLE_BACKEND_IP_SET.toArray(new String[0]), request, false);
                                return Mono.just(SYNC_TYPE.REDIRECT);
                            }
                        }
                        // 当前节点的复制链路通，或不通但没有开双活优化，则正常走异步复制流程
                        if (BucketSyncSwitchCache.getSyncIndexMap(bucket, LOCAL_CLUSTER_INDEX).isEmpty()) {
                            return Mono.just(LOCAL);
                        } else {
                            return Mono.just(SYNC_TYPE.ASYNC);
                        }
                    } else if ("0".equals(tuple2.getT1()) && SWITCH_ON.equals(tuple2.getT2())) {
                        return pool.getReactive(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, LINK_STATE)
                                .flatMap(linkState -> {
                                    if ("1".equals(linkState)) {
                                        String[] localClusterIps = INDEX_IPS_MAP.get(LOCAL_CLUSTER_INDEX);
                                        // 可用ip里无本地eth12 ip，说明本地节点eth12不通，直接转发到eth12通的节点
                                        if (!Arrays.asList(localClusterIps).contains(LOCAL_NODE_IP)) {
                                            return Mono.just(SYNC_TYPE.REDIRECT);
                                        } else {
                                            // 当前节点的复制链路通，则正常走双活流程
                                            return Mono.just(SYNC_TYPE.SYNC);
                                        }
                                    } else {
                                        // 双活复制链路异常，如果本站点心跳正常则走异步复制流程。
                                        // 如果是本站点心跳有问题(比如索引池异常，无法写预提交记录)，则将请求转发到心跳正常的站点走异步复制流程。
                                        if (heartBeatIsNormal(LOCAL_CLUSTER_INDEX)) {
                                            return Mono.just(SYNC_TYPE.ASYNC);
                                        } else {
                                            return Mono.just(SYNC_TYPE.REDIRECT2);
                                        }
                                    }
                                });
                    } else {
                        return Mono.just(LOCAL);
                    }
                });
    }

    public static boolean checkFSProtocol(Map<String, String> bucketInfo) {
        return "1".equals(bucketInfo.get("nfs")) || "1".equals(bucketInfo.get("cifs"));
    }

    /**
     * 本地写之前的预提交记录
     */
    public static Mono<reactor.util.function.Tuple2<Boolean, UnSynchronizedRecord>> beforeService(String bucket, long nodeID, int opt, Map<String, String> map) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);

        return VersionUtil.getVersionNum(bucket, String.valueOf(nodeID))
                .flatMap(syncStamp -> {
                    // 这里生成syncStamp，注意传递给后续业务流程
//                    inode.setSyncStamp(syncStamp);
                    UnSynchronizedRecord record = new UnSynchronizedRecord(bucket, opt, syncStamp, nodeID);
                    if (map != null) {
                        record.setHeaders(map);
                    }
                    return storagePool.mapToNodeInfo(bucketVnode).flatMap(nodeList -> ECUtils.putSynchronizedRecord(storagePool, record.rocksKey(), Json.encode(record), nodeList, WRITE_ASYNC_RECORD)
                            .zipWith(Mono.just(record)));
                });
    }


}
