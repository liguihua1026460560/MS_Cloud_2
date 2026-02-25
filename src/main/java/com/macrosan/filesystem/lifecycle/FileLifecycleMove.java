package com.macrosan.filesystem.lifecycle;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.ec.rebuild.RemovedDisk;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.ec.server.RequestResponseServerHandler;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.utils.functional.Tuple3;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.storage.StorageOperate.DATA;
import static com.macrosan.storage.strategy.StorageStrategy.POOL_STRATEGY_MAP;

@Log4j2
public class FileLifecycleMove implements Runnable {
    protected static RedisConnPool pool = RedisConnPool.getInstance();
    static Queue<ScanTask> scanTasks = new ConcurrentLinkedQueue<>();

    protected MSRocksDB mqDB = MSRocksDB.getRocksDB(com.macrosan.ec.Utils.getMqRocksKey());
    public static final String MQ_LIFECYCLE_KEY_PREFIX = "filelifecycle_";
    private static int RUN_NUM = 10000;
    public static int TRY_NUM = 10;
    private String node;

    public static FileLifecycleMove lifecycleMove = new FileLifecycleMove();

    public FileLifecycleMove() {
        node = ServerConfig.getInstance().getHostUuid();
    }

    @Override
    public void run() {

        long curStamp = System.currentTimeMillis();
        if (curStamp < FileLifecycle.startStamp || curStamp >= FileLifecycle.endStamp) {
            return;
        }

        if (FileLifecycle.fileLifecycleSwitch) {
            Flux.fromStream(scanTasks.stream())
                    .flatMap(task -> {
                        int n = scan(RUN_NUM, task);
                        if (n == 0) {
                            // 标记空转次数，超过 TRY_NUM 的扫描任务会从scanTasks中删除
                            task.scanEnd++;
                        } else {
                            task.scanEnd = 0;
                            log.info("file lifecycle scan {} -> {} {}", task.sourceStrategy, task.targetStrategy, n);
                        }

                        TaskRunner taskRunner = new TaskRunner(mqDB);
                        int runNum = 100;
                        for (int i = 0; i < runNum; i++) {
                            taskRunner.run();
                        }

                        int finalN = n;
                        return taskRunner.res.delayElement(Duration.ofSeconds(10)).flatMap(b -> {
                                    if (finalN > 0) {
                                        log.info("file lifecycle start clear {} -> {} {}", task.sourceStrategy, task.targetStrategy, finalN);
                                    }
                                    ClearTaskRunner clearTaskRunner = new ClearTaskRunner(mqDB);
                                    for (int i = 0; i < runNum; i++) {
                                        clearTaskRunner.run();
                                    }
                                    return clearTaskRunner.res;
                                })
                                .defaultIfEmpty(false)
                                .onErrorReturn(false)
                                .doFinally(s -> {
                                    if (finalN > 0) {
                                        log.info("file lifecycle move end {} -> {} {}", task.sourceStrategy, task.targetStrategy, finalN);
                                    }
                                });
                    }).doFinally(s -> {
                        // 清理扫描任务
                        Iterator<ScanTask> iterator = scanTasks.iterator();
                        while (iterator.hasNext()) {
                            ScanTask next = iterator.next();
                            if (next.scanEnd == TRY_NUM + 1) {
                                iterator.remove();
                            }
                        }
                        ErasureServer.DISK_SCHEDULER.schedule(lifecycleMove, 10, TimeUnit.SECONDS);
                    }).subscribe();
        } else {
            ErasureServer.DISK_SCHEDULER.schedule(lifecycleMove, 10, TimeUnit.SECONDS);
        }
    }

    // 扫描当前桶的所有存储策略下的存储池
    public int scan(int scanNum, ScanTask task) {
        if (scanNum == 0) {
            return 0;
        }
        log.info("scan task : {}", task);
        int n = 0;

        StoragePool[] storagePools = POOL_STRATEGY_MAP.get(task.sourceStrategy).getStoragePools(DATA);
        StoragePool targetPool = POOL_STRATEGY_MAP.get(task.sourceStrategy).getStoragePool(DATA);

        for (StoragePool sourcePool : storagePools) {
            List<String> disks = getCurNodePoolDisk(sourcePool);
            for (String disk : disks) {
                n += scanDataDisk(task.bucket, task.timestamps, task.beginTimestamps, sourcePool, disk, targetPool, scanNum - n);
                if (n >= scanNum) {
                    break;
                }
            }
        }
        return n;
    }

    private int scanDataDisk(String bucket, long moveTimestamps, long beginTimestamps, StoragePool pool, String disk, StoragePool targetPool, int scanNum) {
//        log.info("disk: {} scanNum: {}", disk, scanNum);
        int n = 0;
        MSRocksDB dataDB = MSRocksDB.getRocksDB(disk);
        if (dataDB == null) {
            return 0;
        }
        // 避免空盘扫描
        if (diskIsEmpty(dataDB)) {
            return n;
        }
        int vnodeNum = pool.getVnodeNum();
        for (int vnode = 0; vnode < vnodeNum; vnode++) {
            String vnodeId = pool.getVnodePrefix() + vnode;
            String s_uuid = RedisConnPool.getInstance().getCommand(REDIS_MAPINFO_INDEX).hget(vnodeId, "s_uuid");
            if (s_uuid.equals(node)) {
                String prefix = ROCKS_FILE_META_PREFIX + vnode + "_" + bucket + "_";
                n += scanDataDiskByVnode(moveTimestamps, beginTimestamps, pool, targetPool, disk, scanNum - n, prefix);
            }else {
                // 扫描节点down
                if (!nodeIsUp(node)) {
                    String link = RedisConnPool.getInstance().getCommand(REDIS_MAPINFO_INDEX).hget(vnodeId, "link");
                    String[] nodes = link.split(",");
                    // 找到第一个有效的节点
                    String validNode = "";
                    for (int i = 1; i < nodes.length; i++) {
                        String s_uuid0 = RedisConnPool.getInstance().getCommand(REDIS_MAPINFO_INDEX).hget(vnodeId, "s_uuid");
                        if (nodeIsUp(s_uuid0)) {
                            validNode = s_uuid0;
                            break;
                        }
                    }
                    if (StringUtils.isNotEmpty(validNode) && validNode.equals(node)) {
                        String prefix = ROCKS_FILE_META_PREFIX + vnode + "_" + bucket + "_";
                        n += scanDataDiskByVnode(moveTimestamps, beginTimestamps, pool, targetPool, disk, scanNum - n, prefix);
                    }
                }
            }
        }

        return n;
    }

    private static boolean diskIsEmpty(MSRocksDB dataDB) {
        try (MSRocksIterator iterator = dataDB.newIterator()) {
            iterator.seek(ROCKS_FILE_META_PREFIX.getBytes());
            if (!iterator.isValid() || !new String(iterator.key()).startsWith(ROCKS_FILE_META_PREFIX)) {
                return true;
            }
        } catch (Exception e) {
            log.error(e);
        }
        return false;
    }

    private int scanDataDiskByVnode(long moveTimestamps, long beginTimestamps, StoragePool sourcePool, StoragePool targetPool, String disk, int scanNum, String prefix) {
//        log.info("{}", prefix);
        int n = 0;
        MSRocksDB dataDB = MSRocksDB.getRocksDB(disk);
        if (dataDB == null) {
            return 0;
        }
        try (MSRocksIterator iterator = dataDB.newIterator()) {
            iterator.seek(prefix.getBytes());
            while (iterator.isValid() && n < scanNum) {
                String key = new String(iterator.key());
                if (!key.startsWith(prefix)) {
                    break;
                }
                String[] s = key.split("_");
                if (s.length < 3) {
                    iterator.next();
                    continue;
                }

                long timestamps = Long.parseLong(s[2]);
//                log.info("{}", key);
                if (timestamps > moveTimestamps || (beginTimestamps != -1 && timestamps <= beginTimestamps)) {
                    iterator.next();
                    continue;
                }

                FileMeta meta = Json.decodeValue(new String(iterator.value()), FileMeta.class);
                if (meta == null || meta.getMetaKey() == null) {
                    iterator.next();
                    continue;
                }

//                log.info("key : {}", key);
//                log.info("meta : {}", meta);

                byte[] taskKey = getTaskKey(sourcePool.getVnodePrefix(), meta.getFileName());
                if (mqDB.get(taskKey) != null) {
                    iterator.next();
                    continue;
                }

                List<Tuple3<String, String, String>> nodeList = sourcePool.mapToNodeInfo(sourcePool.getObjectVnodeId(meta.getFileName())).block();
                Set<String> fileDiskSet = nodeList.stream()
                        .filter(t -> t.var1.equalsIgnoreCase(CURRENT_IP))
                        .map(t -> t.var2)
                        .collect(Collectors.toSet());
                if (fileDiskSet.contains(disk)) {
                    String value = new JsonObject()
                            .put("fileOffset", meta.getFileOffset())
                            .put("fileSize", meta.getSize())
                            .put("targetPool", targetPool.getVnodePrefix())
                            .put("metaKey", meta.getMetaKey())
                            .encode();
                    mqDB.put(taskKey, value.getBytes());
                    n++;
                } else {
                    // 数据池加盘迁移时的处理
                    boolean isMigrating = false;
                    try {
                        List<String> runningKeys = RedisConnPool.getInstance().getShortMasterCommand(REDIS_MIGING_V_INDEX).keys("running_*");
                        for (String runningKey : runningKeys) {
                            String type = RedisConnPool.getInstance().getShortMasterCommand(REDIS_MIGING_V_INDEX).type(runningKey);
                            if ("hash".equalsIgnoreCase(type)) {
                                Map<String, String> map = RedisConnPool.getInstance().getShortMasterCommand(REDIS_MIGING_V_INDEX).hgetall(runningKey);
                                String operate = map.getOrDefault("operate", "add_node");
                                String diskName = map.getOrDefault("diskName", "data");
                                if (!map.isEmpty() && diskName.contains("data")) {
                                    isMigrating = "add_node".equals(operate) || "add_disk".equals(operate);
                                }
                            }
                            if (isMigrating) {
                                break;
                            }
                        }
                    } catch (Exception e) {
                        log.error(e);
                        isMigrating = true;
                    }
                    if (!isMigrating) {
                        //当前文件已经不在vnode的映射中
                        log.info("delete file {} in {}", meta.getFileName(), disk);
                        SocketReqMsg msg = new SocketReqMsg("", 0)
                                .put("fileName", Json.encode(new String[]{meta.getFileName()}))
                                .put("lun", disk);
                        RequestResponseServerHandler.deleteFile(DefaultPayload.create(Json.encode(msg)))
                                .subscribe();
                    }
                }

                iterator.next();
            }

        } catch (Exception e) {
            log.error(e);
        }
        return n;
    }

    public static byte[] getTaskKey(String poolPrefix, String key) {
        return (MQ_LIFECYCLE_KEY_PREFIX + poolPrefix + "_" + key).getBytes();
    }

    private boolean nodeIsUp(String node) {
        String serverState = pool.getCommand(REDIS_NODEINFO_INDEX).hget(node, NODE_SERVER_STATE);
        return serverState.equals("1");
    }

    public static List<String> getCurNodePoolDisk(StoragePool pool) {
        List<String> list = new LinkedList<>();
        String curNode = ServerConfig.getInstance().getHostUuid();
        for (String lun : pool.getCache().lunSet) {
            String node = lun.split("@")[0];
            if (curNode.equalsIgnoreCase(node)) {
                if (!RemovedDisk.getInstance().contains(lun)) {
                    list.add(lun.split("@")[1]);
                }
            }
        }

        return new LinkedList<>(Arrays.asList(list.toArray(new String[0])));
    }
}
