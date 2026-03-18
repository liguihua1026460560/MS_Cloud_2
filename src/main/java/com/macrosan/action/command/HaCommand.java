package com.macrosan.action.command;

import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.error.overwrite.OverWriteHandler;
import com.macrosan.fs.AioChannel;
import com.macrosan.httpserver.OppositeVerticle;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.localtransport.Command;
import com.macrosan.localtransport.LunManager;
import com.macrosan.message.jsonmsg.CliResponse;
import com.macrosan.rabbitmq.ObjectConsumer;
import com.macrosan.rsocket.server.Rsocket;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.metaserver.ShardingScheduler;
import com.macrosan.storage.metaserver.ShardingWorker;
import com.macrosan.storage.strategy.StorageStrategy;
import com.macrosan.utils.msutils.ChangeLogLevel;
import com.macrosan.utils.ModuleDebug;
import com.sun.management.HotSpotDiagnosticMXBean;
import io.netty.util.internal.PlatformDependent;
import io.vertx.core.DeploymentOptions;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.CompactionOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.SstFileMetaData;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static com.macrosan.action.managestream.BucketService.startOrStopNfs;
import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.filesystem.utils.CheckUtils.bucketFsCheck;
import static com.macrosan.storage.metaserver.ShardingScheduler.DISABLED_BUCKET_HASH;
import static com.macrosan.storage.strategy.StorageStrategy.BUCKET_STRATEGY_NAME_MAP;
import static com.macrosan.storage.strategy.StorageStrategy.POOL_STRATEGY_MAP;

/**
 * HaCommand
 * <p>
 * LUN的导入导出，http服务的开启和关闭等命令
 *
 * @author liyixin
 * @date 2019/10/17
 */
@Log4j2
public class HaCommand extends Reusable {

    private static HaCommand instance = new HaCommand();

    public static HaCommand getInstance() {
        return instance;
    }

    @Command()
    public CliResponse importLun(String... luns) {
        log.info("import lun : " + Arrays.toString(luns));
        for (String lun : luns) {
            LunManager.setLunAbility(lun, true);
        }
        return response();
    }

    @Command()
    public CliResponse exportLun(String... luns) throws InterruptedException {
        log.info("export lun : " + Arrays.toString(luns));
        CountDownLatch latch = new CountDownLatch(luns.length);
        Runnable r = () -> {
            log.info("counting down : {}.", latch.getCount());
            latch.countDown();
        };

        for (String lun : luns) {
            LunManager.LunInfo lunInfo = LunManager.getLunInfo(lun);
            lunInfo.addListener(r);
            lunInfo.setAvailable(false);
        }

        latch.await();
        log.info("waiting finish.");
        return response();
    }

    @Command()
    public CliResponse startHttp(String... ips) {
        try {
            log.info("start http server");
            OppositeVerticle.init(ips);
            ServerConfig.getInstance().getVertx()
                    .deployVerticle(OppositeVerticle.class.getName(), new DeploymentOptions().setInstances(PROC_NUM));
        } catch (Exception e) {
            log.error(e);
        }
        return response();
    }

    @Command()
    public CliResponse stopHttp() {
        log.info("stop http server");
        OppositeVerticle.unDeploy();
        return response();
    }

    @Command
    public CliResponse startBackEnd(String ip) {
        log.info("start back end server");
        Rsocket.init(ip, true);
        return response();
    }

    @Command
    public CliResponse stopBackEnd() {
        log.info("stop back end server");
        Rsocket.stop();
        return response();
    }

    @Command
    public CliResponse changeLogLevel(String[] level) {
        log.info("change log level to " + level);
        ChangeLogLevel.changeLogLevelM(level);
        return response();
    }

    @Command
    public CliResponse dumpHeap(String[] args) {
        String path = "/cg_sp0/MS_Cloud.hprof";
        if (args != null && args.length > 0) {
            path = args[0];
        }


        log.info("start dumpHeap to " + path);
        CliResponse response = new CliResponse();

        try {
            HotSpotDiagnosticMXBean bean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
            bean.dumpHeap(path, true);
            log.info("end dumpHeap to " + path);
            response.setStatus("dumpHeap to " + path);
        } catch (Throwable t) {
            log.error("dumpHeap fail.", t);
            response.setStatus("dumpHeap fail");
        }

        return response;
    }

    @Command
    public CliResponse jstack(String[] args) {
        CliResponse response = new CliResponse();

        String path = "/cg_sp0/jstack";
        if (args != null && args.length > 0) {
            path = args[0];
        }

        try {
            ThreadInfo[] dump = ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
                for (ThreadInfo info : dump) {
                    writer.write(info.toString());
                    writer.newLine();
                }
                writer.flush();
            }

            response.setStatus("jstack dump to " + path);
        } catch (Throwable e) {
            log.error("jstack fail.", e);
            response.setStatus("jstack fail." + path);
        }

        return response;
    }

    @Command
    public CliResponse nettyUsedDirectMemory(String[] args) {
        CliResponse response = new CliResponse();
        long used = PlatformDependent.usedDirectMemory();
        log.info("nettyUsedDirectMemory:{}", used);
        response.setStatus("nettyUsedDirectMemory:" + used);
        return response;
    }

    @Command
    public CliResponse compactAllRocksDB(String[] args) {
        CliResponse response = new CliResponse();
        response.setStatus("");
        if (args == null || args.length < 1) {
            log.info("error compactAllRocksDB arg");
            return response;
        }

        String lun = args[0];
        int level = 1;
        if (args.length >= 2) {
            level = Integer.parseInt(args[1]);
        }

        log.info("start compactAllRocksDB:{} to {}", lun, level);

        MSRocksDB rocksDB = MSRocksDB.getRocksDB(lun);
        if (rocksDB == null) {
            log.info("compactAllRocksDB no db:{}", lun);
            return response;
        }

        RocksDB db = rocksDB.getRocksDB();
        try {
            try (FlushOptions flushOptions = new FlushOptions()
                    .setWaitForFlush(true)
                    .setAllowWriteStall(true)) {
                db.flush(flushOptions);
            }

            byte[] defaultName = db.getDefaultColumnFamily().getName();

            List<String> totalFiles = db.getLiveFilesMetaData().stream()
                    .filter(m -> Arrays.equals(defaultName, m.columnFamilyName()))
                    .map(SstFileMetaData::fileName).collect(Collectors.toList());
            try (CompactionOptions compactionOptions = new CompactionOptions()
                    .setOutputFileSizeLimit(256L << 20)) {
                db.compactFiles(compactionOptions, totalFiles, level, -1, null);
            }

            log.info("complete compactAllRocksDB:{}", lun);
        } catch (Exception e) {
            log.error("", e);
        }

        return response;
    }

    @Command
    public CliResponse startAsyncOverWrite(String[] params) {
        log.info("start async over write:{}", params != null ? Arrays.toString(params) : null);
        if (params == null) {
            return response();
        }
        if ("Enabled".equals(params[0])) {
            RedisConnPool.getInstance().getShortMasterCommand(SysConstants.REDIS_SYSINFO_INDEX).hset("overwrite_config", "async_over_write", "1");
            OverWriteHandler.ASYNC_OVER_WRITE = true;
        } else if ("Suspended".equals(params[0])) {
            RedisConnPool.getInstance().getShortMasterCommand(SysConstants.REDIS_SYSINFO_INDEX).hset("overwrite_config", "async_over_write", "0");
            OverWriteHandler.ASYNC_OVER_WRITE = false;
        } else {
            log.info("invalid params!");
        }
        return response();
    }

    @Command
    public CliResponse startShardingScheduler(String[] params) {
        log.info("start sharding scheduler:{}", params != null ? Arrays.toString(params) : null);
        if (params == null) {
            return response();
        }
        if ("Enabled".equals(params[0])) {
            RedisConnPool.getInstance().getShortMasterCommand(SysConstants.REDIS_SYSINFO_INDEX).set("bucket_hash_switch", "1");
            ShardingScheduler.BUCKET_HASH_SWITCH = true;
            DISABLED_BUCKET_HASH.set(false);
            ShardingScheduler.start();
        } else if ("Suspended".equals(params[0])) {
            RedisConnPool.getInstance().getShortMasterCommand(SysConstants.REDIS_SYSINFO_INDEX).set("bucket_hash_switch", "0");
            ShardingScheduler.BUCKET_HASH_SWITCH = false;
        } else {
            log.info("invalid params!");
        }
        return response();
    }


    @Command
    public CliResponse dealFs(String[] params) {
        try {
            String bucketName = params[0];
            // 若环境开启小文件聚合，则不允许操作
            String strategy = BUCKET_STRATEGY_NAME_MAP.get(bucketName);
            StorageStrategy storageStrategy = null;
            if (strategy != null) {
                storageStrategy = POOL_STRATEGY_MAP.get(strategy);
            }
            boolean smallFileAggregation = storageStrategy != null && storageStrategy.redisMap.containsKey("small_file_aggregation");
            // 若环境开启桶散列，则不允许操作
            if (ShardingScheduler.BUCKET_HASH_SWITCH
                    || StoragePoolFactory.getMetaStoragePool(bucketName).getBucketVnodeList(bucketName).size() > 1
                    || ShardingWorker.contains(bucketName) || ShardingWorker.bucketHasShardingTask(bucketName)
                    || smallFileAggregation) {
                log.error("bucketName enabled range sharding.");
                return response();
            }

            boolean startNfs = params.length > 1 && "1".equals(params[1]);
            boolean startCifs = params.length > 2 && "1".equals(params[2]);
            log.info("【startNfs:{}】, 【startCifs:{}】", startNfs, startCifs);
            if (!startNfs && !startCifs) {
                log.error("startNfs or startCifs must be 1.");
                return response();
            }

            // 检测之前是否已经挂载了文件系统
            if (bucketFsCheck(bucketName)) {
                log.info("The bucket 【{}】 has already mounted the file system", bucketName);
                if (startNfs) {
                    RedisConnPool.getInstance().getShortMasterCommand(SysConstants.REDIS_BUCKETINFO_INDEX).hset(bucketName, "nfs", "1");
                    log.info("start nfs for bucket:{}", bucketName);
                }
                if (startCifs) {
                    RedisConnPool.getInstance().getShortMasterCommand(SysConstants.REDIS_BUCKETINFO_INDEX).hset(bucketName, "cifs", "1");
                    log.info("start cifs for bucket:{}", bucketName);
                }
                return response();
            }

            startOrStopNfs(bucketName, startNfs, startCifs, false);
            log.info("deal nfs success. {} {} {}", bucketName, startNfs, startCifs);
        } catch (Exception e) {
            log.error("deal nfs error. ", e);
        }
        return response();
    }

    @Command
    public CliResponse action(String[] args) {
        log.info("command action: " + Arrays.toString(args));
        try {
            String action = args[0];
            switch (action) {
                case "getMsgRunning":
                    log.info("msgRunning: {}", ObjectConsumer.getInstance().msgRunning);
                    break;
                case "crash":
                    AioChannel.free0(1);
                    break;
                case "moduleDebug":
                    log.info("moduleDebug: {}", Arrays.toString(args));
                    if (args.length < 3) {
                        log.info("Usage: action moduleDebug <module> <on|off>");
                        log.info("Available modules: {}", ModuleDebug.getAll().keySet());
                        break;
                    }
                    String module = args[1];
                    String value = args[2];
                    if (!ModuleDebug.getAll().containsKey(module)) {
                        log.info("Invalid module: {}", module);
                        log.info("Available modules: {}", ModuleDebug.getAll().keySet());
                        break;
                    }
                    if (!"on".equalsIgnoreCase(value) && !"off".equalsIgnoreCase(value)) {
                        log.info("Invalid value: {}", value);
                        log.info("Usage: moduleDebug <module> <on|off>");
                        break;
                    }
                    boolean enabled = "on".equalsIgnoreCase(value);
                    ModuleDebug.setEnabled(module, enabled);
                    log.info("Set module {} debug to {}", module, enabled);
                    break;
                default:
                    log.info("no such action {}", action);
            }
        } catch (Throwable e) {
            log.error("", e);
        }

        return response();
    }

}
