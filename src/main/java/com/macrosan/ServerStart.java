package com.macrosan;

import com.macrosan.action.controller.ManageStreamController;
import com.macrosan.clearmodel.ClearModelStarter;
import com.macrosan.component.ComponentStarter;
import com.macrosan.database.redis.BatchCommands;
import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.database.redis.Redis6380ConnPool;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.doubleActive.HeartBeatChecker;
import com.macrosan.doubleActive.arbitration.Arbitrator;
import com.macrosan.ec.DelDeleteMark;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.error.ErrorFunctionCache;
import com.macrosan.ec.part.PartUtils;
import com.macrosan.ec.rebuild.DiskStatusChecker;
import com.macrosan.ec.rebuild.ReBuildRunner;
import com.macrosan.ec.rebuild.RebuildDeadLetter;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.cifs.CIFS;
import com.macrosan.filesystem.ftp.FTP;
import com.macrosan.filesystem.lifecycle.FileLifecycle;
import com.macrosan.filesystem.nfs.NFS;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.quota.FSQuotaRealService;
import com.macrosan.filesystem.utils.IpWhitelistUtils;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.fs.BlockDevice;
import com.macrosan.httpserver.DateChecker;
import com.macrosan.httpserver.HealthChecker;
import com.macrosan.httpserver.RestfulVerticle;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.inventory.InventoryService;
import com.macrosan.lifecycle.LifecycleClearTask;
import com.macrosan.lifecycle.LifecycleService;
import com.macrosan.localtransport.TcpServer;
import com.macrosan.message.codec.ControlCodec;
import com.macrosan.message.codec.HandlerCodec;
import com.macrosan.message.mqmessage.RequestMsg;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.SocketSender;
import com.macrosan.rabbitmq.ClearMqQueue;
import com.macrosan.rabbitmq.ObjectConsumer;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.rsocket.server.Rsocket;
import com.macrosan.snapshot.BucketSnapshotStarter;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.compressor.CompressorFactory;
import com.macrosan.storage.crypto.CryptoFactory;
import com.macrosan.storage.metaserver.ShardingScheduler;
import com.macrosan.utils.asm.ASM;
import com.macrosan.utils.bucketLog.LogRecorder;
import com.macrosan.utils.cache.LambdaCache;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.iam.IamUtils;
import com.macrosan.utils.iam.STSTokenMove;
import com.macrosan.utils.msutils.Scheduler;
import com.macrosan.utils.notification.BucketNotification;
import com.macrosan.utils.perf.*;
import com.macrosan.utils.property.PropertyReader;
import com.macrosan.utils.quota.QuotaRecorder;
import com.macrosan.utils.quota.StatisticsRecorder;
import com.macrosan.utils.ratelimiter.LimitThreshold;
import com.macrosan.utils.ratelimiter.LimitTimeSet;
import com.macrosan.utils.regions.MultiRegionUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import com.macrosan.utils.store.StoreManagementServer;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.dynamic.RedisCommandFactory;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.reactivex.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.plugins.util.PluginManager;
import reactor.core.publisher.Hooks;

import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;

import static com.macrosan.constants.RedisConstants.REDIS_PASSWD;
import static com.macrosan.constants.RedisConstants.UDS_ADDRESS;
import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;

/**
 * ServerStart
 * 程序的入口，部署verticles
 *
 * @author liyixin
 * @date 2018/10/27
 */
public class ServerStart {
    static {
        PluginManager.addPackage("com.macrosan.utils.msutils.log4j");
    }

    private static final Logger logger = LogManager.getLogger(ServerStart.class);
    public static Map<String, RedisFuture<Map<String, String>>> futures = null;
    public static Map<String, Long> notPrintDrop = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        ASM.init();
        long startTime = System.nanoTime();
        RedisClient client = RedisClient.create();
        final CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(() -> {
            StatefulRedisConnection<String, String> connection = client.connect(RedisURI.Builder.socket(UDS_ADDRESS)
                    .withPassword(REDIS_PASSWD)
                    .withDatabase(REDIS_MAPINFO_INDEX)
                    .build());
            RedisCommandFactory factory = new RedisCommandFactory(connection, Collections.singletonList(new StringCodec()));
            BatchCommands commands = factory.getCommands(BatchCommands.class);
            ScanIterator<String> iterator = ScanIterator.scan(connection.sync(), new ScanArgs().match("*"));
            futures = new HashMap<>();
            while (iterator.hasNext()) {
                String key = iterator.next();
                futures.put(key, commands.hgetall(key));
            }
            commands.flush();
            client.shutdown();
            return "";
        });


        if (args.length != 2 && args.length != 4) {
            logger.error("The input args were wrong,example : java -jar MS_Cloud.jar ip1 ip2 " +
                    "or java -jar MS_Cloud.jar ip1 ip2 ip3 ip4");
            System.exit(-1);
        }

        //不再打印Operator called default onErrorDropped
        //不再抛出bubble错误。
        //见Operators:514行
        Hooks.onErrorDropped(c -> {
            if (c instanceof RejectedExecutionException) {
                return;
            }

            if (c != null) {
                String message = c.toString();
                if (message != null && c instanceof ClosedChannelException || message.contains("closed connection")) {
                    if (!notPrintDrop.containsKey(message)) {
                        notPrintDrop.compute(message, (k, v) -> {
                            if (null == v) {
                                logger.error("on error drop", c);
                                v = System.currentTimeMillis();
                            }

                            return v;
                        });
                    }

                    return;
                }
            }

            logger.error("on error drop", c);
        });

        if (args.length > 2) {
            logger.info("ip1 : {}, ip2 : {}, p3 : {}, ip4 : {}", args[0], args[1], args[2], args[3]);
        } else {
            logger.info("ip1 : {}, ip2 : {}", args[0], args[1]);
        }

        /* 获得vertx实例 */
        Vertx vertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(PROC_NUM)
                .setPreferNativeTransport(true));

        Vertx tcpVertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(1)
                .setPreferNativeTransport(true));
        ServerConfig.init(args, vertx);
        Scheduler.init();
        /* 初始化Socket客户端 */
        SocketSender.init(vertx);
        /* 初始化连接池 */
        RedisConnPool.init();
        PropertyReader reader = new PropertyReader(IAM_COFIG_PATH);
        Map<String, String> map = reader.getPropertyAsMap();
        String serverIp = map.getOrDefault(IAM_COFIG_SERVER_IP, DEFAULT_SERVER_IP);
        if (!DEFAULT_SERVER_IP.equalsIgnoreCase(serverIp)) {
            IamRedisConnPool.init();
            Redis6380ConnPool.init();
            IamUtils.buildActionType();
            AccountPerfLimiter.getInstance().init();
            BucketPerfLimiter.getInstance().init();
            DataSyncPerfLimiter.getInstance().init();
            StsPerfLimiter.getInstance().init();
            FSProcPerfLimiter.getInstance().init();
            BucketFSPerfLimiter.getInstance().init();
            AddressFSPerfLimiter.getInstance().init();
        }

        /* 初始化缓存 */
        LambdaCache.initCache();
        ErrorFunctionCache.init();

        /* 初始化Jaxb */
        JaxbUtils.initJaxb();

        /* 初始化状态检查 */
        HealthChecker.init(vertx.getDelegate());

        /* 注册事件总线的编解码器 */
        vertx.eventBus().getDelegate().registerDefaultCodec(RequestMsg.class, new HandlerCodec());
        vertx.eventBus().getDelegate().registerDefaultCodec(ResponseMsg.class, new ControlCodec());

        DeploymentOptions oneInstance = new DeploymentOptions().setInstances(1);
        DeploymentOptions multiInstance = new DeploymentOptions().setInstances(PROC_NUM);

        // 将IP_UUID_MAP初始化提至RabbitMqUtils类初始化时，避免重启时publishEcError出现节点null现象
        DISK_SCHEDULER.schedule(RabbitMqUtils::init);
        ACLUtils.init();
        IpWhitelistUtils.init();
        FSQuotaRealService.init();
        ErasureServer.init();
        DiskStatusChecker.startCheck();
        MSRocksDB.init();
        BlockDevice.init();
        NFSBucketInfo.init();

        /* 恢复QoS数值提前写入Redis */
        try {
            RedisConnPool pool = RedisConnPool.getInstance();
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).set("ec_rabbit", "5");
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).set("rebuild_rabbit", "2");
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).set("lifecycle_rabbit", "20");

            Map<String, String> map0 = new HashMap<>();
            map0.put("no_limit", "0");
            map0.put("limit", "262144");
            map0.put("limit_meta", "1");
            map0.put("adapt", "10");

            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hmset("QoSThreshold", map0);
        } catch (Exception e) {
            logger.error("", e);
        }

        CompressorFactory.init();
        CryptoFactory.init();
        try {
            completableFuture.get();
            StoragePoolFactory.initStoragePools();
        } catch (Exception e) {
            logger.error("", e);
        } finally {
            logger.debug("nodeMap:{}", futures);
            ServerStart.futures = null;
        }
        ShardingScheduler.start();
        StoreManagementServer.init();

        VersionUtil.initVersionNum();
        STSTokenMove.start();
        Rsocket.init();
        RSocketClient.init();
        // 调整NFS心跳初始化至RSocket初始化完成之后，避免发往重启节点的NFS 请求因RSocket未启不通而失败
        Node.init();
        StatisticsRecorder.init();
        QuotaRecorder.init();
        DISK_SCHEDULER.schedule(ReBuildRunner::getInstance);
        EsMetaTask.init();

        HeartBeatChecker.multiLiveCheck();
        ObjectConsumer.getInstance().init();
        RebuildDeadLetter.getInstance().init();
        ClearMqQueue.startClear();

        vertx.rxDeployVerticle(DateChecker.class.getName(), oneInstance)
                .flatMap(s -> vertx.rxDeployVerticle(ManageStreamController.class.getName(), oneInstance))
                .flatMap(s -> vertx.rxDeployVerticle(RestfulVerticle.class.getName(), multiInstance))
                .flatMap(s -> tcpVertx.rxDeployVerticle(TcpServer.class.getName(), oneInstance))
                .subscribe(s -> {
                    HealthChecker.getInstance().start();
                    // 多站点仲裁需要获取syncStamp。在端口监听启动且firstCheckMaster完成，方可视为前端包启动完成
                    Arbitrator.getInstance().firstCheckMaster();
                });

        InventoryService.start();
        new LifecycleService().start();
        FileLifecycle.register();
        LogRecorder.getInstance().start();
        BucketNotification.getInstance().start();
        DelDeleteMark.init();
        LimitTimeSet.init();
        LimitThreshold.init();
        LifecycleClearTask.init();
        ComponentStarter.start();
        NFS.start();
        CIFS.start();
//        DCERPC.start();
//        WITNESS.start();
        ClearModelStarter.start();
        PartUtils.init();
        // ftp server启动
        FTP.start();
        BucketSnapshotStarter.start();
        MultiRegionUtils.startDeleteExpireCacheScheduler();
        logger.info("initialize the cloud costs {}ms", (System.nanoTime() - startTime) / 1000_000);
    }
}
