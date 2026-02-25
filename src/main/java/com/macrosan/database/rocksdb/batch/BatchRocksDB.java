package com.macrosan.database.rocksdb.batch;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MsRocksMonitor;
import com.macrosan.ec.rebuild.DiskStatusChecker;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.fs.BlockDevice;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.rsocket.MsPublishOn;
import com.macrosan.storage.PoolHealth;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatchWithIndex;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.constants.SysConstants.REDIS_LUNINFO_INDEX;
import static com.macrosan.database.rocksdb.MSRocksDB.READ_OPTIONS;
import static com.macrosan.database.rocksdb.MSRocksDB.WRITE_OPTIONS;
import static com.macrosan.ec.server.ErasureServer.CUR_NODE;
import static com.macrosan.ec.server.ErasureServer.DISK_EXECUTOR;

/**
 * @author Administrator
 */
@Log4j2
public class BatchRocksDB {
    enum Type {
        /**
         * 直接写入db
         */
        PUT_ONLY,
        /**
         * 从db中取出对应的key，并和待写入的value比较versionNum。比较后写入db
         */
        COMPARE_AND_PUT,
        /**
         * 删除一些key的同时写入一个新的key（将一些key value合并成一个新的key value）
         */
        DELETE_AND_PUT,
        MERGE,
        CUSTOMIZE_OPERATE;
    }

    private final static ThreadFactory ROCKS_THREAD_FACTORY = new MsThreadFactory("moss-rocks");

    public interface RequestConsumer {
        /**
         * 处理对应Type的BatchRequest的方法
         *
         * @param db      原始的rocks db
         * @param w       write batch
         * @param request BatchRequest
         * @throws RocksDBException db异常
         */
        void accept(RocksDB db, WriteBatch w, BatchRequest request) throws RocksDBException;
    }

    static Map<Type, RequestConsumer> functionMap = new HashMap<>();

    private static MsExecutor executor = new MsExecutor(1, 1, ROCKS_THREAD_FACTORY);
    private static Scheduler scheduler = Schedulers.fromExecutor(executor);

    public static Set<String> logPrintMap = new ConcurrentSkipListSet<>();
    public static Set<String> lunRollBackPrint = new ConcurrentSkipListSet<>();
    public static Set<String> logPrintRecord = new ConcurrentSkipListSet<>();

    static {
        /**
         * 注册对应Type的具体处理方法
         */
        functionMap.put(Type.PUT_ONLY, (db, writeBatch, request) -> {
            for (int i = 0; i < request.key.length; i++) {
                writeBatch.put(request.key[i], request.value[i]);
            }
        });

        functionMap.put(Type.COMPARE_AND_PUT, (db, writeBatch, request) -> {
            for (int i = 0; i < request.key.length; i++) {
                byte[] oldValue = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, request.key[i]);
                if (null == oldValue) {
                    writeBatch.put(request.key[i], request.value[i]);
                } else {
                    JsonObject old = new JsonObject(new String(oldValue));
                    JsonObject cur = new JsonObject(new String(request.value[i]));
                    if (old.getString("versionNum").compareTo(cur.getString("versionNum")) < 0) {
                        writeBatch.put(request.key[i], request.value[i]);
                    }
                }
            }
        });

        functionMap.put(Type.DELETE_AND_PUT, (db, writeBatch, request) -> {
            for (int i = 0; i < request.key.length; i++) {
                writeBatch.delete(request.key[i]);
            }

            writeBatch.put(request.tuple.var1, request.tuple.var2);
        });

        functionMap.put(Type.MERGE, (db, writeBatch, request) -> {
            writeBatch.merge(request.tuple.var1, request.tuple.var2);
        });
        scheduler.schedule(BatchRocksDB::regularClean, 0, TimeUnit.SECONDS);
    }

    private static Map<String, BatchRocksDB> map = new HashMap<>();
    private static MsExecutor metaExecutor =
            new MsExecutor(PROC_NUM, 1, ROCKS_THREAD_FACTORY);

    private static MsExecutor dataExecutor =
            new MsExecutor(PROC_NUM, 1, ROCKS_THREAD_FACTORY);

    private static Scheduler metaScheduler = Schedulers.fromExecutor(metaExecutor);
    private static Scheduler dataScheduler = Schedulers.fromExecutor(dataExecutor);

    public static Set<String> errorLun = new ConcurrentSkipListSet<>();
    private static Map<String, Throwable> lunErrorMap = new HashMap<>();

    private static void regularClean() {
        logPrintMap.removeIf(lun -> !PoolHealth.repairLun.contains(CUR_NODE + "@" + lun));
        lunRollBackPrint.clear();
        logPrintRecord.clear();
        ErasureServer.nonLunMap.clear();
        scheduler.schedule(BatchRocksDB::regularClean, 30, TimeUnit.SECONDS);
    }

    private static UnicastProcessor<BatchRequest> newProcessor(boolean metaPool, int i, RocksDB db, String lun) {
        UnicastProcessor<BatchRequest> processor = UnicastProcessor.create(Queues.<BatchRequest>unboundedMultiproducer().get());
        Scheduler scheduler = metaPool ? metaScheduler : dataScheduler;
        WriteBatch writeBatch = new WriteBatch(db, lun, scheduler);
        processor.publishOn(scheduler).subscribe(r -> {
            LinkedList<BatchRequest> list = new LinkedList<>();

            try {
                list.add(r);

                if (r.business) {
                    MsRocksMonitor.enforceWriteAllowed(lun);
                }

                BatchRequest request = processor.poll();
                while (null != request) {
                    list.add(request);

                    if (list.size() > 1000) {
                        break;
                    }

                    request = processor.poll();
                }

                if (errorLun.contains(lun)) {
                    throw new MsException(ErrorNo.UNKNOWN_ERROR, "rocks db in " + lun + " is error");
                }


                LinkedList<BatchRequest> low = new LinkedList<>();

                for (BatchRequest request0 : list) {
                    if (request0.responseEnd.get()) {
                        continue;
                    }

                    if (request0.lowPriority) {
                        low.add(request0);
                        continue;
                    }

                    RequestConsumer consumer = functionMap.get(request0.type);
                    if (null == consumer) {
                        if (null == request0.consumer) {
                            throw new UnsupportedOperationException("");
                        } else {
                            request0.consumer.accept(db, writeBatch, request0);
                        }
                    } else {
                        consumer.accept(db, writeBatch, request0);
                    }
                }

                for (BatchRequest request0 : low) {
                    RequestConsumer consumer = functionMap.get(request0.type);
                    if (null == consumer) {
                        if (null == request0.consumer) {
                            throw new UnsupportedOperationException("");
                        } else {
                            request0.consumer.accept(db, writeBatch, request0);
                        }
                    } else {
                        consumer.accept(db, writeBatch, request0);
                    }
                }

                //暂时用同步方法
                boolean b = writeBatch.write(WRITE_OPTIONS).block();
                if (b) {
                    for (BatchRequest request0 : list) {
                        request0.res.onNext(b);
                    }
                }
            } catch (Exception e) {
                exceptionHandler(lun, list, e);
            } finally {
                writeBatch.w = new WriteBatchWithIndex(true);
            }
        });

        return processor;
    }

    private static void exceptionHandler(String lun, List<BatchRequest> list, Throwable e) {
        if (DiskStatusChecker.isRebuildWaiter(lun)){
            for (BatchRequest request0 : list) {
                request0.res.onError(e);
            }
            return;
        }
        //Lock TimeOut 异常不放入error lun
        if (e instanceof RocksDBException || e.getCause() instanceof RocksDBException) {
            if (!e.getMessage().contains("LockTimeout")
                    && !e.getMessage().contains("Max allowed space was reached")
                    && !e.getMessage().contains("No space left on device")) {
                errorLun.add(lun);
                lunErrorMap.put(lun, e);
            }
        }
        if (e instanceof MsException) {
            if (e.getMessage().contains("no such lun")){
                errorLun.add(lun);
                lunErrorMap.put(lun, e);
            }
        }

        if (lunErrorMap.get(lun) != null) {
            if (!logPrintMap.contains(lun)) {
                log.error("flush {} rocks fail. {}", lun, lunErrorMap.get(lun).getMessage());
            }
        } else {
            if (!logPrintRecord.contains(lun) && e.getMessage() != null
                    && e.getMessage().contains("Write blocked due to low disk space")) {
                log.error("flush {} rocks fail", lun, e);
                logPrintRecord.add(lun);
            }
        }

        for (BatchRequest request0 : list) {
            request0.res.onError(e);
        }
    }

    static int processLen = 4;
    private boolean metaPool;
    UnicastProcessor<BatchRequest>[] processors;

    private static final BatchRocksDB DEFAULT_INSTANCE = new BatchRocksDB();

    private BatchRocksDB() {
        processors = new UnicastProcessor[processLen];
        for (int i = 0; i < processLen; i++) {
            processors[i] = UnicastProcessor.create(Queues.<BatchRequest>unboundedMultiproducer().get());
            processors[i].publishOn(metaScheduler).subscribe(r -> {
                r.res.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "no such rocksdb"));
            });
        }
    }

    private BatchRocksDB(boolean metaPool, boolean isNvme, RocksDB db, String lun) {
        this.metaPool = metaPool;
        if (metaPool) {
            processors = new UnicastProcessor[isNvme ? processLen * 2 : processLen];
            for (int i = 0; i < processors.length; i++) {
                processors[i] = newProcessor(true, i, db, lun);
            }
        } else {
            processors = new UnicastProcessor[]{newProcessor(false, 0, db, lun)};
        }

    }

    private void onNext(BatchRequest request) {
        processors[0].onNext(request);
    }

    private void onNext(int hashcode, BatchRequest request) {
        int index = Math.abs(hashcode % processors.length);
        processors[index].onNext(request);
    }

    public static void addBatch(String lun, RocksDB db) {
        String disk = ServerConfig.getInstance().getHostUuid() + '@' + lun;
        String media = RedisConnPool.getInstance().getCommand(REDIS_LUNINFO_INDEX).hget(disk, "media");
        String protocol = RedisConnPool.getInstance().getCommand(REDIS_LUNINFO_INDEX).hget(disk, "protocol");
        boolean isMetaPool = "SSD".equalsIgnoreCase(media);
        boolean isNvme = "NVME".equalsIgnoreCase(protocol);
        if (media == null) {
            isMetaPool = lun.contains("index");
        }
        map.put(lun, new BatchRocksDB(isMetaPool, isNvme, db, lun));
    }

    public static void addRecordBatch(String path, RocksDB db) {
        map.put(path, new BatchRocksDB(true, false, db, path));
    }

    /**
     * 自定义consumer的rocksDB批量操作。注意只能在操作meta pool相关的rocksDB时使用。
     *
     * @param lun      lun名称
     * @param hashCode 决定使用哪一个processor，一般使用key做哈希
     * @param consumer 自定义批量操作consumer
     * @param res      最后要输出的流
     * @param <T>      res中的数据类型
     */
    public static <T> Mono<T> customizeOperate(String lun, int hashCode, BatchRocksDB.RequestConsumer consumer, MonoProcessor<T> res) {
        return customizeOperateMeta(lun, hashCode, consumer).flatMap(b -> res);
    }

    public static Mono<Boolean> customizeOperateMeta(String lun, int hashCode, BatchRocksDB.RequestConsumer consumer,
                                                     ScheduledExecutorService executor) {
        BatchRequest request = new BatchRequest();
        request.type = Type.CUSTOMIZE_OPERATE;
        request.consumer = consumer;

        map.getOrDefault(lun, DEFAULT_INSTANCE).onNext(hashCode, request);
        return new MsPublishOn(request.res, executor).doOnError(e -> {
            // 加入重构队列的盘不在打印日志
            if (DiskStatusChecker.isRebuildWaiter( lun)){
                return;
            }
            if (e instanceof MsException) {//放入errorLun后的盘错误信息不会再大量重复打印
                if (errorLun.contains(lun)) {
                    if (!logPrintMap.contains(lun)) {
                        log.error("", e);
                        logPrintMap.add(lun);
                    }
                } else {
                    if (!logPrintRecord.contains(lun)) {
                        log.error("", e);
                    }
                }
            } else {
                log.error("", e);
            }
        }).doFinally(s -> {
            request.responseEnd.set(true);
        });
    }

    /**
     * 自定义consumer的rocksDB批量操作。注意只能在操作meta pool相关的rocksDB时使用。
     *
     * @param lun      lun名称
     * @param hashCode 决定使用哪一个processor，一般使用key做哈希
     * @param consumer 自定义批量操作consumer
     */
    public static Mono<Boolean> customizeOperateMeta(String lun, int hashCode, BatchRocksDB.RequestConsumer consumer) {
        return customizeOperateMeta(lun, hashCode, consumer, DISK_EXECUTOR);
    }

    public static <T> Mono<T> customizeOperateMetaLowPriority(String lun, int hashCode, BatchRocksDB.RequestConsumer consumer, MonoProcessor<T> res) {
        return customizeOperateMetaLowPriority(lun, hashCode, consumer).flatMap(b -> res);
    }

    public static Mono<Boolean> customizeOperateMetaLowPriority(String lun, int hashCode, BatchRocksDB.RequestConsumer consumer) {
        BatchRequest request = new BatchRequest();
        request.lowPriority = true;
        request.type = Type.CUSTOMIZE_OPERATE;
        request.consumer = consumer;

        map.getOrDefault(lun, DEFAULT_INSTANCE).onNext(hashCode, request);
        return request.res.publishOn(ErasureServer.DISK_SCHEDULER).doFinally(s -> {
            request.responseEnd.set(true);
        });
    }

    /**
     * 自定义consumer的rocksDB批量操作，返回默认的boolean流。注意只能在操作Data pool相关的rocksDB时使用。
     *
     * @param lun      lun名称
     * @param consumer 自定义批量操作consumer
     * @param res      最后要输出的流
     * @param <T>      res中的数据类型
     */
    public static <T> Mono<T> customizeOperateData(String lun, BatchRocksDB.RequestConsumer consumer, MonoProcessor<T> res) {
        return customizeOperateData(lun, consumer).flatMap(b -> res);
    }


    public static Mono<Boolean> customizeOperateData(String lun, BatchRocksDB.RequestConsumer consumer) {
        return customizeOperateData(lun, 0, consumer);
    }

    /**
     * 自定义consumer的rocksDB批量操作，返回默认的boolean流。注意只能在操作Data pool相关的rocksDB时使用。
     *
     * @param lun      lun名称
     * @param consumer 自定义批量操作consumer
     */
    public static Mono<Boolean> customizeOperateData(String lun, int hash, BatchRocksDB.RequestConsumer consumer) {
        BatchRequest request = new BatchRequest();
        request.type = Type.CUSTOMIZE_OPERATE;
        request.consumer = consumer;

        map.getOrDefault(lun, DEFAULT_INSTANCE).onNext(hash, request);
        return request.res.publishOn(ErasureServer.DISK_SCHEDULER).doOnError(e -> {
            // 加入重构队列的盘不在打印日志
            if (DiskStatusChecker.isRebuildWaiter(lun)){
                return;
            }
            if (e instanceof MsException) {//放入errorLun后的盘错误信息不会再大量重复打印
                if (errorLun.contains(lun)) {
                    if (!logPrintMap.contains(lun)) {
                        log.error("", e);
                        logPrintMap.add(lun);
                    }
                } else {
                    if (!logPrintRecord.contains(lun)) {
                        log.error("", e);
                    }
                }
            } else {
                log.error("", e);
            }
        }).doFinally(s -> {
            request.responseEnd.set(true);
        });
    }

    public static Mono<Boolean> merge(String lun, byte[] key, long value) {
        BatchRequest request = new BatchRequest();
        request.type = Type.MERGE;
        request.tuple = new Tuple2<>(key, toByte(value));

        map.getOrDefault(lun, DEFAULT_INSTANCE).onNext(request);
        return request.res.publishOn(ErasureServer.DISK_SCHEDULER).doFinally(s -> {
            request.responseEnd.set(true);
        });
    }

    public static byte[] toByte(long c) {
        byte[] res = new byte[8];

        for (int i = 0; i < 8; i++) {
            res[i] = (byte) (c & 0xff);
            c >>= 8;
        }

        return res;
    }

    public static void remove(String lun) {
        map.remove(lun);
    }

    public static Mono<Boolean> customizeOperateDataForInitBlockSpace(String lun, BatchRocksDB.RequestConsumer consumer) {
        BatchRequest request = new BatchRequest();
        request.type = Type.CUSTOMIZE_OPERATE;
        request.consumer = consumer;

        map.getOrDefault(lun, DEFAULT_INSTANCE).onNext(request);
        return request.res.publishOn(BlockDevice.INIT_BLOCK_SCHEDULER).doFinally(s -> {
            request.responseEnd.set(true);
        });
    }

    public static Mono<Boolean> customizeOperateMetaForDelete(String lun, int hashCode, BatchRocksDB.RequestConsumer consumer) {
        BatchRequest request = new BatchRequest();
        request.type = Type.CUSTOMIZE_OPERATE;
        request.consumer = consumer;
        request.business = false;

        map.getOrDefault(lun, DEFAULT_INSTANCE).onNext(hashCode, request);
        return request.res.publishOn(ErasureServer.DISK_SCHEDULER).doOnError(e -> {
            // 加入重构队列的盘不在打印日志
            if (DiskStatusChecker.isRebuildWaiter(lun)){
                return;
            }
            if (e instanceof MsException) {//放入errorLun后的盘错误信息不会再大量重复打印
                if (errorLun.contains(lun)) {
                    if (!logPrintMap.contains(lun)) {
                        log.error("", e);
                        logPrintMap.add(lun);
                    }
                } else {
                    if (!logPrintRecord.contains(lun)) {
                        log.error("", e);
                    }
                }
            } else {
                log.error("", e);
            }
        }).doFinally(s -> {
            request.responseEnd.set(true);
        });
    }
}
