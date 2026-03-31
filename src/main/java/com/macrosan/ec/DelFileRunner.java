package com.macrosan.ec;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDBException;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.concurrent.Queues;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;

@Log4j2
public class DelFileRunner {
    public static int HDD_LIMIT = 100;
    public static int SSD_LIMIT = 200;
    public static int NVME_LIMIT = 400;
    public static boolean DEBUG = false;

    public static void init() {
        String hdd = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget("delete_file_limit", "hdd");
        if (StringUtils.isNotBlank(hdd)) {
            HDD_LIMIT = Integer.parseInt(hdd);
        }

        String ssd = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget("delete_file_limit", "ssd");
        if (StringUtils.isNotBlank(ssd)) {
            SSD_LIMIT = Integer.parseInt(ssd);
        }

        String nvme = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget("delete_file_limit", "nvme");
        if (StringUtils.isNotBlank(nvme)) {
            NVME_LIMIT = Integer.parseInt(nvme);
        }

        DISK_SCHEDULER.schedule(DelFileRunner::scanTask, 0, TimeUnit.SECONDS);
    }

    public static void scanTask() {
        try {
            if (DEBUG) {
                log.info("start scanTask");
            }
            Flux<Boolean> flux = Flux.empty();
            for (StoragePool pool : StoragePoolFactory.getAllStoragePools()) {
                String storage = pool.getVnodePrefix();
                long limit;
                switch (pool.media) {
                    case 1:
                        limit = SSD_LIMIT;
                        break;
                    case 2:
                        limit = NVME_LIMIT;
                        break;
                    default:
                        limit = HDD_LIMIT;
                }

                limit = limit * pool.getCache().lunSet.size() / (pool.getK() + pool.getM());
                deleteLimit.put(storage, limit);

                AtomicBoolean end = new AtomicBoolean(false);
                Queue<String> queue = Queues.<String>unbounded().get();
                MonoProcessor<Boolean> storageProcessor = MonoProcessor.create();
                if (DEBUG) {
                    log.info("start scanTask {}", storage);
                    log.info("start dealTask {}", storage);
                }
                AtomicLong num = new AtomicLong(0);
                DISK_SCHEDULER.schedule(() -> scanTask(storage, PREIFX + storage + "#", end, queue));
                DISK_SCHEDULER.schedule(() -> dealTask(storage, end, queue, storageProcessor, num, new AtomicLong()));
                flux = flux.mergeWith(storageProcessor.doOnNext(b -> {
                    if (num.get() > 0) {
                        log.info("async delete {} files from {}", num.get(), storage);
                    }
                }));
            }

            flux.doFinally(s -> {
                if (DEBUG) {
                    log.info("end scanTask");
                }

                DISK_SCHEDULER.schedule(DelFileRunner::scanTask, 10, TimeUnit.SECONDS);
            }).subscribe();
        } catch (Exception e) {
            log.error("", e);
            DISK_SCHEDULER.schedule(DelFileRunner::scanTask, 10, TimeUnit.SECONDS);
        }
    }

    private static void deleteFile0(String storage, String filename) {
        StoragePool pool = StoragePoolFactory.getStoragePool(storage, "");
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(pool.getObjectVnodeId(filename)).block();
        Tuple2<Mono<Boolean>, Disposable> res = ErasureClient.deleteObjectFile0(pool, filename, nodeList);
        res.var1.subscribe(b -> {
            DelFileRunner.endRun(storage);
            removeTask(storage, filename);
        }, e -> {
            log.error("", e);
            DelFileRunner.endRun(storage);
        });
    }

    public static void dealTask(String storage, AtomicBoolean end, Queue<String> queue, MonoProcessor<Boolean> processor,
                                AtomicLong delNum, AtomicLong cycles) {
        String fileName = queue.peek();
        while (null != fileName) {
            if (tryRun(storage)) {
                delNum.incrementAndGet();
                String fileName0 = queue.poll();
                deleteFile0(storage, fileName0);
            } else {
                DISK_SCHEDULER.schedule(() -> dealTask(storage, end, queue, processor, delNum, cycles), 100, TimeUnit.MILLISECONDS);
                return;
            }

            fileName = queue.peek();
        }

        if (end.get()) {
            if (DEBUG) {
                log.info("end dealTask {}", storage);
            }
            processor.onNext(true);
        } else {
            if (DEBUG) {
                if (cycles.incrementAndGet() % 100 == 0) {
                    log.info("dealTask {}:{} waiting {}", storage, delNum.get(), queue.size(), delNum, cycles);
                }
            }
            DISK_SCHEDULER.schedule(() -> dealTask(storage, end, queue, processor, delNum, cycles), 100, TimeUnit.MILLISECONDS);
        }
    }

    public static void scanTask(String storage, String marker, AtomicBoolean end, Queue<String> res) {
        try {
            int size = res.size();
            if (size > 2000) {
                String nextMarker = marker;
                DISK_SCHEDULER.schedule(() -> scanTask(storage, nextMarker, end, res), 1L, TimeUnit.SECONDS);
                return;
            }

            String prefix = PREIFX + storage + "#";

            int count = 0;
            try (MSRocksIterator iterator = MSRocksDB.getRocksDB(Utils.getMqRocksKey()).newIterator()) {
                iterator.seek(marker.getBytes());
                while (iterator.isValid() && count < 1000) {
                    String key = new String(iterator.key());
                    if (key.startsWith(prefix)) {
                        if (!key.equalsIgnoreCase(marker)) {
                            res.add(key.substring(prefix.length()));
                            count++;
                            marker = key;
                        }
                    } else {
                        break;
                    }

                    iterator.next();
                }
            }

            if (count >= 1000) {
                String nextMarker = marker;
                if (size == 0) {
                    DISK_SCHEDULER.schedule(() -> scanTask(storage, nextMarker, end, res));
                } else {
                    DISK_SCHEDULER.schedule(() -> scanTask(storage, nextMarker, end, res), 1L, TimeUnit.SECONDS);
                }
            } else {
                if (DEBUG) {
                    log.info("end scanTask {}", storage);
                }
                end.set(true);
            }
        } catch (Exception e) {
            log.error("", e);
            end.set(true);
        }
    }

    public static ConcurrentHashMap<String, AtomicLong> deleteRunning = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, Long> deleteLimit = new ConcurrentHashMap<>();

    public static boolean tryRun(String pool) {
        AtomicLong running = deleteRunning.computeIfAbsent(pool, k -> new AtomicLong());

        AtomicBoolean run = new AtomicBoolean(true);
        running.updateAndGet(l -> {
            if (l >= deleteLimit.getOrDefault(pool, 1000L)) {
                run.set(false);
                return l;
            } else {
                run.set(true);
                return l + 1;
            }
        });

        return run.get();
    }

    public static void endRun(String pool) {
        AtomicLong running = deleteRunning.computeIfAbsent(pool, k -> new AtomicLong());
        running.decrementAndGet();
    }

    public static String PREIFX = "#del#";

    public static void addTask(String storage, String filename) {
        try {
            String key = PREIFX + storage + "#" + filename;
            MSRocksDB.getRocksDB(Utils.getMqRocksKey()).put(key.getBytes(), "".getBytes());
        } catch (RocksDBException e) {
            log.error("addTask error {}:{}", storage, filename, e);
        }
    }

    public static void removeTask(String storage, String filename) {
        try {
            String key = PREIFX + storage + "#" + filename;
            MSRocksDB.getRocksDB(Utils.getMqRocksKey()).delete(key.getBytes());
        } catch (RocksDBException e) {
            log.error("removeTask error {}:{}", storage, filename, e);
        }
    }

    public static Disposable EMPTY_DISPOSE = new Disposable() {
        @Override
        public void dispose() {

        }

        @Override
        public boolean isDisposed() {
            return true;
        }
    };
}
