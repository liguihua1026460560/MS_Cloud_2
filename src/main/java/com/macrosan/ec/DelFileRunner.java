package com.macrosan.ec;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;
import static com.macrosan.constants.SysConstants.ROCKS_CHUNK_FILE_KEY;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;

@Log4j2
public class DelFileRunner {
    public static int HDD_LIMIT = 100;
    public static int SSD_LIMIT = 200;
    public static int NVME_LIMIT = 400;
    public static boolean DEBUG = false;
    private static final Logger delObjLogger = LogManager.getLogger("DeleteObjLog.DelFileRunner");

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
                Queue<String>[] queue = new Queue[4];
                MonoProcessor<Boolean>[] processor = new MonoProcessor[4];
                for (int i = 0; i < queue.length; i++) {
                    queue[i] = Queues.<String>unbounded().get();
                    processor[i] = MonoProcessor.create();
                }

                if (DEBUG) {
                    log.info("start scanTask {}", storage);
                    log.info("start dealTask {}", storage);
                }

                DISK_SCHEDULER.schedule(() -> scanTask(storage, PREIFX + storage + "#", end, queue));

                for (int i = 0; i < queue.length; i++) {
                    int n = i;
                    AtomicLong num = new AtomicLong(0);
                    Queue<String> curQueue = queue[i];
                    MonoProcessor<Boolean> curStorageProcessor = processor[i];
                    DISK_SCHEDULER.schedule(() -> dealTask(n, storage, end, curQueue, curStorageProcessor, num, new AtomicLong()));
                    flux = flux.mergeWith(curStorageProcessor.doOnNext(b -> {
                        if (num.get() > 0) {
                            log.info("async delete {} files from {}", num.get(), storage);
                        }
                    }));
                }
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

    public static Map<String, MonoProcessor<Boolean>> chunkTaskMap = new ConcurrentHashMap<>();

    private static Mono<Boolean> startChunkTask(String chunkKey, MonoProcessor<Boolean> cur) {
        AtomicReference<Mono<Boolean>> last = new AtomicReference<>();
        last.set(Mono.just(true));
        chunkTaskMap.compute(chunkKey, (k, v) -> {
            if (v == null) {
                return cur;
            } else {
                last.set(v);
                return cur;
            }
        });

        return last.get();
    }

    private static void endChunkTask(String chunkKey, MonoProcessor<Boolean> cur) {
        chunkTaskMap.computeIfPresent(chunkKey, (k, v) -> {
            if (v == cur) {
                return null;
            } else {
                return v;
            }
        });

        cur.onNext(true);
    }

    private static void deleteChunk(String storage, String key) {
        String[] split = key.substring(1).split("\\" + ROCKS_CHUNK_FILE_KEY);
        String chunkKey = ROCKS_CHUNK_FILE_KEY + split[0];
        String bucket = split[1];
        String type = split[2];
        int n;
        if (split.length > 3) {
            n = 1024 - Integer.parseInt(split[3]);
        } else {
            n = 0;
        }

        switch (type) {
            //file
            case "1": {
                boolean needCheck;
                if (split.length > 4) {
                    needCheck = true;
                } else {
                    needCheck = false;
                }

                StoragePool pool = StoragePoolFactory.getStoragePool(storage, "");
                MonoProcessor<Boolean> cur = MonoProcessor.create();

                startChunkTask(chunkKey, cur).flatMap(b -> FsUtils.deleteChunkFile0(pool, n, bucket, new String[]{chunkKey}, needCheck, false))
                        .subscribe(b -> {
                            if (!b) {
                                delObjLogger.info("delete {} fail", key);
                            }
                            DelFileRunner.endRun(storage);
                            removeTask(storage, key);

                            endChunkTask(chunkKey, cur);
                        }, e -> {
                            log.error("", e);
                            DelFileRunner.endRun(storage);
                            endChunkTask(chunkKey, cur);
                        });
                break;
            }
            //meta
            case "2":
                MonoProcessor<Boolean> cur = MonoProcessor.create();
                startChunkTask(chunkKey, cur).flatMap(b -> FsUtils.realDeleteChunkMeta(chunkKey, bucket))
                        .subscribe(b -> {
                            if (!b) {
                                delObjLogger.info("delete {} fail", key);
                            }
                            DelFileRunner.endRun(storage);
                            removeTask(storage, key);
                            endChunkTask(chunkKey, cur);
                        }, e -> {
                            log.error("", e);
                            DelFileRunner.endRun(storage);
                            endChunkTask(chunkKey, cur);
                        });
                break;
            default:
                log.error("deleteChunk key error {}", key);
                DelFileRunner.endRun(storage);
        }
    }

    public static void dealTask(int n, String storage, AtomicBoolean end, Queue<String> queue, MonoProcessor<Boolean> processor,
                                AtomicLong delNum, AtomicLong cycles) {
        String fileName = queue.peek();
        while (null != fileName) {
            if (tryRun(storage)) {
                delNum.incrementAndGet();
                String fileName0 = queue.poll();
                if (DEBUG) {
                    delObjLogger.info("{} delete {}", n, fileName0);
                }
                try {
                    if (fileName0.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                        deleteChunk(storage, fileName0);
                    } else {
                        deleteFile0(storage, fileName0);
                    }
                } catch (Exception e) {
                    log.error("", e);
                    DelFileRunner.endRun(storage);
                }
            } else {
                DISK_SCHEDULER.schedule(() -> dealTask(n, storage, end, queue, processor, delNum, cycles), 100, TimeUnit.MILLISECONDS);
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
                    log.info("dealTask {}:{} waiting {}", storage, delNum.get(), queue.size());
                }
            }
            DISK_SCHEDULER.schedule(() -> dealTask(n, storage, end, queue, processor, delNum, cycles), 100, TimeUnit.MILLISECONDS);
        }
    }

    public static void scanTask(String storage, String marker, AtomicBoolean end, Queue<String>[] res) {
        try {
            int size = Arrays.stream(res).mapToInt(Queue::size).sum();
            if (size > res.length * 2000) {
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
                            String key0 = key.substring(prefix.length());
                            if (key0.startsWith(ROCKS_CHUNK_FILE_KEY)) {
                                int hash = key0.substring(1).split("\\" + ROCKS_CHUNK_FILE_KEY)[0].hashCode();
                                res[Math.abs(hash) % res.length].add(key0);
                            } else {
                                res[count % res.length].add(key0);
                            }
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
    public static Scheduler ADD_SCHEDULER = Schedulers.newSingle("add-scheduler");
    public static UnicastProcessor<byte[]> addProcessor = UnicastProcessor.create(Queues.<byte[]>unboundedMultiproducer().get());
    static WriteOptions writeOption = new WriteOptions();

    static {
        addProcessor.publishOn(ADD_SCHEDULER)
                .subscribe(b -> {
                    try {
                        byte[] tmp = addProcessor.poll();
                        if (tmp != null) {
                            LinkedList<byte[]> list = new LinkedList<>();
                            list.add(b);
                            list.add(tmp);

                            tmp = addProcessor.poll();
                            while (tmp != null) {
                                list.add(tmp);
                                if (list.size() >= 1000) {
                                    break;
                                }

                                tmp = addProcessor.poll();
                            }

                            try (WriteBatch batch = new WriteBatch()) {
                                for (byte[] bs : list) {
                                    batch.put(bs, "".getBytes());
                                }
                                MSRocksDB.getRocksDB(Utils.getMqRocksKey()).getRocksDB().write(writeOption, batch);
                            }
                        } else {
                            MSRocksDB.getRocksDB(Utils.getMqRocksKey()).put(b, "".getBytes());
                        }
                    } catch (Exception e) {
                        log.error("addTask error {}:{}", new String(b), e);
                    }
                });
    }

    public static void addTask(String storage, String filename) {
        String key = PREIFX + storage + "#" + filename;
        addProcessor.onNext(key.getBytes());
    }

    public static void addChunkFileTask(String storage, String bucket, String chunkKey, int n, boolean needCheck) {
        String nStr = String.format("%04d", 1024 - n);
        String key = PREIFX + storage + "#" + chunkKey + ROCKS_CHUNK_FILE_KEY + bucket + ROCKS_CHUNK_FILE_KEY + "1" + ROCKS_CHUNK_FILE_KEY + nStr;
        if (needCheck) {
            key += ROCKS_CHUNK_FILE_KEY + needCheck;
        }
        addProcessor.onNext(key.getBytes());
    }

    public static void addChunkMetaTask(String storage, String bucket, String chunkKey) {
        String key = PREIFX + storage + "#" + chunkKey + ROCKS_CHUNK_FILE_KEY + bucket + ROCKS_CHUNK_FILE_KEY + "2";
        addProcessor.onNext(key.getBytes());
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
