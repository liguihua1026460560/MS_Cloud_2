package com.macrosan.database.rocksdb;

import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.*;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author gaozhiyuan
 */
@Log4j2
class Compaction {
    private static final Scheduler COMPACT_SCHEDULER;

    static {
        MsExecutor executor = new MsExecutor(48, 1, new MsThreadFactory("compact"));
        COMPACT_SCHEDULER = Schedulers.fromExecutor(executor);
    }

    private String lun;
    private RocksDB db;
    private AtomicBoolean end = new AtomicBoolean(false);
    private SstFileManager manager;
    private long allowed;
    private AtomicLong updateCompactBufferNum = new AtomicLong();
    private AtomicBoolean compactBufferZero = new AtomicBoolean(true);

    private Compaction(String lun, RocksDB db, SstFileManager manager, long allowed) {
        this.db = db;
        this.lun = lun;
        this.manager = manager;
        this.allowed = allowed;
    }

    private Set<String> compacted = new ConcurrentSkipListSet<>();


    public static int compare(byte[] a, byte[] b) {
        int l = Math.min(a.length, b.length);
        for (int i = 0; i < l; i++) {
            int c = a[i] - b[i];
            if (c != 0) {
                return c;
            }
        }

        return a.length - b.length;
    }

    public static boolean haveOverlappingKeyRanges(Tuple2<byte[], byte[]> a,
                                                   Tuple2<byte[], byte[]> b) {
        if (compare(a.var1, b.var1) >= 0) {
            if (compare(a.var1, b.var2) <= 0) {
                return true;
            }
        } else if (compare(a.var2, b.var1) >= 0) {
            return true;
        }
        if (compare(a.var2, b.var2) <= 0) {
            if (compare(a.var2, b.var1) >= 0) {
                return true;
            }
        } else if (compare(a.var1, b.var2) <= 0) {
            return true;
        }
        return false;
    }

    private long compactDeletion() throws Exception {
        List<LiveFileMetaData> liveFileMetaData = db.getLiveFilesMetaData();
        List<String> needCompact = null;
        List<LiveFileMetaData> maybeCompact = new LinkedList<>();

        liveFileMetaData = liveFileMetaData.stream()
                .filter(m -> Arrays.equals(m.columnFamilyName(), RocksDB.DEFAULT_COLUMN_FAMILY))
                .collect(Collectors.toList());

        if (liveFileMetaData.size() <= 0) {
            return -1;
        }

        int maxLevel = liveFileMetaData.stream().mapToInt(LiveFileMetaData::level).max().getAsInt();

        Map<String, LiveFileMetaData> metaDataMap = new HashMap<>();
        Map<Integer, List<LiveFileMetaData>> levelMap = new HashMap<>();

        for (LiveFileMetaData metaData : liveFileMetaData) {
            metaDataMap.put(metaData.fileName(), metaData);
            levelMap.computeIfAbsent(metaData.level(), k -> new ArrayList<>(1024)).add(metaData);
        }


        Map<String, TableProperties> tablePropertiesMap = db.getPropertiesOfAllTables();

        for (LiveFileMetaData metaData : liveFileMetaData) {
            String key = metaData.path() + metaData.fileName();
            TableProperties properties = tablePropertiesMap.get(key);
            if (null != properties) {
                if (properties.getNumEntries() > 0 &&
                        (properties.getNumDeletions() > properties.getNumEntries() || properties.getNumDeletions() > 10000L)
                        && !metaData.beingCompacted() && metaData.level() > 1 && metaData.level() < maxLevel) {
                    maybeCompact.add(metaData);
                }
            }
        }

        long minFileSize = Long.MAX_VALUE;
        LiveFileMetaData compactFile = null;

        for (LiveFileMetaData metaData : maybeCompact) {
            Set<String> files = new HashSet<>();
            files.add(metaData.fileName());
            byte[] smallestKey = metaData.smallestKey();
            byte[] largestKey = metaData.largestKey();

            for (int i = metaData.level(); i <= metaData.level() + 1; i++) {
                List<LiveFileMetaData> levelFiles = levelMap.getOrDefault(i, Collections.emptyList());
                int start = levelFiles.size();
                int end = -1;

                for (int j = 0; j < levelFiles.size(); j++) {
                    LiveFileMetaData meta = levelFiles.get(j);
                    if (files.contains(meta.fileName())) {
                        start = Math.min(j, start);
                        end = Math.max(j, end);
                    }
                }

                if (end == -1) {
                    continue;
                }

                while (start > 0) {
                    if (compare(levelFiles.get(start - 1).largestKey(), levelFiles.get(start).smallestKey()) < 0) {
                        break;
                    }
                    start--;
                }

                while (end < levelFiles.size() - 1) {
                    if (compare(levelFiles.get(end + 1).smallestKey(), levelFiles.get(end).largestKey()) > 0) {
                        break;
                    }
                    end++;
                }

                for (int f = start; f <= end; ++f) {
                    files.add(levelFiles.get(f).fileName());
                }

                if (end >= start) {
                    if (compare(smallestKey, levelFiles.get(start).smallestKey()) > 0) {
                        smallestKey = levelFiles.get(start).smallestKey();
                    }

                    if (compare(largestKey, levelFiles.get(end).largestKey()) < 0) {
                        largestKey = levelFiles.get(end).largestKey();
                    }
                }

                Tuple2<byte[], byte[]> tmp = new Tuple2<>(smallestKey, largestKey);
                for (int m = i; m <= metaData.level() + 1; m++) {
                    for (LiveFileMetaData meta : levelMap.getOrDefault(m, Collections.emptyList())) {
                        if (haveOverlappingKeyRanges(tmp, new Tuple2<>(meta.smallestKey(), meta.largestKey()))) {
                            files.add(meta.fileName());
                        }
                    }
                }
            }

            long fileSize = files.stream().mapToLong(f -> metaDataMap.get(f).size()).sum();
            if (fileSize < minFileSize) {
                minFileSize = fileSize;
                needCompact = new ArrayList<>(files);
                compactFile = metaData;
            }
        }

        if (needCompact != null) {
            if (minFileSize > (400L << 30)) {
                log.debug("compaction too big {} {} {}", needCompact, minFileSize, compactFile.fileName());
                return -1;
            }

            long startTime = System.nanoTime();
            needCompact = needCompact.stream().distinct().collect(Collectors.toList());
            log.debug("start compact files {} {} {}", compactFile.fileName(), needCompact, minFileSize);
            try (CompactionOptions options = new CompactionOptions()
                    .setMaxSubcompactions(1)
                    .setOutputFileSizeLimit(256L << 20)) {
                compacted.addAll(needCompact);
                long n = updateCompactionBuffer();
                try {
                    db.compactFiles(options, Collections.singletonList(compactFile.fileName()), compactFile.level() + 1, -1, null);
                } finally {
                    if (updateCompactBufferNum.get() == n && compactBufferZero.compareAndSet(false, true)) {
                        manager.setCompactionBufferSize(0L);
                    }
                }
            } finally {
                compacted.removeAll(needCompact);
            }

            long endTime = System.nanoTime();

            return (endTime - startTime) / 1000_000;
        }

        return -1;
    }

    private void startCompact() {
        long time = 10_000;
        if (!end.get()) {
            try {
                long t = compactDeletion();
                if (t > 0) {
                    log.debug("compact {} time:{} ms", lun, t);
                    if (t > 1000) {
                        time = 0L;
                    }
                }
            } catch (Exception e) {
                if (e.getMessage().contains("currently being compacted") || e.getMessage().contains("does not exist in column family")) {
                    time = 10L;
                    log.debug("{} compact fail {}. try again", lun, e.getMessage());
                } else {
                    log.error("{} compact fail {}", lun, e.getMessage());
                }
            } finally {
                COMPACT_SCHEDULER.schedule(this::startCompact, time, TimeUnit.MILLISECONDS);
            }
        }
    }

    private long updateCompactionBuffer() {
        if (!end.get()) {
            final long n = updateCompactBufferNum.incrementAndGet();
            compactBufferZero.set(false);

            long usable = allowed - manager.getTotalSize();
            long size = usable - (400L << 30);
            manager.setCompactionBufferSize(size < 0 ? 0 : size);

            COMPACT_SCHEDULER.schedule(() -> {
                if (updateCompactBufferNum.get() == n && compactBufferZero.compareAndSet(false, true)) {
                    manager.setCompactionBufferSize(0L);
                }
            }, 10_000, TimeUnit.MILLISECONDS);

            return n;
        }

        return -1;
    }

    private static final Map<String, Compaction> map = new ConcurrentHashMap<>();

    public static void startCompact(String lun, RocksDB db, SstFileManager manager, long allowded) {
        Compaction compaction = new Compaction(lun, db, manager, allowded);
        map.put(lun, compaction);
        COMPACT_SCHEDULER.schedule(compaction::startCompact, 10_000, TimeUnit.MILLISECONDS);
    }

    public static void endCompact(String lun) {
        Compaction compaction = map.remove("/" + lun + "/");
        if (compaction != null) {
            compaction.end.set(true);
        }
    }
}
