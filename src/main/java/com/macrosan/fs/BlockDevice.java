package com.macrosan.fs;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.database.rocksdb.batch.WriteBatch;
import com.macrosan.ec.rebuild.DiskStatusChecker;
import com.macrosan.fs.Allocator.Result;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.BlockInfo;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.vertx.core.json.Json;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;

import static com.macrosan.constants.SysConstants.REDIS_LUNINFO_INDEX;
import static com.macrosan.constants.SysConstants.ROCKS_FILE_SYSTEM_PREFIX;
import static com.macrosan.database.rocksdb.MossMergeOperator.SPACE_LEN;
import static com.macrosan.database.rocksdb.MossMergeOperator.SPACE_SIZE;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;

/**
 * 块设备的操作
 *
 * @author gaozhiyuan
 */
@Log4j2
@Data
public class BlockDevice {
    /**
     * 请求长度较小时会从预留空间分配，一次分配的最小长度BLOCK_SIZE
     */
    public static final int BLOCK_SIZE = 4 * 1024;
    public static final int MOD_BLOCK_SIZE = -BLOCK_SIZE;
    public static final int OFF_BLOCK_SIZE = BLOCK_SIZE - 1;

    public static int fitBlock(int size) {
        return (size + OFF_BLOCK_SIZE) & MOD_BLOCK_SIZE;
    }

    /**
     * 请求长度较大时，alloc一次分配的最小的长度MIN_ALLOC_SIZE
     */
    public static final int MIN_ALLOC_SIZE = 1024 * 1024;

    private static final byte[] BLOCK_DEVICE_START_BYTES = "MOSS_FS_fs-SP0-".getBytes();
    private static final int FS_NAME_LEN = "MOSS_FS_".getBytes().length;
    public static final String ROCKS_FILE_SYSTEM_PREFIX_OFFSET = ROCKS_FILE_SYSTEM_PREFIX + "1" + ROCKS_FILE_SYSTEM_PREFIX;
    public static final String ROCKS_FILE_SYSTEM_PREFIX_INFO = ROCKS_FILE_SYSTEM_PREFIX + "2" + ROCKS_FILE_SYSTEM_PREFIX;
    public static final byte[] ROCKS_FILE_SYSTEM_USED_SIZE = (ROCKS_FILE_SYSTEM_PREFIX_INFO + "used_size").getBytes();
    public static final byte[] ROCKS_FILE_SYSTEM_FILE_SIZE = (ROCKS_FILE_SYSTEM_PREFIX_INFO + "file_size").getBytes();
    public static final byte[] ROCKS_COMPRESSION_FILE_SYSTEM_FILE_SIZE = (ROCKS_FILE_SYSTEM_PREFIX_INFO + "compression_size").getBytes();
    public static final byte[] ROCKS_BEFORE_COMPRESSION_FILE_SYSTEM_FILE_SIZE = (ROCKS_FILE_SYSTEM_PREFIX_INFO + "before_compression_size").getBytes();
    public static final byte[] ROCKS_FILE_SYSTEM_FILE_NUM = (ROCKS_FILE_SYSTEM_PREFIX_INFO + "num").getBytes();
    private final static ThreadFactory INIT_THREAD_FACTORY = new MsThreadFactory("init-blockSpace");
    public static final Scheduler INIT_BLOCK_SCHEDULER;
    private static final byte[] OFFSET_MOVE_END_BYTES = "offset_move_end".getBytes();

    public static Map<String, BlockDevice> map = new ConcurrentHashMap<>();

    static {
        releasePeriodically();
        Scheduler scheduler = null;
        try {
            MsExecutor executor = new MsExecutor(1, 1, INIT_THREAD_FACTORY);
            scheduler = Schedulers.fromExecutor(executor);
        } catch (Exception e) {
            log.error("", e);
        }
        INIT_BLOCK_SCHEDULER = scheduler;
    }

    private static Field mountEntry;
    private static Field mountDirField;
    private static Field mountNameField;

    static {
        try {
            FileSystem fileSystem = FileSystems.getDefault();
            FileStore store = fileSystem.getFileStores().iterator().next();
            Class c = store.getClass();
            mountEntry = c.getSuperclass().getDeclaredField("entry");
            mountEntry.setAccessible(true);
            Object o = mountEntry.get(store);
            mountDirField = o.getClass().getDeclaredField("dir");
            mountDirField.setAccessible(true);
            mountNameField = o.getClass().getDeclaredField("name");
            mountNameField.setAccessible(true);
        } catch (Exception e) {
            log.error("load find mount entry method fail", e);
            System.exit(-1);
        }
    }

    public static synchronized void init() {
        FileSystem fileSystem = FileSystems.getDefault();
        Iterator<FileStore> iterator = fileSystem.getFileStores().iterator();
        int n = 0;

        List<Tuple2<String, String>> list = new LinkedList<>();
        while (iterator.hasNext()) {
            FileStore store = iterator.next();
            try {
                Object mount = mountEntry.get(store);
                String mountDir = new String((byte[]) mountDirField.get(mount));
                String mountName = new String((byte[]) mountNameField.get(mount));
                if (mountName.endsWith("1") || mountName.endsWith("2")) {
                    list.add(new Tuple2<>(mountName, mountDir));
                }
            } catch (Exception e) {
                log.error("", e);
            }
        }

        list.parallelStream().forEach(t -> {
            String mountName = t.var1;
            String mountDir = t.var2;
            String path = mountName.substring(0, mountName.length() - 1) + "3";
            if (!new File(path).exists()) {
                path = mountName.substring(0, mountName.length() - 1) + "2";
                if (!new File(path).exists()) {
                    return;
                }
            }

            try (FileInputStream stream = new FileInputStream(path)) {
                byte[] bytes = new byte[BLOCK_SIZE];
                stream.read(bytes);
                boolean mossFS = true;
                for (int i = 0; i < BLOCK_DEVICE_START_BYTES.length; i++) {
                    if (bytes[i] != BLOCK_DEVICE_START_BYTES[i]) {
                        mossFS = false;
                        break;
                    }
                }

                if (mossFS) {
                    int start = FS_NAME_LEN;
                    int end = start + 1;
                    while (!(bytes[end] == '\r' && bytes[end + 1] == '\n')) {
                        end++;
                    }

                    String name = new String(bytes, start, end - start).replace(" ", "");
                    if (!name.equals(mountDir.substring(1)) || map.containsKey(name)) {
                        return;
                    }

                    start = end + 7;
                    end = start + 1;
                    while (!(bytes[end] == '\r' && bytes[end + 1] == '\n')) {
                        end++;
                    }

                    String size = new String(bytes, start, end - start);

                    boolean offsetMoveEnd = true;
                    start = end + 2;
                    for (int i = 0; i < OFFSET_MOVE_END_BYTES.length; i++) {
                        if (bytes[start + i] != OFFSET_MOVE_END_BYTES[i]) {
                            offsetMoveEnd = false;
                            break;
                        }
                    }

                    String node = ServerConfig.getInstance().getHostUuid();
                    long keyExists = RedisConnPool.getInstance().getCommand(REDIS_LUNINFO_INDEX).exists(node + "@" + name);
                    if (keyExists == 0) {
                        return;
                    }
                    if (DiskStatusChecker.isRebuildWaiter(name)) {
                        log.info("disk rebuild waiter for {} ,skip", mountDir);
                        return;
                    }
                    BlockDevice device = new BlockDevice(name, path, Long.parseLong(size), offsetMoveEnd);
                    map.put(name, device);
                }
            } catch (Exception e) {
                log.error("", e);
            }
        });
    }

    public static synchronized void initDisk(String disk) {
        // 获取锁后判断是否已经加载过
        if (!map.containsKey(disk)) {
            init();
        }
    }

    private String name;
    private String path;
    private long size;
    int fd;
    private Allocator allocator;
    public AioChannel channel;

    private void tryInitBlockSpace(boolean isMoveEnd) {
        try {
            ColumnFamilyHandle columnFamilyHandle = MSRocksDB.getColumnFamily(name);
            byte[] v = MSRocksDB.getRocksDB(name).get(columnFamilyHandle, BlockInfo.getFamilySpaceKey(0).getBytes());
            if (null == v) {
                if (isMoveEnd) {
                    isMoveEnd = false;
                    writeOffsetMoveFlag(false);
                }
                List<byte[]> keyList = new LinkedList<>();
                byte[] value = new byte[SPACE_LEN];
                Flux<Boolean> flux = Flux.empty();
                for (int index = 0; index < size / SPACE_SIZE; index++) {
                    keyList.add(BlockInfo.getFamilySpaceKey(index).getBytes());

                    if (keyList.size() > 1000) {
                        byte[][] keys = keyList.toArray(new byte[keyList.size()][]);
                        flux = flux.mergeWith(BatchRocksDB.customizeOperateDataForInitBlockSpace(name, (db, w, r) -> {
                            for (byte[] key : keys) {
                                w.put(columnFamilyHandle, key, value);
                            }
                        }));

                        keyList.clear();
                    }
                }

                flux.collectList().block();
                log.info("{} init block space ", name);
            }
            if (!isMoveEnd) {
                scanMoveOffsetData();
            }
        } catch (Exception e) {
            log.error("", e);
            return;
        }
    }

    private BlockDevice(String name, String path, long size, boolean offsetMoveEnd) throws RocksDBException {
        long start = System.currentTimeMillis();
        this.name = name;
        this.path = path;
        this.size = size;

        if (null == MSRocksDB.getRocksDB(name)) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "open " + name + " rocks db fail");
        }

        fd = FileChannel.open(path);

        //初始化已有的文件至逻辑空间
        ColumnFamilyHandle columnFamilyHandle = MSRocksDB.getColumnFamily(name);
        if (allocator == null) {
            //逻辑空间块alloc和release的粒度为4K
            allocator = new MsAllocator((MIN_ALLOC_SIZE / BLOCK_SIZE) * (size / MIN_ALLOC_SIZE - 1), 1);
        }
        long offset = 0;
        long len = 0;

        tryInitBlockSpace(offsetMoveEnd);

        for (int index = 0; index < size / SPACE_SIZE; index++) {
            String key = BlockInfo.getFamilySpaceKey(index);
            byte[] value = MSRocksDB.getRocksDB(name).get(columnFamilyHandle, key.getBytes());
            if (null == value) {
                MSRocksDB.getRocksDB(name).put(columnFamilyHandle, key.getBytes(), new byte[SPACE_LEN]);
            } else {
                long spaceIndex = getSpaceIndex(key);
                for (int i = 0; i < value.length; i++) {
                    if (value[i] == -1) {
                        if (len == 0) {
                            offset = i * 8 + SPACE_LEN * spaceIndex * 8 - 1;
                            offset = offset == -1 ? 0 : offset;
                        }
                        len += 8;
                    } else {
                        byte[] arr = BlockInfo.getBitArray(value[i]);
                        for (int j = 0; j < 8; j++) {
                            if (arr[j] == 1) {
                                if (len == 0) {
                                    offset = i * 8 + j + SPACE_LEN * spaceIndex * 8 - 1;
                                    offset = offset == -1 ? 0 : offset;
                                }
                                len++;
                            } else {
                                if (len > 0) {
                                    allocator.initAllocated(offset, len);
                                    len = 0;
                                }
                            }
                        }
                    }
                }
            }
        }
        if (len > 0) {
            allocator.initAllocated(offset, len);
        }
        channel = new AioChannel(this);
        log.info("init block device {} complete with {} ms", name, System.currentTimeMillis() - start);
    }

    private long getSpaceIndex(String key) {
        int index = 3;
        char[] chars = key.toCharArray();
        for (int i = 3; i < chars.length; i++) {
            char temp = chars[i];
            if (temp != '0') {
                index = i;
                break;
            }
        }

        if (index > key.length() - 1) {
            return 0;
        }

        return Long.parseLong(key.substring(index));
    }

    public static BlockDevice get(String lun) {
        BlockDevice res = map.get(lun);
        if (null == res && !DiskStatusChecker.isRebuildWaiter(lun)) {
            initDisk(lun);
            res = map.get(lun);
        }

        return res;
    }

    public static void remove(String lun) {
        map.remove(lun);
        log.info("remove block device {}", lun);
    }

    /**
     * 记录各个lun上待移除的单位逻辑空间块的offset。
     */
    public static Map<String, Queue<Result>> toReleaseMap = new ConcurrentHashMap<>();

    /**
     * 将fileMeta中的offsets和lens转回alloc用的Result，放入toReleaseMap
     */
    public static void addToReleaseMap(long[] offsets, long[] lens, String lun) {
        for (int j = 0; j < offsets.length; j++) {
            //保存该lun下待release的offset
            Queue<Allocator.Result> queue = toReleaseMap.getOrDefault(lun, new ConcurrentLinkedQueue<>());
            synchronized (queue) {
                Allocator.Result fileResult = new Allocator.Result();
                fileResult.offset = offsets[j];
                fileResult.size = lens[j];
                queue.add(fileResult);
                toReleaseMap.put(lun, queue);
            }
        }
    }

    /**
     * 从分配器中请求分配num长度的可写数据
     * 分配器的分配最小单位是MIN_ALLOC_SIZE（1M）
     * 预留空间（smallAllocResList）的最小分配单位是BLOCK_SIZE（4K）
     * 请求长度较大时直接从分配器分配
     * 请求长度较小时从提前预留的一部分空间截取，这部分空间不足时从分配器分配一部分空间到预留空间（allocSmall）
     *
     * @param num 待落盘的字节长度
     * @return 分配结果
     */
    public synchronized Result[] alloc(long num) {
        //申请的逻辑空间块数量
        long allocNum = num / BLOCK_SIZE;
        if (num % BLOCK_SIZE != 0) {
            allocNum += 1;
        }
        Result[] allocResults = allocator.allocate(allocNum);

        if (allocResults.length == 0) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, name + " no enough disk space.");
        }

        long resultSizeCount = 0L;
        for (int i = 0; i < allocResults.length; i++) {
            resultSizeCount += allocResults[i].size;
        }
        if (resultSizeCount != allocNum) {
            get(name).allocator.release(allocResults);
            throw new MsException(ErrorNo.UNKNOWN_ERROR, name + " no enough disk space.");
        }

        //合并连续的逻辑空间块
        List<Result> fileRes = new LinkedList<>();
        for (int i = 0; i < allocResults.length; i++) {
            Result fileResult = new Result();
            fileResult.offset = (allocResults[i].offset + 1) * BLOCK_SIZE;
            fileResult.size = allocResults[i].size * BLOCK_SIZE;

            // size没有达到1M且alloc到的是连续的块，将前后逻辑块合并再传给待落盘的文件。
            while (i + 1 < allocResults.length
                    && allocResults[i].offset + 1 == allocResults[i + 1].offset
                    && fileResult.size < MIN_ALLOC_SIZE) {
                fileResult.size += allocResults[i + 1].size * BLOCK_SIZE;
                i += 1;
            }
            fileRes.add(fileResult);
        }
        return fileRes.toArray(new Result[0]);
    }

    /**
     * 读取块设备的数据
     *
     * @param offset 读取的起始位置
     * @param len    读取的长度
     * @return 数据
     */
    public byte[] read(long offset, int len) {
        byte[] bytes = new byte[len];
        int readLen = FileChannel.read(fd, offset, bytes, len);
        if (readLen != len) {
            throw new UnsupportedOperationException("read len error");
        }

        return bytes;
    }

    /**
     * 将bytes写入块设备
     *
     * @param bytes 需要写入的数据
     * @return 数据写入的位置
     */
    @Deprecated
    public Result[] write(byte[] bytes) {
        byte[] write;

        if (bytes.length % BLOCK_SIZE != 0) {
            int len = bytes.length / BLOCK_SIZE * BLOCK_SIZE + BLOCK_SIZE;
            write = new byte[len];
            System.arraycopy(bytes, 0, write, 0, bytes.length);
        } else {
            write = bytes;
        }

        Result[] allocRes = alloc(write.length);
        if (allocRes.length == 1) {
            int writeLen = FileChannel.write(fd, allocRes[0].offset, write, write.length);
            if (writeLen != write.length) {
                throw new UnsupportedOperationException("write len error");
            }

            allocRes[0].size = write.length;
            return allocRes;
        }

        if (allocRes.length > 0) {
            int all = write.length;

            for (Result result : allocRes) {
                int needWrite = result.size > all ? all : (int) result.size;
                byte[] curWriteBytes = new byte[needWrite];
                System.arraycopy(write, write.length - all, curWriteBytes, 0, needWrite);

                int writeLen = FileChannel.write(fd, result.offset, curWriteBytes, needWrite);
                if (writeLen != needWrite) {
                    throw new UnsupportedOperationException("write len error");
                }

                result.size = needWrite;
                all -= needWrite;
            }

            return allocRes;
        }

        throw new UnsupportedOperationException("alloc size error");
    }

    private static void releasePeriodically() {
        Flux.interval(Duration.ofSeconds(10))
                .subscribeOn(DISK_SCHEDULER)
                .subscribe(l -> {
                    try {
                        toReleaseMap.forEach((lun, queue) -> {
                            synchronized (queue) {
                                if (!queue.isEmpty()) {
                                    List<Result> memsList = new LinkedList<>();
                                    queue.forEach(fileResult -> {
                                        Result allocResult = new Result();
                                        allocResult.offset = fileResult.offset / BLOCK_SIZE - 1;
                                        allocResult.size = fileResult.size / BLOCK_SIZE;
                                        memsList.add(allocResult);
                                    });
                                    get(lun).allocator.release(memsList.toArray(new Result[0]));
                                    queue.clear();
                                }
                            }
                        });
                    } catch (Exception e) {
                        log.error("periodical space release error", e);
                    }
                });
    }


    public void scanMoveOffsetData() {
        BatchRocksDB.RequestConsumer consumer = (db, writeBatch, batchRequest) -> {
            try (MSRocksIterator iterator = MSRocksDB.getRocksDB(name).newIterator()) {
                long maxScanNumber = 10000;
                LinkedList<String> delKeys = new LinkedList<>();
                iterator.seek(ROCKS_FILE_SYSTEM_PREFIX_OFFSET.getBytes());
                String firstKey = iterator.isValid() ? new String(iterator.key()) : null;
                long lastStartOffset = 0;
                long lastLen = 0;
                long lastNum = 1;
                if (iterator.isValid() && firstKey.startsWith(ROCKS_FILE_SYSTEM_PREFIX_OFFSET)) {
                    delKeys.add(firstKey);
                    BlockInfo blockInfo = Json.decodeValue(new String(iterator.value()), BlockInfo.class);
                    lastStartOffset = blockInfo.getOffset();
                    lastLen = blockInfo.getTotal();
                    iterator.next();
                }
                while (iterator.isValid()) {
                    String key = new String(iterator.key());
                    if (key.startsWith(ROCKS_FILE_SYSTEM_PREFIX_OFFSET)) {
                        BlockInfo blockInfo = Json.decodeValue(new String(iterator.value()), BlockInfo.class);
                        if (lastNum == maxScanNumber || lastStartOffset + lastLen != blockInfo.getOffset()) {
                            moveDataToOffsetColumnFamily(writeBatch, lastStartOffset, lastLen, delKeys);
                            lastStartOffset = blockInfo.getOffset();
                            lastLen = blockInfo.getTotal();
                            lastNum = 1;
                        } else {
                            lastLen += blockInfo.getTotal();
                            lastNum++;
                        }
                        delKeys.add(key);
                    }
                    iterator.next();
                }
                if (!delKeys.isEmpty()) {
                    moveDataToOffsetColumnFamily(writeBatch, lastStartOffset, lastLen, delKeys);
                }
            }
        };
        BatchRocksDB.customizeOperateDataForInitBlockSpace(name, consumer).block();
        log.info("{} offset data move end", name);
        writeOffsetMoveFlag(true);
    }

    public void moveDataToOffsetColumnFamily(WriteBatch writeBatch, long offset, long len, List<String> delKeys) {
        List<byte[]> list = BlockInfo.getUpdateValue(offset, len, "upload");
        ColumnFamilyHandle handle = MSRocksDB.getColumnFamily(name);
        try {
            for (int index = 0; index < list.size(); index++) {
                String key = BlockInfo.getFamilySpaceKey((offset / SPACE_SIZE) + index);
                writeBatch.merge(handle, key.getBytes(), list.get(index));
            }
            Iterator<String> iterator = delKeys.iterator();
            while (iterator.hasNext()) {
                writeBatch.delete(iterator.next().getBytes());
                iterator.remove();
            }
        } catch (RocksDBException e) {
            log.info("move data to offset columnFamily error:{}", e.getMessage());
        }
    }

    public void writeOffsetMoveFlag(boolean isMoveEnd) {
        String msg = new String(OFFSET_MOVE_END_BYTES);
        if (!isMoveEnd) {
            msg = new String(new byte[OFFSET_MOVE_END_BYTES.length]);
        }
        try (RandomAccessFile rf = new RandomAccessFile(path, "rw")) {
            rf.readLine();
            rf.readLine();
            rf.writeBytes(msg + "\r\n");
        } catch (IOException e) {
            log.info(e.getMessage());
        }
    }
}
