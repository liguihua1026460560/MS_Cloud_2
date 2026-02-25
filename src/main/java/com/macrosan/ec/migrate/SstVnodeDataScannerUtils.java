package com.macrosan.ec.migrate;

import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksIterator;
import com.macrosan.database.rocksdb.MossMergeOperator;
import com.macrosan.ec.server.LocalMigrateServer;
import com.macrosan.rabbitmq.RequeueMQException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;

/**
 * @Description:
 * @Author wanhao
 * @Date 2023/3/23 0023 下午 1:50
 */
@Log4j2
public class SstVnodeDataScannerUtils {

    public static final MergeOperator MERGE_OPERATOR = new MossMergeOperator();
    public static final Options OPTIONS = new Options().setMergeOperator(MERGE_OPERATOR).setMaxOpenFiles(1);
    public static final ReadOptions READ_OPTIONS = new ReadOptions();
    public static final ReadOptions INTERNAL_READ_OPTIONS = new ReadOptions().setIterStartSeqnum(1);

    public static final List<String> PREFIX_LIST = new ArrayList<>(Arrays.asList(
            ROCKS_PART_PREFIX, ROCKS_BUCKET_META_PREFIX, ROCKS_INODE_PREFIX, ROCKS_CHUNK_FILE_KEY,
            ROCKS_VERSION_PREFIX, ROCKS_LIFE_CYCLE_PREFIX, ROCKS_LATEST_KEY, "", ROCKS_COOKIE_KEY, ROCKS_PART_META_PREFIX,
            ROCKS_UNSYNCHRONIZED_KEY, ROCKS_COMPONENT_VIDEO_KEY + File.separator, ROCKS_COMPONENT_IMAGE_KEY + File.separator, ROCKS_STS_TOKEN_KEY,
            ROCKS_AGGREGATION_RATE_PREFIX, ROCKS_AGGREGATION_UNDO_LOG_PREFIX, ROCKS_AGGREGATION_META_PREFIX));
 

    private static Constructor<LiveFileMetaData> liveFileMetaDataConstructor;

    static {
        Constructor<LiveFileMetaData> constructor = (Constructor<LiveFileMetaData>) LiveFileMetaData.class.getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        liveFileMetaDataConstructor = constructor;
    }

    /**
     * 获取当前vnode可能存在需要迁移数据的sst文件列表
     */
    private static List<LiveFileMetaData> getMigrateSstFileList(String objVnode, List<LiveFileMetaData> list) {
        List<LiveFileMetaData> list1 = new ArrayList<>();
        list.forEach(meta -> {
            boolean res = existMigrateKey(objVnode, new String(meta.smallestKey()), new String(meta.largestKey()));
            if (res) {
                list1.add(meta);
            }
        });
        return list1;
    }

    /**
     * 判断是否可能存在需要迁移的key
     */
    public static boolean existMigrateKey(String objVnode, String smallestKey, String largestKey) {
        boolean b = checkPrefix(smallestKey, largestKey);
        if (!b) {
            return true;
        }
        int smallestRes = compareKey(objVnode, smallestKey);
        int largestRes = compareKey(objVnode, largestKey);
        return smallestRes >= 0 && largestRes <= 0;
    }

    public static boolean checkPrefix(String smallestKey, String largestKey) {
        if (Character.isDigit(smallestKey.charAt(0)) && Character.isDigit(largestKey.charAt(0))) {
            return true;
        }
        String smallestPrefix = smallestKey.substring(0, 1);
        String largestPrefix = largestKey.substring(0, 1);

        return smallestPrefix.equals(largestPrefix);
    }

    public static int compareKey(String objVnode, String key) {
        return objVnode.compareTo(LocalMigrateServer.getVnode(key));
    }


    public static List<LiveFileMetaData> getSstFileList(String objVnode, String vnode, String lun) {
        String path = "/" + lun + "/check_point/" + vnode + "/" + objVnode;
        try (FileInputStream stream = new FileInputStream(path)) {
            byte[] bytes = new byte[(int) stream.getChannel().size()];
            stream.read(bytes);
            List<LiveFileMetaData> cached = decode(new String(bytes));
            return cached;
        } catch (Exception e) {
            log.error("", e);
            return Collections.emptyList();
        }
    }

    private final static Set<String> _SET = new HashSet<String>() {
        {
            add(ROCKS_FILE_META_PREFIX);
            add(ROCKS_CHUNK_FILE_KEY);
        }
    };

    public static String getSeparator(String prefix) {
        if (_SET.contains(prefix)) {
            return "_";
        } else {
            return File.separator;
        }
    }

    public static void removeCheckPoint(String srcDisk, String vnode) {
        String path = "/" + srcDisk + "/check_point/" + vnode;
        removeCheckPoint(path);
    }

    public static String encode(List<LiveFileMetaData> list) {
        JsonArray res = new JsonArray(new LinkedList<>());
        for (LiveFileMetaData metaData : list) {
            JsonObject o = new JsonObject();
            o.put("columnFamilyName", metaData.columnFamilyName());
            o.put("level", metaData.level());
            o.put("fileName", metaData.fileName());
            o.put("path", metaData.path());
            o.put("size", metaData.size());
            o.put("smallestSeqno", metaData.smallestSeqno());
            o.put("largestSeqno", metaData.largestSeqno());
            o.put("smallestKey", metaData.smallestKey());
            o.put("largestKey", metaData.largestKey());
            o.put("numReadsSampled", metaData.numReadsSampled());
            o.put("beingCompacted", metaData.beingCompacted());
            o.put("numEntries", metaData.numEntries());
            o.put("numDeletions", metaData.numDeletions());
            res.add(o);
        }

        return Json.encode(res);
    }

    public static List<LiveFileMetaData> decode(String str) {
        JsonArray array = new JsonArray(str);
        List<LiveFileMetaData> list = new ArrayList<>(array.size());
        for (int i = 0; i < array.size(); i++) {
            JsonObject o = array.getJsonObject(i);
            byte[] columnFamilyName = o.getBinary("columnFamilyName");
            int level = o.getInteger("level");
            String fileName = o.getString("fileName");
            String path = o.getString("path");
            long size = o.getLong("size");
            long smallestSeqno = o.getLong("smallestSeqno");
            long largestSeqno = o.getLong("largestSeqno");
            byte[] smallestKey = o.getBinary("smallestKey");
            byte[] largestKey = o.getBinary("largestKey");
            long numReadsSampled = o.getLong("numReadsSampled");
            boolean beingCompacted = o.getBoolean("beingCompacted");
            long numEntries = o.getLong("numEntries");
            long numDeletions = o.getLong("numDeletions");
            try {
                LiveFileMetaData metaData = liveFileMetaDataConstructor.newInstance(columnFamilyName, level, fileName, path,
                        size, smallestSeqno, largestSeqno, smallestKey, largestKey, numReadsSampled, beingCompacted, numEntries, numDeletions);
                list.add(metaData);
            } catch (Exception e) {

            }
        }

        return list;
    }

    /**
     * 获取checkpoint下所有的sst文件信息
     */
    private static List<LiveFileMetaData> getLiveFileMetaData(String srcDisk, String vnode) {
        String cpPath = "/" + srcDisk + "/check_point/" + vnode;
        try (MossMergeOperator mergeOperator = new MossMergeOperator();
             Options options = new Options().setMergeOperator(mergeOperator).setCreateIfMissing(true)
                     .setCreateMissingColumnFamilies(true).setMaxOpenFiles(10);
             RocksDB rocksdb = TransactionDB.openReadOnly(options, cpPath)) {
            List<LiveFileMetaData> metaDataList = rocksdb.getLiveFilesMetaData();
            return metaDataList.stream()
                    .filter(metaData -> Arrays.equals(metaData.columnFamilyName(), RocksDB.DEFAULT_COLUMN_FAMILY))
                    .sorted(Comparator.comparing(SstFileMetaData::fileName))
                    .sorted((oldMeta, newMeta) -> newMeta.level() - oldMeta.level()).collect(Collectors.toList());
        } catch (Exception ignored) {
            log.error("", ignored);
        }
        return new ArrayList<>();
    }

    /**
     * 索引盘加节点扩容时创建checkpoint
     */
    public static void createCheckPoint(String srcDisk, String vnode, String[] link) {
        String cpPath = "/" + srcDisk + "/check_point/" + vnode;

        try {
            String parentPath = "/" + srcDisk + "/check_point";
            if (Files.notExists(Paths.get(parentPath))) {
                Files.createDirectories(Paths.get(parentPath));
            }
            if (Files.exists(Paths.get(cpPath))) {
                SstVnodeDataScannerUtils.removeCheckPoint(srcDisk, vnode);
            }
        } catch (Exception e) {
            log.error(e);
        }
        Optional.ofNullable(MSRocksDB.getRocksDB(srcDisk))
                .ifPresent(msRocks -> {
                    try (Checkpoint checkpoint = Checkpoint.create(msRocks.getRocksDB())) {
                        checkpoint.createCheckpoint(cpPath);
                        Set<String> hold = new HashSet<>();
                        List<LiveFileMetaData> list = getLiveFileMetaData(srcDisk, vnode);
                        for (String objVNode : link) {
                            List<LiveFileMetaData> curVnode = getMigrateSstFileList(objVNode, list);
                            byte[] cache = encode(curVnode).getBytes();
                            try (FileOutputStream outputStream = new FileOutputStream(cpPath + "/" + objVNode)) {
                                outputStream.write(cache);
                            }

                            hold.add(objVNode);
                            curVnode.forEach(m -> hold.add(m.fileName().substring(1)));
                        }

                        //删除不需要用到的文件
                        Files.walkFileTree(Paths.get(cpPath), new SimpleFileVisitor<Path>() {
                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                                if (!hold.contains(file.toFile().getName())) {
                                    Files.delete(file);
                                }
                                return FileVisitResult.CONTINUE;
                            }
                        });
                    } catch (Exception e) {
                        log.error(e);
                        if (e instanceof RocksDBException && !e.getMessage().contains("LockTimeout")) {
                            log.error("srcDisk create check point error, requeue", e);
                            throw new RequeueMQException("srcDisk create check point error, requeue");
                        }
                    }
                });
    }

    public static void removeCheckPoint(String path) {
        try {
            if (Files.exists(Paths.get(path))) {
                Files.walkFileTree(Paths.get(path), new SimpleFileVisitor<Path>() {

                    //删除vnode下check_point的所有文件
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    //删除当前vnode建立的check_point目录
                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        } catch (IOException ignored) {
        }
    }

    public static boolean vnodeExistMigrateKey(String lun, String vnode) {
        try (MSRocksIterator iterator = MSRocksDB.getRocksDB(lun).newIterator(READ_OPTIONS)) {
            for (String prefix : PREFIX_LIST) {
                String key = prefix + vnode;
                iterator.seek(key.getBytes());
                if (iterator.isValid() && new String(iterator.key()).startsWith(key)) {
                    return true;
                }
            }
        }
        return false;
    }
}
