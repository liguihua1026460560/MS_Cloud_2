package com.macrosan.ec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.ReadWriteLock;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.batch.ValueGetter;
import com.macrosan.message.jsonmsg.Cutting;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.fast.MsObjectMapper;
import com.macrosan.storage.StoragePool;
import com.macrosan.utils.functional.ImmutableTuple;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.property.PropertyReader;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.io.File;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.constants.SysConstants.ROCKS_CACHE_ACCESS_KEY;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.message.jsonmsg.Inode.NOT_FOUND_INODE;

@Log4j2
public class Utils {
    public static byte[] getDeleteMarkKey(byte[] key) {
        byte[] bytes = ROCKS_OBJ_META_DELETE_MARKER.getBytes();
        byte[] deleteMarker = new byte[key.length + bytes.length];
        System.arraycopy(bytes, 0, deleteMarker, 0, bytes.length);
        System.arraycopy(key, 0, deleteMarker, bytes.length, key.length);

        return deleteMarker;
    }

    private static final ThreadLocal<ValueGetter> DELETE_MARKER_GETTER =
            ThreadLocal.withInitial(() -> new ValueGetter("deleteMark"));

    public static boolean isDeleteMarker(byte[] value) {
        String a = DELETE_MARKER_GETTER.get().get(value)[0];
        return a != null && a.contains("true");
    }

    public static String getObjFileName(StoragePool pool, String bucket, String object, String requestId) {
        if (false) {
            return getObjFileName0(pool, bucket, object, requestId);
        }
        ImmutableTuple<String, String> tuple = pool.getObjectVnodeId(bucket, object);

        String objVnode = tuple.var1;
        String sha1 = tuple.var2;

        return File.separator + String.join("_",
                new String[]{objVnode,
                        bucket,
                        sha1,
                        requestId});
    }

    public static String getObjFileName0(StoragePool pool, String bucket, String object, String requestId) {
        ImmutableTuple<String, String> tuple = pool.getObjectVnodeId(bucket, object);

        String objVnode = tuple.var1;
        String sha1 = tuple.var2;
        String timestamps = String.valueOf(System.currentTimeMillis());

        return File.separator + String.join("_",
                new String[]{objVnode,
                        bucket,
                        timestamps,
                        sha1,
                        requestId});
    }

    public static String getHoleFileName(StoragePool pool, String bucket, String requestId) {
        ImmutableTuple<String, String> tuple = pool.getObjectVnodeId(bucket, "");

        String objVnode = tuple.var1;

        return File.separator + String.join("_",
                new String[]{objVnode,
                        bucket,
                        "",
                        requestId});
    }

    public static String getDedupObjFileName(String objVnode, String md5, String storage, String requestId) {
        if (requestId == null) {
            return File.separator + String.join("_",
                    new String[]{objVnode,
                            md5,
                            storage});
        }

        return File.separator + String.join("_",
                new String[]{objVnode,
                        md5,
                        storage,
                        requestId});
    }

    public static final String ZERO_STR;
    public static final String ONE_STR;

    static {
        byte[] bytes = new byte[1];
        bytes[0] = 0;
        ZERO_STR = new String(bytes);
        byte[] oneBytes = new byte[1];
        oneBytes[0] = 1;
        ONE_STR = new String(oneBytes);
    }

    public static String getLatestMetaKey(String vnode, String bucket, String object) {
        return ROCKS_LATEST_KEY + vnode + File.separator + bucket + File.separator + object;
    }

    public static String getLatestMetaKey(String vnode, String bucket, String object, String snapshotMark) {
        return StringUtils.isBlank(snapshotMark) ? getLatestMetaKey(vnode, bucket, object)
                : ROCKS_LATEST_KEY + vnode + File.separator + bucket + File.separator + snapshotMark + File.separator + object;
    }

    public static String getMetaDataKey(String vnode, String bucket, String object, String snapshotMark) {
        return StringUtils.isBlank(snapshotMark) ? vnode + File.separator + bucket + File.separator + object
                : vnode + File.separator + bucket + File.separator + snapshotMark + File.separator + object;
    }

    public static String getMetaDataKey(String vnode, String bucket, String object, String versionId, String stamp) {
        return vnode + File.separator + bucket + File.separator + object + ZERO_STR + stamp + File.separator + versionId;
    }

    // snapshotMark不为空，则key中将带有快照标记
    public static String getMetaDataKey(String vnode, String bucket, String object, String versionId, String stamp, String snapshotMark) {
        return StringUtils.isBlank(snapshotMark) ? getMetaDataKey(vnode, bucket, object, versionId, stamp)
                : vnode + File.separator + bucket + File.separator + snapshotMark + File.separator + object + ZERO_STR + File.separator + stamp + File.separator + versionId;
    }

    //生成版本号对应的rocksDB的key
    public static String getVersionMetaDataKey(String vnode, String bucket, String object, String versionId) {
        if (StringUtils.isEmpty(versionId)) {
            return getMetaDataKey(vnode, bucket, object, null);
        }
        return ROCKS_VERSION_PREFIX + vnode + File.separator + bucket + File.separator + object + ZERO_STR + versionId;
    }

    // 通过rocksKey获取对应的vnode
    public static String getVersionObjectVnode(String rocksKey, String versionId) {
        if (StringUtils.isEmpty(versionId)) {
            return rocksKey.split(File.separator)[0];
        }
        return rocksKey.split(File.separator)[0].substring(1);
    }

    //生成版本号对应的rocksDB的key--snapshotMark不为空，则key中将带有快照标记
    public static String getVersionMetaDataKey(String vnode, String bucket, String object, String versionId, String snapshotMark) {
        if (StringUtils.isEmpty(versionId)) {
            return getMetaDataKey(vnode, bucket, object, snapshotMark);
        }
        return StringUtils.isBlank(snapshotMark) ? getVersionMetaDataKey(vnode, bucket, object, versionId)
                : ROCKS_VERSION_PREFIX + vnode + File.separator + bucket + File.separator + snapshotMark + File.separator + object + ZERO_STR + versionId;
    }

    public static String getVersionMetaDataKeyToSeek(String vnode, String bucket, String object) {
        return ROCKS_VERSION_PREFIX + vnode + File.separator + bucket + File.separator + object;
    }

    //生成生命周期对应的rocksDB的stamp的key
    public static String getLifeCycleMetaKey(String vnode, String bucket, String object, String versionId, String stamp) {
        return ROCKS_LIFE_CYCLE_PREFIX + vnode + File.separator + bucket + File.separator + stamp + File.separator + object + File.separator + versionId;
    }

    public static String getLifeCycleMetaKey(String vnode, String bucket, String object, String versionId, String stamp, long nodeId) {
        if (nodeId > 0) {
            long truncatedTimestamp = Long.parseLong(stamp) / DateUtils.MILLIS_PER_HOUR * DateUtils.MILLIS_PER_HOUR;
            return getLifeCycleMetaKey(vnode, bucket, object, versionId, String.valueOf(truncatedTimestamp));
        } else {
            return getLifeCycleMetaKey(vnode, bucket, object, versionId, stamp);
        }
    }

    public static String getLifeCycleMetaKey(String vnode, String bucket, String object, String versionId, String stamp, String snapshotMark) {
        return snapshotMark == null ? getLifeCycleMetaKey(vnode, bucket, object, versionId, stamp)
                : ROCKS_LIFE_CYCLE_PREFIX + vnode + File.separator + bucket + File.separator + snapshotMark + File.separator + stamp + File.separator + object + File.separator + versionId;
    }

    public static String getLifeCycleMetaKey(String vnode, String bucket, String object, String versionId, String stamp, String snapshotMark, long nodeId) {
        return snapshotMark == null ? getLifeCycleMetaKey(vnode, bucket, object, versionId, stamp, nodeId)
                : ROCKS_LIFE_CYCLE_PREFIX + vnode + File.separator + bucket + File.separator + snapshotMark + File.separator + stamp + File.separator + object + File.separator + versionId;
    }

    //生成生命周期对应的rocksDB的stamp的key
    public static String getLifeCycleStamp(String vnode, String bucket, String stamp) {
        return ROCKS_LIFE_CYCLE_PREFIX + vnode + File.separator + bucket + File.separator + stamp + File.separator;
    }

    public static String getLifeCycleStamp(String vnode, String bucket, String stamp, String snapshotMark) {
        return snapshotMark == null ? getLifeCycleStamp(vnode, bucket, stamp)
                : ROCKS_LIFE_CYCLE_PREFIX + vnode + File.separator + bucket + File.separator + snapshotMark + File.separator + stamp + File.separator;
    }

    //生成重删信息对应Rocksdb中的KEY值
    public static String getDeduplicatMetaKey(String vnode, String etag, String storage) {
        return ROCKS_DEDUPLICATE_KEY + vnode + File.separator + etag + File.separator + storage;
    }

    //生成重删信息对应Rocksdb中的KEY值
    public static String getDeduplicatMetaKey(String vnode, String etag, String storage, String requestId) {
        return ROCKS_DEDUPLICATE_KEY + vnode + File.separator + etag + File.separator + storage + ROCKS_FILE_META_PREFIX + requestId;
    }

    // 根据重删信息key获取数据池前缀
    public static String getStorageByDeduplicateKey(String deduplicateKey) {
        return deduplicateKey.split(File.separator)[2].split(ROCKS_FILE_META_PREFIX)[0];
    }

    public static String getMetaDataPrefix(String vnode, String bucket, String prefix) {
        return vnode + File.separator + bucket + File.separator + prefix;
    }

    public static Tuple3<String, String, String> getAllMetaDataKey(String vnode, String bucket, String object, String versionId, String stamp, String snapshotMark) {
        String t1 = getMetaDataKey(vnode, bucket, object, versionId, stamp, snapshotMark);
        String t2 = getVersionMetaDataKey(vnode, bucket, object, versionId, snapshotMark);
        String t3 = snapshotMark == null ? getLifeCycleMetaKey(vnode, bucket, object, versionId, stamp) : getLifeCycleMetaKey(vnode, bucket, object, versionId, stamp, snapshotMark);
        return new Tuple3<>(t1, t2, t3);
    }

    public static Tuple3<String, String, String> getAllMetaDataKey(String vnode, String bucket, String object, String versionId, String stamp, String snapshotMark, long nodeId) {
        String t1 = getMetaDataKey(vnode, bucket, object, versionId, stamp, snapshotMark);
        String t2 = getVersionMetaDataKey(vnode, bucket, object, versionId, snapshotMark);
        String t3 = snapshotMark == null ? getLifeCycleMetaKey(vnode, bucket, object, versionId, stamp, nodeId) : getLifeCycleMetaKey(vnode, bucket, object, versionId, stamp, snapshotMark, nodeId);
        return new Tuple3<>(t1, t2, t3);
    }

    public static Tuple3<String, String, String> getAllMetaDataKey(String vnode, String bucket, String object, String versionId, String stamp, long nodeId) {
        return getAllMetaDataKey(vnode, bucket, object, versionId, stamp, null, nodeId);
    }

    public static String getCacheOrderKey(String timestamp, String fileName) {
        return ROCKS_CACHE_ORDERED_KEY + timestamp + fileName;
    }

    public static String getFileMetaKeyByCacheOrderKey(String cacheOrderKey) {
        return ROCKS_FILE_META_PREFIX + cacheOrderKey.split(File.separator)[1];
    }

    public static String getAccessTimeKey(String timestamp, String fileName) {
        return ROCKS_CACHE_ACCESS_KEY + timestamp + fileName;
    }

    public static String getEndTimeStampMarker(String endTimeStamp) {
        return ROCKS_CACHE_ACCESS_KEY + endTimeStamp;
    }
    public static String getFileMetaKeyByAccessTimeKey(String accessTimeKey) {
        return ROCKS_FILE_META_PREFIX + accessTimeKey.split(File.separator)[1];
    }

    public static final byte[] ZERO_BYTES = new byte[]{};
    public static final String DEFAULT_STAMP = "0000000000000";
    private static final String DEFAULT_VERSION_ID = "null";

    private static final int len = (ZERO_STR + DEFAULT_STAMP +
            File.separator + DEFAULT_VERSION_ID).length();

    public static Tuple2<String, String> getExtraMetaDataKey(String key) {
        key = key.substring(0, key.length() - len);
        String t1 = ROCKS_VERSION_PREFIX + key + ZERO_STR + DEFAULT_VERSION_ID;

        int num = 0;
        int index = -1;
        for (int i = 0; i < key.length(); i++) {
            if (key.charAt(i) == File.separatorChar) {
                num++;
                if (num == 2) {
                    index = i;
                }
            }
        }

        if (index < 0) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "meta key is invalid");
        }

        String t2 = ROCKS_LIFE_CYCLE_PREFIX + key.substring(0, index) + File.separator +
                DEFAULT_STAMP + key.substring(index) + File.separator + DEFAULT_VERSION_ID;

        return new Tuple2<>(t1, t2);
    }

    public static boolean isMetaJson(byte[] value) {

        try {
            if (value == null || value.length <= 1) {
                return false;
            }
            Json.decodeValue(new String(value), MetaData.class);
            return true;
        } catch (DecodeException e) {
            return false;
        }
    }

    public static InitPartInfo isInitPartJson(byte[] value) {
        try {
            if (value == null || value.length == 0) {
                return null;
            }
            return Json.decodeValue(new String(value), InitPartInfo.class);
        } catch (DecodeException e) {
            return null;
        }
    }

    public static Tuple2<Boolean, Inode> isInodeJson(byte[] value) {
        try {
            if (value == null || value.length == 0) {
                return new Tuple2<>(false, NOT_FOUND_INODE.clone());
            }
            Inode inode = Json.decodeValue(new String(value), Inode.class);
            return new Tuple2<>(true, inode);
        } catch (DecodeException e) {
            return new Tuple2<>(false, ERROR_INODE.clone());
        }
    }

    private static final String[] CUTTING_KEY;

    static {
        Field[] fields = MetaData.class.getFields();
        List<String> keys = new LinkedList<>();

        for (Field field : fields) {
            Cutting annotation = field.getAnnotation(Cutting.class);
            if (annotation != null && annotation.remove()) {
                keys.add(field.getName());
            }
        }

        CUTTING_KEY = keys.toArray(new String[0]);
    }

    /**
     * 减少每次写入元数据的数据量，来减小写放大，从而提升性能
     *
     * @param value
     * @return
     */
    public static byte[] simplifyMetaJson(byte[] value) {
        Map metaData = Json.decodeValue(new String(value), Map.class);

        for (String key : CUTTING_KEY) {
            metaData.remove(key);
        }

        String sysMetaData = (String) metaData.get("sysMetaData");
        if (sysMetaData != null) {
            metaData.put("sysMetaData", simplifySysMeta(sysMetaData));
        }

        return Json.encode(metaData).getBytes();
    }

    public static byte[] simplifyMetaJson(MetaData metaData) {
        return MsObjectMapper.simplifyMetaJson(metaData);
    }

    public static String simplifySysMeta(String sysMeta) {
        try {
            Map map = Json.decodeValue(sysMeta, Map.class);
            map.remove("Content-Type");
            map.remove("Content-Length");
            return Json.encode(map);
        } catch (Exception e) {
            return sysMeta;
        }
    }

    public static String transLifecycleKeyToVersionKey(String lifeCycleKey, boolean hasSnapshotMark) {
        String[] strings = lifeCycleKey.substring(1).split(File.separator);

        int stampIndex = hasSnapshotMark ? 3 : 2;
        StringBuilder result = new StringBuilder("*");
        for (int i = 0; i < strings.length; i++) {
            if (i != stampIndex) {
                if (i == strings.length - 2) {
                    result.append(strings[i]).append(ZERO_STR);
                } else {
                    result.append(strings[i]).append(File.separator);
                }
            }
        }
        return result.substring(0, result.length() - 1);
    }

    public static String transLifecycleKeyToLastKey(String lifeCycleKey, boolean hasSnapshotMark) {
        String[] strings = lifeCycleKey.substring(1).split("/");
        int stampIndex = hasSnapshotMark ? 3 : 2;
        StringBuilder result = new StringBuilder("-");
        for (int i = 0; i < strings.length - 1; i++) {
            if (i != stampIndex) {
                if (i == strings.length - 2) {
                    byte[] bytes = new byte[1];
                    bytes[0] = 0;
                    String ZERO_STR = new String(bytes);
                    result.append(strings[i]).append(ZERO_STR);
                } else {
                    result.append(strings[i]).append(File.separator);
                }
            }
        }
        return result.substring(0, result.length() - 1);
    }

    public static MetaData getLifeMeta(String lifeKey, boolean hasSnapshotMark) {
        int indexPlus = hasSnapshotMark ? 1 : 0;
        //example: +11639/s3b/1661528637306/04-常用软件/CarotDAV//null
        String[] split = lifeKey.split(File.separator, 4 + indexPlus);
        String kv = split[3 + indexPlus];
        String key = kv.substring(0, kv.lastIndexOf(File.separator));
        String versionId = kv.substring(kv.lastIndexOf(File.separator) + 1);
        return new MetaData()
                .setBucket(split[1])
                .setKey(key)
                .setVersionId(versionId)
                .setStamp(split[2 + indexPlus])
                .setDeleteMark(true)
                .setVersionNum("0")
                .setSnapshotMark(hasSnapshotMark ? split[2] : null);
    }

    public static String getPartFileName(StoragePool pool, String bucket, String object, String uploadId, String partNum, String requestId) {
        ImmutableTuple<String, String> tuple = pool.getObjectVnodeId(bucket, object + uploadId + partNum);

        String objVnode = tuple.var1;
        String sha1 = tuple.var2;

        return new Random().nextInt(FILESYSTEM_DIR_NUM) + File.separator + String.join("_",
                new String[]{objVnode,
                        bucket,
                        sha1,
                        uploadId,
                        partNum,
                        requestId});
    }

    private static String rabbitmqKey;

    public static String getMqRocksKey() {
        if (rabbitmqKey == null) {
            synchronized (DEFAULT_STAMP) {
                if (null == rabbitmqKey) {
                    PropertyReader reader = new PropertyReader(RABBITMQ_ENV_CONF_FILE);
                    String rabbitmqPath = reader.getPropertyAsString("RABBITMQ_MNESIA_BASE");
                    rabbitmqKey = rabbitmqPath.replace("mnesia", "");
                    if (System.getProperty("com.macrosan.takeOver") != null) {
                        rabbitmqKey = rabbitmqKey + "/takeOver/";
                        try {
                            new File(rabbitmqKey).mkdirs();
                        } catch (Exception e) {

                        }
                    }
                    try {
                        MSRocksDB.getRocksDB(rabbitmqKey);
                    } catch (Exception e) {
                        log.error("open mq rocks db fail", e);
                        System.exit(1);
                    }
                }
            }
        }

        return rabbitmqKey;
    }

    /**
     * 从配置文件实时读取节点状态
     */
    public static String getRoleState() {
        ReadWriteLock.readLock(true);
        PropertyReader reader = new PropertyReader(PUBLIC_CONF_FILE);
        String state = reader.getPropertyAsString("state");
        ReadWriteLock.unLock(true);
        return state;
    }

    /**
     * 从rocksdb存储的元数据key中提取vnode
     *
     * @return vnode
     */
    public static String getVnode(String key) {
        char[] chars = key.toCharArray();
        boolean flag = false;
        int startIndex = 0;
        int endIndex = 0;
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] >= '0' && chars[i] <= '9') {
                if (!flag) {
                    startIndex = i;
                    flag = true;
                }
            } else {
                if (flag) {
                    endIndex = i;
                    break;
                }
            }
        }
        return key.substring(startIndex, endIndex);
    }

    //syncStamp一般都是第一次生成时写入，不会修改
    public static String metaHash(MetaData metaData) {
        return metaData.syncStamp;
    }

    public static final String DEFAULT_META_HASH = "DEFAULT_META_HASH";

    /**
     * 将key中的快照标记移除---确保不同快照间的同名对象被rocksdb处理时进入同一个队列中
     *
     * @param key              key
     * @param isSnapshotEnable 调用此方法前，请先确认key中是否包含删除标记 判断方法：MetaData中snapshoMark不为空，则key中肯定包含快照标记
     * @return
     */
    public static int getFinalHashCode(String key, boolean isSnapshotEnable) {
        if (StringUtils.isBlank(key)) {
            throw new IllegalArgumentException("key is blank");
        }
        if (!isSnapshotEnable) {
            return key.hashCode();
        }
        String[] split = key.split(File.separator, 4);
        return String.join(File.separator, split[0], split[1], split[3]).hashCode();
    }

    /**
     * 替换key中的快照标记---确保key中已存在快照标记才能进行替换
     * 例如：vnode/bucket/oldSnapshotMark/obj--->  vnode/bucket/newSnapshotMark/obj
     *
     * @param key             key
     * @param newSnapshotMark 快照标记
     * @return 替换后的key
     */
    public static String replaceSnapshotMark(String key, String newSnapshotMark) {
        if (StringUtils.isAnyBlank(key, newSnapshotMark)) {
            throw new IllegalArgumentException("key or newSnapshotMark is blank");
        }
        String[] split = key.split(File.separator, 4);
        return String.join(File.separator, split[0], split[1], newSnapshotMark, split[3]);
    }

    /**
     * 将snapshotMark加入key中--确保key中不存在快照标记才能进行添加
     * 例如：vnode/bucket/obj--->  vnode/bucket/snapshotMark/obj
     *
     * @param key          key
     * @param snapshotMark 需要添加到key中的快照标记
     * @return 添加快照标记后的key
     */
    public static String joinSnapshotMark(String key, String snapshotMark) {
        if (StringUtils.isAnyBlank(key, snapshotMark)) {
            return key;
        }
        String[] split = key.split(File.separator, 3);
        return String.join(File.separator, split[0], split[1], snapshotMark, split[2]);
    }

    /**
     * 获取不带节点信息的磁盘名称
     *
     * @param disk 带节点名称或者不带节点名称的磁盘名称
     * @return 不带节点信息的磁盘名称
     */
    public static String getLunNameByDisk(String disk) {
        if (disk.contains("@")) {
            String[] split = disk.split("@");
            return split.length > 1 ? split[1] : disk;
        }
        return disk;
    }

    public static int longToIntSaturated(long value) {
        if (value > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        if (value < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        return (int) value;
    }

    public static String getEtag(Map<String, String> sysMetaMap) {
        if (sysMetaMap == null) return null;
        if (sysMetaMap.containsKey(COMPRESSION_TYPE)) {
            return '"' + sysMetaMap.get(DECOMPRESSED_ETAG) + '"';
        } else {
            return '"' + sysMetaMap.get(ETAG) + '"';
        }
    }


    public static String getObjectSize(Map<String, String> sysMetaMap, MetaData metaData) {
        if (sysMetaMap.containsKey(COMPRESSION_TYPE)) {
            return sysMetaMap.get(DECOMPRESSED_LENGTH);
        } else {
            return String.valueOf(metaData.endIndex - metaData.startIndex + 1);
        }
    }

    public static long getObjectSize(MetaData metaData) {
        Map<String, String> sysMetaMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
        });
        if (sysMetaMap.containsKey(COMPRESSION_TYPE)) {
            return Long.parseLong(sysMetaMap.get(DECOMPRESSED_LENGTH));
        } else {
            return metaData.endIndex - metaData.startIndex + 1;
        }
    }
}
