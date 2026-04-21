package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.ec.VersionUtil;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import io.vertx.core.http.HttpMethod;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static com.macrosan.constants.ServerConstants.INCLUDE_PARAM_LIST;
import static com.macrosan.constants.SysConstants.ROCKS_UNSYNCHRONIZED_KEY;
import static com.macrosan.database.rocksdb.MSRocksDB.UnsyncRecordDir;
import static com.macrosan.doubleActive.DoubleActiveUtil.getOtherSiteIndex;
import static com.macrosan.doubleActive.arbitration.DAVersionUtils.*;
import static com.macrosan.httpserver.MossHttpClient.*;
import static com.macrosan.message.jsonmsg.UnSynchronizedRecord.Type.*;

/**
 * @auther wuhaizhong
 * @date 2021/4/6
 */
@Data
@CompiledJson
@Accessors(chain = true)
public class UnSynchronizedRecord implements Cloneable {

    @JsonIgnore
    public static Map<String, Type> signTypeMap = new HashMap<>(10);
    /**
     * 本地执行的请求类型
     */
    @JsonIgnore
    public static Set<String> localExecutionSignTypeSet = new HashSet<>();

    static {
        signTypeMap.put("DELETE?uploadId", ERROR_PART_ABORT);
        signTypeMap.put("DELETE", ERROR_DELETE_OBJECT);
        signTypeMap.put("DELETE?versionId", ERROR_DELETE_OBJECT);
        signTypeMap.put("POST?uploads", ERROR_INIT_PART_UPLOAD);
        signTypeMap.put("POST?uploadId", ERROR_COMPLETE_PART);
        signTypeMap.put("PUT", ERROR_PUT_OBJECT);
        signTypeMap.put("PUT?acl", ERROR_PUT_OBJECT_ACL);
        signTypeMap.put("PUT?acl&versionId", ERROR_PUT_OBJECT_ACL);
        signTypeMap.put("PUT?tagging", ERROR_PUT_OBJECT_TAG);
        signTypeMap.put("PUT?tagging&versionId", ERROR_PUT_OBJECT_TAG);
        signTypeMap.put("DELETE?tagging", ERROR_DEL_OBJECT_TAG);
        signTypeMap.put("DELETE?tagging&versionId", ERROR_DEL_OBJECT_TAG);
        signTypeMap.put("PUT?retention", ERROR_PUT_OBJECT_WORM);
        signTypeMap.put("PUT?retention&versionId", ERROR_PUT_OBJECT_WORM);
        signTypeMap.put("PUT?partNumber&uploadId", ERROR_PART_UPLOAD);
        signTypeMap.put("PUT?syncHistory", ERROR_SYNC_HIS_OBJECT);
        signTypeMap.put("POST?delete", ERROR_SYNC_DEL_OBJECTS);
        signTypeMap.put("POST?append&position", ERROR_APPEND_OBJECT);
        signTypeMap.put("PUT?createBucket", ERROR_PUT_BUCKET);

        localExecutionSignTypeSet.add("PUT/?componentStrategy&strategyName");
        localExecutionSignTypeSet.add("GET/?componentStrategies");
        localExecutionSignTypeSet.add("GET/?componentStrategy&strategyName");
        localExecutionSignTypeSet.add("DELETE/?componentStrategy&strategyName");
        localExecutionSignTypeSet.add("PUT/?componentTask&strategyName&taskName");
        localExecutionSignTypeSet.add("GET/?componentTasks");
        localExecutionSignTypeSet.add("GET/?componentTask&taskName");
        localExecutionSignTypeSet.add("DELETE/?componentTask&taskName");
        localExecutionSignTypeSet.add("GET/?componentErrors&taskName");
        localExecutionSignTypeSet.add("POST/?componentErrors&taskName");
        localExecutionSignTypeSet.add("GET//?componentErrors");
        localExecutionSignTypeSet.add("POST//?componentErrors");
        localExecutionSignTypeSet.add("PUT///?componentStrategy");
        localExecutionSignTypeSet.add("PUT///?componentStrategy&versionId");
        localExecutionSignTypeSet.add("GET///?componentStrategy");
        localExecutionSignTypeSet.add("GET///?componentStrategy&versionId");
    }

    public enum Type {
        ERROR_PUT_OBJECT(true),
        ERROR_PUT_OBJECT_VERSION(false),
        ERROR_PUT_OBJECT_ACL(true),
        ERROR_PUT_OBJECT_TAG(true),
        ERROR_DEL_OBJECT_TAG(true),
        ERROR_PUT_OBJECT_WORM(true),
        ERROR_INIT_PART_UPLOAD(false),
        ERROR_PART_ABORT(true),
        ERROR_PART_UPLOAD(false),
        ERROR_COMPLETE_PART(false),
        ERROR_DELETE_OBJECT(true),
        ERROR_COPY_OBJECT(false),
        ERROR_SYNC_HIS_OBJECT(false),
        ERROR_SYNC_DEL_OBJECTS(true),
        AFTER_INIT_PART(false),
        NONE(false),
        ERROR_APPEND_OBJECT(false),
        ERROR_PUT_BUCKET(false),
        FS_RECORD(false);

        /**
         * 是否需要获取当前对象stamp
         **/
        public boolean useCurrStamp;

        Type(boolean useCurrStamp) {
            this.useCurrStamp = useCurrStamp;
        }
    }

    @JsonIgnore
    public static final String SYNC_RECORD_BUCKET = "syncBucket";

    /**
     * 该异常记录属于第几个站点
     */
    @JsonAttribute
    @JsonProperty("1")
    @JsonAlias({"index"})
    public Integer index;
    @JsonAttribute
    @JsonProperty("2")
    @JsonAlias({"successIndex"})
    public Integer successIndex;
    @JsonAttribute
    @JsonProperty("3")
    @JsonAlias({"headers"})
    public Map<String, String> headers;
    /**
     * 因为是本地的versionNum，不能用于多站点间数据的比较。
     * 只在buildUnsyncRecord时调用setVersion，用于uncommited的超时处理
     */
    @JsonAttribute
    public String versionNum;
    /**
     * 该条记录的版本号
     */
    @JsonAttribute
    @JsonProperty("4")
    @JsonAlias({"syncStamp"})
    public String syncStamp;
    @JsonAttribute
    @JsonProperty("5")
    @JsonAlias({"deleteMark"})
    public boolean deleteMark;
    @JsonAttribute
    @JsonProperty("6")
    @JsonAlias({"uri"})
    public String uri;
    @JsonAttribute
    @JsonProperty("7")
    @JsonAlias({"method"})
    public HttpMethod method;
    @JsonAttribute
    @JsonProperty("8")
    @JsonAlias({"bucket"})
    public String bucket;
    @JsonAttribute
    @JsonProperty("9")
    @JsonAlias({"object"})
    public String object;
    @JsonAttribute
    @JsonProperty("a")
    @JsonAlias({"versionId"})
    public String versionId;
    @JsonAttribute
    @JsonProperty("b")
    @JsonAlias({"successIndexSet"})
    public Set<Integer> successIndexSet;

    /**
     * 已提交状态标记。false表示该记录为预提交状态。在请求被发送往相应接口之前会写一个预提交记录。
     * 已提交记录会被扫描并处理。100s内的预提交记录不会被扫描处理。
     * 异步复制是在客户端请求完成后将commited设为已提交；双活是在有部分节点请求失败时将commited设为已提交。
     **/
    @JsonAttribute
    @JsonProperty("c")
    @JsonAlias({"commited"})
    public boolean commited;
    /**
     * 对端同步标记，表示该记录将由对端处理
     **/
    @JsonAttribute
    @JsonProperty("d")
    @JsonAlias({"syncFlag"})
    public boolean syncFlag;
    @JsonAttribute
    @JsonProperty("e")
    @JsonAlias({"recordKey"})
    public String recordKey;
    /**
     * put覆盖已有对象时生成（值为metadata.syncStamp），表示该对象相关的覆盖、删除、合并的unsyncRecord都不生效（从rocksDB删除）
     */
    @JsonAttribute
    @JsonProperty("f")
    @JsonAlias({"lastStamp"})
    public String lastStamp;
    @JsonAttribute
    @JsonProperty("g")
    @JsonAlias({"opt"})
    public Integer opt;
    @JsonAttribute
    @JsonProperty("h")
    @JsonAlias({"nodeId"})
    public Long nodeId;

    public UnSynchronizedRecord() {
    }

    /**
     * 文件专用
     */
    public UnSynchronizedRecord(String bucket, int opt, String syncStamp, long nodeId, String object) {
        this.bucket = bucket;
        this.opt = opt;
        this.syncStamp = syncStamp;
        this.nodeId = nodeId;
        this.versionNum = VersionUtil.getVersionNum(false);
        this.headers = new HashMap<>();
        this.index = getOtherSiteIndex();
        this.successIndex = LOCAL_CLUSTER_INDEX;
        this.commited = false;
        // rocksKey生成使用
        this.uri = File.separator + nodeId;
        // 兼容差异记录扫描流程中的一些标识，record.object不能为空。
        // 作为差异记录处理顺序的依据。非同名对象且inode不相同的差异记录才可以并发处理。
        this.object = object == null ? "" : object;
    }

    public boolean isFSUnsyncRecord() {
        return opt != null;
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return new UnSynchronizedRecord();
    }


    public String rocksKey() {
        if (StringUtils.isNotBlank(recordKey)) {
            return recordKey;
        } else {
            Objects.requireNonNull(bucket);
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
            String bucketVnode = storagePool.getBucketVnodeId(bucket);
            recordKey = ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir + File.separator + syncStamp + File.separator + type() + uri;
            return recordKey;
        }
    }

    public void delRocksKey(String DaVersion) {
        if (!StringUtils.isNotBlank(recordKey)) {
            Objects.requireNonNull(bucket);
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
            String bucketVnode = storagePool.getBucketVnodeId(bucket);
            recordKey = ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + "local" + File.separator + DaVersion + File.separator + type() + uri;
        }
    }

    public String updateRocksKey(String syncStamp, Type type) {
        Objects.requireNonNull(bucket);
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);
        String recordRocksKey = "";
        if (ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(index)) {
            recordRocksKey = ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir
                    + File.separator + "async" + File.separator + index + File.separator + syncStamp + File.separator + type + uri;
        } else {
            recordRocksKey = ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir + File.separator + syncStamp + File.separator + type + uri;
        }
        this.recordKey = recordRocksKey;
        return recordRocksKey;
    }

    public void hisRocksKey(String DaVersion) {
        if (!StringUtils.isNotBlank(recordKey)) {
            Objects.requireNonNull(bucket);
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
            String bucketVnode = storagePool.getBucketVnodeId(bucket);
            recordKey = ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir + File.separator + DaVersion + File.separator + type() + uri;
        }
    }

    public void hisRocksKeyAsync(int index, String DaVersion) {
        if (!StringUtils.isNotBlank(recordKey)) {
            Objects.requireNonNull(bucket);
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
            String bucketVnode = storagePool.getBucketVnodeId(bucket);
            recordKey = ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir + File.separator + "async" + File.separator + index
                    + File.separator + DaVersion + File.separator + type() + uri;
        }
    }

    // 历史数据同步时，如果对象是部署多站点前上传，生成rocksKey时将metaData.versionNum转为多站点的syncStamp格式，方便查找
    public static String versionNum2syncStamp(String versionNum) {
        int i = countHyphen(versionNum);
        if (i == 2) {
            return versionNum;
        } else if (i == 1) {
            return getDaTermStr(0L) + "-" + versionNum + formatedHostUid + "-" + 100_0000L;
        } else {
            throw new RuntimeException("versionNum format err, " + versionNum);
        }
    }

    public String getReWriteCompleteKey(String partUploadId) {
        String newUri = uri + "?uploadId=" + partUploadId;
        StringBuilder stringBuilder = new StringBuilder();
        int index = 0;
        if (recordKey.contains(File.separator + "async" + File.separator)) {
            index = 5;
        } else {
            index = 3;
        }
        String[] split = recordKey.split(File.separator);
        for (int i = 0; i < index + 1; i++) {
            stringBuilder.append(split[i]);
            if (i == index) {
                stringBuilder.append("1");
            }
            stringBuilder.append(File.separatorChar);
        }
        stringBuilder.append(ERROR_COMPLETE_PART).append(newUri);
        return stringBuilder.toString();
    }

    public String getRecordRocksKey(String syncStamp) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);
        String recordRocksKey;
        if (IS_THREE_SYNC) {
            if (LOCAL_CLUSTER_INDEX.equals(THREE_SYNC_INDEX)) {
                recordRocksKey = ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir
                        + File.separator + "async" + File.separator + index + File.separator + syncStamp + File.separator + type() + uri;
            } else {
                if (THREE_SYNC_INDEX.equals(index)) {
                    recordRocksKey = ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir
                            + File.separator + "async" + File.separator + index + File.separator + syncStamp + File.separator + type() + uri;
                } else {
                    recordRocksKey = ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir + File.separator + syncStamp + File.separator + type() + uri;
                }
            }
        } else {
            if (ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(index)) {
                recordRocksKey = ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir
                        + File.separator + "async" + File.separator + index + File.separator + syncStamp + File.separator + type() + uri;
            } else {
                recordRocksKey = ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir + File.separator + syncStamp + File.separator + type() + uri;
            }
        }
        this.recordKey = recordRocksKey;
        return recordRocksKey;
    }

    public String rocksKeyAsync(int asyncIndex) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);
        String rocksKeyAsync = ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir + File.separator + "async" + File.separator + asyncIndex
                + File.separator + syncStamp + File.separator + type() + uri;
        this.recordKey = rocksKeyAsync;
        return rocksKeyAsync;
    }

    public static String getRocksKeyAsync(String daRocksKey, int asyncIndex) {
        if (isOldPath(daRocksKey)) {
            String[] split = daRocksKey.split(File.separator, 3);
            return split[0] + File.separator + split[1] + File.separator + "async" + File.separator + asyncIndex + File.separator + split[2];
        } else {
            String[] split = daRocksKey.split(File.separator, 4);
            return split[0] + File.separator + split[1] + File.separator + split[2] + File.separator + "async" + File.separator + asyncIndex + File.separator + split[3];
        }
    }

    public static String getRecorderPrefixAsync(String bucket, int asyncIndex, boolean metaRocksScan) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);

        if (IS_THREE_SYNC) {
            if (LOCAL_CLUSTER_INDEX.equals(THREE_SYNC_INDEX)) {
                if (!metaRocksScan) {
                    return ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir
                            + File.separator + "async" + File.separator + asyncIndex;
                } else {
                    return ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket
                            + File.separator + "async" + File.separator + asyncIndex;
                }
            } else {
                if (THREE_SYNC_INDEX.equals(asyncIndex)) {
                    if (!metaRocksScan) {
                        return ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir
                                + File.separator + "async" + File.separator + asyncIndex;
                    } else {
                        return ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket
                                + File.separator + "async" + File.separator + asyncIndex;
                    }
                } else {
                    if (!metaRocksScan) {
                        return ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir;
                    } else {
                        return ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket;
                    }
                }
            }
        }

        if (ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(asyncIndex)) {
            if (!metaRocksScan) {
                return ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir
                        + File.separator + "async" + File.separator + asyncIndex;
            } else {
                return ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket
                        + File.separator + "async" + File.separator + asyncIndex;
            }
        }
        if (!metaRocksScan) {
            return ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket + File.separator + UnsyncRecordDir;
        } else {
            return ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket;
        }
    }

    public static String getRecorderPrefixLocal(String bucket) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);
        return ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket
                + File.separator + "local";
    }

    public static String getOnlyDeletePrefix(String bucket) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);
        return ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket;
    }

    public static boolean isOldPath(String rocksKey) {
        String[] split = rocksKey.split(File.separator);
        return !UnsyncRecordDir.equals(split[2]) && !"local".equals(split[2]);
    }

    public static boolean isAsyncRecord(String rocksKey) {
        if (!isOldPath(rocksKey)) {
            return "async".equals(rocksKey.split("/")[3]);
        } else {
            return "async".equals(rocksKey.split("/")[2]);
        }
    }

    public Type type() {
        if (isFSUnsyncRecord()) {
            return FS_RECORD;
        }
        StringBuilder stringBuilder = new StringBuilder(10);
        stringBuilder.append(method);
        String pagrams = Arrays.stream(StringUtils.substringAfter(uri, "?").split("&"))
                .map(s -> s.split("=")[0])
                .filter(s -> INCLUDE_PARAM_LIST.contains(s.hashCode()) || "syncHistory".hashCode() == s.hashCode())
                .sorted().collect(Collectors.joining("&"));
        if (StringUtils.isNotEmpty(pagrams)) {
            stringBuilder.append("?").append(pagrams);
        }
        return signTypeMap.getOrDefault(stringBuilder.toString(), NONE);
    }

    public static Type type(String uri, String method) {
        StringBuilder stringBuilder = new StringBuilder(10);
        stringBuilder.append(method);
        String pagrams = Arrays.stream(StringUtils.substringAfter(uri, "?").split("&"))
                .map(s -> s.split("=")[0])
                .filter(s -> INCLUDE_PARAM_LIST.contains(s.hashCode()) || "syncHistory".hashCode() == s.hashCode())
                .sorted().collect(Collectors.joining("&"));
        if (StringUtils.isNotEmpty(pagrams)) {
            stringBuilder.append("?").append(pagrams);
        }
        return signTypeMap.get(stringBuilder.toString());
    }

    public static UnSynchronizedRecord SIGNAL_RECORD = new UnSynchronizedRecord().setRecordKey("signal");

    public static boolean isSignal(UnSynchronizedRecord record) {
        return "signal".equals(record.rocksKey());
    }

    public String afterInitRecordKey() {
        String uploadId = Objects.requireNonNull(DoubleActiveUtil.getOringinUploadId(this));
        Objects.requireNonNull(bucket);
        Objects.requireNonNull(object);
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);
        return ROCKS_UNSYNCHRONIZED_KEY + bucketVnode + File.separator + bucket
                + File.separator + UnsyncRecordDir
                + File.separator + index
                + File.separator + AFTER_INIT_PART.name()
                + File.separator + object + File.separator + uploadId;
    }

    public static final UnSynchronizedRecord NOT_FOUND_UNSYNC_RECORD = new UnSynchronizedRecord().setRecordKey("not found");
    public static final UnSynchronizedRecord ERROR_UNSYNC_RECORD = new UnSynchronizedRecord().setRecordKey("error");

    @Override
    public boolean equals(Object o) {
        if (o instanceof UnSynchronizedRecord) {
            UnSynchronizedRecord record = (UnSynchronizedRecord) o;
            return recordKey.equals(record.recordKey);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return recordKey.hashCode();
    }

}
