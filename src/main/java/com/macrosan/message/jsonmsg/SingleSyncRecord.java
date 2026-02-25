package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Objects;

import static com.macrosan.constants.SysConstants.ROCKS_SINGLESYNC_KEY;

/**
 * 双活请求有一个站点出错另一个站点成功时记录，用以读请求的转发
 *
 * @author fanjunxi
 */
@Data
@CompiledJson
@Accessors(chain = true)
public class SingleSyncRecord {
    @JsonAttribute
    public String rocksKey;
    @JsonAttribute
    public Integer index;
    @JsonAttribute
    public Integer successIndex;
    @JsonAttribute
    public String bucket;
    @JsonAttribute
    public String object;
    @JsonAttribute
    public String syncStamp;
    @JsonAttribute
    public String uri;
    @JsonAttribute
    public String method;
    /**
     * 用于更新record时比较使用。
     */
    @JsonAttribute
    public String versionNum;
    /**
     * 对象的版本号，用于多版本。
     */
    @JsonAttribute
    public String versioinId;

    public enum SSRecordType {
        OBJECT,
        PART,
        // 收到读请求前使用，表示需要汇总所有站点的数据，如ls
        DUAL,
        // POST/PUT相关请求生成SSRecord Type时使用，表示该请求不会生成SSRecord。
        DISCARD,
        NONE
    }

    public static SSRecordType getSSRecordType(UnSynchronizedRecord.Type type) {
        switch (type) {
            case ERROR_INIT_PART_UPLOAD:
            case ERROR_PART_UPLOAD:
            case ERROR_PART_ABORT:
//            case ERROR_COMPLETE_PART:
                return SSRecordType.PART;
            case ERROR_PUT_OBJECT_ACL:
            case ERROR_PUT_OBJECT_TAG:
            case ERROR_DEL_OBJECT_TAG:
                return SSRecordType.DISCARD;
            case NONE:
                return SSRecordType.NONE;
            default:
                return SSRecordType.OBJECT;
        }

    }

    /**
     * 根据PUT/POST请求类型，决定生成的SSRecord类型。
     */
    public static SSRecordType getSSRecordType(String uri, String method) {
        return getSSRecordType(UnSynchronizedRecord.type(uri, method));
    }

    public String rocksKey() {
        if (StringUtils.isNotBlank(rocksKey)) {
            return rocksKey;
        } else {
            Objects.requireNonNull(bucket);
            rocksKey = getRecordKey(index, bucket, object, versioinId, uri, method, syncStamp);
            return rocksKey;
        }
    }

    public static String getRecordKey(int index, String bucket, String object, String versioinId, String uri, String method, String syncStamp) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);
        return ROCKS_SINGLESYNC_KEY + "single" + File.separator + String.format("%04d", index) + File.separator
                + bucketVnode + File.separator + bucket + File.separator + object + File.separator + versioinId + File.separator + getSSRecordType(uri, method) + File.separator + syncStamp;
    }

    public static String getRecordPrefix(int index) {
        return ROCKS_SINGLESYNC_KEY + "single" + File.separator + String.format("%04d", index);
    }

    public static final SingleSyncRecord NOT_FOUND_SS_RECORD = new SingleSyncRecord().setSyncStamp("not found");
    public static final SingleSyncRecord ERROR_SS_RECORD = new SingleSyncRecord().setSyncStamp("error");

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SingleSyncRecord record = (SingleSyncRecord) o;
        return Objects.equals(bucket, record.bucket) &&
                Objects.equals(object, record.object) &&
                Objects.equals(versioinId, record.versioinId) &&
//                Objects.equals(uri, record.uri) &&
                Objects.equals(syncStamp, record.syncStamp) &&
                Objects.equals(index, record.index) &&
                Objects.equals(method, record.method);
    }

    @Override
    public int hashCode() {
        return Objects.hash(syncStamp, uri, object, bucket);
    }

    @JsonIgnore
    public boolean isAvailable() {
        return !this.equals(NOT_FOUND_SS_RECORD) &&
                !this.equals(ERROR_SS_RECORD);
    }

}
