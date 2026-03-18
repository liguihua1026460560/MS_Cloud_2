package com.macrosan.component.pojo;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.macrosan.constants.ErrorNo;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.msutils.MsException;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Map;
import java.util.Objects;

/**
 * 单个处理任务的记录，保存在rocksDB，也是发出请求时的模板，可以是Task扫描批量生成，也可以是其他情况生成的单条记录。
 */
@Data
@CompiledJson
@Accessors(chain = true)
public class ComponentRecord {
    public enum Type {
        IMAGE("image-process", "!"),
        VIDEO("video-process", ":"),
        DICOM("dicom-process", "!");

        //url中的标识
        public String name;
        //rocksDB中key的前缀
        public String mark;

        Type(String name, String mark) {
            this.name = name;
            this.mark = mark;
        }

        public static Type parseType(String name) {
            for (Type value : Type.values()) {
                if (value.name.equals(name)) {
                    return value;
                }
            }
            throw new MsException(ErrorNo.NO_SUCH_PROCESS_TYPE, "no such process type: " + name);
        }
    }

    @JsonAttribute
    public String bucket;
    @JsonAttribute
    public String object;
    @JsonAttribute
    public String versionId;
    @JsonAttribute
    public String versionNum;
    @JsonAttribute
    public String rocksKey;
    @JsonAttribute
    public String syncStamp;
    @JsonAttribute
    public String ak;
    @JsonAttribute
    public String sk;
    @JsonAttribute
    public Map<String, String> headerMap;
    @JsonAttribute
    public ComponentStrategy strategy = new ComponentStrategy();
    @JsonAttribute
    public String taskMarker;

    @JsonAttribute
    public String taskName;

    public static final ComponentRecord ERROR_COMPONENT_RECORD = new ComponentRecord().setSyncStamp("error");
    public static final ComponentRecord NOT_FOUND_COMPONENT_RECORD = new ComponentRecord().setSyncStamp("not found");

    /**
     * 同名对象的处理记录也直接覆盖
     */
    public String rocksKey() {
        if (rocksKey == null) {
            Objects.requireNonNull(bucket);
            StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
            String bucketVnode = storagePool.getBucketVnodeId(bucket);
            if (StringUtils.isEmpty(taskMarker)) {
                rocksKey = this.strategy.getType().mark + File.separator + bucketVnode + File.separator + bucket
                        + File.separator + object + File.separator + versionId;
            } else {
                rocksKey = this.strategy.getType().mark + File.separator + bucketVnode + File.separator + bucket + File.separator + taskMarker
                        + File.separator + object + File.separator + versionId;
            }
        }
        return rocksKey;
    }

    public static String rocksKeyPrefix(String bucket, String type, String taskMarker) {
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = storagePool.getBucketVnodeId(bucket);
        if (StringUtils.isEmpty(taskMarker)) {
            return Type.valueOf(type).mark + File.separator + bucketVnode + File.separator + bucket;
        }
        return Type.valueOf(type).mark + File.separator + bucketVnode + File.separator + bucket + File.separator + taskMarker;
    }


}
