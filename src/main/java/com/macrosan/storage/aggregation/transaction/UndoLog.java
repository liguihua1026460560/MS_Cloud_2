package com.macrosan.storage.aggregation.transaction;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.macrosan.storage.StoragePoolFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.File;
import java.util.Stack;

import static com.macrosan.constants.SysConstants.ROCKS_AGGREGATION_UNDO_LOG_PREFIX;

@Data
@CompiledJson
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class UndoLog {
    @JsonAttribute
    public String namespace;
    @JsonAttribute
    public String aggregateId; // 事务ID, 全局唯一，此处选择使用聚合ID
    @JsonAttribute
    public int operatorType; // 操作类型，0：FLUSH，1：PUT_META，2：UPDATE
    @JsonAttribute
    public long timestamp;
    @JsonAttribute
    public String content;
    @JsonAttribute
    public String versionNum;
    @JsonAttribute
    public boolean committed;

    public String rocksKey() {
        String vnodeId = StoragePoolFactory.getMetaStoragePool("").getBucketVnodeId(namespace);
        String[] strings = aggregateId.split("-");
        String node = strings[strings.length - 1];
        return ROCKS_AGGREGATION_UNDO_LOG_PREFIX + vnodeId + File.separator + namespace + File.separator + node + File.separator
                + timestamp + File.separator + aggregateId;
    }

    public static String rocksKeyPrefix(String namespace, String nodeUuid) {
        String vnodeId = StoragePoolFactory.getMetaStoragePool("").getBucketVnodeId(namespace);
        return ROCKS_AGGREGATION_UNDO_LOG_PREFIX
                + vnodeId
                + File.separator + namespace
                + File.separator + nodeUuid;
    }

    public static String rocksKeyPrefix(String namespace, String nodeUuid, String startStamp) {
        String vnodeId = StoragePoolFactory.getMetaStoragePool("").getBucketVnodeId(namespace);
        return ROCKS_AGGREGATION_UNDO_LOG_PREFIX
                + vnodeId
                + File.separator + namespace
                + File.separator + nodeUuid
                + File.separator + startStamp;
    }
}
