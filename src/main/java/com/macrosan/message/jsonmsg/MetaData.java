package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.*;
import com.macrosan.snapshot.SnapshotAware;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author gaozhiyuan
 * rocksdb 中存储对象元数据的格式
 */
@Data
@CompiledJson
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class MetaData implements Cloneable, SnapshotAware<MetaData> {
    @JsonAttribute
    @JsonProperty("0")
    @JsonAlias({"sysMetaData"})
    public String sysMetaData;
    @JsonAttribute
    @Cutting
    @JsonProperty("1")
    @JsonAlias({"userMetaData"})
    public String userMetaData = "{}";
    @JsonAttribute
    @Cutting
    @JsonProperty("2")
    @JsonAlias({"objectAcl"})
    public String objectAcl;
    @JsonAttribute
    @Cutting
    @JsonProperty("3")
    @JsonAlias({"fileName"})
    public String fileName;
    @JsonAttribute
    public long startIndex;
    @JsonAttribute
    @JsonProperty("4")
    @JsonAlias({"endIndex"})
    public long endIndex;
    @JsonAttribute
    public String versionNum;
    @JsonAttribute
    @JsonProperty("6")
    @JsonAlias({"syncStamp"})
    public String syncStamp;
    @JsonAttribute
    @JsonProperty("7")
    @JsonAlias({"shardingStamp"})
    public String shardingStamp;
    @JsonAttribute
    public boolean deleteMark;
    @JsonAttribute
    @Cutting
    public String partUploadId;
    @JsonAttribute
    @Cutting
    public PartInfo[] partInfos;
    @JsonAttribute
    @Cutting
    public boolean smallFile;
    @JsonAttribute
    public String versionId = "null";
    @JsonAttribute
    public String stamp;
    @JsonAttribute
    public boolean deleteMarker;
    @JsonAttribute
    @Cutting
    public String referencedBucket;
    @JsonAttribute
    @Cutting
    public String referencedKey;
    @JsonAttribute
    @Cutting
    public String referencedVersionId = "null";
    @JsonAttribute
    @Cutting
    public boolean latest;
    @JsonAttribute
    @JsonProperty("8")
    @JsonAlias({"storage"})
    public String storage = "";
    @JsonAttribute
    @Cutting
    public long size;
    @JsonAttribute
    public long inode;
    @JsonAttribute
    //临时字段，不应该写入rocksdb
    public String tmpInodeStr;
    @JsonAttribute
    public long cookie;

    /**
     * objName
     */
    @JsonAttribute
    @JsonProperty("9")
    @JsonAlias({"key"})
    public String key;
    @JsonAttribute
    @JsonProperty("a")
    @JsonAlias({"bucket"})
    public String bucket;

    @JsonAttribute
    public String duplicateKey;

    @JsonAttribute
    public String crypto;

    /**
     * 桶散列删除过程中使用，用于标记元数据被删除
     */
    @JsonAttribute
    public boolean discard;

    /**
     * 保存该对象已被哪些策略（图像处理）处理过。
     */
    @JsonAttribute
    @Cutting
    public Set<String> strategySet;
    /**
     * 快照标记--当前对象上传时桶的快照标记
     */
    @JsonAttribute
    public String snapshotMark;

    @JsonAttribute
    public Set<String> unView;

    @JsonAttribute
    public Set<String> weakUnView;

    /**
     * 小文件聚合
     */
    @JsonAttribute
    @Cutting
    public String aggregationKey;

    @JsonAttribute
    public long offset;

    // 聚合文件的大小
    @JsonAttribute
    @Cutting
    public long aggSize;

    public boolean latestMetaIsViewable(String mark) {
        return isViewable(mark) && (getWeakUnView() == null || !getWeakUnView().contains(mark));
    }

    public void addWeakUnViewSnapshotMark(String mark) {
        Set<String> unView = new HashSet<>(1);
        unView.add(mark);
        setWeakUnView(unView);
    }

    public static final MetaData ERROR_META = new MetaData().setPartUploadId("error");
    public static final MetaData NOT_FOUND_META = new MetaData().setPartUploadId("not found");

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MetaData metaData = (MetaData) o;
        return deleteMark == metaData.deleteMark &&
                Objects.equals(partUploadId, metaData.partUploadId) &&
                Objects.equals(key, metaData.key) &&
                Objects.equals(bucket, metaData.bucket) &&
                Objects.equals(objectAcl, metaData.objectAcl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(versionNum, deleteMark, partUploadId, key, bucket);
    }


    @JsonIgnore
    public boolean isAvailable() {
        if (this.equals(ERROR_META) || this.equals(MetaData.NOT_FOUND_META) || this.deleteMark) {
            return false;
        }
        return true;
    }

    @JsonGetter(value = "referencedVersionId")
    public String getJsonReferencedVersionId() {
        if (versionId != null && versionId.equals(referencedVersionId)) {
            return null;
        }

        return referencedVersionId;
    }

    @JsonSetter(value = "referencedVersionId")
    public void setJsonReferencedVersionId(String referencedVersionId) {
        if (referencedVersionId == null) {
            this.referencedVersionId = this.versionId;
        } else {
            this.referencedVersionId = referencedVersionId;
        }
    }

    @JsonGetter(value = "userMetaData")
    public String getJsonUserMetaData() {
        if ("{}".equals(userMetaData)) {
            return null;
        }

        return userMetaData;
    }

    @JsonGetter(value = "versionId")
    public String getJsonVersionId() {
        if ("null".equals(versionId)) {
            return null;
        }

        return versionId;
    }

    @JsonSetter(value = "versionId")
    public void setJsonVersionId(String versionId) {
        this.versionId = versionId;
        if (referencedVersionId == null) {
            referencedKey = versionId;
        }
    }

    @JsonGetter(value = "referencedKey")
    public String getJsonReferencedKey() {
        if (key != null && key.equals(referencedKey)) {
            return null;
        }

        return referencedKey;
    }

    @JsonSetter(value = "referencedKey")
    public void setJsonReferencedKey(String referencedKey) {
        if (referencedKey == null) {
            this.referencedKey = this.key;
        } else {
            this.referencedKey = referencedKey;
        }
    }

    @JsonSetter(value = "key")
    public void setJsonKey(String key) {
        this.key = key;
        if (referencedKey == null) {
            referencedKey = key;
        }
    }

    @JsonGetter(value = "referencedBucket")
    public String getJsonReferencedBucket() {
        if (bucket != null && bucket.equals(referencedBucket)) {
            return null;
        }

        return referencedBucket;
    }

    @JsonSetter(value = "referencedBucket")
    public void setJsonReferencedBucket(String referencedBucket) {
        if (referencedKey == null) {
            this.referencedBucket = this.bucket;
        } else {
            this.referencedBucket = referencedBucket;
        }
    }

    @JsonSetter(value = "bucket")
    public void setJsonBucket(String bucket) {
        this.bucket = bucket;
        if (referencedBucket == null) {
            referencedBucket = bucket;
        }
    }

    @JsonSetter(value = "storage")
    public void setJsonStorage(String storage) {
        if (storage == null) {
            this.storage = "";
        } else {
            this.storage = storage;
        }
    }

    @Override
    public MetaData clone() {
        try {
            MetaData clone = (MetaData) super.clone();
            // TODO: copy mutable state here, so the clone can't change the internals of the original
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}


