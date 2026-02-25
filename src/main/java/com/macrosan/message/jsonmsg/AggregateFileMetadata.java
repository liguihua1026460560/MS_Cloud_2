package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Objects;

/**
 * 聚合文件的元数据
 */
@Data
@CompiledJson
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class AggregateFileMetadata {

    /**
     * 聚合文件所在的存储策略、存储池或者桶
     */
    @JsonAttribute
    public String namespace;

    /**
     * 聚合文件id
     */
    @JsonAttribute
    public String aggregationId;

    /**
     * 聚合文件数据块
     */
    @JsonAttribute
    public String fileName;

    /**
     * 聚合文件所在的存储池
     */
    @JsonAttribute
    public String storage;

    /**
     * 聚合文件删除标记
     */
    @JsonAttribute
    public boolean deleteMark;

    /**
     * 聚合文件版本号
     */
    @JsonAttribute
    public String versionNum;

    /**
     * etag
     */
    @JsonAttribute
    public String etag;

    /**
     * 聚合文件的大小
     */
    @JsonAttribute
    public long fileSize;

    /**
     * 上传时间
     */
    @JsonAttribute
    public String stamp;

    @JsonAttribute
    public String[] segmentKeys;

    @JsonAttribute
    public long[] deltaOffsets;

    /**
     * 记录聚合文件的空洞分布情况
     */
    @JsonAttribute
    public String bitmap;

    public static final AggregateFileMetadata ERROR_AGGREGATION_META = new AggregateFileMetadata().setEtag("error");
    public static final AggregateFileMetadata NOT_FOUND_AGGREGATION_META = new AggregateFileMetadata().setEtag("not found");

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateFileMetadata that = (AggregateFileMetadata) o;
        return deleteMark == that.deleteMark &&
                Objects.equals(aggregationId, that.aggregationId) &&
                Objects.equals(fileName, that.fileName) &&
                Objects.equals(storage, that.storage) &&
                Objects.equals(versionNum, that.versionNum) &&
                Objects.equals(etag, that.etag) &&
                Objects.equals(fileSize, that.fileSize);

    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregationId, fileName, storage, deleteMark, versionNum, etag, fileSize);
    }
}
