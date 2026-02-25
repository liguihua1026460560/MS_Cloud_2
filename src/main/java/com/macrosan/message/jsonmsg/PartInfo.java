package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.macrosan.snapshot.SnapshotAware;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import static com.macrosan.constants.SysConstants.MAX_UPLOAD_PART_NUM;
import static com.macrosan.constants.SysConstants.ROCKS_PART_META_PREFIX;

/**
 * 每个分段的信息在RocksDB的存储格式
 *
 * @author gaozhiyuan
 */
@Data
@CompiledJson
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class PartInfo implements SnapshotAware<PartInfo> {
    @JsonAttribute
    public long partSize;
    @JsonAttribute
    public String etag;
    @JsonProperty(value = "Last-Modified")
    public String lastModified;
    @JsonAttribute
    public String bucket;
    @JsonAttribute
    public String object;
    @JsonAttribute
    public String uploadId;
    @JsonAttribute
    public String partNum;
    @JsonAttribute
    public String versionNum;
    @JsonAttribute
    public String fileName;
    @JsonAttribute
    public String versionId;
    @JsonAttribute
    public boolean delete;
    @JsonAttribute
    public String storage;
    @JsonAttribute
    public String syncStamp;
    @JsonAttribute
    public String deduplicateKey;
    @JsonAttribute
    public long offset;
    @JsonAttribute
    public String snapshotMark;

    @JsonAttribute
    public Set<String> unView;

    @JsonIgnore
    public String initSnapshotMark;

    @JsonAttribute
    //临时字段，不应该写入rocksdb
    public String tmpUpdateQuotaKeyStr;

    public static final PartInfo NO_SUCH_UPLOAD_ID_PART_INFO = new PartInfo().setUploadId("no such upload id");
    public static final PartInfo ERROR_PART_INFO = new PartInfo().setUploadId("error");
    public static final PartInfo NOT_FOUND_PART_INFO = new PartInfo().setUploadId("NOT_FOUND");
    static final int PART_NUM_LEN = String.valueOf(MAX_UPLOAD_PART_NUM).length();

    static public String getPartKey(String vnode, String bucket, String object, String uploadId, String partNum, String currentSnapshotMark) {
        if (StringUtils.isNotBlank(partNum)) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < PART_NUM_LEN - partNum.length(); i++) {
                builder.append('0');
            }

            builder.append(partNum);
            return currentSnapshotMark == null ? ROCKS_PART_META_PREFIX + vnode + File.separator + bucket + File.separator + object + uploadId + builder.toString()
                    : ROCKS_PART_META_PREFIX + vnode + File.separator + bucket + File.separator + currentSnapshotMark + File.separator + object + uploadId + builder.toString();
        } else {
            return currentSnapshotMark == null ? ROCKS_PART_META_PREFIX + vnode + File.separator + bucket + File.separator + object + uploadId
                    : ROCKS_PART_META_PREFIX + vnode + File.separator + bucket + File.separator + currentSnapshotMark + File.separator + object + uploadId;
        }
    }

    public String getPartKey(String vnode) {
        return getPartKey(vnode, bucket, object, uploadId, partNum, snapshotMark);
    }

    public void setPartKey(String key) {

    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PartInfo) {
            PartInfo info = (PartInfo) o;
            boolean res = false;
            if (StringUtils.isNotBlank(fileName)) {
                if (fileName.equals(info.fileName) && uploadId.equals(info.uploadId)) {
                    res = true;
                }
            } else {
                if (uploadId.equals(info.uploadId)) {
                    res = true;
                }
            }
            return res;
        }

        return false;
    }

    @Override
    public int hashCode() {
        return uploadId.hashCode();
    }

    public PartInfo clone(){
        return new PartInfo()
                .setUploadId(uploadId)
                .setBucket(bucket)
                .setObject(object)
                .setPartNum(partNum)
                .setFileName(fileName)
                .setPartSize(partSize)
                .setDelete(delete)
                .setEtag(etag)
                .setStorage(storage)
                .setVersionId(versionId)
                .setOffset(offset)
                .setDeduplicateKey(deduplicateKey)
                .setInitSnapshotMark(initSnapshotMark)
                .setUnView(unView != null ? new HashSet<>(unView) : null)
                .setTmpUpdateQuotaKeyStr(tmpUpdateQuotaKeyStr)
                .setSnapshotMark(snapshotMark)
                .setSyncStamp(syncStamp)
                .setVersionNum(versionNum)
                .setLastModified(lastModified);
    }
}
