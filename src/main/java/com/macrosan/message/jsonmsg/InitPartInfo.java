package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.macrosan.snapshot.SnapshotAware;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Set;

import static com.macrosan.constants.SysConstants.ROCKS_PART_PREFIX;
import static com.macrosan.ec.Utils.ZERO_STR;

/**
 * 分段信息在RocksDB的存储格式
 *
 * @author gaozhiyuan
 */
@Data
@CompiledJson
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class InitPartInfo implements SnapshotAware<InitPartInfo> {
    @JsonAttribute
    public String bucket;
    @JsonAttribute
    public String object;
    @JsonAttribute
    public String uploadId;
    @JsonAttribute
    public String initAccount;
    @JsonAttribute
    public String initAccountName;
    @JsonAttribute
    public String initiated;
    @JsonAttribute
    public boolean delete;
    @JsonAttribute
    public String versionNum;
    @JsonAttribute
    public String storage;
    @JsonAttribute
    public MetaData metaData;
    @JsonAttribute
    public String snapshotMark;

    @JsonAttribute
    public Set<String> unView;

    public static final InitPartInfo NO_SUCH_UPLOAD_ID_INIT_PART_INFO = new InitPartInfo().setUploadId("no such upload id");
    public static final InitPartInfo ERROR_INIT_PART_INFO = new InitPartInfo().setUploadId("error");

    @Override
    public boolean equals(Object o) {
        if (o instanceof InitPartInfo) {
            InitPartInfo info = (InitPartInfo) o;
            return uploadId.equals(info.uploadId);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return uploadId.hashCode();
    }

    public void setPartKey(String partKey) {

    }

    public String getPartKey(String vnode) {
        return getPartKey(vnode, bucket, object, uploadId, snapshotMark);
    }

    public static String getPartKey(String vnode, String bucket, String object, String uploadId, String... currentSnapshotMark) {
        return currentSnapshotMark.length == 0 ? getPartKey(vnode, bucket, object) + ZERO_STR + uploadId
                : getPartKeyPrefix(vnode, bucket, object, currentSnapshotMark[0]) + ZERO_STR + uploadId;
    }

    public static String getPartKey(String vnode, String bucket, String object) {
        return ROCKS_PART_PREFIX + vnode + File.separator + bucket + File.separator + object;
    }

    public static String getPartKeyPrefix(String vnode, String bucket, String object, String currentSnapshotMark) {
        return StringUtils.isBlank(currentSnapshotMark) ? getPartKey(vnode, bucket, object)
                : ROCKS_PART_PREFIX + vnode + File.separator + bucket + File.separator + currentSnapshotMark + File.separator + object;
    }
}
