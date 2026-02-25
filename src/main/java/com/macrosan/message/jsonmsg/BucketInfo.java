package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Objects;

import static com.macrosan.constants.SysConstants.ROCKS_BUCKET_META_PREFIX;

/**
 * 桶容量配额信息
 *
 * @auther wuhaizhong
 * @date 2020/5/28
 */
@Data
@CompiledJson
@Accessors(chain = true)
public class BucketInfo {

    @JsonAttribute
    public String accountName;
    @JsonAttribute
    public String bucketName;
    @JsonAttribute
    public String bucketStorage;
    @JsonAttribute
    public String versionNum;
    @JsonAttribute
    public String objectNum;

    public static final BucketInfo ERROR_BUCKET_INFO = new BucketInfo().setObjectNum("0").setBucketStorage("0").setVersionNum("error");
    public static final BucketInfo NOT_FOUND_BUCKET_INFO = new BucketInfo().setObjectNum("0").setBucketStorage("0").setVersionNum("not found");
    public static final String CAPACITY_SUFFIX = "-capacity";
    public static final String OBJNUM_SUFFIX = "-objNum";

    public static String getBucketKey(String vnode, String bucketName) {
        return ROCKS_BUCKET_META_PREFIX + vnode + File.separator + bucketName;
    }

    public String getBucketKey(String vnode) {
        if (StringUtils.isBlank(bucketName)) {
            throw new IllegalArgumentException("accountName or bucketName can not be blank ");
        }
        return getBucketKey(vnode, bucketName);
    }

    public static String getCpacityKey(String vnode, String bucketName) {
        return getBucketKey(vnode, bucketName) + CAPACITY_SUFFIX;
    }

    public static String getObjNumKey(String vnode, String bucketName) {
        return getBucketKey(vnode, bucketName) + OBJNUM_SUFFIX;
    }

    public static String getTmpObjNumKey(String vnode, String bucketName) {
        return getBucketKey(vnode, bucketName) + "_tmp-objNum";
    }

    public static String getTmpCapacityKey(String vnode, String bucketName) {
        return getBucketKey(vnode, bucketName) + "_tmp-capacity";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        BucketInfo that = (BucketInfo) o;
        return Objects.equals(accountName, that.accountName) &&
                Objects.equals(bucketName, that.bucketName) &&
                Objects.equals(bucketStorage, that.bucketStorage) &&
                Objects.equals(versionNum, that.versionNum) &&
                Objects.equals(objectNum, that.objectNum);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accountName, bucketName, bucketStorage, versionNum);
    }

    @JsonIgnore
    public boolean isAvailable() {
        return !this.equals(ERROR_BUCKET_INFO) && !this.equals(NOT_FOUND_BUCKET_INFO);
    }

}
