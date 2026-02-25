package com.macrosan.message.jsonmsg;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.macrosan.filesystem.quota.FSQuotaConstants;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.File;

import static com.macrosan.constants.SysConstants.ROCKS_BUCKET_META_PREFIX;
import static com.macrosan.filesystem.cifs.types.smb2.FsQuotaInfo.FILE_VC_QUOTA_ENFORCE;
import static com.macrosan.filesystem.cifs.types.smb2.FsQuotaInfo.FILE_VC_QUOTA_TRACK;
import static com.macrosan.message.jsonmsg.BucketInfo.CAPACITY_SUFFIX;
import static com.macrosan.message.jsonmsg.BucketInfo.OBJNUM_SUFFIX;

@Data
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class FSQuotaConfig {

    /**
     * 桶名
     */
    public String bucket;

    /**
     * 目录名
     */
    public String dirName;

    /**
     * 目录nodeId
     */
    public long nodeId;

    /**
     * 配额类型：
     * 1：用户配额
     * 2：组配额
     * 3:目录配额
     */
    public Integer quotaType;

    /**
     * 文件数软配额
     */
    public Long filesSoftQuota;

    /**
     * 文件数硬配额
     */
    public Long filesHardQuota;

    /**
     * 容量软配额
     */
    public Long capacitySoftQuota;

    /**
     * 容量硬配额
     */
    public Long capacityHardQuota;

    /**
     * s3账户名
     */
    public String s3AccountName;

    /**
     * 用户id
     */
    public int uid;

    /**
     * 组id
     */
    public int gid;

    /**
     * cifs user，参考MOFS，需创建配额之后进行绑定
     */
    public String cifsUserId;

    /**
     * 即创建配额的时间
     */
    public long startTime;

    /**
     * 修改配额时间
     */
    public long modifyTime;

    /**
     * 容量宽限时间周期
     */
    public int blockTimeLeft;

    /**
     * 文件数宽限时间周期
     */
    public int filesTimeLeft;

    /**
     * cifs quota default flags
     */
    public int cifsQuotaFlags = FILE_VC_QUOTA_ENFORCE | FILE_VC_QUOTA_TRACK;


    /**
     * 是否是修改请求
     */
    public boolean modify = false;

    public static String getCapKey(String vnode, String bucketName, long dirNodeId, int type, int id) {
        switch (type) {
            case FSQuotaConstants.FS_DIR_QUOTA:
                return getDirCapKey(vnode, bucketName, dirNodeId);
            case FSQuotaConstants.FS_USER_QUOTA:
                return getUserCapKey(vnode, bucketName, dirNodeId, id);
            case FSQuotaConstants.FS_GROUP_QUOTA:
                return getGroupCapKey(vnode, bucketName, dirNodeId, id);
            default:
                return "";
        }
    }

    public static String getNumKey(String vnode, String bucketName, long dirNodeId, int type, int id) {
        switch (type) {
            case FSQuotaConstants.FS_DIR_QUOTA:
                return getDirFileNumKey(vnode, bucketName, dirNodeId);
            case FSQuotaConstants.FS_USER_QUOTA:
                return getUserFileNumKey(vnode, bucketName, dirNodeId, id);
            case FSQuotaConstants.FS_GROUP_QUOTA:
                return getGroupFileNumKey(vnode, bucketName, dirNodeId, id);
            default:
                return "";
        }
    }

    public static String getDirCapKey(String vnode, String bucketName, long dirNodeId) {
        return getQuotaKey(vnode, bucketName, dirNodeId) + CAPACITY_SUFFIX;
    }

    public static String getUserCapKey(String vnode, String bucketName, long dirNodeId, int uid) {
        return getQuotaKey(vnode, bucketName, dirNodeId) + "user-" + uid + CAPACITY_SUFFIX;
    }

    public static String getGroupCapKey(String vnode, String bucketName, long dirNodeId, int gid) {
        return getQuotaKey(vnode, bucketName, dirNodeId) + "group-" + gid + CAPACITY_SUFFIX;
    }

    public static String getQuotaKey(String vnode, String bucketName, long dirNodeId) {
        return ROCKS_BUCKET_META_PREFIX + vnode + File.separator + bucketName + File.separator + dirNodeId + "/";
    }

    public static String getDirFileNumKey(String vnode, String bucketName, long dirNodeId) {
        return getQuotaKey(vnode, bucketName, dirNodeId) + OBJNUM_SUFFIX;
    }

    public static String getUserFileNumKey(String vnode, String bucketName, long dirNodeId, int uid) {
        return getQuotaKey(vnode, bucketName, dirNodeId) + "user-" + uid + OBJNUM_SUFFIX;
    }

    public static String getGroupFileNumKey(String vnode, String bucketName, long dirNodeId, int gid) {
        return getQuotaKey(vnode, bucketName, dirNodeId) + "group-" + gid + OBJNUM_SUFFIX;
    }

}
