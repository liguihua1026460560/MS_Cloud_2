package com.macrosan.message.xmlmsg;


import com.macrosan.message.jsonmsg.FSQuotaConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "FsQuotaConfigAndUsedResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "FsQuotaConfigAndUsedResult", propOrder = {
        "bucket",
        "dirName",
        "quotaType",
        "filesSoftQuota",
        "filesHardQuota",
        "capacitySoftQuota",
        "capacityHardQuota",
        "uid",
        "gid",
        "startTime",
        "modifyTime",
        "usedCapacity",
        "usedFiles"
})
public class FsQuotaConfigAndUsedResult {
    /**
     * 桶名
     */
    @XmlElement(name = "Bucket", required = true)
    public String bucket;

    /**
     * 目录名
     */
    @XmlElement(name = "DirName", required = true)
    public String dirName;

    /**
     * 配额类型：
     * <p>
     * 0：用户配额
     * 1：组配额
     * 3:目录配额
     */
    @XmlElement(name = "QuotaType", required = true)
    public Integer quotaType;

    /**
     * 文件数软配额
     */
    @XmlElement(name = "FilesSoftQuota", required = true)
    public Long filesSoftQuota;

    /**
     * 文件数硬配额
     */
    @XmlElement(name = "FilesHardQuota", required = true)
    public Long filesHardQuota;

    /**
     * 容量软配额
     */
    @XmlElement(name = "CapacitySoftQuota", required = true)
    public Long capacitySoftQuota;

    /**
     * 容量硬配额
     */
    @XmlElement(name = "CapacityHardQuota", required = true)
    public Long capacityHardQuota;

    /**
     * 用户id
     */
    @XmlElement(name = "Uid", required = true)
    public int uid;

    /**
     * 组id
     */
    @XmlElement(name = "Gid", required = true)
    public int gid;

    /**
     * 设置的时间 时间戳 System.currentTimeMillis()
     */
    @XmlElement(name = "StartTime", required = true)
    public long startTime;

    @XmlElement(name = "ModifyTime", required = true)
    public long modifyTime;

    /**
     * 已使用的容量
     */
    @XmlElement(name = "UsedCapacity", required = true)
    public long usedCapacity;

    /**
     * 已使用的文件数
     */
    @XmlElement(name = "UsedFiles", required = true)
    public long usedFiles;

    public static FsQuotaConfigAndUsedResult mapToFsQuotaConfigAndUsedResult(FSQuotaConfig fsQuotaConfig) {
        long modifyTime = fsQuotaConfig.getModifyTime();
        if (modifyTime == 0) {
            modifyTime = fsQuotaConfig.getStartTime();
        }
        return new FsQuotaConfigAndUsedResult()
                .setBucket(fsQuotaConfig.getBucket())
                .setDirName(fsQuotaConfig.getDirName())
                .setQuotaType(fsQuotaConfig.getQuotaType())
                .setCapacityHardQuota(fsQuotaConfig.getCapacityHardQuota())
                .setCapacitySoftQuota(fsQuotaConfig.getCapacitySoftQuota())
                .setFilesHardQuota(fsQuotaConfig.getFilesHardQuota())
                .setFilesSoftQuota(fsQuotaConfig.getFilesSoftQuota())
                .setGid(fsQuotaConfig.getGid())
                .setUid(fsQuotaConfig.getUid())
                .setModifyTime(modifyTime)
                .setStartTime(fsQuotaConfig.getStartTime());
    }
}
