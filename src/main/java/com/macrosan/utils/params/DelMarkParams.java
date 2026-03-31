package com.macrosan.utils.params;

import com.macrosan.ec.error.ErrorConstant;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.utils.functional.Tuple3;
import lombok.*;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_MARK_DELETE_META;

@Data
public class DelMarkParams {
    // 桶名
    public String bucket;
    // 对象名
    public String object;
    // 版本号
    public String versionId;
    // 需要被至为deleteMark的元数据
    public MetaData pendingMark;
    // 对象元数据的位置
    public List<Tuple3<String, String, String>> vnodeList;

    // 桶是否启用了多版本
    public String versionStatus;
    // 新删除标记的versionNum
    public String versionNum;

    @Getter
    @Setter
    public MsHttpRequest request;

    @Getter
    @Setter
    public boolean isMigrate;

    @Getter
    @Setter
    public ErrorConstant.ECErrorType type;

    @Getter
    @Setter
    public long inodeId;

    public boolean needDeleteInode;

    @Getter
    @Setter
    public String snapshotMark;

    @Getter
    @Setter
    public String updateQuotaKeyStr;

    @Getter
    @Setter
    public long fileCookie;

    @Getter
    @Setter
    public String lastAccessStamp;

    public DelMarkParams(String bucket, String object, String versionId, MetaData pendingMark, List<Tuple3<String, String, String>> vnodeList, String versionStatus, String currentVersion) {
        this.bucket = bucket;
        this.object = object;
        this.versionId = versionId;
        this.pendingMark = pendingMark;
        this.vnodeList = vnodeList;
        this.versionStatus = versionStatus;
        if (pendingMark != null) {
            String oldVersion = pendingMark.versionNum;
            this.versionNum = StringUtils.isEmpty(oldVersion) || currentVersion.compareTo(oldVersion) > 0 ? currentVersion : oldVersion + "0";
        } else {
            this.versionNum = currentVersion;
        }
        this.type = ERROR_MARK_DELETE_META;
    }

    public DelMarkParams request(MsHttpRequest request) {
        this.request = request;
        return this;
    }

    public DelMarkParams migrate(boolean isMigrate) {
        this.isMigrate = isMigrate;
        return this;
    }

    public DelMarkParams type(ErrorConstant.ECErrorType type) {
        this.type = type;
        return this;
    }

    public DelMarkParams needDeleteInode(boolean needDeleteInode) {
        this.needDeleteInode = needDeleteInode;
        return this;
    }

    public DelMarkParams nodeId(long inodeId) {
        this.inodeId = inodeId;
        return this;
    }

    public DelMarkParams snapshotMark(String snapshotMark) {
        this.snapshotMark = snapshotMark;
        return this;
    }

    public DelMarkParams updateQuotaKeyStr(String updateQuotaDirStr) {
        this.updateQuotaKeyStr = updateQuotaDirStr;
        return this;
    }

    public DelMarkParams fileCookie(long fileCookie) {
        this.fileCookie = fileCookie;
        return this;
    }

    public DelMarkParams lastAccessStamp(String lastAccessStamp) {
        this.lastAccessStamp = lastAccessStamp;
        return this;
    }
}
