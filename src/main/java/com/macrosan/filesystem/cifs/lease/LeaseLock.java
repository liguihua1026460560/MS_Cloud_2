package com.macrosan.filesystem.cifs.lease;

import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.lock.Lock;
import com.macrosan.httpserver.ServerConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/212eb853-7e50-4608-877e-22d42e0664f3
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false, onlyExplicitlyIncluded = true)
@ToString()
public class LeaseLock extends Lock {

    @EqualsAndHashCode.Include
    public String leaseKey;
    public String clientLeaseId;

    public String bucket;
    public String fileName;
    public long ino;
    public int leaseState;

    public int breakToLeaseState;
    public long leaseBreakTimeout;
    public Set<SMB2FileId> leaseOpens = ConcurrentHashMap.newKeySet(); // File Id
    public boolean breaking;
    public boolean held;

    /** 待发送中断通知，本行参数未写
    public BreakNotification
    */
    public boolean fileDeleteOnClose;

    // v2参数
    public short epoch;
    public String parentLeaseKey;
    public int version;


    public String node;
    public boolean block;

    public LeaseLock(String leaseKey, String bucket, String fileName, long ino, int leaseState, SMB2FileId fileId, int version, String node) {
        this.leaseKey = leaseKey;
        this.clientLeaseId = leaseKey;
        this.bucket = bucket;
        this.fileName = fileName;
        this.ino = ino;
        this.leaseState = leaseState;
        this.leaseOpens.add(fileId);
        this.version = version;
        this.node = node;
        this.epoch++;
    }

    @Override
    public boolean needKeep() {
        if (!node.equals(ServerConfig.getInstance().getHostUuid())) {
            return false;
        }
        String k = this.bucket + "/" + this.ino;
        Map<String, LeaseLock> map = LeaseCache.leaseKeyMap.get(k);
        if (map == null) {
            return false;
        }
        return map.containsKey(this.leaseKey);
    }
}
