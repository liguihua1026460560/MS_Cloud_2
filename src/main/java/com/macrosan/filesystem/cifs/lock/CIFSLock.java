package com.macrosan.filesystem.cifs.lock;

import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.call.smb2.LockCall;
import com.macrosan.filesystem.cifs.handler.SMBHandler;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.lock.Lock;
import com.macrosan.httpserver.ServerConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Map;
import java.util.Set;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false, exclude = {"bucket", "fileName", "ino", "failImmediately", "sessionId", "treeId", "isClientLockReq", "isClientUnLockReq"})
@ToString
public class CIFSLock extends Lock {

    public String bucket;
    public String fileName;
    public long ino;
    public SMB2FileId smb2FileId;
    public String node;
    public long offset;
    public long len;
    public int lockType; // shared 1, exclusive 2
    public boolean failImmediately;
    public long sessionId;
    public int treeId;

    public boolean isClientLockReq;
    public boolean isClientUnLockReq;

    public CIFSLock(String bucket, String fileName, long ino, SMB2FileId smb2FileId, SMB2Header header, String node,
                    LockCall.LockElement lock, int lockType, boolean failImmediately, boolean isClientLockReq, boolean isClientUnLockReq) {
        this.bucket = bucket;
        this.fileName = fileName;
        this.ino = ino;
        this.smb2FileId = smb2FileId;
        this.node = node;
        this.offset = lock.offset;
        this.len = lock.len == Long.MIN_VALUE ? Long.MAX_VALUE : lock.len;
        this.lockType = lockType;
        this.failImmediately = failImmediately;
        this.sessionId = header.getSessionId();
        this.treeId = header.getTid();
        this.isClientLockReq = isClientLockReq;
        this.isClientUnLockReq = isClientUnLockReq;
    }

    public CIFSLock(CIFSLock cifsLock) {
        this.bucket = cifsLock.bucket;
        this.fileName = cifsLock.fileName;
        this.ino = cifsLock.ino;
        this.smb2FileId = cifsLock.smb2FileId;
        this.node = cifsLock.node;
        this.offset = cifsLock.offset;
        this.len = cifsLock.len;
        this.lockType = cifsLock.lockType;
        this.failImmediately = false;
        this.sessionId = cifsLock.sessionId;
        this.treeId = cifsLock.treeId;
        this.isClientLockReq = false;
        this.isClientUnLockReq = true;
    }

    @Override
    public boolean needKeep() {
        if (!node.equals(ServerConfig.getInstance().getHostUuid()) || !SMBHandler.session2Map.containsKey(sessionId)) {
            return false;
        }
        Map<Integer, Map<Tuple2<SMB2FileId, String>, Set<CIFSLock>>> treeMap = CIFSLockServer.sessionTreeMap.get(this.sessionId);
        if (treeMap == null) {
            return false;
        }
        Map<Tuple2<SMB2FileId, String>, Set<CIFSLock>> fileIdMap = treeMap.get(this.treeId);
        if (fileIdMap == null) {
            return false;
        }
        Set<CIFSLock> cifsLocks = fileIdMap.get(Tuples.of(this.smb2FileId, this.node));
        if (cifsLocks == null) {
            return false;
        }
        return cifsLocks.contains(this);
    }
}
