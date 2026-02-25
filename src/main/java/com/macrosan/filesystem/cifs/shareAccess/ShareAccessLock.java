package com.macrosan.filesystem.cifs.shareAccess;

import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.call.smb2.CreateCall;
import com.macrosan.filesystem.cifs.handler.SMBHandler;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.lock.Lock;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.Inode;
import lombok.*;

import java.util.Map;
import java.util.Objects;

import static com.macrosan.filesystem.cifs.call.smb2.CreateCall.*;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false, onlyExplicitlyIncluded = true)
@ToString
public class ShareAccessLock extends Lock {

    public String bucket;
    public String fileName;
    public long ino;
    @EqualsAndHashCode.Include
    public SMB2FileId smb2FileId;
    public long accessMask;
    public int shareAccess;
//    @EqualsAndHashCode.Include
    public String node;
    public long sessionId;
    public int treeId;

    public ShareAccessLock(SMB2FileId smb2FileId, String node) {
        this.smb2FileId = smb2FileId;
        this.node = node;
    }

    public ShareAccessLock(Inode inode, SMB2FileId smb2FileId, CreateCall call, SMB2Header header, String node) {
        this.bucket = inode.getBucket();
        this.fileName = inode.getObjName();
        this.ino = inode.getNodeId();
        this.smb2FileId = smb2FileId;
        this.accessMask = call.getAccess();
        this.shareAccess = call.getShareAccess();
        this.node = node;
        this.sessionId = header.getSessionId();
        this.treeId = header.getTid();
    }

    @Override
    public boolean needKeep() {
        if (!node.equals(ServerConfig.getInstance().getHostUuid())) {
            return false;
        }
        Session session = SMBHandler.session2Map.get(sessionId);
        if (session == null || session.localIp == null || !Objects.equals(0, SMBHandler.localIpMap.get(session.localIp))) {
            return false;
        }
        Map<Integer, Map<SMB2FileId, ShareAccessLock>> map = ShareAccessServer.sessionTreeMap.get(this.sessionId);
        if (map == null) {
            return false;
        }
        Map<SMB2FileId, ShareAccessLock> map0 = map.get(this.treeId);
        if (map0 == null) {
            return false;
        }
        return map0.containsKey(this.smb2FileId);
    }
    public static final int FILE_SHARE_READ = 0x00000001;
    public static final int FILE_SHARE_WRITE = 0x00000002;
    public static final int FILE_SHARE_DELETE = 0x00000004;
    public static final int FILE_CHECK_READ = FILE_READ_DATA | FILE_EXECUTE | FILE_GENERIC_READ | FILE_GENERIC_EXECUTE |
            FILE_GENERIC_ALL | FILE_MAXIMUM_ALLOWED;
    public static final int FILE_CHECK_WRITE = FILE_WRITE_DATA | FILE_APPEND_DATA | FILE_GENERIC_WRITE |
            FILE_GENERIC_ALL | FILE_MAXIMUM_ALLOWED;
    public static final int FILE_CHECK_DELETE = FILE_DELETE |
            FILE_GENERIC_ALL | FILE_MAXIMUM_ALLOWED;

    public static final int FILE_CHECK_SHAREACCESS = FILE_CHECK_READ | FILE_CHECK_WRITE | FILE_CHECK_DELETE;
}
