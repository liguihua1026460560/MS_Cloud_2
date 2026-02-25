package com.macrosan.filesystem.cifs.lock;

import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.lock.redlock.LockType;
import com.macrosan.filesystem.lock.redlock.RedLockClient;
import com.macrosan.message.jsonmsg.Inode;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import static com.macrosan.filesystem.FsConstants.S_IFMT;
import static com.macrosan.filesystem.FsConstants.S_IFREG;

@Log4j2
public class FileLockUtils {
    public static final String LOCK_NODE = "0001";
    public static Mono<Boolean> tryFileLock(Session session, ReqInfo reqInfo, String realObj, int mode, long id0) {
        if (isFile(mode)) {
            String value = session.account + "_" + session.socket.remoteAddress().host() + "_" + id0 + "_" + 0;
            return RedLockClient.lock(reqInfo, realObj, value, LOCK_NODE, LockType.WRITE, false, true);
        }
        return Mono.just(true);
    }

    public static Mono<Boolean> tryFileLock(Session session, ReqInfo reqInfo, String realObj, int mode, SMB2FileId fileId) {
        if (isFile(mode)) {
            String value = session.account + "_" + session.socket.remoteAddress().host() + "_" + fileId.persistent + "_" + fileId.volatile_;
            return RedLockClient.lock(reqInfo, realObj, value, LOCK_NODE, LockType.WRITE, false, true);
        }
        return Mono.just(true);
    }

    public static Mono<Boolean> tryFileLock(Session session, ReqInfo reqInfo, String realObj, int mode, SMB2FileId fileId, SMB2FileId oldFileId) {
        if (isFile(mode)) {
            String oldValue = session.account + "_" + session.socket.remoteAddress().host() + "_" + oldFileId.persistent + "_" + oldFileId.volatile_;
            String newValue = session.account + "_" + session.socket.remoteAddress().host() + "_" + fileId.persistent + "_" + fileId.volatile_;
            return RedLockClient.unlock(reqInfo.bucket, realObj, oldValue, true, LOCK_NODE)
                    .flatMap(b -> RedLockClient.lock(reqInfo, realObj, newValue, LOCK_NODE, LockType.WRITE, false, true));
        }
        return Mono.just(true);
    }

    public static boolean isFile(int mode) {
        return (mode & S_IFMT) == S_IFREG;
    }

    public static boolean isFileByCifsMode(int mode) {
        return mode == 32;
    }

    public static Mono<Boolean> releaseFileLock(Session session, Inode inode, SMB2FileId fileId) {
        // s3删除正常上传的file文件时，mode=0，通过cifsMode判断是否为文件
        if (inode!= null && (isFile(inode.getMode()) || isFileByCifsMode(inode.getCifsMode()))) {
            String value = session.account + "_" + session.socket.remoteAddress().host() + "_" + fileId.persistent + "_" + fileId.volatile_;
            return RedLockClient.unlock(inode.getBucket(), inode.getObjName(), value, true, LOCK_NODE);
        }
        return Mono.just(true);
    }
}
