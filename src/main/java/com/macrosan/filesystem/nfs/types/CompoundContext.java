package com.macrosan.filesystem.nfs.types;

import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.nfs.types.NFS4Session;
import com.macrosan.message.jsonmsg.Inode;
import lombok.Data;
import lombok.extern.log4j.Log4j2;

import static com.macrosan.filesystem.FsConstants.EIO;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_NOENT;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS4ERR_NOFILEHANDLE;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.message.jsonmsg.Inode.NOT_FOUND_INODE;

@Data
@Log4j2
public class CompoundContext {
    public FH2 currFh;
    public FH2 saveFh;
    public int gid;
    public int uid;
    public long clientId;
    public byte[] sessionId;
    public StateId currStateId;
    public StateId saveStateId;
    public int minorVersion;
    private Inode currentInode = null;
    public Inode savedInode = null;
    public NFS4Session session = null;
    private boolean cacheThis;

    public int getMinorVersion() {
        return minorVersion;
    }

    public Inode getCurrentInode() {
        if (currentInode == null) {
            throw new NFSException(NFS4ERR_NOFILEHANDLE, "no fileHandle, nodeId:"+ currFh.ino);
        }
        if (NOT_FOUND_INODE.equals(currentInode)){
            throw new NFSException(NFS3ERR_NOENT, "inode not exist, nodeId:" + currFh.ino);
        }
        if (ERROR_INODE.equals(currentInode)){
            throw new NFSException(EIO, "error inode, nodeId:" + currFh.ino);
        }
        return currentInode;
    }

    public void setCurrentInode(Inode inode) {
        currentInode = inode;
    }

    public void clearCurrentInode() {
        currentInode = null;
    }

    public void setSession(NFS4Session session) {
        this.session = session;
    }

    public NFS4Session getSession() {
        return session;
    }
}