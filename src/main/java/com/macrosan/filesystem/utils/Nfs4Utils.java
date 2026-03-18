package com.macrosan.filesystem.utils;

import com.google.common.base.Utf8;
import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.nfs.NFSV4;
import com.macrosan.filesystem.nfs.RpcCallHeader;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.api.NFS4Proc;
import com.macrosan.filesystem.nfs.api.NSMProc;
import com.macrosan.filesystem.nfs.auth.AuthUnix;
import com.macrosan.filesystem.nfs.call.v4.CBSequenceCall;
import com.macrosan.filesystem.nfs.call.v4.CompoundCall;
import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.filesystem.nfs.reply.v4.CompoundReply;
import com.macrosan.filesystem.nfs.types.CBInfo;
import com.macrosan.filesystem.nfs.types.CompoundContext;
import com.macrosan.filesystem.nfs.types.NFS4Session;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import com.macrosan.message.jsonmsg.Inode;
import lombok.extern.log4j.Log4j2;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.*;
import static com.macrosan.filesystem.nfs.call.v4.AccessV4Call.ACCESS_MODIFY;
import static com.macrosan.filesystem.nfs.call.v4.AccessV4Call.ACCESS_READ;
import static com.macrosan.filesystem.nfs.call.v4.OpenV4Call.*;
import static com.macrosan.filesystem.nfs.reply.v4.OpenV4Reply.OPEN_DELEGATE_READ;
import static com.macrosan.filesystem.nfs.types.StateId.*;


@Log4j2
public class Nfs4Utils {
    private static final Charset UTF8 = StandardCharsets.UTF_8;

    public static boolean checkSetAttr(ObjAttr objAttr) {
        return objAttr.hasMtime == 0 && objAttr.hasAtime == 0 && objAttr.hasSize == 0 && objAttr.hasGid == 0 && objAttr.hasUid == 0;
    }

    public static boolean checkAttrSame(ObjAttr objAttr, Inode inode) {
        if (objAttr.hasMode != 0 && (inode.getMode() & 4095) != objAttr.mode) {
            return true;
        }
        if (objAttr.hasSize != 0 && objAttr.size != inode.getSize()) {
            return true;
        }
        if (objAttr.hasAtime != 0 && objAttr.atime != inode.getAtime()) {
            return true;
        }
        if (objAttr.hasMtime != 0 && objAttr.mtime != inode.getMtime()) {
            return true;
        }
        return false;
    }

    public static void checkDir(Inode currInode) {
        if ((currInode.getMode() & S_IFMT) != S_IFDIR) {
            throw new NFSException(NFS3ERR_NOTDIR, "checkDir : currInode is not dir");
        }
    }

    public static void checkNotDir(Inode currInode) {
        if ((currInode.getMode() & S_IFMT) == S_IFDIR) {
            throw new NFSException(NFS3ERR_ISDIR, "checkNotDir : currInode is dir");
        }
    }

    public static void checkSymlink(Inode currInode) {
        if ((currInode.getMode() & S_IFMT) == S_IFLNK) {
            throw new NFSException(NFS4ERR_SYMLINK, "checkSymlink : currInode is symlink");
        }
    }

    public static void checkNotSymlink(Inode currInode) {
        if ((currInode.getMode() & S_IFMT) != S_IFLNK) {
            throw new NFSException(NFS3ERR_INVAL, "checkNotSymlink : currInode is  not symlink");
        }
    }

    public static void checkDirAndName(Inode currInode, byte[] nameBytes) {
        checkDir(currInode);
        checkName(nameBytes);
    }

    public static void checkDirAndNotDir(Inode currInode, Inode saveInode) {
        checkDir(currInode);
        checkNotDir(saveInode);
    }
    public static void checkNotDirAndNotDir(Inode currInode, Inode saveInode) {
        checkNotDir(currInode);
        checkNotDir(saveInode);
    }

    public static void checkDirAndDir(Inode currInode, Inode saveInode) {
        checkDir(currInode);
        checkDir(saveInode);
    }

    public static void checkNotDirAndSym(Inode currInode) {
        checkNotDir(currInode);
        checkSymlink(currInode);
    }

    public static void checkDirAndSym(Inode currInode) {
        checkDir(currInode);
        checkSymlink(currInode);
    }

    public static void checkDirAndNameAndSym(Inode currInode, byte[] nameBytes) {
        checkDir(currInode);
        checkSymlink(currInode);
        checkName(nameBytes);
    }

    public static void checkName(byte[] bytes) {
        String name = new String(bytes, UTF8);
        if (!Utf8.isWellFormed(bytes) || name.length() == 0) {
            throw new NFSException(NFS3ERR_INVAL, "checkName : name pattern illegal");
        }
        if (name.indexOf('\0') != -1) {
            throw new NFSException(NFS4ERR_BADNAME, "checkName : name pattern bad");
        }

        if (name.length() > NFS_MAX_NAME_LENGTH) {
            throw new NFSException(NFS3ERR_NAMETOOLONG, "checkName : name too long");
        }

        if (name.equals(".") || name.equals("..") || name.indexOf('/') != -1) {
            throw new NFSException(NFS4ERR_BADNAME, "checkName : name pattern  error");
        }
    }

    public static void putLong(byte[] bytes, int offset, long value) {
        if (bytes.length - offset < 8) {
            log.error("not enough space to store long");
            return;
        }

        bytes[offset] = (byte) (value >> 56);
        bytes[offset + 1] = (byte) (value >> 48);
        bytes[offset + 2] = (byte) (value >> 40);
        bytes[offset + 3] = (byte) (value >> 32);
        bytes[offset + 4] = (byte) (value >> 24);
        bytes[offset + 5] = (byte) (value >> 16);
        bytes[offset + 6] = (byte) (value >> 8);
        bytes[offset + 7] = (byte) value;
    }

    public static void putInt(byte[] bytes, int offset, int value) {
        if (bytes.length - offset < 4) {
            log.error("not enough space to store int");
            return;
        }
        bytes[offset] = (byte) (value >> 24);
        bytes[offset + 1] = (byte) (value >> 16);
        bytes[offset + 2] = (byte) (value >> 8);
        bytes[offset + 3] = (byte) value;
    }

    public static long getLong(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFFL) << 56
                | (bytes[offset + 1] & 0xFFL) << 48
                | (bytes[offset + 2] & 0xFFL) << 40
                | (bytes[offset + 3] & 0xFFL) << 32
                | (bytes[offset + 4] & 0xFFL) << 24
                | (bytes[offset + 5] & 0xFFL) << 16
                | (bytes[offset + 6] & 0xFFL) << 8
                | (bytes[offset + 7] & 0xFFL);
    }

    public static int getInt(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF) << 24
                | (bytes[offset + 1] & 0xFF) << 16
                | (bytes[offset + 2] & 0xFF) << 8
                | (bytes[offset + 3] & 0xFF);
    }


    public static int getRealAccess(int share) {
        int realShare = 0;
        switch (share) {
            case OPEN_SHARE_ACCESS_BOTH:
                realShare = realShare | OPEN_SHARE_ACCESS_BOTH | OPEN_SHARE_ACCESS_READ | OPEN_SHARE_ACCESS_WRITE;
                break;
            case OPEN_SHARE_ACCESS_READ:
                realShare = realShare | OPEN_SHARE_ACCESS_READ;
                break;
            case OPEN_SHARE_ACCESS_WRITE:
                realShare = realShare | OPEN_SHARE_ACCESS_WRITE;
                break;
            default:
                break;
        }
        return realShare;
    }


    public static int getRealDeny(int share) {
        int realShare = 0;
        switch (share) {
            case OPEN_SHARE_DENY_READ:
                realShare = realShare | OPEN_SHARE_DENY_READ;
                break;
            case OPEN_SHARE_DENY_BOTH:
                realShare = realShare | OPEN_SHARE_DENY_BOTH | OPEN_SHARE_DENY_WRITE | OPEN_SHARE_DENY_READ;
            case OPEN_SHARE_DENY_WRITE:
                realShare = realShare | OPEN_SHARE_DENY_WRITE;
                break;
            case OPEN_SHARE_DENY_NONE:
            default:
                break;
        }
        return realShare;
    }

    public static void checkCanAccess(CompoundReply reply, CompoundContext context, Inode inode, int shareAccess) {

        int accessMode;
        switch (shareAccess & ~NFS4_SHARE_ACCESS_WANT_DELEG_MASK) {
            case OPEN_SHARE_ACCESS_READ:
                accessMode = ACCESS_READ;
                break;
            case OPEN_SHARE_ACCESS_WRITE:
                accessMode = ACCESS_MODIFY;
                break;
            case OPEN_SHARE_ACCESS_BOTH:
                accessMode = ACCESS_READ | ACCESS_MODIFY;
                break;
            default:
                log.error("shareAccess type error ");
        }
        //todo acl
//        if (access(inode, accessMode) != accessMode) {
//            throw new AccessException();
//        }

        int mode = inode.getMode();
        if ((mode & S_IFMT) == S_IFLNK) {
            reply.status = NFS4ERR_SYMLINK;
        } else if ((mode & S_IFMT) == S_IFDIR) {
            reply.status = NFS3ERR_ISDIR;
        } else if ((mode & S_IFMT) != S_IFREG) {
            if (context.getMinorVersion() == 0) {
                reply.status = NFS4ERR_SYMLINK;
            } else {
                reply.status = NFS4ERR_WRONG_TYPE;
            }
        }
    }

    public static boolean checkShare(int accessMask, int access) {
        int mask = 1 << accessMask;
        int accessBit = 1 << access;
        return (accessBit & mask) != 0;
    }

    public static boolean checkOpenMode(boolean isWrite, int access, int stateIdType) {
        switch (stateIdType) {
            case NFS4_OPEN_STID:
            case NFS4_LOCK_STID:
                if (isWrite) {
                    return checkShare(OPEN_SHARE_ACCESS_WRITE, access) || checkShare(OPEN_SHARE_ACCESS_BOTH, access);
                }
                return checkShare(OPEN_SHARE_ACCESS_READ, access) ||
                        checkShare(OPEN_SHARE_ACCESS_WRITE, access) ||
                        checkShare(OPEN_SHARE_ACCESS_BOTH, access);
            case NFS4_DELEG_STID:
                return !(isWrite && (access == OPEN_DELEGATE_READ));
            default:
                //bad_stateId
                return false;
        }
    }

    public static void checkIsRoot(int fsid) {
        if (fsid == NFS4Proc.ROOT_FSID) {
            throw new NFSException(NFS3ERR_ACCES, "not exec this ops");
        }
    }

    public static CompoundCall buildCBCall(NFS4Session session) {
        int xid = (int) NSMProc.nsmNextXid();
        SunRpcHeader sunRpcHeader = SunRpcHeader.newCallHeader(xid);
        RpcCallHeader rpcCallHeader = new RpcCallHeader(sunRpcHeader);
        CompoundCall compoundCall = new CompoundCall();
        rpcCallHeader.rpcVersion = 2;
        rpcCallHeader.program = session.cbProgram;
        rpcCallHeader.programVersion = 1;
        rpcCallHeader.opt = 1;
        AuthUnix authUnix = new AuthUnix();
        authUnix.setFlavor(1);
        authUnix.setStamp(0);
        String name = System.getenv("HOSTNAME");
        authUnix.setNameSize(name.length());
        authUnix.setName(name.getBytes());
        authUnix.setUid(0);
        authUnix.setGid(0);
        authUnix.setGidN(0);
        authUnix.setGids(new int[0]);
        authUnix.setPadding(0);
        authUnix.setAuthLen(authUnix.length());
        rpcCallHeader.auth = authUnix;
        compoundCall.setCount(compoundCall.callList.size());
        int minorVersion = session.getClient().getMinorVersion();
        compoundCall.setMinorVersion(minorVersion);
        compoundCall.setCallHeader(rpcCallHeader);
        compoundCall.setTagContent(new byte[0]);
        //todo
        compoundCall.setCallbackIdent(0);
        CBSequenceCall cbSequenceCall = new CBSequenceCall();
        cbSequenceCall.opt = NFSV4.CBOpcode.NFS4PROC_CB_SEQUENCE.opcode;
        cbSequenceCall.sessionId = session.getSessionId();
        cbSequenceCall.seqId = session.cbSeqId.incrementAndGet();
        cbSequenceCall.slotId = 0;
        cbSequenceCall.highSlotId = 0;
        cbSequenceCall.isCache = false;
        //todo
        cbSequenceCall.referringCallList = 0;
        compoundCall.callList.add(cbSequenceCall);
        return compoundCall;
    }

    public synchronized static CompoundCall sendCB(NFS4Session session, List<CompoundCall> cbCallList, int mainOpt) {
        CompoundCall compoundCall = buildCBCall(session);
        compoundCall.callList.addAll(cbCallList);
        compoundCall.setCount(compoundCall.callList.size());
        session.nfsHandler.write(compoundCall);
        NFSHandler.CBInfoMap.put(compoundCall.callHeader.header.id, new CBInfo(2, session.cbProgram, 1, mainOpt));
        return compoundCall;
    }

//    public static CompoundCall buildNotifyLock()
}
