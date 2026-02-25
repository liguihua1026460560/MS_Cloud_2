package com.macrosan.filesystem.nfs.call.v4;

import com.macrosan.filesystem.nfs.types.FAttr4;
import com.macrosan.filesystem.nfs.types.StateId;
import io.netty.buffer.ByteBuf;
import lombok.ToString;


@ToString
public class OpenV4Call extends CompoundCall {
    public int seqId;
    public int shareAccess;
    public int want;
    //    public int delegWhen;
    public int shareDeny;
    public long clientId;
    public int ownerLen;
    public byte[] owner;
    public int openType;
    public int createMode;
    public long verifier;
    public int maskLen;
    public int[] mask;
    public int maskTotalLen;
    public int claimType;
    public int nameLen;
    public byte[] name;
    public int delegateType;
    public StateId delegateStateId = new StateId();
    public FAttr4 fAttr4;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        seqId = buf.getInt(offset);
        shareAccess = buf.getInt(offset + 4) & NFS4_SHARE_ACCESS_MASK;
        want = buf.getInt(offset + 4) & NFS4_SHARE_ACCESS_WANT_DELEG_MASK;
        shareDeny = buf.getInt(offset + 8);
        clientId = buf.getLong(offset + 12);
        ownerLen = buf.getInt(offset + 20);
        offset += 24;
        owner = new byte[ownerLen];
        buf.getBytes(offset, owner);
        offset += (ownerLen + 3) / 4 * 4;
        openType = buf.getInt(offset);
        offset += 4;
        if (openType == OPEN_CREATE) {
            createMode = buf.getInt(offset);
            offset += 4;
            switch (createMode) {
                case CREATE_UNCHECKED:
                case CREATE_GUARDED:
                    maskLen = buf.getInt(offset);
                    mask = new int[maskLen];
                    for (int i = 0; i < maskLen; i++) {
                        mask[i] = buf.getInt(offset + 4 * (i + 1));
                    }
                    offset += 4 + 4 * maskLen;
                    maskTotalLen = buf.getInt(offset);
                    offset += 4;
                    fAttr4 = new FAttr4(mask, this.context.minorVersion);
                    fAttr4.readStruct(buf, offset);
                    offset += maskTotalLen;
                    break;
                case CREATE_EXCLUSIVE:
                    verifier = buf.getLong(offset + 4);
                    offset += 8;
                    break;
                case CREATE_EXCLUSIVE4_1:
                    //version>=1
                    verifier = buf.getLong(offset + 4);
                    offset += 8;
                    maskLen = buf.getInt(offset);
                    mask = new int[maskLen];
                    for (int i = 0; i < maskLen; i++) {
                        mask[i] = buf.getInt(offset + 4 * (i + 1));
                    }
                    offset += 4 + 4 * maskLen;
                    maskTotalLen = buf.getInt(offset);
                    offset += 4;
                    fAttr4 = new FAttr4(mask, this.context.minorVersion);
                    fAttr4.readStruct(buf, offset);
                    offset += maskTotalLen;
                    break;

            }
        }
        claimType = buf.getInt(offset);
        offset += 4;
        switch (claimType) {
            case NFS4_OPEN_CLAIM_NULL:
            case NFS4_OPEN_CLAIM_DELEGATE_PREV:
                nameLen = buf.getInt(offset);
                offset += 4;
                name = new byte[nameLen];
                buf.getBytes(offset, name);
                offset += (nameLen + 3) / 4 * 4;
                break;
            case NFS4_OPEN_CLAIM_PREVIOUS:
                delegateType = buf.getInt(offset);
                offset += 4;
                break;
            case NFS4_OPEN_CLAIM_DELEGATE_CUR:
                offset += delegateStateId.readStruct(buf, offset);
                nameLen = buf.getInt(offset);
                offset += 4;
                name = new byte[nameLen];
                buf.getBytes(offset, name);
                offset += (nameLen + 3) / 4 * 4;
                break;
            case NFS4_OPEN_CLAIM_FH:
                //此type只需要当前fh即可open
            case NFS4_OPEN_CLAIM_DELEG_PREV_FH:
                //这两种需要version>=1
                break;
            case NFS4_OPEN_CLAIM_DELEG_CUR_FH:
                //需要version>=1
                offset += delegateStateId.readStruct(buf, offset);
                break;

        }
        return offset - start;
    }

    public static final int OPEN_NOCREATE = 0;
    public static final int OPEN_CREATE = 1;
    public static final int CREATE_UNCHECKED = 0;
    public static final int CREATE_GUARDED = 1;
    public static final int CREATE_EXCLUSIVE = 2;
    public static final int CREATE_EXCLUSIVE4_1 = 3;
    //open claim type
    public static final int NFS4_OPEN_CLAIM_NULL = 0;
    public static final int NFS4_OPEN_CLAIM_PREVIOUS = 1;
    //有委托，需要继续委托
    public static final int NFS4_OPEN_CLAIM_DELEGATE_CUR = 2;
    //之前有委托，需要继续之前委托(提供stateid)
    public static final int NFS4_OPEN_CLAIM_DELEGATE_PREV = 3;
    public static final int NFS4_OPEN_CLAIM_FH = 4; /* 4.1 */
    public static final int NFS4_OPEN_CLAIM_DELEG_CUR_FH = 5; /* 4.1 */
    public static final int NFS4_OPEN_CLAIM_DELEG_PREV_FH = 6; /* 4.1 */

    public static final int OPEN4_RESULT_CONFIRM = 0x00000002;
    /* Type of file locking behavior at the server */
    public static final int OPEN4_RESULT_LOCKTYPE_POSIX = 0x00000004;
    /* Server will preserve file if removed while open */
    public static final int OPEN4_RESULT_PRESERVE_UNLINKED = 0x00000008;
    /*
     * Server may use CB_NOTIFY_LOCK on locks
     * derived from this open
     */
    public static final int OPEN4_RESULT_MAY_NOTIFY_LOCK = 0x00000020;
    //所有节点都存在lock已加的锁缓存，那么当一个节点获取有没有锁时，或者说加锁时，需要返回冲突的锁或者加锁成功，
    // 那么由于lock都是缓存，所以可能有节点重启导致部分节点lock缓存失效，那么此时当一个节点要加锁时，如果有节点出现冲突锁，右节点出现加锁成功，这种情况怎么处理最后时加锁成功还是加锁失败

    //shareAccess
    public static final int NFS4_SHARE_ACCESS_MASK = 0x000F;
    public static final int NFS4_SHARE_ACCESS_WANT_DELEG_MASK = 0xFF00;
    //允许其他客户端以只读方式打开文件
    public static final int OPEN_SHARE_ACCESS_READ = 1;
    public static final int OPEN_SHARE_ACCESS_WRITE = 2;
    public static final int OPEN_SHARE_ACCESS_BOTH = 3;
    //shareDeny
    //不拒绝其他客户端的访问
    public static final int OPEN_SHARE_DENY_NONE = 0;
    //拒绝以读方式打开
    public static final int OPEN_SHARE_DENY_READ = 1;
    public static final int OPEN_SHARE_DENY_WRITE = 2;
    public static final int OPEN_SHARE_DENY_BOTH = 3;

    public static final int OPEN_SHARE_ACCESS_WANT_NO_PREFERENCE = 0x0000;
    public static final int OPEN_SHARE_ACCESS_WANT_READ_DELEG = 0x0100;
    public static final int OPEN_SHARE_ACCESS_WANT_WRITE_DELEG = 0x0200;
    public static final int OPEN_SHARE_ACCESS_WANT_ANY_DELEG = 0x0300;
    public static final int OPEN_SHARE_ACCESS_WANT_NO_DELEG = 0x0400;
    public static final int OPEN_SHARE_ACCESS_WANT_CANCEL = 0x0500;
    public static final int OPEN_SHARE_ACCESS_WANT_DELEG_MASK = 0xFF00;
    public static final int OPEN_SHARE_ACCESS_WANT_SIGNAL_DELEG_WHEN_RESRC_AVAIL = 0x10000;
    public static final int OPEN_SHARE_ACCESS_WANT_PUSH_DELEG_WHEN_UNCONTENDED = 0x20000;

}
