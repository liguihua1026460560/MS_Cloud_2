package com.macrosan.filesystem.nfs.call.v4;

import io.netty.buffer.ByteBuf;
import lombok.ToString;


@ToString
public class ExchangeIdCall extends CompoundCall {
    public byte[] verifier;
    public int clientOwnerLen;//
    //系统标识 Linux NFSv4.1 moss-0001
    public byte[] clientOwner;
    public int flags;
    public int stateProtect;
    //值为1
    public int eirClientImplId;
    public int domainLen;
    public byte[] domain;
    public int systemNameLen;
    public byte[] systemName;
    public int seconds;
    public int nSeconds;
    public int spoMustEnforceLen;
    public int[] spoMustEnforce;
    public int spoMustAllowLen;
    public int[] spoMustAllow;
    public int hashArgsLen;
    public byte[][] hashArgs;
    public int encrArgsLen;
    public byte[][] encrArgs;
    public int sspWindow;
    public int sspNumGssHandles;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int head = offset;
        verifier = new byte[8];
        buf.getBytes(offset, verifier);
        offset += 8;
        clientOwnerLen = buf.getInt(offset);
        offset += 4;
        clientOwner = new byte[clientOwnerLen];
        buf.getBytes(offset, clientOwner);
        offset += (clientOwnerLen + 3) / 4 * 4;
        flags = buf.getInt(offset);
        offset += 4;
        stateProtect = buf.getInt(offset);
        offset += 4;
        switch (stateProtect) {
            case STATE_PROTECT_SP4_NONE:
                break;
            case STATE_PROTECT_SP4_MACH_CRED:
                spoMustEnforceLen = buf.getInt(offset);
                offset += 4;
                spoMustEnforce = new int[spoMustEnforceLen];
                for (int i = 0; i < spoMustEnforceLen; i++) {
                    spoMustEnforce[i] = buf.getInt(offset);
                    offset += 4;
                }
                spoMustAllowLen = buf.getInt(offset);
                offset += 4;
                spoMustAllow = new int[spoMustAllowLen];
                for (int i = 0; i < spoMustAllowLen; i++) {
                    spoMustAllow[i] = buf.getInt(offset);
                    offset += 4;
                }
                break;
            case STATE_PROTECT_SP4_SSV:
                spoMustEnforceLen = buf.getInt(offset);
                offset += 4;
                spoMustEnforce = new int[spoMustEnforceLen];
                for (int i = 0; i < spoMustEnforceLen; i++) {
                    spoMustEnforce[i] = buf.getInt(offset);
                    offset += 4;
                }
                spoMustAllowLen = buf.getInt(offset);
                offset += 4;
                spoMustAllow = new int[spoMustAllowLen];
                for (int i = 0; i < spoMustAllowLen; i++) {
                    spoMustAllow[i] = buf.getInt(offset);
                    offset += 4;
                }
                hashArgsLen = buf.getInt(offset);
                offset += 4;
                for (int i = 0; i < hashArgsLen; i++) {
                    int hashArgsPerLen = buf.getInt(offset);
                    offset += 4;
                    hashArgs[i] = new byte[hashArgsPerLen];
                    buf.getBytes(offset, hashArgs[i]);
                    offset += (hashArgsPerLen + 3) * 4 / 4;
                }
                encrArgsLen = buf.getInt(offset);
                offset += 4;
                for (int i = 0; i < encrArgsLen; i++) {
                    int encrArgsPerLen = buf.getInt(offset);
                    offset += 4;
                    encrArgs[i] = new byte[encrArgsPerLen];
                    buf.getBytes(offset, encrArgs[i]);
                    offset += (encrArgsPerLen + 3) * 4 / 4;
                }
                sspWindow = buf.getInt(offset);
                offset += 4;
                sspNumGssHandles = buf.getInt(offset);
                offset += 4;
                break;


        }
        eirClientImplId = buf.getInt(offset);
        offset += 4;
        domainLen = buf.getInt(offset);
        offset += 4;
        domain = new byte[domainLen];
        buf.getBytes(offset, domain);
        offset += (domainLen + 3) / 4 * 4;
        systemNameLen = buf.getInt(offset);
        offset += 4;
        systemName = new byte[systemNameLen];
        buf.getBytes(offset, systemName);
        offset += (systemNameLen + 3) / 4 * 4;
        seconds = buf.getInt(offset);
        offset += 4;
        nSeconds = buf.getInt(offset);
        offset += 4;
        return offset - head;
    }






    public static final int EXCHGID4_FLAG_SUPP_MOVED_REFER = 0x00000001;
    public static final int EXCHGID4_FLAG_SUPP_MOVED_MIGR = 0x00000002;
    public static final int EXCHGID4_FLAG_BIND_PRINC_STATEID = 0x00000100;
    public static final int EXCHGID4_FLAG_USE_NON_PNFS = 0x00010000;
    public static final int EXCHGID4_FLAG_USE_PNFS_MDS = 0x00020000;
    public static final int EXCHGID4_FLAG_USE_PNFS_DS = 0x00040000;
    public static final int EXCHGID4_FLAG_MASK_PNFS = 0x00070000;
    public static final int EXCHGID4_FLAG_UPD_CONFIRMED_REC_A = 0x40000000;
    public static final int EXCHGID4_FLAG_CONFIRMED_R = 0x80000000;
    //exchangeId stateProtect
    public static final int STATE_PROTECT_SP4_NONE = 0;  //表示没有状态保护
    public static final int STATE_PROTECT_SP4_MACH_CRED = 1;//表示使用机器凭证进行状态保护
    public static final int STATE_PROTECT_SP4_SSV = 2;//表示使用安全状态向量（SSV）进行状态保护


    public static final int EXCHGID4_FLAG_MASK = (EXCHGID4_FLAG_USE_PNFS_DS
            | EXCHGID4_FLAG_USE_NON_PNFS
            | EXCHGID4_FLAG_USE_PNFS_MDS
            | EXCHGID4_FLAG_SUPP_MOVED_MIGR
            | EXCHGID4_FLAG_SUPP_MOVED_REFER
            | EXCHGID4_FLAG_MASK_PNFS
            | EXCHGID4_FLAG_UPD_CONFIRMED_REC_A
            | EXCHGID4_FLAG_CONFIRMED_R
            | EXCHGID4_FLAG_BIND_PRINC_STATEID
            | EXCHGID4_FLAG_UPD_CONFIRMED_REC_A
            | EXCHGID4_FLAG_CONFIRMED_R);

}
