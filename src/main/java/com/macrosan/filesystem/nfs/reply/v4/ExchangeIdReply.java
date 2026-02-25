package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.nfs.call.v4.ExchangeIdCall.*;

@ToString(callSuper = true)
public class ExchangeIdReply extends CompoundReply {
    public long clientId;
    public int seqId;
    public int flags;
    public int stateProtect;
    public long minorId;
    public int majorIdLen;
    public byte[] majorId;
    public int serverScopeLen;
    public byte[] serverScope;
    public int eirServerImplId;
    public int spoMustEnforceLen;
    public int[] spoMustEnforce;
    public int spoMustAllowLen;
    public int[] spoMustAllow;
    public int spiHashAlg;
    public int spiEncrAlg;
    public int spiSsvLen;
    public int spiWindow;
    //gssHandles未确定结构
//    public  gssHandles;

    public ExchangeIdReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        offset += 4;
        buf.setInt(offset, status);
        offset += 4;
        buf.setLong(offset, clientId);
        offset += 8;
        buf.setInt(offset, seqId);
        offset += 4;
        buf.setInt(offset, flags);
        offset += 4;
        buf.setInt(offset, stateProtect);
        switch (stateProtect) {
            case STATE_PROTECT_SP4_NONE:
                break;
            case STATE_PROTECT_SP4_MACH_CRED:
                buf.setInt(offset, spoMustEnforceLen);
                offset += 4;
                for (int i = 0; i < spoMustEnforceLen; i++) {
                    buf.setInt(offset, spoMustEnforce[i]);
                    offset += 4;
                }
                buf.setInt(offset, spoMustAllowLen);
                offset += 4;
                for (int i = 0; i < spoMustAllowLen; i++) {
                    buf.setInt(offset, spoMustAllow[i]);
                    offset += 4;
                }
                break;
            case STATE_PROTECT_SP4_SSV:
                buf.setInt(offset, spoMustEnforceLen);
                offset += 4;
                for (int i = 0; i < spoMustEnforceLen; i++) {
                    buf.setInt(offset, spoMustEnforce[i]);
                    offset += 4;
                }
                buf.setInt(offset, spoMustAllowLen);
                offset += 4;
                for (int i = 0; i < spoMustAllowLen; i++) {
                    buf.setInt(offset, spoMustAllow[i]);
                    offset += 4;
                }
                buf.setInt(offset, spiSsvLen);
                offset += 4;
                buf.setInt(offset, spiWindow);
                offset += 4;
                break;
        }
        offset += 4;
        buf.setLong(offset, minorId);
        offset += 8;
        majorIdLen = majorId.length;
        buf.setInt(offset, majorIdLen);
        offset += 4;
        buf.setBytes(offset, majorId);
        offset += (majorIdLen + 3) / 4 * 4;
        serverScopeLen = serverScope.length;
        buf.setInt(offset, serverScopeLen);
        offset += 4;
        buf.setBytes(offset, serverScope);
        offset += (serverScopeLen + 3) / 4 * 4;
        buf.setInt(offset, eirServerImplId);
        offset += 4;
        return offset - start;
    }
}
