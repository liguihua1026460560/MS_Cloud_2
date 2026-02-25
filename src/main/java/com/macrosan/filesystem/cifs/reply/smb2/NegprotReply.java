package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.call.smb2.NegprotCall;
import com.macrosan.filesystem.utils.CifsUtils;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.ThreadLocalRandom;

import static com.macrosan.filesystem.cifs.call.smb2.NegprotCall.SMB2_NEGOTIATE_SIGNING_ENABLED;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/63abf97c-0d09-47e2-88d6-6bfa552949a5
 */
@Log4j2
@Data
@EqualsAndHashCode(callSuper = true)
public class NegprotReply extends SMB2Body {
    short securityMode;
    public short dialect;
    short negotiateContextCount;
    byte[] serverGuid;
    public int flags;
    int maxTransactSize;
    int maxReadSize;
    int maxWriteSize;
    long curTime;
    long bootTime;
    short tokenOff;
    short tokenLen;
    byte[] token;
    int negotiateContextOff;

    private static final byte[] DEFAULT_TOKEN = new byte[]{
            (byte) 0x60, (byte) 0x5e, (byte) 0x06, (byte) 0x06, (byte) 0x2b, (byte) 0x06, (byte) 0x01, (byte) 0x05, (byte) 0x05, (byte) 0x02, (byte) 0xa0, (byte) 0x54, (byte) 0x30, (byte) 0x52, (byte) 0xa0, (byte) 0x24,
            (byte) 0x30, (byte) 0x22, (byte) 0x06, (byte) 0x09, (byte) 0x2a, (byte) 0x86, (byte) 0x48, (byte) 0x82, (byte) 0xf7, (byte) 0x12, (byte) 0x01, (byte) 0x02, (byte) 0x02, (byte) 0x06, (byte) 0x09, (byte) 0x2a,
            (byte) 0x86, (byte) 0x48, (byte) 0x86, (byte) 0xf7, (byte) 0x12, (byte) 0x01, (byte) 0x02, (byte) 0x02, (byte) 0x06, (byte) 0x0a, (byte) 0x2b, (byte) 0x06, (byte) 0x01, (byte) 0x04, (byte) 0x01, (byte) 0x82,
            (byte) 0x37, (byte) 0x02, (byte) 0x02, (byte) 0x0a, (byte) 0xa3, (byte) 0x2a, (byte) 0x30, (byte) 0x28, (byte) 0xa0, (byte) 0x26, (byte) 0x1b, (byte) 0x24, (byte) 0x6e, (byte) 0x6f, (byte) 0x74, (byte) 0x5f,
            (byte) 0x64, (byte) 0x65, (byte) 0x66, (byte) 0x69, (byte) 0x6e, (byte) 0x65, (byte) 0x64, (byte) 0x5f, (byte) 0x69, (byte) 0x6e, (byte) 0x5f, (byte) 0x52, (byte) 0x46, (byte) 0x43, (byte) 0x34, (byte) 0x31,
            (byte) 0x37, (byte) 0x38, (byte) 0x40, (byte) 0x70, (byte) 0x6c, (byte) 0x65, (byte) 0x61, (byte) 0x73, (byte) 0x65, (byte) 0x5f, (byte) 0x69, (byte) 0x67, (byte) 0x6e, (byte) 0x6f, (byte) 0x72, (byte) 0x65
    };

    public static final byte[] SERVER_GUID = new byte[16];

    static {
        ThreadLocalRandom.current().nextBytes(SERVER_GUID);
    }

    public static final int SMB2_CAP_DFS = 0x00000001;
    public static final int SMB2_CAP_LEASING = 0x00000002;
    public static final int SMB2_CAP_LARGE_MTU = 0x00000004;
    public static final int SMB2_CAP_MULTI_CHANNEL = 0x00000008;
    public static final int SMB2_CAP_PERSISTENT_HANDLES = 0x00000010;
    public static final int SMB2_CAP_DIRECTORY_LEASING = 0x00000020;
    public static final int SMB2_CAP_ENCRYPTION = 0x00000040;


    public static NegprotReply smb2XX() {
        NegprotReply reply = new NegprotReply();
        reply.securityMode = SMB2_NEGOTIATE_SIGNING_ENABLED;
        reply.dialect = NegprotCall.SMB_2_X_X;
        reply.negotiateContextCount = 0;
        reply.serverGuid = SERVER_GUID;
        reply.flags = 0;
        reply.maxTransactSize = 64 << 10;
        reply.maxReadSize = 64 << 10;
        reply.maxWriteSize = 64 << 10;

        reply.curTime = CifsUtils.nttime(System.currentTimeMillis());
        reply.bootTime = 0;
        reply.tokenOff = 128;
        reply.token = DEFAULT_TOKEN;
        reply.tokenLen = (short) reply.token.length;
        reply.negotiateContextOff = 0;

        return reply;
    }

    public static NegprotReply smb202() {
        NegprotReply reply = new NegprotReply();
        reply.securityMode = SMB2_NEGOTIATE_SIGNING_ENABLED;
        reply.dialect = NegprotCall.SMB_2_0_2;
        reply.negotiateContextCount = 0;
        reply.serverGuid = SERVER_GUID;
        reply.flags = 0;
        reply.maxTransactSize = 64 << 10;
        reply.maxReadSize = 64 << 10;
        reply.maxWriteSize = 64 << 10;

        reply.curTime = CifsUtils.nttime(System.currentTimeMillis());
        reply.bootTime = 0;
        reply.tokenOff = 128;
        reply.token = DEFAULT_TOKEN;
        reply.tokenLen = (short) reply.token.length;
        reply.negotiateContextOff = 0;

        return reply;
    }

    public static NegprotReply smb210() {
        NegprotReply reply = new NegprotReply();
        reply.securityMode = SMB2_NEGOTIATE_SIGNING_ENABLED;
        reply.dialect = NegprotCall.SMB_2_1_0;
        reply.negotiateContextCount = 0;
        reply.serverGuid = SERVER_GUID;
        // 设置 SMB2_GLOBAL_CAP_LARGE_MTU，客户端需开启 EnableLargeMtu
        reply.flags = SMB2_CAP_LARGE_MTU;
        reply.maxTransactSize = 8 << 20;
        reply.maxReadSize = 8 << 20;
        reply.maxWriteSize = 8 << 20;

        reply.curTime = CifsUtils.nttime(System.currentTimeMillis());
        reply.bootTime = 0;
        reply.tokenOff = 128;
        reply.token = DEFAULT_TOKEN;
        reply.tokenLen = (short) reply.token.length;
        reply.negotiateContextOff = 0;

        return reply;
    }

    public static NegprotReply smb302() {
        NegprotReply reply = new NegprotReply();
        reply.securityMode = SMB2_NEGOTIATE_SIGNING_ENABLED;
        reply.dialect = NegprotCall.SMB_3_0_2;
        reply.negotiateContextCount = 0;
        reply.serverGuid = SERVER_GUID;
        //SMB2_CAP_PERSISTENT_HANDLES 开启open时CreateDurableV2功能
        reply.flags = SMB2_CAP_LARGE_MTU | SMB2_CAP_PERSISTENT_HANDLES | SMB2_CAP_MULTI_CHANNEL;
        reply.maxTransactSize = 8 << 20;
        reply.maxReadSize = 8 << 20;
        reply.maxWriteSize = 8 << 20;

        reply.curTime = CifsUtils.nttime(System.currentTimeMillis());
        reply.bootTime = 0;
        reply.tokenOff = 128;
        reply.token = DEFAULT_TOKEN;
        reply.tokenLen = (short) reply.token.length;
        reply.negotiateContextOff = 0;

        return reply;
    }


    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset + 2;
        buf.setShortLE(start, securityMode);
        buf.setShortLE(start + 2, dialect);
        buf.setShortLE(start + 4, negotiateContextCount);
        buf.setBytes(start + 6, serverGuid);
        buf.setIntLE(start + 22, flags);
        buf.setIntLE(start + 26, maxTransactSize);
        buf.setIntLE(start + 30, maxReadSize);
        buf.setIntLE(start + 34, maxWriteSize);
        buf.setLongLE(start + 38, curTime);
        buf.setLongLE(start + 46, bootTime);
        buf.setShortLE(start + 54, tokenOff);
        buf.setShortLE(start + 56, tokenLen);
        buf.setIntLE(start + 58, negotiateContextOff);
        buf.setBytes(start + 62, token);
        structSize = 65;
        super.writeStruct(buf, offset);
        return 64 + token.length;
    }

}
