package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString
@Data
@EqualsAndHashCode(callSuper = true)
/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/dd34e26c-a75e-47fa-aab2-6efc27502e96
 */
public class TreeConnectReply extends SMB2Body {
    public byte shareType;
    public int flags;
    public int capabilities;
    public int maxAccess;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 16;
        int start = super.writeStruct(buf, offset) + offset;

        buf.setByte(start, shareType);
        buf.setIntLE(start + 2, flags);
        buf.setIntLE(start + 6, capabilities);
        buf.setIntLE(start + 10, maxAccess);

        return 16;
    }

    public static final byte SMB2_SHARE_TYPE_DISK = 0x01;
    public static final byte SMB2_SHARE_TYPE_PIPE = 0x02;
    public static final byte SMB2_SHARE_TYPE_PRINT = 0x03;

    public static final int SMB2_SHARE_CAP_DFS = 0x8;
    //控制客户端发送CreateDurableV2的flag
    public static final int SMB2_SHARE_CAP_CONTINUOUS_AVAILABILITY = 0x10;
    public static final int SMB2_SHARE_CAP_SCALEOUT = 0x20;
    public static final int SMB2_SHARE_CAP_CLUSTER = 0x40;
    public static final int SMB2_SHARE_CAP_ASYMMETRIC = 0x80;
    public static final int SMB2_SHARE_CAP_REDIRECT_TO_OWNER = 0x100;
}
