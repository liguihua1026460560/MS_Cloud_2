package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/832d2130-22e8-4afb-aafd-b30bb0901798
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString
public class TreeConnectCall extends SMB2Body {
    public short flags;
    public short pathOff;
    public short pathLen;
    public byte[] path;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = super.readStruct(buf, offset) + offset;
        flags = buf.getShortLE(start);
        pathOff = buf.getShortLE(start + 2);
        pathLen = buf.getShortLE(start + 4);
        path = new byte[pathLen];
        buf.getBytes(pathOff + 4, path);
        return pathLen + 8;
    }

}
