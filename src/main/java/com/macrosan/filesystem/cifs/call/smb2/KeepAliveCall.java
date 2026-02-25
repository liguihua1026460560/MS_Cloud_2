package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/d939504d-57e2-4c0e-8ad5-1678b6fccca1
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class KeepAliveCall extends SMB2Body {
    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int size = super.readStruct(buf, offset);
        return size + 2;
    }
}
