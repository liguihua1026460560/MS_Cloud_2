package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/26d261db-58d1-4513-a548-074448cbb146
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class FileNetworkOpenInfo extends GetInfoReply.Info {
    public long creationTime;
    public long lastAccessTime;
    public long lastWriteTime;
    public long lastChangeTime;
    public long allocationSize;
    public long endOfFile;
    public int mode;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setLongLE(offset, creationTime);
        buf.setLongLE(offset + 8, lastAccessTime);
        buf.setLongLE(offset + 16, lastWriteTime);
        buf.setLongLE(offset + 24, lastChangeTime);
        buf.setLongLE(offset + 32, allocationSize);
        buf.setLongLE(offset + 40, endOfFile);
        buf.setIntLE(offset + 48, mode);
        return 56;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int size() {
        return 56;
    }
}
