package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

@Log4j2
@EqualsAndHashCode(callSuper = true)
@Data
@ToString
public class FileLinkInfo extends GetInfoReply.Info {

    public boolean fileExist;
    public long rootDirectory;
    public int fileNameLen;
    public char[] fileName;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        fileExist = buf.getBoolean(offset);
        offset += 7;//reserved
        rootDirectory = buf.getLongLE(offset + 1);
        fileNameLen = buf.getIntLE(offset + 9);
        fileName = new char[fileNameLen / 2];
        for (int i = 0; i < fileName.length; i++) {
            fileName[i] = (char) buf.getShortLE(offset + 13 + i * 2);
        }
        return 20 + fileNameLen;
    }

    @Override
    public int size() {
        return 20 + fileNameLen;
    }
}
