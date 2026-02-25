package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

/**
 * @Author: WANG CHENXING
 * @Date: 2024/8/2
 * @Description: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/52aa0b70-8094-4971-862d-79793f41e6a8
 */

@Log4j2
@EqualsAndHashCode(callSuper = true)
@Data
@ToString
public class FileRenameInfo extends GetInfoReply.Info {
    public boolean replaceIfExists;  // 1byte, true replace, false fail
    public long rootDirectory;       // 8byte, the file handle for the directory to which the new name of the file is relative
    public int fileNameLen;          // 4byte, the file name contained within the FileName field.
    public char[] fileName;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        replaceIfExists = buf.getBoolean(offset);
        rootDirectory = buf.getLongLE(offset + 8);
        fileNameLen = buf.getIntLE(offset + 16);
        fileName = new char[fileNameLen / 2];
        ByteBuf slice = buf.slice(offset + 20, fileNameLen);
        for (int i = 0; i < fileNameLen / 2; i++) {
            fileName[i] = (char) slice.getShortLE(i * 2);
        }
        return 20 + fileNameLen;
    }

    @Override
    public int size() {
        return 0;
    }
}
