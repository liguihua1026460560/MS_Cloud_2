package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/14f9d050-27b2-49df-b009-54e08e8bf7b5
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class NotifyReply extends SMB2Body {
    short bufOff = 72;
    int bufLen;
    public List<NotifyInfo> infoList = new LinkedList<>();

    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 9;
        super.writeStruct(buf, offset);
        buf.setShortLE(offset + 2, bufOff);

        bufLen = 0;
        for (NotifyInfo info : infoList) {
            bufLen += info.writeStruct(buf, offset + 8 + bufLen);
        }

        if ((bufLen & 3) != 0) {
            bufLen = (bufLen & ~3) + 4;
        }
        buf.setIntLE(offset + 4, bufLen);

        return bufLen + 8;
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/634043d7-7b39-47e9-9e26-bda64685e4c9
    public static class NotifyInfo {
        public int next;
        public NotifyAction action;
        public String fileName;

        public int writeStruct(ByteBuf buf, int offset) {
            buf.setIntLE(offset, next);
            buf.setIntLE(offset + 4, action.action);
            byte[] file = fileName.getBytes(StandardCharsets.UTF_16LE);
            buf.setIntLE(offset + 8, file.length);
            buf.setBytes(offset + 12, file);
            int size = 12 + file.length;
            if ((size & 3) != 0) {
                size = (size & ~3) + 4;
            }
            return size;
        }

        public int size() {
            byte[] file = fileName.getBytes(StandardCharsets.UTF_16LE);
            int size = 12 + file.length;
            if ((size & 3) != 0) {
                size = (size & ~3) + 4;
            }
            return size;
        }
    }


    public enum NotifyAction {
        FILE_ACTION_ADDED(1),
        FILE_ACTION_REMOVED(2),
        FILE_ACTION_MODIFIED(3),
        FILE_ACTION_RENAMED_OLD_NAME(4),
        FILE_ACTION_RENAMED_NEW_NAME(5),
        FILE_ACTION_ADDED_STREAM(6),
        FILE_ACTION_REMOVED_STREAM(7),
        FILE_ACTION_MODIFIED_STREAM(8),
        FILE_ACTION_REMOVED_BY_DELETE(9),
        FILE_ACTION_ID_NOT_TUNNELED(10),
        FILE_ACTION_TUNNELLED_ID_COLLIS(11);

        public final int action;

        NotifyAction(int action) {
            this.action = action;
        }
    }
}
