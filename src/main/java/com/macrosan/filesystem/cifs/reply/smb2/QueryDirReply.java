package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.types.smb2.QueryDirInfo;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.LinkedList;
import java.util.ListIterator;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/4f75351b-048c-4a0c-9ea3-addd55a71956
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class QueryDirReply extends SMB2Body {
    short off;
    int len;
    LinkedList<QueryDirInfo> infoList = new LinkedList<>();

    public int writeStruct(ByteBuf buf, int offset) {
        structSize = 9;
        int start = offset + super.writeStruct(buf, offset);
        off = (short) (SMB2Header.SIZE + 8);

        int infoOff = start + 6;

        ListIterator<QueryDirInfo> listIterator = infoList.listIterator();
        while (listIterator.hasNext()) {
            QueryDirInfo info = listIterator.next();
            boolean isNotLast = listIterator.hasNext();
            info.nextEntryOffset = isNotLast ? info.size() : 0;
            infoOff += info.writeStruct(buf, infoOff, isNotLast);
        }

        len = infoOff - start - 6;

        buf.setShortLE(start, off);
        buf.setIntLE(start + 2, len);

        return infoOff - offset;
    }
}
