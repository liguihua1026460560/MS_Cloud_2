package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;

import java.util.LinkedList;
import java.util.List;

public class ListMountRecordReply extends RpcReply {
    public ListMountRecordReply(SunRpcHeader header) {
        super(header);
    }

    public int valueFollow;
    public List<MountRecord> mountRecords = new LinkedList<>();

    public static class MountRecord {
        public int hostNameLen;
        public String hostName;
        public int dirNameLen;
        public String dirName;
        public int valueFollow;

        public MountRecord(String hostName, String dirName, int valueFollow) {

            this.hostName = hostName;
            this.hostNameLen = hostName.length();
            this.dirName = dirName;
            this.dirNameLen = dirName.length();
            this.valueFollow = valueFollow;
        }

        public int writeStruct(ByteBuf buf, int offset) {
            int start = offset;
            buf.setInt(offset, hostNameLen);
            offset += 4;
            buf.setBytes(offset, hostName.getBytes());
            int fillLen = (hostNameLen + 3) / 4 * 4;
            offset += fillLen;
            buf.setInt(offset, dirNameLen);
            offset += 4;
            buf.setBytes(offset, dirName.getBytes());
            fillLen = (dirNameLen + 3) / 4 * 4;
            offset += fillLen;
            buf.setInt(offset, valueFollow);
            offset += 4;
            return offset - start;
        }

        public int getSize() {
            return 4 + ((hostNameLen + 3) / 4 * 4) + 4 + ((dirNameLen + 3) / 4 * 4) + 4;
        }
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, valueFollow);
        offset += 4;
        if (valueFollow == 1) {
            for (MountRecord entry : mountRecords) {
                offset += entry.writeStruct(buf, offset);
            }
        }

        return offset - start;
    }

    public int getSize() {
        int size = 4;
        if (valueFollow == 1) {
            for (MountRecord entry : mountRecords) {
                size += entry.getSize();
            }
        }
        return size;
    }
}
