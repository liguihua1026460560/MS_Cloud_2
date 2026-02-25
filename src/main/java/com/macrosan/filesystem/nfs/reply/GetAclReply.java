package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.FAttr3;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;

@ToString
public class GetAclReply extends RpcReply {
    public int status;
    public int attrFollow = 1;
    public FAttr3 attr = new FAttr3();
    public int mask;
    public int aclCount;
    public int totalAclEntries;
    public List<AclEntry> entries = new LinkedList<>();
    public int defaultAclCount = 0;
    public int totalDefaultEntries = 0;
    public List<AclEntry> defaultEntries = new LinkedList<>();

    public GetAclReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;

        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, status);
        buf.setInt(offset + 4, attrFollow);
        offset += 8;
        if (attrFollow == 1) {
            offset += attr.writeStruct(buf, offset);
        }

        buf.setInt(offset, mask);
        buf.setInt(offset + 4, aclCount);
        buf.setInt(offset + 8, totalAclEntries);
        offset += 12;
        for (AclEntry aclEntry : entries) {
            offset += aclEntry.writeStruct(buf, offset);
        }
        buf.setInt(offset, defaultAclCount);
        buf.setInt(offset + 4, totalDefaultEntries);
        offset += 8;
        for (AclEntry aclEntry : defaultEntries) {
            offset += aclEntry.writeStruct(buf, offset);
        }

        return offset - start;
    }

    @ToString
    public static class AclEntry {
        public int type;
        public int uid;
        public int permissions;

        public int writeStruct(ByteBuf buf, int offset) {
            buf.setInt(offset, type);
            buf.setInt(offset + 4, uid);
            buf.setInt(offset + 8, permissions);
            return 12;
        }

        public int readStruct(ByteBuf buf, int offset) {
            this.type = buf.getInt(offset);
            this.uid = buf.getInt(offset + 4);
            this.permissions = buf.getInt(offset + 8);
            return 12;
        }

        public AclEntry() {

        }

        public AclEntry(int type, int uid, int permissions) {
            this.type = type;
            this.uid = uid;
            this.permissions = permissions;
        }
    }
}
