package com.macrosan.filesystem.nfs.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.reply.GetAclReply;
import com.macrosan.filesystem.nfs.types.FH2;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;

@ToString
public class SetAclCall implements ReadStruct {

    public FH2 fh = new FH2();

    public int mask;
    public int aclCount;
    public int totalAclEntry;
    public List<GetAclReply.AclEntry> entries = new LinkedList<>();
    public int defAclCount;
    public int totalDefEntry;
    public List<GetAclReply.AclEntry> defEntries = new LinkedList<>();

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int head = offset;

        offset += fh.readStruct(buf, offset);
        mask = buf.getInt(offset);
        aclCount = buf.getInt(offset + 4);
        totalAclEntry = buf.getInt(offset + 8);
        offset += 12;
        for (int i = 0; i < totalAclEntry; i++) {
            GetAclReply.AclEntry entry = new GetAclReply.AclEntry();
            offset += entry.readStruct(buf, offset);
            entries.add(entry);
        }
        defAclCount = buf.getInt(offset);
        totalDefEntry = buf.getInt(offset + 4);
        offset += 8;
        for (int i = 0; i < totalDefEntry; i++) {
            GetAclReply.AclEntry entry = new GetAclReply.AclEntry();
            offset += entry.readStruct(buf, offset);
            defEntries.add(entry);
        }
        return offset - head;
    }
}
