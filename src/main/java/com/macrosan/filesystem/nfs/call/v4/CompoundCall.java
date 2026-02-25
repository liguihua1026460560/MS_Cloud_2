package com.macrosan.filesystem.nfs.call.v4;


import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.RpcCallHeader;
import com.macrosan.filesystem.nfs.types.CompoundContext;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;


@NoArgsConstructor
@Data
@ToString
public class CompoundCall  implements ReadStruct{
    public RpcCallHeader callHeader;
    private byte[] tagContent;
    private int minorVersion;
    private int callbackIdent;
    private int count;
    //    public CompoundCall compoundCall;
    public CompoundContext context;
    public List<CompoundCall> callList = new ArrayList<>();

    public int getCount() {
        return count;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int head = offset;
        int tagLen = buf.getInt(offset);
        offset += 4;
        tagContent = new byte[tagLen];
        buf.getBytes(offset, tagContent);
        offset += (tagLen + 3) / 4 * 4;
        minorVersion = buf.getInt(offset);
        count = buf.getInt(offset + 4);
        offset += 8;
        context = new CompoundContext();
        context.minorVersion = minorVersion;
        return offset - head;
    }

    public int getMinorVersion() {
        return minorVersion;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        return 0;
    }

    public int writeStruct0(ByteBuf buf, int offset) {
        int head = offset;
        offset += callHeader.writeStruct(buf, offset);
        int tagLen = tagContent.length;
        buf.setInt(offset, tagLen);
        offset += 4;
        buf.setBytes(offset, tagContent);
        offset += (tagLen + 3) / 4 * 4;
        buf.setInt(offset, minorVersion);
        buf.setInt(offset + 4, callbackIdent);
        buf.setInt(offset + 8, count);
        offset += 12;
        for (CompoundCall compoundCall : callList) {
            offset += compoundCall.writeStruct(buf, offset);
        }
        return offset - head;
    }
}
