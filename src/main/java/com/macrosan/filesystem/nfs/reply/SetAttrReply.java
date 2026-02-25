package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.call.SetAttrCall;
import com.macrosan.filesystem.nfs.types.FAttr3;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class SetAttrReply extends RpcReply {

    public int status;
    public int followsOld = 1;
    public SetAttrCall call = new SetAttrCall();

    public int followsNew = 1;
    public FAttr3 attr = new FAttr3();

    public SetAttrReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;

        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, status);
        offset += 4;
        buf.setInt(offset, followsOld);
        offset += 4;
        if (followsOld == 1) {
            buf.setLong(offset, attr.size);
            offset += 8;
            buf.setInt(offset, attr.atime);
            buf.setInt(offset + 4, attr.atimeNano);
            buf.setInt(offset + 8, attr.mtime);
            buf.setInt(offset + 12, attr.mtimeNano);
            offset += 16;
        }
        buf.setInt(offset, followsNew);
        offset += 4;
        if (followsNew==1){
            offset += attr.writeStruct(buf, offset);
        }
        return offset - start;
    }
}
