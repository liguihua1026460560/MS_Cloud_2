package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.FAttr3;
import com.macrosan.filesystem.nfs.types.SimpleAttr;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class ReNameReply extends RpcReply {
    public int status;
    public int fromBeforeFollows = 1;
    public int fromAfterFollows = 1;
    public SimpleAttr fromBeforeAttr = new SimpleAttr();
    public FAttr3 fromAttr = new FAttr3();

    public int toBeforeFollows = 1;
    public SimpleAttr toBeforeAttr = new SimpleAttr();
    public int toAfterFollows = 1;
    public FAttr3 toAttr = new FAttr3();

    public ReNameReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, status);
        buf.setInt(offset + 4, fromBeforeFollows);
        offset += 8;
        if (fromBeforeFollows == 1) {
            offset+=fromBeforeAttr.writeStruct(buf,offset);
        }
        buf.setInt(offset, fromAfterFollows);
        offset += 4;
        if (fromAfterFollows==1){
            offset+=fromAttr.writeStruct(buf,offset);
        }
        buf.setInt(offset , toBeforeFollows);
        offset += 4;

        if (toBeforeFollows == 1) {
            offset+=toBeforeAttr.writeStruct(buf,offset);
        }
        buf.setInt(offset , toAfterFollows);
        offset += 4;
        if (toAfterFollows==1){
            offset+=toAttr.writeStruct(buf,offset);
        }


        return offset - start;
    }

    public static void changeToErrorReply(ReNameReply reNameReply, int errorNum){
        reNameReply.status = errorNum;
        reNameReply.fromBeforeFollows = 0;
        reNameReply.fromAfterFollows = 0;
        reNameReply.toBeforeFollows = 0;
        reNameReply.toAfterFollows = 0;
    }
}
