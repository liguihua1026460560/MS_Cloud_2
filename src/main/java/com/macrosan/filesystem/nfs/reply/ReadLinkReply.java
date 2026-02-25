package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.FAttr3;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_STALE;

@ToString
public class ReadLinkReply extends RpcReply {

    public int status;
    public int attrFollows = 1;
    public FAttr3 attr = new FAttr3();
    public int dataLen;
    public byte[] data;


    public ReadLinkReply(SunRpcHeader header) {
        super(header);
    }


    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, status);
        buf.setInt(offset + 4, attrFollows);
        offset += 8;
        if (status == NFS3ERR_STALE) {
            return offset - start;
        }
        if (attrFollows != 0) {
            offset += attr.writeStruct(buf, offset);
        }
        buf.setInt(offset, dataLen);
        offset += 4;
        buf.setBytes(offset, data);
        int len = (dataLen + 3) / 4 * 4;
        offset += len;
        return offset - start;

    }
}
