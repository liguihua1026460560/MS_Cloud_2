package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.nfs.types.FAttr3;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_STALE;

@ToString
public class AccessReply extends RpcReply {

    public int status;

    public int attrBefore = 1;

    public FAttr3 stat = new FAttr3();

    public int access;

    public AccessReply(SunRpcHeader header) {
        super(header);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);

        buf.setInt(offset, status);

        buf.setInt(offset + 4, attrBefore);
        if (status == NFS3ERR_STALE) {
            return offset + 8 - start;
        }
        if (attrBefore!=0){
            offset += stat.writeStruct(buf, offset + 8);
        }

        buf.setInt(offset + 8, access);

        return offset + 12 - start;
    }

    public static int SPEC_USER   = 0b0000_0000_0000_0000_0000_1000_0000_0000;
    public static int SPEC_GROUP  = 0b0000_0000_0000_0000_0000_0100_0000_0000;
    public static int SPEC_VTX    = 0b0000_0000_0000_0000_0000_0010_0000_0000;
    public static int USER_READ   = 0b0000_0000_0000_0000_0000_0001_0000_0000;
    public static int USER_WRITE  = 0b0000_0000_0000_0000_0000_0000_1000_0000;
    public static int USER_EXEC   = 0b0000_0000_0000_0000_0000_0000_0100_0000;
    public static int GROUP_READ  = 0b0000_0000_0000_0000_0000_0000_0010_0000;
    public static int GROUP_WRITE = 0b0000_0000_0000_0000_0000_0000_0001_0000;
    public static int GROUP_EXEC  = 0b0000_0000_0000_0000_0000_0000_0000_1000;
    public static int OTHER_READ  = 0b0000_0000_0000_0000_0000_0000_0000_0100;
    public static int OTHER_WRITE = 0b0000_0000_0000_0000_0000_0000_0000_0010;
    public static int OTHER_EXEC  = 0b0000_0000_0000_0000_0000_0000_0000_0001;

    public static int REFRESH_USER_READ   = 0b1111_1111_1111_1111_1111_1110_1111_1111;
    public static int REFRESH_USER_WRITE  = 0b1111_1111_1111_1111_1111_1111_0111_1111;
    public static int REFRESH_USER_EXEC   = 0b1111_1111_1111_1111_1111_1111_1011_1111;
    public static int REFRESH_GROUP_READ  = 0b1111_1111_1111_1111_1111_1111_1101_1111;
    public static int REFRESH_GROUP_WRITE = 0b1111_1111_1111_1111_1111_1111_1110_1111;
    public static int REFRESH_GROUP_EXEC  = 0b1111_1111_1111_1111_1111_1111_1111_0111;
    public static int REFRESH_OTHER_READ  = 0b1111_1111_1111_1111_1111_1111_1111_1011;
    public static int REFRESH_OTHER_WRITE = 0b1111_1111_1111_1111_1111_1111_1111_1101;
    public static int REFRESH_OTHER_EXEC  = 0b1111_1111_1111_1111_1111_1111_1111_1110;
}
