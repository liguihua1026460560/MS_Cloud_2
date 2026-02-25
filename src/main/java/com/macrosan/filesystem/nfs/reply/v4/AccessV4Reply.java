package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class AccessV4Reply extends CompoundReply {
    //请求权限的类型
    public int supportedTypes;
    //请求拥有的权限
    public int accessRights;


    public AccessV4Reply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        buf.setInt(offset + 8, supportedTypes);
        buf.setInt(offset + 12, accessRights);
        return 16;
    }
}
