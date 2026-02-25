package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
//IOCTLSubReply.ObjectId1
public class FsObjectIdInfo extends GetInfoReply.Info {
    //只要路径名称不变，同名对象新建文件的objId相同
    public IOCTLSubReply.ObjectId1 objectId;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        return objectId.writeStruct(buf, offset);
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int size() {
        return 64;
    }


}
