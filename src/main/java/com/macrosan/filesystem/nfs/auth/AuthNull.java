package com.macrosan.filesystem.nfs.auth;

import com.macrosan.filesystem.nfs.RpcCallHeader;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@Data
@EqualsAndHashCode(callSuper = false)
public class AuthNull extends Auth {
    @Override
    public int auth(RpcCallHeader header, ByteBuf msg, int offset) {
        return 0;
    }

}
