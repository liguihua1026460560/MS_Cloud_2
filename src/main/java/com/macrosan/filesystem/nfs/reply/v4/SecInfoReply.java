package com.macrosan.filesystem.nfs.reply.v4;


import com.macrosan.filesystem.nfs.SunRpcHeader;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.RpcAuthType.*;

@ToString
public class SecInfoReply extends CompoundReply {
    public SecInfo[] secInfos;

    public SecInfoReply(SunRpcHeader header) {
        super(header);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setInt(offset, opt);
        buf.setInt(offset + 4, status);
        buf.setInt(offset + 8, secInfos.length);
        offset += 12;
        for (SecInfo secInfo : secInfos) {
            offset += secInfo.writeStruct(buf, offset);
        }
        return offset - start;
    }

    public static class SecInfo {
        public int flavor;
        public RpcSecGssInfo flavorInfo;

        public SecInfo() {
        }

        public int writeStruct(ByteBuf buf, int offset) {
            int start = offset;
            buf.setInt(offset, flavor);
            offset += 4;
            switch (flavor) {
                case RPC_AUTH_NULL:
                case RPC_AUTH_UNIX:
                    break;
                case RPC_AUTH_GSS:
                    offset += flavorInfo.writeStruct(buf, offset);
                    break;
            }
            return offset - start;
        }
    }

    public static class RpcSecGssInfo {
        public byte[] oid;
        public int qop;
        public int service;

        public RpcSecGssInfo() {
        }

        public int writeStruct(ByteBuf buf, int offset) {
            int start = offset;
            buf.setInt(offset, oid.length);
            buf.setBytes(offset + 4, oid);
            offset += 4 + (oid.length + 3) / 4 * 4;
            buf.setInt(offset, qop);
            buf.setInt(offset + 4, service);
            offset += 8;
            return offset - start;
        }
    }
}

