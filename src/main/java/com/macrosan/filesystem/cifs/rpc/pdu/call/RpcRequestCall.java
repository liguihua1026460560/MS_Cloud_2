package com.macrosan.filesystem.cifs.rpc.pdu.call;

import com.macrosan.filesystem.cifs.rpc.pdu.RPCHeader;
import io.netty.buffer.ByteBuf;

public class RpcRequestCall {
    public DsRoleHeader header = new DsRoleHeader();
    public byte[] stubData;
    public static class DsRoleHeader extends RPCHeader {
        public int allocHint;
        public short contextId;
        public short opnum;
        public short dsRoleLevel = 0;

        public int readStruct(ByteBuf buf, int offset) {
            int readSize = super.readStruct(buf, offset);
            offset += readSize;
            allocHint = buf.getIntLE(offset);
            contextId = buf.getShortLE(offset + 4);
            opnum = buf.getShortLE(offset + 6);
            return readSize + 8;
        }
    }
}
