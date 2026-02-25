package com.macrosan.filesystem.cifs.rpc.pdu.call;

import com.macrosan.filesystem.cifs.rpc.pdu.RPCHeader;
import io.netty.buffer.ByteBuf;

import static com.macrosan.filesystem.cifs.rpc.RPCConstants.PFC_OBJECT_UUID;

public class RequestCall {
    public Header header = new Header();

    public static class Header extends RPCHeader {
        public int allocHint;    // 数据量
        public short contextId;
        public short opnum;
        public byte[] objectUuid = new byte[16];

        public int readStruct(ByteBuf buf, int offset) {
            int readSize = super.readStruct(buf, offset);
            offset += readSize;
            allocHint = buf.getIntLE(offset);
            contextId = buf.getShortLE(offset + 4);
            opnum = buf.getShortLE(offset + 6);

            boolean hasObjectUuid = (flags & PFC_OBJECT_UUID) != 0;
            if (hasObjectUuid) {
                buf.getBytes(offset + 8, objectUuid);
                return readSize + 24;
            }
            return readSize + 8;
        }
    }
}
