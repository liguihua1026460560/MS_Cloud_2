package com.macrosan.filesystem.cifs.rpc.witness.pdu.ack;

import com.macrosan.filesystem.cifs.rpc.pdu.RPCHeader;
import com.macrosan.filesystem.cifs.rpc.pdu.ack.Ack;
import com.macrosan.filesystem.cifs.rpc.pdu.call.RequestCall;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static com.macrosan.filesystem.cifs.rpc.RPCConstants.RESPONSE;

public abstract class WitnessAck extends Ack {

    public Header header;
    public byte[] sign = new byte[16];  // 16 Byte

    public abstract int writeStruct(ByteBuf buf, int offset);
    public abstract int writeHeader(ByteBuf buf, int offset);
    public abstract int writeStub(ByteBuf buf, int offset);

    @EqualsAndHashCode(callSuper = true)
    @Data
    @ToString(callSuper = true)
    public static class Header extends RPCHeader {
        public int allocHint;    // 数据量
        public short contextId;
        public short cancelCount;
        public Header(RequestCall.Header header) {
            super(header);
            flags = 0x03;
            packetType = RESPONSE;
            contextId = header.contextId;
            cancelCount = 0;
        }

        public int writeStruct(ByteBuf buf, int offset) {
            int start = super.writeStruct(buf, offset) + offset;

            buf.setIntLE(start, allocHint);
            buf.setShortLE(start + 4, contextId);
            buf.setShortLE(start + 6, cancelCount);
            return 24;
        }
    }
}
