package com.macrosan.filesystem.cifs.rpc.witness.pdu.ack;

import com.macrosan.filesystem.cifs.rpc.Session;
import com.macrosan.filesystem.cifs.rpc.pdu.RPCHeader;
import com.macrosan.filesystem.cifs.rpc.pdu.ack.Ack;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.call.OrphanedCall;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

import static com.macrosan.filesystem.cifs.rpc.RPCConstants.FAULT;


@Log4j2
@EqualsAndHashCode(callSuper = true)
@Data
@ToString(callSuper = true)
public class OrphanedAck extends Ack {

    public Header header;

    public OrphanedAck(OrphanedCall call, Session session) {
        header = new Header(call, session);

    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        header.writeStruct(buf, offset);
        return header.fragLength;
    }

    public class Header extends RPCHeader {
        public int allocHint;
        public short contextId;
        public byte cancleCount;
        public byte faultFlags;
        public int status;
        public int reserved;

        public Header(OrphanedCall call, Session session) {
            super(call);
            packetType = FAULT;
            allocHint = 24;
            contextId = 0;
            cancleCount = 0;
            faultFlags = 0x00;
            status = 0x000006ba;
            reserved = 0;
            // 更新 fragLength
            fragLength = 32;
        }

        public int writeStruct(ByteBuf buf, int offset) {
            int start = offset + super.writeStruct(buf, offset);
            buf.setIntLE(start, allocHint);
            buf.setShortLE(start + 4, contextId);
            buf.setByte(start + 6, cancleCount);
            buf.setByte(start + 7, faultFlags);
            buf.setIntLE(start + 8, status);
            buf.setIntLE(start + 12, reserved);

            return start + 16 - offset;
        }
    }
}
