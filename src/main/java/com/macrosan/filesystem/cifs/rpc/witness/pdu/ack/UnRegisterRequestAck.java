package com.macrosan.filesystem.cifs.rpc.witness.pdu.ack;

import com.macrosan.filesystem.cifs.rpc.Session;
import com.macrosan.filesystem.cifs.rpc.pdu.ack.Ack;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.call.UnRegisterRequestCall;
import io.netty.buffer.ByteBuf;

public class UnRegisterRequestAck extends WitnessAck {
    public Stub stub;

    public UnRegisterRequestAck(UnRegisterRequestCall request, Session session, boolean status) {
        header = new Header(request.header);
        header.allocHint = 4;
        header.fragLength = 64;
        header.authLength = 16;
        stub = new Stub(status);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset + writeHeader(buf, offset);
        start += writeStub(buf, start);
        start += writeSign(buf, start);
        return start - offset;
    }

    public int writeHeader(ByteBuf buf, int offset) {
        return header.writeStruct(buf, offset);
    }

    public int writeStub(ByteBuf buf, int offset) {
        return stub.writeStruct(buf, offset);
    }

    public int writeSign(ByteBuf buf, int offset) {
        buf.setBytes(offset, sign);
        return 16;
    }

    public class Stub {
        public int error = 0;
        public byte[] padding = new byte[12];
        public long auth = 0x0c050a;
        public Stub(boolean status) {
            if (!status) {
                error = 0x00000490;
            }
        }
        public int writeStruct(ByteBuf buf, int offset) {
            buf.setIntLE(offset, error);
            buf.setBytes(offset + 4, padding);
            buf.setLongLE(offset + 16, auth);
            return 24;
        }
    }
}
