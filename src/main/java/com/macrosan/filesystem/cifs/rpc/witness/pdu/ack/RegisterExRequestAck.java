package com.macrosan.filesystem.cifs.rpc.witness.pdu.ack;

import com.macrosan.filesystem.cifs.rpc.Session;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.call.RegisterExRequestCall;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

import static com.macrosan.filesystem.cifs.rpc.witness.handler.WitnessHandler.contextHandle;

public class RegisterExRequestAck extends WitnessAck {
    public Stub stub;

    public RegisterExRequestAck(RegisterExRequestCall request, Session session) {
        header = new Header(request.header);
        header.allocHint = 24;
        header.fragLength = 80;
        header.authLength = 16;
        stub = new Stub();
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
        public byte[] handle = new byte[20];
        public byte[] error = new byte[4];
        public byte[] authPadding = new byte[8];
        public long auth = 0x08050a;

        public Stub() {
            handle = ByteBuffer.allocate(20).putInt(contextHandle.incrementAndGet()).array();
        }

        public int writeStruct(ByteBuf buf, int offset) {
            buf.setBytes(offset, handle);
            buf.setBytes(offset + 20, error);
            buf.setBytes(offset + 24, authPadding);
            buf.setLongLE(offset + 32, auth);
            return 40;
        }
    }
}
