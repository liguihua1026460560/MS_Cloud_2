package com.macrosan.filesystem.cifs.rpc.witness.pdu.ack;

import com.macrosan.filesystem.cifs.rpc.Session;
import com.macrosan.filesystem.cifs.rpc.witness.handler.WitnessHandler;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.call.AsyncNotifyRequestCall;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

import static com.macrosan.filesystem.cifs.rpc.RPCConstants.WITNESS_NOTIFY_RESOURCE_CHANGE;
import static com.macrosan.filesystem.cifs.rpc.RPCConstants.WITNESS_RESOURCE_STATE_UNAVAILABLE;

public class AsyncNotifyRequestTimeOutAck extends WitnessAck {
    public Stub stub;

    public AsyncNotifyRequestTimeOutAck(AsyncNotifyRequestCall request, Session session) {
        header = new Header(request.header);
        boolean isFirst = true;
        if (request.header.allocHint > 20) {
            header.allocHint = 8;
        }else {
            header.allocHint = 12;
            isFirst = false;
        }
        header.fragLength = 64;
        header.authLength = 16;
        stub = new Stub(isFirst);
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
        public boolean isFirst;
        public byte[] nullPointer;
        public int error = 0x000005b4;   // time_out
        public byte[] authPadding;
        public long auth = 0x08050a;

        public Stub(boolean isFirst) {
            this.isFirst = isFirst;
            if (isFirst) {
                nullPointer = new byte[4];
                authPadding = new byte[8];
            }else {
                nullPointer = new byte[8];
                authPadding = new byte[4];
                auth = 0x04050a;
            }
        }
        public int writeStruct(ByteBuf buf, int offset) {
            if (isFirst) {
                buf.setBytes(offset, nullPointer);
                buf.setIntLE(offset + 4, error);
                buf.setBytes(offset + 8, authPadding);
                buf.setLongLE(offset + 16, auth);
                return 24;
            }else {
                buf.setBytes(offset, nullPointer);
                buf.setIntLE(offset + 8, error);
                buf.setBytes(offset + 12, authPadding);
                buf.setLongLE(offset + 16, auth);
                return 24;
            }
        }
    }
}
