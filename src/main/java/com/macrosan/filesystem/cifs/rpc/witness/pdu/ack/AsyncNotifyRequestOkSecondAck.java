package com.macrosan.filesystem.cifs.rpc.witness.pdu.ack;

import com.macrosan.filesystem.cifs.rpc.Session;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.call.AsyncNotifyRequestCall;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

import static com.macrosan.filesystem.cifs.rpc.RPCConstants.WITNESS_NOTIFY_RESOURCE_CHANGE;
import static com.macrosan.filesystem.cifs.rpc.RPCConstants.WITNESS_RESOURCE_STATE_UNAVAILABLE;

public class AsyncNotifyRequestOkSecondAck extends WitnessAck {
    public Stub stub;

    public AsyncNotifyRequestOkSecondAck(AsyncNotifyRequestCall request, Session session, String ip) {
        header = new Header(request.header);
        header.allocHint = 92;
        header.fragLength = 144;
        header.authLength = 16;
        stub = new Stub();
        stub.stubLength = header.allocHint;
        stub.name = ip;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset + writeHeader(buf, offset);
        start += writeStub(buf, start);
        start += writeSign(buf, start);
        return header.fragLength;
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

    // 节点异常时的信息
    public class Stub {
        public int stubLength;

        public long referenceId = 0x0000000000020000;
        public int type = WITNESS_NOTIFY_RESOURCE_CHANGE;
        public int length = 48;
        public int num = 1;
        public int reserved = 0;

        // message
        public long messageReferenceId = 0x0000000000020004;
        public long maxCount = 48;  // 这个参数不知道怎么算的
        public int messageLength = 36;

        public int messageType = WITNESS_RESOURCE_STATE_UNAVAILABLE;
        public String name = "255.255.255.255";     // 28字节 怎么知道哪个节点down了 172.51.5.10
        public byte[] supply;
        public byte[] authPadding = new byte[4];
        public long auth = 0x04050a;


        public int writeStruct(ByteBuf buf, int offset) {
            buf.setLongLE(offset, referenceId);
            buf.setIntLE(offset + 8, type);
            buf.setIntLE(offset + 12, length);
            buf.setIntLE(offset + 16, num);
            buf.setIntLE(offset + 20, reserved);

            buf.setLongLE(offset + 24, messageReferenceId);
            buf.setLongLE(offset + 32, maxCount);
//            messageLength = name.getBytes(StandardCharsets.UTF_16LE).length;
            buf.setIntLE(offset + 40, messageLength);
            buf.setIntLE(offset + 44, messageType);

            buf.setBytes(offset + 48, name.getBytes(StandardCharsets.UTF_16LE));
            supply = new byte[(int)maxCount - messageLength];
            buf.setBytes(offset + 48 + messageLength, supply);

            buf.setBytes(offset + stubLength, authPadding);
            buf.setLongLE(offset + stubLength + 4, auth);
            return stubLength + 12;
        }
    }
}
