package com.macrosan.filesystem.cifs.rpc.witness.pdu.ack;

import com.macrosan.filesystem.cifs.rpc.Session;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.call.AsyncNotifyRequestCall;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

import static com.macrosan.filesystem.cifs.rpc.RPCConstants.WITNESS_NOTIFY_RESOURCE_CHANGE;
import static com.macrosan.filesystem.cifs.rpc.RPCConstants.WITNESS_RESOURCE_STATE_UNAVAILABLE;

public class AsyncNotifyRequestOkFirstAck extends WitnessAck {
    public Stub stub;

    public AsyncNotifyRequestOkFirstAck(AsyncNotifyRequestCall request, Session session, String ip) {
        header = new Header(request.header);
        header.allocHint = 64;
        header.fragLength = 112;
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

        public int referenceId = 0x00020000;
        public int type = WITNESS_NOTIFY_RESOURCE_CHANGE;
        public int length = 36;
        public int num = 1;
        public int reserved = 0;

        // message
        public int messageReferenceId = 0x00020004;
        public int maxCount = 36;  // 这个参数不知道怎么算的
        public int messageLength = 36;

        public int messageType = WITNESS_RESOURCE_STATE_UNAVAILABLE;
        public String name = "255.255.255.255";     // 28字节 怎么知道哪个节点down了 172.51.5.10
        public byte[] supply;
        public long auth = 0x00050a;


        public int writeStruct(ByteBuf buf, int offset) {
            buf.setIntLE(offset, referenceId);
            buf.setIntLE(offset + 4, type);
            buf.setIntLE(offset + 8, length);
            buf.setIntLE(offset + 12, num);

            buf.setIntLE(offset + 16, messageReferenceId);
            buf.setIntLE(offset + 20, maxCount);
            buf.setIntLE(offset + 24, messageLength);
            buf.setIntLE(offset + 28, messageType);

            buf.setBytes(offset + 32, name.getBytes(StandardCharsets.UTF_16LE));
            supply = new byte[(int)maxCount - messageLength];
            buf.setBytes(offset + 32 + messageLength, supply);

            buf.setLongLE(offset + stubLength, auth);
            return stubLength + 8;
        }
    }
}
