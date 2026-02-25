package com.macrosan.filesystem.cifs.rpc.witness.pdu.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.cifs.rpc.pdu.call.RequestCall;
import io.netty.buffer.ByteBuf;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class AsyncNotifyRequestCall extends RequestCall implements ReadStruct {
    public Stub stub;
    public byte[] message;
    public byte[] sign = new byte[16];
    public byte[] seqNum = new byte[4];

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int readSize = header.readStruct(buf, offset);
        stub = new Stub(header.allocHint);
        readSize += stub.readStruct(buf, offset + readSize);
        message = new byte[header.fragLength - 16];
        buf.getBytes(0, message);
        buf.getBytes(readSize, sign);
        buf.getBytes(readSize + 12, seqNum);
        return readSize + 16;
    }


    public class Stub implements ReadStruct {
        public int length;
        public byte[] contextHandle = new byte[20];
        public byte[] verificationTrailer = new byte[60];
        public byte[] authPadding = new byte[12];
        public long auth;

        public Stub(int length) {
            this.length =  length;
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            buf.getBytes(offset, contextHandle);
            if (length > 20) {
                buf.getBytes(offset + 20, verificationTrailer);
                auth = buf.getLongLE(length);
                return length + 8;
            }else {
                buf.getBytes(length, authPadding);
                auth = buf.getLongLE(length + 12);
                return length + 20;
            }
        }
    }
}
