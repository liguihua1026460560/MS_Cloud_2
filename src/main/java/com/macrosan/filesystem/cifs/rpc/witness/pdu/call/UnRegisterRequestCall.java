package com.macrosan.filesystem.cifs.rpc.witness.pdu.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.cifs.rpc.pdu.call.RequestCall;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.AuthInfo;
import io.netty.buffer.ByteBuf;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class UnRegisterRequestCall extends RequestCall implements ReadStruct {
    public Stub stub = new Stub();
    public byte[] message;
    public byte[] sign = new byte[16];
    public byte[] seqNum = new byte[4];
    public AuthInfo authInfo = new AuthInfo();

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int readSize = header.readStruct(buf, offset);
        stub.length = header.allocHint;
        stub.readStruct(buf, offset + readSize);

        authInfo.readStruct(buf, offset + header.fragLength - 24);

        message = new byte[header.fragLength - 16];
        buf.getBytes(0, message);
        buf.getBytes(offset + header.fragLength - 16, sign);
        buf.getBytes(offset + header.fragLength - 4, seqNum);

        return header.fragLength;
    }


    public class Stub implements ReadStruct {
        public int length;
        public byte[] contextHandle = new byte[20];
        public byte[] verificationTrailer;

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            buf.getBytes(offset, contextHandle);
            verificationTrailer = new byte[length - contextHandle.length];
            buf.getBytes(offset + 20, verificationTrailer);

            return length;
        }
    }
}
