package com.macrosan.filesystem.cifs.rpc.witness.pdu.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.cifs.rpc.pdu.call.RequestCall;
import io.netty.buffer.ByteBuf;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class GetInterfaceListRequestCall implements ReadStruct {
    public RequestCall.Header header;
    public Stub stub;
    public byte[] sign = new byte[16];
    public byte[] seqNum = new byte[4];
    public byte[] message;

    public GetInterfaceListRequestCall() {
        header = new RequestCall.Header();
        stub = new Stub();
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int readSize = header.readStruct(buf, offset);

        stub.verificationTrailerLength = header.allocHint;
        stub.paddingLength = header.fragLength - 40 - header.allocHint - 24;

        readSize += stub.readStruct(buf, offset + readSize);
        buf.getBytes(readSize, sign);
        buf.getBytes(readSize + 12, seqNum);
        message = new byte[header.fragLength - 16];
        buf.getBytes(0, message);
        return header.authLength;
    }

    public class Stub implements ReadStruct {
        public int verificationTrailerLength;
        public int paddingLength;
        public byte[] verificationTrailer;
        public byte[] padding;
        public long auth;

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            log.info("verificationTrailerLength : {}, paddingLength : {}", verificationTrailerLength, paddingLength);
            verificationTrailer = new byte[verificationTrailerLength];
            buf.getBytes(offset, verificationTrailer);
            if (paddingLength != 0) {
                padding = new byte[paddingLength];
                buf.getBytes(offset + paddingLength, padding);
            }
            auth = buf.getLongLE(offset + verificationTrailerLength + paddingLength);

            return verificationTrailerLength + paddingLength + 8;
        }
    }
}
