package com.macrosan.filesystem.cifs.rpc.witness.pdu.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.cifs.rpc.pdu.call.RequestCall;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.AuthInfo;
import io.netty.buffer.ByteBuf;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class RegisterExRequestCall extends RequestCall implements ReadStruct {
    public Stub stub;
    public AuthInfo authInfo;
    public byte[] message;
    public byte[] sign;
    public byte[] seqNum;

    public RegisterExRequestCall() {
        header = new RequestCall.Header();
        stub = new Stub();
        authInfo = new AuthInfo();
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        header.readStruct(buf, offset);   // 40

        stub.length = header.allocHint;
        int readSize = stub.readStruct(buf, offset + 40);

        authInfo.readStruct(buf, offset + 40 + readSize);

        message = new byte[header.fragLength - 16];
        sign = new byte[16];
        seqNum = new byte[4];

        buf.getBytes(offset, message);
        buf.getBytes(offset + header.fragLength - 16, sign);
        buf.getBytes(offset + header.fragLength - 4, seqNum);

        return header.fragLength;
    }

    public class Stub implements ReadStruct {
        public int length;

        public long version;
        public Pointer netName;
        public byte[] nullPointer;
        public Pointer ipAddress;
        public Pointer clientComputerName;
        public int flags;
        public int timeOut;
        public byte[] reserverd;
        public byte[] padding;

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            int readSize = 0;
            version = buf.getLongLE(offset);

            netName = new Pointer();
            nullPointer = new byte[8];
            ipAddress = new Pointer();
            clientComputerName = new Pointer();
            readSize += netName.readStruct(buf, offset + 8);
            buf.getBytes(offset + readSize + 8, nullPointer);
            readSize += ipAddress.readStruct(buf, offset + readSize + 16);
            readSize += clientComputerName.readStruct(buf, offset + readSize + 16);

            flags = buf.getIntLE(offset + readSize + 16);
            timeOut = buf.getIntLE(offset + readSize + 20);
            reserverd = new byte[length - readSize - 24];
            buf.getBytes(offset + readSize + 24, reserverd);

            padding = new byte[length % 8];
            buf.getBytes(offset + readSize + reserverd.length + 24, padding);

            return length + padding.length;
        }
    }

    public class Pointer implements ReadStruct {
        public long referenceID;
        public long maxCount;
        public long offset_;
        public long actualCount;
        public byte[] data;
        public byte[] supply;

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            referenceID = buf.getLongLE(offset);
            maxCount = buf.getLongLE(offset + 8);
            offset_ = buf.getLongLE(offset + 16);
            actualCount = buf.getLongLE(offset + 24);

            data = new byte[(int) actualCount * 2];
            buf.getBytes(offset + 32, data);

            supply = new byte[(8 - (int)actualCount * 2 % 8) % 8];
            buf.getBytes(offset + (int) actualCount * 2 + 32, supply);


            log.info("size : {}", 32 + (int) actualCount * 2 + supply.length);
            return 32 + (int) actualCount * 2 + supply.length;
        }
    }
}
