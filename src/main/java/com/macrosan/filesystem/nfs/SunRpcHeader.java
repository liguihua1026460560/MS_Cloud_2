package com.macrosan.filesystem.nfs;

import io.netty.buffer.ByteBuf;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@ToString
@Slf4j
public class SunRpcHeader {
    //0x80
    public byte head;

    public int len;
    public int id;
    // 0 call
    // 1 reply
    public int msgType;

    public static final int SIZE = 12;

    public int read(ByteBuf buf, int offset, boolean isUdp) {
        if (!isUdp) {
            return readStruct(buf, offset);
        }
        return readStructUdp(buf, offset);
    }

    public int readStructUdp(ByteBuf buf, int offset) {
        id = buf.getInt(0);
        len = buf.writerIndex();
        msgType = buf.getInt(4);
        return 8;
    }

    public int readStruct(ByteBuf buf, int offset) {
        head = buf.getByte(offset);
        if ((head & 0xff) != 0x80) {
            return -1;
        }

        len = buf.getInt(offset) & 0x00ffffff;

        id = buf.getInt(4);
        msgType = buf.getInt(8);

        return SIZE;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        buf.writerIndex(offset);
        int tmp = ((head & 0xff) << 24) | len;
        buf.writeInt(tmp);
        buf.writeInt(id);
        buf.writeInt(msgType);

        return SIZE;
    }

    //udp协议无head
    public int writeStructUdp(ByteBuf buf, int offset) {

        buf.writerIndex(offset);
        buf.writeInt(id);
        buf.writeInt(msgType);
        return SIZE - 4;
    }

    public static SunRpcHeader newCallHeader(int id) {
        SunRpcHeader callHeader = new SunRpcHeader();
        callHeader.msgType = 0;
        callHeader.head = (byte) 0x80;
        callHeader.id = id;
        return callHeader;
    }

    public static SunRpcHeader newReplyHeader(int id) {
        SunRpcHeader replyHeader = new SunRpcHeader();
        replyHeader.msgType = 1;
        replyHeader.head = (byte) 0x80;
        replyHeader.id = id;
        return replyHeader;
    }

}
