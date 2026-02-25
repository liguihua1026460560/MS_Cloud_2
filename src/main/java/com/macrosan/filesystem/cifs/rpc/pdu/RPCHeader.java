package com.macrosan.filesystem.cifs.rpc.pdu;

import com.macrosan.filesystem.ReadStruct;
import io.netty.buffer.ByteBuf;
import lombok.Data;

@Data
public abstract class RPCHeader implements ReadStruct{
    // 公共头部长度 16 字节
    // DCE/RPC 头部字段
    public byte version;   // 主要版本
    public byte versionMinor;   // 次要版本
    public byte packetType;     // 报文类型
    public byte flags;          // 标志位
    public byte[] dataRepresentation = new byte[4]; // 数据表示
    public short fragLength;    // 报文长度
    public short authLength;    // 认证数据长度
    public int callId;          // 调用 ID

    public RPCHeader() {}

    public RPCHeader(RPCHeader header) {
        version = header.version;
        versionMinor = header.versionMinor;
//        packetType = BIND_ACK.code;
        flags = header.flags;
        dataRepresentation = header.dataRepresentation;
        fragLength = 16;
        authLength = 0;
        callId = header.callId;
    }

    public int readStruct(ByteBuf buf, int offset) {
        version = buf.getByte(offset);
        versionMinor = buf.getByte(offset + 1);
        packetType = buf.getByte(offset + 2);;
        flags = buf.getByte(offset + 3);
        buf.getBytes(offset + 4, dataRepresentation);
        fragLength = buf.getShortLE(offset + 8);
        authLength = buf.getShortLE(offset + 10);
        callId = buf.getIntLE(offset + 12);

        return 16;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setByte(offset, version);
        buf.setByte(offset + 1, versionMinor);
        buf.setByte(offset + 2, packetType);
        buf.setByte(offset + 3, flags);
        buf.setBytes(offset + 4, dataRepresentation);
        buf.setShortLE(offset + 8, fragLength);
        buf.setShortLE(offset + 10, authLength);
        buf.setIntLE(offset + 12, callId);

        return 16;
    }

}
