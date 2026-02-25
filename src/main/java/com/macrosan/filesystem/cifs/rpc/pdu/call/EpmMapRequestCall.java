package com.macrosan.filesystem.cifs.rpc.pdu.call;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.cifs.rpc.pdu.Protocol;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.extern.log4j.Log4j2;

import static com.macrosan.filesystem.cifs.rpc.RPCConstants.*;

@Log4j2
public class EpmMapRequestCall extends RequestCall implements ReadStruct {
    public Stub stub = new Stub();

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int readSize = header.readStruct(buf, offset);
        readSize += stub.readStruct(buf, offset + readSize);
        return readSize;
    }

    @Data
    public class Stub {
        public int objectReferentID;
        public byte[] objectUUID = new byte[16];
        public Tower tower;
        public byte[] handle = new byte[20];
        public int maxTowers;

        public int readStruct(ByteBuf buf, int offset) {
            objectReferentID = buf.getIntLE(offset);
            buf.getBytes(offset + 4, objectUUID);

            tower = new Tower();
            tower.ReferentID = buf.getIntLE(offset + 20);
            tower.length1 = buf.getIntLE(offset + 24);
            tower.length2 = buf.getIntLE(offset + 28);
            tower.numFloors = buf.getShortLE(offset + 32);
            tower.floors = new Floor[tower.numFloors];
            int tmpOffset = offset + 34;
            for (int i = 0; i < tower.numFloors; i++) {
                tower.floors[i] = new Floor();
                tower.floors[i].lhsLength = buf.getShortLE(tmpOffset);

                byte type = buf.getByte(tmpOffset + 2);
                switch (type) {
                    case EPM_PROTOCOL_UUID:
                        Protocol.UUID uuidProtocol = new Protocol.UUID(type);
                        tower.floors[i].protocol = uuidProtocol;

                        buf.getBytes(tmpOffset + 3, uuidProtocol.uuid);
                        uuidProtocol.version = buf.getShortLE(tmpOffset + 19);
                        tower.floors[i].rhsLength = buf.getShortLE(tmpOffset + 21);
                        uuidProtocol.versionMinor = buf.getShortLE(tmpOffset + 23);

                        tmpOffset = tmpOffset + 25;
                        break;
                    case EPM_PROTOCOL_NCACN:
                        Protocol.RPC rpcProtocol = new Protocol.RPC(type);
                        tower.floors[i].protocol = rpcProtocol;

                        tower.floors[i].rhsLength = buf.getShortLE(tmpOffset + 3);
                        rpcProtocol.pad = buf.getShortLE(tmpOffset + 5);
                        tmpOffset = tmpOffset + 7;
                        break;
                    case EPM_PROTOCOL_TCP:
                        Protocol.TCP tcpProtocol = new Protocol.TCP(type);
                        tower.floors[i].protocol = tcpProtocol;

                        tower.floors[i].rhsLength = buf.getShortLE(tmpOffset + 3);
                        buf.getBytes(tmpOffset + 5, tcpProtocol.port);

                        tmpOffset = tmpOffset + 7;
                        break;
                    case EPM_PROTOCOL_IP:
                        Protocol.IP ipProtocol = new Protocol.IP(type);
                        tower.floors[i].protocol = ipProtocol;

                        tower.floors[i].rhsLength = buf.getShortLE(tmpOffset + 3);
                        buf.getBytes(tmpOffset + 5, ipProtocol.ip);

                        tmpOffset = tmpOffset + 9;
                        break;
                    default:
                        break;
                }

            }
            // 补齐 4 字节
            int alignment = 4 - (tower.length1 % 4);
            tmpOffset = tmpOffset + alignment;

            buf.getBytes(tmpOffset, handle);
            maxTowers = buf.getIntLE(tmpOffset + 20);

            return 12 + tower.length1 + alignment + 24;
        }


    }

    @Data
    public static class Tower {
        public int ReferentID;
        public int length1;
        public int length2;
        public short numFloors;
        public Floor[] floors;
    }

    @Data
    public static class Floor {
        public short lhsLength;   // Left-Hand Side 每个Floor分为左侧和右侧
        public short rhsLength;   // Right-Hand Side

        public Protocol protocol;
    }
}
