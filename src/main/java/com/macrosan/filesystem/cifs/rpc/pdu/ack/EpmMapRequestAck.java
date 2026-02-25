package com.macrosan.filesystem.cifs.rpc.pdu.ack;

import com.macrosan.filesystem.cifs.rpc.pdu.Protocol;
import com.macrosan.filesystem.cifs.rpc.pdu.RPCHeader;
import com.macrosan.filesystem.cifs.rpc.pdu.call.EpmMapRequestCall;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

import static com.macrosan.filesystem.cifs.rpc.RPCConstants.*;

@Log4j2
public class EpmMapRequestAck extends Ack {
    public Header header;
    public Stub stub;

    public EpmMapRequestAck(EpmMapRequestCall request, int port) {
        header = new Header(request.header);
        stub = new Stub(request.stub, port);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        header.writeStruct(buf, offset);

        buf.setBytes(offset + 24, stub.handle);
        buf.setIntLE(offset + 44, stub.numTowers);
        buf.setIntLE(offset + 48, stub.towerArray.maxCount);
        buf.setIntLE(offset + 52, stub.towerArray.offset);
        buf.setIntLE(offset + 56, stub.towerArray.actualCount);

        EpmMapRequestCall.Tower tower = stub.towerArray.tower;

        buf.setIntLE(offset + 60, tower.ReferentID);
        buf.setIntLE(offset + 64, tower.length1);
        buf.setIntLE(offset + 68, tower.length2);
        buf.setShortLE(offset + 72, tower.numFloors);

        int tmpOffset = offset + 74;

        for (int i = 0; i < tower.numFloors; i++) {
            buf.setShortLE(tmpOffset, tower.floors[i].lhsLength);
            buf.setByte(tmpOffset + 2, tower.floors[i].protocol.type);

            byte type = tower.floors[i].protocol.type;
            switch (type) {
                case EPM_PROTOCOL_UUID:
                    Protocol.UUID uuidProtocol = (Protocol.UUID) tower.floors[i].protocol;

                    buf.setBytes(tmpOffset + 3, uuidProtocol.uuid);
                    buf.setShortLE(tmpOffset + 19, uuidProtocol.version);
                    buf.setShortLE(tmpOffset + 21, tower.floors[i].rhsLength);
                    buf.setShortLE(tmpOffset + 23, uuidProtocol.versionMinor);

                    tmpOffset = tmpOffset + 25;
                    break;
                case EPM_PROTOCOL_NCACN:
                    Protocol.RPC rpcProtocol = (Protocol.RPC) tower.floors[i].protocol;

                    buf.setShortLE(tmpOffset + 3, tower.floors[i].rhsLength);
                    buf.setShortLE(tmpOffset + 5, rpcProtocol.pad);
                    tmpOffset = tmpOffset + 7;
                    break;
                case EPM_PROTOCOL_TCP:
                    Protocol.TCP tcpProtocol = (Protocol.TCP) tower.floors[i].protocol;

                    buf.setShortLE(tmpOffset + 3, tower.floors[i].rhsLength);
                    buf.setBytes(tmpOffset + 5, tcpProtocol.port);

                    tmpOffset = tmpOffset + 7;
                    break;
                case EPM_PROTOCOL_IP:
                    Protocol.IP ipProtocol = (Protocol.IP) tower.floors[i].protocol;

                    buf.setShortLE(tmpOffset + 3, tower.floors[i].rhsLength);
                    buf.setBytes(tmpOffset + 5, ipProtocol.ip);

                    tmpOffset = tmpOffset + 9;
                    break;
                default:
                    break;
            }

        }

        int alignment = 4 - (tower.length1 % 4);
        tmpOffset = tmpOffset + alignment;

        buf.setIntLE(tmpOffset, stub.returnCode);


        return 152;
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    @ToString(callSuper = true)
    public class Header extends RPCHeader {
        public int allocHint;    // 数据量
        public short contextId;
        public short cancelCount;

        public Header(EpmMapRequestCall.Header header) {
            version = header.version;
            versionMinor = header.versionMinor;
            packetType = RESPONSE;
            flags = header.flags;
            dataRepresentation = header.dataRepresentation;
            fragLength = 152;
            authLength = 0;
            callId = header.callId;
            allocHint = 128;
            contextId = header.contextId;
            cancelCount = 0;
        }

        public int writeStruct(ByteBuf buf, int offset) {
            super.writeStruct(buf, offset);
            buf.setIntLE(offset + 16, header.allocHint);
            buf.setShortLE(offset + 20, header.contextId);
            buf.setShortLE(offset + 22, header.cancelCount);
            return 24;
        }
    }

    @Data
    @ToString(callSuper = true)
    public class Stub {
        public byte[] handle = new byte[20];
        public int numTowers;
        public TowerArray towerArray;
        public int returnCode;

        public Stub(EpmMapRequestCall.Stub stub, int port) {
            handle = stub.handle;
            numTowers = 1;

            towerArray = new TowerArray();
            towerArray.maxCount = 4;
            towerArray.offset = 0;
            towerArray.actualCount = 1;

            EpmMapRequestCall.Tower tower = new EpmMapRequestCall.Tower();
            towerArray.tower = tower;
            tower.ReferentID = 0x03;
            tower.length1 = stub.tower.length1;
            tower.length2 = stub.tower.length2;
            tower.numFloors = stub.tower.numFloors;

            tower.floors = new EpmMapRequestCall.Floor[5];


            for (int i = 0; i < tower.numFloors; i++) {
                tower.floors[i] = new EpmMapRequestCall.Floor();
                tower.floors[i].lhsLength = stub.tower.floors[i].lhsLength;
                tower.floors[i].rhsLength = stub.tower.floors[i].rhsLength;
                tower.floors[i].protocol = stub.tower.floors[i].protocol;
                byte type = stub.tower.floors[i].protocol.type;
                switch (type) {
                    case EPM_PROTOCOL_TCP:
                        Protocol.TCP tcpProtocol = (Protocol.TCP) tower.floors[i].protocol;
                        tcpProtocol.port = getPortBytes(port);     // 配置协议端口号
                        break;
                    case EPM_PROTOCOL_UUID:
                    case EPM_PROTOCOL_NCACN:
                    case EPM_PROTOCOL_IP:
                    default:
                        break;
                }

            }
            returnCode = 0;
        }
        // TODO 重写
//        public int writeStuct() {}

    }
    @Data
    public class TowerArray {
        public int maxCount;
        public int offset;
        public int actualCount;
        public EpmMapRequestCall.Tower tower;
    }

    public static byte[] getPortBytes(int port) {

        // 手动拆分成 2 字节（大端序）
        byte[] bytes = new byte[2];
        bytes[0] = (byte) ((port >> 8) & 0xFF); // 高位字节 0xC0（即 -64）
        bytes[1] = (byte) (port & 0xFF);        // 低位字节 0x02

        return bytes;
    }
}
