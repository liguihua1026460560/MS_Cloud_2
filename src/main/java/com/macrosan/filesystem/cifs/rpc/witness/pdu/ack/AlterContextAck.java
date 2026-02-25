package com.macrosan.filesystem.cifs.rpc.witness.pdu.ack;

import com.macrosan.filesystem.cifs.rpc.pdu.RPCHeader;
import com.macrosan.filesystem.cifs.rpc.pdu.ResultList;
import com.macrosan.filesystem.cifs.rpc.pdu.SecondAddress;
import com.macrosan.filesystem.cifs.rpc.pdu.ack.Ack;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.call.AlterContextCall;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

import static com.macrosan.filesystem.cifs.rpc.DCERPC.AssocGroupID;
import static com.macrosan.filesystem.cifs.rpc.RPCConstants.*;

@Log4j2
@EqualsAndHashCode(callSuper = true)
@Data
@ToString(callSuper = true)
public class AlterContextAck extends Ack {

    public Header header;

    public AlterContextAck(AlterContextCall call) {
        header = new Header(call);
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        return header.writeStruct(buf, offset);
    }


    public class Header extends RPCHeader {

        public short maxXmitFrag;   //  max transmit frag size
        public short maxRecvFrag; //  max receive frag size
        public int assocGroupId;  // incarnation of client-server
        public SecondAddress secondAddress;
        public short pad;   // 用于协议补齐
        public ResultList resultList;
        public Header(AlterContextCall call) {
            super(call);
            packetType = ALTER_CONTEXT_RESP;
            maxXmitFrag = call.maxRecvFrag;
            maxRecvFrag = call.maxRecvFrag;
            assocGroupId = AssocGroupID.incrementAndGet();
            secondAddress = new SecondAddress();  // 二级地址默认不配置
            pad = 0x00;
            resultList = new ResultList(call.contextList);

            resultList.results[0] = new ResultList.Result();
            resultList.results[0].result = ACCEPTANCE;
            resultList.results[0].reason = REASON_NOT_SPECIFIED;
            resultList.results[0].syntax.interfaceUuid = call.contextList.contextItems[0].transferSyntxes[0].interfaceUuid;
            resultList.results[0].syntax.interfaceVer = call.contextList.contextItems[0].transferSyntxes[0].interfaceVer;

            // 更新 fragLength
            fragLength = 56;
        }
        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            int start = offset + super.writeStruct(buf, offset);
            buf.setShortLE(start, maxXmitFrag);
            buf.setShortLE(start + 2, maxRecvFrag);
            buf.setIntLE(start + 4, assocGroupId);


            buf.setShortLE(start + 8, secondAddress.length);
            buf.setBytes(start + 10, secondAddress.port);

            start = start + 10 + secondAddress.length;

            buf.setShortLE(start, pad);

            // 写resultList
            start += resultList.writeStruct(buf, start + 2) + 2;

            return fragLength;
        }
    }
}
