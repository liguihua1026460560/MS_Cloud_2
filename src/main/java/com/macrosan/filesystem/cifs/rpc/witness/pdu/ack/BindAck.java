package com.macrosan.filesystem.cifs.rpc.witness.pdu.ack;

import com.macrosan.filesystem.cifs.rpc.Session;
import com.macrosan.filesystem.cifs.rpc.pdu.RPCHeader;
import com.macrosan.filesystem.cifs.rpc.pdu.ResultList;
import com.macrosan.filesystem.cifs.rpc.pdu.SecondAddress;
import com.macrosan.filesystem.cifs.rpc.pdu.ack.Ack;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.call.BindCall;
import com.macrosan.filesystem.cifs.rpc.witness.ntlmssp.Challenge;
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
public class BindAck extends Ack {
    public Header header;
    public Challenge challenge;

    public BindAck(BindCall bind, Session session) {
        header = new Header(bind, session);
        challenge = new Challenge(bind.negotiate, session);
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
        public long auth;

        public Header(BindCall bind, Session session) {
            super(bind);
            packetType = BIND_ACK;
            maxXmitFrag = bind.maxRecvFrag;
            maxRecvFrag = bind.maxRecvFrag;
            assocGroupId = AssocGroupID.incrementAndGet(); // 生成一个唯一的关联组 id
            secondAddress = new SecondAddress();  // 二级地址默认不配置
            pad = 0x00;
            resultList = new ResultList(bind.contextList);

            resultList.results[0] = new ResultList.Result();
            resultList.results[0].result = PROVIDER_REJECTION;
            resultList.results[0].reason = PROPOSED_TRANSFER_SYNTAXES_NOT_SUPPORTED;
            resultList.results[1] = new ResultList.Result();
            resultList.results[1].result = ACCEPTANCE;
            resultList.results[1].reason = REASON_NOT_SPECIFIED;
            resultList.results[1].syntax.interfaceUuid = bind.contextList.contextItems[1].transferSyntxes[0].interfaceUuid;
            resultList.results[1].syntax.interfaceVer = bind.contextList.contextItems[1].transferSyntxes[0].interfaceVer;
            if (bind.contextList.contextItems.length > 2) {
                resultList.results[2] = new ResultList.Result();
                resultList.results[2].result = NEGOTIATE_ACK;
                resultList.results[2].reason = 0x0003;
            }

            auth = bind.auth;

            // 更新 fragLength 和 authLength
            fragLength = 218;
            if (bind.contextList.contextItems.length > 2) {
                fragLength = 242;
            }

            authLength = 130;
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

            // TODO 迁移到父类writeStruct方法中
            if (authLength != 0) {
                buf.setLongLE(start, auth);
                start += challenge.writeStruct(buf, start + 8) + 8;
            }

            return fragLength;
        }
    }
}
