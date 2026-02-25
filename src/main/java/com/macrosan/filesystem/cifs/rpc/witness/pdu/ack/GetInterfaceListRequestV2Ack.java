package com.macrosan.filesystem.cifs.rpc.witness.pdu.ack;

import com.macrosan.filesystem.cifs.rpc.Session;
import com.macrosan.filesystem.cifs.rpc.witness.api.WitnessProc;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.call.GetInterfaceListRequestCall;
import com.macrosan.utils.functional.Tuple2;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.extern.log4j.Log4j2;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.filesystem.cifs.rpc.RPCConstants.*;


@Log4j2
public class GetInterfaceListRequestV2Ack extends WitnessAck {
    public static AtomicInteger referenceIDCounter = new AtomicInteger(1);
    public Stub stub;

    public GetInterfaceListRequestV2Ack(GetInterfaceListRequestCall request, Session session) {
        header = new Header(request.header);
        header.fragLength = 1728;
        header.authLength = 16;
        header.allocHint = 1676;    //padding  8 +
        stub = new Stub(session);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset + writeHeader(buf, offset);
        start += writeStub(buf, start);
        start += writeSign(buf, start);
        return start - offset;
    }

    public int writeHeader(ByteBuf buf, int offset) {
        return header.writeStruct(buf, offset);
    }


    public int writeStub(ByteBuf buf, int offset) {
        return stub.writeStruct(buf, offset);
    }

    public int writeSign(ByteBuf buf, int offset) {
        buf.setBytes(offset, sign);
        return 16;
    }

    @Data
    public class Stub {
        public int referenceID;
        public int numInterface;
        public int referenceID0;
        public int numInterface0;
        public Interface[] interfaces;
        public byte[] padding = new byte[8];
        public long auth;


        public Stub(Session session) {
            referenceID = referenceIDCounter.incrementAndGet();
            numInterface = 3;
            referenceID0 = referenceIDCounter.incrementAndGet();
            numInterface0 = 3;

            List<Tuple2<String, Boolean>> list = WitnessProc.getCifsStatusList();
            interfaces = new Interface[list.size()];
            for (int i = 0; i < list.size(); i++) {
                interfaces[i] = new Interface();
                byte[] bytes = "moss.com".getBytes(StandardCharsets.UTF_16LE);
                System.arraycopy(bytes, 0, interfaces[i].groupName, 0, bytes.length);
                String ip = list.get(i).var1;
                try {
                    interfaces[i].ipv4 = InetAddress.getByName(ip).getAddress();
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
                if (session.curServerIP != null && session.curServerIP.equals(ip)) {
                    interfaces[i].flags = WITNESS_INFO_IPv4_VALID;
                } else {
                    interfaces[i].flags = WITNESS_INFO_IPv4_VALID | WITNESS_INFO_WITNESS_IF;
                }
                if (!list.get(i).var2) {
                    interfaces[i].state = WITNESS_STATE_UNAVAILABLE;
                }
            }
            auth = 0x04050a;

        }

        public int writeStruct(ByteBuf buf, int offset) {
            buf.setIntLE(offset, referenceID);
            buf.setIntLE(offset + 4, numInterface);
            buf.setIntLE(offset + 8, referenceID0);
            buf.setIntLE(offset + 12, numInterface0);
            int start = offset + 16;
            for (int i = 0; i < interfaces.length; i++) {
                start += interfaces[i].writeStruct(buf, start);
            }
            buf.setBytes(start, padding);
            buf.setLongLE(start + 8, auth);

            byte[] message = new byte[header.fragLength - 16];
            buf.getBytes(0, message);

            return 1688;
        }
    }

    @Data
    public class Interface {
        public byte[] groupName = new byte[520];
        public int version = WITNESS_V2;
        public short state = WITNESS_STATE_AVAILABLE;
        public short pad = 0x00;
        public byte[] ipv4;  // 4 Byte
        public byte[] ipv6 = new byte[16];
        public int flags = WITNESS_INFO_IPv4_VALID | WITNESS_INFO_WITNESS_IF;


        public int writeStruct(ByteBuf buf, int offset) {
            buf.setBytes(offset, groupName);
            buf.setIntLE(offset + 520, version);
            buf.setShortLE(offset + 524, state);
            buf.setShortLE(offset + 526, pad);
            buf.setBytes(offset + 528, ipv4);
            buf.setBytes(offset + 532, ipv6);
            buf.setIntLE(offset + 548, flags);
            return 552;
        }
    }
}
