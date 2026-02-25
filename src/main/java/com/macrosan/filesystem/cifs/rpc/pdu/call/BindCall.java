package com.macrosan.filesystem.cifs.rpc.pdu.call;

import com.macrosan.filesystem.cifs.rpc.pdu.ContextList;
import com.macrosan.filesystem.cifs.rpc.pdu.RPCHeader;
import com.macrosan.filesystem.cifs.rpc.witness.ntlmssp.Negotiate;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

@Log4j2
@EqualsAndHashCode(callSuper = true)
@Data
@ToString(callSuper = true)
public class BindCall extends RPCHeader {
    public short maxXmitFrag;   //  max transmit frag size
    public short maxRecvFrag; //  max receive frag size
    public int assocGroupId;  // incarnation of client-server

    public ContextList contextList;

    public long auth;

    public Negotiate negotiate;

    public int readStruct(ByteBuf buf, int offset) {
        int start = offset + super.readStruct(buf, offset);

        maxXmitFrag = buf.getShortLE(start);
        maxRecvFrag = buf.getShortLE(start + 2);
        assocGroupId = buf.getIntLE(start + 4);

        start += 8;
        contextList = new ContextList();
        start += contextList.readStruct(buf, start);

        if (authLength != 0) {
            auth = buf.getLongLE(offset);
            negotiate = new Negotiate();
            start += negotiate.readStruct(buf, start + 8) + 8;
        }

        return start - offset;
    }
}
