package com.macrosan.filesystem.cifs.rpc.witness.pdu.call;

import com.macrosan.filesystem.cifs.rpc.pdu.RPCHeader;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

@Log4j2
@EqualsAndHashCode(callSuper = true)
@Data
@ToString(callSuper = true)
public class OrphanedCall extends RPCHeader {

    public int readStruct(ByteBuf buf, int offset) {
        return super.readStruct(buf, offset);
    }
}
