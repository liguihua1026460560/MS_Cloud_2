package com.macrosan.filesystem.cifs.rpc.pdu.ack;

import com.macrosan.filesystem.cifs.rpc.witness.pdu.ack.RegisterExRequestAck;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.ack.WitnessAck;
import io.netty.buffer.ByteBuf;

public abstract class Ack{
    public abstract int writeStruct(ByteBuf buf, int offset);
}
