package com.macrosan.filesystem.cifs.types.smb2.pipe;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.log4j.Log4j2;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.macrosan.filesystem.cifs.rpc.RPCConstants.BIND_ACK;

@EqualsAndHashCode(callSuper = true)
@Data
@Log4j2
public class BindAckInfo  extends GetInfoReply.Info {
        private byte version = 5;
        private byte versionMinor = 0;
        private byte pduType = BIND_ACK;
        private byte pduFlags = 0x03;
        private int dataRep = 0x00000010;
        private short fragLength;
        private short authLength = 0;
        private int callId;
        private short maxXmitFrag = 4280;
        private short maxRecvFrag = 4280;
        private int assocGroup;
        private short scndryAddrLen;
        private String scndryAddr;
        private short numResults = 1;
        private List<CtxItem> ctxItems = new ArrayList<>();


        @Override
        public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

        @Override
        public int size() {
        return 44 + numResults * 24;
    }

        @Data
        public static class CtxItem {
            private short ackResult;
            private int transferSyntaxVersion;
            private byte[] transferSyntaxRawBytes;
        }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {

        buf.writeByte(version);
        buf.writeByte(versionMinor);
        buf.writeByte(pduType);
        buf.writeByte(pduFlags);
        buf.writeIntLE(dataRep);
        buf.writeShortLE(fragLength);  // 稍后回填
        buf.writeShortLE(authLength);
        buf.writeIntLE(callId);
        buf.writeShortLE(maxXmitFrag);
        buf.writeShortLE(maxRecvFrag);
        buf.writeIntLE(assocGroup);
        buf.writeShortLE(scndryAddrLen);

        if (scndryAddr != null && !scndryAddr.isEmpty()) {
            byte[] addrBytes = scndryAddr.getBytes(StandardCharsets.US_ASCII);
            buf.writeBytes(addrBytes);

            buf.writeZero(15 - scndryAddrLen);
        }

        buf.writeShortLE(numResults);
        buf.writeShortLE(0);

        for (CtxItem ctx : ctxItems) {
            buf.writeShortLE(ctx.getAckResult());
            buf.writeShortLE(0);
            byte[] tsUuidBytes = ctx.getTransferSyntaxRawBytes();
            if (tsUuidBytes != null && tsUuidBytes.length == 16) {
                buf.writeBytes(tsUuidBytes);
            } else {
                buf.writeZero(16);
            }
            buf.writeIntLE(ctx.getTransferSyntaxVersion());
        }

        // 回填 frag_length
        int totalLength = buf.writerIndex();
        buf.setShortLE(8, (short) totalLength);
        return totalLength;
    }
}
