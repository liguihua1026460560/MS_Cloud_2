package com.macrosan.filesystem.cifs.rpc.pdu;

import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Data
public class ContextList {
    public byte numContextItem;
    public byte reserved;
    public short reserved2;
    public ContextItem[] contextItems;

    public int readStruct(ByteBuf buf, int offset) {
        numContextItem = buf.getByte(offset);
        reserved = buf.getByte(offset + 1);
        reserved2 = buf.getShortLE(offset + 2);

        // nContextElem 是一个 byte，但可能代表 0~255
        int count = (numContextItem & 0xFF);
        contextItems = new ContextItem[count];

        int tmpOffset = offset + 4;
        for (int i = 0; i < count; i++) {
            ContextItem item = new ContextItem();
            tmpOffset += item.readStuct(buf, tmpOffset);
            contextItems[i] = item;
        }
        return tmpOffset - offset;
    }

    @Log4j2
    @Data
    public static class ContextItem {
        public short contextID;
        public byte NumTransItems;
        public byte reserved;
        public Syntax abstractSyntax;
        public Syntax[] transferSyntxes;

        public int readStuct(ByteBuf buf, int offset) {
            contextID = buf.getShortLE(offset);
            NumTransItems = buf.getByte(offset + 2);
            reserved = buf.getByte(offset + 3);

            // 读取 abstractSyntax
            abstractSyntax = new Syntax();
            buf.getBytes(offset + 4, abstractSyntax.interfaceUuid);
            abstractSyntax.interfaceVer = buf.getIntLE(offset + 20);

            // 读取 transferSyntax 列表
            int transferCount = (NumTransItems & 0xFF);
            transferSyntxes = new Syntax[transferCount];

            int tmpOffset0 = offset + 24;
            for (int j = 0; j < transferCount; j++) {
                Syntax st = new Syntax();
                buf.getBytes(tmpOffset0, st.interfaceUuid);
                st.interfaceVer = buf.getIntLE(tmpOffset0 + 16);
                transferSyntxes[j] = st;

                tmpOffset0 = tmpOffset0 + 20;
            }

            return tmpOffset0 - offset;
        }
    }

    @Data
    public static class Syntax {
        public byte[] interfaceUuid = new byte[16];  // 16 Byte
        public int interfaceVer;

    }
}
