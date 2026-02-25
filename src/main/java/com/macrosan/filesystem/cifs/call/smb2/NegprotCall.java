package com.macrosan.filesystem.cifs.call.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/e14db7ff-763a-4263-8b10-0c3944f52fc5
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class NegprotCall extends SMB2Body {
    short dialectCount;
    short securityMode;
    int flag;
    byte[] clientGuid = new byte[16];
    int negotiateContextOff;
    short negotiateContextCount;
    short[] dialects;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int size = super.readStruct(buf, offset);
        dialectCount = buf.getShortLE(offset + size);
        securityMode = buf.getShortLE(offset + size + 2);
        flag = buf.getIntLE(offset + size + 6);
        buf.getBytes(offset + size + 10, clientGuid);

        negotiateContextOff = buf.getIntLE(offset + size + 26);
        negotiateContextCount = buf.getShortLE(offset + size + 30);
        dialects = new short[dialectCount];
        for (int i = 0; i < dialects.length; i++) {
            dialects[i] = buf.getShortLE(offset + size + 34 + i * 2);
        }

        return size + dialectCount * 2 + 34;
    }

    public static final short SMB2_NEGOTIATE_SIGNING_ENABLED = 0x0001;
    public static final short SMB2_NEGOTIATE_SIGNING_REQUIRED = 0x0002;


    public static final short SMB_2_0_2 = 0x0202;
    public static final short SMB_2_1_0 = 0x0210;
    public static final short SMB_3_0_0 = 0x0300;
    public static final short SMB_3_0_2 = 0x0302;
    public static final short SMB_3_1_1 = 0x0311;

    public static final short SMB_2_X_X = 0x02ff;
}
