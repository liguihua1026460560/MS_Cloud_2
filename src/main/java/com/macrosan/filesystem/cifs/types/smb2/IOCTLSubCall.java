package com.macrosan.filesystem.cifs.types.smb2;

import io.netty.buffer.ByteBuf;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.macrosan.filesystem.cifs.call.smb2.IOCTLCall.FSCTL_VALIDATE_NEGOTIATE_INFO;

public abstract class IOCTLSubCall {
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.METHOD, ElementType.TYPE})
    public @interface Smb2IOCTL {
        int value();
    }

    public abstract int readStruct(ByteBuf buf, int offset);

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/261ec397-d692-4e3e-8bcd-c96ce02bb969
    @Smb2IOCTL(value = FSCTL_VALIDATE_NEGOTIATE_INFO)
    public static class ValidateNegotiateInfo extends IOCTLSubCall {
        int capabilities;
        byte[] clientGuid = new byte[16];
        short securityMode;
        short dialectCount;
        short[] dialects;

        public int readStruct(ByteBuf buf, int offset) {
            capabilities = buf.getIntLE(offset);
            buf.getBytes(offset + 4, clientGuid);
            securityMode = buf.getShortLE(offset + 20);
            dialectCount = buf.getShortLE(offset + 22);
            dialects = new short[dialectCount];
            for (int i = 0; i < dialectCount; i++) {
                dialects[i] = buf.getShortLE(offset + 24 + i * 2);
            }

            return 24 + dialectCount * 2;
        }
    }

    public static class EmptyCall extends IOCTLSubCall {
        public int readStruct(ByteBuf buf, int offset) {
            return 0;
        }
    }
}
