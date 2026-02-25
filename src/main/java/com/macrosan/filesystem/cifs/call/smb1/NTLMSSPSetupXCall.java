package com.macrosan.filesystem.cifs.call.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import com.macrosan.filesystem.cifs.types.NTLMSSP;
import com.macrosan.filesystem.utils.CifsUtils;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Arrays;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/81e15dee-8fb6-4102-8644-7eaa7ded63f7
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class NTLMSSPSetupXCall extends SMB1Body {
    byte xOpcode;
    short xOffset;
    short maxBufferSize;
    short maxMpxCount;
    short vcNumber;
    int sessionKey;
    short oemPasswdSize;
    short unicodePasswdSize;
    int capabilities;

    NTLMSSP.NTLMSSPMessage ntlmsspMessage;
    byte[] unicodePasswd;

    char[] nativeOS;
    char[] nativeLanMan;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int size = super.readStruct(buf, offset);
        int start = offset + 1;
        xOpcode = buf.getByte(start);
        xOffset = buf.getShortLE(start + 2);
        maxBufferSize = buf.getShortLE(start + 4);
        maxMpxCount = buf.getShortLE(start + 6);
        vcNumber = buf.getShortLE(start + 8);
        sessionKey = buf.getIntLE(start + 10);
        oemPasswdSize = buf.getShortLE(start + 14);
        unicodePasswdSize = buf.getShortLE(start + 16);
        capabilities = buf.getIntLE(start + 20);

        int off = offset + 3 + wordCount * 2;
        ntlmsspMessage = NTLMSSP.readStruct(buf, off);

        off += oemPasswdSize;
        unicodePasswd = new byte[unicodePasswdSize];
        buf.getBytes(off, unicodePasswd);
        off += unicodePasswdSize;

        //pad
        if ((off & 1) == 1) {
            off++;
        }

        nativeOS = CifsUtils.readChars(buf, off);
        off += nativeOS.length * 2 + 2;
        nativeLanMan = CifsUtils.readChars(buf, off);

        return size;
    }

    @Override
    public String toString() {
        String res = "NTLMSSPSessionSetupXCall{" +
                "xOpcode=" + xOpcode +
                ", xOffset=" + xOffset +
                ", maxBufferSize=" + maxBufferSize +
                ", maxMpxCount=" + maxMpxCount +
                ", vcNumber=" + vcNumber +
                ", sessionKey=" + sessionKey +
                ", oemPasswdSize=" + oemPasswdSize +
                ", unicodePasswdSize=" + unicodePasswdSize +
                ", capabilities=" + capabilities;
        res += ", NTLMSSPMessage=" + ntlmsspMessage +
                ", unicodePasswd=" + Arrays.toString(unicodePasswd) +
                ", nativeOS=" + new String(nativeOS) +
                ", nativeLanMan=" + new String(nativeLanMan) +
                '}';

        return res;
    }
}
