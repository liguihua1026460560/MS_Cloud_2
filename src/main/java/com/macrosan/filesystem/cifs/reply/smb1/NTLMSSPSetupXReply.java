package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import com.macrosan.filesystem.cifs.types.NTLMSSP;
import com.macrosan.filesystem.utils.CifsUtils;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class NTLMSSPSetupXReply extends SMB1Body {

    public byte xOpcode;
    public short xOffset;
    public short action;
    public NTLMSSP.NTLMSSPMessage message;
    public char[] nativeOS;
    public char[] nativeLanMan;
    public char[] primaryDomain;

    public NTLMSSPSetupXReply() {
        nativeOS = "MOSS".toCharArray();
        nativeLanMan = "MOSS".toCharArray();
        primaryDomain = "SAMBA".toCharArray();
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        wordCount = 4;
        buf.setByte(offset + 1, xOpcode);
        buf.setShortLE(offset + 3, xOffset);
        buf.setShortLE(offset + 5, action);

        int byteStart = offset + 3 + wordCount * 2;
        int msgSize = message == null ? 0 : message.writeStruct(buf, byteStart);
        buf.setShortLE(offset + 7, msgSize);

        int byteEnd = byteStart + msgSize;
        //pad
        if ((byteEnd & 1) == 1) {
            byteEnd++;
        }


        CifsUtils.writeChars(buf, byteEnd, nativeOS);
        byteEnd += 2 + nativeOS.length * 2;

        CifsUtils.writeChars(buf, byteEnd, nativeLanMan);
        byteEnd += 2 + nativeLanMan.length * 2;

        CifsUtils.writeChars(buf, byteEnd, primaryDomain);
        byteEnd += 2 + primaryDomain.length * 2;

        this.byteCount = byteEnd - byteStart;
        return super.writeStruct(buf, offset);
    }

    @Override
    public String toString() {
        return "NTLMSSPSetupXReply{" +
                "xOpcode=" + xOpcode +
                ", xOffset=" + xOffset +
                ", action=" + action +
                ", message=" + message +
                ", nativeOS=" + new String(nativeOS) +
                ", nativeLanMan=" + new String(nativeLanMan) +
                ", primaryDomain=" + new String(primaryDomain) +
                '}';
    }
}
