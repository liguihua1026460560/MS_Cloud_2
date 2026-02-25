package com.macrosan.filesystem.cifs.reply.smb2;

import com.macrosan.filesystem.cifs.SMB2Body;
import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.types.smb2.IOCTLSubReply;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

import static com.macrosan.filesystem.cifs.call.smb2.IOCTLCall.FSCTL_CREATE_OR_GET_OBJECT_ID;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/f70eccb6-e1be-4db8-9c47-9ac86ef18dbb
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class IOCTLReply extends SMB2Body {
    short reserved;
    int ctlCode;
    SMB2FileId fileId;
    int inputOffset;
    int inputCount;

    // 4 byte 从smb header开头到 outdata开头的偏移量
    int outputOffset;
    int outputCount;
    int flags;
    int reserved2;
    IOCTLSubReply subReply;

    public int writeStruct(ByteBuf buf, int offset) {
        if (ctlCode == FSCTL_CREATE_OR_GET_OBJECT_ID) {
            structSize = 49;
            super.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, ctlCode);
            fileId.writeStruct(buf, offset + 8);
            //inBuf off and len
            buf.setLongLE(offset + 24, 0);

            outputOffset = SMB2Header.SIZE + 48;
            buf.setIntLE(offset + 32, outputOffset);

            if (null != subReply) {
                outputCount = 64;
            } else {
                outputCount = 0;
            }
            buf.setIntLE(offset + 36, outputCount);
            buf.setIntLE(offset + 40, flags);
            if (null != subReply) {
                subReply.writeStruct(buf, offset + 48);
            }
            return outputCount + 48;
        } else {
            structSize = 49;
            super.writeStruct(buf, offset);
            buf.setIntLE(offset + 4, ctlCode);
            fileId.writeStruct(buf, offset + 8);
            //inBuf off and len
            buf.setLongLE(offset + 24, 0);

            int outOff = offset + 48 - 4;
            buf.setIntLE(offset + 32, outOff);
            int outLen;
            if (null != subReply) {
                outLen = subReply.writeStruct(buf, outOff + 4);
            } else {
                outLen = 0;
            }
            buf.setIntLE(offset + 36, outLen);
            buf.setIntLE(offset + 40, flags);

            return outLen + 48;
        }
    }
}
