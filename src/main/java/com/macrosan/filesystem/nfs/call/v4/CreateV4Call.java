package com.macrosan.filesystem.nfs.call.v4;


import com.macrosan.filesystem.nfs.types.FAttr4;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.NFS4Type.*;


@ToString
public class CreateV4Call extends CompoundCall {
    public int fType;
    public int linkNameLen;
    public byte[] linkName;
    public int specData1;
    public int specData2;
    public int nameLen;
    public byte[] name;
    public int maskLen;
    public int[] mask;
    public int maskTotalLen;
    public boolean mkNod;
    public FAttr4 fAttr4;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        fType = buf.getInt(offset);
        offset += 4;
        switch (fType) {
            case NF4LNK:
                linkNameLen = buf.getInt(offset);
                linkName = new byte[linkNameLen];
                buf.getBytes(offset + 4, linkName);
                offset += 4 + (linkNameLen + 3) / 4 * 4;
                break;
            case NF4BLK:
            case NF4CHR:
                specData1 = buf.getInt(offset);
                specData2 = buf.getInt(offset + 4);
                mkNod = true;
                offset += 8;
                break;
            case NF4SOCK:
            case NF4FIFO:
            case NF4DIR:
                break;
            default:
                fType = NF4BAD;
                break;
        }
        nameLen = buf.getInt(offset);
        offset += 4;
        name = new byte[nameLen];
        buf.getBytes(offset, name);
        offset += (nameLen + 3) / 4 * 4;
        maskLen = buf.getInt(offset);
        mask = new int[maskLen];
        for (int i = 0; i < maskLen; i++) {
            mask[i] = buf.getInt(offset + 4 * (i + 1));
        }
        offset += 4 + 4 * maskLen;
        maskTotalLen = buf.getInt(offset);
        offset += 4;
        fAttr4 = new FAttr4(mask, this.context.minorVersion);
        fAttr4.readStruct(buf, offset);
        offset += maskTotalLen;
        return offset - start;
    }
}
