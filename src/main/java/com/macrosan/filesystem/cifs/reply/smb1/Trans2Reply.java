package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import com.macrosan.filesystem.cifs.SMB1Header;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class Trans2Reply extends SMB1Body {
    public int totalParamCount;
    public int totalDataCount;
    public int paramCount;
    public int paramOffset;
    public int paramDisplacement;
    public int dataCount;
    public int dataOffset;
    public int dataDisplacement;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        wordCount = 10;

        buf.setShortLE(offset + 1, totalParamCount);
        buf.setShortLE(offset + 3, totalDataCount);
        buf.setShortLE(offset + 7, paramCount);
        buf.setShortLE(offset + 9, paramOffset);
        buf.setShortLE(offset + 11, paramDisplacement);
        buf.setShortLE(offset + 13, dataCount);
        buf.setShortLE(offset + 15, dataOffset);
        buf.setShortLE(offset + 17, dataDisplacement);

        return super.writeStruct(buf, offset);
    }

    public void setParam(int defaultSize,int dataSize){
        wordCount = 10 ;
        this.byteCount = defaultSize + dataSize;
        paramCount = totalParamCount = 0;
        dataCount = totalDataCount = dataSize;
        dataOffset = 3 + wordCount * 2 + SMB1Header.SIZE + 1;
    }

}
