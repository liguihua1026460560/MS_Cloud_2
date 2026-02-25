package com.macrosan.filesystem.nfs.call.nsm;

import com.macrosan.filesystem.ReadStruct;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

@ToString
public class MonCall implements ReadStruct {
    public byte[] monName;
    public byte[] myName;
    public int myProg;
    public int myVers;
    public int myProc;
    public byte[] priv;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        int monNameLen = buf.getInt(offset);
        offset += 4;
        monName = new byte[monNameLen];
        buf.getBytes(offset, monName);
        offset += ((monNameLen + 3) / 4) * 4;
        int myNameLen = buf.getInt(offset);
        offset += 4;
        myName = new byte[myNameLen];
        buf.getBytes(offset, myName);
        offset += ((myNameLen + 3) / 4) * 4;
        myProg = buf.getInt(offset);
        offset += 4;
        myVers = buf.getInt(offset);
        offset += 4;
        myProc = buf.getInt(offset);
        offset += 4;
        int privLen = buf.getInt(offset);
        offset += 4;
        priv = new byte[privLen];
        buf.getBytes(offset, priv);
        offset += ((privLen + 3) / 4) * 4;
        return offset - start;
    }
}
