package com.macrosan.filesystem.cifs;

import com.macrosan.filesystem.cifs.handler.SMBHandler;
import com.macrosan.filesystem.cifs.types.smb2.CompoundRequest;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

@EqualsAndHashCode(callSuper = true)
@Data
@Accessors(chain = true)
/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/fb188936-5050-48d3-b350-dc43059638a4
 */
public class SMB2Header extends SMBHeader {
    public static final int SIZE = 64;
    public static final int MAGIC = 0x424D53FE;

    public SMB2Header() {
    }

    SMBHandler handler;
    CompoundRequest compoundRequest;

    public SMB2Header(SMBHandler handler, CompoundRequest compoundRequest) {
        this.handler = handler;
        this.compoundRequest = compoundRequest;
    }

    int magic;
    short headerLen; //64
    short creditCharge;
    public int status;
    public short opcode;
    short creditRequested;
    public int flags;
    int nextCmd;
    long messageId;
    //aysnc情况下pid和tid组合是asyncID
    public int pid;
    public int tid;
    public long sessionId;
    byte[] sign = new byte[16];

    public int size() {
        return SIZE;
    }

    public void setAsyncId(long asyncId) {
        pid = (int) asyncId;
        tid = (int) (asyncId >> 32);
    }

    public long getAsyncId() {
        long asyncId = tid;
        return asyncId << 32 | pid;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        magic = buf.getIntLE(offset);
        headerLen = buf.getShortLE(offset + 4);
        creditCharge = buf.getShortLE(offset + 6);
        status = buf.getIntLE(offset + 8);
        opcode = buf.getShortLE(offset + 12);
        creditRequested = buf.getShortLE(offset + 14);
        flags = buf.getIntLE(offset + 16);
        nextCmd = buf.getIntLE(offset + 20);
        messageId = buf.getLongLE(offset + 24);
        pid = buf.getIntLE(offset + 32);
        tid = buf.getIntLE(offset + 36);
        sessionId = buf.getLongLE(offset + 40);
        buf.getBytes(offset + 48, sign);
        return SIZE;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        buf.setIntLE(offset, magic);
        buf.setShortLE(offset + 4, headerLen);
        buf.setShortLE(offset + 6, creditCharge);
        buf.setIntLE(offset + 8, status);
        buf.setShortLE(offset + 12, opcode);
        buf.setShortLE(offset + 14, creditRequested);
        buf.setIntLE(offset + 16, flags);
        buf.setIntLE(offset + 20, nextCmd);
        buf.setLongLE(offset + 24, messageId);
        buf.setIntLE(offset + 32, pid);
        buf.setIntLE(offset + 36, tid);
        buf.setLongLE(offset + 40, sessionId);
        buf.setBytes(offset + 48, sign);
        return SIZE;
    }

    public static final byte[] EMPTY_SIGN = new byte[16];

    public static SMB2Header newReplyHeader(SMB2Header header) {
        return new SMB2Header()
                .setMagic(header.getMagic())
                .setHeaderLen((short) SMB2Header.SIZE)
                .setCreditCharge(header.getCreditCharge())
                .setOpcode(header.getOpcode())
                .setCreditRequested(header.getCreditRequested())
                //response | sign
                .setFlags(SMB2_HDR_FLAG_REDIRECT | (header.getFlags() & SMB2_HDR_FLAG_SIGNED))
                .setNextCmd(0)
                .setMessageId(header.getMessageId())
                .setPid(header.getPid())
                .setTid(header.getTid())
                .setSessionId(header.getSessionId())
                .setSign(EMPTY_SIGN);
    }

    public static final int SMB2_HDR_FLAG_REDIRECT = 0x01;
    public static final int SMB2_HDR_FLAG_ASYNC = 0x02;
    public static final int SMB2_HDR_FLAG_CHAINED = 0x04;
    public static final int SMB2_HDR_FLAG_SIGNED = 0x08;
    public static final int SMB2_HDR_FLAG_PRIORITY_MASK = 0x70;
    public static final int SMB2_HDR_FLAG_DFS = 0x10000000;
    public static final int SMB2_HDR_FLAG_REPLAY_OPERATION = 0x20000000;
}
