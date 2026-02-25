package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.reply.smb2.GetInfoReply;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
@Data
public class FilePipeLocalInfo extends GetInfoReply.Info {
    public int namedPipeType;
    public int namedPipeConfig;
    public int maximumInstances;
    public int currentInstance;
    public int inBoundQuota;
    public int readDataAvailable;
    public int outBoundQuota;
    public int writeQuotaAvailable;
    public int namedPipeState;
    public int namedPipeEnd;

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setIntLE(offset, namedPipeType);
        buf.setIntLE(offset + 4, namedPipeConfig);
        buf.setIntLE(offset + 8, maximumInstances);
        buf.setIntLE(offset + 12, currentInstance);
        buf.setIntLE(offset + 16, inBoundQuota);
        buf.setIntLE(offset + 20, readDataAvailable);
        buf.setIntLE(offset + 24, outBoundQuota);
        buf.setIntLE(offset + 28, writeQuotaAvailable);
        buf.setIntLE(offset + 32, namedPipeState);
        buf.setIntLE(offset + 36, namedPipeEnd);
        return 40;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        return 0;
    }

    @Override
    public int size() {
        return 40;
    }

    //namedPipeType
    public static final int FILE_PIPE_BYTE_STREAM_TYPE = 0x00000000;
    public static final int FILE_PIPE_MESSAGE_TYPE = 0x00000001;

    //namedPipeConfig
    public static final int FILE_PIPE_INBOUND = 0x00000000;
    public static final int FILE_PIPE_OUTBOUND = 0x00000001;
    public static final int FILE_PIPE_FULL_DUPLEX = 0x00000002;

    //namedPipeState
    public static final int FILE_PIPE_DISCONNECTED_STATE = 0x00000001;
    public static final int FILE_PIPE_LISTENING_STATE = 0x00000002;
    public static final int FILE_PIPE_CONNECTED_STATE = 0x00000003;
    public static final int FILE_PIPE_CLOSING_STATE = 0x00000004;


    //namedPipeEnd
    public static final int FILE_PIPE_CLIENT_END = 0x00000000;
    public static final int FILE_PIPE_SERVER_END = 0x00000001;
};
