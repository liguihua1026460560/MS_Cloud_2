package com.macrosan.filesystem.nfs.call.v4;


import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.RpcAuthType.*;

@ToString
public class BackChannelCtlCall extends CompoundCall {
    public int cbProgram;
    public boolean nrSecFlavs;
    public int flavor;
    public int authType;
    public int stamp;
    public int machineNameLen;
    public byte[] machineName;
    public int uid;
    public int gid;
    public int gcbpService;
    public int gcbpHandlerFromServerLen;
    public int[] gcbpHandlerFromServer;
    public int gcbpHandlerFromClientLen;
    public int[] gcbpHandlerFromClient;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        cbProgram = buf.getInt(offset);
        nrSecFlavs = buf.getBoolean(offset + 4);
        flavor = nrSecFlavs ? -1 : 0;
        authType = buf.getInt(offset + 8);
        switch (authType) {
            case RPC_AUTH_NULL:
                break;
            case RPC_AUTH_UNIX:
                stamp = buf.getInt(offset + 12);
                machineNameLen = buf.getInt(offset + 16);
                offset += 20;
                machineName = new byte[machineNameLen];
                buf.getBytes(offset, machineName);
                offset += (machineNameLen + 3) / 4 * 4;
                uid = buf.getInt(offset);
                gid = buf.getInt(offset + 4);
                offset += 8;
                //验证uid和gid后
                flavor = RPC_AUTH_UNIX;
                break;
            case RPC_AUTH_GSS:
                gcbpService = buf.getInt(offset);
                gcbpHandlerFromServerLen = buf.getInt(offset + 4);
                offset += 8;
                gcbpHandlerFromServer = new int[gcbpHandlerFromServerLen];
                for (int i = 0; i < gcbpHandlerFromServerLen; i++) {
                    gcbpHandlerFromServer[i] = buf.getInt(offset);
                    offset += 4;
                }
                gcbpHandlerFromClientLen = buf.getInt(offset);
                gcbpHandlerFromClient = new int[gcbpHandlerFromClientLen];
                for (int i = 0; i < gcbpHandlerFromClientLen; i++) {
                    gcbpHandlerFromClient[i] = buf.getInt(offset);
                    offset += 4;
                }
                break;

        }

        return offset - start;
    }
}

