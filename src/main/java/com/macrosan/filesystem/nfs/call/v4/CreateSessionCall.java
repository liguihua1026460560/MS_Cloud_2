package com.macrosan.filesystem.nfs.call.v4;


import io.netty.buffer.ByteBuf;
import lombok.ToString;

import static com.macrosan.filesystem.FsConstants.RpcAuthType.*;
import static com.macrosan.filesystem.nfs.types.FAttr4.*;

@ToString
public class CreateSessionCall extends CompoundCall {
    public long clientId;
    public int seqId;
    public int csaFlags;
    public CreateSessionCall.CsaAttrs csaForeChanAttrs = new CsaAttrs();
    public CreateSessionCall.CsaAttrs csaBackChanAttrs = new CsaAttrs();
    public int cbProgram;
    public boolean nrSecFlavs;
    public int flavor;
    public int authType;
    public int stamp;
    public byte[] machineName;
    public int uid;
    public int gid;
    public int[] gids;
    public int gcbpService;
    public byte[] gcbpHandlerFromServer;
    public byte[] gcbpHandlerFromClient;


    public static final int CREATE_SESSION4_FLAG_PERSIST = 0x00000001;
    public static final int CREATE_SESSION4_FLAG_CONN_BACK_CHAN = 0x00000002;
    public static final int CREATE_SESSION4_FLAG_CONN_RDMA = 0x00000004;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int start = offset;
        clientId = buf.getLong(offset);
        seqId = buf.getInt(offset + 8);
        csaFlags = buf.getInt(offset + 12);
        offset += 16;
        offset += csaForeChanAttrs.readStruct(buf, offset);
        offset += csaBackChanAttrs.readStruct(buf, offset);
        cbProgram = buf.getInt(offset);
        nrSecFlavs = buf.getBoolean(offset + 4);
        flavor = nrSecFlavs ? -1 : 0;
        authType = buf.getInt(offset + 8);
        switch (authType) {
            case RPC_AUTH_NULL:
                break;
            case RPC_AUTH_UNIX:
                stamp = buf.getInt(offset + 12);
                int machineNameLen = buf.getInt(offset + 16);
                offset += 20;
                machineName = new byte[machineNameLen];
                buf.getBytes(offset, machineName);
                offset += (machineNameLen + 3) / 4 * 4;
                uid = buf.getInt(offset);
                gid = buf.getInt(offset + 4);
                int gidsLen = buf.getInt(offset + 8);
                gids = new int[gidsLen];
                offset += 12;
                for (int i = 0; i < gids.length; i++) {
                    gids[i] = buf.getInt(offset);
                    offset += 4;
                }
                //验证uid和gid后
                flavor = RPC_AUTH_UNIX;
                break;
            //未验证
            case RPC_AUTH_GSS:
                gcbpService = buf.getInt(offset);
                int gcbpHandlerFromServerLen = buf.getInt(offset + 4);
                offset += 8;
                gcbpHandlerFromServer = new byte[gcbpHandlerFromServerLen];
                buf.getBytes(offset, gcbpHandlerFromServer);
                offset += (gcbpHandlerFromServerLen + 3) / 4 * 4;
                int gcbpHandlerFromClientLen = buf.getInt(offset);
                gcbpHandlerFromClient = new byte[gcbpHandlerFromClientLen];
                buf.getBytes(offset, gcbpHandlerFromClient);
                offset += (gcbpHandlerFromClientLen + 3) / 4 * 4;
                break;
        }
        return offset - start;
    }

    @ToString
    public static class CsaAttrs {
        public int hdrPadSize;
        public int maxReqSize;
        public int maxRespSize;
        public int maxRespSizeCached;
        public int maxOps;
        public int maxReqs;
        public int nrRdmaAttrs;
        public int rdmaAttrs;

        public int readStruct(ByteBuf buf, int offset) {
            int start = offset;
            hdrPadSize = buf.getInt(offset);
            maxReqSize = buf.getInt(offset + 4);
            maxRespSize = buf.getInt(offset + 8);
            maxRespSizeCached = buf.getInt(offset + 12);
            maxOps = buf.getInt(offset + 16);
            maxReqs = buf.getInt(offset + 20);
            nrRdmaAttrs = buf.getInt(offset + 24);
            offset += 28;
            if (nrRdmaAttrs == 1) {
                rdmaAttrs = buf.getInt(offset);
                offset += 4;
            }
            return offset - start;
        }

        public int writeStruct(ByteBuf buf, int offset) {
            int start = offset;
            buf.setInt(offset, hdrPadSize);
            buf.setInt(offset + 4, maxReqSize);
            buf.setInt(offset + 8, maxRespSize);
            buf.setInt(offset + 12, maxRespSizeCached);
            buf.setInt(offset + 16, maxOps);
            buf.setInt(offset + 20, maxReqs);
            buf.setInt(offset + 24, nrRdmaAttrs);
            offset += 28;
            if (nrRdmaAttrs == 1) {
                buf.setInt(offset, rdmaAttrs);
                offset += 4;
            }
            return offset - start;
        }
    }
}

