package com.macrosan.filesystem.nfs.auth;

import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.nfs.RpcCallHeader;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static com.macrosan.filesystem.nfs.auth.AuthGSS.GssProc.RPC_GSS_PROC_DATA;

@ToString(callSuper = true)
@Data
@EqualsAndHashCode(callSuper = false)
public class AuthGSS extends Auth implements ReadStruct {
    int version;
    GssProc procedure;
    int seq;
    int service;
    byte[] context;
    public long contextId;

    GssVerifier verifier = new GssVerifier();
    public long authContextId = -1;
    boolean needUnWarp = false;

    int rpcStart;
    int rpcEnd;
    public GssVerifier resVerifier;

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        rpcStart = offset - 32;
        version = buf.getInt(offset);
        procedure = GSS_PROC_VALUE[buf.getInt(offset + 4)];
        seq = buf.getInt(offset + 8);
        service = buf.getInt(offset + 12);

        int contextLen = buf.getInt(offset + 16);
        context = new byte[contextLen];
        buf.getBytes(offset + 20, context);
        if (contextLen == 8) {
            contextId = buf.getLong(offset + 20);
        }

        rpcEnd = 20 + contextLen + offset;

        int verifierLen = verifier.readStruct(buf, 20 + contextLen + offset);

        return 20 + contextLen + verifierLen;
    }

    @Override
    public int auth(RpcCallHeader header, ByteBuf msg, int offset) {
        if (procedure != RPC_GSS_PROC_DATA && header.opt != 0) {
            return -1;
        }

        int tokenLen;
        byte[] token;
        KrbJni.GssContext gssContext;

        switch (procedure) {
            case RPC_GSS_PROC_INIT:
                tokenLen = msg.getInt(offset);
                token = new byte[tokenLen];
                msg.getBytes(offset + 4, token);
                resVerifier = KrbJni.acceptContext(this, token);
                return 0;
            case RPC_GSS_PROC_DATA:
            case RPC_GSS_PROC_DESTROY:
                if (context.length != 8) {
                    return -1;
                }

                gssContext = KrbJni.getContext(contextId);
                if (gssContext == null) {
                    return -1;
                }

                byte[] rpcHeader = new byte[rpcEnd - rpcStart];
                msg.getBytes(rpcStart, rpcHeader);
                resVerifier = KrbJni.checkAndGetVerifier(gssContext, rpcHeader, verifier.bytes, seq);
                if (resVerifier == null) {
                    return -1;
                }

                if (procedure == RPC_GSS_PROC_DATA) {
//                    needUnWarp = true;
                } else {
                    KrbJni.releaseContext(contextId);
                }
                return 0;
            case RPC_GSS_PROC_CONTINUE_INIT:
                //TODO
            default:
                return -1;
        }
    }

    @Override
    public boolean needUnWarp() {
        return needUnWarp;
    }

    @Override
    public ByteBuf unWarp(ByteBuf msg, int offset) {
        if (needUnWarp) {
            int tokenLen = msg.getInt(offset);
            byte[] token = new byte[tokenLen];
            msg.getBytes(offset + 4, token);

            KrbJni.GssContext context = KrbJni.getContext(contextId);
            if (context == null) {
                return null;
            }

            return KrbJni.unWarp(context, msg, offset, token);
        }

        return null;
    }

    public enum GssProc {
        RPC_GSS_PROC_DATA(0),
        RPC_GSS_PROC_INIT(1),
        RPC_GSS_PROC_CONTINUE_INIT(2),
        RPC_GSS_PROC_DESTROY(3);

        int proc;

        GssProc(int proc) {
            this.proc = proc;
        }
    }

    public static final GssProc[] GSS_PROC_VALUE = GssProc.values();
}
