package com.macrosan.filesystem.nfs.auth;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.utils.msutils.UnsafeUtils;
import io.netty.buffer.ByteBuf;
import sun.misc.Unsafe;

public class AuthReply extends RpcReply {

    public RpcReply reply;

    public AuthReply(RpcReply reply) {
        super(reply.getHeader());
        this.reply = reply;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        return reply.writeStruct(buf, offset);
    }

    public static class InitAuthReply extends AuthReply {
        long contextId;
        int majStatus = 0;
        int minStatus = 0;
        int seqWindow = 128;
        byte[] token;

        //输入一定是NullReply
        public InitAuthReply(KrbJni.GssContext context, RpcReply reply) {
            super(reply);
            contextId = context.id;
            token = context.initOutToken;
        }

        public int writeStruct(ByteBuf buf, int offset) {
            int start = offset;
            offset += super.writeStruct(buf, offset);

            buf.setInt(offset, 8);
            buf.setLong(offset + 4, contextId);
            buf.setInt(offset + 12, majStatus);
            buf.setInt(offset + 16, minStatus);
            buf.setInt(offset + 20, seqWindow);
            buf.setInt(offset + 24, token.length);
            buf.setBytes(offset + 28, token);
            offset += 28 + token.length;

            return offset - start;
        }
    }

    public static class DataAuthReply extends AuthReply {
        KrbJni.GssContext context;
        int seq;

        public DataAuthReply(KrbJni.GssContext context, RpcReply reply, int seq) {
            super(reply);
            this.context = context;
            this.seq = seq;
        }

        public int writeStruct(ByteBuf buf, int offset) {
            int start = offset;
            offset += super.writeStruct(buf, offset);

            if (false) {
                int replyStart = start + reply.verifier.len + 16;
                byte[] replyBytes = new byte[4 + offset - replyStart];
                UnsafeUtils.unsafe.putInt(replyBytes, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET, Integer.reverseBytes(seq));
                buf.getBytes(replyStart, replyBytes, 4, replyBytes.length - 4);

                int warp = KrbJni.warp(context, buf, replyStart, replyBytes);
                if (warp < 0) {
                    return -1;
                }

                return replyStart + warp - start;
            } else {
                return offset - start;
            }

        }
    }
}
