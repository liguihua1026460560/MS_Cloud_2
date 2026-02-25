package com.macrosan.filesystem.nfs.auth;

import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.fs.AioChannel;
import com.macrosan.utils.msutils.UnsafeUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import sun.misc.Unsafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Log4j2
public class KrbJni {
    static {
        System.load("/moss/jni/libkrb.so");
    }

    public static class GssContext {
        int retFlags;
        long contextAddr;
        long id = id0.incrementAndGet();
        byte[] initOutToken;

        static AtomicLong id0 = new AtomicLong();
    }

    public static final int GSS_S_COMPLETE = 0;
    public static final int GSS_S_CONTINUE_NEEDED = 1;

    public static native long[] acceptContext(byte[] token, int len);

    public static native long[] getVerifier(long context, int seq);

    public static native long checkVerifier(long context, byte[] rpcHeader, int rpcLen, byte[] verifier, int verifierLen);

    public static native long[] unWarp(long context, byte[] token, int tokenLen);

    public static native long[] warp(long context, byte[] token, int tokenLen);

    public static native int freeCtx(long context);

    public static native byte[] getSessionKey(long context);

    public static native String getAccountName(long context);

    public static Map<Long, GssContext> gssCtxMap = new ConcurrentHashMap<>();

    public static GssVerifier acceptContext(AuthGSS auth, byte[] initToken) {
        long[] res = acceptContext(initToken, initToken.length);
        //fail
        if (res == null || (res[0] != GSS_S_COMPLETE && res[0] != GSS_S_CONTINUE_NEEDED)) {
            log.error("acceptContext fail {}", res == null ? "null" : res[0]);
        } else {
            GssContext context = new GssContext();
            context.retFlags = (int) res[1];
            context.contextAddr = res[2];
            byte[] outToken = new byte[(int) res[3]];
            UnsafeUtils.unsafe.copyMemory(null, res[4], outToken, Unsafe.ARRAY_BYTE_BASE_OFFSET, outToken.length);
            AioChannel.free0(res[4]);
            context.initOutToken = outToken;

            res = getVerifier(context.contextAddr, 0x80000000);

            if (res == null || res[0] != GSS_S_COMPLETE) {
                log.error("acceptContext getVerifier fail {}", res == null ? "null" : res[0]);
            } else {
                gssCtxMap.put(context.id, context);

                GssVerifier verifier = new GssVerifier();
                verifier.flavor = 6;//AUTH_GSS
                verifier.len = (int) res[1];
                verifier.bytes = new byte[verifier.len];
                UnsafeUtils.unsafe.copyMemory(null, res[2], verifier.bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, verifier.bytes.length);
                AioChannel.free0(res[2]);
                auth.authContextId = context.id;
                return verifier;
            }
        }

        //TODO release context

        return null;
    }

    public static GssContext getContext(long id) {
        return gssCtxMap.get(id);
    }

    public static void releaseContext(long id) {
        GssContext context = gssCtxMap.remove(id);
        if (context != null) {
            freeCtx(context.contextAddr);
        }
    }

    public static GssVerifier checkAndGetVerifier(GssContext context, byte[] rpcHeader, byte[] verifierToken, int seq) {
        long check = checkVerifier(context.contextAddr, rpcHeader, rpcHeader.length, verifierToken, verifierToken.length);
        if (check != GSS_S_COMPLETE) {
            log.error("check verifier fail {}:{}", context.id, check);
        } else {
            int seq0 = Integer.reverseBytes(seq);
            long[] res = getVerifier(context.contextAddr, seq0);
            if (res == null || res[0] != GSS_S_COMPLETE) {
                log.error("{}: getVerifier fail {}", context.id, res == null ? "null" : res[0]);
            } else {
                GssVerifier verifier = new GssVerifier();
                verifier.flavor = 6;//AUTH_GSS
                verifier.len = (int) res[1];
                verifier.bytes = new byte[verifier.len];
                UnsafeUtils.unsafe.copyMemory(null, res[2], verifier.bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, verifier.bytes.length);
                AioChannel.free0(res[2]);
                return verifier;
            }
        }


        return null;
    }

    public static ByteBuf unWarp(GssContext context, ByteBuf msg, int offset, byte[] token) {
        long[] res = unWarp(context.contextAddr, token, token.length);
        if (res[0] != GSS_S_COMPLETE) {
            log.error("unWarp fail {}:{}", context.id, res[0]);
        } else {
            int len = (int) res[1];

            try {
                ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.heapBuffer(len - 4);
                byte[] tmp = buf.array();
                long arrayOffset = buf.arrayOffset();
                //前4位是seq
                //TODO 校验seq
                UnsafeUtils.unsafe.copyMemory(null, res[2] + 4, tmp, Unsafe.ARRAY_BYTE_BASE_OFFSET + arrayOffset, len - 4);
                buf.writerIndex(len - 4 + buf.writerIndex());

                return Unpooled.wrappedBuffer(msg.slice(0, offset), buf);
            } finally {
                AioChannel.free0(res[2]);
            }
        }

        return null;
    }

    public static int warp(GssContext context, ByteBuf msg, int offset, byte[] token) {
        long[] res = warp(context.contextAddr, token, token.length);
        if (res[0] != GSS_S_COMPLETE) {
            log.error("warp fail {}:{}", context.id, res[0]);
            return -1;
        }

        int len = (int) res[1];
        ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.heapBuffer(len + 4);
        byte[] tmp = buf.array();
        long arrayOffset = buf.arrayOffset();
        UnsafeUtils.unsafe.copyMemory(null, res[2], tmp, Unsafe.ARRAY_BYTE_BASE_OFFSET + arrayOffset + 4, len);
        buf.setInt(0, len);
        buf.writerIndex(buf.writerIndex() + len + 4);

        msg.setBytes(offset, buf);

        return len + 4;
    }



    @Data
    public static class SmbKrbContext {
        public long newCtx;
        public int retFlags;
        public byte[] outToken;
        public byte[] sessionKey;
        public String account;
    }


    public static SmbKrbContext getSmbKrbContext(byte[] apReqBytes) {
        long[] accept = KrbJni.acceptContext(apReqBytes, apReqBytes.length);
        if (accept == null) {
            throw  new NFSException(FsConstants.NfsErrorNo.NFS3ERR_I0, "acceptContext fail null");
        }
        int maj = (int) accept[0];
        int retFlags = (int) accept[1];
        long newCtx = accept[2];
        int outLen = (int) accept[3];
        long outAddr = accept[4];
        if (maj != KrbJni.GSS_S_COMPLETE && maj != KrbJni.GSS_S_CONTINUE_NEEDED) {
            KrbJni.freeCtx(newCtx);
            throw  new NFSException(FsConstants.NfsErrorNo.NFS3ERR_I0, "acceptContext fail " + maj);
        } else {
            byte[] outToken = new byte[outLen];
            UnsafeUtils.unsafe.copyMemory(null, outAddr,
                    outToken, Unsafe.ARRAY_BYTE_BASE_OFFSET, outLen);
            AioChannel.free0(outAddr);
            byte[] sessionKey = KrbJni.getSessionKey(newCtx);
            //name@域控 a@example.com
            String userName = KrbJni.getAccountName(newCtx);
            SmbKrbContext context = new SmbKrbContext();
            context.newCtx = newCtx;
            context.retFlags = retFlags;
            context.outToken = outToken;
            context.sessionKey = sessionKey;
            context.account = userName;
            if (maj != KrbJni.GSS_S_CONTINUE_NEEDED) {
                KrbJni.freeCtx(newCtx);
            }
            return context;
        }
    }

}
