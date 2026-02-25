package com.macrosan.filesystem.nfs.auth;

import com.macrosan.filesystem.nfs.RpcCallHeader;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.atomic.AtomicReference;

@ToString
@Data
@Log4j2
public abstract class Auth {
    public int flavor;
    public int authLen;

    public abstract int auth(RpcCallHeader header, ByteBuf msg, int offset);

    public boolean needUnWarp() {
        return false;
    }

    public ByteBuf unWarp(ByteBuf msg, int offset) {
        return null;
    }

    //不处理auth
    public static int readAuth(ByteBuf buf, int offset, AtomicReference<Auth> reference) {
        int flavor = buf.getInt(offset);
        int authLen = buf.getInt(offset + 4);

        switch (flavor) {
            //AUTH_NULL
            case 0:
                reference.set(new AuthNull());
                return 16;
            //AUTH_UNIX
            case 1:
                AuthUnix auth = new AuthUnix();
                try {
                    auth.flavor = flavor;
                    auth.authLen = authLen;
                    offset += 8;
                    auth.readStruct(buf, offset);
                    reference.set(auth);
                } catch (Exception e) {
                    log.error("", e);
                    reference.set(new AuthUnix());
                }

                return authLen + 16;
            //AUTH_GSS
            case 6:
                AuthGSS gss = new AuthGSS();
                gss.readStruct(buf, offset + 8);
                reference.set(gss);
                int verLen = buf.getInt(offset + authLen + 8 + 4);
                return authLen + 8 + 8 + verLen;
            default:
                return -1;
        }
    }

    public static int writeAuth(ByteBuf buf, int offset, Auth auth) {
        int flavor = auth.flavor;
        buf.setInt(offset, flavor);
        offset += 4;
        int authLen = auth.authLen;
        buf.setInt(offset, authLen);
        offset += 4;
        switch (flavor) {
            //AUTH_NULL
            case 0:
                return 16;
            //AUTH_UNIX
            case 1:
                AuthUnix authUnix = (AuthUnix) auth;
                authUnix.writeStruct(buf, offset);
                return authLen + 16;
            //AUTH_GSS
            case 6:
                return authLen + 16;
            default:
                return -1;
        }
    }

}
