package com.macrosan.utils.msutils.md5;

import de.sfuhrm.openssl4j.OpenSSL4JProvider;
import lombok.extern.log4j.Log4j2;
import sun.security.provider.MD5;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author zhaoyang
 * @date 2025/10/22
 **/
@Log4j2
public class FastMd5Digest extends Digest {
    private static OpenSSL4JProvider openSSL4JProvider;
    public static final boolean DISABLE_FAST_MD5;

    static {
        DISABLE_FAST_MD5 = Boolean.parseBoolean(System.getProperty("macrosan.fast.md5.disable", "false"));
        try {
            if (!DISABLE_FAST_MD5) {
                openSSL4JProvider = new OpenSSL4JProvider();
                log.info("load OpenSSL4J provider success");
            }
        } catch (Exception e) {
            log.error("load OpenSSL4J provider fail");
        }
    }

    public static boolean isAvailable() {
        return openSSL4JProvider != null;
    }

    MessageDigest digest;

    public FastMd5Digest() {
        try {
            digest = MessageDigest.getInstance("MD5", openSSL4JProvider);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void update(byte[] bytes) {
        digest.update(bytes);
    }

    @Override
    public void update(ByteBuffer buffer) {
        digest.update(buffer);
    }

    @Override
    public void reset() {
        digest.reset();
    }

    @Override
    public byte[] digest() {
        return digest.digest();
    }
}
