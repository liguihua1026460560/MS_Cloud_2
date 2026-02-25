package com.macrosan.utils.msutils.md5;

import lombok.extern.log4j.Log4j2;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.MessageDigestSpi;

@Log4j2
public abstract class Digest {
    protected static Constructor constructor;

    static {
        try {
            Class clazz = Class.forName("java.security.MessageDigest$Delegate");
            constructor = clazz.getDeclaredConstructor(MessageDigestSpi.class, String.class);
            constructor.setAccessible(true);
        } catch (Exception e) {
            log.error("load md5 digest fail", e);
            System.exit(-1);
        }
    }

    public abstract void update(byte[] bytes);

    public abstract byte[] digest();

    public abstract void update(ByteBuffer buffer);

    protected MessageDigest newInstance(MessageDigestSpi digestSpi, String arg) {
        try {
            return (MessageDigest) constructor.newInstance(digestSpi, arg);
        } catch (Exception e) {
            log.error("", e);
            return null;
        }
    }

    public abstract void reset();
}
