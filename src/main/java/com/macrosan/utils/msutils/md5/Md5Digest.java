package com.macrosan.utils.msutils.md5;

import lombok.extern.log4j.Log4j2;
import sun.security.provider.MD5;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

@Log4j2
public class Md5Digest extends Digest {

    MessageDigest digest;

    public Md5Digest() {
        digest = newInstance(new MD5(), "moss Md5Digest");
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
