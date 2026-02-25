package com.macrosan.utils.msutils.checksum;

import com.macrosan.utils.cache.Md5DigestPool;
import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.util.zip.CRC32;

/**
 * @author zhaoyang
 */
public class ChecksumProvider {
    private static final String CHECKSUM_METHOD;
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
    static {
        String method = System.getProperty("checksum.method", "crc32");
        if (!"crc32".equals(method) && !"md5".equals(method)) {
            method = "crc32";
        }
        CHECKSUM_METHOD = method;
    }

    /**
     * 将long值转为8位16进制字符串
     * 同String.format("%08x", value);
     *
     * @return 8位16进制字符串，前导补零
     */
    public static String toHexString8(long value) {
        char[] chars = new char[8];
        for (int i = 7; i >= 0; i--) {
            chars[i] = HEX_CHARS[(int) (value & 0xF)];
            value >>>= 4;
        }
        return new String(chars);
    }

    private final ChecksumStrategy strategy;

    private ChecksumProvider(ChecksumStrategy strategy) {
        this.strategy = strategy;
    }


    /**
     * 创建一个校验和提供器实例
     */
    public static ChecksumProvider create() {
        if ("crc32".equals(CHECKSUM_METHOD)) {
            return new ChecksumProvider(new Crc32Strategy());
        } else {
            return new ChecksumProvider(new Md5Strategy());
        }
    }

    public void update(byte[] data) {
        strategy.update(data, 0, data.length);
    }

    public void update(byte[] data, int offset, int length) {
        strategy.update(data, offset, length);
    }

    /**
     * 获取最终校验和（十六进制小写字符串）
     */
    public String getChecksum() {
        return strategy.getChecksum();
    }


    public void release() {
        strategy.release();
    }


    private interface ChecksumStrategy {
        void update(byte[] data, int offset, int length);

        String getChecksum();

        void release();
    }


    private static class Md5Strategy implements ChecksumStrategy {
        private final MessageDigest digest;

        Md5Strategy() {
            this.digest = Md5DigestPool.acquire();
        }


        @Override
        public void release() {
            Md5DigestPool.release(digest);
        }

        @Override
        public void update(byte[] data, int offset, int length) {
            digest.update(data, offset, length);
        }

        @Override
        public String getChecksum() {
            return Hex.encodeHexString(digest.digest());
        }
    }


    private static class Crc32Strategy implements ChecksumStrategy {
        private final CRC32 crc = new CRC32();

        @Override
        public void update(byte[] data, int offset, int length) {
            crc.update(data, offset, length);
        }

        @Override
        public String getChecksum() {
            return toHexString8(crc.getValue());
        }


        @Override
        public void release() {
        }
    }


}