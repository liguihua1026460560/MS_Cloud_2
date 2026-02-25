package com.macrosan.utils.authorize;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;

public class BigEndianCRC32 {
    public static String longToBigEndianBase64(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(4)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt((int) value);
        return Base64.getEncoder().encodeToString(buffer.array());
    }
}
