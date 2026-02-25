package com.macrosan.utils.codec;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class MixedStringConverter {

    public static String iso88591ToXstr(String s0) {
        String str = new String(s0.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
        byte[] utf8Bytes = str.getBytes(StandardCharsets.UTF_8);
        StringBuilder result = new StringBuilder();
        for (byte b : utf8Bytes) {
            // 将字节转换为无符号整数，然后格式化为两位十六进制
            // 使用\\x进行转义，确保输出字面值\x
            result.append(String.format("\\x%02x", b & 0xFF));
        }
        return result.toString();
    }

    public static String convertMixedStringSimple(String mixedString) {
        StringBuilder result = new StringBuilder();
        List<Byte> currentByteSequence = new ArrayList<>();

        for (int i = 0; i < mixedString.length(); i++) {
            char c = mixedString.charAt(i);

            // 检查是否开始一个\x转义序列
            if (c == '\\' && i + 3 < mixedString.length() && mixedString.charAt(i + 1) == 'x') {
                // 提取十六进制部分
                String hex = mixedString.substring(i + 2, i + 4);
                if (isHex(hex)) {
                    // 将十六进制转换为字节并添加到当前序列
                    byte b = (byte) Integer.parseInt(hex, 16);
                    currentByteSequence.add(b);
                    i += 3; // 跳过\x和两个十六进制字符
                    continue;
                }
            }

            // 如果不是转义序列，先处理当前字节序列（如果有）
            if (!currentByteSequence.isEmpty()) {
                result.append(convertBytesToString(currentByteSequence));
                currentByteSequence.clear();
            }

            // 添加普通字符
            result.append(c);
        }

        // 处理末尾可能剩余的字节序列
        if (!currentByteSequence.isEmpty()) {
            result.append(convertBytesToString(currentByteSequence));
        }

        return result.toString();
    }

    private static boolean isHex(String str) {
        return str.matches("[0-9a-fA-F]{2}");
    }

    private static String convertBytesToString(List<Byte> byteList) {
        byte[] bytes = new byte[byteList.size()];
        for (int i = 0; i < byteList.size(); i++) {
            bytes[i] = byteList.get(i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }
}