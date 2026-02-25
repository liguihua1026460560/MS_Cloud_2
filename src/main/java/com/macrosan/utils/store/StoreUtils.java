package com.macrosan.utils.store;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;

public class StoreUtils {
    private static String SECRET_KEY = "";
    private static final long TIME_WINDOW_SECONDS = 300;
    public static String generateHmacSignature(long timestamp) {
        try {
            if (SECRET_KEY.isEmpty()){
                SECRET_KEY = generateHexKey();
            }
            Mac sha256Hmac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKey = new SecretKeySpec(
                    SECRET_KEY.getBytes(StandardCharsets.UTF_8),
                    "HmacSHA256"
            );
            sha256Hmac.init(secretKey);

            byte[] signatureBytes = sha256Hmac.doFinal(
                    String.valueOf(timestamp).getBytes(StandardCharsets.UTF_8)
            );

            return bytesToHex(signatureBytes);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException("HMAC generation failed", e);
        }
    }

    public static String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

    public static boolean validateRequest(String headerValue) {
        // 1. 解析头部
        String[] parts = headerValue.split(":");
        if (parts.length != 2) return false;

        long clientTimestamp;
        try {
            clientTimestamp = Long.parseLong(parts[0]);
        } catch (NumberFormatException e) {
            return false;
        }
        String clientSignature = parts[1];

        // 2. 验证时间窗口
        long currentTime = Instant.now().getEpochSecond();
        if (Math.abs(currentTime - clientTimestamp) > TIME_WINDOW_SECONDS) {
            return false;
        }
        // 3. 重新计算签名
        String serverSignature = generateHmacSignature(clientTimestamp);
        // 4. 安全对比签名（避免时序攻击）
        return secureCompare(clientSignature, serverSignature);
    }


    // 安全对比字符串（固定时间比较）
    public static boolean secureCompare(String a, String b) {
        if (a.length() != b.length()) return false;

        int result = 0;
        for (int i = 0; i < a.length(); i++) {
            result |= a.charAt(i) ^ b.charAt(i);
        }
        return result == 0;
    }

    public static byte[] generateHmacKey() throws NoSuchAlgorithmException {
        KeyGenerator keyGen = KeyGenerator.getInstance("HmacSHA256");
        keyGen.init(256, new SecureRandom()); // 使用安全随机数生成器
        return keyGen.generateKey().getEncoded();
    }

    // 转换为十六进制字符串存储
    public static String generateHexKey() throws NoSuchAlgorithmException {
        return bytesToHex(generateHmacKey());
    }
}
