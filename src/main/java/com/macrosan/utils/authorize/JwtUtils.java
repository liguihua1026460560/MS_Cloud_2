package com.macrosan.utils.authorize;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

/**
 * <p>
 *     用于生成与解析jwt令牌工具类
 * </p>
 * @date 2022/6/16 14:19
 */
public class JwtUtils {

    /**
     * 生成jwt令牌
     * @param payload 负载信息
     * @param ttlMillis 过期时间
     * @param secret 签发密钥
     * @param subject 签发人
     * @return jwt令牌字符串
     */
    public static String generateJwtToken(Map<String, Object> payload, long ttlMillis, String secret, String subject) {
        // 指定为HS256签名算法
        SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.HS256;
        // 令牌生成时间
        long nowMillis = System.currentTimeMillis();
        // 签发时间
        Date now = new Date(nowMillis);
        JwtBuilder builder = Jwts.builder()
                .setClaims(payload)
                .setId(UUID.randomUUID().toString())
                .setIssuedAt(now)
                .setSubject(subject)
                .signWith(signatureAlgorithm, secret);

        // 设置过期时间
        if (ttlMillis > 0) {
            long expireMillis = nowMillis + ttlMillis;
            Date exp = new Date(expireMillis);
            builder.setExpiration(exp);
        }

        return builder.compact();
    }

    /**
     * 解析jwt令牌信息
     * @param token 令牌字符串
     * @param secret 密钥
     * @return 负载信息
     */
    public static Claims parseJwtToken(String token, String secret) {
        return Jwts.parser()
                //设置签名的秘钥
                .setSigningKey(secret)
                //设置需要解析的jwt
                .parseClaimsJws(token).getBody();
    }

}
