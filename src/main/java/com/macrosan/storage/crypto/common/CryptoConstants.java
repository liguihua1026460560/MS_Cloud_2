package com.macrosan.storage.crypto.common;

/**
 * @Description: 加密模块常量
 * @Author wanhao
 * @Date 2023/1/9 上午 10:30
 */
public class CryptoConstants {
    /**
     * 初始向量IV, 初始向量IV的长度规定为128位16个字节, 默认初始向量为随机生成.
     */
    public static final byte[] KEY_VI = "c558Gq0YQK2QUlMc".getBytes();


    public static final String SM4 = "SM4";
    public static final String AES = "AES";
}
