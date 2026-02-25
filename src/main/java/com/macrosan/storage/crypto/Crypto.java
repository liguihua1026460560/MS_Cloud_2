package com.macrosan.storage.crypto;

/**
 * @Description: 加密算法的接口，实现在impl目录下
 * @Author wanhao
 * @Date 2023/1/9 上午 10:01
 */
public interface Crypto {
    /**
     * 加密数据
     *
     * @param data      待加密的数据
     * @param cryptoKey 密钥
     * @return 加密后的数据
     */
    byte[] encrypt(byte[] data, byte[] cryptoKey) throws Exception;

    /**
     * 数据解密
     *
     * @param data      待解密的数据
     * @param cryptoKey 密钥
     * @return 解密后的数据
     */
    byte[] decrypt(byte[] data, byte[] cryptoKey) throws Exception;

    /**
     * 随机生成密钥
     *
     * @return 生成的密钥
     */
    String generateKey() throws Exception;
}
