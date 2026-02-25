package com.macrosan.storage.crypto.impl;

import com.macrosan.storage.crypto.Crypto;
import com.macrosan.storage.crypto.common.CryptoAlgorithm;
import com.macrosan.storage.crypto.common.CryptoConstants;
import com.macrosan.storage.crypto.common.CryptoType;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;

import static com.macrosan.storage.crypto.CryptoUtils.bytesToString;
import static com.macrosan.storage.crypto.common.CryptoConstants.SM4;

/**
 * @Description: SM4国密算法
 * @Author wanhao
 * @Date 2023/1/9 上午 10:11
 */
@CryptoType(CryptoAlgorithm.SM4)
public class SM4Crypto implements Crypto {
    private static final String DEFFAULT_SM4_SECRET_KEY = "M8R4LCFMuMZe5qOTmcjHVA==";

    //密钥的长度
    private static final int CRYPTO_KEY_SIZE = 128;

    /**
     * 加密解密算法/加密模式/填充方式
     */
    public static final String CIPHER_ALGORITHM = "SM4/CBC/PKCS7Padding";


    @Override
    public byte[] encrypt(byte[] data, byte[] cryptoKey) throws Exception {
        SecretKey secretKey = new SecretKeySpec(cryptoKey, SM4);
        Cipher encrtptCipher = Cipher.getInstance(CIPHER_ALGORITHM, BouncyCastleProvider.PROVIDER_NAME);
        encrtptCipher.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(CryptoConstants.KEY_VI));
        return encrtptCipher.doFinal(data);
    }

    @Override
    public byte[] decrypt(byte[] data, byte[] cryptoKey) throws Exception {
        SecretKey secretKey = new SecretKeySpec(cryptoKey, SM4);
        Cipher decryptCipher = Cipher.getInstance(CIPHER_ALGORITHM, BouncyCastleProvider.PROVIDER_NAME);
        decryptCipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(CryptoConstants.KEY_VI));
        return decryptCipher.doFinal(data);
    }

    @Override
    public String generateKey() throws Exception {
        KeyGenerator kg = KeyGenerator.getInstance(SM4, BouncyCastleProvider.PROVIDER_NAME);
        kg.init(CRYPTO_KEY_SIZE, new SecureRandom());
        try {
            return bytesToString(kg.generateKey().getEncoded());
        } catch (Exception ignored) {
        }
        return DEFFAULT_SM4_SECRET_KEY;
    }
}
