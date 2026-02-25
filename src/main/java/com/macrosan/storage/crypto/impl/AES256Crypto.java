package com.macrosan.storage.crypto.impl;

import com.macrosan.storage.crypto.Crypto;
import com.macrosan.storage.crypto.common.CryptoAlgorithm;
import com.macrosan.storage.crypto.common.CryptoConstants;
import com.macrosan.storage.crypto.common.CryptoType;
import lombok.extern.log4j.Log4j2;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;

import static com.macrosan.storage.crypto.CryptoUtils.bytesToString;
import static com.macrosan.storage.crypto.common.CryptoConstants.AES;

/**
 * @Description: AES256加密
 * @Author wanhao
 * @Date 2023/1/9 上午 10:07
 */
@Log4j2
@CryptoType(CryptoAlgorithm.AES256)
public class AES256Crypto implements Crypto {
    public static final String DEFAULT_AES256_SECRET_KEY = "JUyez2Hbhpb9qZ72lAqE6KEcv/AJ4KHY8LywZCXg/cE=";

    private static final int CRYPTO_KEY_SIZE = 256;

    /**
     * 加密解密算法/加密模式/填充方式
     */
    public static final String CIPHER_ALGORITHM = "AES/CBC/PKCS7Padding";


    @Override
    public byte[] encrypt(byte[] data, byte[] cryptoKey) throws Exception {
        SecretKey secretKey = new SecretKeySpec(cryptoKey, AES);
        Cipher encrtptCipher = Cipher.getInstance(CIPHER_ALGORITHM, BouncyCastleProvider.PROVIDER_NAME);
        encrtptCipher.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(CryptoConstants.KEY_VI));
        return encrtptCipher.doFinal(data);
    }

    @Override
    public byte[] decrypt(byte[] data, byte[] cryptoKey) throws Exception {
        SecretKey secretKey = new SecretKeySpec(cryptoKey, AES);
        Cipher decryptCipher = Cipher.getInstance(CIPHER_ALGORITHM, BouncyCastleProvider.PROVIDER_NAME);
        decryptCipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(CryptoConstants.KEY_VI));
        return decryptCipher.doFinal(data);
    }

    @Override
    public String generateKey() {
        try {
            KeyGenerator kg = KeyGenerator.getInstance(AES, BouncyCastleProvider.PROVIDER_NAME);
            kg.init(CRYPTO_KEY_SIZE, new SecureRandom());
            return bytesToString(kg.generateKey().getEncoded());
        } catch (Exception ignored) {
        }
        return DEFAULT_AES256_SECRET_KEY;
    }
}
