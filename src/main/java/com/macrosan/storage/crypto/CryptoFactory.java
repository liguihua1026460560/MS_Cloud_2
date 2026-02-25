package com.macrosan.storage.crypto;

import com.macrosan.storage.crypto.common.CryptoType;
import com.macrosan.storage.crypto.rootKey.RootSecretKeyManager;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description: 加密工厂，用于初始化加密算法和获取加密算法
 * @Author wanhao
 * @Date 2023/1/9 上午 10:13
 */
public class CryptoFactory {
    private static final Map<String, Crypto> CRYPTO_MAP = new ConcurrentHashMap<>();

    public static void init() {
        ServiceLoader<Crypto> cryptos = ServiceLoader.load(Crypto.class);
        for (Crypto crypto : cryptos) {
            CryptoType cryptoType = crypto.getClass().getAnnotation(CryptoType.class);
            String name = cryptoType.value().name();
            CRYPTO_MAP.put(name, crypto);
        }
        RootSecretKeyManager.init();
    }

    static Crypto getCrypto(String cryptoAlgorthm) {
        return CRYPTO_MAP.get(cryptoAlgorthm);
    }

    static boolean containsCryptoAlgorithm(String crypto) {
        return CRYPTO_MAP.containsKey(crypto);
    }
}
