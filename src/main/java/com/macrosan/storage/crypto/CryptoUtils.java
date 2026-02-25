package com.macrosan.storage.crypto;

import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.ServerConstants;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.security.Security;
import java.util.Base64;


/**
 * @Description: 加密解密工具类，外部调用加密和解密的接口
 * @Author wanhao
 * @Date 2023/1/9 上午 11:32
 */
@Log4j2
public class CryptoUtils {

    private static final Base64.Encoder ENCODER = Base64.getEncoder();

    private static final Base64.Decoder DECODER = Base64.getDecoder();

    static {
        if (null == Security.getProvider(BouncyCastleProvider.PROVIDER_NAME)) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    public static byte[] encrypt(String cryptoType, String secretKey, byte[] data) {
        byte[] key = stringToBytes(secretKey);
        Crypto crypto = CryptoFactory.getCrypto(cryptoType);
        try {
            return crypto.encrypt(data, key);
        } catch (Exception e) {
            log.error("", e);
        }
        return data;
    }

    public static byte[] decrypt(String cryptoType, String secretKey, byte[] data) {
        byte[] key = stringToBytes(secretKey);
        Crypto crypto = CryptoFactory.getCrypto(cryptoType);
        try {
            return crypto.decrypt(data, key);
        } catch (Exception e) {
            log.error("", e);
        }
        return data;
    }

    public static byte[] rootDecrypt(String cryptoType, String secretKey, byte[] data) {
        byte[] key = stringToBytes(secretKey);
        Crypto crypto = CryptoFactory.getCrypto(cryptoType);
        try {
            return crypto.decrypt(data, key);
        } catch (Exception e) {
            log.error("", e);
        }
        return data;
    }

    public static String generateSecretKey(String cryptoType) {
        try {
            Crypto crypto = CryptoFactory.getCrypto(cryptoType);
            return crypto.generateKey();
        } catch (Exception ignored) {
        }
        return "";
    }

    public static boolean checkCryptoEnable(String cryptoName) {
        return cryptoName != null && !"none".equals(cryptoName);
    }

    public static void containsCryptoAlgorithm(String crypto) {
        if (StringUtils.isNotBlank(crypto)) {
            boolean res = CryptoFactory.containsCryptoAlgorithm(crypto);
            if (!res) {
                throw new MsException(ErrorNo.INVALID_ENCRYPTION_ALGORITHM_ERROR, "The encryption method specified is not supported");
            }
        }
    }

    public static byte[] stringToBytes(String secretKey) {
        return DECODER.decode(secretKey);
    }

    public static String bytesToString(byte[] secretKey) {
        return ENCODER.encodeToString(secretKey);
    }


    /**
     * 将加密信息写入SocketReqMsg
     */
    public static void putCryptoInfoToMsg(String crypto, String secretKey, SocketReqMsg msg) {
        if (checkCryptoEnable(crypto)) {
            msg.put("crypto", crypto);
            msg.put("secretKey", secretKey);
        }
    }

    /**
     * 生成密钥并将加密信息写入SocketReqMsg中
     */
    public static void generateKeyPutToMsg(String crypto, SocketReqMsg msg) {
        if (checkCryptoEnable(crypto)) {
            String secretKey = generateSecretKey(crypto);
            msg.put("crypto", crypto);
            msg.put("secretKey", secretKey);
        }
    }

    public static void clearCryptoInfo(JsonObject obj) {
        obj.remove("crypto");
        obj.remove("cryptoBeforeLen");
        obj.remove("cryptoAfterLen");
        obj.remove("secretKey");
        obj.remove("cryptoVersion");
    }

    public static void addCryptoResponse(MsHttpRequest request, String crypto) {
        if (checkCryptoEnable(crypto)) {
            request.response().putHeader(ServerConstants.X_AMZ_SERVER_SIDE_ENCRYPTION, crypto);
        }
    }
}
