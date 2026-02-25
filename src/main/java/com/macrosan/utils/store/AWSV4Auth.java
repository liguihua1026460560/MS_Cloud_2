package com.macrosan.utils.store;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * AWS V4 签名处理工具
 * <p>
 */
@Log4j2
public class AWSV4Auth {

    private AWSV4Auth() {
    }


    public static class Builder {

        private String accessKeyID;
        private String secretAccessKey;
        private String regionName;
        private String serviceName;
        private String httpMethodName;
        private String canonicalURI;
        private TreeMap<String, String> queryParametes;
        private String queryParameters;
        private TreeMap<String, String> awsHeaders;
        private String payload;
        private String host;
        private String xAmzDate;
        private String xAmzContent;
        private boolean toChunked;
        private boolean debug = false;

        public Builder xAmzDate(String xAmzDate) {
            this.xAmzDate = xAmzDate;
            return this;
        }

        public Builder(String accessKeyID, String secretAccessKey) {
            this.accessKeyID = accessKeyID;
            this.secretAccessKey = secretAccessKey;
        }

        public Builder regionName(String regionName) {
            this.regionName = regionName;
            return this;
        }

        public Builder serviceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Builder httpMethodName(String httpMethodName) {
            this.httpMethodName = httpMethodName;
            return this;
        }

        public Builder canonicalURI(String canonicalURI) {
            this.canonicalURI = canonicalURI;
            return this;
        }

        public Builder queryParametes(TreeMap<String, String> queryParametes) {
            this.queryParametes = queryParametes;
            return this;
        }
        public Builder queryParameters(String queryParameters) {
            this.queryParameters = queryParameters;
            return this;
        }

        public Builder awsHeaders(TreeMap<String, String> awsHeaders) {
            this.awsHeaders = awsHeaders;
            return this;
        }

        public Builder payload(String payload) {
            this.payload = payload;
            return this;
        }

        public Builder debug() {
            this.debug = true;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder xAmzContent(String xAmzContent) {
            this.xAmzContent = xAmzContent;
            return this;
        }

        public Builder toChunked(boolean toChunked) {
            this.toChunked = toChunked;
            return this;
        }

        public AWSV4Auth build() {
            return new AWSV4Auth(this);
        }
    }

    private String accessKeyID;
    private String secretAccessKey;
    private String regionName;
    private String serviceName;
    private String httpMethodName;
    private String canonicalURI;
    private TreeMap<String, String> queryParametes;
    private String queryParameters;
    private TreeMap<String, String> awsHeaders;
    private String payload;
    private boolean debug = false;

    /* Other variables */
    private final String HMACAlgorithm = "AWS4-HMAC-SHA256";
    private final String aws4Request = "aws4_request";
    private String strSignedHeader;
    private String xAmzDate;
    private String host;
    private String xAmzContent;
    private String currentDate;
    private boolean toChunked;

    private AWSV4Auth(Builder builder) {
        accessKeyID = builder.accessKeyID;
        secretAccessKey = builder.secretAccessKey;
        regionName = builder.regionName;
        serviceName = builder.serviceName;
        httpMethodName = builder.httpMethodName;
        canonicalURI = builder.canonicalURI;
        queryParametes = builder.queryParametes;
        queryParameters = builder.queryParameters;
        awsHeaders = builder.awsHeaders;
        payload = builder.payload;
        debug = builder.debug;
        host = builder.host;
        xAmzContent = builder.xAmzContent;
        /* Get current timestamp value.(UTC) */
        xAmzDate = builder.xAmzDate;
        currentDate = getDate();
        toChunked = builder.toChunked;
    }

    public String UriEncode(CharSequence input, boolean
            encodeSlash) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            char ch = input.charAt(i);
            if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a'
                    && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' ||
                    ch == '-' || ch == '~' || ch == '.') {
                result.append(ch);
            } else if (ch == '/') {
                result.append(encodeSlash ? "%2F" : ch);
            } else {
                result.append(generateHex(String.valueOf(ch)));
            }
        }
        return result.toString();
    }


    /**
     * 创建V4多块传输的规范
     *
     * @return CanonicalRequest
     */
    public String prepareCanonicalRequest() {
        StringBuilder canonicalURL = new StringBuilder("");
        canonicalURL.append(httpMethodName).append("\n");
        canonicalURI = canonicalURI == null || canonicalURI.trim().isEmpty() ? "/" : canonicalURI;
        canonicalURL.append(canonicalURI).append("\n");
        StringBuilder queryString = new StringBuilder("");
        if (queryParametes != null && !queryParametes.isEmpty()) {
            for (Map.Entry<String, String> entrySet : queryParametes.entrySet()) {
                String key = entrySet.getKey();
                String value = entrySet.getValue();
                queryString.append(key).append("=").append(encodeParameter(value)).append("&");
            }
            queryString.deleteCharAt(queryString.lastIndexOf("&"));
            queryString.append("\n");
        } else if (StringUtils.isNotBlank(queryParameters)) {
            queryString.append(queryParameters);
            queryString.append("\n");
        } else {
            queryString.append("\n");
        }
        canonicalURL.append(queryString);
        StringBuilder signedHeaders = new StringBuilder("");
        if (awsHeaders != null && !awsHeaders.isEmpty()) {
            for (Map.Entry<String, String> entrySet : awsHeaders.entrySet()) {
                String key = entrySet.getKey();
                String value = entrySet.getValue();
                signedHeaders.append(key).append(";");
                canonicalURL.append(key).append(":").append(value).append("\n");
            }
            canonicalURL.append("\n");
        } else {
            canonicalURL.append("\n");
        }
        strSignedHeader = signedHeaders.substring(0, signedHeaders.length() - 1);
        canonicalURL.append(strSignedHeader).append("\n");
        if (toChunked) {
            canonicalURL.append("STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
        } else {
            if (payload == null) {
                canonicalURL.append("UNSIGNED-PAYLOAD");
            } else {
                canonicalURL.append(generateHex(payload));
            }
            if (debug) {
                log.info("##Canonical Request:\n" + canonicalURL);
            }
        }
//                .append("\n");

        /* Step 1.6 对HTTP或HTTPS的body进行SHA256处理. */
//        payload = payload == null ? "" : payload;
//        canonicalURL.append(generateHex(payload));

        if (debug) {
            log.info("##Canonical Request:\n" + canonicalURL);
        }

        return canonicalURL.toString();
    }


    /**
     * 创建V4待签字符
     *
     * @param canonicalURL
     * @return
     */
    public String prepareStringToSign(String canonicalURL) {
        String stringToSign = "";
        stringToSign = HMACAlgorithm + "\n";
        stringToSign += xAmzDate + "\n";
        stringToSign += currentDate + "/" + regionName + "/" + serviceName + "/" + aws4Request + "\n";
        stringToSign += generateHex(canonicalURL);
        return stringToSign;
    }

    /**
     * 创建v4的块待签字符
     *
     * @param signature 上一个的签名或初始签
     * @param data      块数
     * @return
     */
    public String prepareStringToSignK(String signature, byte[] data) {
        String stringToSign = "";
        stringToSign = "AWS4-HMAC-SHA256-PAYLOAD" + "\n";
        stringToSign += xAmzDate + "\n";
        stringToSign += currentDate + "/" + regionName + "/" + serviceName + "/" + aws4Request + "\n";
        stringToSign += signature + "\n";
        stringToSign += generateHex("") + "\n";
        stringToSign += generateHex1(data);
        return stringToSign;
    }

    /**
     * v4 计算签名
     *
     * @param stringToSign
     * @return
     */
    public String calculateSignature(String stringToSign) {
        try {
            /* 派生签名密钥 */
            byte[] signatureKey = getSignatureKey(secretAccessKey, currentDate, regionName, serviceName);
            byte[] signature = HmacSHA256(signatureKey, stringToSign);
            return bytesToHex(signature);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    /**
     * v4 计算块签
     *
     * @param stringToSign 代签字符
     * @return
     */
    public String calculateKSignature(String stringToSign) {
        try {
            byte[] signatureKey = getSignatureKey(secretAccessKey, currentDate, regionName, serviceName);
            byte[] signature = HmacSHA256(signatureKey, stringToSign);
            String strHexSignature = bytesToHex(signature);
            return strHexSignature;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    /**
     * * 单块传输
     *
     * @return
     */
    public Map<String, String> getHeaders() {
        String canonicalURL = prepareCanonicalRequest();
        String stringToSign = prepareStringToSign(canonicalURL);
        String signature = calculateSignature(stringToSign);
        log.debug("the canonicalURL:\n" + canonicalURL + "\nthe stringToSign:" + stringToSign + "\nthe signature:" + signature);
        if (signature != null) {
            Map<String, String> header = new HashMap<String, String>(0);
            header.put("x-amz-date", xAmzDate);
            header.put("host", host);
            header.put("Authorization", buildAuthorizationString(signature));

            if (debug) {
                log.info("##Signature:\n" + signature);
                log.info("##Header:");
                for (Map.Entry<String, String> entrySet : header.entrySet()) {
                    log.info(entrySet.getKey() + " = " + entrySet.getValue());
                }
                log.info("================================");
            }
            return header;
        } else {
            if (debug) {
                log.info("##Signature:\n" + signature);
            }
            return null;
        }
    }

    /**
     * 连接前几步处理的字符串生成Authorization header.
     *
     * @param strSignature
     * @return
     */
    private String buildAuthorizationString(String strSignature) {
        return HMACAlgorithm + " "
                + "Credential=" + accessKeyID + "/" + getDate() + "/" + regionName + "/" + serviceName + "/" + aws4Request + ", "
                + "SignedHeaders=" + strSignedHeader + ", "
                + "Signature=" + strSignature;
    }

    /**
     * 将字符串16进制.
     *
     * @param data
     * @return
     */
    public String generateHex(String data) {
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(data.getBytes("UTF-8"));
            byte[] digest = messageDigest.digest();
            return String.format("%064x", new java.math.BigInteger(1, digest));
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        AWSV4Auth awsv4Auth = new AWSV4Auth();
        int a = 10;
        log.info(Integer.toHexString(a));
    }

    public String generateHex1(byte[] data) {
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(data);
            byte[] digest = messageDigest.digest();
            return String.format("%064x", new java.math.BigInteger(1, digest));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 以给定的key应用HmacSHA256算法处理数据.
     *
     * @param data
     * @param key
     * @return
     * @throws Exception
     * @reference: http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-java
     */
    public byte[] HmacSHA256(byte[] key, String data) throws Exception {
        String algorithm = "HmacSHA256";
        Mac mac = Mac.getInstance(algorithm);
        mac.init(new SecretKeySpec(key, algorithm));
        return mac.doFinal(data.getBytes("UTF8"));
    }

    /**
     * 生成AWS 签名
     *
     * @param key
     * @param date
     * @param regionName
     * @param serviceName
     * @return
     * @throws Exception
     * @reference http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-java
     */
    private byte[] getSignatureKey(String key, String date, String regionName, String serviceName) throws Exception {
        byte[] kSecret = ("AWS4" + key).getBytes("UTF8");
        byte[] kDate = HmacSHA256(kSecret, date);
        byte[] kRegion = HmacSHA256(kDate, regionName);
        byte[] kService = HmacSHA256(kRegion, serviceName);
        byte[] kSigning = HmacSHA256(kService, aws4Request);
        return kSigning;
    }

    public final static char[] hexArray = "0123456789ABCDEF".toCharArray();

    /**
     * 将字节数组转换为16进制字符
     *
     * @param bytes
     * @return
     */
    private String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars).toLowerCase();
    }

    /**
     * 获取yyyyMMdd'T'HHmmss'Z'格式的当前时
     *
     * @return
     */
    public static String getTimeStamp() {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));//server timezone
        return dateFormat.format(new Date());
    }

    /**
     * 获取yyyyMMdd格式的当前日
     *
     * @return
     */
    private String getDate() {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));//server timezone
        return dateFormat.format(new Date());
    }

    /**
     * UTF-8编码
     *
     * @param param
     * @return
     */
    private String encodeParameter(String param) {
        try {
            return URLEncoder.encode(param, "UTF-8");
        } catch (Exception e) {
            return URLEncoder.encode(param);
        }
    }
}