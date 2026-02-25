package com.macrosan.utils.authorize;

import com.macrosan.constants.ErrorNo;
import com.macrosan.httpserver.DateChecker;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.ResponseUtils;
import com.macrosan.utils.codec.UrlEncoder;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import io.reactivex.annotations.NonNull;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.reactivex.core.http.HttpClientRequest;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.tz.FixedDateTimeZone;
import reactor.core.publisher.Mono;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.macrosan.action.datastream.ActiveService.SYNC_AK;
import static com.macrosan.action.datastream.ActiveService.SYNC_SK;
import static com.macrosan.constants.AccountConstants.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.httpserver.MossHttpClient.EXTRA_AK_SK_MAP;
import static com.macrosan.httpserver.ResponseUtils.responseError;
import static com.macrosan.utils.authorize.AuthorizeV2.getAwsAk;
import static com.macrosan.utils.authorize.AuthorizeV2.params;
import static com.macrosan.utils.store.AWSV4Auth.hexArray;
import static com.macrosan.utils.store.StoreManagementServer.STORE_MANAGEMENT_HEADER;
import static com.macrosan.utils.store.StoreManagementServer.TARGET_IP_HEADER;

/**
 * AuthorizeV4
 * <p>
 * V4版本的签名验证
 *
 * @author liyixin
 * @date 2018/12/6
 */
public class AuthorizeV4 extends Authorize {
    public static final Logger logger = LogManager.getLogger(AuthorizeV4.class);
    private static ThreadLocal<Mac> macThreadLocal = new ThreadLocal<>();
    private static long TIME_ZONE_OFFSET = ZoneOffset.systemDefault().getRules().getOffset(Instant.now()).getTotalSeconds() * 1000;

    /**
     * 请求头或数据块中指定的签名算法
     */
    public static final String AWS4_HMAC_SHA256 = "AWS4-HMAC-SHA256";
    private static final String AWS4_HMAC_SHA256_PAYLOAD = "AWS4-HMAC-SHA256-PAYLOAD";
    /**
     * 派生签名密钥所使用的算法
     */
    private static final String ALGORITHM_HMAC_SHA256 = "hmacSHA256";

    private static final String AWS4 = "AWS4";
    private static final String CREDENTIAL_SCOPE_SUFFIX = "aws4_request";
    public static final String SERVICE_S3 = "s3";
    public static final String SERVICE_IAM = "iam";
    public static final String CONTENT_SHA256_MULTIPLE_CHUNKS_PAYLOAD = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
    public static final String CONTENT_SHA256_UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
    public static final String CONTENT_SHA256_UNSIGNED_PAYLOAD_TRAILER = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
    public static final String CONTENT_SHA256_SIGNED_PAYLOAD_TRAILER = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
    public static final String EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    /**
     * 请求头或query param中的key
     */
    public static final String X_AMZ_DECODED_CONTENT_LENGTH = "x-amz-decoded-content-length";
    public static final String X_AMZ_CONTENT_SHA_256 = "x-amz-content-sha256";
    public static final String X_AMZ_SIGNATURE = "X-Amz-Signature";
    private static final String X_AMZ_EXPIRES = "X-Amz-Expires";
    private static final String X_AMZ_SIGNED_HEADERS = "X-Amz-SignedHeaders";
    private static final String X_AMZ_CREDENTIAL = "X-Amz-Credential";
    private static final String X_AMZ_ALGORITHM = "X-Amz-Algorithm";
    public static final String X_AMZ_TRAILER = "x-amz-trailer";
    public static final String X_AMZ_TRAILER_ALGORITHM = "x-amz-checksum-crc32";
    /**
     * 匹配YYYYMMDD日期
     */
    private static final Pattern PATTERN_DATE = Pattern.compile("^(?:(?!0000)[0-9]{4}(?:(?:0?[1-9]|1[0-2])(?:0?[1-9]|1[0-9]|2[0-8])|(?:0?[13-9]|1[0-2])(?:29|30)|(?:0?[13578]|1[02])(?:31))|" +
            "(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)([-/.]?)0?2\\1(?:29))$");

    private AuthorizeV4() {
    }

    /**
     * AWS V4 签名校验
     *
     * @param request   http请求
     * @param sign      request中的签名
     * @param accessKey ak
     * @return 是否通过验证
     */
    @NonNull
    static Mono<Boolean> getSignatureMono(MsHttpRequest request, String sign, String accessKey, String signatureInRequest) {
        if (EXCLUDE_AUTH_RESOURCE.contains(sign.hashCode())) {
            return Mono.just(true);
        }
        if (StringUtils.isBlank(accessKey)) {
            request.setAccessKey(DEFAULT_ACCESS_KEY).setUserId(DEFAULT_USER_ID);
            return Mono.just(true);
        }

        Mono<Map<String, String>> mono;
        if (request.headers().contains(IS_SYNCING) && SYNC_AK.equals(accessKey)) {
            Map<String, String> secretKeyMap = new HashMap<>();
            secretKeyMap.put(SECRET_KEY, SYNC_SK);
            String userId = request.headers().get(ID);
            String userName = request.headers().get(USERNAME);
            secretKeyMap.put(ID, StringUtils.isNotEmpty(userId) ? userId : DEFAULT_USER_ID);
            secretKeyMap.put(USERNAME, StringUtils.isNotEmpty(userName) ? userId : DEFAULT_USER_NAME);
            mono = Mono.just(secretKeyMap);
        } else {
            mono = findSecretAccessKey(accessKey)
                    .flatMap(secretKeyMap -> {
                        if (secretKeyMap.isEmpty()) {
                            return findSecretAccessKeyByIam(request, sign, accessKey);
                        } else {
                            return Mono.just(secretKeyMap);
                        }
                    });
        }
        return mono
                .map(secretKeyMap -> {
                    if (request.headers().contains(SYNC_V4_CONTENT_LENGTH)) {
                        request.headers().set(CONTENT_LENGTH, request.getHeader(SYNC_V4_CONTENT_LENGTH));
                    }
                    request.setUserId(secretKeyMap.getOrDefault(ID, "")).setAccessKey(accessKey);
                    request.setUserName(secretKeyMap.getOrDefault(USERNAME, ""));

                    if (secretKeyMap.containsKey(STORE_MANAGEMENT_HEADER)) {
                        request.addMember(STORE_MANAGEMENT_HEADER, "");
                        request.addMember(TARGET_IP_HEADER, secretKeyMap.get(TARGET_IP_HEADER));
                    }

                    V4Authorization authorization = V4Authorization.from(request);
                    //规范化请求
                    CanonicalRequest canonicalRequest = CanonicalRequest.from(request, authorization);
                    //待签字符串
                    String stringToSign = getStringToSign(request, authorization, canonicalRequest);
                    //凭证
                    String[] credentialFields = authorization.get(V4Authorization.CREDENTIAL).split("/");
                    //签名byte数组
                    byte[] signatureKey = getSignatureKey(secretKeyMap.get(SECRET_KEY),
                            credentialFields[1],
                            //使用请求中的区域参数
                            credentialFields[2], credentialFields[3]);
                    String signature = Hex.encodeHexString(hmacSHA256(stringToSign, signatureKey));
                    //使用multiple chunk上传时Content-Length不是原始数据长度，需要使用原始数据长度覆盖掉再给后面获取系统元数据和传输使用
                    if (AuthorizeV4.CONTENT_SHA256_MULTIPLE_CHUNKS_PAYLOAD.equals(request.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256))) {
                        if (request.getHeader(TRANSFER_ENCODING) == null || !request.getHeader(TRANSFER_ENCODING).contains("chunked")) {
                            String decodedContentLength = request.getHeader(X_AMZ_DECODED_CONTENT_LENGTH);
                            if (decodedContentLength == null) {
                                throw new MsException(ErrorNo.MISSING_DECODED_CONTENT_LENGTH, "You must provide the Content-Length HTTP header.");
                            }
                            try {
                                Long.parseLong(decodedContentLength);
                            } catch (NumberFormatException e) {
                                throw new MsException(ErrorNo.MISSING_DECODED_CONTENT_LENGTH, e.getMessage());
                            }
                            //原来的Content-Length缓存下来
                            request.addMember(CONTENT_LENGTH, request.getHeader(CONTENT_LENGTH));
                            request.headers().set(CONTENT_LENGTH, decodedContentLength);
                        }
                    }
                    boolean ret = signature.equals(signatureInRequest);
                    if (!ret) {
                        logger.error("The Authorization was wrong : {}, the calculated signature:[{}] \n" +
                                        "the canonical request : \n" +
                                        "{}\n\n" +
                                        "the string to sign:\n" +
                                        "{}",
                                request.getHeader(AUTHORIZATION),
                                signature,
                                canonicalRequest.getRequest(),
                                stringToSign);
                    }
                    prepareV4Data(request, signature, Hex.encodeHexString(signatureKey));
                    return ret;
                });
    }

    /**
     * 生成待签字符串
     *
     * @param request          请求
     * @param authorization    被解析后的认证信息
     * @param canonicalRequest 规范化请求
     * @return 待签字符串
     */
    private static String getStringToSign(HttpServerRequest request, V4Authorization authorization, CanonicalRequest canonicalRequest) {
        return getStringToSignNoHash(AuthorizeV4.AWS4_HMAC_SHA256, request, authorization.get(V4Authorization.CREDENTIAL)) + canonicalRequest.getHashRequest();
    }

    /**
     * 生成待签字符串不包含hash的部分
     *
     * @param request    请求
     * @param credential 凭据区域信息
     * @return 待签字符串
     */
    private static String getStringToSignNoHash(String algorithm, HttpServerRequest request, String credential) {
        StringBuilder builder = new StringBuilder();
        //亚马逊通用文档：计算签名时先获取x-amz-date，后获取Date
        String date = Optional.ofNullable(request.getHeader(X_AMZ_DATE)).orElse(request.getHeader(DATE));
        if (date == null) {
            //AccessDenied auth require date  403
            throw new MsException(ErrorNo.AUTH_REQUIRE_DATE, "AWS authentication requires a valid Date or x-amz-date header");
        }
        String isoDate = DateFormatUtils.format(MsDateUtils.dateToStamp(date), MsDateUtils.ISO8601_AWS_HEADER);
        int dateStart = credential.indexOf("/") + 1;
        String dateInCredential = credential.substring(dateStart, dateStart + 8);
        //credential与header时间不一致
        if (!dateInCredential.equals(isoDate.substring(0, 8))) {
            throw new MsException(ErrorNo.AUTHORIZATION_HEADER_DATE_MALFORMED, "The authorization header is malformed; Invalid credential date. Date is not the same as X-Amz-Date.");
        }
        builder.append(algorithm).append(LINE_BREAKER)
                .append(date).append(LINE_BREAKER)
                .append(dateInCredential)
                .append(credential.substring(credential.indexOf("/", dateStart))).append(LINE_BREAKER);
        return builder.toString();
    }

    /**
     * 生成chunk待签字符串不包含hash的部分
     *
     * @param request 请求
     * @return 待签字符串
     */
    private static String getChunkStringToSignNoHash(MsHttpRequest request) {
        V4Authorization authorization = V4Authorization.from(request);
        return getStringToSignNoHash(AWS4_HMAC_SHA256_PAYLOAD, request, authorization.get(V4Authorization.CREDENTIAL));
    }

    /**
     * 派生签名密钥
     *
     * @param secretKey   sk
     * @param dateStamp   时间格式为：YYYYMMDD
     * @param regionName  区域名称
     * @param serviceName 服务类型
     * @return 签名
     */
    private static byte[] getSignatureKey(String secretKey, String dateStamp, String regionName, String serviceName) {
        byte[] kSecret = (AWS4 + secretKey).getBytes(StandardCharsets.UTF_8);
        byte[] kDate = hmacSHA256(dateStamp, kSecret);
        byte[] kRegion = hmacSHA256(regionName, kDate);
        byte[] kService = hmacSHA256(serviceName, kRegion);
        return hmacSHA256(CREDENTIAL_SCOPE_SUFFIX, kService);
    }

    private static Mac getMac() {
        Mac mac = macThreadLocal.get();
        if (mac == null) {
            try {
                macThreadLocal.set(Mac.getInstance(ALGORITHM_HMAC_SHA256));
            } catch (NoSuchAlgorithmException e) {
                logger.error(e);
            }
        }
        return macThreadLocal.get();
    }

    /**
     * HMAC-SHA256 加密
     *
     * @param data 数据
     * @param key  密钥
     * @return 加密后的数据
     */
    static byte[] hmacSHA256(String data, byte[] key) {

        Mac mac = getMac();
        byte[] ret;
        try {
            mac.init(new SecretKeySpec(key, ALGORITHM_HMAC_SHA256));
            ret = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            logger.error("calculate hmac sha256 fail :" + e);
            return DEFAULT_DATA;
        } finally {
            mac.reset();
        }
        return ret;
    }

    /**
     * 校验 payload sha256 仅v4签名 管理流部分、getObject
     *
     * @param request 请求
     * @param payload 内容
     */
    public static int checkManageStreamPayloadSHA256(MsHttpRequest request, byte[] payload) {
        //不是v4签名的请求不校验
        if (request.getHeader(AUTHORIZATION) == null || !request.getHeader(AUTHORIZATION).startsWith(AWS4_HMAC_SHA256)) {
            return ErrorNo.SUCCESS_STATUS;
        }
        String inHeader = request.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256);
        if ("GET".equals(request.rawMethod())) {
            return ErrorNo.SUCCESS_STATUS;
        }
        if (CONTENT_SHA256_UNSIGNED_PAYLOAD.equals(inHeader) || CONTENT_SHA256_MULTIPLE_CHUNKS_PAYLOAD.equals(inHeader)) {
            return ErrorNo.SUCCESS_STATUS;
        }
        String inPayload = DigestUtils.sha256Hex(payload);
        if (!inPayload.equals(inHeader)) {
            logger.error("The Authorization was wrong. content sha256 provided in header:[" + inHeader + "] payload:[" + inPayload + "]");
            responseError(request, ErrorNo.X_AMZ_CONTENT_SHA_256_MISMATCH);
            return ErrorNo.X_AMZ_CONTENT_SHA_256_MISMATCH;
        }
        return ErrorNo.SUCCESS_STATUS;
    }

    public static void checkSingleChunkObjectSHA256(MsHttpRequest request, Handler<Buffer> v4DataStreamHandler, Consumer<String> consumer) {
        String inHeader = request.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256);
        final String authorization = request.getHeader(AUTHORIZATION);
        if (authorization == null) {
            return;
        }
        if (authorization.startsWith(AuthorizeV4.AWS4_HMAC_SHA256)
                && !(AuthorizeV4.CONTENT_SHA256_MULTIPLE_CHUNKS_PAYLOAD.equals(inHeader) || AuthorizeV4.CONTENT_SHA256_SIGNED_PAYLOAD_TRAILER.equals(inHeader))) {
            if (CONTENT_SHA256_UNSIGNED_PAYLOAD.equals(inHeader) || CONTENT_SHA256_UNSIGNED_PAYLOAD_TRAILER.equals(inHeader)) {
                return;
            }
            MessageDigest sha256 = ((V4DataStreamHandler) v4DataStreamHandler).getSha256();
            String contentSHA256 = Hex.encodeHexString(sha256.digest());
            if (Objects.nonNull(request.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256))
                    && !contentSHA256.equals(request.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256))) {
                consumer.accept(contentSHA256);
            }
        }
    }

    public static void checkDecodedContentLength(MsHttpRequest request, long originalDataLength, Consumer<String> consumer) {
        String inHeader = request.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256);
        final String authorization = request.getHeader(AUTHORIZATION);
        if (AuthorizeV4.CONTENT_SHA256_SIGNED_PAYLOAD_TRAILER.equals(inHeader)) {
            if (!String.valueOf(originalDataLength).equals(request.getHeader(CONTENT_LENGTH))) {
                request.headers().set(CONTENT_LENGTH, "change-" + originalDataLength);
            }
        }
        //不是分块传输,检查length
        if (request.getHeader(TRANSFER_ENCODING) == null || !request.getHeader(TRANSFER_ENCODING).contains("chunked")) {
            String decodedContentLength = request.getHeader(X_AMZ_DECODED_CONTENT_LENGTH);
            if (authorization == null) {
                return;
            }
            if (authorization.startsWith(AuthorizeV4.AWS4_HMAC_SHA256)
                    && (AuthorizeV4.CONTENT_SHA256_MULTIPLE_CHUNKS_PAYLOAD.equals(inHeader) || AuthorizeV4.CONTENT_SHA256_SIGNED_PAYLOAD_TRAILER.equals(inHeader))) {
                if (Long.parseLong(decodedContentLength) != originalDataLength) {
                    consumer.accept(decodedContentLength);
                }
            }
        }
    }

    private static void prepareV4Data(MsHttpRequest request, String signature, String signatureKey) {
        if (request.getHeader(TRANSFER_ENCODING) == null || !request.getHeader(TRANSFER_ENCODING).contains("chunked")) {//如果不是传输分块，就使用X_AMZ_DECODED_CONTENT_LENGTH
            final String contentLength = request.getHeader(CONTENT_LENGTH);
            String decodeContentLength = Optional.ofNullable(request.getHeader(X_AMZ_DECODED_CONTENT_LENGTH))
                    .orElse(contentLength);
            request.addMember("decoded_content_length", decodeContentLength);
            request.addMember("obj_size", decodeContentLength);
        }
        String contentSHA256 = Optional.ofNullable(request.getHeader("x-amz-content-sha256"))
                .orElse("");
        request.addMember("authorization", request.getHeader(AUTHORIZATION));
        request.addMember("content_sha256", contentSHA256);
        request.addMember("string_to_sign_no_hash", AuthorizeV4.getChunkStringToSignNoHash(request));
        request.addMember("seed_signature", signature);
        request.addMember("signature_key", signatureKey);
    }

    /**
     * 对v4签名的临时url进行处理
     *
     * @param request
     * @return
     */
    public static boolean processTempUrl(MsHttpRequest request) {
        MultiMap params = request.params();
        if (!AWS4_HMAC_SHA256.equals(params.get(X_AMZ_ALGORITHM))) {
            ResponseUtils.responseError(request, ErrorNo.AUTHORIZATION_ALGORITHM_QUERY_PARAMETERS_ERROR);
            return false;
        }
        //验证URL超期时间
        String expires = params.get(X_AMZ_EXPIRES);
        long expiresLong;
        try {
            expiresLong = Long.parseLong(expires);
        } catch (NumberFormatException e) {
            logger.error("X-Amz-Expires should be a number");
            ResponseUtils.responseError(request, ErrorNo.AUTHORIZATION_EXPIRES_QUERY_PARAMETERS_ERROR);
            return false;
        }
        if (0 > expiresLong || 604800 < expiresLong) {
            logger.error("X-Amz-Expires must be less than a week (in seconds); that is, the given X-Amz-Expires must be less than 604800 seconds");
            ResponseUtils.responseError(request, ErrorNo.AUTHORIZATION_EXPIRES_QUERY_PARAMETERS_INVALID_ERROR);
            return false;
        }
        String date = params.get(X_AMZ_DATE);
        long presignedTime = MsDateUtils.dateToStamp(date);
        long currentTime = DateChecker.getCurrentTime();
        if (StringUtils.isBlank(expires) || presignedTime + expiresLong * 1000 + TIME_ZONE_OFFSET < currentTime) {
            ResponseUtils.responseError(request, ErrorNo.EXPIRED_REQUEST);
            return false;
        }
        String shouldBeSignedHeaders = request.headers().entries().stream().filter(stringStringEntry -> {
            String key = stringStringEntry.getKey().toLowerCase();
            return key.startsWith("x-amz-");
        }).map(stringStringEntry -> stringStringEntry.getKey().toLowerCase()).collect(Collectors.joining(";"));
        request.addMember("shouldBeSignedHeaders", shouldBeSignedHeaders);

        String authorization = AWS4_HMAC_SHA256 + " Credential=" + params.get(X_AMZ_CREDENTIAL) +
                ",SignedHeaders=" + params.get(X_AMZ_SIGNED_HEADERS) +
                ",Signature=" + params.get(X_AMZ_SIGNATURE);
        request.headers().set(AUTHORIZATION, new String(authorization.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1));
        request.headers().set(X_AMZ_CONTENT_SHA_256, Optional.ofNullable(request.getHeader(X_AMZ_CONTENT_SHA_256))
                .orElse(CONTENT_SHA256_UNSIGNED_PAYLOAD));
        return true;
    }

    public static void checkTransferEncoding(MsHttpRequest request) {
        if (request.getHeader(TRANSFER_ENCODING) != null
                && !request.getHeader(TRANSFER_ENCODING).contains("identity")
                && request.getHeader(AUTHORIZATION) != null
                && !(request.getHeader(AUTHORIZATION).startsWith(AuthorizeV4.AWS4_HMAC_SHA256)
                && AuthorizeV4.CONTENT_SHA256_MULTIPLE_CHUNKS_PAYLOAD.equals(request.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256)))
        ) {
            throw new MsException(ErrorNo.TRANSFER_ENCODING_NOT_IMPLEMENTED, "A header you provided implies functionality that is not implemented");
        }
    }

    @Accessors(chain = true)
    @Data
    public static class V4Authorization {
        public static final String CREDENTIAL = "Credential";
        public static final String SIGNED_HEADERS = "SignedHeaders";
        public static final String SIGNATURE = "Signature";
        String algorithm;
        Map<String, String> fields;

        private V4Authorization() {
            fields = new HashMap<>();
        }

        public static V4Authorization from(MsHttpRequest request) {
            String authorization = request.getHeader(AUTHORIZATION);
            return from(request, authorization);
        }

        static V4Authorization from(MsHttpRequest request, String authorization) {
            V4Authorization v4Authorization = new V4Authorization();
            authorization = authorization.trim();
            String[] auth = authorization.split("\\s+", 2);
            if (auth.length != 2) {
                throw new MsException(ErrorNo.AUTHORIZATION_INVALID, "Authorization header is invalid -- one and only one ' ' (space) required");
            }
            v4Authorization.setAlgorithm(auth[0]);
            String[] fields = auth[1].split(",");
            if (3 != fields.length) {
                throw new MsException(ErrorNo.AUTHORIZATION_HEADER_FIELD_MALFORMED, "The authorization header is malformed; the authorization header requires three components: Credential, " +
                        "SignedHeaders, and Signature.");
            }
            for (String field : fields) {
                String[] keyValue = field.split("=");
                v4Authorization.fields.put(keyValue[0].trim().toLowerCase(), keyValue[1].trim());
            }
            String credential = v4Authorization.get(CREDENTIAL);
            String[] credentials = credential.split("/");
            if (5 != credentials.length) {
                throw new MsException(ErrorNo.AUTHORIZATION_HEADER_MALFORMED, "The authorization header is malformed");
            }

            //date credentials[1] 时间
            if (!PATTERN_DATE.matcher(credentials[1]).matches()) {
                throw new MsException(ErrorNo.AUTHORIZATION_HEADER_DATE_FORMAT_MALFORMED, "The authorization header is malformed; This date in the credential must be in the format \"yyyyMMdd\".");
            }

            //credentials[3] 服务
            if (!request.getMember("service").equals(credentials[3])) {
                throw new MsException(ErrorNo.AUTHORIZATION_HEADER_SERVICE_MALFORMED, "The authorization header is malformed;This endpoint belongs to s3");
            }
            //credentials[4] end
            if (!"aws4_request".equals(credentials[4])) {
                throw new MsException(ErrorNo.AUTHORIZATION_HEADER_MALFORMED, "The authorization header is malformed");
            }
            return v4Authorization;
        }

        public String get(String field) {
            String ret = fields.get(field.toLowerCase());
            if (ret == null) {
                throw new MsException(ErrorNo.AUTHORIZATION_HEADER_FIELD_MALFORMED, "The authorization header is malformed; the authorization header requires three components: Credential, " +
                        "SignedHeaders, and Signature.");
            }
            return ret;
        }
    }

    @Accessors(chain = true)
    @Data
    public static class CanonicalRequest {

        /**
         * CanonicalRequest 所有成分
         */
        String method;
        String uri;
        String queryString;
        String headers;
        String signedHeaders;
        String base16HashPayload;
        /**
         * 最终的CanonicalRequest
         */
        String request;
        /**
         * hash后的CanonicalRequest
         */
        String hashRequest;

        private CanonicalRequest() {
        }

        static CanonicalRequest from(MsHttpRequest request, V4Authorization authorization) {
            CanonicalRequest canonicalRequest = new CanonicalRequest();

            Set<String> shouldBeSignedHeadersSet = new HashSet<>();
            String requestSignedHeadersStr = authorization.get(V4Authorization.SIGNED_HEADERS);
            List<String> requestSignedHeaders = Arrays.asList(requestSignedHeadersStr.split(";"));
            //host必须参与签名
            shouldBeSignedHeadersSet.add("host");

            if (request.isTempUrlAccess()) {
                //临时url签名
                //x-amz-* 必须参与签名
                String shouldBeSignedHeaders = request.getMember("shouldBeSignedHeaders");
                if (!"".equals(shouldBeSignedHeaders)) {
                    shouldBeSignedHeadersSet.addAll(Arrays.asList(shouldBeSignedHeaders.split(";")));
                }
            } else {
                //非临时url签名
                //x-amz-* 必须参与签名
                request.headers().forEach(stringStringEntry -> {
                    String key = stringStringEntry.getKey().toLowerCase();
                    if (key.startsWith("x-amz-") && !X_AMZ_CONTENT_SHA_256.equals(key)) {
                        shouldBeSignedHeadersSet.add(key);
                    }
                });
            }
            if (!requestSignedHeaders.containsAll(shouldBeSignedHeadersSet)) {
                //必须参与签名的字段未签名时返回错误
                //AccessDenied  There were headers present in the request which were not signed  403
                logger.error("signed headers:{}\n should be signed headers:{}", requestSignedHeaders, shouldBeSignedHeadersSet);
                throw new MsException(ErrorNo.HEADERS_PRESENT_NOT_SIGNED, "There were headers present in the request which were not signed");
            }

            String contentSHA256 = request.getHeader(X_AMZ_CONTENT_SHA_256);
            if (contentSHA256 == null) {
                if (SERVICE_IAM.equals(request.getMember("service")) || "sts".equals(request.getMember("service"))) {
                    String body = request.getMember("body");
                    MessageDigest sha256 = DigestUtils.getSha256Digest();
                    contentSHA256 = Hex.encodeHexString(sha256.digest(body.getBytes()));
                } else {
                    //x-amz-content-sha256 不存在
                    throw new MsException(ErrorNo.MISSING_X_AMZ_CONTENT_SHA_256, "Missing required header for this request: x-amz-content-sha256");
                }
            }
            int flag = 0;
            //不计算请求体摘要
            if (!CONTENT_SHA256_UNSIGNED_PAYLOAD.equals(contentSHA256)) {
                flag++;
            }
            //chunk方式计算请求头摘要
            if (!CONTENT_SHA256_MULTIPLE_CHUNKS_PAYLOAD.equals(contentSHA256)) {
                flag++;
            }
            if (!CONTENT_SHA256_UNSIGNED_PAYLOAD_TRAILER.equals(contentSHA256)) {
                flag++;
            }
            if (!CONTENT_SHA256_SIGNED_PAYLOAD_TRAILER.equals(contentSHA256)) {
                flag++;
            }
            if (contentSHA256.length() != 64) {
                flag++;
            } else {
                try {
                    Hex.decodeHex(contentSHA256);
                } catch (DecoderException e) {
                    flag++;
                }
            }

            if (5 <= flag) {
                //content-sha256 值非法
                throw new MsException(ErrorNo.X_AMZ_CONTENT_SHA_256_INVALID, "x-amz-content-sha256 must be UNSIGNED-PAYLOAD, STREAMING-AWS4-HMAC-SHA256-PAYLOAD, or a valid sha256 value.");
            }
            //query params 排序组合
            String _queryString = request.params().entries().stream()
                    .filter(stringStringEntry -> !X_AMZ_SIGNATURE.equals(stringStringEntry.getKey()))
                    .map(entry -> {
                        String key = entry.getKey();
                        String value = (entry.getValue() == null ? "" : entry.getValue());
                        //查询参数中的/仍需编码
                        key = UrlEncoder.encode(key, "UTF-8").replace("/", "%2F");
                        value = UrlEncoder.encode(value, "UTF-8").replace("/", "%2F");
                        return key + EQUAL + value;
                    })
                    .sorted()
                    .collect(Collectors.joining("&"));
            //signed headers 排序组合
            String _headers = requestSignedHeaders.stream()
                    .sorted(Comparator.comparing(String::toLowerCase))
                    .map(s -> s.toLowerCase() + COLON + request.headers().getAll(s).stream()
                            .map(s1 -> s1.replaceAll("\\s+", " ").trim())
                            .collect(Collectors.joining(",")) + "\n")
                    .collect(Collectors.joining(""));
            String uri;
            // 多个/连续情况下sdk发请求时会对部分进行编码，需要解码一次再编码得到未对/编码的uri
            try {
                uri = UrlEncoder.encode(URLDecoder.decode(request.getUri(), "UTF-8"), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                uri = request.getUri();
            }

            return canonicalRequest.setMethod(request.rawMethod())
                    .setUri(uri)
                    .setQueryString(_queryString)
                    .setHeaders(_headers)
                    .setSignedHeaders(requestSignedHeadersStr)
                    //使用header中的sha256，对于payload校验放在完全拿到数据之后再校验。管理流在分发时校验，数据流在后端校验
                    .setBase16HashPayload(contentSHA256)
                    .build();
        }


        /**
         * 根据各部分组成request,并计算摘要
         *
         * @return CanonicalRequest
         */
        CanonicalRequest build() {
            request = String.join(LINE_BREAKER, method, uri, queryString, headers, signedHeaders, base16HashPayload);
            hashRequest = DigestUtils.sha256Hex(request);
            return this;
        }
    }

    public static Mono<String> getAuthorizationHeader(String bucket, String authString,
                                                      HttpClientRequest request, String payloadHash, boolean defaultAk, boolean isExtra) {
        String ak;
        if (defaultAk && !isExtra) {
            ak = SYNC_AK;
        } else {
            ak = StringUtils.isNotBlank(authString) ? getAwsAk(authString) : "";
        }
        if (StringUtils.isEmpty(ak)) {
            request.headers().remove(AUTHORIZATION);
            return Mono.just("");
        }
        String methodName = request.method().name();
        String uri = request.uri();
        MultiMap params = params(uri);
        String resourcePath = uri.split("\\?")[0];
        String paramStr = params.entries().stream()
                .filter(stringStringEntry -> !X_AMZ_SIGNATURE.equals(stringStringEntry.getKey()))
                .map(entry -> {
                    String key = entry.getKey();
                    String value = (entry.getValue() == null ? "" : entry.getValue());
                    //查询参数中的/仍需编码
                    key = UrlEncoder.encode(key, "UTF-8").replace("/", "%2F");
                    value = UrlEncoder.encode(value, "UTF-8").replace("/", "%2F");
                    return key + EQUAL + value;
                })
                .sorted()
                .collect(Collectors.joining("&"));
        //计算body的hash

        if (StringUtils.isBlank(payloadHash)) {
            payloadHash = "UNSIGNED-PAYLOAD";
        }
        request.putHeader("X-Amz-Content-Sha256", payloadHash);
        String iso08601DateStr = formatBasicISO8601Date(new Date());
        request.putHeader("X-Amz-Date", iso08601DateStr);
        // 移除不参与签名的头部字段
        request.headers().remove(AUTHORIZATION);

        io.vertx.reactivex.core.MultiMap reqHeaders = request.headers();
        Map<String, String> headers = new HashMap<>();
        reqHeaders.forEach(entry -> headers.put(entry.getKey(), entry.getValue()));

        String signHeaderValue = headers.entrySet().stream()
                .sorted(Comparator.comparing(entry -> entry.getKey().toLowerCase()))
                .map(entry -> entry.getKey().toLowerCase() + ":" + entry.getValue())
                .map(str -> str.trim().replaceAll(" +", " ")).collect(Collectors.joining("\n"));

        String signHeaders = headers.keySet().stream().map(String::toLowerCase).sorted().collect(Collectors.joining(";"));
        //1.7 创建规范请求
        String canonicalRequest =
                Optional.of(methodName).orElse("") + "\n" +
                        Optional.of(resourcePath).orElse("") + "\n" +
                        Optional.of(paramStr).orElse("") + "\n" +
                        Optional.of(signHeaderValue).orElse("") + "\n" + "\n" +
                        Optional.of(signHeaders).orElse("") + "\n" + payloadHash;
        //签名算法
        String algorithmName = "AWS4-HMAC-SHA256";
        //2.2获取请求时间 这是一个 ISO8601 格式的日期时间格式 YYYYMMDD’T’HHMMSS’Z’

//        String date = DateFormatUtils.format(DateChecker.getCurrentTime(), MsDateUtils.ISO8601_AWS_HEADER);
        //2.3 获取 日期/区域/iam/aws4_request
        String region = "us-east-1";
        String service = "s3";
        String aws4Reqeust = "aws4_request";
        String otherInfo = iso08601DateStr.substring(0, 8) + "/"
                + region + "/"
                + service + "/"
                + aws4Reqeust;
        //待签字符串
        String buildSign = Optional.of(algorithmName).orElse("") + "\n" +
                Optional.of(iso08601DateStr).orElse("") + "\n" +
                Optional.of(otherInfo).orElse("") + "\n" +
                Optional.ofNullable(generateHex(canonicalRequest)).orElse("");

        return Mono.just(true)
                .flatMap(b -> {
                    if (defaultAk && !isExtra) {
                        Map<String, String> secretKeyMap = new HashMap<>();
                        secretKeyMap.put(SECRET_KEY, SYNC_SK);
                        return Mono.just(secretKeyMap);
                    } else if (StringUtils.isNotEmpty(bucket) && EXTRA_AK_SK_MAP.get(bucket) != null && ak.equals(EXTRA_AK_SK_MAP.get(bucket).var1)) {
                        Map<String, String> secretKeyMap = new HashMap<>();
                        secretKeyMap.put(SECRET_KEY, EXTRA_AK_SK_MAP.get(bucket).var2());
                        return Mono.just(secretKeyMap);
                    } else {
                        // 复制的请求都使用defaultAK，理论上不会走到这里
                        return findSecretAccessKey(ak);
                    }
                })
                .flatMap(secretKeyMap -> {
                    String sk = secretKeyMap.get(SECRET_KEY);
                    byte[] kDate = HmacSHA256(("AWS4" + sk).getBytes(StandardCharsets.UTF_8), iso08601DateStr.substring(0, 8));
                    byte[] kRegion = HmacSHA256(kDate, region);
                    byte[] kService = HmacSHA256(kRegion, service);
                    byte[] kSigning = HmacSHA256(kService, aws4Reqeust);
                    byte[] signStr = HmacSHA256(kSigning, buildSign);
                    /* Step 3.2.1 对签名编码处理 */
                    String signature = bytesToHex(signStr);
                    //AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date,
                    // Signature=b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500
                    return Mono.just("AWS4-HMAC-SHA256" + " "
                            + "Credential=" + ak + "/" + otherInfo + ", "
                            + "SignedHeaders=" + signHeaders + ", "
                            + "Signature=" + signature);
                })
                .doOnError(e -> logger.error("calculate v4 sign fail :", e));


    }

    /**
     * 计算数据的摘要
     *
     * @param data 需计算的数据
     * @return 计算出的摘要转换为16进制的字符串
     */
    private static String generateHex(String data) {
        //messageDigest,信息摘要
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(data.getBytes(StandardCharsets.UTF_8));
            byte[] digest = messageDigest.digest();
            return String.format("%064x", new BigInteger(1, digest));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static byte[] HmacSHA256(byte[] key, String data) {
        Mac mac = getMac();
        byte[] ret;
        try {
            mac.init(new SecretKeySpec(key, ALGORITHM_HMAC_SHA256));
            ret = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            logger.error("calculate hmac sha256 fail :" + e);
            return DEFAULT_DATA;
        } finally {
            mac.reset();
        }
        return ret;
    }

    private static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars).toLowerCase();
    }

    private static final DateTimeZone GMT = new FixedDateTimeZone("GMT", "GMT", 0, 0);

    protected static final DateTimeFormatter alternateBasicIso8601DateFormat = DateTimeFormat.forPattern("yyyyMMdd'T'HHmmss'Z'").withZone(GMT);

    public static String formatBasicISO8601Date(Date date) {
        return alternateBasicIso8601DateFormat.print(date.getTime());
    }


}
