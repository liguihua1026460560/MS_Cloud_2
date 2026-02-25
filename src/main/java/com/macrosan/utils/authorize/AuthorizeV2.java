package com.macrosan.utils.authorize;

import com.macrosan.httpserver.DateChecker;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.utils.codec.UrlEncoder;
import com.macrosan.utils.msutils.MsDateUtils;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.vertx.core.MultiMap;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.reactivex.core.http.HttpClientRequest;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.macrosan.action.datastream.ActiveService.SYNC_AK;
import static com.macrosan.action.datastream.ActiveService.SYNC_SK;
import static com.macrosan.constants.AccountConstants.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.COMPONENT_USER_ID;
import static com.macrosan.httpserver.MossHttpClient.EXTRA_AK_SK_MAP;
import static com.macrosan.httpserver.MossHttpClient.EXTRA_INDEX_IPS_ENTIRE_MAP;
import static com.macrosan.httpserver.RestfulVerticle.getSignPath;
import static com.macrosan.utils.store.StoreManagementServer.STORE_MANAGEMENT_HEADER;
import static com.macrosan.utils.store.StoreManagementServer.TARGET_IP_HEADER;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

/**
 * AuthorizeV2
 * <p>
 * V2版本的签名验证
 *
 * @author liyixin
 * @date 2018/12/4
 */
public class AuthorizeV2 extends Authorize {

    public static final Logger logger = LogManager.getLogger(AuthorizeV2.class.getName());

    private static ConcurrentHashMap<Long, Mac> macMap = new ConcurrentHashMap<>((int) (PROC_NUM / 0.75) + 1);

    private AuthorizeV2() {
    }

    /**
     * 从redis中查询sk和用户id，并进行校验，同时保存用户id
     *
     * @param request   http请求
     * @param sign      请求签名
     * @param accessKey 要查询的ak
     * @return 计算鉴权之后的结果
     */
    static Mono<Boolean> getSignatureMono(MsHttpRequest request, String sign, String accessKey, String signatureInRequest) {
        if (EXCLUDE_AUTH_RESOURCE.contains(sign.hashCode())) {
            return Mono.just(true);
        }
        if (StringUtils.isBlank(accessKey)) {
            request.setAccessKey(DEFAULT_ACCESS_KEY).setUserId(DEFAULT_USER_ID);
            return Mono.just(true);
        }

        if (request.headers().contains(IS_SYNCING) && SYNC_AK.equals(accessKey)) {
            return getSignatureMono(request, signatureInRequest);
        }
        return findSecretAccessKey(accessKey)
                .flatMap(secretKeyMap -> {
                    if (secretKeyMap.isEmpty()) {
                        if (!request.headers().contains(COMPONENT_USER_ID)) {
                            return findSecretAccessKeyByIam(request, sign, accessKey);
                        } else {
                            return  findComponentSecretAccessKey(request, accessKey);
                        }
                    } else {
                        return Mono.just(secretKeyMap);
                    }
                }).map(secretKeyMap -> {
                    Mac mac = getMac();
                    try {
                        SecretKeySpec secretKeySpec = new SecretKeySpec(secretKeyMap.get(SECRET_KEY).getBytes(),
                                ALGORITHM_HMACSHA1);
                        mac.init(secretKeySpec);

                        request.setUserId(secretKeyMap.getOrDefault(ID, "")).setAccessKey(accessKey);
                        request.setUserName(secretKeyMap.getOrDefault(USERNAME, ""));

                        if (secretKeyMap.containsKey(STORE_MANAGEMENT_HEADER)) {
                            request.addMember(STORE_MANAGEMENT_HEADER, "");
                            request.addMember(TARGET_IP_HEADER, secretKeyMap.get(TARGET_IP_HEADER));
                        }

                        byte[] stringToSign = buildSign(request);
                        boolean ret = Arrays.equals(Base64.encodeBase64(mac.doFinal(stringToSign)), signatureInRequest.getBytes());
                        if (!ret) {
                            logger.error("The Authorization was wrong : {}, the request sign : \n{}",
                                    request.getHeader(AUTHORIZATION), new String(stringToSign));
                        }
                        return ret;
                    } catch (Exception e) {
                        logger.error("calculate hmac sha1 fail :" + e);
                        return Arrays.equals(DEFAULT_SIGNATURE, signatureInRequest.getBytes());
                    } finally {
                        mac.reset();
                    }
                });
    }

    private static Mono<Boolean> getSignatureMono(MsHttpRequest request, String signatureInRequest) {
        return Mono.just(true)
                .map(b -> {
                    Mac mac = getMac();
                    try {
                        SecretKeySpec secretKeySpec = new SecretKeySpec(SYNC_SK.getBytes(), ALGORITHM_HMACSHA1);
                        mac.init(secretKeySpec);
                        String userId = request.headers().get(ID);
                        String userName = request.headers().get(USERNAME);
                        request.setUserId(StringUtils.isNotEmpty(userId) ? userId : DEFAULT_USER_ID).setAccessKey(SYNC_AK);
                        request.setUserName(StringUtils.isNotEmpty(userName) ? userName : DEFAULT_USER_NAME);
                        byte[] stringToSign = buildSign(request);
                        boolean ret = Arrays.equals(Base64.encodeBase64(mac.doFinal(stringToSign)), signatureInRequest.getBytes());
                        if (!ret) {
                            logger.error("The Authorization was wrong : {}, the request sign : \n{}",
                                    request.getHeader(AUTHORIZATION), new String(stringToSign));
                        }
                        return ret;
                    } catch (Exception e) {
                        logger.error("calculate hmac sha1 fail :" + e);
                        return Arrays.equals(DEFAULT_SIGNATURE, signatureInRequest.getBytes());
                    } finally {
                        mac.reset();
                    }
                });
    }

    private static Mac getMac() {
        long threadId = Thread.currentThread().getId();
        try {
            if (!macMap.containsKey(threadId)) {
                macMap.put(threadId, Mac.getInstance(ALGORITHM_HMACSHA1));
            }
        } catch (NoSuchAlgorithmException e) {
            logger.error(e);
        }
        return macMap.get(threadId);
    }

    /**
     * 获取用于鉴权的 sign 字节
     * <p>
     * TODO 可以尝试直接操作bytes
     *
     * @param request 请求
     * @return sign 字节
     */
    public static byte[] buildSign(MsHttpRequest request) {
        StringBuilder builder = new StringBuilder(256);
        MultiMap headMap = request.headers();

        /* 取出Content—MD5，Content-Type以及Date，并且加上换行符 */
        builder.append(request.method().name()).append(LINE_BREAKER)
                .append(Optional.ofNullable(headMap.get(CONTENT_MD5)).orElse("")).append(LINE_BREAKER)
                .append(Optional.ofNullable(headMap.get(CONTENT_TYPE)).orElse("")).append(LINE_BREAKER)
                .append(Optional.ofNullable(headMap.get(DATE)).orElse("")).append(LINE_BREAKER);

        List<String[]> sortedLis = new LinkedList<>();
        /* 取出以x-amz-开头的http header，将key value连起来后排序，再加上换行符 */
        for (Map.Entry<String, String> entry : headMap.entries()) {
            final String lowerKey = entry.getKey().toLowerCase();
            if (!lowerKey.startsWith(AUTH_HEADER)) {
                continue;
            }
            String utf8Val = entry.getValue();
            entry.setValue(utf8Val);
            sortedLis.add(new String[]{lowerKey, utf8Val});
        }

        if (sortedLis.size() > 0) {
            final StringBuilder metaData = new StringBuilder();
            sortedLis.sort(Comparator.comparing(array -> array[0]));
            for (String[] array : sortedLis) {
                metaData.append(array[0]).append(COLON).append(array[1]).append(LINE_BREAKER);
            }
            builder.append(metaData);
        }

        /* 取出查询参数，排序后连成字符串 */
        String paramStr = request.params().entries().stream()
                .filter(entry -> SIG_INCLUDE_PARAM_LIST.contains(entry.getKey().hashCode()))
                .map(entry -> {
                    /* 不需要进行对其parameter值进行encode的参数，放在这个list中  */
                    if (NO_ENCODE_PARAM_LIST.contains(entry.getKey().hashCode())) {
                        return StringUtils.isBlank(entry.getValue()) ? entry.getKey() : entry.getKey() + EQUAL + entry.getValue();
                    } else {
                        return StringUtils.isBlank(entry.getValue()) ? entry.getKey() : entry.getKey() + EQUAL + UrlEncoder.encode(entry.getValue(), request.getCodec());
                    }
                })
                .sorted().collect(Collectors.joining("&"));

        /* 获取之前计算的uri */
        builder.append(request.getUri());

        /* 如果查询参数不为空，就将uri和查询参数拼起来 */
        if (StringUtils.isNotBlank(paramStr)) {
            builder.append('?').append(paramStr);
        }


        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * @return
     * @Description 处理不同的sdk发送的元数据编码方式不同造成的签名不过的问题
     * <p>
     * 需求改变，暂时不用这个方法，改为只支持亚马逊sdk的编码，自己的java，python sdk需要修改相关内容
     * @Param request
     **/
    private static void dealDiffSdkDiffEncodeWithMetadata(MsHttpRequest request, List<String[]> sortedLis, Map.Entry<String, String> entry, String lowerKey) {

        String userAgent = request.getHeader("User-Agent");
        String utf8Val = "";
        if (userAgent == null || !userAgent.contains("aws-sdk-java")) {
            utf8Val = new String(entry.getValue().getBytes(ISO_8859_1), StandardCharsets.UTF_8);
            entry.setValue(utf8Val);
        } else {
            utf8Val = entry.getValue();
            entry.setValue(utf8Val);
        }
        sortedLis.add(new String[]{lowerKey, utf8Val});

    }

    /**
     * 根据url获取params集合。
     * 详见 io.vertx.core.http.impl.HttpUtils#params(java.lang.String)
     */
    static MultiMap params(String uri) {
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri);
        Map<String, List<String>> prms = queryStringDecoder.parameters();
        MultiMap params = new CaseInsensitiveHeaders();
        if (!prms.isEmpty()) {
            for (Map.Entry<String, List<String>> entry : prms.entrySet()) {
                params.add(entry.getKey(), entry.getValue());
            }
        }
        return params;
    }

    public static String getAwsAk(String authorization) {
        String[] array = authorization.split(" ");
        String authType = array[0];
        String ak = "";
        switch (authType) {
            case AuthorizeFactory.AWS:
                ak = array[1].split(":")[0];
                break;
            case AuthorizeFactory.SHA256:
                authorization = authorization.trim();
                String[] auth = authorization.split("\\s+", 2);
                String[] fileds = auth[1].split(",");
                String credential = fileds[0].split("=")[1];
                String[] credentials = credential.split("/");
                ak = credentials[0];
                break;
            default:
                logger.info("No such auth Type {}", authType);
                break;
        }
        return ak;
    }

    public static Mono<String> getAuthorizationHeader(UnSynchronizedRecord record, HttpClientRequest request) {
        final boolean isExtra = !EXTRA_INDEX_IPS_ENTIRE_MAP.isEmpty() && record.index != null && EXTRA_INDEX_IPS_ENTIRE_MAP.containsKey(record.index);
        return getAuthorizationHeader(record.bucket, record.object, record.headers.get(AUTHORIZATION), request, request.headers().contains(IS_SYNCING), isExtra);
    }

    public static Mono<String> getAuthorizationHeader(String bucket, String object, Map<String, String> headers, HttpClientRequest request, boolean isExtra) {
        return getAuthorizationHeader(bucket, object, headers.get(AUTHORIZATION), request, headers.containsKey(IS_SYNCING), isExtra);
    }

    public static Mono<String> getAuthorizationHeader(String bucket, String object, String authString, HttpClientRequest request, boolean isExtra) {
        return getAuthorizationHeader(bucket, object, authString, request, false, isExtra);
    }

    public static Mono<String> getAuthorizationHeader(String bucket, String object, String authString,
                                                      HttpClientRequest request, boolean defaultAk, boolean isExtra) {
        String ak;
        if (defaultAk && !isExtra) {
            ak = SYNC_AK;
        } else {
            ak = StringUtils.isNotBlank(authString) ? getAwsAk(authString) : "";
        }
        String dateGmt = MsDateUtils.stampToGMT(DateChecker.getCurrentTime());
        request.putHeader(DATE, dateGmt);
        //匿名访问请求不进行签名计算
        if (StringUtils.isEmpty(ak)) {
            request.headers().remove(AUTHORIZATION);
            return Mono.just("");
        }
        /* 将公共请求头和用户自定义请求头放入一个map，自定义请求头可以覆盖公共请求头中已有的字段 */
        io.vertx.reactivex.core.MultiMap headers = request.headers();
        Map<String, String> allHeaders = new HashMap<>();
        headers.forEach(entry -> allHeaders.put(entry.getKey(), entry.getValue()));

        /* httpMethod无需转化为string类型再append */
        HttpMethod httpMethod = request.method();

        String contentMD5 = allHeaders.getOrDefault(CONTENT_MD5, "");
        String contentType = allHeaders.getOrDefault(CONTENT_TYPE, "");

        /* 生成canonicalizedHeaders */
        /* headers中开头为x-amz的字段名提取出来，且全都转为小写，并按字典排序（TreeMap） */
        TreeMap<String, String> signHeaders = buildSignHeaders(allHeaders);
        /* 将排序完毕的headers转化为字符串 */
        String canonicalizedHeaders = buildCanonicalizedHeaders(signHeaders);

        /* 取出查询参数，排序后连成字符串 */
        MultiMap params = params(request.uri());
//        logger.info("uri:{}, params: {}", request.uri(), params);
        String paramStr = params.entries().stream()
                .filter(entry -> SIG_INCLUDE_PARAM_LIST.contains(entry.getKey().hashCode()))
                .map(entry -> {
                    if (NO_ENCODE_PARAM_LIST.contains(entry.getKey().hashCode())) {
                        return StringUtils.isBlank(entry.getValue()) ? entry.getKey() : entry.getKey() + EQUAL + entry.getValue();
                    } else {
                        return StringUtils.isBlank(entry.getValue()) ? entry.getKey() : entry.getKey() + EQUAL + UrlEncoder.encode(entry.getValue(), "UTF-8");
                    }
                })
                .sorted().collect(Collectors.joining("&"));

        /* 生成CanonicalizedResource（就是http请求行中的url） */
        String canonicalizedResource = request.path();

        if (StringUtils.isNotBlank(paramStr)) {
            canonicalizedResource += "?" + paramStr;
        }
        /* 生成stringToSign */
        String stringToSign = // httpMethod
                httpMethod.name() + "\n" +
                        // content-md5
                        contentMD5 + "\n" +
                        // content_type
                        contentType + "\n" +
                        // date
                        dateGmt + "\n" +
                        // canonicalizedHeaders
                        canonicalizedHeaders +
                        // canonicalizedResource
                        canonicalizedResource;

        String sign = getRequestSign(request, params);
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
                        return findSecretAccessKey(ak)
                                .flatMap(secretKeyMap -> {
                                    if (secretKeyMap.isEmpty()) {
                                        return findSecretAccessKeyByIam(bucket, object, sign, ak);
                                    } else {
                                        return Mono.just(secretKeyMap);
                                    }
                                });
                    }
                })
                .map(secretKeyMap -> {
                    Mac mac = getMac();
                    try {
                        SecretKeySpec secretKeySpec = new SecretKeySpec(secretKeyMap.get(SECRET_KEY).getBytes(),
                                ALGORITHM_HMACSHA1);
                        mac.init(secretKeySpec);

                        byte[] stringToSignBytes = stringToSign.getBytes(StandardCharsets.UTF_8);
                        byte[] base64EncodedBytes = Base64.encodeBase64(mac.doFinal(stringToSignBytes));
                        String signature = new String(base64EncodedBytes, StandardCharsets.UTF_8);
                        return "AWS " + ak + ":" +
                                signature;
                    } catch (Exception e) {
                        logger.error("calculate hmac sha1 fail :" + e);
                        return "";
                    } finally {
                        mac.reset();
                    }
                });
    }

    /**
     * 把排序好的headers变成一个string： 删除头与内容分隔符两端的空格，如x-amz-meta-name: MOSS
     *
     * @param signHeaders headers的 TreeMap
     *                    转换为x-amz-meta-name:MOSS； 每个字段与内容后增加\n 分隔符生成CanonicalizedHeaders;
     */
    private static String buildCanonicalizedHeaders(TreeMap<String, String> signHeaders) {
        StringBuilder canonicalizedHeaders = new StringBuilder();
        boolean flag = false;
        for (Map.Entry<String, String> entry : signHeaders.entrySet()) {
            if (!flag) {
                flag = true;
            } else {
                canonicalizedHeaders.append("\n");
            }
            canonicalizedHeaders.append(entry.getKey()).append(":").append(entry.getValue());
        }
        if (canonicalizedHeaders.toString().equals("")) {
            return "";
        }
        return canonicalizedHeaders.append("\n").toString();
    }

    /**
     * headers中开头为x-amz的字段名全都转为小写,并放入一个TreeMap返回
     *
     * @param headers 要处理的header field
     */
    private static TreeMap<String, String> buildSignHeaders(Map<String, String> headers) {
        TreeMap<String, String> signHeaders = new TreeMap<String, String>();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String key = entry.getKey().toLowerCase();
            if (key.startsWith("x-amz-")) {
                String value = entry.getValue();
                signHeaders.put(key, value);
            }
        }
        return signHeaders;
    }

    public static String getRequestSign(HttpClientRequest request, MultiMap params) {
        String host = request.getHost();
        String path = request.path();
        String pathStr = getSignPath(host, path);

        try {
            pathStr = URLDecoder.decode(pathStr, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.error("url decode failed, pathStr: {}", pathStr);
        }
        String[] array = pathStr.split(SLASH, 3);

        /* 不是斜杠结尾则统一带上斜杠 */
        if (!pathStr.endsWith(SLASH)) {
            pathStr = pathStr + SLASH;
        }

        /* 因为ObjectName中可以带斜杠，所以签名中最多就三个斜杠 */
        StringBuilder builder = new StringBuilder(32);
        int count = StringUtils.countMatches(pathStr, SLASH);
        count = Math.min(count, 3);
        builder.append(request.method());
        for (int i = 0; i < count; i++) {
            builder.append(SLASH);
        }

        String paramStr = params.entries().stream()
                .map(Map.Entry::getKey)
                .filter(key -> INCLUDE_PARAM_LIST.contains(key.hashCode()))
                .sorted()
                .collect(Collectors.joining("&"));
        if (StringUtils.isNotBlank(paramStr)) {
            builder.append('?').append(paramStr);
        }
        return builder.toString();
    }
}
