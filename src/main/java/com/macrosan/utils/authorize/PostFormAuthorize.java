package com.macrosan.utils.authorize;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.AccessKeyVO;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.serialize.JsonUtils;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.macrosan.constants.AccountConstants.DEFAULT_USER_ID;
import static com.macrosan.constants.AccountConstants.DEFAULT_USER_NAME;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.REDIS_IAM_INDEX;

@Log4j2
public class PostFormAuthorize extends Authorize {

    private static final RedisConnPool pool = RedisConnPool.getInstance();

    private static final String HMAC_SHA256_ALGORITHM = "HmacSHA256";
    private static final String AWS4_PREFIX = "AWS4";
    private static final String CREDENTIAL_SCOPE_SUFFIX = "aws4_request";
    private static final String SERVICE_S3 = "s3";

    /**
     * Policy 中不需要校验的表单字段
     * 根据 AWS 文档: policy, x-amz-signature, file 以及 x-ignore-* 前缀的字段不需要出现在 conditions 中
     */
    private static final Set<String> IGNORE_POLICY_FIELDS = new HashSet<>(Arrays.asList(
            "policy", "x-amz-signature", "file", "submit"
    ));

    /**
     * 支持 starts-with 匹配的字段
     */
    private static final Set<String> STARTS_WITH_FIELDS = new HashSet<>(Arrays.asList(
            "acl", "key", "Cache-Control", "Content-Type", "Content-Disposition",
            "Content-Encoding", "Expires", "success_action_redirect", "redirect"
    ));

    /**
     * 只支持精确匹配的字段
     */
    private static final Set<String> EXACT_MATCH_ONLY_FIELDS = new HashSet<>(Arrays.asList(
            "bucket", "success_action_status", "x-amz-algorithm", "x-amz-credential",
            "x-amz-date", "x-amz-security-token"
    ));

    public static String getOptionalField(MsHttpRequest request, String fieldName) {
        return request.getFormAttribute(fieldName);
    }

    // ======================== Policy 验证 ========================

    /**
     * 判断 Policy 是否过期
     *
     * @param expiration ISO8601 格式的过期时间，如 "2007-12-01T12:00:00.000Z"
     * @return true 表示已过期
     */
    public boolean isPolicyExpired(String expiration) {
        try {
            // AWS 使用 ISO8601 格式: 2007-12-01T12:00:00.000Z
            // 也支持不带毫秒的格式: 2007-12-01T12:00:00Z
            SimpleDateFormat sdf = parseExpirationFormat(expiration);
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date expiryDate = sdf.parse(expiration);
            return System.currentTimeMillis() > expiryDate.getTime();
        } catch (Exception e) {
            log.warn("PostObject: failed to parse policy expiration: {}", expiration, e);
            return true;
        }
    }

    /**
     * 根据过期时间字符串格式选择对应的 SimpleDateFormat
     */
    private SimpleDateFormat parseExpirationFormat(String expiration) {
        if (expiration.contains(".")) {
            return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        } else {
            return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        }
    }

    // ======================== Policy Conditions 验证 ========================

    /**
     * 不支持的表单字段
     * PostObject 目前不支持 acl 和 tagging 功能
     */
    private static final Set<String> UNSUPPORTED_FIELDS = new HashSet<>(Arrays.asList(
//            "acl",
//            "tagging"
//            "x-amz-acl",
//            "x-amz-grant-read", "x-amz-grant-write",
//            "x-amz-grant-read-acp", "x-amz-grant-write-acp",
//            "x-amz-grant-full-control",
//            "x-amz-tagging"
    ));

    /**
     * 检查不支持的表单字段
     *
     * @param formAttributes 表单属性
     * @return Mono.error 表示存在不支持的字段，null 表示检查通过
     */
    private Mono<Boolean> checkUnsupportedFields(MultiMap formAttributes) {
        for (Map.Entry<String, String> entry : formAttributes.entries()) {
            String fieldName = entry.getKey().toLowerCase();
            if (UNSUPPORTED_FIELDS.contains(fieldName)) {
                log.warn("PostObject: unsupported form field '{}' provided", fieldName);
                return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT,
                        "PostObject does not support field: " + fieldName));
            }
        }
        return null;
    }

    /**
     * 验证 Policy Conditions
     * <p>
     * 根据 AWS 文档，conditions 是一个数组，包含以下三种类型：
     * 1. 精确匹配: {"acl": "public-read"} 或 ["eq", "$acl", "public-read"]
     * 2. Starts With: ["starts-with", "$key", "user/user1/"]
     * 3. Content Length Range: ["content-length-range", min, max]
     *
     * @param policyJson     Policy JSON 字符串
     * @param formAttributes 表单属性
     * @param bucketName     桶名
     * @param contentLength  上传内容长度
     * @return Mono<Boolean> 验证结果
     */
    public Mono<Boolean> verifyPolicyConditions(String policyJson, MultiMap formAttributes,
                                                String bucketName, long contentLength) {
        // 检查不支持的字段：acl 和 tagging
        Mono<Boolean> unsupportedCheck = checkUnsupportedFields(formAttributes);
        if (unsupportedCheck != null) {
            return unsupportedCheck;
        }

        JsonObject policy;
        try {
            policy = new JsonObject(policyJson);
        } catch (Exception e) {
            return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT, "Invalid policy JSON format")); // todo 需要优化响应输出
        }

        // 获取 conditions 数组
        JsonArray conditions = policy.getJsonArray("conditions");
        if (conditions == null) {
            conditions = new JsonArray();
        }

        // 遍历验证每个 condition
        for (int i = 0; i < conditions.size(); i++) {
            Object condition = conditions.getValue(i);
            Mono<Boolean> result = verifySingleCondition(condition, formAttributes, bucketName, contentLength, i);
            if (result != null) {
                return result;
            }
        }

        // 验证所有表单字段都在 conditions 中有对应条件
        // 即使 conditions 为空，也需要验证（空 conditions 只允许 file/policy/x-amz-signature/x-ignore-* 字段）
        return verifyAllFormFieldsCovered(formAttributes, conditions);
    }

    /**
     * 验证单个 condition
     *
     * @return 返回 Mono.error 表示验证失败，返回 null 表示继续验证下一个
     */
    private Mono<Boolean> verifySingleCondition(Object condition, MultiMap formAttributes,
                                                String bucketName, long contentLength, int index) {
        if (condition instanceof JsonObject) {
            // 精确匹配: {"key": "value"}
            return verifyExactMatchCondition((JsonObject) condition, formAttributes, bucketName, index);
        } else if (condition instanceof JsonArray) {
            JsonArray arr = (JsonArray) condition;
            if (arr.size() < 2) {
                return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT,
                        "Invalid condition format at index " + index));
            }

            String operator = arr.getString(0);
            if ("eq".equalsIgnoreCase(operator)) {
                // 精确匹配: ["eq", "$key", "value"]
                return verifyEqCondition(arr, formAttributes, bucketName, index);
            } else if ("starts-with".equalsIgnoreCase(operator)) {
                // 前缀匹配: ["starts-with", "$key", "prefix"]
                return verifyStartsWithCondition(arr, formAttributes, bucketName, index);
            } else if ("content-length-range".equalsIgnoreCase(operator)) {
                // 内容长度范围: ["content-length-range", min, max]
                return verifyContentLengthRange(arr, contentLength, index);
            } else {
                return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT,
                        "Unknown condition operator: " + operator + " at index " + index));
            }
        } else {
            return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT,
                    "Invalid condition type at index " + index));
        }
    }

    /**
     * 验证精确匹配条件: {"key": "value"}
     */
    private Mono<Boolean> verifyExactMatchCondition(JsonObject condition, MultiMap formAttributes,
                                                    String bucketName, int index) {
        for (String fieldName : condition.fieldNames()) {
            String expectedValue = condition.getString(fieldName);
            String actualValue = getFormFieldValue(fieldName, formAttributes, bucketName);

            if (actualValue == null) {
                // 字段未提供
                log.warn("PostObject: condition field '{}' not found in form at index {}", fieldName, index);
                return Mono.error(new MsException(ErrorNo.ACCESS_DENY,
                        "Form field '" + fieldName + "' required by policy not found"));
            }

            if (!expectedValue.equals(actualValue)) {
                log.warn("PostObject: condition mismatch for '{}'. expected: {}, actual: {}",
                        fieldName, expectedValue, actualValue);
                return Mono.error(new MsException(ErrorNo.ACCESS_DENY,
                        "Policy condition failed: " + fieldName + " must be '" + expectedValue + "'"));
            }
        }
        return null; // 继续验证下一个条件
    }

    /**
     * 验证精确匹配条件: ["eq", "$key", "value"]
     */
    private Mono<Boolean> verifyEqCondition(JsonArray condition, MultiMap formAttributes,
                                            String bucketName, int index) {
        if (condition.size() != 3) {
            return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT,
                    "Invalid eq condition format at index " + index));
        }

        String fieldName = condition.getString(1);
        String expectedValue = condition.getString(2);

        // 去掉 $ 前缀
        fieldName = normalizeFieldName(fieldName);

        String actualValue = getFormFieldValue(fieldName, formAttributes, bucketName);

        if (actualValue == null) {
            log.warn("PostObject: eq condition field '{}' not found in form at index {}", fieldName, index);
            return Mono.error(new MsException(ErrorNo.ACCESS_DENY,
                    "Form field '" + fieldName + "' required by policy not found"));
        }

        if (!expectedValue.equals(actualValue)) {
            log.warn("PostObject: eq condition mismatch for '{}'. expected: {}, actual: {}",
                    fieldName, expectedValue, actualValue);
            return Mono.error(new MsException(ErrorNo.ACCESS_DENY,
                    "Policy condition failed: " + fieldName + " must equal '" + expectedValue + "'"));
        }

        return null;
    }

    /**
     * 验证 starts-with 条件: ["starts-with", "$key", "prefix"]
     * <p>
     * 特殊情况：空字符串 "" 表示允许任何内容
     */
    private Mono<Boolean> verifyStartsWithCondition(JsonArray condition, MultiMap formAttributes,
                                                    String bucketName, int index) {
        if (condition.size() != 3) {
            return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT,
                    "Invalid starts-with condition format at index " + index));
        }

        String fieldName = condition.getString(1);
        String prefix = condition.getString(2);

        // 去掉 $ 前缀
        fieldName = normalizeFieldName(fieldName);

        String actualValue = getFormFieldValue(fieldName, formAttributes, bucketName);

        if (actualValue == null) {
            // 对于 starts-with，字段缺失也是不允许的
            log.warn("PostObject: starts-with condition field '{}' not found in form at index {}", fieldName, index);
            return Mono.error(new MsException(ErrorNo.ACCESS_DENY,
                    "Form field '" + fieldName + "' required by policy not found"));
        }

        // 空前缀表示允许任何内容
        if (StringUtils.isEmpty(prefix)) {
            return null;
        }

        // 处理 Content-Type 的逗号分隔列表
        if ("Content-Type".equalsIgnoreCase(fieldName) && actualValue.contains(",")) {
            return verifyContentTypeList(actualValue, prefix, index);
        }

        if (!actualValue.startsWith(prefix)) {
            log.warn("PostObject: starts-with condition failed for '{}'. value: '{}', required prefix: '{}'",
                    fieldName, actualValue, prefix);
            return Mono.error(new MsException(ErrorNo.ACCESS_DENY,
                    "Policy condition failed: " + fieldName + " must start with '" + prefix + "'"));
        }

        return null;
    }

    /**
     * 验证 Content-Type 逗号分隔列表
     * 每个值都必须满足 starts-with 条件
     */
    private Mono<Boolean> verifyContentTypeList(String contentTypes, String prefix, int index) {
        String[] types = contentTypes.split(",");
        for (String type : types) {
            String trimmed = type.trim();
            if (!trimmed.startsWith(prefix)) {
                log.warn("PostObject: Content-Type list item '{}' does not start with '{}'", trimmed, prefix);
                return Mono.error(new MsException(ErrorNo.ACCESS_DENY,
                        "Content-Type '" + trimmed + "' does not match required prefix '" + prefix + "'"));
            }
        }
        return null;
    }

    /**
     * 验证 content-length-range 条件: ["content-length-range", min, max]
     */
    private Mono<Boolean> verifyContentLengthRange(JsonArray condition, long contentLength, int index) {
        if (condition.size() != 3) {
            return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT,
                    "Invalid content-length-range condition format at index " + index));
        }

        // contentLength 为 -1 表示不检查（可能在验证签名时还无法获取内容长度）
        if (contentLength < 0) {
            log.debug("PostObject: skipping content-length-range check, content length not available");
            return null;
        }

        // 兼容 Integer 和 Long 类型
        long minLen = getLongValue(condition.getValue(1));
        long maxLen = getLongValue(condition.getValue(2));

        if (minLen < 0 || maxLen < 0 || contentLength < minLen || contentLength > maxLen) {
            log.warn("PostObject: content-length-range condition failed. actual: {}, allowed: {}-{}",
                    contentLength, minLen, maxLen);
            return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT,
                    "Content length " + contentLength + " not in allowed range [" + minLen + ", " + maxLen + "]"));
        }

        return null;
    }

    /**
     * 验证所有表单字段都被 policy conditions 覆盖
     * <p>
     * 根据 AWS 文档，除了以下字段，其他表单字段都必须出现在 conditions 中：
     * - file
     * - policy
     * - x-amz-signature
     * - x-ignore-* 前缀的字段
     *
     * @param formAttributes 表单属性
     * @param conditions     policy conditions 数组
     * @return Mono<Boolean> 验证结果
     */
    private Mono<Boolean> verifyAllFormFieldsCovered(MultiMap formAttributes, JsonArray conditions) {
        // 收集 conditions 中涵盖的所有字段名
        Set<String> coveredFields = collectCoveredFields(conditions);

        // 检查每个表单字段是否被覆盖
        for (Map.Entry<String, String> entry : formAttributes.entries()) {
            String fieldName = entry.getKey();
            String lowerFieldName = fieldName.toLowerCase();

            // 跳过不需要在 conditions 中的字段
            if (shouldIgnoreFieldForPolicyCoverage(lowerFieldName)) {
                continue;
            }

            // 检查字段是否被 conditions 覆盖
            if (!isFieldCovered(fieldName, coveredFields)) {
                log.warn("PostObject: form field '{}' is not covered by policy conditions", fieldName);
                return Mono.error(new MsException(ErrorNo.ACCESS_DENY,
                        "Form field '" + fieldName + "' must appear in policy conditions"));
            }
        }

        return Mono.just(true);
    }

    /**
     * 从 conditions 中收集所有涵盖的字段名
     *
     * @param conditions policy conditions 数组
     * @return 被涵盖的字段名集合（小写）
     */
    private Set<String> collectCoveredFields(JsonArray conditions) {
        Set<String> coveredFields = new HashSet<>();

        for (int i = 0; i < conditions.size(); i++) {
            Object condition = conditions.getValue(i);

            if (condition instanceof JsonObject) {
                // 精确匹配: {"key": "value"} - 收集所有字段名
                JsonObject jsonObj = (JsonObject) condition;
                for (String fieldName : jsonObj.fieldNames()) {
                    coveredFields.add(fieldName.toLowerCase());
                }
            } else if (condition instanceof JsonArray) {
                JsonArray arr = (JsonArray) condition;
                if (arr.size() >= 2) {
                    String operator = arr.getString(0);
                    if ("eq".equalsIgnoreCase(operator) || "starts-with".equalsIgnoreCase(operator)) {
                        // ["eq", "$key", "value"] 或 ["starts-with", "$key", "prefix"]
                        String fieldName = arr.getString(1);
                        fieldName = normalizeFieldName(fieldName);
                        if (fieldName != null) {
                            coveredFields.add(fieldName.toLowerCase());
                        }
                    }
                    // content-length-range 不针对特定字段，跳过
                }
            }
        }

        return coveredFields;
    }

    /**
     * 判断字段是否应该在 policy coverage 检查中被忽略
     * <p>
     * 忽略的字段包括：file, policy, x-amz-signature, x-ignore-*
     *
     * @param lowerFieldName 字段名（小写）
     * @return true 表示应忽略
     */
    private boolean shouldIgnoreFieldForPolicyCoverage(String lowerFieldName) {
        // file 字段
        if ("file".equals(lowerFieldName)) {
            return true;
        }
        // policy 字段
        if ("policy".equals(lowerFieldName)) {
            return true;
        }
        // x-amz-signature 字段
        if ("x-amz-signature".equals(lowerFieldName) || "signature".equals(lowerFieldName)) {
            return true;
        }
        if ("awsaccesskeyid".equals(lowerFieldName)) {
            return true;
        }
        // x-ignore-* 前缀字段
        if (lowerFieldName.startsWith("x-ignore-")) {
            return true;
        }
        return false;
    }

    /**
     * 检查字段是否被 conditions 覆盖
     * 支持精确匹配和 x-amz-meta-* 等动态字段名
     *
     * @param fieldName     字段名
     * @param coveredFields 被涵盖的字段名集合（小写）
     * @return true 表示被覆盖
     */
    private boolean isFieldCovered(String fieldName, Set<String> coveredFields) {
        String lowerFieldName = fieldName.toLowerCase();

        // 精确匹配
        if (coveredFields.contains(lowerFieldName)) {
            return true;
        }

        // 对于 x-amz-meta-* 字段，检查是否有对应的 starts-with "" 条件（允许任意值）
        // 或者检查是否有 x-amz-meta-* 通配条件
        if (lowerFieldName.startsWith("x-amz-meta-")) {
            // 检查是否有 starts-with 空字符串条件（允许任意值）
            return coveredFields.contains(lowerFieldName);
        }

        return false;
    }

    /**
     * 获取表单字段值，处理特殊字段
     */
    private String getFormFieldValue(String fieldName, MultiMap formAttributes, String bucketName) {
        // 特殊处理 bucket 字段：从请求路径获取
        if ("bucket".equalsIgnoreCase(fieldName)) {
            return bucketName;
        }

        // 处理 x-amz-meta-* 元数据字段
        if (fieldName.toLowerCase().startsWith("x-amz-meta-")) {
            return formAttributes.get(fieldName);
        }

        // 处理其他 x-amz-* 字段
        if (fieldName.toLowerCase().startsWith("x-amz-")) {
            return formAttributes.get(fieldName);
        }

        // 普通字段
        return formAttributes.get(fieldName);
    }

    /**
     * 标准化字段名：去掉 $ 前缀
     */
    private String normalizeFieldName(String fieldName) {
        if (fieldName != null && fieldName.startsWith("$")) {
            return fieldName.substring(1);
        }
        throw new MsException(ErrorNo.INVALID_ARGUMENT, "the field name is null or not start with $, field name is " + fieldName);
    }

    /**
     * 获取 JSON 值的 long 表示
     */
    private long getLongValue(Object value) {
        try {
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            String strValue = (String) value;
            return Long.parseLong((String) value);
        } catch (NumberFormatException e) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "Invalid numeric value: " + value);
        }
    }


    /**
     * 从 Redis 查询 access key 对应的 secret key
     */
    public String getSecretKeyFromRedis(String accessKeyId) {
        try {
            // AK 在 Redis 中存储方式: REDIS_IAM_INDEX.GET(accessKeyId) -> AccessKeyVO JSON
            String raw = pool.getCommand(REDIS_IAM_INDEX).get(accessKeyId);
            if (StringUtils.isBlank(raw)) {
                return null;
            }
            AccessKeyVO accessKeyVO = JsonUtils.toObject(AccessKeyVO.class, raw.getBytes());
            return accessKeyVO != null ? accessKeyVO.getSecretKey() : null;
        } catch (Exception e) {
            log.error("PostObject: failed to get secret key from Redis for AK: {}", accessKeyId, e);
            return null;
        }
    }

    /**
     * 派生 AWS SigV4 signing key
     * signingKey = HMAC-SHA256(HMAC-SHA256(HMAC-SHA256(HMAC-SHA256("AWS4"+secretKey, date), region), "s3"), "aws4_request")
     */
    public byte[] deriveSigningKey(String secretKey, String dateStamp, String region, String service) {
        try {
            byte[] kSecret = (AWS4_PREFIX + secretKey).getBytes(StandardCharsets.UTF_8);
            byte[] kDate = hmacSHA256(dateStamp, kSecret);
            byte[] kRegion = hmacSHA256(region, kDate);
            byte[] kService = hmacSHA256(service, kRegion);
            return hmacSHA256(CREDENTIAL_SCOPE_SUFFIX, kService);
        } catch (Exception e) {
            throw new MsException(ErrorNo.SIGN_NOT_MATCH, "failed to derive signing key");
        }
    }

    /**
     * 计算 policy 签名: Hex(HMAC-SHA256(signingKey, policyBase64))
     */
    public String calculateSignature(byte[] signingKey, String policyBase64) {
        try {
            byte[] signatureBytes = hmacSHA256(policyBase64, signingKey);
            return Hex.encodeHexString(signatureBytes);
        } catch (Exception e) {
            throw new MsException(ErrorNo.SIGN_NOT_MATCH, "failed to calculate policy signature");
        }
    }

    /**
     * HMAC-SHA256 计算
     */
    public byte[] hmacSHA256(String data, byte[] key) {
        try {
            Mac mac = Mac.getInstance(HMAC_SHA256_ALGORITHM);
            mac.init(new SecretKeySpec(key, HMAC_SHA256_ALGORITHM));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new MsException(ErrorNo.SIGN_NOT_MATCH, "HMAC-SHA256 calculation failed");
        }
    }

    // ======================== POST 签名校验（响应式流内调用） ========================

    private static final String AWS4_HMAC_SHA256_POST = "AWS4-HMAC-SHA256";
    private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";

    /**
     * 签名校验入口，根据表单字段自动判断 V4 或 V2
     * <p>
     * 此方法不检查 content-length-range 条件
     *
     * @param request    请求对象
     * @param bucketName 桶名
     * @return Mono<Boolean> 签名校验结果
     */
    public Mono<Boolean> verifyPostSignatureReactive(MsHttpRequest request, String bucketName) {
        return verifyPostSignatureReactive(request, bucketName, -1);
    }

    /**
     * 签名校验入口，根据表单字段自动判断 V4 或 V2
     *
     * @param request       请求对象
     * @param bucketName    桶名
     * @param contentLength 上传内容长度，-1 表示不检查 content-length-range
     * @return Mono<Boolean> 签名校验结果
     */
    public Mono<Boolean> verifyPostSignatureReactive(MsHttpRequest request, String bucketName, long contentLength) {
        MultiMap formAttributes = request.formAttributes();
        Iterator<Map.Entry<String, String>> iterator = formAttributes.iterator();
        Set<String> keySet = new HashSet<>();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            if (keySet.contains(entry.getKey())){
                return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT, "the form attribute contains duplicate key: " + entry.getKey()));
            }
            keySet.add(entry.getKey());
        }
        String algorithm = getFormField(formAttributes, "x-amz-algorithm");
        if (AWS4_HMAC_SHA256_POST.equals(algorithm)) {
            return verifyPostSignatureV4Reactive(request, bucketName, contentLength);
        } else if (StringUtils.isNotBlank(getFormField(formAttributes, "AWSAccessKeyId"))) {
            return verifyPostSignatureV2Reactive(request, bucketName, contentLength);
        } else if (StringUtils.isBlank(getOptionalField(request, "policy"))) {
            request.setUserId(DEFAULT_USER_ID);
            request.setUserName(DEFAULT_USER_NAME);
            return Mono.just(true);
        } else {
            log.warn("PostObject: no valid signature fields found in form");
            return Mono.error(new MsException(ErrorNo.ACCESS_DENY, "Missing required signature fields"));
        }
    }

    /**
     * AWS Signature Version 4 签名校验（响应式版本）
     * <p>
     * 签名计算: Hex(HMAC-SHA256(signingKey, policyBase64))
     * signingKey 派生: HMAC-SHA256(HMAC-SHA256(HMAC-SHA256(HMAC-SHA256("AWS4"+SK, date), region), "s3"), "aws4_request")
     *
     * @param request       请求对象
     * @param bucketName    桶名
     * @param contentLength 上传内容长度
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-authentication-HTTPPOST.html">AWS SigV4 POST Authentication</a>
     */
    public Mono<Boolean> verifyPostSignatureV4Reactive(MsHttpRequest request, String bucketName, long contentLength) {
        MultiMap formAttributes = request.formAttributes();
        // 1. 提取必需表单字段
        String policyBase64 = getRequiredFormField(formAttributes, "policy");
        String signature = getRequiredFormField(formAttributes, "x-amz-signature");
        String credential = getRequiredFormField(formAttributes, "x-amz-credential");
        String amzDate = getRequiredFormField(formAttributes, "x-amz-date");

        // 2. Base64 解码 policy 并验证过期
        String policyJson;
        try {
            policyJson = new String(Base64.getDecoder().decode(policyBase64), StandardCharsets.UTF_8).replace("\\$", "$");
        } catch (IllegalArgumentException e) {
            return Mono.error(new MsException(ErrorNo.MALFORMED_XML, "Invalid base64 policy"));
        }

        String expiration;
        try {
            expiration = new JsonObject(policyJson).getString("expiration");
        } catch (Exception e) {
            return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT, "Invalid policy JSON"));
        }

        if (StringUtils.isBlank(expiration)) {
            return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT, "Policy missing expiration"));
        }
        if (isPolicyExpired(expiration)) {
            return Mono.error(new MsException(ErrorNo.EXPIRED_REQUEST, "Policy expired at " + expiration));
        }

        // 3. 解析 credential: <AK>/<date>/<region>/<service>/aws4_request
        String[] credentialParts = credential.split("/");
        if (credentialParts.length != 5 || !CREDENTIAL_SCOPE_SUFFIX.equals(credentialParts[4])) {
            return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT, "Invalid credential format"));
        }

        String accessKeyId = credentialParts[0];
        String credentialDate = credentialParts[1];
        String region = credentialParts[2];
        String service = credentialParts[3];

        if (!SERVICE_S3.equals(service)) {
            return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT, "Invalid service in credential: " + service));
        }

        // 4. 响应式从 Redis 查询 secret key
        return findSecretAccessKeyReactive(request, accessKeyId)
                .flatMap(secretKeyMap -> {
                    if (secretKeyMap.isEmpty()) {
                        return Mono.error(new MsException(ErrorNo.ACCESS_DENY, "Access key not found: " + accessKeyId));
                    }
                    String secretAccessKey = secretKeyMap.get(SECRET_KEY);
                    if (StringUtils.isBlank(secretAccessKey)) {
                        return Mono.error(new MsException(ErrorNo.ACCESS_DENY, "Secret key not found for AK: " + accessKeyId));
                    }

                    request.setUserId(secretKeyMap.getOrDefault(ID, "")).setAccessKey(accessKeyId);
                    request.setUserName(secretKeyMap.getOrDefault(USERNAME, ""));

                    // 5. 计算 signing key 并验证签名
                    byte[] signingKey = deriveSigningKey(secretAccessKey, credentialDate, region, service);
                    String calculatedSignature = calculateSignature(signingKey, policyBase64);

                    if (!calculatedSignature.equals(signature)) {
                        log.error("PostObject V4: signature mismatch. provided: {}, calculated: {}", signature, calculatedSignature);
                        return Mono.error(new MsException(ErrorNo.SIGN_NOT_MATCH, "Signature does not match"));
                    }

                    log.debug("PostObject V4: signature verified for AK={}", accessKeyId);

                    // 6. 验证 Policy Conditions
                    return verifyPolicyConditions(policyJson, formAttributes, bucketName, contentLength);
                });
    }

    /**
     * 响应式查询 AccessKey 对应的 SecretKey
     * 参考 AuthorizeV4.getSignatureMono 中的实现
     *
     * @param accessKeyId AccessKey ID
     * @return Mono<Map < String, String>> 包含 secretKey 等信息的 Map
     */
    private Mono<Map<String, String>> findSecretAccessKeyReactive(MsHttpRequest request, String accessKeyId) {
        return AuthorizeV4.findSecretAccessKey(accessKeyId)
                .flatMap(secretKeyMap -> {
                    if (secretKeyMap.isEmpty()) {
                        return findSecretAccessKeyByIam(request, "POST//", accessKeyId);
                    } else {
                        return Mono.just(secretKeyMap);
                    }
                });
    }

    /**
     * AWS Signature Version 2 签名校验（响应式版本）
     *
     * @param request       请求对象
     * @param bucketName    桶名
     * @param contentLength 上传内容长度
     */
    private Mono<Boolean> verifyPostSignatureV2Reactive(MsHttpRequest request, String bucketName, long contentLength) {
        MultiMap formAttributes = request.formAttributes();
        // 1. 提取必需表单字段
        String policyBase64 = getRequiredFormField(formAttributes, "policy");
        String accessKeyId = getRequiredFormField(formAttributes, "AWSAccessKeyId");
        String providedSignature = getRequiredFormField(formAttributes, "signature");

        // 2. Base64 解码 policy 并验证过期
        String policyJson;
        try {
            policyJson = new String(Base64.getDecoder().decode(policyBase64), StandardCharsets.UTF_8).replace("\\$", "$");
        } catch (IllegalArgumentException e) {
            return Mono.error(new MsException(ErrorNo.MALFORMED_XML, "Invalid base64 policy"));
        }

        String expiration;
        try {
            expiration = new JsonObject(policyJson).getString("expiration");
        } catch (Exception e) {
            return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT, "Invalid policy JSON"));
        }

        if (StringUtils.isBlank(expiration)) {
            return Mono.error(new MsException(ErrorNo.INVALID_ARGUMENT, "Policy missing expiration"));
        }
        if (isPolicyExpired(expiration)) {
            return Mono.error(new MsException(ErrorNo.EXPIRED_REQUEST, "Policy expired at " + expiration));
        }

        // 3. 响应式从 Redis 查询 secret key
        return findSecretAccessKeyReactive(request, accessKeyId)
                .flatMap(secretKeyMap -> {
                    if (secretKeyMap.isEmpty()) {
                        return Mono.error(new MsException(ErrorNo.ACCESS_DENY, "Access key not found: " + accessKeyId));
                    }
                    String secretAccessKey = secretKeyMap.get(SECRET_KEY);
                    if (StringUtils.isBlank(secretAccessKey)) {
                        return Mono.error(new MsException(ErrorNo.ACCESS_DENY, "Secret key not found for AK: " + accessKeyId));
                    }

                    // 4. 计算 V2 签名: Base64(HMAC-SHA1(policyBase64, secretAccessKey))
                    String calculatedSignature = hmacSHA1Base64(policyBase64, secretAccessKey);

                    if (!calculatedSignature.equals(providedSignature)) {
                        log.error("PostObject V2: signature mismatch. provided: {}, calculated: {}", providedSignature, calculatedSignature);
                        return Mono.error(new MsException(ErrorNo.SIGN_NOT_MATCH, "Signature does not match"));
                    }
                    request.setUserId(secretKeyMap.getOrDefault(ID, "")).setAccessKey(accessKeyId);
                    request.setUserName(secretKeyMap.getOrDefault(USERNAME, ""));

                    log.debug("PostObject V2: signature verified for AK={}", accessKeyId);

                    // 5. 验证 Policy Conditions
                    return verifyPolicyConditions(policyJson, formAttributes, bucketName, contentLength);
                });
    }

    /**
     * 从 MultiMap 中获取表单字段（忽略大小写）
     */
    private String getFormField(MultiMap formAttributes, String fieldName) {
        // Vert.x MultiMap 的 get 方法不区分大小写
        String value = formAttributes.get(fieldName);
        return StringUtils.isNotBlank(value) ? value.trim() : null;
    }

    /**
     * 从 MultiMap 中获取必需表单字段，缺失时抛出异常
     */
    private String getRequiredFormField(MultiMap formAttributes, String fieldName) {
        String value = getFormField(formAttributes, fieldName);
        if (value == null) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "Missing required form field: " + fieldName);
        }
        return value;
    }

    /**
     * HMAC-SHA1 计算，返回 Base64 编码字符串（用于 V2 签名）
     */
    private String hmacSHA1Base64(String data, String key) {
        try {
            Mac mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
            mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), HMAC_SHA1_ALGORITHM));
            byte[] signatureBytes = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(signatureBytes);
        } catch (Exception e) {
            throw new MsException(ErrorNo.SIGN_NOT_MATCH, "HMAC-SHA1 calculation failed");
        }
    }
}
