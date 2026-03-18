package com.macrosan.utils.store;

import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.utils.codec.UrlEncoder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.ServerConstants.EQUAL;

@Slf4j
@Data
public class AWSV2Auth {
    private String HTTP_Verb;//请求方式
    private String Content_MD5;
    private String Content_Type;//消息类型
    private String Date;//请求时间
    private String CanonicalizedHeaders;
    private String CanonicalizedResources;

    private String AccessKeyID;
    private String SecretAccessKey;

    public static class Builder {
        private String HTTPVerb;
        private String ContentMD5;
        private String ContentType;
        private String Date;
        private String CanonicalizedHeaders;
        private String CanonicalizedResources;
        private String AccessKeyID;
        private String SecretAccessKey;

        public Builder(String accessKeyID, String secretAccessKey) {
            AccessKeyID = accessKeyID;
            SecretAccessKey = secretAccessKey;
        }

        public String getHTTPVerb() {
            return HTTPVerb;
        }

        public Builder HTTPVerb(String HTTPVerb) {
            this.HTTPVerb = HTTPVerb;
            return this;
        }

        public String getContentMD5() {
            return ContentMD5;
        }

        public Builder ContentMD5(String contentMD5) {
            ContentMD5 = contentMD5;
            return this;
        }

        public String getContentType() {
            return ContentType;
        }

        public Builder ContentType(String contentType) {
            ContentType = contentType;
            return this;
        }

        public String getDate() {
            return Date;
        }

        public Builder Date(String date) {
            Date = date;
            return this;
        }

        public String getCanonicalizedHeaders() {
            return CanonicalizedHeaders;
        }

        public Builder CanonicalizedHeaders(String canonicalizedHeaders) {
            CanonicalizedHeaders = canonicalizedHeaders;
            return this;
        }

        public String getCanonicalizedResources() {
            return CanonicalizedResources;
        }

        public Builder CanonicalizedResources(String canonicalizedResources) {
            CanonicalizedResources = canonicalizedResources;
            return this;
        }

        public AWSV2Auth build() {
            return new AWSV2Auth(this);
        }
    }

    private AWSV2Auth(Builder builder) {
        this.HTTP_Verb = builder.HTTPVerb;
        this.Content_MD5 = builder.ContentMD5;
        this.Content_Type = builder.ContentType;
        this.Date = builder.Date;
        this.CanonicalizedHeaders = builder.CanonicalizedHeaders;
        this.CanonicalizedResources = builder.CanonicalizedResources;
        this.AccessKeyID = builder.AccessKeyID;
        this.SecretAccessKey = builder.SecretAccessKey;
    }

    /**
     * * 生成代签字符
     *
     * @return StringToSign
     */
    private String getStringToSign() {
        StringBuilder sb = new StringBuilder();
        sb.append(HTTP_Verb)
                .append("\n")
                .append(Content_MD5)
                .append("\n")
                .append(Content_Type)
                .append("\n")
                .append(Date)
                .append("\n")
                .append(CanonicalizedHeaders)
                .append(CanonicalizedResources);
        log.debug("the string is :\n" + sb.toString());
        return sb.toString();
    }

    public String getAuthorization() {
        String algorithm = "HmacSHA1";
        Mac mac;
        try {
            mac = Mac.getInstance(algorithm);
            mac.init(new SecretKeySpec(SecretAccessKey.getBytes(), algorithm));
            byte[] bytes = mac.doFinal(getStringToSign().getBytes());
            byte[] signature = Base64.encodeBase64(bytes);
            return "AWS " + AccessKeyID + ":" + new String(signature);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * * 获取CanonicalizedHeaders
     *
     * @param headersMap 请求
     * @return CanonicalizedHeaders
     */
    public static String getCanonicalizedHeaders(Map<String, String> headersMap) {
        StringBuilder CanonicalizedHeaders = new StringBuilder();
        TreeMap<String, String> treeMap = new TreeMap<>(String::compareTo);
        for (Map.Entry<String, String> entry : headersMap.entrySet()) {
            String headName = entry.getKey();
            String headValue = entry.getValue();
            String lowerCase = headName.toLowerCase();
            if (lowerCase.startsWith("x-amz-")) {
                headName = lowerCase;
                treeMap.put(headName.trim(), ":" + headValue.trim() + "\n");
            }
        }
        if (!treeMap.isEmpty()) {
            {
                for (Map.Entry<String, String> entry : treeMap.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    CanonicalizedHeaders.append(key)
                            .append(value);
                }
            }
        }
        return new String(CanonicalizedHeaders);
    }

    /**
     * * 子资
     *
     * @param resourcesMap 查询参数
     * @param request
     * @return 处理后查询参数字符串
     */
    private static String getResources(TreeMap<String, String> resourcesMap, MsHttpRequest request) {
        StringBuilder resources = new StringBuilder();
        if (resourcesMap != null && !resourcesMap.isEmpty()) {
            for (Map.Entry<String, String> entry : resourcesMap.entrySet()) {
                if (SIG_INCLUDE_PARAM_LIST.contains(entry.getKey().hashCode())){
//                    resources.append(key).append("=").append(value).append("&");
                    if (NO_ENCODE_PARAM_LIST.contains(entry.getKey().hashCode())) {
                        resources.append(StringUtils.isBlank(entry.getValue()) ? entry.getKey() : entry.getKey() + EQUAL + entry.getValue());
                    } else {
                        resources.append(StringUtils.isBlank(entry.getValue()) ? entry.getKey() : entry.getKey() + EQUAL + UrlEncoder.encode(entry.getValue(), request.getCodec()));
                    }
                }
            }
            if (!"".equals(resources.toString())){
                resources.deleteCharAt(resources.lastIndexOf("&"));
            }
        }
        return new String(resources);
    }

    private static String getResources(TreeSet<String> resourcesSet) {
        StringBuilder resources = new StringBuilder();
        for (String s : resourcesSet) {
            resources.append(s).append("&");
        }
        resources.deleteCharAt(resources.lastIndexOf("&"));
        return new String(resources);
    }


    public static String getCanonicalizedResources(String bucketName, String objectName, TreeSet<String> resources) {
        StringBuilder sb = new StringBuilder();
        sb.append("/");
        //判断桶名称是否存
        if (Objects.nonNull(bucketName) && bucketName.length() > 0) {
            sb.append(bucketName);
            //对象名称是否存在
            if (Objects.nonNull(objectName) && objectName.length() > 0) {
                sb.append("/").append(objectName);
            }
            //添加子资源参
            if (Objects.nonNull(resources) && !resources.isEmpty()) {
                sb.append("?").append(getResources(resources));
            }
        }
        return new String(sb);
    }

    public static String getCanonicalizedResources(String bucketName, String objectName, TreeMap<String, String> resources, Boolean isV2, MsHttpRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append("/");
        //判断桶名称是否存
        if (Objects.nonNull(bucketName) && bucketName.length() > 0) {
            sb.append(bucketName);
//            if (isV2) {
//                sb.append("/");
//            }
            //对象名称是否存在
            if (Objects.nonNull(objectName) && objectName.length() > 0) {
                sb.append("/").append(objectName);
            }
            //添加子资源参
            if (Objects.nonNull(resources) && !resources.isEmpty()) {
                String resources1 = getResources(resources, request);
                if (StringUtils.isNotBlank(resources1)){
                    sb.append("?").append(resources1);
                }

            }
        }
        return new String(sb);
    }
}
