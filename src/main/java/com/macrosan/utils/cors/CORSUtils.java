package com.macrosan.utils.cors;

import com.macrosan.message.xmlmsg.cors.CORSConfiguration;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.serialize.JaxbUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.ErrorNo.INVALID_ARGUMENT;

public class CORSUtils {
    private static final List<String> methods = Arrays.asList("GET", "PUT", "POST", "DELETE", "HEAD");

    // 只允许英文字母、数字和连字符
    private static final String SAFE_HEADER_KEY_REGEX = "^[a-zA-Z0-9-]+$";
    public static String getXmlCorsConfig(CORSConfiguration corsConfiguration) {
        //至少包含一条规则
        if (corsConfiguration.getCorsRules().isEmpty()){
            throw new MsException(MALFORMED_XML, "The cors rules is empty.");
        }
        //至多十条规则
        if (corsConfiguration.getCorsRules().size() > 10){
            throw new MsException(CORS_RULE_TOO_LONG, "The cors rules number over the limitation(10).");
        }
        //校验ResponseVary,默认为false
        if (StringUtils.isNotEmpty(corsConfiguration.getResponseVary())){
            if (!("true".equals(corsConfiguration.getResponseVary()) || "false".equals(corsConfiguration.getResponseVary()))){
                throw new MsException(INVALID_ARGUMENT, "The response vary is invalid.");
            }
        }
        if (StringUtils.isEmpty(corsConfiguration.getResponseVary())){
            corsConfiguration.setResponseVary("false");
        }
        corsConfiguration.getCorsRules().forEach(rule -> {
            //检查必要项
            if (rule.getAllowedOrigins().isEmpty()){
                throw new MsException(MALFORMED_XML, "The allowed origins is empty.");
            }
            if (rule.getAllowedMethods().isEmpty()){
                throw new MsException(MALFORMED_XML, "The allowed methods is empty.");
            }
            //检查allowedOrigins
            boolean validAllowedOrigin = isValidAllowedOrigin(rule.getAllowedOrigins());
            if (!validAllowedOrigin){
                throw new MsException(INVALID_ARGUMENT, "The validAllowedOrigin origins is invalid.");
            }
            //检查methods
            boolean validAllowedMethods = isValidAllowedMethods(rule.getAllowedMethods());
            if (!validAllowedMethods){
                throw new MsException(INVALID_ARGUMENT, "The allowed methods is invalid.");
            }
            //检查 allowed headers
            boolean validAllowedHeaders = isValidAllowedHeaders(rule.getAllowedHeaders());
            if (!validAllowedHeaders){
                throw new MsException(INVALID_ARGUMENT, "The allowed headers is invalid.");
            }
            //检查ExposeHeader
            boolean validExposeHeaders = isValidExposeHeaders(rule.getExposeHeaders());
            if (!validExposeHeaders){
                throw new MsException(INVALID_ARGUMENT, "The exposeHeader headers is invalid.");
            }
            //检查MaxAgeSeconds
            if (StringUtils.isNotEmpty(rule.getMaxAgeSeconds())){
                validateMaxAgeSeconds(rule.getMaxAgeSeconds());
            }
        });
        return new String(JaxbUtils.toByteArray(corsConfiguration));
    }



    public static boolean isValidAllowedOrigin(Set<String> origins) {
        for (String origin : origins){
            if (!isValidAllowedOrigin(origin)){
                return false;
            }
        }
        return true;
    }

    public static boolean isValidAllowedOrigin(String origin) {
        if (origin == null || origin.isEmpty()) {
            return false;
        }
        if ("*".equals(origin)) {
            return true;
        }
        long starCount = origin.chars().filter(ch -> ch == '*').count();
        if (starCount > 1) {
            return false;
        }
        // 协议分隔符
        int schemeEnd = origin.indexOf("://");
        if (schemeEnd < 0) {
            return false;
        }
        String scheme = origin.substring(0, schemeEnd);
        if (StringUtils.isEmpty(scheme)){
            return false;
        }
        String hostPort = origin.substring(schemeEnd + 3); // skip "://"
        if (hostPort.isEmpty()) {
            return false;
        }
        String host;
        String portPart = null;
        int colonIndex = hostPort.lastIndexOf(':');
        if (colonIndex != -1) {
            host = hostPort.substring(0, colonIndex);
            portPart = hostPort.substring(colonIndex + 1);
            if (host.isEmpty()) {
                return false;
            }
        } else {
            host = hostPort;
        }
        if (StringUtils.isEmpty(host)){
            return false;
        }
        if (StringUtils.isNotEmpty(portPart)){
            // 端口必须是 1-65535 范围内的整数
            int port;
            if (portPart.contains("*") && portPart.length() == 1){
                return true;
            }
            portPart = portPart.replace("*", "");
            try {
                port = Integer.parseInt(portPart);
            } catch (NumberFormatException e) {
                return false;
            }
            if (port < 1 || port > 65535) {
                return false;
            }
        }
        if (hostPort.contains("/")) {
            return false;
        }
        return true;
    }

    private static boolean isValidAllowedMethods(Set<String> allowedMethods) {
        for (String method : allowedMethods){
            if (method == null || method.isEmpty()){
                return false;
            }
            if (!methods.contains(method)){
                return false;
            }
        }
        return true;
    }
    private static boolean isValidAllowedHeaders(Set<String> allowedHeaders) {
        for (String header : allowedHeaders){
            if (StringUtils.isEmpty(header)){
                return false;
            }
        }
        return true;
    }

    private static boolean isValidExposeHeaders(Set<String> exposeHeaders) {
        for (String header : exposeHeaders){
            if (StringUtils.isEmpty(header)) {
                return false;
            }
        }
        return true;
    }

    public static void validateMaxAgeSeconds(String maxAgeSeconds) {
        try {
            int res = Integer.parseInt(maxAgeSeconds);
            if (!(res > 0 && Integer.toString(res).equals(maxAgeSeconds))){
                throw new MsException(INVALID_ARGUMENT, "The maxAgeSeconds is invalid.");
            }
        } catch (NumberFormatException e) {
            throw new MsException(INVALID_ARGUMENT, "the maxAgeSeconds input is invalid.");
        }
    }

    public static boolean matchesAllowedOrigins(Set<String> allowedOrigins, String requestOrigin) {
        if (allowedOrigins == null || requestOrigin == null) return false;

        for (String allowedOrigin : allowedOrigins) {
            if (matchesSingle(allowedOrigin, requestOrigin)) {
                return true;
            }
        }

        return false;
    }

    private static boolean matchesSingle(String allowedOrigin, String requestOrigin) {
        if (allowedOrigin == null || requestOrigin == null) return false;

        if ("*".equals(allowedOrigin)) {
            return true;
        }

        if (!allowedOrigin.contains("*")) {
            return stripTrailingSlash(allowedOrigin).equalsIgnoreCase(stripTrailingSlash(requestOrigin));
        }

        String regex = wildcardToRegex(allowedOrigin);
        return requestOrigin.matches(regex);
    }

    private static String stripTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private static String wildcardToRegex(String pattern) {
        StringBuilder sb = new StringBuilder();
        sb.append("^");
        for (int i = 0; i < pattern.length(); i++) {
            char ch = pattern.charAt(i);
            switch (ch) {
                case '*':
                    sb.append(".*");
                    break;
                case '.':
                case '?':
                case '+':
                case '(':
                case ')':
                case '[':
                case ']':
                case '{':
                case '}':
                case '|':
                case '\\':
                case '^':
                case '$':
                    sb.append("\\").append(ch);
                    break;
                default:
                    sb.append(ch);
            }
        }
        sb.append("$");
        return sb.toString();
    }

    public static boolean matchesAllowedHeaders(Set<String> allowedHeaders, String requestHeaders) {
        //空
        if (StringUtils.isEmpty(requestHeaders)){
            return true;
        }
        //空-非空
        if (allowedHeaders.isEmpty() && !StringUtils.isEmpty(requestHeaders)){
            return false;
        }
        if (allowedHeaders.contains("*")){
            return true;
        }
        String[] headers = requestHeaders.split(",");
        for (String header : headers) {
            if (!allowedHeaders.contains(header)) {
                return false;
            }
        }
        return true;
    }
}
