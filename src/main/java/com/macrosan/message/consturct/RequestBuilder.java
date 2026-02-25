package com.macrosan.message.consturct;

import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.mqmessage.RequestMsg;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static com.macrosan.constants.ServerConstants.*;

/**
 * RequestBuilder
 * 提供自动构建RequestMsg类的方法
 * 构建出来的RequestMsg中的Map中会包含如下几大部分
 * 一.headers（去掉Authorization）
 * 二.body中附带的xml
 * 三.请求方法
 * 四.RequestId
 * 五.用户id
 *
 * @author liyixin
 * @date 2018/11/8
 */
public class RequestBuilder {

    private RequestBuilder() {
    }

    private static final Logger logger = LogManager.getLogger(RequestBuilder.class);

    public static String getRequestId() {
        // 如果lang3的版本大于3.14，且操作系统为centos，这个地方会变得很慢
        return RandomStringUtils.randomAlphanumeric(32);
    }

    /**
     * parseRequest
     * 解析头,查询参数和请求方法，并将其存到Map中
     * 此方法糅合了paramMap初始化
     *
     * @param request 被解析的请求
     * @return 解析后的Map
     */
    public static UnifiedMap<String, String> parseRequest(MsHttpRequest request) {
        List<Map.Entry<String, String>> paramList = request.params().entries();
        List<Map.Entry<String, String>> headerList = request.headers().entries();
        /* 5代表：1.method 2.bucketname 3.objectname 4.body 5.requestid*/
        UnifiedMap<String, String> resMap = new UnifiedMap<>((int) (headerList.size() + paramList.size() + 5 / 0.75) + 1);
        for (Map.Entry<String, String> entry : headerList) {
            if (!EXCLUDE_HEADER_LIST.contains(entry.getKey().hashCode()) && StringUtils.isNotBlank(entry.getValue())) {
                resMap.put(entry.getKey().toLowerCase(), entry.getValue());
            }
        }

        for (Map.Entry<String, String> entry : paramList) {
            resMap.put(entry.getKey(), entry.getValue());
        }

        for (Field field : MsHttpRequest.class.getDeclaredFields()) {
            field.setAccessible(true);
            Param annotation = field.getAnnotation(Param.class);
            if (annotation != null) {
                try {
                    resMap.put(annotation.key(), (String) field.get(request));
                } catch (IllegalAccessException e) {
                    logger.error("get annotation fail, e :", e);
                }
            }
        }

        resMap.put(METHOD, request.method().name());
        return resMap;
    }

    /**
     * buildMsg
     * 构造请求消息
     *
     * @param request   http请求
     * @param context   请求的body
     * @param signature 被调用方法的签名
     * @param requestId request id
     * @return 构造的消息
     */
    public static RequestMsg buildMsg(MsHttpRequest request, String context, int signature, String requestId) {
        RequestMsg msg = new RequestMsg();
        UnifiedMap<String, String> paramMap = parseRequest(request);
        paramMap.put(SOURCEIP, request.remoteAddress().host());

        if (StringUtils.isNotBlank(context)) {
            paramMap.put(BODY, context);
        }
        paramMap.put(REQUESTID, requestId);

        if (request.getMember("isRole") != null){
            paramMap.put("isRole",request.getMember("isRole"));
        }

        msg.setSignature(signature);
        msg.setParamMap(paramMap);
        return msg;
    }

    public static Mono<RequestMsg> buildStsMsg(MsHttpRequest request, String context, int signature, String requestId) {
        RequestMsg msg = new RequestMsg();
        UnifiedMap<String, String> paramMap = parseRequest(request);
        paramMap.put(SOURCEIP, request.remoteAddress().host());

        if (StringUtils.isNotBlank(context)) {
            paramMap.put(BODY, context);
        }
        paramMap.put(REQUESTID, requestId);

        if (request.getMember("isRole") != null){
            paramMap.put("isRole",request.getMember("isRole"));
        }

        msg.setSignature(signature);
        msg.setParamMap(paramMap);
        return Mono.just(msg);
    }
}
