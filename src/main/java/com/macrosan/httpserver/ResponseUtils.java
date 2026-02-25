package com.macrosan.httpserver;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.message.consturct.ErrMsgBuilder;
import com.macrosan.message.xmlmsg.Error;
import com.macrosan.utils.bucketLog.LogFilter;
import com.macrosan.utils.serialize.JaxbUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Optional;

import static com.macrosan.constants.AccountConstants.DEFAULT_USER_ID;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.utils.bucketLog.LogRecorder.addBucketLogging;
import static com.macrosan.utils.msutils.MsDateUtils.nowToGMT;

/**
 * ResponseUtils
 * <p>
 * 帮助处理response的工具类
 *
 * @author liyixin
 * @date 2019/5/9
 */
@Log4j2
public class ResponseUtils {

    private ResponseUtils() {
    }

    public static HttpServerResponse addAllowHeader(HttpServerResponse response) {
        final StringBuilder allowHeader = new StringBuilder(64);
        for (Map.Entry<String, String> header : response.headers()) {
            allowHeader.append(header.getKey()).append(',');
        }
        return response.putHeader(ACCESS_CONTROL_HEADERS, allowHeader.toString());
    }

    /**
     * 给http请求添加公共消息头
     *
     * @param request http请求
     * @return http response
     */
    public static HttpServerResponse addPublicHeaders(HttpServerRequest request, String requestId) {
        if (StringUtils.isNotBlank(requestId)) {
            if (request.headers().contains(X_AUTH_TOKEN)) {
                request.response().putHeader(X_OPENSTACK_REQUEST_ID, requestId);
            } else {
                request.response().putHeader(X_AMZ_REQUEST_ID, requestId);
            }
        }
        return request.response()
                .putHeader(SERVER, "MOSS")
                .putHeader(ACCESS_CONTROL_HEADERS, "*")
                .putHeader(ACCESS_CONTROL_ORIGIN, "*")
                .putHeader(DATE, nowToGMT());
    }

    public static HttpServerResponse addPublicHeaders(HttpServerRequest request) {
        return request.response()
                .putHeader(SERVER, "MOSS")
                .putHeader(ACCESS_CONTROL_HEADERS, "*")
                .putHeader(ACCESS_CONTROL_ORIGIN, "*")
                .putHeader(DATE, nowToGMT());
    }

    /**
     * 请求超时返回的异常xml
     *
     * @param request http请求
     */
    static void requestTimeOut(HttpServerRequest request, String requestId) {
        responseError(request, requestId, ErrorNo.UNKNOWN_ERROR, false);
    }

    /**
     * 访问拒绝时返回的异常xml
     *
     * @param request http请求
     */
    static void accessDeny(HttpServerRequest request) {
        responseError(request, ErrorNo.ACCESS_DENY);
    }

    static void signNotMatch(HttpServerRequest request) {
        responseError(request, ErrorNo.SIGN_NOT_MATCH);
    }

    /**
     * 参数异常时返回的异常xml
     *
     * @param request http请求
     */
    static void invalidRequest(HttpServerRequest request) {
        responseError(request, ErrorNo.INVALID_REQUEST);
    }

    /**
     * 参数异常时返回的异常xml
     *
     * @param request http请求
     */
    static void invalidArgument(HttpServerRequest request) {
        responseError(request, ErrorNo.INVALID_ARGUMENT);
    }

    /**
     * 根据错误码返回xml
     *
     * @param request   http请求
     * @param errorCode 错误码
     */
    public static void responseError(HttpServerRequest request, int errorCode) {
        responseError(request, getRequestId(), errorCode, false);
    }

    /**
     * 根据错误码返回xml
     *
     * @param request   http请求
     * @param requestId request id
     * @param errorCode 错误码
     */
    public static void responseError(HttpServerRequest request, String requestId, int errorCode, boolean hasTrunk) {
        String bucketName = "";
        String objName = "";
        if (request instanceof MsHttpRequest) {
            bucketName = ((MsHttpRequest) request).getBucketName();
            objName = ((MsHttpRequest) request).getObjectName();
        }
        Error error = ErrMsgBuilder.build(
                SLASH +
                        Optional.ofNullable(bucketName).orElse("") + SLASH +
                        Optional.ofNullable(objName).orElse(""),
                ServerConfig.getInstance().getHostUuid(),
                requestId,
                errorCode);
        if (request instanceof MsHttpRequest && StringUtils.isNotBlank(((MsHttpRequest) request).getUserId())) {
            if (((MsHttpRequest) request).getUserId().equalsIgnoreCase(DEFAULT_USER_ID) && error.getCode().equals("AccessForbidden")) {
                error.setCode("AccessDenied");
            }
        }

        try {

            if (hasTrunk) {
                Buffer buffer = Buffer.buffer(JaxbUtils.toByteArray(error));
                request.response()
//                      .putHeader(CONNECTION, "close")
//                      .putHeader(CONTENT_LENGTH, String.valueOf(buffer.length()))
                        .setStatusCode(ErrMsgBuilder.getHttpCode(errorCode))
                        .end(buffer.slice("<?xml version=\"1.0\" encoding=\"UTF-8\"?>".length(), JaxbUtils.toByteArray(error).length));
            } else {
                Buffer buffer = Buffer.buffer(JaxbUtils.toByteArray(error));
                if (request.headers().contains(X_AUTH_TOKEN) && ErrorNo.SIGN_NOT_MATCH == errorCode) {
                    request.response().setStatusCode(401);
                } else {
                    request.response().setStatusCode(ErrMsgBuilder.getHttpCode(errorCode));
                }
                request.response()
                        .putHeader(CONTENT_TYPE, "application/xml")
                        .putHeader(SERVER, "MOSS")
                        .putHeader(ACCESS_CONTROL_HEADERS, "Etag")
                        .putHeader(ACCESS_CONTROL_ORIGIN, "*")
                        .putHeader(DATE, nowToGMT())
                        .putHeader(CONNECTION, "close")
                        .putHeader(CONTENT_LENGTH, String.valueOf(buffer.length()))
                        .end(buffer);
            }

            request.connection().close();

//            dealUnsynRecord(request, errorCode);
        } catch (Exception e) {
            if (!request.headers().contains(IS_SYNCING)) {
                log.error("response error, cause : {}", e.getMessage());
            }
        }

        if (request instanceof MsHttpRequest) {
            //使用过滤器对错误请求进行过滤
            LogFilter.judgeRequest((MsHttpRequest) request).subscribe(result -> {
                if (!"putLog".equals(((MsHttpRequest) request).getMember("requestType"))) {
                    if (result) {
                        MsHttpRequest request1 = (MsHttpRequest) request;
                        request1.addMember("ErrorCode", error.getCode());
                        Long startTime = Long.valueOf(((MsHttpRequest) request).getMember("startTime"));
                        RedisConnPool.getInstance().getReactive(REDIS_BUCKETINFO_INDEX).hget(request1.getBucketName(), "user_id").subscribe(bucketOwner -> {
                            addBucketLogging(request1, startTime, startTime, bucketOwner);
                        });
                    }
                }
            });
        }
    }
}
