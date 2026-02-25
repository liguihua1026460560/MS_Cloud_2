package com.macrosan.utils.msutils;

import com.macrosan.httpserver.MsHttpRequest;
import io.rsocket.exceptions.ApplicationErrorException;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.util.Map;
import java.util.function.Predicate;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.httpserver.ResponseUtils.responseError;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;

/**
 * MsException
 * <p>
 * 自定义异常，额外携带了错误码
 *
 * @author liyixin
 * @date 2018/12/21
 */
public class MsException extends RuntimeException {

    private static final Logger logger = LogManager.getLogger(MsException.class.getName());

    private static final long serialVersionUID = 9083720827068312589L;

    @Getter
    private final int errCode;

    public MsException(int errCode, String message) {
        super(message, null, false, false);
        this.errCode = errCode;
    }

    public MsException(int errCode, String message, Throwable cause) {
        super(message, cause);
        this.errCode = errCode;
    }

    public static <T> void throwWhen(Predicate<T> tester, T param, MsException ex) {
        if (tester.test(param)) {
            throw ex;
        }
    }

    public static <T, U> void throwWhenEmpty(Map<T, U> param, MsException ex) {
        throwWhen(Map::isEmpty, param, ex);
    }

    public static <T extends Throwable> void dealException(String requestId, MsHttpRequest request, T exception, boolean hasTrunk) {
        if (exception instanceof MsException) {
            request.response().headers().set(X_AMZ_REQUEST_ID, requestId);
            MsException e = (MsException) exception;
            if (e.getMessage().contains("No enough space")) {
                logger.info(e.getMessage() + " bucket: {} object: {} request id: {}",
                        request.getBucketName(), request.getObjectName(), requestId);
            } else if (request.headers().contains(IS_SYNCING)) {
                if (!e.getMessage().contains("No such upload id") && !e.getMessage().contains("Complete multi part fail")
                        && !e.getMessage().contains("The specified key version")) {
                    logger.error(e.getMessage() + " bucket: {} object: {} request id: {}",
                            request.getBucketName(), request.getObjectName(), requestId);
                }
            } else if (request.headers().contains("check-syncstamp")) {
                // 后台校验的headObject异常不打印
            } else {
                logger.error(e.getMessage() + " bucket: {} object: {} request id: {}",
                        request.getBucketName(), request.getObjectName(), requestId);
            }
            responseError(request, requestId, e.errCode, hasTrunk);
        } else if (exception instanceof SocketException) {
            logger.error("Connect time out ,please check connect. " + exception.getMessage());
            responseError(request, requestId, INTERNAL_SERVER_ERROR, hasTrunk);
        } else if (exception instanceof ApplicationErrorException) {
            String message = exception.getMessage();
            try {
                logger.error("application error exception. ");
                responseError(request, requestId, Integer.parseInt(message), hasTrunk);
            } catch (Exception e) {
                logger.error(message + e);
            }
        } else if (exception instanceof ClosedChannelException) {
            logger.error("closed channel {} {}", request.remoteAddress(), request.getUri());
            responseError(request, requestId, INTERNAL_SERVER_ERROR, hasTrunk);
        } else {
            logger.error(exception.getMessage(), exception);
            responseError(request, requestId, INTERNAL_SERVER_ERROR, hasTrunk);
        }
    }

    public static <T extends Throwable> void dealException(MsHttpRequest request, T exception) {
        dealException(getRequestId(), request, exception, false);
    }

    public static <T extends Throwable> void dealException(MsHttpRequest request, T exception, boolean hasTrunk) {
        dealException(getRequestId(), request, exception, hasTrunk);
    }
}
