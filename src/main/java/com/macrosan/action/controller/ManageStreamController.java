package com.macrosan.action.controller;

import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.consturct.ErrMsgBuilder;
import com.macrosan.message.mqmessage.RequestMsg;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.Error;
import com.macrosan.message.xmlmsg.iam.IamErrorResponse;
import com.macrosan.utils.cache.LambdaCache;
import com.macrosan.utils.msutils.MsException;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.EventBus;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Map;
import java.util.function.Function;

import static com.macrosan.constants.AccountConstants.DEFAULT_USER_ID;
import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.utils.msutils.MsException.dealException;
import static reactor.adapter.rxjava.RxJava2Adapter.flowableToFlux;

/**
 * ManageStreamController
 * 担任控制器的角色，通过反射或者动态生成类调用方法
 * <p>
 * TODO 暂未处理缓存失效的情况
 *
 * @author liyixin
 * @date 2018/10/28
 */
public class ManageStreamController extends AbstractVerticle {

    private static final Logger logger = LogManager.getLogger(ManageStreamController.class.getName());

    private final LambdaCache cache = LambdaCache.getInstance();

    @Override
    public void start() {
        /* TODO 需要背压机制，压力过大需要及时拒绝 */
        EventBus eventBus = vertx.eventBus();

        /* 统一使用Flux的调度器，避免产生过多的线程 */
        flowableToFlux(eventBus.<RequestMsg>consumer(MANAGE_STREAM_ADDRESS).toFlowable())
                .parallel()
                .runOn(Schedulers.parallel())
                .subscribe(msg -> {
                    UnifiedMap<String, String> paramMap = msg.body().getParamMap();
                    ResponseMsg responseMsg;
                    try {
                        responseMsg = lambdaExecute(msg.body().getSignature(), paramMap);
                    } catch (MsException e) {
                        logger.error(e.getMessage() + " request id : " + paramMap.get(REQUESTID));
                        responseMsg = new ResponseMsg(e.getErrCode())
                                .setData(buildErrorMsg(e.getErrCode(), paramMap))
                                .setHttpCode(ErrMsgBuilder.getHttpCode(e.getErrCode()))
                                .addHeader(CONTENT_TYPE, "application/xml");
                        responseMsg = responseMsg.addHeader(CONTENT_LENGTH, String.valueOf(responseMsg.getData().length));
                    } catch (Exception e) {
                        /* 可能是方法没找到，也可能是方法中出现RuntimeException，例如Lettuce或Socket超时 */
                        logger.error("execute error", e);
                        responseMsg = new ResponseMsg(UNKNOWN_ERROR)
                                .setData(buildDefaultErrorMsg(paramMap))
                                .setHttpCode(INTERNAL_SERVER_ERROR)
                                .addHeader(CONTENT_TYPE, "application/xml");
                    }
                    responseMsg.addHeader(X_AMZ_REQUEST_ID, paramMap.get(REQUESTID));
                    msg.reply(responseMsg);
                });

        flowableToFlux(eventBus.<RequestMsg>consumer(IAM_MANAGE_STREAM_ADDRESS).toFlowable())
                .parallel()
                .runOn(Schedulers.parallel())
                .subscribe(msg -> {
                    UnifiedMap<String, String> paramMap = msg.body().getParamMap();
                    ResponseMsg responseMsg;
                    try {
                        responseMsg = lambdaExecute(msg.body().getSignature(), paramMap);
                    } catch (MsException e) {
                        logger.error(e.getMessage() + " request id : " + paramMap.get(REQUESTID));
                        responseMsg = new ResponseMsg(e.getErrCode())
                                .setData(buildIamErrorMsg(e.getErrCode(), paramMap, e.getMessage()))
                                .setHttpCode(ErrMsgBuilder.getHttpCode(e.getErrCode()))
                                .addHeader(CONTENT_TYPE, "application/xml");
                        responseMsg = responseMsg.addHeader(CONTENT_LENGTH, String.valueOf(responseMsg.getData().length));
                    } catch (Exception e) {
                        /* 可能是方法没找到，也可能是方法中出现RuntimeException，例如Lettuce或Socket超时 */
                        logger.error("execute error", e);
                        responseMsg = new ResponseMsg(UNKNOWN_ERROR)
                                .setData(buildIamErrorMsg(UNKNOWN_ERROR, paramMap, INTERNAL_ERROR_MSG))
                                .setHttpCode(INTERNAL_SERVER_ERROR)
                                .addHeader(CONTENT_TYPE, "application/xml");
                    }
                    responseMsg.addHeader(X_AMZ_REQUEST_ID, paramMap.get(REQUESTID));
                    msg.reply(responseMsg);
                });

        flowableToFlux(eventBus.<RequestMsg>consumer(STS_MANAGE_STREAM_ADDRESS).toFlowable())
                .parallel()
                .runOn(Schedulers.parallel())
                .subscribe(msg -> {
                    UnifiedMap<String, String> paramMap = msg.body().getParamMap();
                    // 异步执行任务，返回 Mono<ResponseMsg>
                    stsLambdaExecute(msg.body().getSignature(), paramMap)
                            .doOnError(MsException.class, e -> {
                                // 捕获 MsException 错误并生成相应的 ResponseMsg
                                logger.error(e.getMessage() + " request id : " + paramMap.get(REQUESTID));
                                ResponseMsg responseMsg = new ResponseMsg(e.getErrCode())
                                        .setData(buildIamErrorMsg(e.getErrCode(), paramMap, e.getMessage()))
                                        .setHttpCode(ErrMsgBuilder.getHttpCode(e.getErrCode()))
                                        .addHeader(CONTENT_TYPE, "application/xml");
                                responseMsg = responseMsg.addHeader(CONTENT_LENGTH, String.valueOf(responseMsg.getData().length));
                                msg.reply(responseMsg);
                            })
                            .doOnError(Exception.class, e -> {
                                // 捕获通用异常并生成响应
                                logger.error("execute error", e);
                                ResponseMsg responseMsg = new ResponseMsg(UNKNOWN_ERROR)
                                        .setData(buildIamErrorMsg(UNKNOWN_ERROR, paramMap, INTERNAL_ERROR_MSG))
                                        .setHttpCode(INTERNAL_SERVER_ERROR)
                                        .addHeader(CONTENT_TYPE, "application/xml");
                                msg.reply(responseMsg);
                            })
                            .subscribe(responseMsg -> {
                                responseMsg.addHeader(X_AMZ_REQUEST_ID, paramMap.get(REQUESTID));
                                msg.reply(responseMsg);
                            });

                });
    }

    private Object buildIamErrorMsg(int errorCode, Map<String, String> paramMap, String message) {
        String[] errorStr = ErrMsgBuilder.getErrStr(errorCode);

        IamErrorResponse.Error error = new IamErrorResponse.Error()
                .setCode(errorStr[0])
                .setMessage(message);

        return new IamErrorResponse()
                .setError(error)
                .setRequestId(paramMap.get(REQUESTID));
    }

    /**
     * 通过缓存的动态生成类调用方法
     *
     * @param sign    请求的签名
     * @param parmMap 传入方法的参数映射
     * @return 调用结果
     */
    private ResponseMsg lambdaExecute(int sign, UnifiedMap<String, String> parmMap) {
        Function<UnifiedMap<String, String>, ResponseMsg> lambda = cache.getManageLambda(sign);
        if (lambda != null) {
            return lambda.apply(parmMap);
        }
        throw new MsException(UNKNOWN_ERROR, "Class not found, the hash code of the sign is " + sign);
    }

    private Mono<ResponseMsg> stsLambdaExecute(int sign, UnifiedMap<String, String> parmMap) {
        Function<UnifiedMap<String, String>, Mono<ResponseMsg>> lambda = cache.getStsLambda(sign);
        if (lambda != null) {
            try {
                return lambda.apply(parmMap);
            } catch (MsException e) {
                logger.error(e);
                return Mono.error(new MsException(e.getErrCode(), e.getMessage()));
            } catch (Exception e) {
                logger.error(e);
                return Mono.error(new MsException(UNKNOWN_ERROR, e.getMessage()));
            }
        }
        return Mono.error(new MsException(UNKNOWN_ERROR, "Class not found, the hash code of the sign is " + sign));
    }

    private Object buildErrorMsg(int errorCode, Map<String, String> paramMap) {
        Error error = ErrMsgBuilder.build(
                SLASH +
                        paramMap.getOrDefault(BUCKET_NAME, "") + SLASH +
                        paramMap.getOrDefault(OBJECT_NAME, ""),
                ServerConfig.getInstance().getHostUuid(),
                paramMap.get(REQUESTID),
                errorCode);
        if (StringUtils.isNotBlank(paramMap.get(USER_ID))) {
            if (paramMap.get(USER_ID).equalsIgnoreCase(DEFAULT_USER_ID) && error.getCode().equals("AccessForbidden")) {
                error.setCode("AccessDenied");
            }
        }
        return error;
    }

    private Object buildDefaultErrorMsg(Map<String, String> paramMap) {
        return ErrMsgBuilder.build(
                SLASH +
                        paramMap.getOrDefault(BUCKET_NAME, "") + SLASH +
                        paramMap.getOrDefault(OBJECT_NAME, ""),
                ServerConfig.getInstance().getHostUuid(),
                paramMap.get(REQUESTID));
    }
}
