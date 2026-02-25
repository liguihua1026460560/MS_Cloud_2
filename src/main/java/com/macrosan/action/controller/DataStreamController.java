package com.macrosan.action.controller;

import com.macrosan.constants.ErrorNo;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.utils.cache.LambdaCache;

import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.utils.msutils.MsException.dealException;

/**
 * DataStreamController
 *
 * @author liyixin
 * @date 2018/12/20
 */
public class DataStreamController {

    private static final LambdaCache CACHE = LambdaCache.getInstance();

    private DataStreamController() {
    }

    public static int dataStreamRoute(MsHttpRequest request, int sign) {
        try {
            return CACHE.getDataLambda(sign).apply(request);
        } catch (Exception e) {
            dealException(getRequestId(), request, e, false);
        }
        return ErrorNo.SUCCESS_STATUS;
    }
}
