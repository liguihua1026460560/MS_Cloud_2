package com.macrosan.utils.authorize;

import com.macrosan.httpserver.MsHttpRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;

import static com.macrosan.constants.AccountConstants.DEFAULT_USER_ID;

/**
 * EmptyAuthorize
 *
 * @author liyixin
 * @date 2019/1/16
 */
class EmptyAuthorize {

    private static final Logger logger = LogManager.getLogger(EmptyAuthorize.class.getName());

    private EmptyAuthorize() {
    }

    static Mono<Boolean> getSignatureMono(MsHttpRequest request, String sign, String accessKey, String signatureInRequest) {
        request.setUserId(DEFAULT_USER_ID).setAccessKey(accessKey);
        return Mono.just(true);
    }
}
