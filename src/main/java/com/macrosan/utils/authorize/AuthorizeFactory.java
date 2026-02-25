package com.macrosan.utils.authorize;

import com.macrosan.httpserver.MsHttpRequest;
import io.reactivex.functions.Function4;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;

/**
 * AuthorizeFactory
 *
 * @author liyixin
 * @date 2019/1/16
 */
public class AuthorizeFactory {

    private static final Logger logger = LogManager.getLogger(AuthorizeFactory.class.getName());

    public static final String SHA256 = "AWS4-HMAC-SHA256";

    public static final String AWS = "AWS";

    public static final String TOKEN = "token";

    private AuthorizeFactory() {
    }

    /**
     * 获取鉴权函数式接口的工厂方法 Project Reactor版本
     *
     * @param authType 鉴权类型 ：
     *                 <p>
     *                 AWS -> V2鉴权
     *                 </p>
     *                 <p>
     *                 SHA256 -> V4鉴权
     *                 </p>
     * @return 用于鉴权的方法
     */
    public static Function4<MsHttpRequest, String, String, String, Mono<Boolean>> getAuthorizer(String authType) {
        if (StringUtils.isBlank(authType)) {
            return EmptyAuthorize::getSignatureMono;
        }

        switch (authType) {
            case SHA256:
                return AuthorizeV4::getSignatureMono;
            case AWS:
                return AuthorizeV2::getSignatureMono;
            case TOKEN:
                return AuthorizeToken::getSignatureMono;
            default:
                logger.info("The auth type is " + authType);
                return EmptyAuthorize::getSignatureMono;
        }
    }

}
