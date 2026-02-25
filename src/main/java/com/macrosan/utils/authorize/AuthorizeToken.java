package com.macrosan.utils.authorize;

import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.MsHttpRequest;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.SignatureException;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.ServerConstants.EXCLUDE_AUTH_RESOURCE;
import static com.macrosan.constants.ServerConstants.ID;
import static com.macrosan.constants.SysConstants.*;

/**
 * token的签名验证
 */
@Log4j2
public class AuthorizeToken {

    private static RedisConnPool pool = RedisConnPool.getInstance();

    private static IamRedisConnPool iamRedisConnPool = IamRedisConnPool.getInstance();

    static Mono<Boolean> getSignatureMono(MsHttpRequest request, String sign, String accessKey, String signatureInRequest) {
        if (EXCLUDE_AUTH_RESOURCE.contains(sign.hashCode())) {
            return Mono.just(true);
        }
        AtomicBoolean res = new AtomicBoolean();
        return pool.getReactive(REDIS_SYSINFO_INDEX)
                .get("swift_token")
                .defaultIfEmpty(SECRET)
                .map(secret -> JwtUtils.parseJwtToken(signatureInRequest, secret))
                .flatMap(claims -> {
                    String userName = claims.get("userName", String.class);
                    String passWord = claims.get("passWord", String.class);
                    if (!StringUtils.equalsIgnoreCase(userName, request.getUserName())) {
                        return Mono.just(false);
                    }
                    return pool.getReactive(REDIS_USERINFO_INDEX).hgetall(userName)
                            .doOnNext(userMap -> {
                                if (StringUtils.equals(passWord, userMap.get(USER_DATABASE_NAME_PASSWD))) {
                                    request.setUserId(userMap.getOrDefault(ID, ""));
                                    request.setIsSwift("true");
                                    res.set(true);
                                } else {
                                    log.error("Account does not exist or password is incorrect.account: {}",userName);
                                    res.set(false);
                                }
                            })
                            .map(b -> Mono.just(res.get()));
                })
                .doOnError(e -> {
                    if (e instanceof ExpiredJwtException) {
                        log.error("token has expired :" + e);
                    } else if (e instanceof SignatureException) {
                        log.error("invalid request token : " + e);
                    }
                })
                .onErrorReturn(Mono.just(false))
                .map(b -> res.get());
    }
}
