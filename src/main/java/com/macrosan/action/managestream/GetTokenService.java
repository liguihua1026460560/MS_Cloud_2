package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.utils.authorize.JwtUtils;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.HashMap;
import java.util.Map;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;


@Log4j2
public class GetTokenService extends BaseService {

    private static RedisConnPool pool = RedisConnPool.getInstance();

    private static GetTokenService instance = null;

    private GetTokenService() {
        super();
    }

    public static GetTokenService getInstance() {
        if (instance == null) {
            instance = new GetTokenService();
        }
        return instance;
    }

    /**
     * 根据user_name和password生成token
     *
     * @param paramMap
     * @return
     */
    public ResponseMsg getToken(UnifiedMap<String, String> paramMap) {
        try {
            String userName = "";
            String passWord = "";
            String expireTime = "";
            if (StringUtils.isNotEmpty(paramMap.get("x-auth-user"))) {
                userName = paramMap.get("x-auth-user");
                passWord = paramMap.get("x-auth-key");
            } else {
                JsonObject userJson = new JsonObject(paramMap.get("body"))
                        .getJsonObject("auth")
                        .getJsonObject("identity")
                        .getJsonObject("password")
                        .getJsonObject("user");
                userName = userJson.getString("name");
                passWord = userJson.getString("password");
                String accountName = userJson.getJsonObject("domain").getString("name");
                if (!StringUtils.equalsIgnoreCase(userName, accountName)) {
                    return new ResponseMsg(ErrorNo.SUCCESS_STATUS).setHttpCode(UNAUTHORIZED);
                }
                accountName = new JsonObject(paramMap.get("body"))
                        .getJsonObject("auth")
                        .getJsonObject("scope")
                        .getJsonObject("domain")
                        .getString("name");
                if (!StringUtils.equalsIgnoreCase(userName, accountName)) {
                    return new ResponseMsg(ErrorNo.SUCCESS_STATUS).setHttpCode(UNAUTHORIZED);
                }
                expireTime = new JsonObject(paramMap.get("body"))
                        .getJsonObject("auth")
                        .getJsonObject("identity")
                        .getString("expiretime");
            }
            if (StringUtils.isBlank(expireTime) && StringUtils.isNotEmpty(expireTime)) {
                return new ResponseMsg(ErrorNo.SUCCESS_STATUS).setHttpCode(UNAUTHORIZED);
            }
            long ttl = StringUtils.isBlank(expireTime) ? TTL_MILLIS : Long.parseLong(expireTime);
            if (ttl <= 0 || ttl > 604800000) {
                return new ResponseMsg(ErrorNo.SUCCESS_STATUS).setHttpCode(UNAUTHORIZED);
            }
            Map<String, Object> payload = new HashMap<>();
            payload.put("userName", userName);
            payload.put("passWord", passWord);
            Map<String, String> userMap = pool.getCommand(REDIS_USERINFO_INDEX).hgetall(userName);
            if (userMap == null) {
                return new ResponseMsg(ErrorNo.SUCCESS_STATUS).setHttpCode(UNAUTHORIZED);
            }
            boolean ret = StringUtils.equals(passWord, userMap.get(USER_DATABASE_NAME_PASSWD));
            if (!ret) {
                return new ResponseMsg(ErrorNo.SUCCESS_STATUS).setHttpCode(UNAUTHORIZED);
            }
            String secret = StringUtils.isBlank(pool.getCommand(REDIS_SYSINFO_INDEX).get("swift_token")) ?
                    SECRET : pool.getCommand(REDIS_SYSINFO_INDEX).get("swift_token");
            String token = JwtUtils.generateJwtToken(payload, ttl, secret, "subject");
            if (StringUtils.isNotEmpty(paramMap.get("x-auth-user"))) {
                return new ResponseMsg(ErrorNo.SUCCESS_STATUS).setHttpCode(CREATED).addHeader(X_AUTH_TOKEN, token);
            }
            return new ResponseMsg(ErrorNo.SUCCESS_STATUS).setHttpCode(CREATED).addHeader(X_SUBJECT_TOKEN, token);
        } catch (NumberFormatException e) {
            log.error("The expiration date is illegal.");
            return new ResponseMsg(ErrorNo.SUCCESS_STATUS).setHttpCode(UNAUTHORIZED);
        } catch (DecodeException e) {
            log.error("User information is illegal.");
            return new ResponseMsg(ErrorNo.SUCCESS_STATUS).setHttpCode(UNAUTHORIZED);
        } catch (Exception e) {
            return new ResponseMsg(ErrorNo.SUCCESS_STATUS).setHttpCode(UNAUTHORIZED);
        }
    }
}
