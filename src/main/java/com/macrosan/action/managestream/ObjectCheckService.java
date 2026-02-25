package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.macrosan.constants.AccountConstants.DEFAULT_ACCESS_KEY;
import static com.macrosan.constants.AccountConstants.DEFAULT_MGT_USER_ID;
import static com.macrosan.constants.ErrorNo.SUCCESS_STATUS;
import static com.macrosan.constants.ServerConstants.BODY;
import static com.macrosan.constants.ServerConstants.USER_ID;


/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/12/4 10:24
 */

@Log4j2
public class ObjectCheckService extends BaseService {
    private static ObjectCheckService instance;

    private ObjectCheckService(){super();}

    public static ObjectCheckService getInstance() {
        if (instance == null) {
            instance = new ObjectCheckService();
        }
        return instance;
    }

    public ResponseMsg putObjectCheck(UnifiedMap<String, String> paramMap){
        String body = paramMap.get(BODY);
        String userId = paramMap.get(USER_ID);
        if (!DEFAULT_MGT_USER_ID.equals(userId)){
            throw new MsException(ErrorNo.ACCESS_DENY,"You are not authorized to access this resource.");
        }
        if (StringUtils.isEmpty(body)){
            throw new MsException(ErrorNo.MALFORMED_JSON,"The Json you provided was not wellformed or did not validate against our published schema.");
        }

        JsonObject setting = new JsonObject(body);
        JsonObject property = setting.getJsonObject("pool");
        JsonObject time = property.getJsonObject("property");
        String startTime = time.getString("scrub_begin");
        String endTime = time.getString("scrub_end");

        if (StringUtils.isNotEmpty(startTime) && StringUtils.isNotEmpty(endTime)){
            if (checkTimeFormat(startTime) && checkTimeFormat(endTime)){
                pool.getShortMasterCommand(2).hset("objectTime","startTime",startTime);
                pool.getShortMasterCommand(2).hset("objectTime","endTime",endTime);
                return new ResponseMsg(SUCCESS_STATUS);
            }else {
                throw new MsException(ErrorNo.TIME_FORMAT_ERROR,"time format is error");
            }
        }else {
            throw new MsException(ErrorNo.THE_SETTING_ERROR,"body value is null");
        }
    }

    public ResponseMsg getObjectCheck(UnifiedMap<String, String> paramMap){
        String userId = paramMap.get(USER_ID);
        if (!DEFAULT_MGT_USER_ID.equals(userId)){
            throw new MsException(ErrorNo.ACCESS_DENY,"You are not authorized to access this resource.");
        }
        JsonObject setting = new JsonObject();
        JsonObject property = new JsonObject();
        JsonObject time = new JsonObject();

        String startTime = pool.getCommand(2).hget("objectTime","startTime");
        String endTime = pool.getCommand(2).hget("objectTime","endTime");
        if (StringUtils.isNotEmpty(startTime) && StringUtils.isNotEmpty(endTime)){
            time.put("scrub_begin",startTime);
            time.put("scrub_end",endTime);
            property.put("property",time);
            setting.put("pool",property);
            return new ResponseMsg(SUCCESS_STATUS).setData(setting.toString());
        }else {
            return new ResponseMsg(SUCCESS_STATUS);
        }
    }

    public ResponseMsg deleteObjectCheck(UnifiedMap<String, String> paramMap){
        pool.getShortMasterCommand(2).hdel("objectTime","startTime");
        pool.getShortMasterCommand(2).hdel("objectTime","endTime");
        return new ResponseMsg(SUCCESS_STATUS);
    }

    private boolean checkTimeFormat(String time){
        Pattern pattern = Pattern.compile("^([01]?[0-9]|2[0-3]):[0-5][0-9]$");
        Matcher matcher  = pattern.matcher(time);
        return matcher.matches();
    }
}
