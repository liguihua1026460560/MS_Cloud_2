package com.macrosan.message.consturct;

import com.macrosan.action.controller.ManageStreamController;
import com.macrosan.message.xmlmsg.Error;
import com.macrosan.utils.property.PropertyReader;
import io.netty.util.collection.IntObjectHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.macrosan.constants.ServerConstants.*;

/**
 * ErrMsgBuilder
 * <p>
 * 构建错误XML
 *
 * @author liyixin
 * @date 2018/12/14
 */
public class ErrMsgBuilder {

    private static String[] defaultErrMap = {INTERNAL_ERROR_CODE, INTERNAL_ERROR_MSG, "500"};

    private static final Logger logger = LogManager.getLogger(ErrMsgBuilder.class.getName());

    private ErrMsgBuilder() {
    }

    private static final IntObjectHashMap<String[]> ERR_MAP = new PropertyReader(ERROR_MAP).getPropertyAsMultiMap();

    public static String[] getErrStr(int errCode) {
        return ERR_MAP.getOrDefault(errCode, defaultErrMap);
    }

    public static Error build(String resName, String hostId, String requestId, int errCode) {
        return new Error()
                .setResource(resName)
                .setRequestId(requestId)
                .setHostId(hostId)
                .setCode(ERR_MAP.getOrDefault(errCode, defaultErrMap)[0])
                .setMessage(ERR_MAP.getOrDefault(errCode, defaultErrMap)[1]);
    }

    public static int getHttpCode(int errCode) {
        return Integer.valueOf(ERR_MAP.getOrDefault(errCode, defaultErrMap)[2]);
    }

    public static Error build(String resName, String hostId, String requestId) {
        return new Error()
                .setResource(resName)
                .setRequestId(requestId)
                .setHostId(hostId)
                .setCode(defaultErrMap[0])
                .setMessage(defaultErrMap[1]);
    }

    public static Error build(String resName, int errCode) {
        return new Error()
                .setKey(resName)
                .setCode(ERR_MAP.getOrDefault(errCode, defaultErrMap)[0])
                .setMessage(ERR_MAP.getOrDefault(errCode, defaultErrMap)[1]);
    }

    public static Error build(String resName, String versionId, int errCode) {
        return new Error()
                .setKey(resName)
                .setVersionId(versionId)
                .setCode(ERR_MAP.getOrDefault(errCode, defaultErrMap)[0])
                .setMessage(ERR_MAP.getOrDefault(errCode, defaultErrMap)[1]);
    }
}
