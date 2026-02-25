package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.socketmsg.StringResMsg;
import com.macrosan.utils.msutils.MsException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.CONTENT_LENGTH;
import static com.macrosan.constants.SysConstants.MSG_TYPE_POST_BACKUP_INFO;

/**
 * 归档所用的api
 *
 * @author shilixiang
 */
public class BackUpService extends BaseService {

    private static Logger logger = LogManager.getLogger(BackUpService.class.getName());

    private static BackUpService instance = null;

    private BackUpService() {
        super();
    }

    /**
     * 每一个Service都必须提供一个getInstance方法
     */
    public static BackUpService getInstance() {
        if (instance == null) {
            instance = new BackUpService();
        }
        return instance;
    }

    /**
     * 判断备份的阶段发送一个socket请求给
     *
     * @param paramMap 请求参数
     * @return 是否接收到socket消息
     */
    public ResponseMsg postBackupInfo(UnifiedMap<String, String> paramMap) {
        logger.info("body: " + paramMap.get("body"));
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_POST_BACKUP_INFO, 0)
                .put("param", paramMap.get("body"));
        StringResMsg resMsg = sender.sendAndGetResponse(msg, StringResMsg.class, false);
        logger.info("resMsg: " + resMsg);
        int resMsgData = resMsg.getCode();
        if (resMsgData != ErrorNo.SUCCESS_STATUS) {
            throw new MsException(UNKNOWN_ERROR, "Backup process stop with internal error.");
        }
        String data = resMsg.getData();
        if (data != null) {
            return new ResponseMsg(ErrorNo.SUCCESS_STATUS, data.getBytes());
        } else {
            return new ResponseMsg(ErrorNo.SUCCESS_STATUS, new byte[0]).addHeader(CONTENT_LENGTH, "0");
        }
    }

}
