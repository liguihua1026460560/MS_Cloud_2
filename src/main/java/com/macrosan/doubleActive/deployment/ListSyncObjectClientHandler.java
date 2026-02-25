package com.macrosan.doubleActive.deployment;

import com.macrosan.constants.ErrorNo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.LifecycleClientHandler;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;

import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;

public class ListSyncObjectClientHandler extends LifecycleClientHandler {
    public static final Logger logger = LogManager.getLogger(ListSyncObjectClientHandler.class.getName());

    public ListSyncObjectClientHandler(ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo, String vnode,
                                       SocketReqMsg msg, String bucketName) {
        super(null, responseInfo, vnode, msg, bucketName);
        msg.put("retryTimes", "0");
    }

    @Override
    public void completeResponse() {
        try {
            if (responseInfo.successNum < storagePool.getK()) {
                res.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "get bucket sync data error !"));
                return;
            }
            res.onNext(result);
        } catch (Exception e) {
            logger.error(e);
        }
    }
}
