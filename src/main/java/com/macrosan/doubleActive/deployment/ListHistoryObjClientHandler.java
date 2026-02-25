package com.macrosan.doubleActive.deployment;

import com.macrosan.constants.ErrorNo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.LifecycleClientHandler;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;

import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;

/**
 * @author chengyinfeng
 */
@Log4j2
public class ListHistoryObjClientHandler extends LifecycleClientHandler {

    Integer clusterIndex;

    public ListHistoryObjClientHandler(ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo, String vnode,
                                       SocketReqMsg msg, Integer clusterIndex, String bucketName) {
        super(null, responseInfo, vnode, msg, bucketName);
        this.clusterIndex = clusterIndex;
        msg.put("retryTimes", "0");
    }

    @Override
    public void completeResponse() {
        try {
            if (responseInfo.successNum < storagePool.getK()) {
                res.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "get history data error !"));
                return;
            }
            res.onNext(result);
        } catch (Exception e) {
            log.error(e);
        }
    }
}
