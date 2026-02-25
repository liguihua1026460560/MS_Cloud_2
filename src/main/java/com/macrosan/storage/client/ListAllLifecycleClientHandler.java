package com.macrosan.storage.client;

import com.macrosan.constants.ErrorNo;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;

/**
 * @author chengyinfeng
 */
@Log4j2
public class ListAllLifecycleClientHandler extends LifecycleClientHandler {

    public ListAllLifecycleClientHandler(ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo, String vnode,
                                         SocketReqMsg msg, String bucketName) {
        super(null, responseInfo, vnode, msg, bucketName);
    }

    @Override
    public void completeResponse() {
        try {
            if (responseInfo.successNum < storagePool.getK()) {
                res.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "get lifecycle data error !"));
                return;
            }
            res.onNext(result);
        } catch (Exception e) {
            log.error(e);
        }
    }
}
