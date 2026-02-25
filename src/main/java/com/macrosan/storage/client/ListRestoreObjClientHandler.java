package com.macrosan.storage.client;/**
 * @author niechengxing
 * @create 2023-06-26 16:13
 */

import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.UnicastProcessor;

import static com.macrosan.ec.Utils.getLifeCycleMetaKey;

/**
 *@program: MS_Cloud
 *@description:
 *@author: niechengxing
 *@create: 2023-06-26 16:13
 */
@Log4j2
public class ListRestoreObjClientHandler extends LifecycleClientHandler {
    public ListRestoreObjClientHandler(StoragePool storagePool, UnicastProcessor<SocketReqMsg> msgUnicastProcessor,
                                       ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo, String vnode, SocketReqMsg msg) {
        super(storagePool, msgUnicastProcessor, responseInfo, vnode, msg);
    }

    @Override
    public void completeResponse() {
        try {
            if (responseInfo.successNum == 0 && (storagePool.getK() + storagePool.getM()) > 1) {
                log.error("Restore get object list error!");
                msgUnicastProcessor.onComplete();
                return;
            } else if (responseInfo.successNum < storagePool.getK()) {
                int retryTimes = Integer.parseInt(msg.get("retryTimes"));
                retryTimes++;
                msg.put("retryTimes", String.valueOf(retryTimes));
                if (retryTimes > 10) {
                    msgUnicastProcessor.onComplete();
                } else {
                    log.info("Restore get object list error, retry " + retryTimes + "th !");
                    msgUnicastProcessor.onNext(msg);
                }
                return;
            }

            msg.put("retryTimes", "0");
            if (result.size() > 1000) {
                MetaData metaData = result.get(1000).getMetaData();
                String beginPrefix = getLifeCycleMetaKey(vnode, metaData.getBucket(), metaData.key, metaData.versionId, metaData.stamp);
                msg.put("beginPrefix", beginPrefix);
                result.remove(result.size() - 1);
                res.onNext(result);
                msgUnicastProcessor.onNext(msg);
            } else {
                res.onNext(result);
                log.info("list bucket vnode object complete!!!");
                msgUnicastProcessor.onComplete();
            }
        } catch (Exception e) {
            log.error(e);
        }
    }
}

