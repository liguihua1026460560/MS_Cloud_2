package com.macrosan.component.scanners;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ListAllLifecycleClientHandler;
import com.macrosan.utils.ModuleDebug;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.stream.Collectors;

import static com.macrosan.component.ComponentStarter.COMP_SCHEDULER;
import static com.macrosan.constants.SysConstants.ROCKS_LIFE_CYCLE_PREFIX;
import static com.macrosan.ec.Utils.getLifeCycleStamp;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_LIFE_OBJECT;

/**
 * 元数据扫描器
 * @author zhaoyang
 * @date 2026/04/13
 **/
@Log4j2
public class MetaDataScanner {

    private static MetaDataScanner instance;

    public static MetaDataScanner getInstance() {
        if (instance == null) {
            instance = new MetaDataScanner();

        }
        return instance;
    }

    public Mono<List<Tuple2<String, MetaData>>> scanMeta(String bucketName, String scanMark, int scanBatchSize) {
        if (ModuleDebug.mediaComponentDebug()) {
            log.info("start scanMeta bucket:{} scanMark:{} batchSize:{}", bucketName, scanMark, scanBatchSize);
        }
        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        List<String> bucketVnodeList = metaStoragePool.getBucketVnodeList(bucketName);
        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String stamp = "0";

        SocketReqMsg reqMsg = new SocketReqMsg("", 0);
        reqMsg.put("bucket", bucketName)
                .put("maxKeys", String.valueOf(scanBatchSize))
                .put("beginPrefix", scanMark);
        return Mono.just(bucketVnodeList.toArray(new String[0]))
                .flatMapMany(Flux::fromArray)
                .flatMap(bucketVnode -> {
                    String keySuffix = reqMsg.dataMap.getOrDefault("beginPrefix", "");
                    String curBeginPrefix = StringUtils.isNotEmpty(keySuffix) ? ROCKS_LIFE_CYCLE_PREFIX + bucketVnode + keySuffix
                            : getLifeCycleStamp(bucketVnode, bucketName, stamp);
                    return metaPool.mapToNodeInfo(bucketVnode)
                            .publishOn(COMP_SCHEDULER)
                            .flatMap(infoList -> {
                                String[] nodeArr = infoList.stream().map(info -> info.var3).toArray(String[]::new);
                                List<SocketReqMsg> msgs = infoList.stream().map(info -> reqMsg.copy().put("lun", info.var2)
                                        .put("beginPrefix", curBeginPrefix).put("vnode", nodeArr[0])).collect(Collectors.toList());

                                ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo = ClientTemplate.oneResponse(msgs, LIST_LIFE_OBJECT, new TypeReference<Tuple3<Boolean,
                                        String, MetaData>[]>() {
                                }, infoList);
                                ListAllLifecycleClientHandler clientHandler = new ListAllLifecycleClientHandler(responseInfo, nodeArr[0], reqMsg, bucketName);
                                responseInfo.responses.publishOn(COMP_SCHEDULER).subscribe(clientHandler::handleResponse, e -> log.error("", e),
                                        clientHandler::completeResponse);
                                return clientHandler.res;
                            });
                })
                .collectList()
                .map(ComponentScanner::getMetaList);

    }

}
