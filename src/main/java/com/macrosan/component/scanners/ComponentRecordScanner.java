package com.macrosan.component.scanners;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ListMultiMediaTaskHandler;
import com.macrosan.utils.ModuleDebug;
import com.macrosan.utils.functional.Tuple2;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.stream.Collectors;

import static com.macrosan.component.ComponentStarter.COMP_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_COMPONENT_RECORD;

/**
 * record 扫描器
 *
 * @author zhaoyang
 * @date 2026/04/15
 **/
@Log4j2
public class ComponentRecordScanner {

    private static ComponentRecordScanner instance;

    public static ComponentRecordScanner getInstance() {
        if (instance == null) {
            instance = new ComponentRecordScanner();

        }
        return instance;
    }

    public Flux<ComponentRecord> scanRecord(ComponentRecord.Type type, String bucketName, String taskMarker) {
        if (ModuleDebug.mediaComponentDebug()) {
            log.info("start scan dicom record, bucket:{} marker:{}", bucketName, taskMarker);
        }
        StoragePool metaPool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String bucketVnode = metaPool.getBucketVnodeId(bucketName);
        TypeReference<Tuple2<String, ComponentRecord>[]> reference = new TypeReference<Tuple2<String, ComponentRecord>[]>() {
        };
        return metaPool.mapToNodeInfo(bucketVnode)
                .flatMap(nodeList -> {
                    List<SocketReqMsg> msgs = nodeList.stream().map(info ->
                            new SocketReqMsg("", 0)
                                    .put("bucket", bucketName)
                                    .put("maxKeys", String.valueOf(ListMultiMediaTaskHandler.MAX_KEY))
                                    .put("type", type.name())
                                    .put("marker", "")
                                    .put("lun", MSRocksDB.getComponentRecordLun(info.var2))
                                    .put("taskMarker", taskMarker)
                    ).collect(Collectors.toList());


                    ClientTemplate.ResponseInfo<Tuple2<String, ComponentRecord>[]> responseInfo =
                            ClientTemplate.oneResponse(msgs, LIST_COMPONENT_RECORD, reference, nodeList);
                    ListMultiMediaTaskHandler clientHandler = new ListMultiMediaTaskHandler(responseInfo, nodeList, bucketName);
                    responseInfo.responses.publishOn(COMP_SCHEDULER)
                            .subscribe(clientHandler::handleResponse, log::error, clientHandler::handleComplete);
                    return clientHandler.listProcessor;
                })
                .flatMapIterable(b -> b);
    }
}
