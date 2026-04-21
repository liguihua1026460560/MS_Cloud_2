package com.macrosan.ec.rebuild;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple2;
import lombok.Getter;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.CHECK_FILE_EXISTS;

/**
 * @author zhaoyang
 * @date 2026/04/17
 **/
public class FileExistChecker {

    @Getter
    private static FileExistChecker instance = new FileExistChecker();

    public Mono<Boolean> checkFileNotExists(StoragePool dataPool, String fileName) {
        return dataPool.mapToNodeInfo(dataPool.getObjectVnodeId(fileName))
                .flatMap(nodeList -> {
                    List<SocketReqMsg> msgs = nodeList.stream()
                            .map(tuple3 -> new SocketReqMsg("", 0)
                                    .put("fileName", fileName)
                                    .put("lun", tuple3.var2)
                            )
                            .collect(Collectors.toList());

                    ClientTemplate.ResponseInfo<Boolean> responseInfo = ClientTemplate.oneResponse(msgs, CHECK_FILE_EXISTS, Boolean.class, nodeList);
                    return Mono.create(sink -> {
                        responseInfo.responses
                                .doOnComplete(() -> {
                                    if (responseInfo.successNum != dataPool.getK() + dataPool.getM()) {
                                        sink.success(false);
                                    } else {
                                        boolean notExists = true;
                                        for (Tuple2<ErasureServer.PayloadMetaType, Boolean> result : responseInfo.res) {
                                            if (result.var2()) {
                                                notExists = false;
                                                break;
                                            }
                                        }
                                        sink.success(notExists);
                                    }
                                })
                                .doOnError(sink::error)
                                .subscribe();
                    });
                });
    }

}
