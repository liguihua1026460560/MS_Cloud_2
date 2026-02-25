package com.macrosan.doubleActive.arbitration;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.VersionUtil;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.SingleSyncRecord;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.ec.ECUtils.updateRocksKey;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_PUT_SINGLE_SYNC_RECORD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.HAS_SS_RECORD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.UPDATE_SYNC_RECORD;
import static com.macrosan.httpserver.MossHttpClient.SYNC_RECORD_STORAGE_POOL;

@Log4j2
public class SingleSyncHandler {
    public static Mono<Integer> updateSingleSyncRecord(SingleSyncRecord record, List<Tuple3<String, String, String>> nodeList, MsHttpRequest r) {
        if (record == null) {
            return Mono.just(1);
        }

        record.setVersionNum(VersionUtil.getVersionNum());
        Map<String, String> oldVersionNum = new HashMap<>();
        for (Tuple3<String, String, String> tuple3 : nodeList) {
            oldVersionNum.put(tuple3.var1, record.versionNum);
        }
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(record.bucket);
        return updateRocksKey(storagePool, oldVersionNum, record.rocksKey(), Json.encode(record), UPDATE_SYNC_RECORD, ERROR_PUT_SINGLE_SYNC_RECORD, nodeList, r);
    }

    public static Mono<Boolean> hasSSRecord(int clusterIndex) {
        String bucketVnode = SYNC_RECORD_STORAGE_POOL.getBucketVnodeId("single_sync");
        MonoProcessor<Boolean> res = MonoProcessor.create();

        SYNC_RECORD_STORAGE_POOL.mapToNodeInfo(bucketVnode)
                .publishOn(SCAN_SCHEDULER)
                .subscribe(list -> {
                    List<SocketReqMsg> msgs = list.stream()
                            .map(t -> new SocketReqMsg("", 0)
                                    .put("lun", MSRocksDB.getSyncRecordLun(t.var2))
                                    .put("clusterIndex", String.valueOf(clusterIndex))
                            )
                            .collect(Collectors.toList());

                    ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, HAS_SS_RECORD, String.class, list);
                    responseInfo.responses.subscribe(s -> {
                    }, e -> log.error("", e), () -> {
                        if (responseInfo.errorNum > SYNC_RECORD_STORAGE_POOL.getM()) {
                            res.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "get sync record error !"));
                        } else if (responseInfo.successNum > 0) {
                            res.onNext(true);
                        } else {
                            // 返回not_found
                            log.debug("no ssrecord in {} cluster", clusterIndex);
                            res.onNext(false);
                        }
                    });
                });

        return res;
    }
}
