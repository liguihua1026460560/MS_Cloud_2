package com.macrosan.database.rocksdb;

import com.macrosan.ec.server.LocalMigrateServer;
import com.macrosan.ec.server.MigrateServer;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.client.RSocketClient;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;

import java.time.Duration;

import static com.macrosan.constants.SysConstants.ROCKS_FILE_META_PREFIX;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.DELETE_FILE;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.MIGRATE_PUT_ROCKS;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;

@Log4j2
public class MigrateRocksDB extends MSRocksDB {
    private String lun;

    MigrateRocksDB(TransactionDB rocksDB, String lun) {
        super(rocksDB);
        this.lun = lun;
    }

    MigrateRocksDB(RocksDB rocksDB, String lun, boolean check) {
        super(rocksDB, check);
        this.lun = lun;
    }

    @Override
    public void delete(byte[] key) throws RocksDBException {
        String keyStr = new String(key);
        String vnode = LocalMigrateServer.getVnode(keyStr);
        MigrateServer.DstLunInfo dstLunInfo = MigrateServer.getInstance().getDstLunInfo(lun, vnode);
        if (null != dstLunInfo) {
            if (keyStr.startsWith(ROCKS_FILE_META_PREFIX)) {
                String fileName = keyStr.substring(ROCKS_FILE_META_PREFIX.length());
                SocketReqMsg msg = new SocketReqMsg("", 0)
                        .put("lun", dstLunInfo.lun)
                        .put("fileName", Json.encode(new String[]{fileName}));

                RSocketClient.getRSocket(dstLunInfo.ip, BACK_END_PORT)
                        .flatMapMany(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), DELETE_FILE.name())))
                        .timeout(Duration.ofSeconds(30))
                        .doOnError(e -> log.debug("", e))
                        .subscribe(p -> p.release());
            } else {
                //只有删除对象的FileMeta不使用事务直接删除，其他删除使用事务
                throw new UnsupportedOperationException("");
            }
        }

        super.delete(key);
    }

    @Override
    public void put(final byte[] key, final byte[] value)
            throws RocksDBException {
        super.put(key, value);
        String keyStr = new String(key);
        String vnode = LocalMigrateServer.getVnode(keyStr);
        MigrateServer.DstLunInfo dstLunInfo = MigrateServer.getInstance().getDstLunInfo(lun, vnode);
        if (null != dstLunInfo) {
            SocketReqMsg msg = new SocketReqMsg("", 0)
                    .put("lun", dstLunInfo.lun)
                    .put("key", new String(key))
                    .put("value", new String(value));

            RSocketClient.getRSocket(dstLunInfo.ip, BACK_END_PORT)
                    .flatMapMany(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), MIGRATE_PUT_ROCKS.name())))
                    .timeout(Duration.ofSeconds(30))
                    .doOnError(e -> log.debug("", e))
                    .subscribe(p -> p.release());
        }
    }
}
