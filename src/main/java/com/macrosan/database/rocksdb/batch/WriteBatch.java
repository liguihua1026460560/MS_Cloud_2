package com.macrosan.database.rocksdb.batch;

import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.Utils;
import com.macrosan.ec.migrate.Migrate;
import com.macrosan.ec.server.ErasureServer.PayloadMetaType;
import com.macrosan.ec.server.LocalMigrateServer;
import com.macrosan.ec.server.MigrateServer;
import com.macrosan.fs.Allocator.Result;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.utils.functional.Tuple2;
import io.netty.buffer.ByteBuf;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.SysConstants.ROCKS_OBJ_META_DELETE_MARKER;
import static com.macrosan.database.rocksdb.MSRocksDB.READ_OPTIONS;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.MIGRATE_MERGE_ROCKS;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.MIGRATE_PUT_ROCKS;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;

@Log4j2
public class WriteBatch {
    static MigrateServer migrate = MigrateServer.getInstance();
    static LocalMigrateServer localMigrateServer = LocalMigrateServer.getInstance();

    private static boolean isNeedMigrate(byte[] key) {
        int b = key[0] & 0xff;
        return (b >= '0' && b <= '9') || b == '!' || b == '@' || b == '*' || b == '+' || b == '-' || b == '|'
                || b == ':' || b == '(' || b == ')' || b == '<' || b == '=' || b == '?' || b == '%' || b == '>';
    }


    WriteBatchWithIndex w;
    RocksDB db;
    String lun;
    Scheduler scheduler;
    BatchWriter batchWriter;

    public WriteBatch(RocksDB db, String lun, Scheduler scheduler) {
        this.db = db;
        this.lun = lun;
        this.scheduler = scheduler;
        batchWriter = new BatchWriter(lun);
        w = new WriteBatchWithIndex(true);
    }

    public byte[] getFromBatchAndDB(final RocksDB db, final byte[] key) throws RocksDBException {
        return getFromBatchAndDB(db, READ_OPTIONS, key);
    }

    public byte[] getFromBatchAndDB(final RocksDB db, final ReadOptions options,
                                    final byte[] key) throws RocksDBException {
        if (key[0] >= '0' && key[0] <= '9') {
            byte[] res = w.getFromBatchAndDB(db, options, key);
            if (null == res) {
                byte[] deleteMarker = Utils.getDeleteMarkKey(key);
                res = w.getFromBatchAndDB(db, options, deleteMarker);
            }

            return res;
        }

        return w.getFromBatchAndDB(db, options, key);
    }

    public void delete(byte[] key) throws RocksDBException {
        if (key[0] >= '0' && key[0] <= '9') {
            byte[] deleteMarker = Utils.getDeleteMarkKey(key);
            delete(deleteMarker);
        }

        if (isNeedMigrate(key) && migrate.needWriteToMigrateColumnFamily(lun, key)) {
            byte[] beforeDelete = w.getFromBatchAndDB(db, READ_OPTIONS, key);
            if (beforeDelete != null) {
                byte[] markerValue = new byte[1 + beforeDelete.length];
                //在value前加~标记是删除
                markerValue[0] = ROCKS_OBJ_META_DELETE_MARKER.getBytes()[0];
                System.arraycopy(beforeDelete, 0, markerValue, 1, beforeDelete.length);
                w.put(MSRocksDB.getMigrateColumnFamilyHandle(lun), key, markerValue);
            }
        }

        if (migrate.start > 0 && isNeedMigrate(key)) {
            try {
                String keyStr = new String(key);
                String vnode = LocalMigrateServer.getVnode(keyStr);
                MigrateServer.DstLunInfo dstLunInfo = migrate.getDstLunInfo(lun, vnode);
                if (null != dstLunInfo) {
                    byte[] value = w.getFromBatchAndDB(db, READ_OPTIONS, key);
                    if (null != value) {
                        dstLunInfo.addDelete(new Tuple2<>(key, value));
                    }
                }
            } catch (Exception e) {

            }
        }

        if (localMigrateServer.start > 0 && isNeedMigrate(key)) {
            try {
                String keyStr = new String(key);
                String vnode = LocalMigrateServer.getVnode(keyStr);
                String dstLun = localMigrateServer.getDstLun(lun, vnode);
                if (null != dstLun) {
                    byte[] value = w.getFromBatchAndDB(db, READ_OPTIONS, key);
                    if (null != value) {
                        localMigrateServer.addDelete(lun, vnode, new Tuple2<>(key, value));
                    }
                }
            } catch (Exception e) {

            }
        }

        w.delete(key);
    }

    public void transferAndPut(byte[] key, byte[] value) throws RocksDBException {
        delete(key);
        byte[] bytes = ROCKS_OBJ_META_DELETE_MARKER.getBytes();
        byte[] deleteMarker = new byte[key.length];
        System.arraycopy(bytes, 0, deleteMarker, 0, bytes.length);
        System.arraycopy(key, 1, deleteMarker, bytes.length, key.length - 1);
        w.put(deleteMarker, value);
    }

    /**
     * @param cutting 是否去除多余的元数据
     * @throws RocksDBException
     */
    public void put(boolean cutting, byte[] key, byte[] value) throws RocksDBException {
        if (cutting) {
            put(key, Utils.simplifyMetaJson(value));
        } else {
            put(key, value);
        }
    }

    private static void rewrite(String ip, SocketReqMsg msg, PayloadMetaType type, int retry) {
        RSocketClient.getRSocket(ip, BACK_END_PORT)
                .flatMapMany(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), type.name())))
                .timeout(Duration.ofSeconds(30))
                .doOnError(e -> {
                    log.error("rewrite fail. type:{},key:{}, retry num {}:{}", type.name(), msg.get("key"), retry, e.getMessage());
                    if (retry < 10) {
                        Migrate.ADD_NODE_SCHEDULER.schedule(() -> rewrite(ip, msg, type, retry + 1), 10, TimeUnit.SECONDS);
                    }
                })
                .subscribe(p -> p.release());
    }

    public void put(byte[] key, byte[] value) throws RocksDBException {
        if (key[0] >= '0' && key[0] <= '9') {
            delete(key);
            if (Utils.isDeleteMarker(value)) {
                key = Utils.getDeleteMarkKey(key);
            }
        }

        if (key[0] == '+' && Utils.isMetaJson(value)) {
            MetaData metaData = Json.decodeValue(new String(value), MetaData.class);
            if (metaData.deleteMark && ((metaData.snapshotMark == null) || !metaData.discard)) {
                delete(key);
                return;
            }
        }

        if (isNeedMigrate(key) && migrate.needWriteToMigrateColumnFamily(lun, key)) {
            w.put(MSRocksDB.getMigrateColumnFamilyHandle(lun), key, value);
        }

        if (migrate.start > 0 && isNeedMigrate(key)) {
            try {
                String keyStr = new String(key);
                String vnode = LocalMigrateServer.getVnode(keyStr);
                MigrateServer.DstLunInfo dstLunInfo = MigrateServer.getInstance().getDstLunInfo(lun, vnode);
                if (null != dstLunInfo) {
                    SocketReqMsg msg = new SocketReqMsg("", 0)
                            .put("lun", dstLunInfo.lun)
                            .put("key", new String(key))
                            .put("value", new String(value));

                    rewrite(dstLunInfo.ip, msg, MIGRATE_PUT_ROCKS, 0);
                }
            } catch (Exception e) {
                log.error("", e);
            }
        }

        if (localMigrateServer.start > 0 && isNeedMigrate(key)) {
            try {
                String keyStr = new String(key);
                String vnode = LocalMigrateServer.getVnode(keyStr);
                String dstLun = localMigrateServer.getDstLun(lun, vnode);
                if (null != dstLun) {
                    //这里使用与加盘迁移相同的writeBatch
                    byte[] finalKey = key;
                    BatchRocksDB.RequestConsumer consumer = (db, writeB, request) -> {
                        writeB.put(finalKey, value);
                    };
                    BatchRocksDB.customizeOperateData(dstLun, consumer)
                            .subscribe(b -> {
                            }, e -> log.error("", e));
//                    MSRocksDB.getRocksDB(lun).put(key, value);
                }
            } catch (Exception e) {

            }
        }

        w.put(key, value);
    }

    public void put(ColumnFamilyHandle handle, byte[] key, byte[] value) throws RocksDBException {
        w.put(handle, key, value);
    }

    public void merge(byte[] key, byte[] value) throws RocksDBException {
        if (isNeedMigrate(key) && migrate.needWriteToMigrateColumnFamily(lun, key)) {
            w.merge(MSRocksDB.getMigrateColumnFamilyHandle(lun), key, value);
        }
        if (value.length > 8) {
            if (migrate.start > 0 && isNeedMigrate(key)) {
                try {
                    String keyStr = new String(key);
                    String vnode = LocalMigrateServer.getVnode(keyStr);
                    MigrateServer.DstLunInfo dstLunInfo = MigrateServer.getInstance().getDstLunInfo(lun, vnode);
                    if (null != dstLunInfo) {
                        SocketReqMsg msg = new SocketReqMsg("", 0)
                                .put("lun", dstLunInfo.lun)
                                .put("key", new String(key))
                                .put("value", Json.encode(value));
                        rewrite(dstLunInfo.ip, msg, MIGRATE_MERGE_ROCKS, 0);
                    }
                } catch (Exception e) {
                    log.error(e);
                }
            }

            if (localMigrateServer.start > 0 && isNeedMigrate(key)) {
                try {
                    String keyStr = new String(key);
                    String vnode = LocalMigrateServer.getVnode(keyStr);
                    String dstLun = localMigrateServer.getDstLun(lun, vnode);
                    if (null != dstLun) {
                        MSRocksDB.getRocksDB(lun).merge(key, value);
                    }
                } catch (Exception e) {

                }
            }
        }

        w.merge(key, value);
    }

    public void merge(ColumnFamilyHandle handle, byte[] key, byte[] value) throws RocksDBException {
        w.merge(handle, key, value);
    }

    public RocksIterator getRocksIterator(RocksIterator baseIterator) {
        return w.newIteratorWithBase(baseIterator);
    }

    public void copyMigrate(String key, String keySuffix, String value) {
        if (migrate.start > 0) {
            try {
                String vnode = LocalMigrateServer.getVnode(key);
                MigrateServer.DstLunInfo dstLunInfo = migrate.getDstLunInfo(lun, vnode);
                if (null != dstLunInfo) {
                    dstLunInfo.addCopy(new Tuple2<>(keySuffix.getBytes(), value.getBytes()));
                }
            } catch (Exception e) {

            }
        }

        if (localMigrateServer.start > 0) {
            try {
                String vnode = LocalMigrateServer.getVnode(key);
                String dstLun = localMigrateServer.getDstLun(lun, vnode);
                if (null != dstLun) {
                    localMigrateServer.addCopy(lun, vnode, new Tuple2<>(keySuffix.getBytes(), value.getBytes()));
                }
            } catch (Exception e) {

            }
        }
    }

    public void delete(ColumnFamilyHandle handle, byte[] key) throws RocksDBException {
        w.delete(handle, key);
    }

    public Mono<Boolean> write(WriteOptions writeOptions) throws Exception {
        return batchWriter.write(w, writeOptions, db);
    }

    public Result[] hookData(ByteBuf buf) {
        return batchWriter.hookData(buf);
    }
}
