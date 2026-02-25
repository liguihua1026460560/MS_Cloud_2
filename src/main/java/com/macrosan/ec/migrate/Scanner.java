package com.macrosan.ec.migrate;

import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.storage.client.ListMetaVnode;
import com.macrosan.storage.client.ListObjVnode;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import reactor.core.publisher.Flux;

/**
 * 扫描vnode相关的元数据和数据
 *
 * @author gaozhiyuan
 */
public class Scanner {
    private final Flux<Tuple3<VnodeDataScanner.Type, byte[], byte[]>> res;
    private final ScannerConfig config;

    public Scanner(ScannerConfig config) {
        this.config = config;
        this.res = createScanFlux();
    }

    private Flux<Tuple3<VnodeDataScanner.Type, byte[], byte[]>> createScanFlux() {
        if (config.getIndexDBEnum() == MSRocksDB.IndexDBEnum.ROCKS_DB) {
            // 普通rocksdb目录，扫描元数据合数据块
            return scanMetaAndData();
        } else {
            // 其他索引盘rocksdb目录，只扫描元数据
            return scanMeta();
        }
    }

    private Flux<Tuple3<VnodeDataScanner.Type, byte[], byte[]>> scanMeta() {
        return ListMetaVnode.listVnodeMeta(config)
                .map(t -> new Tuple3<>(VnodeDataScanner.Type.META, t.var1.getBytes(), t.var2.toString().getBytes()));
    }

    private Flux<Tuple3<VnodeDataScanner.Type, byte[], byte[]>> scanData() {
        return ListObjVnode.listVnodeObj(config)
                .map(fileMeta -> new Tuple3<>(VnodeDataScanner.Type.FILE, fileMeta.getKey().getBytes(), Json.encode(fileMeta).getBytes()));
    }

    private Flux<Tuple3<VnodeDataScanner.Type, byte[], byte[]>> scanMetaAndData() {
        return scanMeta().mergeWith(scanData());
    }


    public Flux<Tuple3<VnodeDataScanner.Type, byte[], byte[]>> res() {
        return res;
    }
}
