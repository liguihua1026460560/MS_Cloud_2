package com.macrosan.ec.migrate;

import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.storage.StoragePool;
import com.macrosan.utils.functional.Tuple3;
import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * @author zhaoyang
 * @date 2026/01/14
 **/
@Builder
@Data
public class ScannerConfig {
    private StoragePool pool;
    private String vnode;
    private String dstDisk;
    private List<Tuple3<String, String, String>> nodeList;
    private String vKey;
    @Builder.Default
    private MSRocksDB.IndexDBEnum indexDBEnum = MSRocksDB.IndexDBEnum.ROCKS_DB;
    @Builder.Default
    private boolean listLink = true;
    private String runningKey;
    private String operator;
}

