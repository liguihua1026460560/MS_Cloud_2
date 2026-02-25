package com.macrosan.database.rocksdb.batch;

import com.macrosan.utils.functional.Tuple2;
import reactor.core.publisher.MonoProcessor;

import java.util.concurrent.atomic.AtomicBoolean;

public class BatchRequest {
    BatchRocksDB.Type type;
    byte[][] key;
    byte[][] value;
    Tuple2<byte[], byte[]> tuple;
    MonoProcessor<Boolean> res = MonoProcessor.create();
    BatchRocksDB.RequestConsumer consumer;
    boolean lowPriority = false;
    boolean business = true;
    AtomicBoolean responseEnd = new AtomicBoolean();
}
