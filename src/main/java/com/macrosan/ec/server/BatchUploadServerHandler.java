package com.macrosan.ec.server;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.fs.BlockDevice;
import com.macrosan.message.jsonmsg.BlockInfo;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.utils.cache.Md5DigestPool;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.BATCH_PUT_OBJECT;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.ec.server.ErasureServer.SUCCESS_PAYLOAD;
import static com.macrosan.fs.BlockDevice.*;

/**
 * 同时写入多个文件
 * 一个文件所有的数据在一个put里完成
 * 只支持小文件
 *
 * @author gaozhiyuan
 */
@Log4j2
public class BatchUploadServerHandler implements RequestChannalHandler {
    private UnicastProcessor<Payload> responseFlux;
    private String[] fileName;
    private long[] fileSize;
    private String[] lun;
    private FileMeta[] fileMeta;
    private MessageDigest digest;

    private int total = 0;
    private AtomicInteger num = new AtomicInteger();

    private AtomicLong startNum = new AtomicLong();
    private AtomicLong endNum = new AtomicLong();
    private AtomicLong totalEndNum = new AtomicLong(-1);

    private Map<String, List<Integer>> lunIndexMap = new HashMap<>();

    private Map<String, Queue<Tuple2<byte[], byte[]>>> queueMap = new ConcurrentHashMap<>();

    BatchUploadServerHandler(UnicastProcessor<Payload> responseFlux) {
        this.responseFlux = responseFlux;
    }

    public void start(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        fileName = Json.decodeValue(msg.get("fileName"), String[].class);
        String[] metaKey = Json.decodeValue(msg.get("metaKey"), String[].class);
        fileSize = Json.decodeValue(msg.get("fileSize"), long[].class);
        lun = Json.decodeValue(msg.get("lun"), String[].class);

        for (int i = 0; i < lun.length; i++) {
            if (StringUtils.isNotBlank(lun[i])) {
                total++;
                List<Integer> list = lunIndexMap.computeIfAbsent(lun[i], k -> {
                    queueMap.put(k, new ConcurrentLinkedQueue<>());
                    return new LinkedList<>();
                });
                list.add(i);
            }
        }

        fileMeta = new FileMeta[fileName.length];
        digest = Md5DigestPool.acquire();

        for (int i = 0; i < fileName.length; i++) {
            fileMeta[i] = new FileMeta()
                    .setMetaKey(metaKey[i])
                    .setFileName(fileName[i])
                    .setLun(lun[i])
                    .setSmallFile(false);
        }

//        log.info("put {}", Arrays.toString(fileName));
    }

    public void put(Payload payload) {
        String meta = payload.getMetadataUtf8();
        int index = Integer.parseInt(meta.substring(BATCH_PUT_OBJECT.name().length()));
        String lun = this.lun[index];

        ByteBuf byteBuf = payload.sliceData();
        int read = byteBuf.readableBytes();
        byte[] bs = new byte[read];
        byteBuf.readBytes(bs);

        startNum.incrementAndGet();

        fileMeta[index].setEtag(Hex.encodeHexString(digest.digest(bs)));

        if (num.incrementAndGet() == total) {
            totalEndNum.compareAndSet(-1, startNum.get());
        }

        BlockDevice device = get(lun);
        if (null == device) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "get " + lun + " rocks db fail");
        }
        BlockDevice.get(lun).channel.write(bs).subscribe(res -> {
            long[] offset = new long[res.length];
            long[] size = new long[res.length];

            for (int i = 0; i < res.length; i++) {
                offset[i] = res[i].offset;
                size[i] = res[i].size;

                BlockInfo blockInfo = new BlockInfo()
                        .setFileName(new String[]{fileName[index]})
                        .setOffset(res[i].offset)
                        .setTotal(res[i].size)
                        .setLen(new long[]{res[i].size});
                queueMap.get(lun).add(new Tuple2<>(blockInfo.getKey().getBytes(), Json.encode(blockInfo).getBytes()));
            }

            fileMeta[index].setOffset(offset)
                    .setLen(size);

            if (endNum.incrementAndGet() == totalEndNum.get()) {
                complete();
            } else {
                responseFlux.onNext(DefaultPayload.create(index + "", SUCCESS.name()));
            }
        });
    }

    private void complete() {
        Flux<Boolean> flux = Flux.empty();

        for (String lun : lunIndexMap.keySet()) {
            List<Integer> list = lunIndexMap.get(lun);

            flux = flux.concatWith(BatchRocksDB.customizeOperateData(lun, (db, w, r) -> {
                int n = 0;
                long total = 0L;
                long totalAlloc = 0L;

                for (int index : list) {
                    FileMeta meta = fileMeta[index];
                    n++;
                    total += fileSize[index];
                    for (long len : meta.getLen()) {
                        totalAlloc += len;
                    }

                    w.put(meta.getKey().getBytes(), Json.encode(meta).getBytes());
                }

                for (Tuple2<byte[], byte[]> tuple : queueMap.get(lun)) {
                    w.put(tuple.var1, tuple.var2);
                }

                w.merge(ROCKS_FILE_SYSTEM_FILE_NUM, BatchRocksDB.toByte(n));
                w.merge(ROCKS_FILE_SYSTEM_FILE_SIZE, BatchRocksDB.toByte(total));
                w.merge(ROCKS_FILE_SYSTEM_USED_SIZE, BatchRocksDB.toByte(totalAlloc));
            }));
        }

        flux.doOnError(e -> {
//            log.error("", e);
            responseFlux.onNext(ERROR_PAYLOAD);
            responseFlux.onComplete();
        }).doOnComplete(() -> {
            responseFlux.onNext(SUCCESS_PAYLOAD);
            responseFlux.onComplete();
        }).subscribe();
    }

    @Override
    public void timeOut() {

    }
}
