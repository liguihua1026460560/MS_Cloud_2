package com.macrosan.ec.server;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.fs.BlockDevice;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.compressor.CompressorUtils;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.storage.crypto.rootKey.RootSecretKeyUtils;
import com.macrosan.utils.msutils.MsException;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.CONTINUE;
import static com.macrosan.ec.server.ErasureServer.SUCCESS_PAYLOAD;
import static com.macrosan.storage.compressor.CompressorUtils.checkCompressEnable;
import static com.macrosan.storage.crypto.CryptoUtils.checkCryptoEnable;

@Log4j2
public class AioGetServerHandler extends GetServerHandler {
    private final UnicastProcessor<Payload> responseFlux;

    private String path;
    private String lun;
    private int packetSize;
    private FileMeta meta;
    private long startStripe;
    private long endStripe;

    private int metaIndex = 0;
    private long index;
    private long endIndex;
    private int flushSize;
    private long readIndex;

    public String crypto;
    public String secretKey;
    boolean isRange = false;

    private long curIndex = 0;

    UnicastProcessor<Task> processor = UnicastProcessor.create(Queues.<Task>unboundedMultiproducer().get());


    AioGetServerHandler(UnicastProcessor<Payload> responseFlux) {
        super(responseFlux);
        this.responseFlux = responseFlux;
    }

    @Override
    protected void start(Payload payload) throws Exception {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String fileName = msg.get("fileName");
        lun = msg.get("lun");
        packetSize = Integer.parseInt(msg.get("packetSize"));
        //要读取的part起止条带
        startStripe = Long.parseLong(msg.get("startStripe"));
        endStripe = Long.parseLong(msg.get("endStripe"));
        int splitIndex = fileName.indexOf(File.separator);
        path = fileName.substring(splitIndex);
        //该part的条带数
        index = packetSize * startStripe;
        endIndex = packetSize * (endStripe + 1);

        int hash = Math.abs(FileMeta.getKey(path).hashCode() % RUNNING_MAX_NUM);
        RUNNING[hash].incrementAndGet();
        MSRocksDB rocksDB = MSRocksDB.getRocksDB(lun);
        if (rocksDB == null) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "get " + lun + " rocks db fail");
        }
        byte[] value = rocksDB.get(FileMeta.getKey(path).getBytes());
        if (null == value) {
            throw new NoSuchFileException("no such file " + path);
        }

        meta = Json.decodeValue(new String(value), FileMeta.class);
        compression = meta.getCompression();
        if (StringUtils.isNotEmpty(meta.getMetaKey()) && meta.getMetaKey().startsWith("?") && StringUtils.isEmpty(compression)) {
            isRange = true;
        }
        CompressorUtils.updateCompressStateIfNull(meta);
        crypto = meta.getCrypto();
        secretKey = RootSecretKeyUtils.rootKeyDecrypt(meta.getSecretKey(), meta.getCryptoVersion());

        flushSize = (BlockDevice.MIN_ALLOC_SIZE + packetSize - 1) / packetSize * packetSize;

        long skip = packetSize * startStripe;
        long[] lens = getBeforeLens();
        int curLen = 0;
        while (skip > 0) {
            int len = (int) lens[metaIndex];
            if (skip >= len) {
                metaIndex++;
                while (metaIndex < lens.length && lens[metaIndex] == -1) {
                    metaIndex++;
                }

                curLen = 0;
                skip -= len;
            } else {
                curLen = (int) skip;
                skip -= curLen;
            }
        }
        curIndex = curLen;
        readIndex = isRange ? index : index - curLen;
        processor.subscribe(this::taskHandler);
    }

    private static class Task {

    }

    @RequiredArgsConstructor
    @ToString
    private static class NeedReadTask extends Task {
        final long index;
        final int len;
    }

    @RequiredArgsConstructor
    @ToString
    private static class ReadTask extends Task {
        final long index;
        final int len;
        final List<byte[]> bytes;
    }

    @Override
    protected void read() {
        int readSize = (int) Math.min(packetSize, meta.getSize() - index);

        processor.onNext(new NeedReadTask(index, readSize));

        //尝试预读flushSize
        if (readIndex <= index + flushSize) {
            readFlush();
        }

        index += readSize;
    }

    TreeMap<Long, ReadTask> cacheMap = new TreeMap<>();
    LinkedList<NeedReadTask> waiting = new LinkedList<>();

    private void taskHandler(Task task) {
        try {
            if (task instanceof NeedReadTask) {
                NeedReadTask needReadTask = (NeedReadTask) task;
                waiting.add(needReadTask);
            } else {
                ReadTask readTask = (ReadTask) task;
                cacheMap.put(readTask.index, readTask);
            }

            NeedReadTask wait = waiting.peekFirst();

            while (wait != null) {
                Map.Entry<Long, ReadTask> cache = cacheMap.floorEntry(wait.index);
                if (cache != null) {
                    ReadTask readTask = cache.getValue();

                    if (readTask.len + readTask.index <= wait.index) {
                        break;
                    } else {
                        if (wait.index + wait.len > readTask.len + readTask.index) {
                            throw new MsException(ErrorNo.UNKNOWN_ERROR, "unknown read len fail");
                        }

                        byte[] read = new byte[wait.len];
                        long index = readTask.index;
                        for (byte[] bytes : readTask.bytes) {
                            int readOff = (int) Math.max(0, index - wait.index);
                            int readLen = (int) Math.min(index + bytes.length - wait.index, wait.len - readOff);
                            readLen = Math.min(bytes.length, readLen);

                            if (readLen <= 0) {

                            } else if (readOff >= read.length) {
                                break;
                            } else {
                                int bytesOff = (int) Math.max(0, wait.index - index);
                                System.arraycopy(bytes, bytesOff, read, readOff, readLen);
                            }

                            index += bytes.length;
                        }

                        if (wait.index + wait.len == meta.getSize() || wait.index + wait.len == (endStripe + 1) * packetSize) {
                            processor.onComplete();
                            responseFlux.onNext(DefaultPayload.create(read, CONTINUE.name().getBytes()));
                            responseFlux.onNext(SUCCESS_PAYLOAD);
                            responseFlux.onComplete();
                        } else {
                            responseFlux.onNext(DefaultPayload.create(read, CONTINUE.name().getBytes()));
                        }

                        if (wait.index + wait.len == readTask.index + readTask.len) {
                            cacheMap.remove(readTask.index);
                        }

                        waiting.pollFirst();
                        wait = waiting.peekFirst();
                    }
                } else {
                    break;
                }
            }
        } catch (Exception e) {
            onError(e);
        }
    }

    private void onError(Throwable e) {
        log.error("", e);
        processor.onComplete();
        responseFlux.onNext(ERROR_PAYLOAD);
        responseFlux.onComplete();
    }

    public void readFlush() {
        try {
            long[] lens = getBeforeLens();

            if (metaIndex >= lens.length) {
                return;
            }

            LinkedList<Mono<byte[]>> flushRes = new LinkedList<>();
            long startIndex = readIndex;
            do {
                flushRes.add(readPart());
            } while (metaIndex < lens.length && readIndex % flushSize != 0 && readIndex < endIndex);

            int len = (int) (readIndex - startIndex);
            Flux.concat(flushRes)
                    .collectList()
                    .subscribe(list -> processor.onNext(new ReadTask(startIndex, len, list)), this::onError);
        } catch (Throwable e) {
            onError(e);
        }
    }

    protected Mono<byte[]> readOnce() {
        BlockDevice device = BlockDevice.get(lun);
        int curMetaIndex = metaIndex;
        long[] lens = getAfterLens();

        long offset = meta.getOffset()[curMetaIndex];
        int len = (int) lens[curMetaIndex];

        if (curMetaIndex == meta.getLen().length - 1 && !checkCryptoEnable(crypto)) {
            //防止小对象读多余数据
            len = (int) Math.min(len, meta.getSize() - readIndex);
        }

        metaIndex++;

        if (len > 0) {
            return device.channel.read(offset, len);
        } else {
            return Mono.empty();
        }
    }

    private List<Mono<byte[]>> readRange(long readSize) {
        List<Mono<byte[]>> partRes = new LinkedList<>();
        BlockDevice device = BlockDevice.get(lun);
        long[] lens = getAfterLens();
        while (readSize > 0 && metaIndex < lens.length) {
            long offset = meta.getOffset()[metaIndex];
            int len = (int) lens[metaIndex];

            int canRead = (int) Math.min(len - curIndex, readSize);
            if (canRead == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "read size error");
            }
            Mono<byte[]> read = device.channel.read(offset + curIndex, canRead);
            partRes.add(read);
            readSize -= canRead;
            curIndex += canRead;
            readIndex += canRead;
            if (curIndex >= len) {
                metaIndex++;
                curIndex = 0;
            }
        }

        return partRes;
    }

    private Mono<byte[]> readPart() {
        int metaIndexStart = metaIndex;
        List<Mono<byte[]>> partRes = new LinkedList<>();
        if (!isRange) {
            partRes.add(readOnce());

            long[] lens = getBeforeLens();

            while (metaIndex < lens.length && lens[metaIndex] == -1) {
                partRes.add(readOnce());
            }

            readIndex += lens[metaIndexStart];
        } else {
            long needRead = endIndex - readIndex;
            partRes = readRange(needRead);
        }
        return Flux.concat(partRes)
                .collectList()
                .map(bytesList -> {
                    if (bytesList.size() == 1) {
                        return bytesList.get(0);
                    } else {
                        int size = bytesList.stream().mapToInt(b -> b.length).sum();
                        byte[] bytes = new byte[size];
                        int cur = 0;
                        for (byte[] b : bytesList) {
                            System.arraycopy(b, 0, bytes, cur, b.length);
                            cur += b.length;
                        }

                        return bytes;
                    }
                })
                .flatMap(srcBytes -> {
                    try {
                        if (checkCryptoEnable(crypto)) {
                            srcBytes = CryptoUtils.decrypt(crypto, secretKey, srcBytes);
                        }

                        //解压数据
                        if (checkCompressEnable(compression) && meta.getCompressState()[metaIndexStart] == 1) {
                            srcBytes = CompressorUtils.uncompressData(srcBytes, compression);
                        }

                        return Mono.just(srcBytes);
                    } catch (Exception e) {
                        return Mono.error(e);
                    }
                });
    }

    private long[] beforeLens = null;

    /**
     * 获得记录原始数据长度的lens
     * 如果开启压缩，则返回压缩前的长度。(写入数据前，先进行压缩)
     * 如果没有压缩并开启加密，则返回加密前的长度。
     */
    private long[] getBeforeLens() {
        if (beforeLens != null) {
            return beforeLens;
        }

        if (checkCompressEnable(compression)) {
            beforeLens = meta.getCompressBeforeLen();
        } else if (checkCryptoEnable(crypto)) {
            beforeLens = meta.getCryptoBeforeLen();
        } else {
            beforeLens = meta.getLen();
        }

        return beforeLens;
    }

    private long[] getAfterLens() {
        long[] lens;

        if (checkCryptoEnable(crypto)) {
            lens = meta.getCryptoAfterLen();
        } else if (checkCompressEnable(compression)) {
            lens = meta.getCompressAfterLen();
        } else {
            lens = meta.getLen();
        }

        return lens;
    }

    @Override
    public void timeOut() {
        if (StringUtils.isNotEmpty(path)) {
            int hash = Math.abs(FileMeta.getKey(path).hashCode() % RUNNING_MAX_NUM);
            RUNNING[hash].decrementAndGet();
        }
    }

    protected void complete() throws IOException {
    }

}
