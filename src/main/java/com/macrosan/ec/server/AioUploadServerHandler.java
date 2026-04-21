package com.macrosan.ec.server;

import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.fs.Allocator;
import com.macrosan.fs.BlockDevice;
import com.macrosan.message.jsonmsg.BlockInfo;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.LocalPayload;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.compressor.CompressorUtils;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.storage.crypto.rootKey.RootSecretKeyUtils;
import com.macrosan.utils.cache.ByteArrayPool;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.checksum.ChecksumProvider;
import com.macrosan.utils.ratelimiter.RecoverLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.macrosan.database.rocksdb.MSRocksDB.READ_OPTIONS;
import static com.macrosan.database.rocksdb.MossMergeOperator.SPACE_LEN;
import static com.macrosan.database.rocksdb.MossMergeOperator.SPACE_SIZE;
import static com.macrosan.database.rocksdb.batch.BatchRocksDB.*;
import static com.macrosan.ec.Utils.*;
import static com.macrosan.ec.server.ErasureServer.CONTINUE_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.fs.BlockDevice.*;

@Log4j2
public class AioUploadServerHandler implements RequestChannalHandler {
    public static final ByteArrayPool BYTE_ARRAY_POOL = new ByteArrayPool(BlockDevice.MIN_ALLOC_SIZE, 256, 64, 32);
    public static final int MAX_INFLIGHT_FLUSH;

    static {
        MAX_INFLIGHT_FLUSH = Integer.parseInt(System.getProperty("macrosan.aio.inflightFlush", "1"));
    }

    protected UnicastProcessor<Payload> responseFlux;
    protected String path;
    protected String lun;
    protected ChecksumProvider checksumProvider;
    protected String fileName;
    protected long fileSize = 0;
    /**
     * 存放写入数据池rocksdb的key和value，包括FileMeta和BlockInfo。（通过FileMeta找到数据所在的BlockInfo）
     * 上传完成后才会将tuple中的数据写入rocksdb
     */
    protected Queue<Tuple2<byte[], byte[]>> tuple = new ConcurrentLinkedQueue<>();
    protected FileMeta fileMeta;
    protected boolean overwrite = true;
    /**
     * 缓存未达到BlockDevice.MIN_ALLOC_SIZE的字节，之后再合成一个字节数组后一起flush
     */
    private LinkedList<ByteBuf> linkedList = new LinkedList<>();
    /**
     * flush开始的次数
     */
    private AtomicInteger startFlushNum = new AtomicInteger(0);
    /**
     * flush结束的次数
     */
    private AtomicInteger endFlushNum = new AtomicInteger(0);
    /**
     * alloc到并已用于flush的Result数组（经合并后）的长度
     */
    private AtomicInteger allocResNum = new AtomicInteger(0);
    /**
     * 存放所有blockInfo的offset和size，用来生成fileMeta
     */
    private List<Info> blockInfoList = new LinkedList<>();
    /**
     * 每次payload.sliceData()传入的数据长度，flush后归零
     */
    private int byteBufLen = 0;
    /**
     * 该注册的方法在最后一次调用flush完毕时才会执行一次。
     */
    private Consumer<Void> completeFlush = null;
    private AtomicLong total = new AtomicLong(-1);

    protected String compression;
    protected long compressionFileSize = 0;
    protected long beforeCompressionLen = 0;
    protected boolean recover = false;

    protected final List<CompressionInfo> compressionInfoList = new LinkedList<>();

    protected String crypto;
    protected String secretKey;
    protected final List<CryptoInfo> cryptoInfoList = new LinkedList<>();

    public boolean isNullDevice;

    // 替换掉旧的fileMeta，overwrite为true时生效
    protected boolean replace = false;

    protected String dataPool;//回迁后记录数据来源于哪个数据池


    Payload ERROR_PAYLOAD;
    Payload SUCCESS_PAYLOAD;


    private final AtomicInteger flushingNum = new AtomicInteger(0);

    public AioUploadServerHandler(UnicastProcessor<Payload> responseFlux) {
        this(responseFlux, false);
    }

    public AioUploadServerHandler(UnicastProcessor<Payload> responseFlux, boolean local) {
        this.responseFlux = responseFlux;

        if (local) {
            ERROR_PAYLOAD = LocalPayload.ERROR_PAYLOAD;
            SUCCESS_PAYLOAD = LocalPayload.SUCCESS_PAYLOAD;
        } else {
            ERROR_PAYLOAD = ErasureServer.ERROR_PAYLOAD;
            SUCCESS_PAYLOAD = ErasureServer.SUCCESS_PAYLOAD;
        }
    }


    public void start(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        start(msg);
    }

    /**
     * 处理初始化上传
     *
     * @param msg
     */
    public void start(SocketReqMsg msg) {
        lun = msg.get("lun");
        fileName = msg.get("fileName");
        if ("1".equals(msg.get("noGet"))) {
            overwrite = false;
        } else {
            overwrite = true;
        }

        if ("1".equals(msg.get("replace"))) {
            replace = true;
        }

        compression = msg.get("compression");

        crypto = msg.get("crypto");
        secretKey = msg.get("secretKey");

        path = File.separator + lun + File.separator + fileName;
        checksumProvider = ChecksumProvider.create();
        fileMeta = new FileMeta()
                .setMetaKey(msg.get("metaKey"))
                .setFileName(fileName)
                .setSmallFile(false)
                .setLun(lun)
                .setFlushStamp(msg.get("flushStamp"));

        if (StringUtils.isNotEmpty(msg.get("lastAccessStamp"))) {
            fileMeta.setLastAccessStamp(msg.get("lastAccessStamp"));//这里同时要在设置fileMeta时在缓存盘设置一个访问记录
        }
        if (StringUtils.isNotEmpty(msg.get("dataPool"))) {
            dataPool = msg.get("dataPool");
        }

        if (msg.getDataMap().containsKey("fileOffset")) {
            fileMeta.setFileOffset(Long.parseLong(msg.get("fileOffset")));
        } else {
            String metaKey = fileMeta.getMetaKey();
            if (StringUtils.isNotBlank(metaKey) && metaKey.startsWith(SysConstants.ROCKS_INODE_PREFIX)) {
                fileMeta.setFileOffset(-1L);
            }
        }

        if ("1".equalsIgnoreCase(msg.get("recover"))) {
            recover = true;
        }
        isNullDevice = false;

    }

    /**
     * 当内存中待写数据达到MIN_ALLOC_SIZE，落盘。
     */
    private Mono<Boolean> flush() {
        byte[] bs;
        boolean pooled = false;
        try {
            if (byteBufLen == MIN_ALLOC_SIZE) {
                bs = BYTE_ARRAY_POOL.borrowArray();
                pooled = true;
            } else {
                bs = new byte[byteBufLen];
            }
        } catch (Exception e) {
            bs = new byte[byteBufLen];
        }
        int index = 0;
        for (ByteBuf buf : linkedList) {
            int len = buf.readableBytes();
            buf.readBytes(bs, index, len);
            index += len;
            buf.release();
        }

        linkedList.clear();
        byteBufLen = 0;

        boolean finalPooled = pooled;
        byte[] finalBs = bs;
        return flush(bs).doFinally(s -> {
                    if (finalPooled) {
                        BYTE_ARRAY_POOL.returnArray(finalBs);
                    }
                }
        );
    }

    /**
     * 当内存中待写数据达到MIN_ALLOC_SIZE，落盘。
     */
    public Mono<Boolean> flush(byte[] bs) {
        fileSize += bs.length;
        checksumProvider.update(bs);
        Info info = new Info();
        blockInfoList.add(info);

        startFlushNum.incrementAndGet();

        BlockDevice device = get(lun);
        if (null == device) {
            isNullDevice = true;
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "get " + lun + " rocks db fail");
        }
        if (StringUtils.isNotBlank(lun) && errorLun.contains(lun)) {
            isNullDevice = true;
            throw new MsException(ErrorNo.UNKNOWN_ERROR, lun + " is error");
        }

        AtomicReference<CompressionInfo> compressionInfo = new AtomicReference<>();
        //todo 这里修改下统计原始数据补齐4KB后的长度，不再使用file_size去直接统计，统计使用压缩后补齐4KB的长度计算压缩比
        long beforeLen = bs.length % 4096 == 0 ? bs.length : (bs.length / 4096 * 4096 + 4096);
        beforeCompressionLen += beforeLen;
        bs = dealCompressionBeforeWrite(bs, compressionInfo);

        AtomicReference<CryptoInfo> cryptoInfo = new AtomicReference<>();
        bs = dealCryptoBeforeWrite(bs, cryptoInfo);

        byte[] finalBytes = bs;
        return RecoverLimiter.getInstance().acquireData(device.getName(), finalBytes.length, recover)
                .flatMap(b -> device.channel.write(finalBytes))
                .map(res -> {
                    info.offset = new long[res.length];
                    info.len = new long[res.length];

                    for (int i = 0; i < res.length; i++) {
                        Allocator.Result r = res[i];

                        List<byte[]> list = BlockInfo.getUpdateValue(r.offset, r.size, "upload");
                        for (int j = 0; j < list.size(); j++) {
                            String key = BlockInfo.getFamilySpaceKey((r.offset / SPACE_SIZE) + j);
                            tuple.add(new Tuple2<>(key.getBytes(), list.get(j)));
                        }
                        info.offset[i] = r.offset;
                        info.len[i] = r.size;
                    }

                    dealCompressionAfterWrite(res, compressionInfo.get());

                    dealCryptoAfterWrite(res, cryptoInfo.get());

                    allocResNum.addAndGet(res.length);
                    if (endFlushNum.incrementAndGet() == total.get()) {
                        completeFlush.accept(null);
                    }
                    return true;
                });
    }

    protected ByteBuf dealCompressionBeforeWrite(ByteBuf data, AtomicReference<CompressionInfo> compressionInfo) {
        if (CompressorUtils.checkCompressEnable(compression)) {
            compressionInfo.set(new CompressionInfo());
            compressionInfoList.add(compressionInfo.get());
            compressionInfo.get().beforeLen = data.readableBytes();
            try {
                Tuple2<byte[], Boolean> res = CompressorUtils.compressDataCheckRatio(data.array(), compression, 1);
                byte[] bs1 = res.var1;
                compressionInfo.get().compressFlag = res.var2;
                compressionFileSize += bs1.length;
                compressionInfo.get().afterLen = bs1.length;
                return Unpooled.wrappedBuffer(bs1);
            } catch (Exception e) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "compression error:" + e.getMessage());
            }
        }
        return data;
    }

    protected byte[] dealCompressionBeforeWrite(byte[] data, AtomicReference<CompressionInfo> compressionInfo) {
        if (CompressorUtils.checkCompressEnable(compression)) {
            compressionInfo.set(new CompressionInfo());
            compressionInfoList.add(compressionInfo.get());
            compressionInfo.get().beforeLen = data.length;
            try {
                Tuple2<byte[], Boolean> res = CompressorUtils.compressDataCheckRatio(data, compression, 1);
                byte[] bs1 = res.var1;
                compressionInfo.get().compressFlag = res.var2;
                compressionFileSize += bs1.length;
                compressionInfo.get().afterLen = bs1.length;
                return bs1;
            } catch (Exception e) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "compression error:" + e.getMessage());
            }
        }
        return data;
    }

    protected void dealCompressionAfterWrite(Allocator.Result[] res, CompressionInfo compressionInfo) {
        if (CompressorUtils.checkCompressEnable(compression)) {

            compressionInfo.compressionBeforeLen = new long[res.length];
            compressionInfo.compressionAfterLen = new long[res.length];
            compressionInfo.comressionState = new long[res.length];
            for (int i = 0; i < res.length; i++) {
                Allocator.Result r = res[i];
                compressionInfo.compressionAfterLen[i] = Math.min(compressionInfo.afterLen, r.size);
                compressionInfo.afterLen -= r.size;
            }

            Arrays.fill(compressionInfo.compressionBeforeLen, 1, res.length, -1);
            compressionInfo.compressionBeforeLen[0] = compressionInfo.beforeLen;

            Arrays.fill(compressionInfo.comressionState, 1, res.length, -1);
            if (compressionInfo.compressFlag) {
                compressionInfo.comressionState[0] = 1;
            }
        }
    }

    protected ByteBuf dealCryptoBeforeWrite(ByteBuf data, AtomicReference<CryptoInfo> cryptoInfo) {
        if (CryptoUtils.checkCryptoEnable(crypto)) {
            cryptoInfo.set(new CryptoInfo());
            cryptoInfoList.add(cryptoInfo.get());
            cryptoInfo.get().beforeLen = data.readableBytes();
            byte[] data1 = CryptoUtils.encrypt(crypto, secretKey, data.array());
            cryptoInfo.get().afterLen = data1.length;
            return Unpooled.wrappedBuffer(data1);
        }
        return data;
    }

    protected byte[] dealCryptoBeforeWrite(byte[] data, AtomicReference<CryptoInfo> cryptoInfo) {
        if (CryptoUtils.checkCryptoEnable(crypto)) {
            cryptoInfo.set(new CryptoInfo());
            cryptoInfoList.add(cryptoInfo.get());
            cryptoInfo.get().beforeLen = data.length;
            byte[] data1 = CryptoUtils.encrypt(crypto, secretKey, data);
            cryptoInfo.get().afterLen = data1.length;
            return data1;
        }
        return data;
    }

    protected void dealCryptoAfterWrite(Allocator.Result[] res, CryptoInfo cryptoInfo) {
        if (CryptoUtils.checkCryptoEnable(crypto)) {
            cryptoInfo.cryptoBeforeLen = new long[res.length];
            cryptoInfo.cryptoAfterLen = new long[res.length];
            for (int i = 0; i < res.length; i++) {
                Allocator.Result r = res[i];
                cryptoInfo.cryptoAfterLen[i] = Math.min(cryptoInfo.afterLen, r.size);
                cryptoInfo.afterLen -= r.size;
            }
            Arrays.fill(cryptoInfo.cryptoBeforeLen, 1, res.length, -1);
            cryptoInfo.cryptoBeforeLen[0] = cryptoInfo.beforeLen;
        }
    }


    /**
     * 注册flush结束后要调用的方法。只可以注册一次。
     */
    private synchronized void registerCompleteFlush(Consumer<Void> completeFlush) {
        if (this.completeFlush == null) {
            this.completeFlush = completeFlush;
            this.total.set(startFlushNum.get() + 1);

            if (endFlushNum.incrementAndGet() == total.get()) {
                completeFlush.accept(null);
            }
        } else {
            // throw new MsException(ErrorNo.UNKNOWN_ERROR, "duplicate complete flush");
        }
    }

    public void put(Payload payload) throws IOException {
        ByteBuf byteBuf = payload.sliceData();
        int read = byteBuf.readableBytes();
        linkedList.addLast(byteBuf);
        byteBuf.retain();
        byteBufLen += read;
        if (byteBufLen >= BlockDevice.MIN_ALLOC_SIZE) {
            boolean asyncFlush = flushingNum.incrementAndGet() <= MAX_INFLIGHT_FLUSH;
            flush().doOnError(e -> {
                log.error("", e);  // 流中出现错误，外部ErasureServer捕获不到，因而在此处理responseFlux
                this.timeOut();
                responseFlux.onNext(ERROR_PAYLOAD);
                responseFlux.onComplete();
            }).doFinally(signalType -> {
                flushingNum.decrementAndGet();
            }).subscribe(b -> {
                if (!asyncFlush) {
                    responseFlux.onNext(CONTINUE_PAYLOAD);
                }
            });
            if (asyncFlush) {
                responseFlux.onNext(CONTINUE_PAYLOAD);
            }
        } else {
            //此时并未flush，而是将不足MIN_ALLOC_SIZE的部分先放入缓存linkedList
            responseFlux.onNext(CONTINUE_PAYLOAD);
        }
    }

    public void complete() {
        if (byteBufLen != 0) {
            flush().doOnError(e -> {
                log.error("Complete error: ", e);  // 流中出现错误，外部ErasureServer捕获不到，因此在此处理responseFlux
                this.timeOut();
                responseFlux.onNext(ERROR_PAYLOAD);
                responseFlux.onComplete();
            }).subscribe();
        }

        registerCompleteFlush(v -> {
            try {
                String checksum = checksumProvider.getChecksum();
                checksumProvider.release();
                fileMeta.setEtag(checksum)
                        .setSize(fileSize);

                long[] offsetArray = new long[allocResNum.get()];
                long[] lenArray = new long[allocResNum.get()];
                int i = 0;
                long totalLen = 0L;

                for (Info info : blockInfoList) {
                    for (int j = 0; j < info.offset.length; j++) {
                        offsetArray[i] = info.offset[j];
                        lenArray[i] = info.len[j];
                        totalLen += lenArray[i];
                        i++;
                    }
                }

                fileMeta.setOffset(offsetArray);
                fileMeta.setLen(lenArray);
                AtomicBoolean needCompression = new AtomicBoolean(false);
                if (CompressorUtils.checkCompressEnable(compression)) {
                    long[] compressStateArray = new long[allocResNum.get()];
                    long[] compressBeforeLenArray = new long[allocResNum.get()];
                    long[] compressAfterLenArray = new long[allocResNum.get()];
                    int compressIndex = 0;
                    for (CompressionInfo compressInfo : compressionInfoList) {
                        if (!needCompression.get() && compressInfo.comressionState[0] == 1) {
                            needCompression.set(true);
                        }
                        for (int j = 0; j < compressInfo.compressionAfterLen.length; j++) {
                            compressStateArray[compressIndex] = compressInfo.comressionState[j];
                            compressBeforeLenArray[compressIndex] = compressInfo.compressionBeforeLen[j];
                            compressAfterLenArray[compressIndex] = compressInfo.compressionAfterLen[j];
                            compressIndex++;
                        }
                    }
                    if (needCompression.get()) {
                        fileMeta.setCompressState(compressStateArray);
                        fileMeta.setCompressAfterLen(compressAfterLenArray);
                        fileMeta.setCompressBeforeLen(compressBeforeLenArray);
                        fileMeta.setCompression(compression);
                    }
                }


                if (CryptoUtils.checkCryptoEnable(crypto)) {
                    long[] cryptoBeforeLenArray = new long[allocResNum.get()];
                    long[] cryptoAfterLenArray = new long[allocResNum.get()];
                    int cryptoIndex = 0;
                    for (CryptoInfo cryptoInfo : cryptoInfoList) {
                        for (int j = 0; j < cryptoInfo.cryptoBeforeLen.length; j++) {
                            cryptoBeforeLenArray[cryptoIndex] = cryptoInfo.cryptoBeforeLen[j];
                            cryptoAfterLenArray[cryptoIndex] = cryptoInfo.cryptoAfterLen[j];
                            cryptoIndex++;
                        }
                    }
                    fileMeta.setCryptoBeforeLen(cryptoBeforeLenArray);
                    fileMeta.setCryptoAfterLen(cryptoAfterLenArray);
                    fileMeta.setCrypto(crypto);
                    Tuple2<String, String> tuple2 = RootSecretKeyUtils.rootKeyEncrypt(secretKey);
                    fileMeta.setCryptoVersion(tuple2.var1);
                    fileMeta.setSecretKey(tuple2.var2);
                }

                String fileMetaKey = fileMeta.getKey();
                tuple.add(new Tuple2<>(fileMetaKey.getBytes(), Json.encode(fileMeta).getBytes()));

                long finalTotalLen = totalLen;

                BatchRocksDB.RequestConsumer consumer0 = (db, writeBatch, request) -> {
                    try {
                        tuple.forEach((data) -> {
                            if (new String(data.var1).startsWith(ROCKS_FILE_SYSTEM_PREFIX_OFFSET)) {
                                try {
                                    writeBatch.merge(MSRocksDB.getColumnFamily(lun), data.var1, data.var2);
                                } catch (RocksDBException e) {
                                    log.info("rocksdb merge error:{}", e.getMessage());
                                }
                            } else {
                                try {
                                    writeBatch.put(data.var1, data.var2);
                                    if (fileMeta.getFlushStamp() != null) {
                                        writeBatch.put(getCacheOrderKey(fileMeta.getFlushStamp(), fileMeta.getFileName()).getBytes(), ZERO_BYTES);
                                    }
                                    if (StringUtils.isNotEmpty(fileMeta.getLastAccessStamp())) {//put时同步增加访问记录
                                        StoragePool pool = StoragePoolFactory.getStoragePoolByDisk(lun);
                                        if (pool != null && pool.getVnodePrefix().startsWith("cache")) {
                                            if (StringUtils.isNotEmpty(dataPool)) {
                                                writeBatch.put(getAccessTimeKey(fileMeta.getLastAccessStamp(), fileMeta.getFileName()).getBytes(), dataPool.getBytes());//数据池回迁到缓存池的数据记录数据池vnode前缀
                                            } else {
                                                writeBatch.put(getAccessTimeKey(fileMeta.getLastAccessStamp(), fileMeta.getFileName()).getBytes(), ZERO_BYTES);
                                            }
                                        }
                                    }
                                } catch (RocksDBException e) {
                                    log.info("rocksdb put error:{}", e.getMessage());
                                }
                            }
                        });
                        writeBatch.merge(ROCKS_FILE_SYSTEM_FILE_NUM, BatchRocksDB.toByte(1L));
                        writeBatch.merge(ROCKS_FILE_SYSTEM_FILE_SIZE, BatchRocksDB.toByte(fileMeta.getSize()));
                        writeBatch.merge(ROCKS_FILE_SYSTEM_USED_SIZE, BatchRocksDB.toByte(finalTotalLen));
                        if (needCompression.get()) {
                            writeBatch.merge(ROCKS_COMPRESSION_FILE_SYSTEM_FILE_SIZE, BatchRocksDB.toByte(fileMeta.getSize() - compressionFileSize));
                            writeBatch.merge(ROCKS_BEFORE_COMPRESSION_FILE_SYSTEM_FILE_SIZE, BatchRocksDB.toByte(beforeCompressionLen));//增加统计压缩前的文件量
                        } else {
                            writeBatch.merge(ROCKS_BEFORE_COMPRESSION_FILE_SYSTEM_FILE_SIZE, BatchRocksDB.toByte(finalTotalLen));//不压缩就直接统计原始落盘流量
                        }

                    } catch (Exception e) {
                        log.error("put file meta error", e);
                    }
                };
                BatchRocksDB.RequestConsumer consumer;
                if (overwrite) {
                    consumer = (db, writeBatch, request) -> {
                        byte[] oldValue = writeBatch.getFromBatchAndDB(db, fileMeta.getKey().getBytes());
                        if (null == oldValue) {
                            consumer0.accept(db, writeBatch, request);
                        } else {
                            if (replace) {
                                try {
                                    FileMeta oldMeta = Json.decodeValue(new String(oldValue), FileMeta.class);
                                    long totalLen1 = 0L;
                                    for (int i1 = 0; i1 < oldMeta.getOffset().length; i1++) {
                                        long offset = oldMeta.getOffset()[i1];
                                        long len = oldMeta.getLen()[i1];
                                        totalLen1 += len;

                                        List<byte[]> list = BlockInfo.getUpdateValue(offset, len, "upload");
                                        for (int j = 0; j < list.size(); j++) {
                                            byte[] key = BlockInfo.getFamilySpaceKey((offset / SPACE_SIZE) + j).getBytes();
                                            byte[] value = list.get(j);
                                            byte[] result = new byte[SPACE_LEN * 2];
                                            for (int a = 0; a < SPACE_LEN; a++) {
                                                result[a + SPACE_LEN] = (byte) ~value[a];
                                            }
                                            writeBatch.merge(MSRocksDB.getColumnFamily(lun), key, result);
                                        }
                                    }

                                    writeBatch.merge(ROCKS_FILE_SYSTEM_FILE_NUM, toByte(-1L));
                                    writeBatch.merge(ROCKS_FILE_SYSTEM_FILE_SIZE, toByte(-oldMeta.getSize()));
                                    writeBatch.merge(ROCKS_FILE_SYSTEM_USED_SIZE, toByte(-totalLen1));

                                    consumer0.accept(db, writeBatch, request);
                                } catch (Exception e) {
                                    log.info("replace fileMeta err, {}", fileMeta.getKey(), e);
                                }

                            } else {
                                addToReleaseMap(offsetArray, lenArray, lun);
                            }
                        }
                    };
                } else {
                    consumer = consumer0;
                }


                //写入FileMeta
                BatchRocksDB.customizeOperateData(lun, fileMetaKey.hashCode(), consumer)
                        .doOnError(e -> {
//                            log.error("", e);
                            responseFlux.onNext(ERROR_PAYLOAD);
                            responseFlux.onComplete();
                            rollBack();
                        })
                        .subscribe(s -> {
                            responseFlux.onNext(SUCCESS_PAYLOAD);
                            responseFlux.onComplete();
                        });
            } catch (Exception e) {
                log.error("", e);
            }
        });

    }

    private void rollBack() {
        if (StringUtils.isNotBlank(lun) && errorLun.contains(lun)) {
            if (!lunRollBackPrint.contains(lun)) {
                log.info("roll back {}", fileName);
                lunRollBackPrint.add(lun);
            }
        } else {
            log.info("roll back {}", fileName);
        }
        long[] offsets = new long[allocResNum.get()];
        long[] lens = new long[allocResNum.get()];
        long totalLen = 0L;
        int i = 0;
        synchronized (blockInfoList) {
            for (Info info : blockInfoList) {
                if (info.offset != null) {
                    for (int j = 0; j < info.offset.length; j++) {
                        offsets[i] = info.offset[j];
                        lens[i] = info.len[j];
                        totalLen += lens[i];
                        i++;
//                        errorReleaseAmount.addAndGet(info.len[j]);
                    }
                }
            }
            blockInfoList.clear();
            addToReleaseMap(offsets, lens, lun);
        }

        delRocksThings(offsets, totalLen);
    }

    @Override
    public void timeOut() {
        registerCompleteFlush(v -> {
            rollBack();
        });
    }

    private void delRocksThings(long[] offsetArray, long totalLen) {
        BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
            try {
                String fileMetaKey = FileMeta.getKey(fileName);
                byte[] fileMetaBytes = writeBatch.getFromBatchAndDB(db, READ_OPTIONS, fileMetaKey.getBytes());
                //只有当这个请求实际已经写入才执行回退
                if (fileMetaBytes != null) {
                    FileMeta oldMeta = Json.decodeValue(new String(fileMetaBytes), FileMeta.class);
                    if (Arrays.equals(oldMeta.getOffset(), offsetArray)) {
                        writeBatch.merge(ROCKS_FILE_SYSTEM_FILE_NUM, toByte(-1L));
                        writeBatch.merge(ROCKS_FILE_SYSTEM_FILE_SIZE, toByte(-fileMeta.getSize()));
                        writeBatch.merge(ROCKS_FILE_SYSTEM_USED_SIZE, toByte(-totalLen));
                        for (Tuple2<byte[], byte[]> tuple2 : tuple) {
                            byte[] key = tuple2.var1;
                            byte[] value = tuple2.var2;

                            if (key[0] == '.' && key[1] == '1' && key[2] == '.') {
                                byte[] result = new byte[SPACE_LEN * 2];
                                for (int i = 0; i < SPACE_LEN; i++) {
                                    result[i + SPACE_LEN] = (byte) ~value[i];
                                }
                                writeBatch.merge(MSRocksDB.getColumnFamily(lun), tuple2.var1, result);
                            } else {
                                writeBatch.delete(tuple2.var1);
                                log.info("delete fileMeta: {}", tuple2.var1);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.error("delRocksThings error", e);
            }
        };
        try {
            BatchRocksDB.customizeOperateData(lun, consumer)
                    .subscribeOn(DISK_SCHEDULER)
//                    .doOnError(e -> log.error("", e))
                    .subscribe();
        } catch (Exception e) {
            if (!"ReactiveException".equals(e.getClass().getSimpleName())) {
                log.error("delRocksThings exec error", e);
            }
        }
    }

    protected static class Info {
        long[] offset;
        long[] len;

        @Override
        public String toString() {
            return Arrays.toString(offset) + " " + Arrays.toString(len);
        }
    }

    protected static class CompressionInfo {
        long[] compressionBeforeLen;

        long[] compressionAfterLen;

        long[] comressionState;

        long beforeLen;

        long afterLen;

        boolean compressFlag;
    }

    protected static class CryptoInfo {
        long[] cryptoBeforeLen;

        long[] cryptoAfterLen;

        long beforeLen;

        long afterLen;
    }

}
