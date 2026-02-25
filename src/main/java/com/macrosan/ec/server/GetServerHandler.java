package com.macrosan.ec.server;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.fs.BlockDevice;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.compressor.CompressorUtils;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.UnicastProcessor;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.CONTINUE;
import static com.macrosan.ec.server.ErasureServer.SUCCESS_PAYLOAD;
import static com.macrosan.storage.compressor.CompressorUtils.checkCompressEnable;
import static java.nio.file.StandardOpenOption.READ;

@Log4j2
public class GetServerHandler implements RequestChannalHandler {
    public static final EnumSet<StandardOpenOption> READ_OPTION = EnumSet.of(READ);
    public static final int RUNNING_MAX_NUM = 50000;
    public static final AtomicInteger[] RUNNING = new AtomicInteger[RUNNING_MAX_NUM];
    private UnicastProcessor<Payload> responseFlux;

    private String path;
    private long index;
    private long startIndex;
    private long endStripe;
    private String lun;
    private FileMeta meta;
    /**
     * FileMeta的offset和len的索引。
     */
    private int metaIndex = 0;
    private long curLen = 0;
    private int packetSize;

    public String compression;

    static {
        for (int i = 0; i < RUNNING_MAX_NUM; i++) {
            RUNNING[i] = new AtomicInteger();
        }
    }

    GetServerHandler(UnicastProcessor<Payload> responseFlux) {
        this.responseFlux = responseFlux;
    }


    protected void start(Payload payload) throws Exception {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String fileName = msg.get("fileName");
        lun = msg.get("lun");
        packetSize = Integer.parseInt(msg.get("packetSize"));
        //要读取的part起止条带
        long startStripe = Long.parseLong(msg.get("startStripe"));
        endStripe = Long.parseLong(msg.get("endStripe"));
        long fileSize = Long.parseLong(msg.get("fileSize"));

        int splitIndex = fileName.indexOf(File.separator);
        path = fileName.substring(splitIndex);
        //该part的条带数
        startIndex = packetSize * startStripe;
        index = startIndex;
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

        long skip = packetSize * startStripe;

        compression = meta.getCompression();
        if (checkCompressEnable(compression) && meta.getCompressState() == null) {
            long[] compressState = new long[meta.getCompressAfterLen().length];
            Arrays.fill(compressState, 1);
            meta.setCompressState(compressState);
        }
        if (!checkCompressEnable(compression)) {
            while (skip > 0) {
                int len = (int) meta.getLen()[metaIndex];
                if (skip >= len) {
                    metaIndex++;
                    curLen = 0;
                    skip -= len;
                } else {
                    curLen = skip;
                    skip -= curLen;
                }
            }
        } else {
            while (skip > 0) {
                int len = (int) meta.getCompressBeforeLen()[metaIndex];
                if (skip >= len) {
                    updateNextMetaLenIndex(meta.getCompressBeforeLen());
                    curLen = 0;
                    skip -= len;
                } else {
                    curLen = skip;
                    skip -= curLen;
                }
            }
        }
    }

    protected void read() throws Exception {
        if (!checkCompressEnable(compression)) {
            readIfUncompressed();
        } else {
            readIfCompressed();
        }
    }

    private void readIfUncompressed() {
        int readSize;
        if (meta.getSize() - index < packetSize) {
            readSize = (int) (meta.getSize() - index);
        } else {
            readSize = packetSize;
        }


        int lastReadSize = readSize;
        LinkedList<byte[]> res = new LinkedList<>();

        while (lastReadSize > 0) {
            long offset = meta.getOffset()[metaIndex];
            int len = (int) meta.getLen()[metaIndex];

            int canRead = (int) Math.min(len - curLen, lastReadSize);
            if (canRead == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "read size error");
            }

            byte[] tmp = BlockDevice.get(lun).read(offset + curLen, canRead);
            res.addLast(tmp);
            lastReadSize -= canRead;
            curLen += canRead;

            if (curLen >= len) {
                metaIndex++;
                curLen = 0;
            }
        }

        index += readSize;

        if (res.size() == 1) {
            responseFlux.onNext(DefaultPayload.create(res.pollFirst(), CONTINUE.name().getBytes()));
        } else {
            byte[] bytes = new byte[readSize];
            int curIndex = 0;
            for (byte[] b : res) {
                System.arraycopy(b, 0, bytes, curIndex, b.length);
                curIndex += b.length;
            }

            responseFlux.onNext(DefaultPayload.create(bytes, CONTINUE.name().getBytes()));
        }

        if (index == meta.getSize()) {
            responseFlux.onNext(SUCCESS_PAYLOAD);
            responseFlux.onComplete();
        }

    }

    private void readIfCompressed() throws Exception {
        int readSize;
        if (meta.getSize() - index < packetSize) {
            readSize = (int) (meta.getSize() - index);
        } else {
            readSize = packetSize;
        }

        int lastReadSize = readSize;
        LinkedList<byte[]> res = new LinkedList<>();

        long compressCurLen = curLen;

        while (lastReadSize > 0) {
            long offset = meta.getOffset()[metaIndex];
            long compressBeforeLen = meta.getCompressBeforeLen()[metaIndex];
            long compressAfterLen = meta.getCompressAfterLen()[metaIndex];

            int canRead = (int) Math.min(compressBeforeLen - curLen, lastReadSize);
            if (canRead == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "read size error");
            }

            byte[] tmp = BlockDevice.get(lun).read(offset, (int) compressAfterLen);

            //获取此compressBeforeLen的offsets对应的所有byte[]
            Tuple2<List<byte[]>, Integer> tuple2 = getAllPartBytes(lun, metaIndex, meta, tmp);

            if (tuple2.var1.size() > 1) {
                tmp = bytesPartMerge(tuple2);
            }
            if (meta.getCompressState()[metaIndex] == 1) {
                tmp = CompressorUtils.uncompressData(tmp, compression);
            }
            res.addLast(tmp);
            lastReadSize -= canRead;
            curLen += canRead;

            if (curLen >= compressBeforeLen) {
                updateNextMetaLenIndex(meta.getCompressBeforeLen());
                curLen = 0;
            }
        }

        index += readSize;

        int compressionCurLenTemp = (int) compressCurLen;
        if (res.size() == 1) {
            if (compressionCurLenTemp == 0 && readSize == res.get(0).length) {
                responseFlux.onNext(DefaultPayload.create(res.pollFirst(), CONTINUE.name().getBytes()));
            } else {
                byte[] bytes = new byte[readSize];
                System.arraycopy(res.pollFirst(), compressionCurLenTemp, bytes, 0, readSize);
                responseFlux.onNext(DefaultPayload.create(bytes, CONTINUE.name().getBytes()));
            }
        } else {
            byte[] bytes = new byte[readSize];
            int curIndex = 0;

            int canCopySize = readSize;

            long indexReadLen;
            long indexCanCopySize;
            for (byte[] by : res) {
                if (compressionCurLenTemp > by.length) {
                    compressionCurLenTemp -= by.length;
                    continue;
                }
                indexReadLen = by.length - compressionCurLenTemp;
                indexCanCopySize = Math.min(canCopySize, indexReadLen);
                System.arraycopy(by, compressionCurLenTemp, bytes, curIndex, (int) indexCanCopySize);
                curIndex += indexCanCopySize;
                canCopySize -= indexReadLen;
                if (canCopySize <= 0) {
                    break;
                }
                compressionCurLenTemp = 0;
            }

            responseFlux.onNext(DefaultPayload.create(bytes, CONTINUE.name().getBytes()));
        }

        if (index == meta.getSize()) {
            responseFlux.onNext(SUCCESS_PAYLOAD);
            responseFlux.onComplete();
        }

    }

    protected void complete() throws IOException {
    }

    @Override
    public void timeOut() {
        if (StringUtils.isNotEmpty(path)){
            int hash = Math.abs(FileMeta.getKey(path).hashCode() % RUNNING_MAX_NUM);
            RUNNING[hash].decrementAndGet();
        }
    }

    private void updateNextMetaLenIndex(long[] lens) {
        for (int i = metaIndex + 1; i < lens.length; i++) {
            if (lens[i] != -1) {
                metaIndex = i;
                break;
            }
        }
    }


    //合并多个byte数组为一个byte数组
    public static byte[] bytesPartMerge(Tuple2<List<byte[]>, Integer> res) {
        byte[] data = new byte[res.var2];
        int partIndex = 0;
        for (byte[] bytes1 : res.var1) {
            System.arraycopy(bytes1, 0, data, partIndex, bytes1.length);
            partIndex += bytes1.length;
        }
        return data;
    }

    public static Tuple2<List<byte[]>, Integer> getAllPartBytes(String lun, int metaIndex, FileMeta meta, byte[] tmp) {
        List<byte[]> partList = new LinkedList<>();
        partList.add(tmp);
        int partSize = tmp.length;
        long[] beforeLen;
        long[] afterLen;
        boolean checkCrypto = CryptoUtils.checkCryptoEnable(meta.getCrypto());
        if (checkCrypto) {
            beforeLen = meta.getCryptoBeforeLen();
            afterLen = meta.getCryptoAfterLen();
        } else {
            beforeLen = meta.getCompressBeforeLen();
            afterLen = meta.getCompressAfterLen();
        }
        for (int next = metaIndex + 1; next < beforeLen.length; next++) {
            if (beforeLen[next] != -1) {
                break;
            }
            long offsetNext = meta.getOffset()[next];
            long afterLenNext = afterLen[next];
            byte[] tmpNext = BlockDevice.get(lun).read(offsetNext, (int) afterLenNext);
            partList.add(tmpNext);
            partSize += tmpNext.length;
        }
        return new Tuple2<>(partList, partSize);
    }
}
