package com.macrosan.ec.server;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.fs.BlockDevice;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.LocalPayload;
import com.macrosan.storage.compressor.CompressorUtils;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.ec.server.ErasureServer.ERROR_PAYLOAD;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.storage.compressor.CompressorUtils.checkCompressEnable;
import static com.macrosan.storage.crypto.CryptoUtils.checkCryptoEnable;

@Log4j2
public class ReadObjServer {
    public static final Logger rebuildLog = LogManager.getLogger("RebuildLog.ReadObj");
    public static final ByteBuf SUCCESS_BYTE_BUF = Unpooled.wrappedBuffer(SUCCESS.name().getBytes());

    //TODO 支持加密
    public static Mono<Payload> readObj(Payload payload) {
        try {
            boolean local = payload instanceof LocalPayload;
            SocketReqMsg msg = local ? ((LocalPayload<SocketReqMsg>) payload).data : Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);

            String fileName = msg.get("fileName");
            String lun = msg.get("lun");
            MSRocksDB rocksDB = MSRocksDB.getRocksDB(lun);
            if (rocksDB == null) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "get " + lun + " rocks db fail");
            }

            int splitIndex = fileName.indexOf(File.separator);
            String path = fileName.substring(splitIndex);
            byte[] value = rocksDB.get(FileMeta.getKey(path).getBytes());
            if (null == value) {
                throw new NoSuchFileException("no such file " + path);
            }

            FileMeta meta = Json.decodeValue(new String(value), FileMeta.class);
            long start = Long.parseLong(msg.get("start"));
            long end = Long.parseLong(msg.get("end"));

            int metaIndex = 0;
            long skip = start;
            long[] lens = getBeforeLens(meta);

            String compression = meta.getCompression();
            boolean compressEnable = checkCompressEnable(compression);
            CompressorUtils.updateCompressStateIfNull(meta);

            int metaOff = 0;

            //跳过start之前的数据
            while (skip > 0) {
                int len = (int) lens[metaIndex];
                if (skip >= len) {
                    metaIndex++;
                    while (metaIndex < lens.length && lens[metaIndex] == -1) {
                        metaIndex++;
                    }

                    metaOff = 0;
                    skip -= len;
                } else {
                    metaOff = (int) skip;
                    skip -= metaOff;
                }
            }

            long readIndex = start;
            LinkedList<Mono<Tuple2<Integer, byte[]>>> res = new LinkedList<>();
            Map<Integer, Tuple2<Integer, Integer>> compressAfterOffsets = new ConcurrentHashMap<>();
            Map<Integer, Tuple2<Integer, Integer>> unCompressAfterOffsets = new ConcurrentHashMap<>();
            do {
                long readSize;
                //如果当前块存在压缩部分则需要完整读整个块
                if (CompressorUtils.checkCompressEnable(meta.getCompression())) {
                    readSize = (int) lens[metaIndex];
                } else {
                    readSize = (int) Math.min(end - readIndex, lens[metaIndex] - metaOff);
                }
                List<Mono<byte[]>> partRes = new LinkedList<>();
                int headMetaIndex = metaIndex;

                Mono<byte[]> mono = readOnce(lun, meta, metaIndex, metaOff, readIndex, end);

                metaIndex++;

                partRes.add(mono);
                long[] afterLen = getAfterLens(meta);


                while (metaIndex < lens.length && lens[metaIndex] == -1) {
                    partRes.add(readPart(lun, meta, metaIndex, afterLen));
                    metaIndex++;
                }

                Mono<byte[]> merge = Flux.concat(partRes).collectList().map(bytesList -> {
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
                });
                int i = res.size();
                res.add(merge.map(b -> new Tuple2<>(i, b)));
                if (compressEnable) {
                    if (meta.getCompressState()[headMetaIndex] == 1) {
                        //读被压缩部分
                        int len = (int) Math.min(end - readIndex, meta.getCompressBeforeLen()[headMetaIndex] - metaOff);
                        readSize = len;
                        compressAfterOffsets.put(i, new Tuple2<>(metaOff, len));
                    } else {
                        //读未压缩部分
                        int len = (int) Math.min(end - readIndex, meta.getCompressBeforeLen()[headMetaIndex] - metaOff);
                        readSize = len;
                        unCompressAfterOffsets.put(i, new Tuple2<>(metaOff, len));
                    }
                }
                metaOff = 0;
                readIndex += readSize;
            } while (metaIndex < lens.length && readIndex < end);
            log.debug("readSize:{}, start:{}, end:{}, needRead:{}, lun:{}, metaIndex:{}", readIndex - start, start, end, end - start, lun, metaIndex);

            return Flux.merge(Flux.fromStream(res.stream()))
                    .flatMap(tuple2 -> {
                        int i = tuple2.var1;
                        byte[] bytes = tuple2.var2;
                        Tuple2<Integer, Integer> sub = compressAfterOffsets.get(i);
                        if (sub != null) {
                            try {
                                bytes = CompressorUtils.uncompressData(bytes, compression);
                                bytes = Arrays.copyOfRange(bytes, sub.var1, sub.var1 + sub.var2);
                            } catch (IOException e) {
                                return Mono.error(e);
                            }
                        } else {
                            Tuple2<Integer, Integer> sub1 = unCompressAfterOffsets.get(i);
                            if (sub1 != null) {
                                bytes = Arrays.copyOfRange(bytes, sub1.var1, sub1.var1 + sub1.var2);
                            }
                        }
                        return Mono.just(new Tuple2<>(i, bytes));
                    })
                    .collect(() -> new byte[res.size()][], (array, b) -> {
                        array[b.var1] = b.var2;
                    })
                    .map(bytesArray -> {
                        ByteBuf buf = Unpooled.wrappedBuffer(bytesArray);
                        log.debug("lun:{}, actual read size:{}, start:{}, end:{}, needRead:{}，res:{}", lun, Arrays.stream(bytesArray).mapToLong(bytes -> bytes.length).sum(), start, end, end - start, Arrays.stream(bytesArray).mapToLong(bytes -> bytes.length).sum() == (end - start));
                        if (local) {
                            return new LocalPayload<>(SUCCESS, bytesArray);
                        } else {
                            return DefaultPayload.create(buf, SUCCESS_BYTE_BUF);
                        }
                    })
                    .onErrorResume(e -> {
                        log.error("", e);
                        return Mono.just(ERROR_PAYLOAD);
                    });
        } catch (Exception e) {
            if (e instanceof NoSuchFileException) {
                rebuildLog.error(e);
            } else {
                log.error("{}", e.getMessage());
            }

            return Mono.just(ERROR_PAYLOAD);
        }
    }

    private static Mono<byte[]> readOnce(String lun, FileMeta meta, int metaIndex, int metaOff, long readIndex, long end) {
        BlockDevice device = BlockDevice.get(lun);
        long[] offsets = meta.getOffset();
        long[] lens = getAfterLens(meta);
        int readSize;
        long curReadOff;
        if (CompressorUtils.checkCompressEnable(meta.getCompression())) {
            curReadOff = offsets[metaIndex];
            readSize = (int) lens[metaIndex];
        } else {
            readSize = (int) Math.min(end - readIndex, lens[metaIndex] - metaOff);
            curReadOff = offsets[metaIndex] + metaOff;
        }
        Mono<byte[]> mono;
        if (curReadOff % 512 != 0) {
            long tmpOff = curReadOff / 512 * 512;
            int overReadSize = (int) Math.max(0, (curReadOff - tmpOff));
            mono = device.channel.read(tmpOff, overReadSize + readSize)
                    .map(b -> {
                        int from = (int) (curReadOff - tmpOff);
                        return Arrays.copyOfRange(b, from, from + readSize);
                    });
        } else {
            mono = device.channel.read(curReadOff, readSize);
        }
        return mono;
    }

    private static Mono<byte[]> readPart(String lun, FileMeta meta, int metaIndex, long[] afterLen) {
        BlockDevice device = BlockDevice.get(lun);

        long offset = meta.getOffset()[metaIndex];
        int len = (int) afterLen[metaIndex];

        if (len > 0) {
            return device.channel.read(offset, len);
        } else {
            return Mono.empty();
        }
    }

    private static long[] getBeforeLens(FileMeta meta) {
        long[] beforeLens;
        if (checkCompressEnable(meta.getCompression())) {
            beforeLens = meta.getCompressBeforeLen();
        } else if (checkCryptoEnable(meta.getCrypto())) {
            beforeLens = meta.getCryptoBeforeLen();
        } else {
            beforeLens = meta.getLen();
        }

        return beforeLens;
    }

    private static long[] getAfterLens(FileMeta meta) {
        long[] lens;

        if (checkCryptoEnable(meta.getCrypto())) {
            lens = meta.getCryptoAfterLen();
        } else if (checkCompressEnable(meta.getCompression())) {
            lens = meta.getCompressAfterLen();
        } else {
            lens = meta.getLen();
        }

        return lens;
    }
} 
