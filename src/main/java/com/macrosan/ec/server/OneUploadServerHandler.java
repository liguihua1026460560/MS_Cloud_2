package com.macrosan.ec.server;

import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.fs.Allocator.Result;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.storage.compressor.CompressorUtils;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.storage.crypto.rootKey.RootSecretKeyUtils;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.ratelimiter.RecoverLimiter;
import io.rsocket.Payload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.UnicastProcessor;

import java.util.concurrent.atomic.AtomicReference;

import static com.macrosan.ec.Utils.ZERO_BYTES;
import static com.macrosan.ec.Utils.getCacheOrderKey;
import static com.macrosan.fs.BlockDevice.*;

@Log4j2
public class OneUploadServerHandler extends AioUploadServerHandler {
    OneUploadServerHandler(UnicastProcessor<Payload> responseFlux, boolean local) {
        super(responseFlux, local);
    }

    public void complete(byte[] bs) {
        try {
            fileSize = bs.length;
            checksumProvider.update(bs);

            AtomicReference<CompressionInfo> compressionInfo = new AtomicReference<>();
            long beforeLen = bs.length % 4096 == 0 ? bs.length : (bs.length / 4096 * 4096 + 4096);
            beforeCompressionLen += beforeLen;
            bs = dealCompressionBeforeWrite(bs, compressionInfo);

            AtomicReference<CryptoInfo> cryptoInfo0 = new AtomicReference<>();
            bs = dealCryptoBeforeWrite(bs, cryptoInfo0);

            String checksum = checksumProvider.getChecksum();
            checksumProvider.release();

            fileMeta.setEtag(checksum)
                    .setSize(fileSize);

            String fileMetaKey = FileMeta.getKey(fileName);

            byte[] finalBytes = bs;

            BatchRocksDB.RequestConsumer consumer = (db, writeBatch, request) -> {
                Result[] hookRes = writeBatch.hookData(finalBytes);
                Result[] res = hookRes;

                long[] offset = new long[res.length];
                long[] len = new long[res.length];
                long totalLen = 0L;
                for (int i = 0; i < res.length; i++) {
                    Result r = res[i];
                    offset[i] = r.offset;
                    len[i] = r.size;
                    totalLen += r.size;
                }

                dealCompressionAfterWrite(res, compressionInfo.get());
                dealCryptoAfterWrite(res, cryptoInfo0.get());

                fileMeta.setOffset(offset);
                fileMeta.setLen(len);

                boolean needCompression = false;
                if (CompressorUtils.checkCompressEnable(compression) && !compressionInfoList.isEmpty()) {
                    long[] compressStateArray = new long[res.length];
                    long[] compressBeforeLenArray = new long[res.length];
                    long[] compressAfterLenArray = new long[res.length];
                    int compressIndex = 0;
                    for (CompressionInfo compressInfo : compressionInfoList) {
                        if (!needCompression && compressInfo.comressionState[0] == 1) {
                            needCompression = true;
                        }
                        for (int j = 0; j < compressInfo.compressionAfterLen.length; j++) {
                            compressStateArray[compressIndex] = compressInfo.comressionState[j];
                            compressBeforeLenArray[compressIndex] = compressInfo.compressionBeforeLen[j];
                            compressAfterLenArray[compressIndex] = compressInfo.compressionAfterLen[j];
                            compressIndex++;
                        }
                    }
                    if (needCompression) {
                        fileMeta.setCompressState(compressStateArray);
                        fileMeta.setCompressAfterLen(compressAfterLenArray);
                        fileMeta.setCompressBeforeLen(compressBeforeLenArray);
                        fileMeta.setCompression(compression);
                    }
                }

                if (CryptoUtils.checkCryptoEnable(crypto)) {
                    long[] cryptoBeforeLenArray = new long[res.length];
                    long[] cryptoAfterLenArray = new long[res.length];
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

                if (needCompression) {
                    writeBatch.merge(ROCKS_COMPRESSION_FILE_SYSTEM_FILE_SIZE, BatchRocksDB.toByte(fileMeta.getSize() - compressionFileSize));
                    writeBatch.merge(ROCKS_BEFORE_COMPRESSION_FILE_SYSTEM_FILE_SIZE, BatchRocksDB.toByte(beforeCompressionLen));
                } else {
                    writeBatch.merge(ROCKS_BEFORE_COMPRESSION_FILE_SYSTEM_FILE_SIZE, BatchRocksDB.toByte(totalLen));
                }


                writeBatch.merge(ROCKS_FILE_SYSTEM_FILE_NUM, BatchRocksDB.toByte(1L));
                writeBatch.merge(ROCKS_FILE_SYSTEM_FILE_SIZE, BatchRocksDB.toByte(fileMeta.getSize()));
                writeBatch.merge(ROCKS_FILE_SYSTEM_USED_SIZE, BatchRocksDB.toByte(totalLen));

                writeBatch.put(fileMetaKey.getBytes(), Json.encode(fileMeta).getBytes());
                if (fileMeta.getFlushStamp() != null) {
                    writeBatch.put(getCacheOrderKey(fileMeta.getFlushStamp(), fileMeta.getFileName()).getBytes(), ZERO_BYTES);
                }
            };

            //写入FileMeta
            RecoverLimiter.getInstance().acquireData(lun, finalBytes.length, recover)
                    .flatMap(b -> BatchRocksDB.customizeOperateData(lun, fileMetaKey.hashCode(), consumer))
                    .doOnError(e -> {
//                            log.error("", e);
                        responseFlux.onNext(ERROR_PAYLOAD);
                        responseFlux.onComplete();
                    })
                    .subscribe(s -> {
                        responseFlux.onNext(SUCCESS_PAYLOAD);
                        responseFlux.onComplete();
                    });
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public void complete() {

    }


    @Override
    public void timeOut() {

    }
}
