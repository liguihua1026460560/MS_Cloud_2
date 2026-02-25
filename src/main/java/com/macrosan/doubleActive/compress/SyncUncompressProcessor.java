package com.macrosan.doubleActive.compress;

import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.storage.compressor.CompressorUtils;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import static com.macrosan.storage.coder.Limiter.DEFAULT_FETCH_AMOUNT;

@Log4j2
public class SyncUncompressProcessor {
    public static final byte[] SIGNAL = new byte[]{-128, 127, 54, 12, -21, 66, -5, 87, 45, 88};
    private MsHttpRequest request;
    private String syncCompress;
    public UnicastProcessor<byte[]> outputProcessor;


    public SyncUncompressProcessor(MsHttpRequest request, String syncCompress) {
        this.request = request;
        this.syncCompress = syncCompress;
        this.outputProcessor = UnicastProcessor.create(Queues.<byte[]>unboundedMultiproducer().get());

    }

    // 最大长度为dataByes+1
    byte[] afterBytes = new byte[SyncCompressProcessor.DEF_LENGTH + SIGNAL.length + 1];

    int cursor = 0;

    // 上游已经fetch的数量
    long fetchCount = DEFAULT_FETCH_AMOUNT;

    // limiter触发addFetchN的次数
    long backFetchCount = 0L;

    /**
     * 持续读取bytes直到读到SIGNAL
     */
    public void parse(byte[] bytes) {
        try {
//            log.info("bytes: {}, {}, {}", bytes.length, fetchCount, cursor);
            fetchCount--;
            tryFetch();
            for (int i = 0; i < bytes.length; i++) {
                byte current = bytes[i];
                if (checkSignal(current)) {
                    // 包含data和最后一位的压缩状态
                    byte[] formatBytes;
                    // 记录本段数据块是否压缩的索引位置
                    int stateIndex = cursor - 1 - (SIGNAL.length - 1);
                    formatBytes = new byte[stateIndex + 1];
                    System.arraycopy(afterBytes, 0, formatBytes, 0, stateIndex + 1);
                    outputProcessor.onNext(formatBytes);
                    afterBytes = new byte[SyncCompressProcessor.DEF_LENGTH + SIGNAL.length + 1];
                    cursor = 0;
                    // Limiter.addFetchN()中的fetchN每次encoder.put若只加一，tryFetch不会触发requeset.fetch()
                    // limiter触发request.fetch(16)需要encoder.put 16次
                    backFetchCount++;
                    if (backFetchCount >= DEFAULT_FETCH_AMOUNT) {
                        fetchCount += 16;
                        backFetchCount = 0;
                    }
                } else {
                    afterBytes[cursor] = current;
                    cursor++;
                }
            }
        } catch (Exception e) {
            log.error("parse err, ", e);
        }
    }

    /**
     * 扫到末尾的字节，判断前面的几位byte是否符合SIGNAL
     */
    public boolean checkSignal(byte current) {
        if (current == SIGNAL[SIGNAL.length - 1]) {
            for (int i = cursor - 1, j = SIGNAL.length - 2; j >= 0; i--, j--) {
                // afterBytes比signal更短，返回false
                if (i < 0) {
                    return false;
                }
                if (afterBytes[i] != SIGNAL[j]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public void tryFetch() {
        if (fetchCount <= 0) {
            request.fetch(DEFAULT_FETCH_AMOUNT);
            fetchCount += DEFAULT_FETCH_AMOUNT;
        }
    }

    /**
     * 处理压缩过且最后一位是压缩状态的bytes。
     *
     * @param bytes 并非是对象数据，最后1位是是否压缩(0 or 1)。长度为压缩后的对象数据+1
     * @return 解压后的对象数据
     */
    public byte[] uncompressBytes(byte[] bytes) {
        byte[] dstBytes;
        byte[] dataBytes = new byte[bytes.length - 1];
        System.arraycopy(bytes, 0, dataBytes, 0, dataBytes.length);
        if (bytes[bytes.length - 1] == 1) {
            try {
                dstBytes = CompressorUtils.uncompressData(dataBytes, syncCompress);
            } catch (Exception e) {
                log.error("uncompress sync compress request failed.", e);
                throw new RuntimeException("uncompress sync compress request failed.");
            }
        } else if (bytes[bytes.length - 1] == 0) {
            dstBytes = dataBytes;
        } else {
            throw new RuntimeException("compress state is not correct. " + bytes[bytes.length - 1]);
        }
        // 压缩数据、解压数据
//        log.info("====={}, {}, {}", bytes.length - 1, dstBytes.length, bytes[bytes.length - 1] == 1);
        return dstBytes;
    }

    public void close() {
        try {
            if (!this.outputProcessor.isDisposed()) {
                this.outputProcessor.cancel();
            }
        } catch (Exception e) {

        }
    }
}
