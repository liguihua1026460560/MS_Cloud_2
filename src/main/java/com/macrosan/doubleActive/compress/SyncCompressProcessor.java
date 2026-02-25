package com.macrosan.doubleActive.compress;


import com.macrosan.storage.compressor.CompressorUtils;
import com.macrosan.utils.functional.Tuple2;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.macrosan.doubleActive.compress.SyncUncompressProcessor.SIGNAL;

@Log4j2
public class SyncCompressProcessor {

    // 不能小于131072
    public static final int DEF_LENGTH = 1024 * 1024;

    // 输出的待压缩bytes的长度。待解压的bytes长度要长2位。
    public int standardLength;

    private String syncCompress;

    Function<Void, Boolean> fetchFunction;

    Mono<Boolean> completeSignal;

    /**
     * 输出固定长度的bytes
     */
    public UnicastProcessor<byte[]> outputProcessor;

    private byte[] afterBytes;

    private final LinkedList<byte[]> tmpBytesList;

    /**
     * 要输出的标准化bytes，目前被填充到的索引
     */
    private int cursor;

    private AtomicBoolean flushingTmp = new AtomicBoolean(false);

    public SyncCompressProcessor(int standardLength, String syncCompress, Function<Void, Boolean> fetchFunction, Mono<Boolean> completeSignal) {
        this.standardLength = standardLength;
        this.syncCompress = syncCompress;
        this.fetchFunction = fetchFunction;
        this.afterBytes = new byte[standardLength];
        this.tmpBytesList = new LinkedList<>();
        this.cursor = 0;
        this.outputProcessor = UnicastProcessor.create(Queues.<byte[]>unboundedMultiproducer().get());
        this.completeSignal = completeSignal;
        this.completeSignal.subscribe(b -> {
            // 保证最后在此进行一次flush，用来确认tempList被清空
            flushTemp();
            if (cursor != 0) {
                // 最后一个bytes长度小于afterBytes，需要将后面空的部分去掉
                byte[] lastBytes = new byte[cursor];
                System.arraycopy(afterBytes, 0, lastBytes, 0, cursor);
                outputProcessor.onNext(lastBytes);
            }
            outputProcessor.onComplete();
        });


    }

    /**
     * 生成固定长度的bytes[]，用于压缩。
     *
     * @param bytes 每次接收的字符串
     */
    public void formatBytesStream(byte[] bytes) {
        // 如果tempList不为空或flush正在进行，将bytes规范化后放入队列末尾
        // 注意，必须保证每次进来的bytes长度大部分时候比compressBytesLength小，否则可能会堆积。
        if (!tmpBytesList.isEmpty() || flushingTmp.get()) {
            int cursorA = 0;
            while (cursorA < bytes.length) {
                int remainLength = bytes.length - cursorA;
                int tmpBytesLength = Math.min(remainLength, standardLength);
                byte[] tmpBytes = new byte[tmpBytesLength];
                System.arraycopy(bytes, cursorA, tmpBytes, 0, tmpBytesLength);
                tmpBytesList.add(tmpBytes);
                cursorA += tmpBytesLength;
            }
            flushTemp();
            return;
        }

        // 如果tmpList不为空，不允许走到这里。
        if (bytes.length + cursor < standardLength) {
            System.arraycopy(bytes, 0, afterBytes, cursor, bytes.length);
            cursor += bytes.length;
            fetch();
        } else if (bytes.length + cursor == standardLength) {
            System.arraycopy(bytes, 0, afterBytes, cursor, bytes.length);
            outputProcessor.onNext(afterBytes);
            this.afterBytes = new byte[standardLength];
            cursor = 0;
        } else {
            // 超出了当前数组大小，将超出部分缓存进tmpList
            int cutLength = standardLength - cursor;
            int remainLength = bytes.length - cutLength;
            while (remainLength > 0) {
                int tmpBytesLength = Math.min(remainLength, standardLength);
                byte[] tmpBytes = new byte[tmpBytesLength];
                System.arraycopy(bytes, cutLength, tmpBytes, 0, tmpBytesLength);
                tmpBytesList.add(tmpBytes);
                remainLength -= tmpBytesLength;
            }
            System.arraycopy(bytes, 0, afterBytes, cursor, cutLength);
            outputProcessor.onNext(afterBytes);
            this.afterBytes = new byte[standardLength];
            cursor = 0;
            flushTemp();
        }
    }

    /**
     * 继续获取上游的bytes
     */
    public void fetch() {
        fetchFunction.apply(null);
    }

    /**
     * temp队列中的bytes下刷。该方法不允许同时运行。
     */
    public void flushTemp() {
        if (flushingTmp.compareAndSet(false, true)) {
            while (!tmpBytesList.isEmpty()) {
                byte[] poll = tmpBytesList.poll();
                if (cursor + poll.length < standardLength) {
                    System.arraycopy(poll, 0, afterBytes, cursor, poll.length);
                    cursor += poll.length;
                } else if (cursor + poll.length == standardLength) {
                    System.arraycopy(poll, 0, afterBytes, cursor, poll.length);
                    outputProcessor.onNext(afterBytes);
                    this.afterBytes = new byte[standardLength];
                    cursor = 0;
                } else {
                    int cutLength = standardLength - cursor;
                    int remainLength = poll.length - cutLength;
                    System.arraycopy(poll, 0, afterBytes, cursor, cutLength);
                    outputProcessor.onNext(afterBytes);
                    this.afterBytes = new byte[standardLength];
                    cursor = 0;
                    // 多出部分放到tmpList队列头部
                    byte[] remainBytes = new byte[remainLength];
                    System.arraycopy(poll, cutLength, remainBytes, 0, remainLength);
                    tmpBytesList.addFirst(remainBytes);
                }
            }
            flushingTmp.compareAndSet(true, false);
        }
    }


    /**
     * 将标准长度的byte[]压缩，末尾添加压缩结果和SIGNAL
     *
     * @param bytes 标准长度的原始数据的bytes，注意末尾的bytes并非标准长度
     * @return 标准长度字符串压缩后，末尾多一位+SIGNAL
     */
    public byte[] compressBytes(byte[] bytes) {
        byte[] res;
        try {
            Tuple2<byte[], Boolean> tuple2 = CompressorUtils.compressDataCheckRatio(bytes, syncCompress, 1);
            byte[] bytesAfter = tuple2.var1;
            Boolean compressed = tuple2.var2;
            res = new byte[bytesAfter.length + 1 + SIGNAL.length];
            System.arraycopy(bytesAfter, 0, res, 0, bytesAfter.length);
            if (compressed) {
                res[bytesAfter.length] = 1;
            } else {
                res[bytesAfter.length] = 0;
            }
            System.arraycopy(SIGNAL, 0, res, bytesAfter.length + 1, SIGNAL.length);
            // 原始数据，压缩后的数据，添加status和signal后的压缩数据
//            log.info("+++++{}, {}, {}, {}", bytes.length, bytesAfter.length, res.length, compressed);
            countCompressLength.addAndGet(bytes.length - res.length);

        } catch (Exception e) {
            log.error("dealBytesStream failed", e);
            throw new RuntimeException("dealBytesStream failed");
        }
        return res;
    }

    public static AtomicLong countCompressLength = new AtomicLong();


    public void close() {
        try {
            if (!this.outputProcessor.isDisposed()) {
                this.outputProcessor.dispose();
            }
        } catch (Exception e) {

        }
    }

}
