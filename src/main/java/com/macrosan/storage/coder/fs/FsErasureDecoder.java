package com.macrosan.storage.coder.fs;

import com.macrosan.constants.ErrorNo;
import com.macrosan.filesystem.utils.ReadObjClient;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.codec.ErasureCodc;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.storage.coder.fs.FsReplicaDecoder.EMPTY_SUBSCRIPTION;

@Log4j2
public class FsErasureDecoder extends Flux<Tuple2<Integer, byte[]>> {
    int readIndex;
    StoragePool pool;
    String fileName;
    long offset;
    long size;
    long fileSize;
    List<Tuple3<String, String, String>> nodeList;
    AtomicInteger successNum = new AtomicInteger();
    byte[][][] res;
    CoreSubscriber<? super Tuple2<Integer, byte[]>> actual;
    AtomicBoolean done = new AtomicBoolean();
    boolean isPrefetch = false;

    public FsErasureDecoder(int readIndex, StoragePool pool, String fileName, long offset, long size, long fileSize, boolean isPrefetch) {
        this.readIndex = readIndex;
        this.pool = pool;
        this.fileName = fileName;
        this.offset = offset;
        this.size = size;
        this.fileSize = fileSize;
        this.isPrefetch = isPrefetch;
        nodeList = pool.mapToNodeInfo(pool.getObjectVnodeId(fileName)).block();
        res = new byte[pool.getK() + pool.getM()][][];
    }


    @Override
    public void subscribe(CoreSubscriber<? super Tuple2<Integer, byte[]>> actual) {
        this.actual = actual;
        actual.onSubscribe(EMPTY_SUBSCRIPTION);
        read();
        //TODO处理数据块缺失的情况
    }

    private void read() {
        int k = pool.getK();
        long packageSize = pool.getPackageSize();

        int packageIndex = (int) (offset / packageSize / k);

        long[] readOff = new long[k];
        long[] readSize = new long[k];
        List<Integer>[] readSizeList = new List[k];

        long startOff = packageIndex * packageSize;
        Arrays.fill(readOff, startOff);

        long curPackageOff = packageIndex * packageSize * k;
        long curPackageSize = Math.min(fileSize - curPackageOff, packageSize * k);
        long curPeerSize = curPackageSize == packageSize * k ? packageSize : (curPackageSize / 64 / k * 64 + 64);
        long curReadStart = offset;
        long curReadEnd = Math.min(curPeerSize * k + curPackageOff, size + offset);


        long offIndex = curPackageOff;
        for (int i = 0; i < k; i++) {
            long o = Math.min(offset - offIndex, curPeerSize);
            readOff[i] += o;
            offIndex += o;
            readSizeList[i] = new LinkedList<>();
        }

        while (curReadEnd > curReadStart) {
            for (int i = 0; i < k; i++) {
                long curPeerEnd = Math.min(curPackageOff + (i + 1) * curPeerSize, curReadEnd);
                long curPeerStart = Math.max(curPackageOff + i * curPeerSize, curReadStart);
                long curPeerReadSize = Math.max(curPeerEnd - curPeerStart, 0);
                readSize[i] += curPeerReadSize;
                curReadStart += curPeerReadSize;


                readSizeList[i].add((int) curPeerReadSize);
            }

            packageIndex++;
            curPackageOff = packageIndex * packageSize * k;
            curPackageSize = Math.min(fileSize - curPackageOff, packageSize * k);
            curPeerSize = curPackageSize == packageSize * k ? packageSize : (curPackageSize / 64 / k * 64 + 64);
            curReadStart = curPackageOff;
            curReadEnd = Math.min(curPeerSize * k + curPackageOff, size + offset);
        }

        int needRead = (int) Arrays.stream(readSize).filter(l -> l > 0).count();

        for (int i = 0; i < k; i++) {
            if (readSize[i] > 0) {
                int resIndex = i;
                tryFastRead(i, fileName, readOff[i], readOff[i] + readSize[i])
                        .subscribe(t -> {
                            if (t.var1) {
                                res[resIndex] = t.var2.toArray(new byte[0][]);

                                if (successNum.incrementAndGet() == needRead) {
                                    if (done.compareAndSet(false, true)) {
                                        if (successNum.get() == 1) {
                                            for (byte[] bytes : t.var2) {
                                                actual.onNext(new Tuple2<>(readIndex, bytes));
                                                readIndex += bytes.length;
                                            }
                                        } else {
                                            byte[] bytes = new byte[(int) size];
                                            int[][] sizes = new int[k][];
                                            for (int p = 0; p < k; p++) {
                                                sizes[p] = readSizeList[p].stream().mapToInt(l -> l).toArray();
                                            }

                                            int bytesOff = 0;
                                            //TODO 直接从res处拷贝，减少tmpRes的这个一次拷贝
                                            byte[][] tmpRes = new byte[k][];
                                            for (int p = 0; p < k; p++) {
                                                if (res[p] != null) {
                                                    if (res[p].length == 1) {
                                                        tmpRes[p] = res[p][0];
                                                    } else {
                                                        tmpRes[p] = new byte[(int) readSize[p]];
                                                        int o = 0;
                                                        for (byte[] b : res[p]) {
                                                            System.arraycopy(b, 0, tmpRes[p], o, b.length);
                                                            o += b.length;
                                                        }
                                                    }
                                                }
                                            }

                                            int[] resOff = new int[res.length];

                                            for (int p = 0; p < sizes[0].length; p++) {
                                                for (int q = 0; q < k; q++) {
                                                    if (sizes[q][p] > 0) {
                                                        try {
                                                            System.arraycopy(tmpRes[q], resOff[q], bytes, bytesOff, sizes[q][p]);
                                                        } catch (Exception e) {
                                                            log.error("", e);
                                                        }
                                                        bytesOff += sizes[q][p];
                                                        resOff[q] += sizes[q][p];
                                                    }
                                                }
                                            }

                                            actual.onNext(new Tuple2<>(readIndex, bytes));
                                        }

                                        actual.onComplete();
                                    }
                                }
                            } else {
                                if (done.compareAndSet(false, true)) {
                                    if (isPrefetch) {
                                        actual.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "pre-read data modified(erasure)- " + fileName));
                                    } else {
                                        log.error("normal read object {} fail", fileName);
                                        actual.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "read object fail"));
                                    }
                                }
                            }
                        }, e -> {
                            if (done.compareAndSet(false, true)) {
                                log.error("", e);
                                actual.onError(e);
                            }
                        });
            }
        }
    }

    private Mono<Tuple2<Boolean, List<byte[]>>> tryFastRead(int index, String fileName, long start, long end) {
        MonoProcessor<Tuple2<Boolean, List<byte[]>>> res = MonoProcessor.create();

        Tuple3<String, String, String> tuple3 = nodeList.get(index);

        ReadObjClient.readOnce(tuple3.var1, tuple3.var2, fileName, start, end)
                .subscribe(t -> {
                    if (t.var1) {
                        res.onNext(t);
                    } else {
                        //原始数据块读失败，从其他数据块恢复
                        tryPackageRead(index, fileName, start, end, res);
                    }
                });

        return res;
    }

    private static class PackageReadTask {
        int src;
        long start;
        long end;
        MonoProcessor<Tuple2<Boolean, List<byte[]>>> res;
        String fileName;
        int k;
        int m;
        Queue<Integer> indexQueue = new ConcurrentLinkedQueue<>();
        long start0;
        long end0;
        List<byte[]>[] readRes;
        StoragePool pool;

        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger totalEnd = new AtomicInteger();


        PackageReadTask(int src, long start, long end, MonoProcessor<Tuple2<Boolean, List<byte[]>>> res, String fileName, StoragePool pool) {
            this.src = src;
            this.start = start;
            this.end = end;
            this.res = res;
            this.fileName = fileName;
            start0 = start / 64 * 64;
            end0 = (end + 63) / 64 * 64;
            this.pool = pool;
            this.k = pool.getK();
            this.m = pool.getM();
            readRes = new List[k + m];
        }

        private void onSuccess(int index, List<byte[]> data) {
            readRes[index] = data;
            if (success.incrementAndGet() == k) {
                ErasureCodc codc = pool.getCodc();

                int[] codeIndex = new int[k + m];
                byte[][] srcData = new byte[k][];
                int srcIndex = 0;

                for (int i = 0; i < readRes.length; i++) {
                    if (readRes[i] != null) {
                        codeIndex[i] = 1;
                        byte[] bytes;
                        if (readRes[i].size() == 1) {
                            bytes = readRes[i].get(0);
                        } else {
                            bytes = new byte[(int) (end0 - start0)];
                            int off = 0;
                            for (byte[] slice : readRes[i]) {
                                System.arraycopy(slice, 0, bytes, off, slice.length);
                                off += slice.length;
                            }

                            if (off != bytes.length) {
                                log.info("read size error");
                                res.onNext(new Tuple2<>(false, null));
                                return;
                            }
                        }

                        srcData[srcIndex++] = bytes;
                    } else {
                        codeIndex[i] = 0;
                    }
                }


                byte[][] dstData = codc.fsDecode(srcData, codeIndex);

                byte[] realData;
                if (start == start0 && end0 == end) {
                    realData = dstData[src];
                } else {
                    realData = new byte[(int) (end - start)];
                    int off = (int) (start - start0);
                    int size = (int) (end - start);
                    System.arraycopy(dstData[src], off, realData, 0, size);
                }
                res.onNext(new Tuple2<>(true, Collections.singletonList(realData)));
            } else if (totalEnd.incrementAndGet() == k) {
                res.onNext(new Tuple2<>(false, null));
            }
        }

        private void onFail(int index) {
            if (totalEnd.incrementAndGet() == k) {
                res.onNext(new Tuple2<>(false, null));
            }
        }
    }

    private void tryPackageRead(int src, String fileName, long start, long end, MonoProcessor<Tuple2<Boolean, List<byte[]>>> res) {
        PackageReadTask task = new PackageReadTask(src, start, end, res, fileName, pool);
        for (int i = 0; i < nodeList.size(); i++) {
            if (i == src) {
                continue;
            }
            task.indexQueue.add(i);
        }

        for (int i = 0; i < pool.getK(); i++) {
            Integer index = task.indexQueue.poll();
            if (index != null) {
                read(index, task);
            }
        }
    }

    private void read(int index, PackageReadTask task) {
        Tuple3<String, String, String> tuple3 = nodeList.get(index);

        ReadObjClient.readOnce(tuple3.var1, tuple3.var2, fileName, task.start0, task.end0)
                .subscribe(t -> {
                    if (t.var1) {
                        task.onSuccess(index, t.var2);
                    } else {
                        //原始数据块读失败，从其他数据块恢复
                        Integer next = task.indexQueue.poll();
                        if (next != null) {
                            read(next, task);
                        } else {
                            task.onFail(index);
                        }
                    }
                });
    }
}
