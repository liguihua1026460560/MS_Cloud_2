package com.macrosan.fs;

import com.macrosan.constants.ErrorNo;
import com.macrosan.fs.Allocator.Result;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import com.macrosan.utils.msutils.UnsafeUtils;
import io.netty.buffer.ByteBuf;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.utils.msutils.UnsafeUtils.unsafe;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class AioChannel {
    static {
        try {
            System.load("/moss/jni/libafile.so");
        } catch (Throwable e) {
            log.error("load file library fail", e);
            System.exit(0);
        }
    }

    private static Map<Long, MonoProcessor<Boolean>> map = new ConcurrentHashMap<>();
    private static Map<Long, MonoProcessor<byte[]>> readMap = new ConcurrentHashMap<>();
    private static AtomicLong id = new AtomicLong(0);

    private BlockDevice device;
    private int fd;

    private long ctxAddr;
    private long eventAddr;
    private long readEventAddr;
    private long readCtxAddr;

    private Thread aioThread;
    private Thread readThread;

    private final static ThreadFactory AIO_THREAD_FACTORY = new MsThreadFactory("aio");
    public static final Scheduler AIO_SCHEDULER;
    public static long addrOffset;

    public static long getBufferAddress(ByteBuffer buffer) {
        return unsafe.getLong(buffer, AioChannel.addrOffset) + buffer.position();
    }

    static {
        Scheduler scheduler = null;
        try {
            MsExecutor executor = null;
            if (ServerConfig.isUnify() && PROC_NUM < 8) {
                executor = new MsExecutor(PROC_NUM, 1, AIO_THREAD_FACTORY);
            } else {
                executor = new MsExecutor(PROC_NUM, 8, AIO_THREAD_FACTORY);
            }
            scheduler = Schedulers.fromExecutor(executor);

            Class ByteBuffer = Class.forName("java.nio.Buffer");
            Field field = ByteBuffer.getDeclaredField("address");
            addrOffset = UnsafeUtils.unsafe.objectFieldOffset(field);
        } catch (Exception e) {
            log.error("", e);
        }

        AIO_SCHEDULER = scheduler;
    }

    AioChannel(BlockDevice device) {
        this.device = device;
        this.fd = open(device.getPath());
        ctxAddr = ioContext();
        eventAddr = ioEvent();
        readEventAddr = ioEvent();
        readCtxAddr = ioContext();

        aioThread = new Thread(() -> {
            while (true) {
                try {
                    long[] completedId = getComplete(ctxAddr, eventAddr);
                    if (null != completedId) {
                        for (long id : completedId) {
                            Mono.just(true).publishOn(DISK_SCHEDULER).subscribe(b -> map.remove(id).onNext(b));
                        }
                    }
                } catch (Exception e) {
                    log.error("write thread error", e);
                }
            }
        });

        aioThread.start();


        readThread = new Thread(() -> {
            while (true) {
                try {
                    long[] completedId = getReadComplete(readCtxAddr, readEventAddr);
                    if (null != completedId) {
                        long head;
                        for (int i = 0; i < completedId.length / 3; i++) {
                            int finalI = i * 3;
                            long addr = completedId[finalI + 1];
                            head = addr;
                            long size = completedId[finalI + 2];
                            //size:  -1:全部数据读取失败， -2：部分数据读取失败  非负数:数据读取成功
                            if (size < 0) {
                                String msg = "";
                                if (size == -1) {
                                    msg = "read data error";
                                } else {
                                    msg = "read len error";
                                    free0(addr);
                                }
                                if (readMap.get(completedId[finalI]) != null) {
                                    String finalMsg = msg;
                                    Mono.just(false).publishOn(DISK_SCHEDULER).subscribe(b -> readMap.remove(completedId[finalI]).onError(new MsException(ErrorNo.UNKNOWN_ERROR, finalMsg)));
                                }
                            } else {
                                byte[] bytes = new byte[(int) size];
                                UnsafeUtils.unsafe.copyMemory(null, addr, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, size);
                                free0(head);
                                if (readMap.get(completedId[finalI]) != null) {
                                    Mono.just(true).publishOn(DISK_SCHEDULER).subscribe(b -> readMap.remove(completedId[finalI]).onNext(bytes));
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("read thread error", e);
                }
            }
        });
        readThread.start();
    }

    public Mono<Result[]> write(byte[] bytes) {
        //申请到的逻辑空间，每一个元素都是一个逻辑空间块
        Result[] allocRes = device.alloc(bytes.length);
        return write(bytes, allocRes);
    }

    public Mono<Result[]> write(ByteBuf buf, Result... allocRes) {
        if (buf.isDirect()) {
            ByteBuffer[] nioBuffers = buf.nioBuffers();
            if (nioBuffers.length != 1) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "buf is not direct");
            }

            long addr = buf.memoryAddress() + buf.readerIndex();
            int size = (buf.readableBytes() + 4095) & ~4095;
            if (size > buf.capacity() - buf.readerIndex()) {
                byte[] bytes = new byte[buf.readableBytes()];
                buf.readBytes(bytes);
                return write(bytes, allocRes);
            }

            MonoProcessor<Result[]> res = MonoProcessor.create();
            Flux<Boolean> flux = Flux.empty();

            for (int i = 0; i < allocRes.length; i++) {
                Result alloc = allocRes[i];
                MonoProcessor<Boolean> processor = MonoProcessor.create();
                long id = AioChannel.id.incrementAndGet();
                flux = flux.concatWith(processor);
                map.put(id, processor);

                int ret;

                int writeLen = Math.min(size, (int) alloc.size);
                long[] addrArray = new long[]{addr};
                int[] sizeArray = new int[]{writeLen};

                for (int tryNum = 0; tryNum < 10; tryNum++) {
                    if ((ret = directWrite(id, ctxAddr, fd, alloc.offset, addrArray, sizeArray, 1)) == 1) {
                        break;
                    }
                    log.error("submit write error {} in {}", ret, device.getName());
                    synchronized (Thread.currentThread()) {
                        try {
                            Thread.currentThread().wait(100);
                        } catch (InterruptedException e) {
                            log.error("", e);
                        }
                    }

                    if (tryNum == 9) {
                        map.remove(id);
                        throw new UnsupportedOperationException("submit write error " + ret);
                    }
                }

                addr += writeLen;
                size -= writeLen;
            }

            flux.doOnComplete(() -> res.onNext(allocRes))
                    .doOnError(res::onError)
                    .subscribe();

            return res;
        } else {
            return write(buf.nioBuffer().array(), allocRes);
        }
    }

    /**
     * 根据分配到的空间调用jni接口进行落盘
     *
     * @param bytes 待落盘的字节数组，大小约为MIN_ALLOC_SIZE
     * @return alloc的结果，写入partInfo的offset和size
     */
    public Mono<Result[]> write(byte[] bytes, Result... allocRes) {
        //逻辑空间中的位置
        int offset = 0;
        MonoProcessor<Result[]> res = MonoProcessor.create();
        Flux<Boolean> flux = Flux.empty();

        for (int i = 0; i < allocRes.length; i++) {
            Result alloc = allocRes[i];
            MonoProcessor<Boolean> processor = MonoProcessor.create();
            long id = AioChannel.id.incrementAndGet();
            flux = flux.concatWith(processor);
            map.put(id, processor);

//            AioUploadServerHandler.allocAmount.getAndAdd(alloc.size);

            int ret;
            for (int tryNum = 0; tryNum < 10; tryNum++) {
                //bytes.length + offset 不能比 alloc.size 大，否则会造成数组越界
//                log.info("check---- {} {} {}", bytes.length, offset, alloc.size);
                int writeLen = (int) Math.min(bytes.length - offset, alloc.size);
                if ((ret = write(id, ctxAddr, fd, alloc.offset, bytes, offset, writeLen)) == 1) {
                    break;
                }
                log.error("submit write error {} in {}", ret, device.getName());
                synchronized (Thread.currentThread()) {
                    try {
                        Thread.currentThread().wait(100);
                    } catch (InterruptedException e) {
                        log.error("", e);
                    }
                }

                if (tryNum == 9) {
                    map.remove(id);
                    throw new UnsupportedOperationException("submit write error " + ret);
                }
            }

            offset += alloc.size;
        }

        flux.doOnComplete(() -> res.onNext(allocRes))
                .doOnError(res::onError)
                .subscribe();

        return res;
    }

    public Mono<byte[]> read(long offset, int len) {
        long id = AioChannel.id.incrementAndGet();
        MonoProcessor<byte[]> res = MonoProcessor.create();
        readMap.put(id, res);
        int readRes;
        for (int tryNum = 0; tryNum < 10; tryNum++) {
            readRes = read(id, readCtxAddr, fd, offset, len);
            if (readRes == 1) {
                break;
            }
            log.error("submit read error {} in {}", readRes, device.getName());
            if (tryNum == 9) {
                readMap.remove(id);
                throw new UnsupportedOperationException("submit read error");
            }
            synchronized (Thread.currentThread()) {
                try {
                    Thread.currentThread().wait(100);
                } catch (InterruptedException e) {
                    log.error("", e);
                }
            }
        }
        return res;
    }

    public static native int open(String path);

    public static native long ioContext();

    public static native long ioEvent();

    public static native int write(long id, long ctxAddr, int fd, long offset, byte[] bytes, int bytesOffset,
                                   int len);

    public static native int read(long id, long ctxAddr, int fd, long offset, int len);

    private static native long[] getComplete(long ctxAddr, long eventAddr);

    private static native long[] getReadComplete(long ctxAddr, long eventAddr);

    public static native void free0(long addr);

    public static native int directWrite(long id, long ctxAddr, int fd, long offset,
                                         long[] addr, int[] size, int num);
}
