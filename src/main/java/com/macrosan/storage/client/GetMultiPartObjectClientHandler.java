package com.macrosan.storage.client;

import com.macrosan.doubleActive.MsClientRequest;
import com.macrosan.ec.ECUtils;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple3;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.macrosan.constants.ServerConstants.IS_SYNCING;

/**
 * @author gaozhiyuan
 * 下载分段上传对象的处理类
 */
@Log4j2
public class GetMultiPartObjectClientHandler {
    private long controlStreamCount = 0L;
    private long returnStreamCount = 0L;
    private long partStart;
    private long partEnd;

    private static class Controller {
        UnicastProcessor<Long> last;
        UnicastProcessor<Long> cur;
        long streamWaitNum = -1L;

        Controller(UnicastProcessor<Long> cur) {
            this.cur = cur;
            last = null;
        }
    }

    private volatile Controller partStreamController;

    private static AtomicReferenceFieldUpdater<GetMultiPartObjectClientHandler,
            Controller> UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(GetMultiPartObjectClientHandler.class, Controller.class, "partStreamController");

    private UnicastProcessor<byte[]> res = UnicastProcessor.create();

    public Flux<byte[]> getBytes() {
        return res;
    }

    private Disposable transformDisposable;

    /**
     * 转换控制流可以被多次subscribe
     * 同时完成一个分段的读取后
     * 这个分段后续的stream不再向后转发
     * 不影响下一个分段的流量控制
     *
     * @param streamController 原始的控制流
     * @return
     */
    private void transformStreamController(Flux<Long> streamController) {
        transformDisposable = streamController.subscribe(l -> {
            controlStreamCount++;
            Controller curPartStreamController = UPDATER.get(this);

            if (curPartStreamController.streamWaitNum < controlStreamCount) {
                curPartStreamController.cur.onNext(l);
            } else {
                curPartStreamController.last.onNext(l);
            }
        });
    }

    /**
     * @param start            起点
     * @param end              文件终点
     * @param partInfos        分段信息数组
     * @param streamController 对象数据流控制器
     */
    public GetMultiPartObjectClientHandler(StoragePool storagePool, long start, long end, long fileSize, PartInfo[] partInfos,
                                           Flux<Long> streamController, MsHttpRequest request, MsClientRequest clientRequest) {

        partStart = start;
        partEnd = end;
        AtomicLong readSum = new AtomicLong();
        AtomicLong len = new AtomicLong();
        UnicastProcessor<Long> streamControllerCache = UnicastProcessor.create();
        UPDATER.set(this, new Controller(streamControllerCache));

        transformStreamController(streamController);

        UnicastProcessor<Integer> partControlSignal = UnicastProcessor.create();
        partControlSignal.onNext(0);

        MsHttpRequest msHttpRequest = request;
        if (request != null && (StringUtils.isNotEmpty(request.getSyncTag()) && request.getSyncTag().equals(IS_SYNCING))) {
            msHttpRequest = null;
        }

        Disposable[] disposables = new Disposable[2];
        disposables[0] = partControlSignal.subscribe(partNum -> {
            if ((partNum >= partInfos.length || partEnd < 0) || (readSum.get() >= fileSize)) {
                res.onComplete();
                return;
            }
            if (partInfos[partNum].partSize <= partStart) {
                partStart -= partInfos[partNum].partSize;
                partEnd -= partInfos[partNum].partSize;
                partControlSignal.onNext(partNum + 1);
                return;
            }

            // 多part的文件时，支持不同part(file)在不同存储池的读取
            StoragePool pool0;
            if (!storagePool.getVnodePrefix().equalsIgnoreCase(partInfos[partNum].storage)) {
                pool0 = StoragePoolFactory.getStoragePool(partInfos[partNum].storage, partInfos[partNum].bucket);
            } else {
                pool0 = storagePool;
            }

            //计算当前分段需要读取的start和end
            long curPartStart = partStart;
            long curPartEnd;
            if (partEnd >= partInfos[partNum].partSize) {
                curPartEnd = partInfos[partNum].partSize - 1;
                partEnd -= partInfos[partNum].partSize;
            } else {
                curPartEnd = partEnd;
                partEnd = -1L;
            }
            partStart = 0L;
            if (fileSize - readSum.get() < partInfos[partNum].partSize - curPartStart) {
                len.set(fileSize - readSum.get());
                readSum.set(fileSize);
            } else {
                len.set(partInfos[partNum].partSize - curPartStart);
                readSum.addAndGet(partInfos[partNum].partSize - curPartStart);
            }


            if (StringUtils.isNotEmpty(partInfos[partNum].deduplicateKey)) {
                List<Tuple3<String, String, String>> nodeList = pool0.mapToNodeInfo(pool0.getObjectVnodeId(partInfos[partNum].fileName))
                        .block();
                String vnode = partInfos[partNum].fileName.split(File.separator)[1].split("_")[0];
                // curPartStart和curPartEnd表示当前数据块起止位，增加partInfos[partNum].offset适配NFS文件下载，该offset从inodeData.offset转换而来
                // s3 上传对象offset默认为0；NFS上传文件在出现数据块交叉时会把offset从0改成其它数值，修改逻辑见updateInodeData
                disposables[1] = pool0.mapToNodeInfo(vnode)
                        .flatMapMany(nodelist -> ECUtils.getObject(pool0, partInfos[partNum].fileName, false, curPartStart + partInfos[partNum].offset, curPartEnd + partInfos[partNum].offset, len.get()
                                , nodeList, UPDATER.get(this).cur, request, clientRequest))
                        .doOnNext(res::onNext)
                        .doOnNext(b -> returnStreamCount++)
                        .doOnError(res::onError)
                        .doOnComplete(() -> {
                            UPDATER.updateAndGet(this, old -> {
                                old.last = old.cur;
                                old.cur = UnicastProcessor.create();
                                old.streamWaitNum = returnStreamCount;
                                return old;
                            });
                            partControlSignal.onNext(partNum + 1);
                        })
                        .subscribe();
                ;
            } else if (StringUtils.isBlank(partInfos[partNum].fileName)) {
                disposables[1] = FsUtils.getHoleFileBytes(curPartStart, curPartEnd, pool0, UPDATER.get(this).cur)
                        .doOnNext(res::onNext)
                        .doOnNext(b -> returnStreamCount++)
                        .doOnError(res::onError)
                        .doOnComplete(() -> {
                            UPDATER.updateAndGet(this, old -> {
                                old.last = old.cur;
                                old.cur = UnicastProcessor.create();
                                old.streamWaitNum = returnStreamCount;
                                return old;
                            });
                            partControlSignal.onNext(partNum + 1);
                        })
                        .subscribe();
            } else {
                // curPartStart和curPartEnd表示当前数据块起止位，增加partInfos[partNum].offset适配NFS文件下载，该offset从inodeData.offset转换而来
                // s3 上传对象offset默认为0；NFS上传文件在出现数据块交叉时会把offset从0改成其它数值，修改逻辑见updateInodeData
                List<Tuple3<String, String, String>> nodeList = pool0.mapToNodeInfo(pool0.getObjectVnodeId(partInfos[partNum].fileName))
                        .block();
                disposables[1] = ECUtils.getObject(pool0, partInfos[partNum].fileName, false, curPartStart + partInfos[partNum].offset, curPartEnd + partInfos[partNum].offset, len.get(),
                                nodeList, UPDATER.get(this).cur, request, clientRequest)
                        .doOnNext(res::onNext)
                        .doOnNext(b -> returnStreamCount++)
                        .doOnError(res::onError)
                        .doOnComplete(() -> {
                            UPDATER.updateAndGet(this, old -> {
                                old.last = old.cur;
                                old.cur = UnicastProcessor.create();
                                old.streamWaitNum = returnStreamCount;
                                return old;
                            });
                            partControlSignal.onNext(partNum + 1);
                        })
                        .subscribe();
            }
        }, res::onError);

        Optional.ofNullable(msHttpRequest).ifPresent(r -> r.addResponseCloseHandler(v -> {
            for (Disposable disposable : disposables) {
                if (disposable != null) {
                    disposable.dispose();
                }
                if (transformDisposable != null) {
                    transformDisposable.dispose();
                }
            }
        }));

        Optional.ofNullable(clientRequest).ifPresent(r -> r.addResponseCloseHandler(v -> {
            for (Disposable disposable : disposables) {
                if (disposable != null) {
                    disposable.dispose();
                }
                if (transformDisposable != null) {
                    transformDisposable.dispose();
                }
            }
        }));
    }
}
