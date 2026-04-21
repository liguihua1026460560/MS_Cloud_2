package com.macrosan.filesystem.cache;

import com.macrosan.filesystem.async.AsyncUtils;
import com.macrosan.filesystem.cache.InodeOperator.UpdateArgs;
import com.macrosan.filesystem.cifs.notify.NotifyServer;
import com.macrosan.filesystem.cifs.reply.smb2.NotifyReply;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.filesystem.utils.timeout.FastMonoTimeOut;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.utils.quota.QuotaRecorder;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.macrosan.message.jsonmsg.Inode.*;

@Log4j2
public class InodeCache {
    private static class RunningHook {
        int readChunk;
        long writeNum;
        long readNum;
        int chunkLimit;
        LinkedList<InodeOperator> waitList = new LinkedList<>();

    }

    public static final int MAX_CACHE_NUM = 10_0000;
    public static final long MAX_CACHE_TIME = 300_000_000_000L; //300s
    private static final int EXEC_TIMEOUT = 300;
    static final Set<Integer> ONE_BATCH_PUT_TYPES = new HashSet<>();

    public static long MIN_INODE_SIZE = 5L << 30;
    public static long MAX_INODE_SIZE = 100L << 30;
    public static int maxPrefetch = 96;
    public static int minPrefetch = 32;
    public static double sizeFactor = (double) (maxPrefetch - minPrefetch) / (MAX_INODE_SIZE - MIN_INODE_SIZE);

    private static class ReadCounter {
        public long inodeSize = -1;

        public static int getPrefetch(int opt, ReadCounter readCounter) {
            int prefetch = -1;

            if (opt == 9) {
                //超过size后，getChunk并发为min
                if (readCounter.inodeSize > MAX_INODE_SIZE) {
                    return minPrefetch;
                }

                //在size范围之间，getChunk并发从max->min；inodeSize不足min则不限制并发
                if (readCounter.inodeSize > MIN_INODE_SIZE) {
                    prefetch = maxPrefetch - (int) (sizeFactor * (readCounter.inodeSize - MIN_INODE_SIZE));
                    if (prefetch < 1) {
                        prefetch = 1;
                    }
                    return prefetch;
                }
            }

            return prefetch;
        }
    }

    static {
        /**
         * 删除和重命名操作对应的putType
         */
        ONE_BATCH_PUT_TYPES.add(1);
        ONE_BATCH_PUT_TYPES.add(3);
    }

    Cache<Long, Inode> cache;
    TimeCache<String, Inode> s3InodeCache;
    Map<Long, RunningHook> running = new ConcurrentHashMap<>();
    Scheduler executor;

    InodeCache(Scheduler executor) {
        this.executor = executor;
        cache = new Cache<>(MAX_CACHE_NUM, executor);
        s3InodeCache = new TimeCache<>(MAX_CACHE_TIME, executor);
    }

    public void hook(InodeOperator operator) {
        AtomicBoolean res = new AtomicBoolean(true);
        AtomicBoolean isReleaseNum = new AtomicBoolean(false);
        running.compute(operator.nodeId, (k, v) -> {
            if (v == null) {
                v = new RunningHook();
            }

            if (operator.readOpt) {
                if (v.writeNum == 0) {
                    //判断当前是否存在限流
                    if ((operator.opt == 9 && v.chunkLimit > 0)) {
                        //有限流则根据阈值限流，被限的进行等待
                        if (v.readChunk < v.chunkLimit) {
                            v.readChunk++;
                            v.readNum++;
                            isReleaseNum.set(true);
                        } else {
                            v.waitList.add(operator);
                            res.set(false);
                        }
                    } else {
                        //没有限流则直接运行
                        v.readNum++;
                    }


                } else {
                    v.waitList.add(operator);
                    res.set(false);
                }
            } else {
                if (v.readNum == 0 && v.writeNum == 0) {
                    v.writeNum++;
                } else {
                    v.waitList.add(operator);
                    res.set(false);
                }
            }

            return v;
        });

        if (res.get()) {
            exec(Collections.singletonList(operator), isReleaseNum.get());
        }
    }

    private void exec(List<InodeOperator> batch, boolean isReleaseNum) {
        InodeOperator inodeOperator = batch.get(0);
        ReadCounter readCounter = new ReadCounter();

        try {
            if (inodeOperator.readOpt) {
                Mono<Inode> res = batch.get(0).getOpt.get();
                Mono<Inode> mono;
                if (inodeOperator.getMsg() != null) {
                    List<SocketReqMsg> msgs = batch.stream()
                            .map(InodeOperator::getMsg)
                            .collect(Collectors.toList());
                    mono = AsyncUtils.asyncBatch(inodeOperator.getMsg().get("bucket"), inodeOperator.nodeId, msgs, res);
                } else {
                    mono = res;
                }
                FastMonoTimeOut.fastTimeout(mono, Duration.ofSeconds(EXEC_TIMEOUT))
                        .doFinally(s -> {
                            release(inodeOperator, readCounter, isReleaseNum);
                        })
                        .doOnNext(i -> {
                            recordRead(i, inodeOperator, readCounter);
                            if (i.getLinkN() == ERROR_INODE.getLinkN()) {
                                onNext(batch, RETRY_INODE);
                            } else {
                                onNext(batch, i);
                            }
                        })
                        .doOnError(e -> {
                            if (e instanceof TimeoutException || "closed connection".equals(e.getMessage())
                                    || (e.getMessage() != null && e.getMessage().startsWith("No keep-alive acks for"))) {
                                log.error("readOpt timeout, {}", e.getMessage());
                            } else {
                                log.error("readOpt error ", e);
                            }
                            onNext(batch, RETRY_INODE);
                        })
                        .subscribe();
            } else {
                Mono<Inode> res = inodeOperator.getOpt.get()
                        .flatMap(inode -> {
                            if (inode.getNodeId() > 0 && !inode.isDeleteMark()) {
                                Inode old = inode.clone();
                                UpdateArgs args = new UpdateArgs();
                                for (InodeOperator operator : batch) {
                                    if (operator.updateOpt != null) {
                                        operator.updateOpt.update(args, inode);
                                    }
                                }
                                if (batch.get(0).putType == 1) {
                                    Inode cacheInode = Node.getInstance().getInodeV(inode.getNodeId()).inodeCache.cache.get(inode.getNodeId());
                                    if (cacheInode == null || inode.isDeleteMark()) {
                                        return Mono.just(NOT_FOUND_INODE);
                                    }
                                }
                                return batch.get(0).putOpt.put(args, old, inode)
                                        .doOnNext(i -> {
                                            recordRead(i, inodeOperator, readCounter);
                                            QuotaRecorder.addCheckBucket(inode.getBucket());
                                            if (i.getLinkN() > 0 && args.notifyFilters != 0) {
                                                NotifyServer.getInstance().maybeNotify(i.getBucket(), i.getObjName(), args.notifyFilters, NotifyReply.NotifyAction.FILE_ACTION_MODIFIED);
                                            }
                                        });
                            } else {
                                if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                                    return Mono.just(RETRY_INODE);
                                }
                                return Mono.just(inode);
                            }
                        });
                Mono<Inode> mono;
                if (inodeOperator.getMsg() != null) {
                    // 异步复制写预提交记录操作放在这里是为了保证写预提交记录的顺序和waitLit中任务处理的顺序一致
                    // todo del
                    log.debug("opt1 : {}, {}, {}", inodeOperator.nodeId, inodeOperator.opt, inodeOperator.getMsg());
                    List<SocketReqMsg> msgs = batch.stream()
                            .map(InodeOperator::getMsg)
                            .collect(Collectors.toList());
                    mono = AsyncUtils.asyncBatch(inodeOperator.getMsg().get("bucket"), inodeOperator.nodeId, msgs, res);
                } else {
                    mono = res;
                }
                FastMonoTimeOut.fastTimeout(mono, Duration.ofSeconds(EXEC_TIMEOUT))
                        .doFinally(s -> release(inodeOperator, readCounter, isReleaseNum))
                        .doOnNext(i -> onNext(batch, i))
                        .doOnError(e -> {
                            if (e instanceof TimeoutException || "closed connection".equals(e.getMessage())
                                    || (e.getMessage() != null && e.getMessage().startsWith("No keep-alive acks for"))) {
                                //超时之后需清除inode缓存
                                if (inodeOperator.nodeId > 1) {
                                    Node.getInstance().getInodeV(inodeOperator.nodeId).inodeCache.cache.remove(inodeOperator.nodeId);
                                }
                                log.error("nodeId:{},writeOpt timeout, {}", inodeOperator.nodeId, e.getMessage());
                            } else {
                                log.error("nodeId:{},writeOpt error ", inodeOperator.nodeId, e);
                            }
                            onNext(batch, RETRY_INODE);
                        })
                        .subscribe();
            }
        } catch (Exception e) {
            log.error("", e);
            release(inodeOperator, readCounter, isReleaseNum);
            onNext(batch, RETRY_INODE);
        }
    }

    private void onNext(List<InodeOperator> batch, Inode inode) {
        Inode i = inode.clone();
        for (InodeOperator operator : batch) {
            operator.res.onNext(i);
        }
    }

    private RunningHook checkWait(RunningHook hook, ReadCounter readCounter) {
        int chunkPrefetch = ReadCounter.getPrefetch(9, readCounter);
        hook.chunkLimit = chunkPrefetch;
        //保存暂时被限制而丢弃的operator，在循环结束后加回来
        LinkedList<InodeOperator> backList = new LinkedList<>();

        while (hook.writeNum == 0 && !hook.waitList.isEmpty()) {
            InodeOperator exec = hook.waitList.pollFirst();
            if (exec.readOpt) {
                if (exec.opt == 9 && chunkPrefetch > 0) {
                    //chunkPrefetch>0时限制getChunk并发
                    if (hook.readChunk < chunkPrefetch) {
                        hook.readNum++;
                        this.executor.schedule(() -> {
                            this.exec(Collections.singletonList(exec), true);
                        });
                    } else {
                        backList.add(exec);
                    }
                } else {
                    //未被限流的读操作release时候无需减去readChunk
                    hook.readNum++;
                    this.executor.schedule(() -> {
                        this.exec(Collections.singletonList(exec), false);
                    });
                }
            } else if (hook.readNum == 0) {
                hook.writeNum++;
                List<InodeOperator> batch = new LinkedList<>();
                batch.add(exec);

                int count = 0;
                ListIterator<InodeOperator> listIterator = hook.waitList.listIterator();
                while (!ONE_BATCH_PUT_TYPES.contains(exec.putType) && count < 100 && listIterator.hasNext()) {
                    InodeOperator operator = listIterator.next();
                    if (!operator.readOpt && exec.putType == operator.putType) {
                        count++;
                        batch.add(operator);
                        listIterator.remove();
                    }
                    if (ONE_BATCH_PUT_TYPES.contains(operator.putType)) {
                        break;
                    }
                }
                this.executor.schedule(() -> {
                    this.exec(batch, false);
                });
            } else {
                hook.waitList.addFirst(exec);
                break;
            }
        }

        if (!backList.isEmpty()) {
            hook.waitList.addAll(backList);
        }

        if (hook.waitList.isEmpty() && hook.readNum == 0 && hook.writeNum == 0) {
            return null;
        } else {
            return hook;
        }
    }

    public void release(InodeOperator operator, ReadCounter readCounter, boolean isReleaseNum) {
        running.compute(operator.nodeId, (k, v) -> {
            if (v == null) {
                //错误分支 多次释放
                log.error("error release read");
            } else {
                if (operator.readOpt) {
                    v.readNum--;
                    if (isReleaseNum) {
                        if (operator.opt == 9) {
                            v.readChunk--;
                        }
                    }
                } else {
                    v.writeNum--;
                }
                return checkWait(v, readCounter);
            }

            return v;
        });
    }

    public Cache<Long, Inode> getCache() {
        return this.cache;
    }

    public void recordRead(Inode inode, InodeOperator operator, ReadCounter counter) {
        if (InodeUtils.isError(inode) || inode.getNodeId() <= 0 || inode.getLinkN() < 0) {
            return;
        }

        int opt = operator.opt;
        int putType = operator.putType;
        if (operator.readOpt) {
            //读操作，记录getInode和getChunk时inode的size
            if (opt == 0 || opt == 9) {
                counter.inodeSize = inode.getSize();
            }
        } else {
            //写操作，记录updateInode/rename/setattr/emptyUpdate
            if (putType == 0 || putType == 3 || putType == 9 || putType == 4) {
                counter.inodeSize = inode.getSize();
            }
        }
    }
}
