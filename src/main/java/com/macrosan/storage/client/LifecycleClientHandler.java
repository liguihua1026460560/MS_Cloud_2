package com.macrosan.storage.client;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.Utils;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.SysConstants.LIFECYCLE_RECORD;
import static com.macrosan.constants.SysConstants.REDIS_TASKINFO_INDEX;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.ec.Utils.getLifeCycleMetaKey;
import static com.macrosan.ec.Utils.getLifeCycleStamp;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.ERROR;
import static com.macrosan.lifecycle.LifecycleService.getEndStamp;
import static com.macrosan.utils.lifecycle.LifecycleUtils.logger;
import static com.macrosan.utils.lifecycle.LifecycleUtils.putLifecycleRecord;

@Log4j2
public class LifecycleClientHandler {

    protected final UnicastProcessor<SocketReqMsg> msgUnicastProcessor;
    protected final ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo;
    protected static RedisConnPool pool = RedisConnPool.getInstance();
    protected final LinkedList<Counter> result;
    protected final String vnode;
    protected final SocketReqMsg msg;
    public MonoProcessor<List<Counter>> res;
    protected StoragePool storagePool;

    public static class Counter {
        //记录对应的key
        String key;
        //某条记录
        MetaData metaData;

        Counter(String key, MetaData metaData) {
            this.metaData = metaData;
            this.key = key;
        }

        public MetaData getMetaData() {
            return this.metaData;
        }

        public String getKey() {
            return this.key;
        }
    }

    public LifecycleClientHandler(UnicastProcessor<SocketReqMsg> msgUnicastProcessor,
                                  ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo, String vnode, SocketReqMsg msg, String bucket) {
        storagePool = StoragePoolFactory.getStoragePool(StorageOperate.META, bucket);
        this.msgUnicastProcessor = msgUnicastProcessor;
        this.responseInfo = responseInfo;
        this.vnode = vnode;
        this.msg = msg;
        result = new LinkedList<>();
        res = MonoProcessor.create();
    }


    public LifecycleClientHandler(StoragePool storagePool, UnicastProcessor<SocketReqMsg> msgUnicastProcessor,
                                  ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo, String vnode, SocketReqMsg msg) {
        this.storagePool = storagePool;
        this.msgUnicastProcessor = msgUnicastProcessor;
        this.responseInfo = responseInfo;
        this.vnode = vnode;
        this.msg = msg;
        result = new LinkedList<>();
        res = MonoProcessor.create();
    }

    public void handleResponse(Tuple3<Integer, ErasureServer.PayloadMetaType, Tuple3<Boolean, String, MetaData>[]> tuple) {
        try {
            if (ERROR.equals(tuple.var2)) {
                return;
            }
            Tuple3<Boolean, String, MetaData>[] tupleArray = tuple.getVar3();
            if (result.isEmpty()) {
                //第一次直接写入
                Arrays.stream(tupleArray).forEach(tuple3 -> result.add(new Counter(getKey(tuple3.getVar3()), tuple3.getVar3())));
            } else {
                ListIterator<Counter> iterator = result.listIterator();
                Counter curCounter;
                //如果不是第一个响应的节点，遍历这个节点的响应结果
                for (Tuple3 t : tupleArray) {
                    MetaData metaData = (MetaData) t.var3;
                    //如果链表已经遍历结束，但当前节点返回的结果还没遍历结束，就将这条记录加到链表中
                    //例如：第一个节点返回3条记录，第二个节点返回4条记录的情况
                    if (!iterator.hasNext()) {
                        iterator.add(new Counter(getKey(metaData), metaData));
                        continue;
                    }
                    //新响应的节点返回的记录的key
                    String key = getKey(metaData);
                    curCounter = iterator.next();
                    int i = curCounter.key.compareTo(key);
                    if (i > 0) {
                        //如果新来的key比链表中当前的key小，将新来的记录加到当前key前面
                        iterator.previous();
                        curCounter = new Counter(key, metaData);
                        iterator.add(curCounter);
                    } else if (i < 0) {
                        //如果新来的key比链表中当前的key大，遍历链表：
                        //如果有key和当前一致的，对应的counter.c++
                        //如果找不到与当前对应的，遇到比当前大的就在他前面插入这条记录
                        //如果到最后一直是比当前小的，就在最后插入这条记录
                        int j = -1;
                        while (iterator.hasNext()) {
                            curCounter = iterator.next();
                            j = curCounter.key.compareTo(getKey(metaData));
                            if (j == 0) {
                                break;
                            }

                            if (j > 0) {
                                iterator.previous();
                                curCounter = new Counter(key, metaData);
                                iterator.add(curCounter);
                                break;
                            }
                        }

                        if (j < 0) {
                            curCounter = new Counter(key, metaData);
                            iterator.add(curCounter);
                        }
                    } else {
                        // 新来的key和链表中key相同时，比对versionnum，取较大的
                        if (curCounter.getMetaData().versionNum.compareTo(metaData.versionNum) < 0) {
                            curCounter.metaData = metaData;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("handleResponse error ! " + e);
        }
    }

    protected String getKey(MetaData meta) {
        return Utils.getLifeCycleMetaKey(vnode, meta.getBucket(), meta.getKey(), meta.getVersionId(), meta.getStamp());
    }

    public void completeResponse(String bucketName, String recordKey, AtomicBoolean restartSign, String lifecycleRecord) {
        try {
            if (System.currentTimeMillis() >= getEndStamp()) {
                logger.info("execute lifecycle time end!!!");
                putLifecycleRecord(bucketName, vnode, recordKey, result.get(0).getMetaData(), pool);
                msgUnicastProcessor.onComplete();
                return;
            }
            if (checkSuccessNum()) {
                return;
            }
            if (result.size() > 1000) {
                MetaData metaData = result.get(1000).getMetaData();
                String beginPrefix = getLifeCycleMetaKey(vnode, metaData.bucket, metaData.key, metaData.versionId, metaData.stamp);
                if (StringUtils.isNotEmpty(lifecycleRecord) && !restartSign.get() && beginPrefix.compareTo(lifecycleRecord) >= 0) {
                    msgUnicastProcessor.onComplete();
                    Mono.just(1).publishOn(SCAN_SCHEDULER).subscribe(s -> {
                        pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hdel(bucketName + LIFECYCLE_RECORD, recordKey);
                    });
                    log.info("bucket: {}, vnode: {}, list object complete!!!", bucketName, vnode);
                } else {
                    msg.put("beginPrefix", beginPrefix);
                    //添加节点时的异步复制限流
                    msgUnicastProcessor.onNext(msg);
                }
            } else {
                if (restartSign.get()) {
                    restartSign.set(false);
                    String beginPrefix = getLifeCycleStamp(vnode, bucketName, "0");
                    msg.put("beginPrefix", beginPrefix);
                    msgUnicastProcessor.onNext(msg);
                    log.info("bucket: {}, vnode: {}, restart list object!!!", bucketName, vnode);
                } else {
                    msgUnicastProcessor.onComplete();
                    Mono.just(1).publishOn(SCAN_SCHEDULER).subscribe(s -> {
                        pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hdel(bucketName + LIFECYCLE_RECORD, recordKey);
                    });
                    log.info("bucket: {}, vnode: {}, list object complete!!!", bucketName, vnode);
                }
            }
        } catch (Exception e) {
            log.error(e);
        }
    }

    public void completeResponse() {
        try {
            if (System.currentTimeMillis() >= getEndStamp()) {
                logger.info("execute lifecycle time end!!!");
                msgUnicastProcessor.onComplete();
                return;
            }
            if (checkSuccessNum()) {
                return;
            }
            if (result.size() > 1000) {
                MetaData metaData = result.get(1000).getMetaData();
                String beginPrefix = getLifeCycleMetaKey(vnode, metaData.getBucket(), metaData.key, metaData.versionId, metaData.stamp);
                msg.put("beginPrefix", beginPrefix);
                //添加节点时的异步复制限流
                msgUnicastProcessor.onNext(msg);
            } else {
                msgUnicastProcessor.onComplete();
                log.info("list object complete!!!");
            }
        } catch (Exception e) {
            log.error(e);
        }
    }

    private boolean checkSuccessNum() {
        if (responseInfo.successNum == 0 && (storagePool.getK() + storagePool.getM()) > 1) {
            log.error("Lifecycle get object list error!");
            msgUnicastProcessor.onComplete();
            return true;
        } else if (responseInfo.successNum < storagePool.getK()) {
            int retryTimes = Integer.parseInt(msg.get("retryTimes"));
            retryTimes++;
            msg.put("retryTimes", String.valueOf(retryTimes));
            if (retryTimes > 10) {
                logger.info("--------------- onComplete ------------------");
                msgUnicastProcessor.onComplete();
            } else {
                log.info("Lifecycle get object list error, retry " + retryTimes + "th !");
                msgUnicastProcessor.onNext(msg);
            }
            return true;
        }
        res.onNext(result);
        msg.put("retryTimes", "0");
        return false;
    }


}
