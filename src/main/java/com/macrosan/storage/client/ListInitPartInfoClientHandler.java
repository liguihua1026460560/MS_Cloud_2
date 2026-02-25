package com.macrosan.storage.client;/**
 * @author niechengxing
 * @create 2023-09-01 14:49
 */

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.InitPartInfo;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.ERROR;

/**
 *@program: MS_Cloud
 *@description:
 *@author: niechengxing
 *@create: 2023-09-01 14:49
 */
@Log4j2
public class ListInitPartInfoClientHandler {
    protected final UnicastProcessor<SocketReqMsg> msgUnicastProcessor;
    protected final ClientTemplate.ResponseInfo<Tuple3<Boolean, String, InitPartInfo>[]> responseInfo;
    protected static RedisConnPool pool = RedisConnPool.getInstance();
    protected final LinkedList<Counter> result;
    protected final String vnode;
    protected final SocketReqMsg msg;
    public MonoProcessor<List<Counter>> res;
    protected StoragePool storagePool;

    public static class Counter {
        String key;
        InitPartInfo initPartInfo;

        Counter(String key, InitPartInfo initPartInfo) {
            this.key = key;
            this.initPartInfo = initPartInfo;
        }

        public InitPartInfo getInitPartInfo() {
            return this.initPartInfo;
        }
    }

    public ListInitPartInfoClientHandler(StoragePool storagePool, UnicastProcessor<SocketReqMsg> msgUnicastProcessor,
                                         ClientTemplate.ResponseInfo<Tuple3<Boolean, String, InitPartInfo>[]> responseInfo, String vnode, SocketReqMsg msg) {
        this.storagePool = storagePool;
        this.msgUnicastProcessor = msgUnicastProcessor;
        this.responseInfo = responseInfo;
        this.vnode = vnode;
        this.msg = msg;
        result = new LinkedList<>();
        res = MonoProcessor.create();
    }

    public void handleResponse(Tuple3<Integer, ErasureServer.PayloadMetaType, Tuple3<Boolean, String, InitPartInfo>[]> tuple) {
        try {
            if (ERROR.equals(tuple.var2)) {
                return;
            }
            Tuple3<Boolean, String, InitPartInfo>[] tupleArray = tuple.getVar3();
            if (result.isEmpty()) {
                //第一次直接写入
                Arrays.stream(tupleArray).forEach(tuple3 -> result.add(new Counter(getKey(tuple3), tuple3.getVar3())));
            } else {
                ListIterator<Counter> iterator = result.listIterator();
                Counter curCounter;
                //如果不是第一个响应的节点，遍历这个节点的响应结果
                for (Tuple3<Boolean, String, InitPartInfo> t : tupleArray) {
                    InitPartInfo initPartInfo = t.var3;
                    //如果链表已经遍历结束，但当前节点返回的结果还没遍历结束，就将这条记录加到链表中
                    //例如：第一个节点返回3条记录，第二个节点返回4条记录的情况
                    if (!iterator.hasNext()) {
                        iterator.add(new Counter(getKey(t), initPartInfo));
                        continue;
                    }
                    //新响应的节点返回的记录的key
                    String key = getKey(t);
                    curCounter = iterator.next();
                    int i = curCounter.key.compareTo(key);
                    if (i > 0) {
                        //如果新来的key比链表中当前的key小，将新来的记录加到当前key前面
                        iterator.previous();
                        curCounter = new Counter(key, initPartInfo);
                        iterator.add(curCounter);
                    } else if (i < 0) {
                        //如果新来的key比链表中当前的key大，遍历链表：
                        //如果有key和当前一致的，对应的counter.c++
                        //如果找不到与当前对应的，遇到比当前大的就在他前面插入这条记录
                        //如果到最后一直是比当前小的，就在最后插入这条记录
                        //如果当前为链表最大的元素，则直接在链表最后插入这条记录，不再遍历
                        int j = 0;
                        if (!iterator.hasNext()) {
                            curCounter = new Counter(key, initPartInfo);
                            iterator.add(curCounter);
                        } else {
                            while (iterator.hasNext()) {
                                curCounter = iterator.next();
                                j = curCounter.key.compareTo(getKey(t));
                                if (j == 0) {
                                    break;
                                }

                                if (j > 0) {
                                    iterator.previous();
                                    curCounter = new Counter(key, initPartInfo);
                                    iterator.add(curCounter);
                                    break;
                                }
                            }

                            if (j < 0) {
                                curCounter = new Counter(key, initPartInfo);
                                iterator.add(curCounter);
                            }
                        }

                    }
                }
            }
        } catch (Exception e) {
            log.error("handleResponse error ! " + e);
        }
    }

    protected String getKey(Tuple3<Boolean, String, InitPartInfo> t) {
        return t.var3.getPartKey(vnode);
    }

    public void completeResponse() {
        try {
            if (responseInfo.successNum == 0 && (storagePool.getK() + storagePool.getM()) > 1) {
                log.error("Restore get initPartInfo list error!");
                msgUnicastProcessor.onComplete();
                return;
            } else if (responseInfo.successNum < storagePool.getK()) {
                int retryTimes = Integer.parseInt(msg.get("retryTimes"));
                retryTimes++;
                msg.put("retryTimes", String.valueOf(retryTimes));
                if (retryTimes > 10) {
                    msgUnicastProcessor.onComplete();
                } else {
                    log.info("Restore get initPartInfo list error, retry " + retryTimes + "th !");
                    msgUnicastProcessor.onNext(msg);
                }
                return;
            }

            msg.put("retryTimes", "0");
            if (result.size() > 1000) {
                InitPartInfo initPartInfo = result.get(1000).getInitPartInfo();
                msg.put("marker", initPartInfo.object);
                result.remove(result.size() - 1);
                res.onNext(result);
                msgUnicastProcessor.onNext(msg);
            } else {
                res.onNext(result);
                log.info("list bucket vnode initPartInfo complete!!!");
                msgUnicastProcessor.onComplete();
            }
        } catch (Exception e) {
            log.error(e);
        }
    }

}

