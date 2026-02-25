package com.macrosan.storage.client;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.storage.client.channel.Channel;
import com.macrosan.utils.functional.Function2;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.util.*;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.ERROR;

/**
 * EC中List操作的模板
 *
 * @author gaozhiyuan
 */
@Log4j2
public abstract class AbstractListClient<T> {
    protected List<Tuple3<String, String, String>> nodeList;
    public ResponseInfo<T[]> responseInfo;
    public MonoProcessor<Boolean> res = MonoProcessor.create();
    protected UnicastProcessor<Integer> repairFlux = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
    private boolean repairEnd = false;
    //repairNum最后要为0，表示异常数据的repair已执行
    private int repairNum = 0;
    protected String vnode;
    //保存list结果
    protected LinkedList<Counter> linkedList = new LinkedList<>();
    public StoragePool pool;
    protected MsHttpRequest request;
    protected Channel<T> channel;
    protected String currentSnapshotMark;
    protected String snapshotLink;

    protected class Counter {
        //这条记录有多少个节点返回了
        int c;
        //记录对应的key
        String key;
        //某条记录
        T t;
        //是否一致
        boolean identical = true;


        Counter(int c, String key, T t) {
            this.c = c;
            this.t = t;
            this.key = key;
        }

        Counter(String key, T t) {
            this.key = key;
            this.t = t;
        }

        public T getT() {
            return this.t;
        }
    }

    protected AbstractListClient(StoragePool pool, ResponseInfo<T[]> responseInfo, List<Tuple3<String, String, String>> nodeList) {
        this.pool = pool;
        this.responseInfo = responseInfo;
        this.nodeList = nodeList;
        this.vnode = nodeList.get(0).var3;

        Disposable subscribe = repairFlux.subscribe(i -> {
            if (i == 0) {
                repairEnd = true;
            } else {
                repairNum += i;
            }

            if (repairEnd && repairNum == 0) {
                if (channel == null) {
                    for (Counter counter : linkedList) {
                        handleResult(counter.t);
                    }
                    publishResult();
                } else {
                    Iterator<Counter> iterator = linkedList.iterator();
                    while (iterator.hasNext()) {
                        channel.write(iterator.next().t);
                        iterator.remove();
                    }
                    res.onNext(true);
                }
            }
        });
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> {
            subscribe.dispose();
            repairFlux.dispose();
        }));
    }

    protected AbstractListClient(StoragePool pool, ResponseInfo<T[]> responseInfo, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request) {
        this(pool, responseInfo, nodeList);
        this.request = request;
    }

    protected AbstractListClient(ResponseInfo<T[]> responseInfo, List<Tuple3<String, String, String>> nodeList, String bucket) {
        this(StoragePoolFactory.getStoragePool(StorageOperate.META, bucket), responseInfo, nodeList);
    }

    protected AbstractListClient(ResponseInfo<T[]> responseInfo, List<Tuple3<String, String, String>> nodeList, MsHttpRequest request, String bucket) {
        this(StoragePoolFactory.getStoragePool(StorageOperate.META, bucket), responseInfo, nodeList);
        this.request = request;
    }

    /**
     * 完成所有处理后，调用该方法返回结果
     */
    abstract protected void publishResult();

    /**
     * 返回结果的key，key相同的返回结果在List中是同一条记录
     *
     * @param t
     * @return
     */
    abstract protected String getKey(T t);

    /**
     * 比较List结果中同一条记录在不同节点返回结果的优先级，一般是比较版本号
     *
     * @param t1
     * @param t2
     * @return t1的优先级大于t2则返回大于0的值，相同则返回0，t1的优先级小于t2则返回小于于0的值
     */
    abstract protected int compareTo(T t1, T t2);

    abstract protected void handleResult(T t);

    /**
     * 修复不一致的记录
     *
     * @param counter
     * @param nodeList
     * @return 修复成功返回true，修复失败返回false
     */
    abstract protected Mono<Boolean> repair(Counter counter, List<Tuple3<String, String, String>> nodeList);

    /**
     * 将异常处理时所需的数据，放入errorList
     */
    abstract protected void putErrorList(Counter counter);

    /**
     * 发送errorList到消息队列。通过current_ip和msg中的lun进行缓冲判断
     */
    abstract protected void publishErrorList();

    protected T mergeOperator(T t1, T t2) {
        return null;
    }

    /**
     * 处理从节点拿到的响应
     *
     * @param tuple 节点的响应，这个三元元组中数据分别为 ：link序号，响应类型，返回的元数据list，要求rsocket返回的list是有序的
     */
    public void handleResponse(Tuple3<Integer, ErasureServer.PayloadMetaType, T[]> tuple) {
        //如果这个节点响应的是error，直接返回
        if (ERROR.equals(tuple.var2)) {
            return;
        }
        //如果链表为空，也就是第一个节点响应结果，直接将第一个节点的响应结果放入链表中
        if (linkedList.isEmpty()) {
            for (T t : tuple.var3) {
                linkedList.add(new Counter(1, getKey(t), t));
            }
        } else {
            ListIterator<Counter> iterator = linkedList.listIterator();
            Counter curCounter;
            //如果不是第一个响应的节点，遍历这个节点的响应结果
            for (T t : tuple.var3) {
                //如果链表已经遍历结束，但当前节点返回的结果还没遍历结束，就将这条记录加到链表中
                //例如：第一个节点返回3条记录，第二个节点返回4条记录的情况
                if (!iterator.hasNext()) {
                    iterator.add(new Counter(1, getKey(t), t));
                    continue;
                }
                //新响应的节点返回的记录的key
                String key = getKey(t);
                curCounter = iterator.next();
                int i = curCounter.key.compareTo(key);
                //比较当前这条记录的key与之前节点返回的记录，如果相等
                //对应位置的counter的c加一，然后比较对应的记录对象，如果不相等，就设置counter的identical属性为false
                //如果新的记录比之前的记录新，就替换掉旧记录
                if (i == 0) {
                    curCounter.c++;
                    int j = compareTo(t, curCounter.t);

                    if (j != 0) {
                        curCounter.identical = false;

                        T merged = mergeOperator(t, curCounter.t);
                        if (merged != null) {
                            curCounter.t = merged;
                        } else {
                            if (j > 0) {
                                curCounter.t = t;
                            }
                        }
                    }
                } else if (i > 0) {
                    //如果新来的key比链表中当前的key大，将新来的记录加到当前key前面
                    iterator.previous();
                    curCounter = new Counter(1, key, t);
                    iterator.add(curCounter);
                } else {
                    //如果新来的key比链表中当前的key小，遍历链表：
                    //如果有key和当前一致的，对应的counter.c++
                    //如果找不到与当前对应的，遇到比当前大的就在他前面插入这条记录
                    //如果到最后一直是比当前小的，就在最后插入这条记录
                    int j = -1;
                    while (iterator.hasNext()) {
                        curCounter = iterator.next();
                        j = curCounter.key.compareTo(getKey(t));
                        if (j == 0) {
                            curCounter.c++;
                            break;
                        }

                        if (j > 0) {
                            iterator.previous();
                            curCounter = new Counter(1, key, t);
                            iterator.add(curCounter);
                            break;
                        }
                    }

                    if (j < 0) {
                        curCounter = new Counter(1, key, t);
                        iterator.add(curCounter);
                    }
                }
            }
        }
    }

    public void handleComplete() {
        if (responseInfo.successNum < pool.getK()) {
            res.onNext(false);
            return;
        }

        //存放没有得到k+m份返回的元数据
        for (Counter counter : linkedList) {
            //返回响应不及k个的元数据需要马上进行repair，若失败则lsfile请求返500
            //else，没有得到k+m份返回或有不一致的元数据在异常处理中修复，可延后。
            if (counter.c < pool.getK()) {
                repairFlux.onNext(1);
                Disposable subscribe = repair(counter, nodeList).subscribe(b -> {
                    if (b) {
                        repairFlux.onNext(-1);
                    } else {
                        try {
//                            log.error("repair {} fail", counter.t);
                            res.onNext(false);
                        } catch (Exception e) {

                        }
                    }
                });
                res.onErrorReturn(false);
                Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
            } else {
                if (counter.c != pool.getK() + pool.getM() || !counter.identical) {
                    putErrorList(counter);
                }
            }
        }
        publishErrorList();
        repairFlux.onNext(0);
    }


    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }
}
