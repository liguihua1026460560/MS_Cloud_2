package com.macrosan.utils.sts;/**
 * @author niechengxing
 * @create 2024-10-31 13:46
 */

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.Redis6380ConnPool;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.rebuild.RemovedDisk;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.Credential;
import com.macrosan.message.socketmsg.BaseResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.socketmsg.SocketSender;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.AbstractListClient;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_SHARDING_META_OBJ;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_VNODE_META;
import static com.macrosan.utils.sts.RoleUtils.getAssumeInfo;

/**
 *@program: MS_Cloud
 *@description: 处理多站点部署时的临时凭证同步
 *@author: niechengxing
 *@create: 2024-10-31 13:46
 */
@Log4j2
public class StsCredentialSyncTask {

    private static Redis6380ConnPool pool_6380 = Redis6380ConnPool.getInstance();
    private static final int HIGH_THRESHOLD = 20_000;
    private static final int LOW_THRESHOLD = 10_000;
    private final static ThreadFactory STS_SYNC_THREAD_FACTORY = new MsThreadFactory("sts-sync");
    private final static Scheduler STS_SYNC_SCHEDULER;

    /// 默认临时缓存1w条
    public static final LRUCache<String, Credential> credentialCache = new LRUCache<>(10000);
    public static ScheduledThreadPoolExecutor flushThread = new ScheduledThreadPoolExecutor(1, runnable -> new Thread(runnable, "IamCacheFlushThread"));
    static {
        Scheduler scheduler = null;
        try {
            MsExecutor executor = new MsExecutor(PROC_NUM / 4, PROC_NUM / 4, STS_SYNC_THREAD_FACTORY);
            scheduler = Schedulers.fromExecutor(executor);
        } catch (Exception e) {
            log.error("", e);
        }
        STS_SYNC_SCHEDULER = scheduler;
        flushThread.schedule(StsCredentialSyncTask::flushCredentialCache, 10,  TimeUnit.SECONDS);
    }

    private static void flushCredentialCache() {
        long nextDelay = 10;
        try {
            log.debug("flushCredentialCache:{}", credentialCache.size());
            long start = System.currentTimeMillis();
            if (credentialCache.isEmpty()) {
                return;
            }
            Credential[] credentials = credentialCache.values().toArray(new Credential[0]);
            for (Credential credential : credentials) {
                long cur = System.currentTimeMillis() / 1000;
                long dur = credential.deadline - cur;
                try {
                    if (dur > 0) {
                        if (credential.inlinePolicy != null) {
                            pool_6380.getCommand(REDIS_IAM_INDEX).setex(credential.inlinePolicy.policyId, dur, credential.inlinePolicy.policy);
                        }
                        String assumeInfo = getAssumeInfo(credential.accountId, new JsonArray(credential.groupIds), new JsonArray(credential.policyIds));
                        pool_6380.getCommand(REDIS_IAM_INDEX).setex(credential.assumeId, dur, assumeInfo);
                        JsonObject object = new JsonObject();
                        object.put("accountId", credential.accountId);
                        object.put("secretKey", credential.secretKey);
                        object.put("userId", credential.assumeId);
                        object.put("userName", credential.useName);
                        //校验成功将凭证信息写入6380表0缓存
                        pool_6380.getCommand(REDIS_IAM_INDEX).setex(credential.accessKey, dur, object.toString());
                    }
                    credentialCache.remove(credential.accessKey);
                } catch (Exception e) {
                    log.error("", e);
                }
            }
            long end = System.currentTimeMillis();
            nextDelay = Math.max(1, 10 - ((end - start) / 1000));
            log.debug("flushCredentialCache:{},cost:{} nextDelay:{}", credentialCache.size(), end - start, nextDelay);
        } finally {
            flushThread.schedule(StsCredentialSyncTask::flushCredentialCache, nextDelay,  TimeUnit.SECONDS);
        }
    }

    protected static SocketSender sender = SocketSender.getInstance();
    public static final TypeReference<List<Credential>> CRED_TYPE_REFERENCE = new TypeReference<List<Credential>>() {
    };

    private static final String curNode = ServerConfig.getInstance().getHostUuid();

    public static void start(String remoteIp) {
        log.info("begin to sync STS");
        List<Credential> list = new ArrayList<>();
        AtomicLong curSize = new AtomicLong();
        int maxNum = 1000;
        long maxSize = 1024 * 1024;
        listCredential()
                .publishOn(STS_SYNC_SCHEDULER)
                .flatMap(credential -> {
                    //TODO 考虑再发回后端包然后通过后端包sdk发给远端站点
                    log.info(credential);
                    list.add(credential);
                    curSize.addAndGet(Json.encode(credential).length());
                    if (list.size() < maxNum && curSize.get() < maxSize) {
                        return Mono.just(true);
                    }
                    //将list转为json传递
                    SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_SYNC_STS, 0)
                            .put("remoteIp", remoteIp)
                            .put("value", Json.encode(list));//考虑优化为传输包含1000个凭证的集合

                    int code = sender.sendAndGetResponse(msg, BaseResMsg.class, false).getCode();

                    if (1 == code) {
                        list.clear();
                        curSize.set(0);
                        return Mono.just(true);
                    } else {
                        sendCredentialToRemote(remoteIp, list);
                        list.clear();
                        curSize.set(0);
                        return Mono.just(true);
                    }
                })
                .doFinally(s ->{
                    RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX).set("syncSTSRes", "1");
                })
                .subscribe(b -> {}, e -> {
                    log.error("", e);
                }, () -> {
                    if (list.size() < maxNum && curSize.get() < maxSize) {
                        if (!list.isEmpty()) {
                            SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_SYNC_STS, 0)
                                    .put("remoteIp", remoteIp)
                                    .put("value", Json.encode(list));//考虑优化为传输包含1000个凭证的集合

                            int code = sender.sendAndGetResponse(msg, BaseResMsg.class, false).getCode();
                            if (1 != code) {
                                sendCredentialToRemote(remoteIp, list);
                            }
                            list.clear();
                            curSize.set(0);
                        }
                    }
                    //TODO 设置完成的标志位，供后端包检测
                    RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX).set("syncSTSRes", "1");
                    log.info("STS synchronization is complete!");
                });

    }


    public static Flux<Credential> listCredential() {
        UnicastProcessor<Credential> res = UnicastProcessor.create();
        //首先需要获取索引池
        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool("");
        //获取所有vnodeList
        Set<String> vnodeKeyList = metaStoragePool.getCache().getCache().keySet();
        long curTime = System.currentTimeMillis() / 1000;
        Flux.fromStream(vnodeKeyList.stream())
                .publishOn(STS_SYNC_SCHEDULER)
                .flatMap(vnodeKey -> {
                    //扫描每个vnode上的数据
                    String vnode = vnodeKey.substring(getPrefix(vnodeKey).length());
                    return metaStoragePool.mapToNodeInfo(vnode).zipWith(Mono.just(vnode));
                })
                .flatMap(tuple -> listVnodeCred(metaStoragePool, tuple.getT2(),tuple.getT1(), MSRocksDB.IndexDBEnum.STS_TOKEN_DB, false, ""))
                .subscribe(tuple1 -> {
                    Credential credential = Json.decodeValue(tuple1.var2.toString(), Credential.class);
                    if (null != credential && curTime < credential.getDeadline()) {
                        res.onNext(credential);
                    }
                }, e -> log.error("", e), res::onComplete);

        return res;
    }

    public static String getPrefix(String key) {
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (c >= '0' && c <= '9') {
                return key.substring(0, i);
            }
        }

        return "";
    }

    protected static String[] getCurNodePoolDisk(StoragePool pool) {
        List<String> list = new LinkedList<>();
        for (String lun : pool.getCache().lunSet) {
            String node = lun.split("@")[0];
            if (curNode.equalsIgnoreCase(node)) {
                if (!RemovedDisk.getInstance().contains(lun)) {
                    list.add(lun.split("@")[1]);
                }
            }
        }

        return list.toArray(new String[0]);
    }

    public static Mono<Boolean> syncSTSToken(Credential credential) {
//        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
//        String value = msg.get("value");
        //将value解析为credential
//        Credential credential = Json.decodeValue(value, Credential.class);
        StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool(credential.accessKey);
        String vnode = metaStoragePool.getBucketVnodeId(credential.accessKey);//使用账户id和凭证id计算存储的vnode

        return metaStoragePool.mapToNodeInfo(vnode)
                .flatMap(nodeList -> {
                    String credentialKey = credential.getCredentialKey(vnode);
                    return ErasureClient.putCredential(credential, credentialKey, nodeList);
                })
                .flatMap(b -> {
                    if (!b) {
                        return Mono.just(false);
                    } else {
                        return Mono.just(true);
                    }
                })
                .doOnError(e -> log.error("", e))
                .onErrorReturn(false);
    }


    /**
     * 发送同步sts信息给远端站点失败，定期重试
     */
    protected static void sendCredentialToRemote(String remoteIp, List<Credential> list) {
        log.info("retry sync sts ak: {}, sk: {}", list.get(0).accessKey, list.get(0).assumeId);
        log.info(list.get(0));

        AtomicInteger retry = new AtomicInteger(0);
        Disposable[] disposables = new Disposable[1];
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_SYNC_STS, 0)
                .put("remoteIp", remoteIp)
                .put("value", Json.encode(list));
        //将信息发送至远端站点同步存储
        disposables[0] = STS_SYNC_SCHEDULER.schedulePeriodically(() -> Mono.just(true)
                .flatMap(b -> {
                    int code = sender.sendAndGetResponse(msg, BaseResMsg.class, false).getCode();
                    return Mono.just(code == 1);
                })
                .doOnError(e -> {
                    retry.incrementAndGet();
                    if (retry.get() > 2) {
                        disposables[0].dispose();
                    }
                })
                .subscribe(b -> {
                    retry.incrementAndGet();
                    if (b || retry.get() > 2) {
                        disposables[0].dispose();
                    }
                }), 30, 30, TimeUnit.SECONDS);
    }

    private static class ListCredVnode extends AbstractListClient<Tuple2<String, JsonObject>> {
        UnicastProcessor<Tuple2<String, JsonObject>> listFlux = UnicastProcessor.create();
        int count = 0;
        String[] markers;
        ErasureServer.PayloadMetaType listType;


        ListCredVnode(StoragePool storagePool, ClientTemplate.ResponseInfo<Tuple2<String, JsonObject>[]> responseInfo,
                      List<Tuple3<String, String, String>> nodeList, String[] markers, ErasureServer.PayloadMetaType listType) {
            super(storagePool, responseInfo, nodeList);
            this.markers = markers;
            this.listType = listType;
        }

        @Override
        protected void publishResult() {
            listFlux.onComplete();
            res.onNext(true);
        }

        @Override
        protected String getKey(Tuple2<String, JsonObject> tuple2) {
            return tuple2.var1;
        }

        @Override
        protected int compareTo(Tuple2<String, JsonObject> t1, Tuple2<String, JsonObject> t2) {
            if (t2.var1.equals(t1.var1)) {
                if (t2.var2.containsKey("versionNum")) {
                    return t1.var2.getString("versionNum").compareTo(t2.var2.getString("versionNum"));
                } else {
                    return 0;
                }
            } else {
                return t1.var1.compareTo(t2.var1);
            }
        }

        @Override
        protected void handleResult(Tuple2<String, JsonObject> tuple2) {
            listFlux.onNext(tuple2);
            count++;
        }


        @Override
        protected Mono<Boolean> repair(AbstractListClient<Tuple2<String, JsonObject>>.Counter counter, List<Tuple3<String, String, String>> nodeList) {
            return Mono.just(true);
        }

        @Override
        protected void putErrorList(AbstractListClient.Counter counter) {

        }

        @Override
        protected void publishErrorList() {
        }

        /**
         * 移除父类对错误数量的判断，在失败节点大于k时仍然按成功流程继续
         */
        @Override
        public void handleComplete() {
            if (LIST_SHARDING_META_OBJ.equals(listType)) {
                super.handleComplete();
            } else if (responseInfo.successNum == 0) {
                res.onNext(false);
            } else {
                repairFlux.onNext(0);
            }
        }

        @Override
        public void handleResponse(Tuple3<Integer, ErasureServer.PayloadMetaType, Tuple2<String, JsonObject>[]> tuple) {
            int index = tuple.var1;
            if (null != tuple.var3 && tuple.var3.length > 0) {
                markers[index] = tuple.var3[tuple.var3.length - 1].var1;
            }

            super.handleResponse(tuple);
        }
    }

    public static Flux<Tuple2<String, JsonObject>> listVnodeCred(StoragePool storagePool, String vnode, List<Tuple3<String, String, String>> nodeList, MSRocksDB.IndexDBEnum indexDBEnum,
                                                                 boolean listLink, String startMarker) {
        UnicastProcessor<Tuple2<String, JsonObject>> listFlux = UnicastProcessor.create(
                Queues.<Tuple2<String, JsonObject>>unboundedMultiproducer().get());
        String[] link = listLink ? storagePool.getLink(vnode) : new String[]{vnode};
        ErasureServer.PayloadMetaType listType =  LIST_VNODE_META;
        Flux.just(ROCKS_STS_TOKEN_KEY)
                .subscribe(prefix -> {
                    UnicastProcessor<String> listController = UnicastProcessor.create();

                    //扫vnode中的数据是通过直接去扫得到的nodeList中各节点盘下此vnode的数据
                    List<SocketReqMsg> msg = nodeList.stream().map(info -> {
                        SocketReqMsg msg0 = new SocketReqMsg("", 0)
                                .put("prefix", prefix).put("lun", info.var2).put("vnode", vnode).put("link", Json.encode(link));
                        if (MSRocksDB.IndexDBEnum.UNSYNC_RECORD_DB.equals(indexDBEnum)) {
                            msg0.put("lun", MSRocksDB.getSyncRecordLun(info.var2));
                        }
                        if (MSRocksDB.IndexDBEnum.COMPONENT_RECORD_DB.equals(indexDBEnum)){
                            msg0.put("lun", MSRocksDB.getComponentRecordLun(info.var2));
                        }
                        if (MSRocksDB.IndexDBEnum.STS_TOKEN_DB.equals(indexDBEnum)) {
                            msg0.put("lun", MSRocksDB.getSTSTokenLun(info.var2));
                        }
                        if (MSRocksDB.IndexDBEnum.RABBITMQ_RECORD_DB.equals(indexDBEnum)){
                            msg0.put("lun", MSRocksDB.getRabbitmqRecordLun(info.var2));
                        }
                        return msg0;
                    }).collect(Collectors.toList());

                    String[] marker = new String[nodeList.size()];
                    for (int j = 0; j < marker.length; j++) {
                        marker[j] = startMarker == null ? "" : startMarker;
                    }

                    String[] finalMarker = marker;
                    listController.publishOn(STS_SYNC_SCHEDULER).subscribe(s -> {

                        if ("0".equalsIgnoreCase(s)) {
                            listFlux.onComplete();
                        } else {
                            if (listFlux.size() <= LOW_THRESHOLD) {
                                listNext(storagePool, finalMarker, msg, nodeList, listFlux, listController, listType);
                            } else {
                                Disposable[] disposables = new Disposable[]{null};
                                disposables[0] = Flux.interval(Duration.ofSeconds(1))
                                        .subscribe(l -> {
                                            if (listFlux.size() <= HIGH_THRESHOLD) {
                                                disposables[0].dispose();
                                                listNext(storagePool, finalMarker, msg, nodeList, listFlux, listController, listType);
                                            }
                                        });
                            }
                        }
                    }, listFlux::onError);

                    listNext(storagePool, finalMarker, msg, nodeList, listFlux, listController, listType);
                }, listFlux::onError);

        return listFlux;
    }

    private static void listNext(StoragePool storagePool, String[] marker, List<SocketReqMsg> msg, List<Tuple3<String, String, String>> nodeList,
                                 UnicastProcessor<Tuple2<String, JsonObject>> listFlux, UnicastProcessor<String> listController, ErasureServer.PayloadMetaType payloadMetaType) {
        Iterator<SocketReqMsg> iterator = msg.iterator();

        int i = 0;
        while (iterator.hasNext()) {
            iterator.next().put("marker", marker[i++]);
        }
        TypeReference<Tuple2<String, JsonObject>[]> reference = new TypeReference<Tuple2<String, JsonObject>[]>() {
        };

        ClientTemplate.ResponseInfo<Tuple2<String, JsonObject>[]> responseInfo =
                ClientTemplate.oneResponse(msg, payloadMetaType, reference, nodeList);
        ListCredVnode clientHandler = new ListCredVnode(storagePool, responseInfo, nodeList, marker, payloadMetaType);
        responseInfo.responses
                .subscribe(clientHandler::handleResponse, listFlux::onError, clientHandler::handleComplete);
        clientHandler.listFlux.subscribe(listFlux::onNext, e -> log.error("", e));
        clientHandler.res.subscribe(b -> {
            if (!b) {
                listFlux.onError(new RuntimeException("list vnode meta error!"));
                return;
            }
            if (clientHandler.count <= 0) {
                listController.onNext("0");
            } else {
                listController.onNext("1");
            }
        });
    }


}

