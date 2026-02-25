package com.macrosan.storage.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.database.rocksdb.MSRocksDB.IndexDBEnum;
import com.macrosan.ec.Utils;
import com.macrosan.ec.migrate.ScannerConfig;
import com.macrosan.ec.rebuild.*;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.rabbitmq.RequeueMQException;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.utils.aggregation.AggregationUtils;
import com.macrosan.utils.functional.ExpiringSet;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.*;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.ROCKS_AGGREGATION_RATE_PREFIX;
import static com.macrosan.constants.SysConstants.ROCKS_FILE_META_PREFIX;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class ListMetaVnode {
    private static class ListVnode extends AbstractListClient<Tuple2<String, JsonObject>> {
        UnicastProcessor<Tuple2<String, JsonObject>> listFlux = UnicastProcessor.create();
        String[] markers;
        ErasureServer.PayloadMetaType listType;
        String runningKey;
        boolean rePublishVnode;
        String vKey;
        String finalKey;
        boolean migrate;
        boolean[] hasData = new boolean[nodeList.size()];
        Tuple2<String, JsonObject> minLastMetaData = null;
        int dstDiskIndex;
        Set<String> diskDiskListResult;
        boolean listComplete = true;

        ListVnode(StoragePool storagePool, ResponseInfo<Tuple2<String, JsonObject>[]> responseInfo,
                  List<Tuple3<String, String, String>> nodeList, String[] markers, ErasureServer.PayloadMetaType listType,
                  String runningKey, String vKey, String finalKey, boolean migrate, int dstDiskIndex) {
            super(storagePool, responseInfo, nodeList);
            this.markers = markers;
            this.listType = listType;
            this.runningKey = runningKey;
            this.vKey = vKey;
            this.finalKey = finalKey;
            this.migrate = migrate;
            this.dstDiskIndex = dstDiskIndex;
        }

        @Override
        protected void publishResult() {
            diskDiskListResult = null;
            if (minLastMetaData != null) {
                if (dstDiskIndex != -1) {
                    // 目标盘 marker 每次都与其他节点一致，保证list时与其他节点同步
                    hasData[dstDiskIndex] = true;
                }
                for (int i = 0; i < markers.length; i++) {
                    if (hasData[i]) {
                        markers[i] = getKey(minLastMetaData);
                    }
                }
            }
            listComplete = linkedList.isEmpty();
            trySendSignal();
            listFlux.onComplete();
            res.onNext(true);
        }

        /**
         * 尝试发送信号，接收到信号后会写入marker到redis
         */
        private void trySendSignal() {
            if (!migrate || listComplete) {
                return;
            }
            String signalKey = SIGNAL_PREFIX + finalKey;
            migrateVKeyMap.computeIfAbsent(vKey, k -> new ConcurrentHashMap<>()).compute(signalKey, (k, v) -> {
                if (v == null) {
                    return new AtomicInteger(1);
                }
                if (v.incrementAndGet() >= 10) {
                    // list 10次发送一次信号，用于保存list的maker
                    listFlux.onNext(new Tuple2<>(signalKey, new JsonObject().put("marker", Json.encode(markers))));
                    v.set(0);
                }
                return v;
            });
        }

        @Override
        protected String getKey(Tuple2<String, JsonObject> tuple2) {
            return tuple2.var1;
        }

        @Override
        protected int compareTo(Tuple2<String, JsonObject> t1, Tuple2<String, JsonObject> t2) {
            if (t2.var1.equals(t1.var1)) {
                if (t1.var1.startsWith(ROCKS_AGGREGATION_RATE_PREFIX) && t2.var1.startsWith(ROCKS_AGGREGATION_RATE_PREFIX)) {
                    if (t1.var2.getString("value").equals(t2.var2.getString("value"))) {
                        return 0;
                    }
                    return t1.var2.getString("value").compareTo(t2.var2.getString("value"));
                } else if (t2.var2.containsKey("versionNum")) {
                    return t1.var2.getString("versionNum").compareTo(t2.var2.getString("versionNum"));
                } else {
                    return 0;
                }
            } else {
                return t1.var1.compareTo(t2.var1);
            }
        }

        @Override
        protected Tuple2<String, JsonObject> mergeOperator(Tuple2<String, JsonObject> t1, Tuple2<String, JsonObject> t2) {
            if (t1 == null || t2 == null) {
                return null;
            }
            if (!t1.var1.startsWith(ROCKS_AGGREGATION_RATE_PREFIX) || !t2.var1.startsWith(ROCKS_AGGREGATION_RATE_PREFIX)) {
                return null;
            }
            if (!t1.var1.equals(t2.var1)) {
                return null;
            }
            BitSet b1 = AggregationUtils.deserialize(t1.var2.getString("value"));
            BitSet b2 = AggregationUtils.deserialize(t2.var2.getString("value"));
            b1.and(b2);
            String finalValue = AggregationUtils.serialize(b1);
            return new Tuple2<>(t1.var1, new JsonObject().put("versionNum", "0").put("value", finalValue));
        }

        @Override
        protected void handleResult(Tuple2<String, JsonObject> tuple2) {
            if (diskDiskListResult != null && diskDiskListResult.contains(tuple2.var1)) {
                // 目标盘已存在的数据则不进行处理
                return;
            }
            listFlux.onNext(tuple2);
        }

        @Override
        protected Mono<Boolean> repair(Counter counter, List<Tuple3<String, String, String>> nodeList) {
            return Mono.just(true);
        }

        @Override
        protected void putErrorList(Counter counter) {

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
            if (index == dstDiskIndex && tuple.var2.equals(SUCCESS)) {
                // 目标盘返回的数据，不需要与其他节点返回数据进行合并
                // 这些数据目标盘已存在，无需再进行重构或迁移，则从最终合并后到数据中将 目标盘返回数据进行移除
                diskDiskListResult = Arrays.stream(tuple.var3)
                        .map(Tuple2::var1)
                        .collect(Collectors.toSet());
                if (nodeList.size() > pool.getM() + pool.getK()) {
                    // 加盘时，由于映射未更新，为了扫描目标盘将 目标盘加入到nodelist中，为了不影响结果判断，successNum-1
                    responseInfo.successNum--;
                }
                return;
            }
            if (null != tuple.var3 && tuple.var3.length > 0) {
                hasData[index] = true;
                Tuple2<String, JsonObject> metaData = tuple.var3[tuple.var3.length - 1];
                minLastMetaData = minLastMetaData == null ? metaData : compareTo(metaData, minLastMetaData) < 0 ? metaData : minLastMetaData;
            }
            if (tuple.var2.equals(REBUILD_CREATE_CHECK_POINT) && runningKey != null) {
                // 节点未创建checkpoint
                Tuple3<String, String, String> nodeInfo = nodeList.get(index);
                String failLun = RebuildCheckpointUtil.nodeListMapToLun(nodeInfo);
                ExpiringSet<String> expiringSet = RebuildCheckpointManager.CHECK_POINT_FAIL_CACHE.computeIfAbsent(runningKey, k -> new ExpiringSet<>());
                if (!expiringSet.contains(failLun) && !RemovedDisk.getInstance().contains(failLun) && RabbitMqUtils.diskIsAvailable(failLun)) {
                    // 盘没有被移除，则需要checkpoint
                    rePublishVnode = true;
                }
                return;
            }

            super.handleResponse(tuple);
        }
    }

    private static final int HIGH_THRESHOLD = 20_000;
    private static final int LOW_THRESHOLD = 10_000;

    public static Map<String, AtomicLong> vKeyMap = new ConcurrentHashMap<>();
    public static Map<String, Map<String, AtomicInteger>> migrateVKeyMap = new ConcurrentHashMap<>();
    public static final String SIGNAL_PREFIX = "signal";
    public static final String SCAN_MIGRATE_POSITION_PREFIX = "scan_migrate_position_";

    public static void consumerComplete(Set<String> vKeySet, UnicastProcessor<?> listFlux) {
        for (String vkey : vKeySet) {
            vKeyMap.remove(vkey);
        }
        listFlux.onComplete();
    }

    public static void consumerError(Set<String> vKeySet, UnicastProcessor<?> listFlux, Throwable error) {
        for (String vKey : vKeySet) {
            vKeyMap.remove(vKey);
        }
        listFlux.onError(error);
    }

    public static Flux<Tuple2<String, JsonObject>> listVnodeMeta(ScannerConfig config) {
        UnicastProcessor<Tuple2<String, JsonObject>> listFlux = UnicastProcessor.create(
                Queues.<Tuple2<String, JsonObject>>unboundedMultiproducer().get());
        AtomicInteger completeNum = new AtomicInteger();
        AtomicBoolean isEnd = new AtomicBoolean(false);

        String runningKey = config.getRunningKey();
        String vKey = config.getVKey();
        StoragePool storagePool = config.getPool();
        final List<Tuple3<String, String, String>> nodeList = new ArrayList<>(config.getNodeList());
        IndexDBEnum indexDBEnum = config.getIndexDBEnum();
        String disk = config.getDstDisk();

        String[] link = config.isListLink() ? storagePool.getLink(config.getVnode()) : new String[]{config.getVnode()};
        ErasureServer.PayloadMetaType listType = LIST_VNODE_META;
        final boolean removeDisk = StringUtils.isNotBlank(vKey) && !vKey.contains(SCAN_MIGRATE_POSITION_PREFIX);
        final boolean addDisk = StringUtils.isNotBlank(vKey) && vKey.contains(SCAN_MIGRATE_POSITION_PREFIX);
        Set<String> listVKeySet = new HashSet<>();
        if (removeDisk && !"remove_node".equals(config.getOperator())) {
            // 不发往需要恢复数据的盘
            nodeList.removeIf(t3 -> t3.var1.equals(CURRENT_IP) && disk.contains(t3.var2));
        }
        AtomicInteger dstDiskIndex = new AtomicInteger(-1);
        getPrefixes(nodeList, indexDBEnum, removeDisk, disk, runningKey)
                .flatMapMany(prefixes -> {
                    completeNum.set(prefixes.length);
                    if (prefixes.length == 0) {
                        listFlux.onComplete();
                    }
                    if (addDisk) {
                        // 加盘扫描，由于映射还未更新，则将目标盘 添加到nodeList中，对目标盘也进行扫描
                        nodeList.add(new Tuple3<>(CURRENT_IP, Utils.getLunNameByDisk(config.getDstDisk()), config.getVnode()));
                        dstDiskIndex.set(nodeList.size() - 1);
                    }
                    return Flux.fromArray(prefixes);
                })
                .subscribe(prefix -> {
                    if (".".equals(prefix) || ROCKS_FILE_META_PREFIX.equals(prefix)) {
                        if (completeNum.decrementAndGet() == 0) {
                            listFlux.onComplete();
                        }
                        return;
                    }

                    String finalKey = prefix + "_" + indexDBEnum.name().toLowerCase(Locale.ROOT) + "_" + vKey;
                    if (removeDisk) {
                        if (ReBuildRunner.getInstance().prefixList.contains(prefix)) {
                            if (completeNum.decrementAndGet() == 0) {
                                listFlux.onComplete();
                            }
                            return;
                        }
                        vKeyMap.computeIfAbsent(finalKey, key -> new AtomicLong());
                        listVKeySet.add(finalKey);
                    }

                    UnicastProcessor<String> listController = UnicastProcessor.create();


                    List<SocketReqMsg> msg = nodeList.stream().map(info -> {
                        SocketReqMsg msg0 = new SocketReqMsg("", 0)
                                .put("prefix", prefix)
                                .put("lun", MSRocksDB.getLunByIndexDBEnum(info.var2, indexDBEnum))
                                .put("vnode", config.getVnode())
                                .put("link", Json.encode(link))
                                .put("remove", String.valueOf(removeDisk));
                        return msg0;
                    }).collect(Collectors.toList());

                    String[] marker = new String[nodeList.size()];
                    Arrays.fill(marker, "");
                    try {
                        String str = null;
                        if (removeDisk) {
                            str = RebuildRabbitMq.getMaster().get(finalKey);
                        } else if (addDisk) {
                            str = RebuildRabbitMq.getMaster().hget(vKey, finalKey);
                        }
                        if (str != null) {
                            String[] oldMarkers = Json.decodeValue(str, String[].class);
                            if (marker.length == oldMarkers.length) {
                                marker = oldMarkers;
                            } else {
                                // 加盘时 nodeList会多一个，如果redis里面存有之前老的marker 则长度可能对不上，则将oldMarkers设置为nodeList的长度
                                System.arraycopy(oldMarkers, 0, marker, 0, Math.min(marker.length, oldMarkers.length));
                            }
                        }
                    } catch (Exception e) {

                    }

                    String[] finalMarker = marker;
                    listController.publishOn(DISK_SCHEDULER).subscribe(s -> {

                        if ("0".equalsIgnoreCase(s)) {
                            if (removeDisk) {
                                try {
                                    if (completeNum.decrementAndGet() == 0) {
                                        consumerComplete(listVKeySet, listFlux);
                                    }
                                    RebuildRabbitMq.getMaster().del(finalKey);
                                } catch (Exception e) {

                                }
                            } else {
                                if (completeNum.decrementAndGet() == 0) {
                                    listFlux.onComplete();
                                }
                            }
                        } else if ("-1".equalsIgnoreCase(s)) {
                            if (completeNum.decrementAndGet() == 0) {
                                // 等待该vnode上所有前缀的list都暂停后，重新发布到mq中
                                throw new RequeueMQException("list vnode stop,need rePublish mq");
                            }
                        } else {
                            if (listFlux.size() <= LOW_THRESHOLD) {
                                if (!isEnd.get()) {
                                    listNext(storagePool, finalMarker, msg, nodeList, listFlux, listController, listType, disk, vKey, finalKey, runningKey, removeDisk, addDisk, dstDiskIndex.get());
                                } else {
                                    listController.onComplete();
                                }
                            } else {
                                Disposable[] disposables = new Disposable[]{null};
                                disposables[0] = Flux.interval(Duration.ofSeconds(1))
                                        .subscribe(l -> {
                                            if (listFlux.size() <= HIGH_THRESHOLD) {
                                                disposables[0].dispose();
                                                if (!isEnd.get()) {
                                                    listNext(storagePool, finalMarker, msg, nodeList, listFlux, listController, listType, disk, vKey, finalKey, runningKey, removeDisk, addDisk, dstDiskIndex.get());
                                                } else {
                                                    listController.onComplete();
                                                }
                                            }
                                        });
                            }
                        }
                    }, e -> consumerError(listVKeySet, listFlux, e));

                    listNext(storagePool, finalMarker, msg, nodeList, listFlux, listController, listType, disk, vKey, finalKey, runningKey, removeDisk, addDisk, dstDiskIndex.get());
                }, listFlux::onError);

        return listFlux;
    }

    public static void listNext(StoragePool storagePool, String[] marker, List<SocketReqMsg> msg, List<Tuple3<String, String, String>> nodeList,
                                UnicastProcessor<Tuple2<String, JsonObject>> listFlux, UnicastProcessor<String> listController, ErasureServer.PayloadMetaType payloadMetaType,
                                String disk, String vKey, String finalKey, String runningKey, boolean removeDisk, boolean migrate, int dstDiskIndex) {
        Iterator<SocketReqMsg> iterator = msg.iterator();

        int i = 0;
        while (iterator.hasNext()) {
            iterator.next().put("marker", marker[i++]);
        }
        TypeReference<Tuple2<String, JsonObject>[]> reference = new TypeReference<Tuple2<String, JsonObject>[]>() {
        };

        try {
            if (removeDisk) {
                RebuildRabbitMq.getMaster().set(finalKey, Json.encode(marker));
            }
        } catch (Exception e) {

        }

        ResponseInfo<Tuple2<String, JsonObject>[]> responseInfo =
                ClientTemplate.oneResponse(msg, payloadMetaType, reference, nodeList);
        ListVnode clientHandler = new ListVnode(storagePool, responseInfo, nodeList, marker, payloadMetaType, runningKey, vKey, finalKey, migrate, dstDiskIndex);
        responseInfo.responses
                .subscribe(clientHandler::handleResponse, listFlux::onError, clientHandler::handleComplete);
        // 非重构，则将扫描到数据返回上游
        if (!removeDisk) {
            clientHandler.listFlux.subscribe(listFlux::onNext, e -> log.error("", e));
            clientHandler.res.subscribe(b -> {
                if (!b) {
                    listFlux.onError(new RuntimeException("list vnode meta error!"));
                    return;
                }
                if (clientHandler.listComplete) {
                    listController.onNext("0");
                } else {
                    listController.onNext("1");
                }
            });
            return;
        }
        // 重构，则直接对扫描到数据进行处理
        clientHandler.res.flatMap(b -> {
            MonoProcessor<Integer> res = MonoProcessor.create();
            if (!b) {
                listFlux.onError(new RequeueMQException("list vnode meta error!"));
                res.onNext(-1);
                return res;
            }
            if (clientHandler.listComplete) {
                res.onNext(0);
            } else if (clientHandler.listFlux.isEmpty()) {
                // 没有list完成，但是list到的数据是无效的，因此继续往后面进行list
                res.onNext(1);
            } else {
                int size = clientHandler.listFlux.size();
                vKeyMap.get(finalKey).getAndAdd(size);
                clientHandler.listFlux.publishOn(DISK_SCHEDULER).subscribe(new BaseSubscriber<Tuple2<String, JsonObject>>() {
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        request(1);
                    }

                    @Override
                    protected void hookOnNext(Tuple2<String, JsonObject> t) {
                        listFlux.onNext(t);
                        ReBuildRunner.getInstance().addMetaTask(t, storagePool, disk, finalKey, res).doFinally(d -> {
                            request(1);
                        }).subscribe(b -> {
                        }, e -> log.error("", e));
                    }
                });
            }
            return res;
        }).subscribe(num -> {
            if (num == -1) {
                return;
            }
            if (clientHandler.rePublishVnode) {
                // 需要暂停本轮list，重新发布到mq中
                listController.onNext("-1");
                return;
            }
            if (num == 1) {
                listController.onNext("1");
            } else {
                listController.onNext("0");
            }
        }, e -> log.error("", e));
    }

    /**
     * 获得rocksDB key的前缀。
     */
    private static Mono<String[]> getPrefixes(List<Tuple3<String, String, String>> nodeList, IndexDBEnum indexDBEnum, boolean removeDisk, String disk, String runningKey) {
        List<SocketReqMsg> msgs = nodeList.stream()
                .map(t -> {
                    SocketReqMsg msg0 = new SocketReqMsg("", 0)
                            .put("removeDisk", String.valueOf(removeDisk))
                            .put("lun", MSRocksDB.getLunByIndexDBEnum(t.var2, indexDBEnum));
                    return msg0;
                })
                .collect(Collectors.toList());

        String[] prefix = new String[nodeList.size()];
        for (int i = 0; i < prefix.length; i++) {
            prefix[i] = "";
        }

        UnicastProcessor<Long> processor = UnicastProcessor.create();
        MonoProcessor<String[]> res = MonoProcessor.create();
        Set<String> prefixes = new HashSet<>();

        processor.subscribe(l -> {
            ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, LIST_VNODE_META_MARKER, String.class, nodeList);

            responseInfo.responses.subscribe(t -> {
                if (t.var2 == SUCCESS) {
                    prefixes.add(t.var3);
                    prefix[t.var1] = t.var3;
                }
                if (t.var2 == REBUILD_CREATE_CHECK_POINT) {
                    String failLun = RebuildCheckpointUtil.nodeListMapToLun(nodeList.get(t.var1));
                    ExpiringSet<String> expiringSet = RebuildCheckpointManager.CHECK_POINT_FAIL_CACHE.computeIfAbsent(runningKey, k -> new ExpiringSet<>());
                    if (!expiringSet.contains(failLun) && !RemovedDisk.getInstance().contains(failLun) && RabbitMqUtils.diskIsAvailable(failLun)) {
                        throw new RequeueMQException("get prefixes stop,need rePublish mq");
                    }
                }
            }, res::onError, () -> {
                boolean end = true;
                for (int i = 0; i < prefix.length; i++) {
                    if (!prefix[i].equalsIgnoreCase(msgs.get(i).get("prefix"))) {
                        end = false;
                        msgs.get(i).put("prefix", prefix[i]);
                    }
                }

                if (end) {
                    res.onNext(prefixes.toArray(new String[0]));
                } else {
                    processor.onNext(l + 1);
                }
            });
        });

        processor.onNext(0L);

        return res;
    }
}
