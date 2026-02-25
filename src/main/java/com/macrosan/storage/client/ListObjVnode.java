package com.macrosan.storage.client;

import com.macrosan.ec.Utils;
import com.macrosan.ec.migrate.ScannerConfig;
import com.macrosan.ec.rebuild.ReBuildRunner;
import com.macrosan.ec.rebuild.RebuildRabbitMq;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.RequeueMQException;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.client.ClientTemplate.ResponseInfo;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.LIST_VNODE_OBJ;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.storage.client.ListMetaVnode.SCAN_MIGRATE_POSITION_PREFIX;
import static com.macrosan.storage.client.ListMetaVnode.vKeyMap;

/**
 * 获得数据池中vnode对应的所有文件名
 *
 * @author gaozhiyuan
 */
@Log4j2
public class ListObjVnode {
    private static class ListVnode extends AbstractListClient<FileMeta> {
        UnicastProcessor<FileMeta> listFlux = UnicastProcessor.create();
        boolean listComplete = true;
        String[] markers;
        boolean[] hasData = new boolean[nodeList.size()];
        String marker = "";
        int dstDiskIndex;
        Set<String> diskDiskListResult;

        protected ListVnode(StoragePool storagePool, ResponseInfo<FileMeta[]> responseInfo,
                            List<Tuple3<String, String, String>> nodeList, String[] markers, int dstDiskIndex) {
            super(storagePool, responseInfo, nodeList);
            this.markers = markers;
            this.dstDiskIndex = dstDiskIndex;
        }

        @Override
        protected void publishResult() {
            diskDiskListResult = null;
            if (dstDiskIndex != -1) {
                // 目标盘 marker 每次都与其他节点一致，保证list时与其他节点同步
                hasData[dstDiskIndex] = true;
            }
            for (int i = 0; i < markers.length; i++) {
                if (hasData[i]) {
                    markers[i] = marker;
                }
            }
            listComplete = linkedList.isEmpty();
            listFlux.onComplete();
            res.onNext(true);
        }

        @Override
        protected String getKey(FileMeta fileMeta) {
            return fileMeta.getKey();
        }

        @Override
        protected int compareTo(FileMeta t1, FileMeta t2) {
            return t1.getKey().compareTo(t2.getKey());
        }

        @Override
        protected void handleResult(FileMeta fileMeta) {
            if (diskDiskListResult != null && diskDiskListResult.contains(fileMeta.getKey())) {
                // 目标盘已存在的数据则不进行处理
                return;
            }
            listFlux.onNext(fileMeta);
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

        @Override
        public void handleResponse(Tuple3<Integer, ErasureServer.PayloadMetaType, FileMeta[]> tuple) {
            int index = tuple.var1;
            if (index == dstDiskIndex && tuple.var2.equals(SUCCESS)) {
                // 目标盘返回的数据，不需要与其他节点返回数据进行合并
                // 这些数据目标盘已存在，无需再进行重构或迁移，则从最终合并后到数据中将 目标盘返回数据进行移除
                diskDiskListResult = Arrays.stream(tuple.var3)
                        .map(FileMeta::getKey)
                        .collect(Collectors.toSet());
                if (nodeList.size() > pool.getM() + pool.getK()) {
                    // 加盘时，由于映射未更新，为了扫描目标盘将 目标盘加入到nodelist中，为了不影响结果判断，successNum-1
                    responseInfo.successNum--;
                }
                return;
            }
            if (null != tuple.var3 && tuple.var3.length > 0) {
                hasData[index] = true;
                FileMeta fileMeta = tuple.var3[tuple.var3.length - 1];
                if (StringUtils.isBlank(marker) || fileMeta.getKey().compareTo(marker) < 0) {
                    marker = fileMeta.getKey();
                }
            }

            super.handleResponse(tuple);
        }
    }

    public static Flux<FileMeta> listVnodeObj(ScannerConfig config) {
        UnicastProcessor<FileMeta> listFlux = UnicastProcessor.create();
        UnicastProcessor<String> listController = UnicastProcessor.create();
        String vKey = config.getVKey();
        StoragePool storagePool = config.getPool();
        List<Tuple3<String, String, String>> nodeList = new ArrayList<>(config.getNodeList());
        String[] link = config.isListLink() ? storagePool.getLink(config.getVnode()) : new String[]{config.getVnode()};

        final boolean removeDisk = StringUtils.isNotBlank(vKey) && !vKey.contains(SCAN_MIGRATE_POSITION_PREFIX);
        final boolean addDisk = StringUtils.isNotBlank(vKey) && vKey.contains(SCAN_MIGRATE_POSITION_PREFIX);
        // 获取重构迁移的目标盘
        int dstDiskIndex;
        if (addDisk) {
            // 加盘扫描，由于映射还未更新，则将目标盘 添加到nodeList中，对目标盘也进行扫描
            nodeList.add(new Tuple3<>(CURRENT_IP, Utils.getLunNameByDisk(config.getDstDisk()), config.getVnode()));
            dstDiskIndex = nodeList.size() - 1;
        } else if (removeDisk) {
            // 重构扫描，映射已经更新，直接获取目标盘的索引
            dstDiskIndex = IntStream.range(0, nodeList.size())
                    .filter(i -> nodeList.get(i).getVar1().equals(CURRENT_IP) && nodeList.get(i).getVar2().contains(Utils.getLunNameByDisk(config.getDstDisk())))
                    .findFirst()
                    .orElse(-1);
        } else {
            dstDiskIndex = -1;
        }
        List<SocketReqMsg> msg = nodeList.stream().map(info -> new SocketReqMsg("", 0)
                        .put("lun", info.var2).put("vnode", config.getVnode()).put("link", Json.encode(link)))
                .collect(Collectors.toList());

        if (removeDisk) {
            vKey = "#_obj_" + vKey;
            vKeyMap.computeIfAbsent(vKey, k -> new AtomicLong());
        }

        String[] marker = new String[nodeList.size()];
        Arrays.fill(marker, "");

        try {
            if (removeDisk) {
                String str = RebuildRabbitMq.getMaster().get(vKey);
                if (str != null) {
                    String[] oldMarkers = Json.decodeValue(str, String[].class);
                    if (marker.length == oldMarkers.length) {
                        marker = oldMarkers;
                    } else {
                        // 加盘时 nodeList会多一个，如果redis里面存有之前老的marker 则长度可能对不上，则将oldMarkers设置为nodeList的长度
                        System.arraycopy(oldMarkers, 0, marker, 0, Math.min(marker.length, oldMarkers.length));
                    }
                }
            }
        } catch (Exception e) {

        }

        String finalVKey = vKey;
        String[] finalMarker = marker;
        listController.subscribe(s -> {
            Disposable[] disposables = new Disposable[]{null};
            if (s.equalsIgnoreCase("")) {
                disposables[0] = Flux.interval(Duration.ofSeconds(1L)).filter(l -> listFlux.size() < 1000)
                        .subscribe(l -> {
                            if (listFlux.isTerminated()) {
                                disposables[0].dispose();
                                return;
                            }
                            listNext(storagePool, finalMarker, msg, nodeList, listFlux, listController, config.getDstDisk(), finalVKey, dstDiskIndex, removeDisk);
                            disposables[0].dispose();
                        });
            }
            if (s.equalsIgnoreCase("0")) {
                try {
                    ListMetaVnode.consumerComplete(Collections.singleton(finalVKey), listFlux);
                    RebuildRabbitMq.getMaster().del(finalVKey);
                } catch (Exception e) {

                }
            }

        });

        listController.onNext("");

        return listFlux;
    }

    private static void listNext(StoragePool storagePool, String[] marker, List<SocketReqMsg> msg, List<Tuple3<String, String, String>> nodeList,
                                 UnicastProcessor<FileMeta> listFlux, UnicastProcessor<String> listController, String disk, String vKey, int targetDiskIndex, boolean removeDisk) {
        Iterator<SocketReqMsg> iterator = msg.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            iterator.next().put("marker", marker[i++]);
        }

        try {
            if (removeDisk) {
                RebuildRabbitMq.getMaster().set(vKey, Json.encode(marker));
            }
        } catch (Exception e) {

        }

        ResponseInfo<FileMeta[]> responseInfo = ClientTemplate.oneResponse(msg, LIST_VNODE_OBJ, FileMeta[].class, nodeList);
        ListVnode clientHandler = new ListVnode(storagePool, responseInfo, nodeList, marker, targetDiskIndex);
        responseInfo.responses.subscribe(clientHandler::handleResponse, e -> {
            log.error("", e);
            listFlux.onError(e);
        }, clientHandler::handleComplete);

        // 非重构，则将扫描到数据返回上游
        if (!removeDisk) {
            clientHandler.listFlux.subscribe(listFlux::onNext);
            clientHandler.res.subscribe(b -> {
                if (clientHandler.listComplete) {
                    listFlux.onComplete();
                } else {
                    listController.onNext("");
                }
            });
            return;
        }
        // 重构，则直接对扫描到数据进行处理
        clientHandler.res.flatMap(b -> {
            MonoProcessor<Integer> res = MonoProcessor.create();
            if (!b) {
                listFlux.onError(new RequeueMQException("list object vnode error!"));
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
                vKeyMap.get(vKey).getAndAdd(size);
                clientHandler.listFlux.publishOn(DISK_SCHEDULER).subscribe(new BaseSubscriber<FileMeta>() {
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        request(1);
                    }

                    @Override
                    protected void hookOnNext(FileMeta meta) {
                        listFlux.onNext(meta);
                        ReBuildRunner.getInstance().addObjTask(msg.get(0).get("vnode"), meta, storagePool, disk, vKey, res).doFinally(d -> {
                            request(1);
                        }).subscribe(b -> {
                        }, e -> log.error("", e));
                    }
                });
            }
            return res;
        }).subscribe(num -> {
            if (num == -1) {
                listController.onComplete();
                return;
            }
            if (num == 0) {
                listController.onNext("0");
            } else {
                listController.onNext("");
            }
        });
    }
}
