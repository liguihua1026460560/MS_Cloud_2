package com.macrosan.filesystem.async;

import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.utils.ReadObjClient;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import com.macrosan.message.socketmsg.SocketDataMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static com.macrosan.doubleActive.DataSynChecker.isDebug;
import static com.macrosan.doubleActive.DataSynChecker.timeoutMinute;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.httpserver.MossHttpClient.INDEX_IPS_MAP;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.rsocket.server.Rsocket.DA_RSOCKET_PORT;

@Log4j2
public class FSUnsyncRecordHandler {
    // 单次payload的数据大小上限
    public static final int CHUNK_SIZE = 64 << 10;


    public static String getFSRecordMapKey(UnSynchronizedRecord record) {
//        if (record.nodeId == null || record.nodeId <= 0) {
//            // 文件桶的s3请求，不会生成nodeId。
//            return record.bucket + File.separator + record.object;
//        }
//        return record.bucket + File.separator + record.nodeId;
        return record.bucket;
    }

    public static Mono<Boolean> dealRecord(UnSynchronizedRecord record) {
        int clusterIndex = record.index;
        long nodeId = record.nodeId;
        Map<String, String> dataMap = record.headers;
        int opt = record.opt;

        String bucket = record.bucket;
        dataMap.put("bucket", bucket);
        dataMap.put("nodeId", String.valueOf(nodeId));
        dataMap.put("async", "1");

        int currentIndex = ThreadLocalRandom.current().nextInt(0, INDEX_IPS_MAP.get(clusterIndex).length);
        if (isDebug) {
            currentIndex = 0;
        }
        String ip = INDEX_IPS_MAP.get(clusterIndex)[currentIndex];

        switch (opt) {
            case 1: {
                // nfs3 write、commit都通过这里同步。
                // 只有本地站点落盘（updateInode成功），这样的数据才认为需要同步给对端站点，否则属于缓存，本身也存在掉电丢失的可能，不需要同步。
                // 因此可能存在落盘前的数据只能在一边站点读取到的现象
                String oldInodeData = dataMap.get("oldInodeData");
                //oldInodeData不为空，只是更新数据块的存储池，不需要复制
                if (StringUtils.isNotBlank(oldInodeData)) {
                    return Mono.just(true);
                } else {
                    Inode.InodeData inodeData = Json.decodeValue(dataMap.get("inodeData"), Inode.InodeData.class);
                    Mono<Boolean> mono = getInode(bucket, nodeId)
                            .flatMap(curInode -> {
                                long fileOffset = Long.parseLong(dataMap.get("fileOffset"));
                                if (!InodeDataCache.checkInodeData(inodeData.fileName, curInode)) {
                                    log.info("fileName not match or file deleted, skip. {}, {}, {}", bucket, nodeId, Json.encode(record));
                                    return Mono.just(true);
                                }
                                dataMap.put("obj", curInode.getObjName());
                                String storage = curInode.getInodeData().get(0).storage;
                                // 如果后续要做背压，考虑用一个流订阅文件数据，根据offset和size来按顺序获取数据并推流
                                return ReadObjClient.readObj(0, storage, bucket, inodeData.fileName, 0, inodeData.size, inodeData.size, false)
//                                return ReadObjCache.readObj(curInode, fileOffset, inodeData.size)
                                        .collectList()
                                        .flatMap(list -> {
                                            SocketDataMsg socketDataMsg = new SocketDataMsg();

                                            if (list.size() == 1) {
                                                socketDataMsg.data = list.get(0).var2;
                                            } else {
                                                list.sort(Comparator.comparingInt(o -> o.var1));
                                                int size = 0;
                                                for (Tuple2<Integer, byte[]> t : list) {
                                                    size += t.var2.length;
                                                }

                                                socketDataMsg.data = new byte[size];
                                                int index = 0;
                                                for (Tuple2<Integer, byte[]> t : list) {
                                                    System.arraycopy(t.var2, 0, socketDataMsg.data, index, t.var2.length);
                                                    index += t.var2.length;
                                                }
                                            }

                                            for (String key : dataMap.keySet()) {
                                                socketDataMsg.put(key, dataMap.get(key));
                                            }

                                            if (isDebug) {
                                                log.info("start send file data, {} {} {}" , nodeId, socketDataMsg.data.length, socketDataMsg.getObjMap());
                                            }
                                            if (socketDataMsg.data.length < CHUNK_SIZE) {
                                                return sendFileAsyncResponse(opt, socketDataMsg, ip);
                                            } else {
                                                return sendFileAsyncChannel(opt, ip, dataMap, socketDataMsg.data);
                                            }

                                        });
                            });
                    return mono
                            .timeout(Duration.ofMinutes(timeoutMinute))
                            .onErrorResume(e -> {
                                log.error("deal fs record err, {}, opt:{}, nodeId:{}, bucket:{}, {}", record.index, opt, nodeId, bucket, dataMap, e);
                                return Mono.just(false);
                            });
                }
            }
            case 50: {
                Inode createInode = Json.decodeValue(dataMap.get("inode"), Inode.class);
                createInode.setBucket(bucket)
                        .setNodeId(nodeId);
                dataMap.put("inode", Json.encode(createInode));
                break;
            }
            default:
                break;
        }

        SocketReqMsg msg = new SocketReqMsg("", 0);
        msg.dataMap.putAll(dataMap);

        return sendFileAsyncResponse(opt, msg, ip)
                .timeout(Duration.ofMinutes(timeoutMinute))
                .onErrorResume(e -> {
                    log.error("deal fs record err0, {}, opt:{}, nodeId:{}, bucket:{}, {}", record.index, opt, nodeId, bucket, msg.dataMap, e);
                    return Mono.just(false);
                });
    }

    static Mono<Inode> getInode(String bucket, long nodeId) {
        return Node.getInstance().getInode(bucket, nodeId)
                .flatMap(inode -> {
                    if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                        return Mono.error(new RuntimeException("getInode err"));
                    }
                    return Mono.just(inode);
                });
    }

    public static Mono<Boolean> sendFileAsyncResponse(int opt, SocketReqMsg msg, String ip) {
        MonoProcessor<Boolean> res = MonoProcessor.create();

        List<Tuple3<String, String, String>> nodeList = Collections.singletonList(new Tuple3<>(ip, "", ""));
        List<SocketReqMsg> msgs = Collections.singletonList(msg);

        ClientTemplate.ResponseInfo<Inode> responseInfo = ClientTemplate.oneResponse(msgs, FILE_ASYNC, Inode.class, nodeList);
        responseInfo.responses.subscribe(s -> {

        }, e -> {
            log.error("sendFileAsync err, {} {}", opt, msg.dataMap, e);
            res.onNext(false);
        }, () -> {
            try {
                if (responseInfo.successNum == 1) {
                    if (isDebug) {
                        log.info("sendFileAsyncResponse done, {} {} {}", ip, opt, msg.dataMap);
                    } else {
                        log.debug("sendFileAsyncResponse done, {} {} {}", ip, opt, msg.dataMap);
                    }
                    switch (opt) {
                        case 3:
                            if (Inode.NOT_FOUND_INODE.getLinkN() == responseInfo.res[0].var2.getLinkN()) {
                                // deleteInode时没有在对端找到找到相关inode，响应为ERROR
                                res.onNext(true);
                                return;
                            }
                            break;
                        default:
                            break;
                    }
                    res.onNext(true);
                } else {
                    log.info("sendFileAsync fail, {}, {} {}", ip, opt, msg.dataMap);
                    res.onNext(false);
                }
            } catch (Exception e) {
                log.error("response err, {}", Json.encode(msg), e);
                res.onNext(false);
            }
        });

        return res;
    }

    public static Mono<Boolean> sendFileAsyncChannel(int opt, String ip, Map<String, String> map, byte[] data) {
        MonoProcessor<Boolean> res = MonoProcessor.create();

        List<Tuple3<String, String, String>> nodeList = Collections.singletonList(new Tuple3<>(ip, "", ""));

        List<UnicastProcessor<Payload>> publisher = nodeList.stream()
                .map(t -> UnicastProcessor.<Payload>create())
                .collect(Collectors.toList());

        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(publisher, String.class, nodeList, DA_RSOCKET_PORT);

        UnicastProcessor<Integer> processor = UnicastProcessor.create(Queues.<Integer>unboundedMultiproducer().get());
        FileAysncSubscriber subscriber = new FileAysncSubscriber(map, data, publisher, processor);
        responseInfo.responses
                .timeout(Duration.ofSeconds(30))
                .subscribe(s -> {
                    if (s.var2.equals(ERROR)) {
                        processor.onError(new RuntimeException("channel err, " + ip));
                        res.onNext(false);
                    } else {
                        processor.onNext(1);
                    }
                }, e -> {
                    log.error("sendFileAsyncChannel err,{}, {} {}", ip, opt, map, e);
                    processor.onComplete();
                    res.onNext(false);
                }, () -> {
                    if (responseInfo.successNum == 1) {
                        if (isDebug) {
                            log.info("sendFileAsyncChannel done, {} {} {}", ip, opt, map);
                        } else {
                            log.debug("sendFileAsyncChannel done, {} {} {}", ip, opt, map);
                        }
                        res.onNext(true);
                    } else {
                        log.info("sendFileAsyncChannel fail,{}, {} {}", ip, opt, map);
                        res.onNext(false);
                    }
                });

        processor.subscribe(subscriber);
        processor.onNext(1);
        return res;
    }

    static final class FileAysncSubscriber implements CoreSubscriber<Integer> {
        Subscription s;
        UnicastProcessor<Integer> processor;
        Map<String, String> map;
        byte[] data;
        // 长度为1
        List<UnicastProcessor<Payload>> publisher;
        int position = 0;
        int chunkSize = CHUNK_SIZE;

        FileAysncSubscriber(Map<String, String> map, byte[] data, List<UnicastProcessor<Payload>> publisher, UnicastProcessor<Integer> processor) {
            this.map = map;
            this.data = data;
            this.publisher = publisher;
            this.processor = processor;
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.s = s;
            publisher.get(0).onNext(DefaultPayload.create(Json.encode(map), START_FILE_ASYNC_CHANNEL.name()));
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Integer integer) {
            try {
                int bytesToRead = Math.min(chunkSize, data.length - position);
                byte[] chunk = new byte[bytesToRead];
                System.arraycopy(data, position, chunk, 0, bytesToRead);

                publisher.get(0).onNext(DefaultPayload.create(chunk, PUT_FILE_ASYNC_CHANNEL.name().getBytes()));
                position += bytesToRead;
                if (position >= data.length) {
                    processor.onComplete();
                }
            } catch (Exception e) {
                onError(e);
            }
        }

        @Override
        public void onError(Throwable t) {
            try {
                log.error("exception in FileAysncSubscriber : {}", map, t);
                publisher.get(0).onNext(DefaultPayload.create("put file error", ERROR.name()));
                publisher.get(0).onComplete();
                s.cancel();
            } catch (Exception e) {

            }
        }

        @Override
        public void onComplete() {
//            publisher.get(0).onNext(DefaultPayload.create("", COMPLETE_PUT_OBJECT.name()));
            publisher.get(0).onComplete();
        }
    }
}
