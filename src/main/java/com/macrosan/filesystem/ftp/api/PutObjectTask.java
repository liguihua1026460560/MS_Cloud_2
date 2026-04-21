package com.macrosan.filesystem.ftp.api;

import com.macrosan.database.rocksdb.batch.BatchRocksDB;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.Utils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.ftp.Session;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.ObjectPublisher;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.coder.LimitEncoder;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.md5.Md5Digest;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.net.NetSocket;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.RandomStringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.macrosan.ec.ECUtils.publishEcError;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_FS_PUT_FILE;
import static com.macrosan.ec.error.ErrorConstant.ECErrorType.ERROR_ROLL_BACK_FILE;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.rabbitmq.RabbitMqUtils.getDiskName;
import static com.macrosan.storage.StorageOperate.PoolType.DATA;

@Log4j2
public class PutObjectTask {
    String bucket;
    String objectName;
    String versionId;
    String snapshotMark;
    Session session;
    NetSocket dataSocket;

    Md5Digest digest = new Md5Digest();
    LimitEncoder encoder;
    MonoProcessor<Boolean> res = MonoProcessor.create();
    MonoProcessor<Boolean> dataRes = MonoProcessor.create();
    MonoProcessor<Boolean> recoverDataProcessor = MonoProcessor.create();
    MonoProcessor<Boolean> rollBackProcessor = MonoProcessor.create();
    StoragePool dataPool;

    String fileName;
    long offset;
    Inode inode;

    // 参考limiter
    public PutObjectTask(Session session, NetSocket dataSocket, Inode inode, long offset, boolean requestData) {
        this.bucket = inode.getBucket();
        this.objectName = inode.getObjName();
        this.versionId = inode.getVersionId();
        this.session = session;
        this.dataSocket = dataSocket;
        this.inode = inode;
        this.offset = offset;

        StorageOperate storageOperate = new StorageOperate(DATA, inode.getObjName(), Long.MAX_VALUE);
        dataPool = StoragePoolFactory.getStoragePool(storageOperate, bucket);
        encoder = dataPool.getLimitEncoder(dataSocket.getDelegate());

        // 生成一个新的fileName
        fileName = Utils.getObjFileName(dataPool, bucket, objectName + RandomStringUtils.randomAlphanumeric(4),
                RandomStringUtils.randomAlphanumeric(32)) + '/';

        // 获得nodeList
        String vnode = dataPool.getObjectVnodeId(fileName);
        List<Tuple3<String, String, String>> nodeList = dataPool.mapToNodeInfo(vnode).block();

        // 注册数据上传
        String bucketVnode = StoragePoolFactory.getMetaStoragePool(bucket).getBucketVnodeId(bucket, objectName);
        String versionMetaKey = Utils.getVersionMetaDataKey(bucketVnode, bucket, objectName, versionId, snapshotMark);
        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("metaKey", versionMetaKey)
                .put("noGet", "1")
                .put("compression", dataPool.getCompression())
                .put("fileName", fileName)
                .put("bucket", inode.getBucket())
                .put("object", inode.getObjName())
                .put("versionId", inode.getVersionId())
                .put("storage", dataPool.getVnodePrefix());


        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> msg.copy()
                        .put("lun", tuple.var2)
                        .put("vnode", tuple.var3))
                .collect(Collectors.toList());

        List<UnicastProcessor<Payload>> publisher = nodeList.stream()
                .map(t -> {
                    UnicastProcessor<Payload> processor = UnicastProcessor.create(Queues.<Payload>unboundedMultiproducer().get());
                    return processor;
                })
                .collect(Collectors.toList());

        boolean[] first = new boolean[publisher.size()];
        Arrays.fill(first, true);
        boolean[] published = new boolean[]{false};

        for (int i = 0; i < publisher.size(); i++) {
            int index = i;
            if (requestData) {
                encoder.request(i, 1L);
            }
            encoder.data()[index].subscribe(bytes -> {
                        published[0] = true;
                        if (first[index]) {
                            if (bytes.length < dataPool.getPackageSize()) {
                                byte[] startBytes = Json.encode(msgs.get(index)).getBytes();
                                byte[] lenBytes = BatchRocksDB.toByte(startBytes.length);
                                byte[] res = new byte[bytes.length + startBytes.length + lenBytes.length];
                                System.arraycopy(lenBytes, 0, res, 0, lenBytes.length);
                                System.arraycopy(startBytes, 0, res, lenBytes.length, startBytes.length);
                                System.arraycopy(bytes, 0, res, lenBytes.length + startBytes.length, bytes.length);
                                publisher.get(index).onNext(DefaultPayload.create(res, PUT_AND_COMPLETE_PUT_OBJECT.name().getBytes()));
                                publisher.get(index).onComplete();
                            } else {
                                publisher.get(index).onNext(DefaultPayload.create(Json.encode(msgs.get(index)), START_PUT_OBJECT.name()));
                                publisher.get(index).onNext(DefaultPayload.create(bytes, PUT_OBJECT.name().getBytes()));
                            }

                            first[index] = false;
                        } else {
                            publisher.get(index).onNext(DefaultPayload.create(bytes, PUT_OBJECT.name().getBytes()));
                        }
                    },
                    e -> {
                        log.error("", e);
                        publisher.get(index).onNext(DefaultPayload.create("put file error", ERROR.name()));
                        publisher.get(index).onComplete();
                    },
                    () -> {
                        if (published[0]) {
                            publisher.get(index).onNext(DefaultPayload.create("", COMPLETE_PUT_OBJECT.name()));
                        } else {
                            publisher.get(index).onNext(DefaultPayload.create("", PUT_EMPTY_OBJECT.name()));
                        }
                        publisher.get(index).onComplete();
                    }
            );
        }


        ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.multiResponse(publisher, String.class, nodeList);

        List<Integer> errorChunksList = new ArrayList<>(dataPool.getM());
        String poolQueueTag = StoragePoolFactory.getPoolNameByPrefix(dataPool.getVnodePrefix());

        Disposable disposable = responseInfo.responses
                .subscribe(s -> {
                    if (s.var2.equals(ERROR)) {
                        errorChunksList.add(s.var1);
                        encoder.request(s.var1, Long.MAX_VALUE);
                    } else {
                        encoder.request(s.var1, 1L);
                    }

                }, e -> log.error("", e), () -> {
                    if (responseInfo.successNum == dataPool.getK() + dataPool.getM()) {
                        dataRes.onNext(true);
                    } else if (responseInfo.successNum >= dataPool.getK()) {
                        dataRes.onNext(true);

                        //订阅数据修复消息的发出。b表示k+m个元数据是否至少写上了一个。
                        recoverDataProcessor.subscribe(b0 -> {
                            if (b0) {
                                SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                        .put("errorChunksList", Json.encode(errorChunksList))
                                        .put("bucket", bucket)
                                        .put("object", objectName)
                                        .put("fileName", fileName)
                                        .put("versionId", versionId)
                                        .put("storage", dataPool.getVnodePrefix())
                                        .put("nodeId", String.valueOf(inode.getNodeId()))
                                        .put("fileSize", String.valueOf(encoder.size()))
                                        .put("poolQueueTag", poolQueueTag)
                                        .put("fileOffset", String.valueOf(offset));
                                publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_FS_PUT_FILE);
                            }
                        });

                    } else {
                        dataRes.onNext(false);
                        SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                .put("bucket", inode.getBucket())
                                .put("object", inode.getObjName())
                                .put("fileName", fileName)
                                .put("storage", dataPool.getVnodePrefix())
                                .put("poolQueueTag", poolQueueTag);
                        log.debug("【publishRoll】 res:{} >>>>>> errorMsg:{}", responseInfo.res, errorMsg);
                        ECUtils.publishEcError(responseInfo.res, nodeList, errorMsg, ERROR_ROLL_BACK_FILE);
                    }

                    rollBackProcessor.subscribe(noExceedRedundancy -> {
                        if (noExceedRedundancy) {
                            SocketReqMsg errorMsg = new SocketReqMsg("", 0)
                                    .put("bucket", inode.getBucket())
                                    .put("object", inode.getObjName())
                                    .put("fileName", fileName)
                                    .put("storage", dataPool.getVnodePrefix())
                                    .put("poolQueueTag", poolQueueTag)
                                    .put("lun", getDiskName(nodeList.get(0).var1, nodeList.get(0).var2));
                            ObjectPublisher.publish(CURRENT_IP, errorMsg, ERROR_ROLL_BACK_FILE);
                            log.debug("rollBack ino: {}, obj: {}, fileName: {}", inode.getNodeId(), inode.getObjName(), fileName);
                        }
                    });
                });
    }

    public void request() {
        for (int i = 0; i < dataPool.getK() + dataPool.getM(); i++) {
            encoder.request(i, 1L);
        }
    }

    public void handle(Buffer b) {
        byte[] bytes = b.getBytes();
        // 数据处理
        digest.update(bytes);
        encoder.put(bytes);
    }

    public void complete() {
        // 数据编码结束
        encoder.complete();

        // 计算md5
        String md5 = Hex.encodeHexString(digest.digest());

        dataRes.subscribe(b -> {
            if (b) {
                // 更新inodeData
                Inode.InodeData inodeData = new Inode.InodeData()
                        .setOffset(0)
                        .setEtag(md5)
                        .setStorage(dataPool.getVnodePrefix())
                        .setSize(encoder.size())
                        .setFileName(fileName);
                Node.getInstance().updateInodeData1(bucket, inode.getNodeId(), offset, inodeData, "", objectName)
                        .subscribe(i -> {
                            if (InodeUtils.isError(i)) {
                                res.onNext(false);
                            } else {
                                if (!i.isDeleteMark()) {
                                    recoverDataProcessor.onNext(true);
                                } else {
                                    rollBackProcessor.onNext(true);
                                }
                                res.onNext(true);
                            }
                        });
            } else {
                res.onNext(false);
            }
        });

    }
}