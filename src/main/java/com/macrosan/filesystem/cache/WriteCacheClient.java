package com.macrosan.filesystem.cache;

import com.macrosan.ec.server.WriteCacheServer;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.socketmsg.SocketDataMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.coder.Encoder;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.md5.Digest;
import com.macrosan.utils.msutils.md5.Md5Digest;
import io.vertx.core.json.Json;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.WRITE_CACHE;
import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_DQUOT;
import static com.macrosan.filesystem.cache.WriteCacheNode.putObj;
import static com.macrosan.filesystem.utils.InodeUtils.isError;
import static com.macrosan.message.jsonmsg.Inode.CAP_QUOTA_EXCCED_INODE;

@Slf4j
public class WriteCacheClient {
    private static final AtomicBoolean FLUSHING = new AtomicBoolean(false);
    private static final Set<SMB2FileId> existCacheSet = ConcurrentHashMap.newKeySet();

    public static boolean isExistCache(SMB2FileId smb2FileId) {
        return existCacheSet.contains(smb2FileId);
    }

    public static void clear(long id) {
        SMB2FileId smb2FileId = new SMB2FileId()
                .setPersistent(id)
                .setVolatile_(0);
        existCacheSet.remove(smb2FileId);
    }

    public static Mono<Boolean> cifsWrite(SMB2FileId smb2FileId, long offset, byte[] bytes, Inode inode, int flag, long allocationSize) {
        return FSQuotaUtils.addQuotaDirInfo(inode, System.currentTimeMillis(), true)
                .flatMap(i -> {
                    if (i.getLinkN() == CAP_QUOTA_EXCCED_INODE.getLinkN()) {
                        throw new NFSException(NFS3ERR_DQUOT, "can not write ,because of exceed quota.bucket:" + inode.getBucket() + ",objName:" + inode.getObjName() + ",nodeId:" + inode.getNodeId());
                    }
                    if (flag != 0 || FLUSHING.get()) {
                        return directWrite(offset, bytes, i, false);
                    } else {
                        // bytes超过allocationSize的部分不写入缓存
                        if (offset >= allocationSize) {
                            return Mono.just(true);
                        } else {
                            byte[] writeBytes;
                            if (offset + bytes.length > allocationSize) {
                                int writeLen = (int) (allocationSize - offset);
                                writeBytes = new byte[writeLen];
                                System.arraycopy(bytes, 0, writeBytes, 0, writeLen);
                            } else {
                                writeBytes = bytes;
                            }
                            return tryWriteCache(writeBytes, inode, offset)
                                    .flatMap(writeCacheRes -> {
                                        if (WriteCacheServer.writeCacheDebug) {
                                            log.info("writeCacheRes {}", writeCacheRes);
                                        }
                                        if (writeCacheRes >= 0) {
                                            existCacheSet.add(smb2FileId);
                                            // 发送下刷请求，仅下刷自身
                                            if (writeCacheRes == 0) {
                                                FsUtils.fsExecutor.submit(() -> Node.getInstance().flushWriteCache(inode, 0, 0, 2).subscribe());
                                            }
                                            return Mono.just(true);
                                        } else {
                                            return directWrite(offset, bytes, i, true);
                                        }
                                    });
                        }
                    }
                });
    }

    private static Mono<Boolean> directWrite(long offset, byte[] bytes, Inode inode, boolean flushAll) {
        // 根据文件大小决定写入数据池还是缓存池
        StoragePool dataPool = StoragePoolFactory.getStoragePool(inode.getStorage(), inode.getBucket());
        Encoder encoder = dataPool.getEncoder(bytes.length);
        encoder.put(bytes);
        Digest digest = new Md5Digest();
        digest.update(bytes);
        String md5 = Hex.encodeHexString(digest.digest());
        return putObj(encoder, inode, offset, bytes.length, md5, 1, dataPool)
                .map(inode1 -> !isError(inode1))
                .doOnNext(res -> {
                    if (flushAll && FLUSHING.compareAndSet(false, true)) {
                        // 发送下刷请求，下刷全部
                        FsUtils.fsExecutor.submit(() -> Node.getInstance().flushWriteCache(inode, 0, 0, 3).subscribe(b -> FLUSHING.compareAndSet(true, false)));
                    }
                });
    }

    private static Mono<Integer> tryWriteCache(byte[] bytes, Inode inode, long curOffset) {
        MonoProcessor<Integer> res = MonoProcessor.create();

        // 缓存副本上传桶索引节点
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(inode.getBucket());
        String vnode = pool.getBucketVnodeId(inode.getBucket());
        List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(vnode).block();

        SocketDataMsg msg = new SocketDataMsg("", 0)
                .put("inode", Json.encode(inode))
                .put("curOffset", String.valueOf(curOffset));

        List<SocketReqMsg> msgs = nodeList.stream()
                .map(tuple -> msg.copy().putData(bytes))
                .collect(Collectors.toList());


        ClientTemplate.ResponseInfo<Integer> responseInfo = ClientTemplate.oneResponse(msgs, WRITE_CACHE, Integer.class, nodeList);

        AtomicBoolean needFlush = new AtomicBoolean(false);
        responseInfo.responses.subscribe(tuple -> {
            if (SUCCESS.equals(tuple.var2) && tuple.var3 == 0) { // error时var3会被mapPayloadToTuple方法处理为null
                needFlush.set(true);
            }
            if (WriteCacheServer.writeCacheDebug) {
                log.info("{}, {}, {}, inode:{}, offset:{}, size:{}", tuple.var1, tuple.var2.name(), tuple.var3, inode.getNodeId(), curOffset, bytes.length);
            }
        }, e -> log.error("", e), () -> {
            if (responseInfo.successNum == nodeList.size()) {
                // 全部保存至写缓存
                if (needFlush.get()) {
                    res.onNext(0);
                } else {
                    res.onNext(1);
                }
            } else {
                // 有节点保存失败，直接写并下刷写缓存
                res.onNext(-1);
            }
        });

        return res;
    }

    public static Mono<Boolean> flush(Inode inode, long offset, int count, SMB2FileId smb2FileId) {
        if (existCacheSet.remove(smb2FileId)) {
            return Node.getInstance().flushWriteCache(inode, offset, count, 1);
        } else {
            return Mono.just(true);
        }
    }

    public static Mono<Boolean> remove(Inode inode, long offset, int count, SMB2FileId smb2FileId) {
        if (existCacheSet.remove(smb2FileId)) {
            return Node.getInstance().flushWriteCache(inode, offset, count, 4);
        } else {
            return Mono.just(true);
        }
    }

}
