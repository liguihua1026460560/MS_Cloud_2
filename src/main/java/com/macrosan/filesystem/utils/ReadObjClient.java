package com.macrosan.filesystem.utils;

import com.macrosan.ec.server.ErasureServer.PayloadMetaType;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.message.jsonmsg.ChunkFile;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.LocalPayload;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.StoragePoolType;
import com.macrosan.storage.coder.fs.FsErasureDecoder;
import com.macrosan.storage.coder.fs.FsReplicaDecoder;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.EscapeException;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.macrosan.constants.SysConstants.ROCKS_CHUNK_FILE_KEY;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.GET_OBJECT;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.rabbitmq.RabbitMqUtils.CURRENT_IP;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;

@Log4j2
public class ReadObjClient {
    public static Flux<Tuple2<Integer, byte[]>> readFromList(int readIndex, long offset, long size, String bucket,
                                                             List<Inode.InodeData> list, boolean isPrefetch) {
        List<Flux<Tuple2<Integer, byte[]>>> resList = new LinkedList<>();

        long cur = 0;
        long readOffset = offset;
        long readEnd = offset + size;

        for (Inode.InodeData data : list) {
            long curEnd = cur + data.size;
            if (readEnd < cur) {
                break;
            } else if (readOffset > curEnd) {
                cur = curEnd;
            } else {
                long readFileOffset = data.offset + (readOffset - cur);
                int readFileSize = (int) (Math.min(readEnd, curEnd) - readOffset);

                if (readFileSize > 0) {
                    AtomicLong sum = new AtomicLong();
                    resList.add(readObj(readIndex, data.storage, bucket, data.fileName, readFileOffset, readFileSize,
                            data.size + data.offset + data.size0, isPrefetch)
                            .doOnNext(b -> {
                                sum.addAndGet(b.var2.length);
                            })
                            .doOnComplete(() -> {
                                if (sum.get() != readFileSize) {
                                    log.error("read size error {}:{} {}", data.fileName, readFileOffset, readFileSize);
                                }
                            }));
                }

                readOffset += readFileSize;
                readIndex += readFileSize;

                if (readOffset >= readEnd) {
                    break;
                }

                cur = curEnd;
            }
        }

        if (resList.size() == 1) {
            return resList.get(0);
        } else {
            //设置并发
            return Flux.merge(Flux.fromStream(resList.stream()), 1, Queues.XS_BUFFER_SIZE);
        }
    }

    //文件接口使用，输入的size不会太大，所有读取数据会在一次rsocket消息中全部返回。
    public static Flux<Tuple2<Integer, byte[]>> readObj(int readIndex, String storage, String bucket, String fileName,
                                                        long offset, long size, long fileSize, boolean isPrefetch) {
        if (StringUtils.isBlank(fileName)) {
            return Flux.just(new Tuple2<>(readIndex, new byte[(int) size]));
        }

        if (fileName.startsWith(ROCKS_CHUNK_FILE_KEY)) {
            return Node.getInstance().getChunk(fileName)
                    .map(chunkFile -> {
                        if (chunkFile.size == ChunkFile.ERROR_CHUNK.size) {
                            throw new NFSException(FsConstants.NfsErrorNo.NFS3ERR_I0, "getObjectChunkFile error.fileName:" + fileName);
                        }
                        return chunkFile;
                    })
                    .flatMapMany(chunk -> readFromList(readIndex, offset, size, bucket, chunk.getChunkList(), isPrefetch));
        } else {
            StoragePool pool = StoragePoolFactory.getStoragePool(storage, bucket);
            return read(readIndex, pool, fileName, offset, size, fileSize, isPrefetch);
        }
    }

    private static Flux<Tuple2<Integer, byte[]>> read(int readIndex, StoragePool pool, String fileName,
                                                      long offset, long size, long fileSize, boolean isPrefetch) {
        if (pool.getType() == StoragePoolType.REPLICA) {
            return new FsReplicaDecoder(readIndex, pool, fileName, offset, size, isPrefetch);
        } else {
            return new FsErasureDecoder(readIndex, pool, fileName, offset, size, fileSize, isPrefetch);
        }
    }

    public static Mono<Tuple2<Boolean, List<byte[]>>> readOnce(String ip, String lun, String fileName, long start, long end) {
        SocketReqMsg msg = new SocketReqMsg("", 0);
        msg.put("start", String.valueOf(start))
                .put("end", String.valueOf(end))
                .put("lun", lun)
                .put("fileName", fileName);

        Payload payload;
        if (CURRENT_IP.equals(ip)) {
            payload = new LocalPayload<>(GET_OBJECT, msg);
        } else {
            payload = DefaultPayload.create(Json.encode(msg), GET_OBJECT.name());
        }

        return RSocketClient.getRSocket(ip, BACK_END_PORT)
                .flatMap(r -> r.requestResponse(payload))
                .name("")
                .timeout(Duration.ofSeconds(30))
                .map(p -> {
                    PayloadMetaType type;
                    byte[][] bytesArray = null;
                    if (p instanceof LocalPayload) {
                        LocalPayload<byte[][]> localRes = (LocalPayload<byte[][]>) p;
                        type = localRes.type;
                        if (type == SUCCESS) {
                            bytesArray = localRes.data;
                        }
                    } else {
                        type = PayloadMetaType.valueOf(p.getMetadataUtf8());
                        if (type == SUCCESS) {
                            byte[] bytes = new byte[p.sliceData().readableBytes()];
                            p.sliceData().getBytes(0, bytes);
                            bytesArray = new byte[][]{bytes};
                        }
                    }

                    if (type == SUCCESS) {
                        long sum = Arrays.stream(bytesArray).mapToLong(bytes -> bytes.length).sum();
                        if (sum != end - start) {
                            log.error("read size error");
                        }
                        if (bytesArray.length == 1) {
                            return new Tuple2<>(true, Collections.singletonList(bytesArray[0]));
                        } else {
                            return new Tuple2<>(true, Arrays.asList(bytesArray));
                        }
                    } else {
                        return new Tuple2<Boolean, List<byte[]>>(false, null);
                    }
                })
                .onErrorResume(e -> {
                    if (!(e instanceof EscapeException)) {
                        if (e instanceof TimeoutException || "closed connection".equals(e.getMessage())
                                || (e.getMessage() != null && e.getMessage().startsWith("No keep-alive acks for"))) {
                            log.error("requestResponse {} {} error {}", ip, msg, e.getMessage());
                        } else {
                            log.error("requestResponse {} {}  error", ip, msg, e);
                        }
                    }

                    return Mono.just(new Tuple2<>(false, null));
                });
    }
}
