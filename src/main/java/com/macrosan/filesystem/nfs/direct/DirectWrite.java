package com.macrosan.filesystem.nfs.direct;

import com.macrosan.ec.server.ErasureServer.PayloadMetaType;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cache.WriteCache;
import com.macrosan.filesystem.nfs.call.DirectWriteCall;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.data.DataClient;
import com.macrosan.storage.StorageOperate;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.coder.direct.DirectEncoder;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.checksum.ChecksumProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.List;

import static com.macrosan.filesystem.utils.InodeUtils.isError;
import static com.macrosan.storage.StorageOperate.PoolType.DATA;

@Log4j2
public class DirectWrite {
    public static Mono<Boolean> directWrite(DirectWriteCall call, Inode inode) {
        int flag = call.sync;
        long offset = call.writeOffset;
        ByteBuf buf = call.bytes;

        if (flag != 0) {
            return directWrite0(offset, buf, inode, call);
        } else {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            call.close();
            return WriteCache.getCache(inode.getBucket(), inode.getNodeId(), flag, inode.getStorage())
                    .flatMap(w -> w.nfsWrite(offset, bytes, inode, 1));
        }
    }

    public static Mono<Boolean> directWrite0(long offset, ByteBuf buf, Inode inode, DirectWriteCall call) {
        int size = buf.readableBytes();
        StorageOperate dataOperate = new StorageOperate(DATA, inode.getObjName(), size);
        StoragePool dataPool = StoragePoolFactory.getStoragePool(dataOperate, inode.getBucket());
        DirectEncoder encoder = dataPool.getDirectEncoder(size);
        encoder.putAndComplete(buf);

        ChecksumProvider digest = ChecksumProvider.create();
        digest.update(buf.slice());
        String md5 = digest.getChecksum();
        digest.release();

        call.close();
        return putObj(encoder, inode, offset, size, md5, dataPool);
    }

    private static Mono<Boolean> putObj(DirectEncoder encoder, Inode inode, long curOffset, int curSize, String md5, StoragePool dataPool) {
        FsUtils.PutObjectFunction function = (p, list, msgs) -> sendDirectEncoder(p, encoder, list, msgs);
        return FsUtils.putObj(inode, function, curOffset, curSize, md5, 1, dataPool)
                .map(inode1 -> !isError(inode1));
    }

    public static ClientTemplate.ResponseInfo<String> sendDirectEncoder(StoragePool pool, DirectEncoder encoder,
                                                                        List<Tuple3<String, String, String>> nodeList, List<SocketReqMsg> msgs) {

        ByteBuf[] bufs = new ByteBuf[nodeList.size()];
        for (int i = 0; i < nodeList.size(); i++) {
            ByteBuf buf = msgs.get(i).toDirectBytes();
            bufs[i] = buf;
        }

        MonoProcessor<ByteBuf[]>[] dataFlux = encoder.data();

        Mono<Tuple3<Integer, PayloadMetaType, String>>[] responses = new Mono[nodeList.size()];

        for (int i = 0; i < bufs.length; i++) {
            ByteBuf[] data = dataFlux[i].peek();
            String ip = nodeList.get(i).var1;
            ByteBuf msg = bufs[i];
            ByteBuf[] msgAndData = new ByteBuf[1 + data.length];
            msgAndData[0] = msg;
            System.arraycopy(data, 0, msgAndData, 1, data.length);
            ByteBuf buf = Unpooled.wrappedBuffer(msgAndData);

            MonoProcessor<Tuple3<Integer, PayloadMetaType, String>> response = MonoProcessor.create();

            int finalI = i;
            DataClient.getRSocket(ip).flatMap(s -> s.handle(buf))
                    .timeout(Duration.ofSeconds(30))
                    .subscribe(p -> {
                        byte status = p.readByte();
                        if (status == 0) {
                            response.onNext(new Tuple3<>(finalI, PayloadMetaType.SUCCESS, ""));
                        } else {
                            response.onNext(new Tuple3<>(finalI, PayloadMetaType.ERROR, ""));
                        }
                        p.release();
                    }, e -> {
                        log.error("", e);
                        response.onNext(new Tuple3<>(finalI, PayloadMetaType.ERROR, ""));
                    });

            responses[i] = response;
        }

        return new ClientTemplate.ResponseInfo<>(Flux.merge(responses), nodeList.size());
    }
}
