package com.macrosan.filesystem.cifs.rpc.handler;

import com.macrosan.filesystem.cifs.rpc.Session;
import com.macrosan.filesystem.cifs.rpc.api.RPCProc;
import com.macrosan.filesystem.cifs.rpc.pdu.ack.Ack;
import com.macrosan.filesystem.cifs.rpc.pdu.call.BindCall;
import com.macrosan.filesystem.cifs.rpc.pdu.call.EpmMapRequestCall;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.net.NetSocket;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import static com.macrosan.filesystem.cifs.rpc.DCERPC.rpcDebug;
import static com.macrosan.filesystem.cifs.rpc.RPCConstants.*;

@Log4j2
public class RPCHandler {

    NetSocket socket;
    Session session;

    public RPCHandler(NetSocket socket) {
        this.socket = socket;
        session = new Session();
        session.clientIP = socket.remoteAddress().host();
        session.curServerIP = socket.localAddress().host();
        socket.exceptionHandler(e -> {
            log.info(e);
        });
    }

    public void handler(Buffer b) {
        // TODO loop处理
        ByteBuf in = b.getByteBuf();
        byte type = in.getByte(2);
        if (rpcDebug) {
            log.info(PACKET_TYPE_NAMES[type] + " call");
        }
        int bufSize = 4096;
        ByteBuf out = UnpooledByteBufAllocator.DEFAULT.heapBuffer(bufSize, bufSize);

        RPCProc rpcProc = new RPCProc();
        Mono<Ack> res = Mono.empty();
        switch (type) {
            case BIND:
                BindCall bind = new BindCall();
                bind.readStruct(in, 0);
                res = rpcProc.bind(bind,  session);
                break;
            case REQUEST:
                EpmMapRequestCall request = new EpmMapRequestCall();
                request.readStruct(in, 0);
                res = rpcProc.request(request,  session);
                break;
            default:
                break;
        }

        res.subscribe(ack -> {
            int size = ack.writeStruct(out, 0);
            out.writerIndex(size);
            socket.write(Buffer.buffer(out), v -> {
                if (rpcDebug) {
                    log.info(PACKET_TYPE_NAMES[type] + " reply success");
                }
            });
        });
    }

}
