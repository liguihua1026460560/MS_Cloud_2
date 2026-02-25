package com.macrosan.filesystem.cifs.rpc.witness.handler;

import com.macrosan.filesystem.cifs.rpc.Session;
import com.macrosan.filesystem.cifs.rpc.pdu.ack.Ack;
import com.macrosan.filesystem.cifs.rpc.pdu.call.RequestCall;
import com.macrosan.filesystem.cifs.rpc.witness.api.WitnessProc;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.call.*;
import com.macrosan.utils.functional.Tuple2;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.net.NetSocket;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.filesystem.cifs.rpc.DCERPC.rpcDebug;
import static com.macrosan.filesystem.cifs.rpc.RPCConstants.*;
import static com.macrosan.filesystem.cifs.rpc.witness.WITNESS.witnessDebug;

@Log4j2
public class WitnessHandler {
    int bufSize = 4096;
    ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.heapBuffer(bufSize, bufSize);
    int fragLength = -1;
    NetSocket socket;
    Session session;
    public static AtomicInteger contextHandle = new AtomicInteger(0);
    WitnessProc witnessProc;

    public WitnessHandler(NetSocket socket) {
        this.socket = socket;
        // TODO 与RPC的session对应
        session = new Session();
        session.curServerIP = socket.localAddress().host();
        session.clientIP = socket.remoteAddress().host();
        session.socket = socket;
        socket.exceptionHandler(e -> {
            log.error(e);
            session.socketIsClose = true;
        });
        witnessProc = new WitnessProc(session);
    }

    private void loop() {
        if (buf.readableBytes() >= 10) {
            byte t = buf.getByte(2);
            fragLength = buf.getShortLE(8);

            dispatch(t);

            clear();
            loop();
        }
    }

    private void clear() {
        if (this.fragLength == -1) {
            return;
        }

        int writerIndex = buf.writerIndex();

        // 需要移动的长度 = 总长度 - 删除段末尾
        int moveLength = writerIndex - fragLength;
        if (moveLength > 0) {
            // 把 index+length 后面的数据拷贝到 index 位置上
            buf.setBytes(0, buf, fragLength, moveLength);
        }

        // 更新 writerIndex，数据长度缩短
        buf.writerIndex(writerIndex - fragLength);

        fragLength = -1;
    }

    public void handler(int port, ByteBuf b) {
        // TODO 粘包、分包处理
        buf.writeBytes(b);
        loop();
    }

    private void dispatch(byte t) {
        ByteBuf out = UnpooledByteBufAllocator.DEFAULT.heapBuffer(bufSize, bufSize);

        Mono<Ack> res = Mono.empty();
        if (rpcDebug) {
            log.info(PACKET_TYPE_NAMES[t] + " call");
        }

        int opNum = -1;

        switch (t) {
            case BIND:
                BindCall bind = new BindCall();
                bind.readStruct(buf, 0);
                res = witnessProc.bind(bind, session);
                break;
            case AUTH:
                AuthCall auth = new AuthCall();
                auth.readStruct(buf, 0);
                res = witnessProc.auth(auth, session);
                break;
            case ALTER_CONTEXT:
                AlterContextCall alterContext = new AlterContextCall();
                alterContext.readStruct(buf, 0);
                res = witnessProc.alterContext(alterContext, session);
                break;
            case REQUEST:
                RequestCall requestCall = new RequestCall();
                requestCall.header.readStruct(buf, 0);
                opNum = requestCall.header.opnum;
                // TODO 验证签名
//                checkClientSign()
                if (witnessDebug) {
                    log.info(WITNESS_OP_NAMES[requestCall.header.opnum] + " call");
                }
                switch (requestCall.header.opnum) {
                    case WITNESS_GETINTERFACELIST:
                        GetInterfaceListRequestCall getInterfaceListRequestCall = new GetInterfaceListRequestCall();
                        getInterfaceListRequestCall.readStruct(buf, 0);
                        res = witnessProc.getInterfaceList(getInterfaceListRequestCall, session);
                        break;
                    case WITNESS_REGISTEREX:
                        RegisterExRequestCall registerExRequestCall = new RegisterExRequestCall();
                        registerExRequestCall.readStruct(buf, 0);
                        res = witnessProc.registerEx(registerExRequestCall, session);
                        break;
                    case WITNESS_UNREGISTER:
                        UnRegisterRequestCall unRegisterRequestCall = new UnRegisterRequestCall();
                        unRegisterRequestCall.readStruct(buf, 0);
                        res = witnessProc.unRegister(unRegisterRequestCall, session);
                        break;
                    case WITNESS_ASYNCNOTIFY:
                        AsyncNotifyRequestCall asyncNotifyRequestCall = new AsyncNotifyRequestCall();
                        asyncNotifyRequestCall.readStruct(buf, 0);
                        res = witnessProc.asyncNotify(asyncNotifyRequestCall, session);
                        break;
                    default:
                        break;
                }
                break;
            case ORPHANED:
                OrphanedCall orphaned = new OrphanedCall();
                orphaned.readStruct(buf, 0);
                res = witnessProc.orphaned(orphaned, session);
                break;
            default:
                break;
        }

        res.zipWith(Mono.just(new Tuple2<Byte, Integer>(t, opNum)))
        .subscribe(t2 -> {
            int op = t2.getT2().var2;
            Ack ack = t2.getT1();
            int size = ack.writeStruct(out, 0);
            log.info("size: {}", size);
            out.writerIndex(size);
            socket.write(Buffer.buffer(out), v -> {
                if (rpcDebug) {
                    log.info(PACKET_TYPE_NAMES[t] + " reply");
                }
            });
            if (op == 0 || op == 2) {
                socket.close();
            }
        });
    }
}
