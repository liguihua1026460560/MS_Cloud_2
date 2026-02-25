package com.macrosan.filesystem.cifs.rpc.api;

import com.macrosan.filesystem.cifs.rpc.Session;
import com.macrosan.filesystem.cifs.rpc.pdu.ack.Ack;
import com.macrosan.filesystem.cifs.rpc.pdu.ack.BindAck;
import com.macrosan.filesystem.cifs.rpc.pdu.ack.EpmMapRequestAck;
import com.macrosan.filesystem.cifs.rpc.pdu.call.BindCall;
import com.macrosan.filesystem.cifs.rpc.pdu.call.EpmMapRequestCall;
import com.macrosan.filesystem.cifs.rpc.pdu.call.RequestCall;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.ack.OrphanedAck;
import com.macrosan.filesystem.cifs.rpc.witness.pdu.call.OrphanedCall;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.macrosan.filesystem.cifs.rpc.Session.witnessPort;
import static com.macrosan.filesystem.cifs.rpc.witness.WITNESS.WITNESS_MAX_PORT;
import static com.macrosan.filesystem.cifs.rpc.witness.WITNESS.WITNESS_MIN_PORT;


public class RPCProc {
    public Mono<Ack> bind(BindCall call, Session session) {
        BindAck bindAck = new BindAck(call, session);
        return Mono.just(bindAck);
    }

    public Mono<Ack> request(RequestCall call, Session session) {
        int len = WITNESS_MAX_PORT - WITNESS_MIN_PORT + 1;
        int port = witnessPort.compute(session.clientIP, (k, v) -> {
            if (v == null) {
                Map<String, Integer> map = new HashMap<>();
                map.put(session.curServerIP, 0);
                return map;
            } else {
                v.compute(session.curServerIP, (ip, i) -> i == null ? 0 : (i + 1) % len);
                return v;
            }
        }).get(session.curServerIP);
        EpmMapRequestAck epmMapRequestAck = new EpmMapRequestAck((EpmMapRequestCall) call, WITNESS_MIN_PORT + port);

        return Mono.just(epmMapRequestAck);
    }

    public Mono<Ack> orphaned(OrphanedCall orphaned, Session session) {
        OrphanedAck orphanedAck = new OrphanedAck(orphaned, session);

        return Mono.just(orphanedAck);
    }
}
