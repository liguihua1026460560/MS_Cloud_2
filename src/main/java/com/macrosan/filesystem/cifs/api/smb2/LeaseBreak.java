package com.macrosan.filesystem.cifs.api.smb2;

import com.macrosan.filesystem.cifs.SMB2;
import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.call.smb2.LeaseBreakAcknowledgmentCall;
import com.macrosan.filesystem.cifs.lease.LeaseCache;
import com.macrosan.filesystem.cifs.lease.LeaseClient;
import com.macrosan.filesystem.cifs.lease.LeaseLock;
import com.macrosan.filesystem.cifs.reply.smb2.LeaseBreakReply;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import static com.macrosan.filesystem.cifs.SMB2.SMB2_OPCODE.SMB2_BREAK;

@Slf4j
public class LeaseBreak {

    private static final LeaseBreak instance = new LeaseBreak();

    private LeaseBreak() {
    }

    public static LeaseBreak getInstance() {
        return instance;
    }

    @SMB2.Smb2Opt(value = SMB2_BREAK)
    public Mono<SMB2.SMB2Reply> leaseBreakAcknowledgment(SMB2Header header, Session session, LeaseBreakAcknowledgmentCall call) {
        SMB2.SMB2Reply reply = new SMB2.SMB2Reply(header);
        LeaseBreakReply body = new LeaseBreakReply();

        LeaseLock leaseLock = LeaseCache.sendBreakMap.remove(call.getLeaseKeyStr());
        if (leaseLock != null) {
            if (leaseLock.leaseState != call.getLeaseState()) {
                leaseLock.leaseState = call.getLeaseState();
                return LeaseClient.lease(leaseLock.bucket, String.valueOf(leaseLock.ino), leaseLock)
                        .flatMap(state -> {
                            if (state < 0) {
                                state = 0;
                            }
                            leaseLock.leaseState = state;
                            LeaseCache.changeCache(leaseLock);

                            body.setLeaseKey(call.getLeaseKey());
                            body.setLeaseFlags(0);
                            body.setLeaseState(state);
                            reply.setBody(body);

                            // 通知打破成功
                            Tuple2<SMB2FileId, String> tuple2 = LeaseCache.sendBlockMap.remove(call.getLeaseKeyStr());
                            if (tuple2 != null) {
                                LeaseClient.breakLeaseEnd(leaseLock.bucket, String.valueOf(leaseLock.ino), leaseLock.leaseKey,  tuple2.getT1(), tuple2.getT2()).subscribe();
                            }

                            return Mono.just(reply);
                        });
            } else {
                body.setLeaseKey(call.getLeaseKey());
                body.setLeaseFlags(0);
                body.setLeaseState(leaseLock.leaseState);
                reply.setBody(body);

                // 通知打破成功
                Tuple2<SMB2FileId, String> tuple2 = LeaseCache.sendBlockMap.remove(call.getLeaseKeyStr());
                if (tuple2 != null) {
                    LeaseClient.breakLeaseEnd(leaseLock.bucket, String.valueOf(leaseLock.ino), leaseLock.leaseKey, tuple2.getT1(), tuple2.getT2()).subscribe();
                }

                return Mono.just(reply);
            }
        }
        body.setLeaseKey(call.getLeaseKey());
        body.setLeaseFlags(0);
        body.setLeaseState(call.getLeaseState());
        reply.setBody(body);
        return Mono.just(reply);
    }
}
