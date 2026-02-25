package com.macrosan.filesystem.cifs.api.smb2;

import com.macrosan.filesystem.cifs.SMB2;
import com.macrosan.filesystem.cifs.SMB2.SMB2Reply;
import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.call.smb2.KeepAliveCall;
import com.macrosan.filesystem.cifs.call.smb2.NegprotCall;
import com.macrosan.filesystem.cifs.reply.smb2.KeepAliveReply;
import com.macrosan.filesystem.cifs.reply.smb2.NegprotReply;
import com.macrosan.filesystem.cifs.types.NegprotInfo;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.utils.CheckUtils;
import reactor.core.publisher.Mono;

import java.util.Arrays;

import static com.macrosan.filesystem.FsConstants.NTStatus.STATUS_NOT_SUPPORTED;
import static com.macrosan.filesystem.cifs.SMB2.SMB2_OPCODE.SMB2_KEEPALIVE;
import static com.macrosan.filesystem.cifs.SMB2.SMB2_OPCODE.SMB2_NEGPROT;

public class Negprot {
    private static final Negprot instance = new Negprot();

    private Negprot() {
    }

    public static Negprot getInstance() {
        return instance;
    }

    @SMB2.Smb2Opt(value = SMB2_NEGPROT, allowNoSession = true)
    public Mono<SMB2Reply> negprot(SMB2Header header, Session session, NegprotCall call) {
        SMB2Reply reply = new SMB2Reply(header);

        short[] dialects = call.getDialects();
        Arrays.sort(dialects);


        return CheckUtils.cifsLeaseOpenCheck()
                .flatMap(b -> {
                    //优先返回高版本
                    try {
                        for (int i = dialects.length - 1; i >= 0; i--) {
                            if (dialects[i] == NegprotCall.SMB_2_0_2) {
                                NegprotReply body = NegprotReply.smb202();
                                reply.setBody(body);
                                return Mono.just(reply);
                            }

                            if (dialects[i] == NegprotCall.SMB_2_1_0) {
                                NegprotReply body = NegprotReply.smb210();
                                if (b) {
                                    body.setFlags(body.getFlags() | NegprotReply.SMB2_CAP_LEASING);
                                }
                                reply.setBody(body);
                                return Mono.just(reply);
                            }

                            if (dialects[i] == NegprotCall.SMB_3_0_2) {
                                NegprotReply body = NegprotReply.smb302();
                                if (b) {
                                    body.setFlags(body.getFlags() | NegprotReply.SMB2_CAP_LEASING);
                                }
                                reply.setBody(body);
                                return Mono.just(reply);
                            }
                        }
                    } finally {
                        if (reply.getBody() instanceof NegprotReply) {
                            NegprotReply body = (NegprotReply) reply.getBody();
                            NegprotInfo negprotInfo = header.getHandler().negprotInfo;
                            negprotInfo.setFlags(body.flags);
                            negprotInfo.setDialect(body.dialect);
                        }
                    }

                    header.setStatus(STATUS_NOT_SUPPORTED);
                    return Mono.just(reply);
                });
    }

    @SMB2.Smb2Opt(value = SMB2_KEEPALIVE, allowNoSession = true, allowIPCSession = true)
    public Mono<SMB2Reply> keepalived(SMB2Header header, Session session, KeepAliveCall call) {
        SMB2Reply reply = new SMB2Reply(header);
        reply.setBody(KeepAliveReply.DEFAULT);
        return Mono.just(reply);
    }
}
