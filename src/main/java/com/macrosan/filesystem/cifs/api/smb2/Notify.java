package com.macrosan.filesystem.cifs.api.smb2;

import com.macrosan.ec.VersionUtil;
import com.macrosan.filesystem.cifs.SMB2;
import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.call.smb2.CancelCall;
import com.macrosan.filesystem.cifs.call.smb2.NotifyCall;
import com.macrosan.filesystem.cifs.notify.NotifyCache;
import com.macrosan.filesystem.cifs.notify.NotifyLock;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.lock.LockClient;
import com.macrosan.httpserver.ServerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import static com.macrosan.filesystem.FsConstants.NTStatus.*;
import static com.macrosan.filesystem.cifs.SMB2.SMB2_OPCODE.SMB2_CANCEL;
import static com.macrosan.filesystem.cifs.SMB2.SMB2_OPCODE.SMB2_NOTIFY;
import static com.macrosan.filesystem.cifs.SMB2Header.*;

@Slf4j
public class Notify {
    private static final Notify instance = new Notify();

    private Notify() {
    }

    public static Notify getInstance() {
        return instance;
    }

    private static final String localNode = ServerConfig.getInstance().getHostUuid();

    @SMB2.Smb2Opt(value = SMB2_NOTIFY)
    public Mono<SMB2.SMB2Reply> notify(SMB2Header header, Session session, NotifyCall call) {
        SMB2.SMB2Reply reply = new SMB2.SMB2Reply(header);

        SMB2FileId.FileInfo fileInfo = call.getFileId().getFileInfo(header.getCompoundRequest());
        if (fileInfo == null) {
            reply.getHeader().setStatus(STATUS_FILE_CLOSED);
            return Mono.just(reply);
        }

        if (!StringUtils.isBlank(fileInfo.obj) && !fileInfo.obj.endsWith("/")) {
            reply.getHeader().setStatus(STATUS_INVALID_PARAMETER);
            return Mono.just(reply);
        }

        long asyncID = NotifyCache.acquireAsyncID();
        boolean tree = call.flags != 0;
        SMB2Header replyHeader = newReplyHeader(header);
        replyHeader.setFlags(reply.getHeader().getFlags() | SMB2_HDR_FLAG_ASYNC);
        replyHeader.setAsyncId(asyncID);

        NotifyLock notify = new NotifyLock(tree, session.sessionId, asyncID, call.filter, localNode,
                VersionUtil.getVersionNum());
        NotifyCache.addCache(asyncID, header.getMessageId(), fileInfo.bucket, fileInfo.obj,
                session.handler, notify, replyHeader);
        return LockClient.lock(fileInfo.bucket, fileInfo.obj, notify)
                .flatMap(b -> {
                    if (b) {
                        int flags = reply.getHeader().getFlags();
                        reply.getHeader().setFlags(flags | SMB2_HDR_FLAG_ASYNC);
                        reply.getHeader().setStatus(STATUS_PENDING);


                        reply.getHeader().setAsyncId(asyncID);

                        if ((header.getFlags() & SMB2_HDR_FLAG_ASYNC) == 0 && NotifyCache.isCancel(header.getMessageId())) {
                            return LockClient.unlock(fileInfo.bucket, fileInfo.obj, notify).flatMap(bb -> {
                                return Mono.just(createCancelledReply(header, asyncID));
                            });
                        }

                        return Mono.just(reply);
                    } else {
                        NotifyCache.clearByAsync(asyncID);
                        reply.getHeader().setStatus(STATUS_INSUFFICIENT_RESOURCES);
                        return Mono.just(reply);
                    }
                });
    }

    @SMB2.Smb2Opt(value = SMB2_CANCEL)
    //取消async请求
    public Mono<SMB2.SMB2Reply> cancel(SMB2Header header, Session session, CancelCall call) {
        SMB2.SMB2Reply reply = new SMB2.SMB2Reply(header);

        //asyncId默认和header中相同.如果不是async请求，根据msgId查询对应的asyncId
        NotifyCache cache;
        if ((header.getFlags() & SMB2_HDR_FLAG_ASYNC) == 0) {
            cache = NotifyCache.clearByMsg(header.getMessageId());
            if (cache == null) {
                NotifyCache.addCancel(header.getMessageId());
            }
        } else {
            cache = NotifyCache.clearByAsync(header.getAsyncId());
        }

        if (cache != null) {
            return LockClient.unlock(cache.bucket, cache.key, cache.notify)
                    .flatMap(b -> {
                        if (b) {
                            reply.getHeader().setAsyncId(cache.notify.getAsyncID());
                            reply.getHeader().setOpcode(SMB2_NOTIFY.opcode);
                            int flags = reply.getHeader().getFlags();
                            reply.getHeader().setFlags(flags | SMB2_HDR_FLAG_ASYNC);
                            reply.getHeader().setStatus(STATUS_CANCELLED);
                            return Mono.just(reply);
                        } else {
                            //不返回 reply
                            return Mono.empty();
                        }
                    });
        } else {
            //不返回 reply
            return Mono.empty();
        }
    }

    public static SMB2.SMB2Reply createCancelledReply(SMB2Header header, long asyncID) {
        SMB2.SMB2Reply reply = new SMB2.SMB2Reply(header);
        reply.getHeader().setAsyncId(asyncID);
        reply.getHeader().setStatus(STATUS_CANCELLED);
        reply.getHeader().setCreditCharge((short) 1);
        return reply;
    }
}
