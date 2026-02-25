package com.macrosan.filesystem.cifs.api.smb1;

import com.macrosan.filesystem.FsConstants.NTStatus;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.cache.WriteCache;
import com.macrosan.filesystem.cifs.SMB1;
import com.macrosan.filesystem.cifs.SMB1.SMB1Reply;
import com.macrosan.filesystem.cifs.SMB1Header;
import com.macrosan.filesystem.cifs.call.smb1.NTCreateXCall;
import com.macrosan.filesystem.cifs.call.smb1.ReadXCall;
import com.macrosan.filesystem.cifs.call.smb1.WriteXCall;
import com.macrosan.filesystem.cifs.reply.smb1.EmptyReply;
import com.macrosan.filesystem.cifs.reply.smb1.NTCreateXReply;
import com.macrosan.filesystem.cifs.reply.smb1.ReadXReply;
import com.macrosan.filesystem.cifs.reply.smb1.WriteXReply;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.functional.Tuple2;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.filesystem.FsConstants.S_IFREG;
import static com.macrosan.filesystem.cifs.SMB1.SMB1_OPCODE.*;
import static com.macrosan.filesystem.cifs.call.smb1.WriteXCall.HIGH_BLOCK_SIZE;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.message.jsonmsg.Inode.NOT_FOUND_INODE;

@Slf4j
public class NT {
    private static final NT instance = new NT();

    private NT() {
    }

    public static Map<Short, Inode> openedFiles = new HashMap<Short, Inode>();
    public static AtomicInteger GLOBAL_FSID = new AtomicInteger();

    public static NT getInstance() {
        return instance;
    }

    @SMB1.Smb1Opt(SMB_NTCREATEX)
    public Mono<SMB1Reply> createX(SMB1Header header, Session session0, NTCreateXCall call) {
        SMB1Reply reply = new SMB1Reply(header);
        if (header.tid == -1) {
            reply.setBody(EmptyReply.DEFAULT);
            reply.getHeader().setStatus(NTStatus.STATUS_OBJECT_NAME_NOT_FOUND);
            return Mono.just(reply);
        } else {
            String bucket = NFSBucketInfo.getBucketName(header.tid);
            String objName = new String(call.fileName).substring(1);
            return FsUtils.lookup(bucket, objName, null, false, -1, null)
                    .flatMap(inode -> {
                        ReqInfo reqInfo = new ReqInfo();
                        reqInfo.bucket = bucket;
                        reqInfo.bucketInfo = NFSBucketInfo.getBucketInfo(bucket);
                        if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                            return CheckUtils.cifsWritePermissionCheck(inode.getBucket())
                                    .flatMap(pass -> {
                                        if (!pass) {
                                            reply.getHeader().status = NTStatus.STATUS_ACCESS_DENIED;
                                            return Mono.just(reply);
                                        }
                                        return InodeUtils.create(reqInfo, 0744 | S_IFREG, call.access, objName, objName, -1, "", null, null)
                                                .flatMap(createInode -> {
                                                    if (InodeUtils.isError(createInode)) {
                                                        header.status = NTStatus.STATUS_DATA_ERROR;
                                                        return Mono.just(reply);
                                                    }
                                                    return getCreateXReply(createInode, call.xOpcode, reply);
                                                });
                                    });

                        } else if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                            header.status = NTStatus.STATUS_DATA_ERROR;
                            return Mono.just(reply);
                        } else {
                            return getCreateXReply(inode, call.xOpcode, reply);
                        }
                    });
        }
    }

    public Mono<SMB1Reply> getCreateXReply(Inode inode, byte xOpcode, SMB1Reply reply) {
        NTCreateXReply body = NTCreateXReply.mapToNTCreateXReply(inode);
        body.setDefaultValue(xOpcode);
        reply.setBody(body);
        if ((body.fid = (short) (GLOBAL_FSID.incrementAndGet() & 0xffff)) == Short.MAX_VALUE) {
            GLOBAL_FSID.set(0);
        }
        openedFiles.compute(body.fid, (k, v) -> {
            if (v == null) {
                return inode;
            }
            return v;
        });
        return Mono.just(reply);
    }

    @SMB1.Smb1Opt(value = SMB_WRITEX)
    public Mono<SMB1Reply> writeX(SMB1Header header, Session session0, WriteXCall call) {
        SMB1Reply reply = new SMB1Reply(header);
        SMB1Header replyHeader = reply.getHeader();
        WriteXReply body = new WriteXReply();
        return Mono.just(openedFiles.get(call.fsid))
                .flatMap(inode -> {
                    if (inode == null || InodeUtils.isError(inode)) {
                        header.status = NTStatus.STATUS_DATA_ERROR;
                        return Mono.just(reply);
                    }

                    return CheckUtils.cifsWritePermissionCheck(inode.getBucket())
                            .flatMap(pass -> {
                                if (!pass) {
                                    replyHeader.status = NTStatus.STATUS_ACCESS_DENIED;
                                    return Mono.just(reply);
                                }
                                return WriteCache.getCache(inode.getBucket(), inode.getNodeId(), call.writeMode, inode.getStorage(), true)
                                        .flatMap(fileCache -> fileCache.nfsWrite(call.offset, call.fileData, inode, call.writeMode))
                                        .flatMap(b -> {
                                            body.dataLenLow = call.dateLenLow;
                                            body.dataLenHigh = call.dateLenHigh;
                                            body.xOpcode = call.xOpcode;
                                            if (!b) {
                                                replyHeader.status = NTStatus.STATUS_DATA_ERROR;
                                            }
                                            reply.setBody(body);
                                            return Mono.just(reply);
                                        });
                            });
                });
    }

    @SMB1.Smb1Opt(value = SMB_READX, buf = 1 << 21)
    public Mono<SMB1Reply> readX(SMB1Header header, Session session0, ReadXCall call) {
        SMB1Reply reply = new SMB1Reply(header);
        SMB1Header replyHeader = reply.getHeader();
        ReadXReply body = new ReadXReply();
        body.xOpcode = call.xOpcode;
        String bucket = NFSBucketInfo.getBucketName(header.tid);

        return Mono.just(openedFiles.get(call.fsid))
                .flatMap(inode -> Node.getInstance().getInode(bucket, inode.getNodeId()))
                .flatMap(inode -> {
                    if (inode == null || InodeUtils.isError(inode)) {
                        header.status = NTStatus.STATUS_DATA_ERROR;
                        return Mono.just(reply);
                    }
                    MonoProcessor<SMB1Reply> res = MonoProcessor.create();
                    List<Inode.InodeData> inodeData = inode.getInodeData();
                    if (inodeData.isEmpty()) {
                        if (inode.getSize() <= 0) {//处理0kb大小的文件
                            replyHeader.status = 0;
                        } else {
                            replyHeader.status = NTStatus.STATUS_DATA_ERROR;
                        }

                        body.dataHighLen = 0;
                        body.dataLowLen = 0;
                        body.data = new byte[0];
                        reply.setBody(body);
                        return Mono.just(reply);
                    }

                    long cur = 0;
                    long readOffset = call.readOffset;
                    long readEnd = call.readOffset + call.maxCountLow + (long) call.maxCountHigh * HIGH_BLOCK_SIZE;
                    long inodeEnd = inode.getSize();
                    long dataSize = call.maxCountLow + (long) call.maxCountHigh * HIGH_BLOCK_SIZE;
                    body.available = inodeEnd - readEnd;
                    if (readEnd >= inodeEnd) {
                        readEnd = inodeEnd;
                        body.available = 0L;
                        dataSize = (readEnd - call.readOffset);
                    }

                    if (dataSize <= 0) {
                        body.dataHighLen = 0;
                        body.dataLowLen = 0;
                        body.data = new byte[0];
                        reply.setBody(body);
                        return Mono.just(reply);
                    }

                    int readN = 0;
                    byte[] bytes = new byte[(int) dataSize];

                    Flux<Tuple2<Integer, byte[]>> flux = Flux.empty();

                    for (Inode.InodeData data : inodeData) {
                        long curEnd = cur + data.size;
                        if (readEnd < cur) {
                            break;
                        } else if (readOffset > curEnd) {
                            cur = curEnd;
                        } else {
                            long readFileOffset = data.offset + (readOffset - cur);
                            int readFileSize = (int) (Math.min(readEnd, curEnd) - readOffset);

                            flux = flux.mergeWith(FsUtils.readObj(readN, data.storage, bucket,
                                    data.fileName, readFileOffset, readFileSize, data.size));

                            readOffset += readFileSize;
                            readN += readFileSize;

                            if (readOffset >= readEnd) {
                                break;
                            }

                            cur = curEnd;
                        }
                    }
                    flux.doOnNext(t -> {
                                System.arraycopy(t.var2, 0, bytes, t.var1, t.var2.length);
                            })
                            .doOnError(e -> {
                                log.error("", e);
                                replyHeader.status = NTStatus.STATUS_DATA_ERROR;
                                body.data = new byte[0];
                                reply.setBody(body);
                                res.onNext(reply);
                            })
                            .doOnComplete(() -> {
                                body.data = bytes;
                                body.dataHighLen = call.maxCountHigh;
                                if (bytes.length < call.maxCountLow) {
                                    body.dataLowLen = (short) bytes.length;
                                } else if (call.maxCountLow > Short.MAX_VALUE) {
                                    body.dataLowLen = Short.MIN_VALUE;
                                } else {
                                    body.dataLowLen = (short) call.maxCountLow;
                                }
                                reply.setBody(body);

                                res.onNext(reply);
                            }).subscribe();

                    return res;
                });
    }
}
