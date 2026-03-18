package com.macrosan.filesystem.ftp.api;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.ECUtils;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.ftp.FTPRequest;
import com.macrosan.filesystem.ftp.Session;
import com.macrosan.filesystem.lock.redlock.RedLockClient;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.httpserver.DateChecker;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.quota.StatisticsRecorder;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.net.NetSocket;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.yaml.snakeyaml.util.UriEncoder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.GET_OBJECT_META;
import static com.macrosan.filesystem.FsConstants.DEFAULT_FILE_MODE;
import static com.macrosan.filesystem.FsConstants.S_IFREG;
import static com.macrosan.filesystem.ftp.FTPReply.*;
import static com.macrosan.filesystem.quota.FSQuotaConstants.QUOTA_KEY;
import static com.macrosan.message.jsonmsg.Inode.FILES_QUOTA_EXCCED_INODE;
import static com.macrosan.message.jsonmsg.Inode.NOT_FOUND_INODE;
import static com.macrosan.message.jsonmsg.MetaData.ERROR_META;
import static com.macrosan.message.jsonmsg.MetaData.NOT_FOUND_META;
import static com.macrosan.utils.quota.StatisticsRecorder.addFileStatisticRecord;
import static com.macrosan.utils.quota.StatisticsRecorder.getRequestType;

@Log4j2
public class FTPDataTransfer {
    protected static RedisConnPool pool = RedisConnPool.getInstance();
    private static final FTPDataTransfer instance = new FTPDataTransfer();
    public static final long READ_AHEAD_SIZE = 1 << 20;

    // 100MB
    private static final int MAX_FILE_SIZE = 104857600;

    public static final int FILE_MODE = 33252;
    public static final int FILE_CIFS_MODE = 32;

    private static final Node nodeInstance = Node.getInstance();

    public static FTPDataTransfer getInstance() {
        return instance;
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.APPE)
    public Mono<String> appe(FTPRequest ftpRequest, Session session, NetSocket socket) {
        socket.pause();

        MonoProcessor<Boolean> suc = MonoProcessor.create();

        if (ftpRequest.args.isEmpty()) {
            return Mono.just(FILE_NAME_NOT_ALLOWED.reply());
        }

        Tuple2<String, String> tuple2 = session.getBucketAndObject(ftpRequest.args.get(0));
        String bucket = tuple2.var1;
        String objName = tuple2.var2;

        if (!session.writePermission) {
            return Mono.just(notReply());
        }

        if (StringUtils.isBlank(bucket) || StringUtils.isBlank(objName)) {
            return Mono.just(FILE_NAME_NOT_ALLOWED.reply());
        }

        if (objName.endsWith("/")) {
            return Mono.just(NOT_A_PLAIN_FILE.reply());
        }

        ReqInfo reqHeader = new ReqInfo();


        return NFSBucketInfo.getFTPBucketInfoReactive(bucket)
                .flatMap(bktInfo -> {
                    reqHeader.bucketInfo = bktInfo;
                    reqHeader.bucket = bucket;
                    return FsUtils.lookup(bucket, objName, reqHeader, false, -1, null);
                })
                .flatMap(inode -> {
                    if (inode.equals(NOT_FOUND_INODE)) {
                        // 文件不存在
                        return createObj(ftpRequest, session, socket, 0);
                    }
                    // 文件存在，直接追加
                    return FSQuotaUtils.existQuotaInfo(inode.getBucket(), inode.getObjName(), System.currentTimeMillis(), inode.getUid(), inode.getGid())
                            .flatMap(t2 -> ftpWriteQuotaCheck(session, socket, suc, inode, inode.getSize(), t2));
                });
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.RETR)
    public Mono<String> retr(FTPRequest ftpRequest, Session session, NetSocket socket) {
        // 记录操作开始时间
        final AtomicLong startTime = new AtomicLong(DateChecker.getCurrentTime());
        final AtomicLong readBytes = new AtomicLong(0L);

        // 获取rest命令设置的文件偏移量
        long offset = session.getRestValue() == -1 ? 0 : session.getRestValue();
        session.setRestValue(-1);

        Tuple2<String, String> tuple2 = session.getBucketAndObject(ftpRequest.args.get(0));
        String bucket = tuple2.var1;
        String objName = tuple2.var2;

        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
        String bucketVnode = pool.getBucketVnodeId(bucket);

        // 生成metakey
        String metaKey = Utils.getVersionMetaDataKey(bucketVnode, bucket, objName, "null");

        // 获取MetaData
        return pool.mapToNodeInfo(bucketVnode).flatMap(nodeList -> ECUtils.getRocksKey(pool, metaKey, GET_OBJECT_META, NOT_FOUND_META, ERROR_META, nodeList, null, "", "")).flatMap(tuple3 -> {
            MetaData metaData = tuple3.var2;
            if (metaData.equals(ERROR_META)) {
                return Mono.just(ERROR_ON_INPUT_FILE.reply());
            }

            if (metaData.equals(NOT_FOUND_META) || metaData.deleteMark || metaData.deleteMarker) {
                return Mono.just(NO_SUCH_FILE_OR_DIRECTORY.reply());
            }

            // 写入状态
            MonoProcessor<Boolean> suc = MonoProcessor.create();

            StoragePool dataPool = StoragePoolFactory.getStoragePool(metaData);

            // 流控制
            UnicastProcessor<Long> streamController = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());

            // 数据写入socket
            dataPool.mapToNodeInfo(dataPool.getObjectVnodeId(metaData))
                    .flatMapMany(nodeList1 -> ErasureClient.getObject(dataPool, metaData, offset, metaData.endIndex, nodeList1, streamController, null, null))
                    .doOnNext(bytes -> readBytes.addAndGet(bytes.length))
                    .subscribe(new DownLoadSubscriber(socket, session.getContext(), streamController, suc));

            return NFSBucketInfo.getFTPBucketInfoReactive(bucket)
                    .flatMap(bktInfo -> {
                        return suc.doOnNext(success -> {
                                    if (bktInfo != null) {
                                        String accountId = bktInfo.get("user_id");
                                        if (!StringUtils.isEmpty(bucket) && !StringUtils.isEmpty(accountId)) {
                                            long executionTime = DateChecker.getCurrentTime() - startTime.get();
                                            StatisticsRecorder.RequestType requestType = getRequestType(HttpMethod.GET);
                                            addFileStatisticRecord(accountId, bucket, HttpMethod.GET, -1, requestType, success, executionTime, readBytes.get());
                                        }
                                    }
                                })
                                .map(b -> {
                                    if (b) {
                                        return TRANSFER_COMPLETE.reply();
                                    } else {
                                        return ERROR_ON_INPUT_FILE.reply();
                                    }
                                });
                    });

        });
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.STOR)
    public Mono<String> stor(FTPRequest ftpRequest, Session session, NetSocket socket) {
        socket.pause();
        // 获取rest命令设置的文件偏移量
        long offset = session.getRestValue() == -1 ? 0 : session.getRestValue();
        session.setRestValue(-1);

        MonoProcessor<Boolean> suc = MonoProcessor.create();

        if (!session.writePermission) {
            return Mono.just(notReply());
        }

        if (session.cachedInode == null || InodeUtils.isError(session.cachedInode)) {
            return Mono.just(ERROR_ON_OUTPUT_FILE.reply());
        }
        if (session.cachedInode != null && session.cachedInode.getLinkN() == FILES_QUOTA_EXCCED_INODE.getLinkN()) {
            return Mono.just(EXCEEDED_QUOTA.reply());
        }

        Inode inode = session.cachedInode;
        return ftpWriteQuotaCheck(session, socket, suc, inode, offset, null);
    }

    public static Mono<String> ftpWriteQuotaCheck(Session session, NetSocket socket, MonoProcessor<Boolean> suc, Inode inode, long fileOffset, Tuple2<Boolean, List<String>> tuple2) {
        return Mono.defer(() -> {
            if (tuple2 == null) {
                return FSQuotaUtils.existQuotaInfo(inode.getBucket(), inode.getObjName(), System.currentTimeMillis(), inode.getUid(), inode.getGid());
            }
            return Mono.just(tuple2);
        }).flatMap(t2 -> {
            return FSQuotaUtils.fsCheckS3Quota(inode.getBucket(), true)
                    .flatMap(res -> {
                        if (t2.var1) {
                            inode.getXAttrMap().put(QUOTA_KEY, Json.encode(t2.var2));
                            return FSQuotaUtils.fsCanWrite(inode.getBucket(), t2).flatMap(b -> {
                                if (!b) {
                                    log.info("exceeded quota.bucket:{},obj:{},nodeId:{}", inode.getBucket(), inode.getObjName(), inode.getNodeId());
                                    session.releaseLock();
                                    return Mono.just(EXCEEDED_QUOTA.reply());
                                }
                                return uploadFile(session, socket, suc, inode, inode.getSize());
                            });
                        }
                        return uploadFile(session, socket, suc, inode, fileOffset);
                    });
        });
    }

    private static Mono<String> uploadFile(Session session, NetSocket socket, MonoProcessor<Boolean> suc, Inode inode, long fileOffset) {
        final AtomicLong startTime = new AtomicLong(DateChecker.getCurrentTime());
        final AtomicLong writeBytes = new AtomicLong(0L);

        AtomicLong size = new AtomicLong(0L);
        AtomicLong offset = new AtomicLong(fileOffset);
        LinkedList<PutObjectTask> list = new LinkedList<>();

        socket.handler(b -> {
            if (size.get() >= MAX_FILE_SIZE) {
                long offset0 = offset.addAndGet(size.get());
                PutObjectTask lastTask = list.getLast();
                lastTask.res.subscribe(partSuccess -> {
                    if (!partSuccess) {
                        suc.onNext(false);
                    }
                });
                lastTask.complete();

                list.add(new PutObjectTask(session, socket, inode, offset0, true));
                size.set(0L);
            }

            PutObjectTask task = list.getLast();
            task.handle(b);
            size.addAndGet(b.length());
            writeBytes.addAndGet(b.length());
        });

        socket.endHandler(v -> {
            PutObjectTask task = list.getLast();
            task.res.subscribe(suc::onNext);
            task.complete();
        });

        list.add(new PutObjectTask(session, socket, inode, offset.get(), false));
        list.getLast().request();

        return suc.doOnNext(success -> {
                    if (session.reqHeader != null && session.reqHeader.bucketInfo != null) {
                        String accountId = session.reqHeader.bucketInfo.get("user_id");
                        String bucket = session.reqHeader.bucket;
                        if (!StringUtils.isEmpty(bucket) && !StringUtils.isEmpty(accountId)) {
                            long executionTime = DateChecker.getCurrentTime() - startTime.get();
                            StatisticsRecorder.RequestType requestType = getRequestType(HttpMethod.PUT);
                            addFileStatisticRecord(accountId, bucket, HttpMethod.PUT, -1, requestType, success, executionTime, writeBytes.get());
                        }
                    }
                })
                .flatMap(b -> {
                    session.releaseLock();
                    if (b) {
                        return NFSBucketInfo.getFTPBucketInfoReactive(inode.getBucket())
                                .map(bktInfo -> ES_ON.equals(bktInfo.get(ES_SWITCH)))
                                .flatMap(f -> {
                                    if (f) {
                                        return nodeInstance.getInode(inode.getBucket(), inode.getNodeId()).flatMap(i -> {
                                            if (!InodeUtils.isError(i)) {
                                                return EsMetaTask.putEsMeta(i);
                                            }
                                            return Mono.just(f);
                                        }).map(res -> true);
                                    }
                                    return Mono.just(true);
                                }).map(c -> TRANSFER_COMPLETE.reply());
                    } else {
                        return Mono.just(ERROR_ON_OUTPUT_FILE.reply());
                    }
                });
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.STOU)
    public Mono<String> stou(FTPRequest ftpRequest, Session session, NetSocket socket) {
        socket.pause();
        if (!session.writePermission) {
            return Mono.just(notReply());
        }
        return createObj(ftpRequest, session, socket, 0);
    }

    private static Mono<String> createObj(FTPRequest ftpRequest, Session session, NetSocket socket, long offset) {
        MonoProcessor<Boolean> suc = MonoProcessor.create();

        if (ftpRequest.args.isEmpty()) {
            return Mono.just(FILE_NAME_NOT_ALLOWED.reply());
        }

        Tuple2<String, String> tuple2 = session.getBucketAndObject(ftpRequest.args.get(0));
        String bucket = tuple2.var1;
        String objName = tuple2.var2;

        if (StringUtils.isBlank(bucket) || StringUtils.isBlank(objName)) {
            return Mono.just(FILE_NAME_NOT_ALLOWED.reply());
        }

        return InodeUtils.findDirInode(objName, bucket).flatMap(dirInode -> {
            if (InodeUtils.isError(dirInode)) {
                return Mono.just(ERROR_ON_OUTPUT_FILE.reply());
            }

            return NFSBucketInfo.getFTPBucketInfoReactive(bucket)
                    .flatMap(bktInfo -> {
                        ReqInfo reqHeader = new ReqInfo();
                        reqHeader.bucketInfo = bktInfo;
                        reqHeader.bucket = bucket;
                        return InodeUtils.ftpCreate(reqHeader, dirInode, DEFAULT_FILE_MODE | S_IFREG, FILE_CIFS_MODE, objName);
                    })
                    .flatMap(inode -> {
                        if (InodeUtils.isError(inode)) {
                            return Mono.just(ERROR_ON_OUTPUT_FILE.reply());
                        }

                        return ftpWriteQuotaCheck(session, socket, suc, inode, offset, null);
                    });
        });

    }

    public static Flux<Map<String, String>> scanAnonymousBucketInfo() {
        UnicastProcessor<Map<String, String>> res = UnicastProcessor.create();
        ScanArgs scanArgs = new ScanArgs().match("*").limit(10);
        KeyScanCursor<String> keyScanCursor = new KeyScanCursor<>();
        keyScanCursor.setCursor("0");
        UnicastProcessor<KeyScanCursor<String>> listController = UnicastProcessor.create();
        listController
                .flatMap(curKeyScanCursor -> pool.getReactive(REDIS_BUCKETINFO_INDEX).scan(curKeyScanCursor, scanArgs))
                .flatMap(curKeyScanCursor -> Flux.fromIterable(curKeyScanCursor.getKeys())
                        .filter(bucket -> Pattern.matches("^(?!-)[a-z0-9][a-z0-9-]{2,58}(?<!-)$", bucket))
                        .flatMap(bucket -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket))
                        .filter(bucketInfo -> StringUtils.isNotBlank(bucketInfo.get("ftp")) && bucketInfo.get("ftp").equals("1") && "1".equals(bucketInfo.get("ftp_anonymous")))
                        .filter(bucketInfo -> {
                            int acl = Integer.parseInt(bucketInfo.get(BUCKET_ACL));
                            return (acl & PERMISSION_SHARE_READ_WRITE_NUM) != 0 || (acl & PERMISSION_SHARE_READ_NUM) != 0;
                        })
                        .doOnNext(res::onNext)
                        .collectList()
                        .map(ignore -> curKeyScanCursor))
                .subscribe(curKeyScanCursor -> {
                    if (!curKeyScanCursor.isFinished()) {
                        listController.onNext(curKeyScanCursor);
                    } else {
                        listController.onComplete();
                        res.onComplete();
                    }
                }, e -> {
                    log.info("ftp scanAnonymousBucket error", e);
                    listController.onComplete();
                    res.onError(e);
                });
        listController.onNext(keyScanCursor);
        return res;
    }

    /**
     * rfc文档没有定义list返回的标准格式，但是一般有windows和unix两种
     * 当前实现unix
     */
    @FTPRequest.FTPOpt(FTPRequest.Command.LIST)
    public Mono<String> list(FTPRequest ftpRequest, Session session, NetSocket socket) {
        String curDirectory = session.getWorkDir();
        Flux<List<Inode>> res;
        UnicastProcessor<String> processor = UnicastProcessor.create();

        if (curDirectory.equals(File.separator)) {
            if (!"anonymous".equalsIgnoreCase(session.getUser())) {
                // list bucket
                // 从redis获取当前用户下所有桶的 key
                String userBucketSet = session.getUserId() + USER_BUCKET_SET_SUFFIX;
                res = pool.getReactive(REDIS_USERINFO_INDEX)
                        .smembers(userBucketSet)
                        .flatMap(bucket -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket))
                        .filter(bucketInfo -> StringUtils.isNotBlank(bucketInfo.get("ftp")) && bucketInfo.get("ftp").equals("1"))
                        .flatMap(map -> nodeInstance.getInode(map.get("bucket_name"), 1L))
                        .collectList()
                        .flux();
            } else {
                // 匿名用户列举所有开启匿名访问的桶
                res = scanAnonymousBucketInfo()
                        .flatMap(map -> nodeInstance.getInode(map.get("bucket_name"), 1L))
                        .collectList()
                        .flux();
            }
        } else {
            Tuple2<String, String> tuple2 = session.getBucketAndObject("");
            String bucket = tuple2.var1;
            String prefix = tuple2.var2;

            res = processor.flatMap(marker -> listObject(bucket, prefix, marker));
        }

        MonoProcessor<String> reply = MonoProcessor.create();

        long cur = System.currentTimeMillis();

        res.subscribe(inodeList -> {
            List<byte[]> lineList = new LinkedList<>();
            String nextMarker = "";

            for (Inode inode : inodeList) {
                String fullPath = '/' + inode.getBucket() + '/' + inode.getObjName();
                String commonPrefix = StringUtils.getCommonPrefix(curDirectory, fullPath);
                byte[] name = StringUtils.removeStart(fullPath, commonPrefix).getBytes();
                int length = name.length;
                if (length > 1 && name[name.length - 1] == '/') {
                    length--;
                }

                byte[] rw = InodeUtils.getModeStr(inode.getMode());

                String time;
                long MTime = inode.getMtime() * 1000;
                if (MTime > cur - 15768000_000L) {
                    time = DateFormatUtils.formatUTC(MTime, "MMM dd HH:mm");
                } else {
                    time = DateFormatUtils.formatUTC(MTime, "MMM dd  yyyy");
                }

                //userID=0 groupID=0
                byte[] tmp = String.format(" %4d %-8d %-8d %8d %s ", inode.getLinkN(), 0, 0, inode.getSize(), time).getBytes();

                byte[] line = new byte[rw.length + tmp.length + length + 2];

                System.arraycopy(rw, 0, line, 0, rw.length);
                System.arraycopy(tmp, 0, line, rw.length, tmp.length);
                System.arraycopy(name, 0, line, rw.length + tmp.length, length);
                line[line.length - 2] = '\r';
                line[line.length - 1] = '\n';

                nextMarker = inode.getObjName();
                lineList.add(line);
            }

            for (byte[] line : lineList) {
                socket.write(Buffer.buffer(line));
            }

            if (inodeList.isEmpty()) {
                processor.onComplete();
            } else {
                processor.onNext(nextMarker);
            }
        }, e -> {
            log.error("", e);
        }, () -> {
            socket.end();
            reply.onNext(CLOSING_DATA_CONNECTION.reply());
        });

        processor.onNext("");

        return reply;
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.NLST)
    public Mono<String> nlst(FTPRequest ftpRequest, Session session, NetSocket socket) {
        String curDirectory = session.getWorkDir();
        if (curDirectory.equals(File.separator)) {
            // list bucket
            // 从redis获取当前用户下所有桶的 key
            String userBucketSet = session.getUserId() + USER_BUCKET_SET_SUFFIX;
            // 拼接结果
            StringBuilder listing = new StringBuilder();
            if (!"anonymous".equalsIgnoreCase(session.getUser())) {
                // 列举桶
                return pool.getReactive(REDIS_USERINFO_INDEX)
                        .smembers(userBucketSet)
                        .flatMap(bucket -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket))
                        .filter(bucketInfo -> StringUtils.isNotBlank(bucketInfo.get("fsid")))
                        .map(map -> {
                            listing.append(map.get("bucket_name") + "\r\n");
                            return true;
                        }).collectList().flatMap(list -> {
                            socket.write(listing.toString());
                            socket.end();

                            return Mono.just(CLOSING_DATA_CONNECTION.reply());
                        });
            } else {
                // 匿名用户列举所有开启匿名访问的桶
                return scanAnonymousBucketInfo()
                        .map(map -> {
                            listing.append(map.get("bucket_name") + "\r\n");
                            return true;
                        }).collectList().flatMap(list -> {
                            socket.write(listing.toString());
                            socket.end();

                            return Mono.just(CLOSING_DATA_CONNECTION.reply());
                        });
            }
        } else {
            Tuple2<String, String> tuple2 = session.getBucketAndObject("");
            String bucket = tuple2.var1;
            String prefix = tuple2.var2;

            //没有多次调用，不需要缓存
            UnicastProcessor<String> processor = UnicastProcessor.create();
            MonoProcessor<String> res = MonoProcessor.create();

            processor.flatMap(marker -> listObject(bucket, prefix, marker)).subscribe(list -> {
                StringBuilder listing = new StringBuilder();
                String nextMarker = "";
                for (Inode inode : list) {
                    nextMarker = inode.getObjName();

                    String filename = "";
                    String[] array = inode.getObjName().split("/");
                    if (array[array.length - 1].equals("/")) {
                        // 目录去掉后缀
                        filename = array[array.length - 2];
                    } else {
                        // 文件过滤前缀
                        filename = array[array.length - 1];
                    }

                    listing.append(filename + "\r\n");

                }
                socket.write(listing.toString());

                if (list.isEmpty()) {
                    processor.onComplete();
                    socket.end();
                    res.onNext(CLOSING_DATA_CONNECTION.reply());
                } else {
                    processor.onNext(nextMarker);
                }
            });

            processor.onNext("");

            return res;
        }
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.MLSD)
    public Mono<String> mlsd(FTPRequest ftpRequest, Session session, NetSocket socket) {
        String curDirectory = session.getWorkDir();
        if (curDirectory.equals(File.separator) && ftpRequest.getArgs().size() == 0) {
            // list bucket
            // 从readis获取当前用户下所有桶的 key
            return mlsdBucket(session, socket);
        } else if (ftpRequest.getArgs().size() == 0) {
            Tuple2<String, String> tuple2 = session.getBucketAndObject("");
            return mlsdObject(socket, tuple2);
        } else {
            String arg = ftpRequest.getArgs().get(0);

            if (arg.startsWith("/")) {
                curDirectory = ftpRequest.getArgs().get(0);
                if (curDirectory.equals("/")) {
                    return mlsdBucket(session, socket);
                }
            } else {
                curDirectory = curDirectory + ftpRequest.getArgs().get(0);
            }

            curDirectory += curDirectory.endsWith("/") ? "" : "/";

            Tuple2<String, String> tuple2 = session.getBucketAndObjectByFullPath(curDirectory);
            return mlsdObject(socket, tuple2);
        }
    }

    private static Mono<String> mlsdBucket(Session session, NetSocket socket) {
        String userBucketSet = session.getUserId() + USER_BUCKET_SET_SUFFIX;
        // 拼接结果
        StringBuilder listing = new StringBuilder();
        if (!"anonymous".equalsIgnoreCase(session.getUser())) {
            // 列举桶
            return pool.getReactive(REDIS_USERINFO_INDEX)
                    .smembers(userBucketSet)
                    .flatMap(bucket -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket))
                    .filter(bucketInfo -> StringUtils.isNotBlank(bucketInfo.get("ftp")) && bucketInfo.get("ftp").equals("1"))
                    .map(map -> {
                        listing.append("Size=").append(0).append(";").append("Modify=").append(MsDateUtils.formatFTPTime(Long.parseLong(map.get("ctime")))).append(";").append("Type=").append("dir").append("; ").append(map.get("bucket_name")).append("\r\n");
                        return true;
                    }).collectList().flatMap(list -> {
                        socket.write(listing.toString());
                        socket.end();

                        return Mono.just(CLOSING_DATA_CONNECTION.reply());
                    });
        } else {
            // 匿名用户列举所有开启匿名访问的桶
            return scanAnonymousBucketInfo()
                    .map(map -> {
                        listing.append("Size=").append(0).append(";").append("Modify=").append(MsDateUtils.formatFTPTime(Long.parseLong(map.get("ctime")))).append(";").append("Type=").append("dir").append("; ").append(map.get("bucket_name")).append("\r\n");
                        return true;
                    }).collectList().flatMap(list -> {
                        socket.write(listing.toString());
                        socket.end();

                        return Mono.just(CLOSING_DATA_CONNECTION.reply());
                    });
        }
    }

    private static Mono<List<Inode>> listObject(String bucket, String prefix, String marker) {
        return FsUtils.listObject(bucket, prefix, marker, READ_AHEAD_SIZE).flatMapMany(inodeList -> Flux.fromStream(inodeList.stream())).index().flatMap(t -> {
            Inode inode = t.getT2();
            if (inode.getCookie() == 0) {
                return Mono.just(t.getT1()).zipWith(Node.getInstance().createS3Inode(1, bucket, inode.getObjName(), inode.getVersionId(), inode.getReference(), inode.getACEs()));
            } else if (inode.getCounter() > 0) {
                return Mono.just(t.getT1()).zipWith(Node.getInstance().repairCookieAndInode(inode.getNodeId(), bucket, inode.getCreateTime()).map(inode1 -> {
                    if (inode1.getCreateTime() == 0 && inode1.getNodeId() > 0 && inode1.getAtime() > 0) {
                        inode1.setCreateTime(Inode.getMinTime(inode1));
                    }
                    return inode1;
                }));
            } else {
                return Mono.just(t);
            }
        }).collectList().map(inodeList -> {
            // 排序
            inodeList.sort((t1, t2) -> (int) (t1.getT1() - t2.getT1()));
            List<Inode> list = new ArrayList<>();
            for (reactor.util.function.Tuple2<Long, Inode> t : inodeList) {
                list.add(t.getT2());
            }
            return list;
        });
    }

    private MonoProcessor<String> mlsdObject(NetSocket socket, Tuple2<String, String> tuple2) {
        String bucket = tuple2.var1;
        String prefix = tuple2.var2;

        //没有多次调用，不需要缓存
        UnicastProcessor<String> processor = UnicastProcessor.create();
        MonoProcessor<String> res = MonoProcessor.create();

        processor.flatMap(marker -> listObject(bucket, prefix, marker)).subscribe(list -> {
            StringBuilder listing = new StringBuilder();
            String nextMarker = "";
            for (Inode inode : list) {
                String type = inode.getObjName().endsWith("/") ? "dir" : "file";
                listing.append("Size=").append(inode.getSize()).append(";").append("Modify=").append(MsDateUtils.formatFTPTime(inode.getMtime() * 1000)).append(";").append("Type=").append(type).append("; ");
                nextMarker = inode.getObjName();
                if (type.equals("dir")) {
                    // 目录去掉后缀
                    listing.append(inode.getObjName(), prefix.length(), inode.getObjName().length() - 1).append("\r\n");
                } else {
                    // 文件过滤前缀
                    listing.append(inode.getObjName().substring(prefix.length())).append("\r\n");
                }
            }

            socket.write(listing.toString());

            if (list.isEmpty()) {
                processor.onComplete();
                socket.end();
                res.onNext(CLOSING_DATA_CONNECTION.reply());
            } else {
                processor.onNext(nextMarker);
            }
        });

        processor.onNext("");

        return res;
    }
}
