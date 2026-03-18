package com.macrosan.filesystem.ftp.api;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.VersionUtil;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.ftp.FTPPort;
import com.macrosan.filesystem.ftp.FTPRequest;
import com.macrosan.filesystem.ftp.Session;
import com.macrosan.filesystem.lock.redlock.LockType;
import com.macrosan.filesystem.lock.redlock.RedLockClient;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.essearch.EsMetaTask;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsDateUtils;
import io.vertx.reactivex.core.net.NetSocket;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.ftp.FTP.ftpDebug;
import static com.macrosan.filesystem.ftp.FTPReply.*;
import static com.macrosan.filesystem.ftp.Session.encodePath;
import static com.macrosan.filesystem.ftp.api.FTPDataTransfer.FILE_CIFS_MODE;
import static com.macrosan.filesystem.ftp.api.FTPDataTransfer.FILE_MODE;
import static com.macrosan.message.jsonmsg.Inode.*;

/**
 * FTP 控制命令
 * 所有命令和响应参考 https://mina.apache.org/ftpserver-project/ftpserver_commands.html
 */
@Slf4j
public class FTPControl {

    private static RedisConnPool pool = RedisConnPool.getInstance();

    private static final Node nodeInstance = Node.getInstance();
    private static final FTPControl instance = new FTPControl();

    public static FTPControl getInstance() {
        return instance;
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.AUTH)
    public Mono<String> auth(FTPRequest ftpRequest, Session session) {
        if (ftpRequest.args.size() == 0 || !ftpRequest.args.get(0).equalsIgnoreCase("TLS")) {
            return Mono.just(SYNTAX_ERROR.reply());
        }
        return Mono.just(AUTH_OKAY.reply())
                .doFinally(s -> {
                    NetSocket socket = session.getControlSocket();
                    socket.upgradeToSsl(v0 -> {
                        if (ftpDebug) {
                            log.info("Connection upgraded to SSL successfully.");
                        }
                    });
                });
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.USER)
    public Mono<String> user(FTPRequest ftpRequest, Session session) {
        if (ftpRequest.args.size() < 1) {
            return Mono.just(INVALID_USER_NAME.reply());
        }

        String account = ftpRequest.args.get(0);

        session.setUser(account);
        if ("anonymous".equalsIgnoreCase(account)) {
            return Mono.just(GUEST_LOGIN_OK.reply());
        } else {
            return Mono.just(USER_NAME_OK_NEED_PASSWORD.reply());
        }
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.PASS)
    public Mono<String> pass(FTPRequest ftpRequest, Session session) {
        //TODO 没有登陆时不允许其他操作
        if (ftpRequest.args.size() < 1) {
            return Mono.just(AUTHENTICATION_FAILED.reply());
        }

        String pw = ftpRequest.args.get(0);

        if ("anonymous".equalsIgnoreCase(session.getUser())) {
            return Mono.just(USER_LOGGED_IN.reply());
        }

        return pool.getReactive(REDIS_USERINFO_INDEX).hgetall(session.getUser())
                .flatMap(map -> {
                    if (map.size() > 0 && map.get("passwd").equals(pw)) {
                        session.setUserId(map.get("id"));
                        return Mono.just(USER_LOGGED_IN.reply());
                    } else {
                        return Mono.just(AUTHENTICATION_FAILED.reply());
                    }
                });
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.CDUP)
    public Mono<String> cdup(FTPRequest ftpRequest, Session session) {
        String cur = session.getWorkDir();
        if ("/".equalsIgnoreCase(cur)) {
            return Mono.just(COMMAND_OK.reply());
        }

        //进入父目录不校验是否存在
        cur = encodePath(cur);
        String newWorkDir = URI.create(cur + "../").normalize().getPath();
        if (!newWorkDir.endsWith("/")) {
            newWorkDir += '/';
        }

        session.setWorkDir(newWorkDir);
        return Mono.just(COMMAND_OK.reply());
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.PWD)
    public Mono<String> pwd(FTPRequest ftpRequest, Session session) {
        return Mono.just(CURRENT_DIRECTORY.reply(session.getWorkDir()));
    }

    private static void tryPort(MonoProcessor<String> res, Session session) {
        if (!session.closed.get()) {
            int port = FTPPort.acquirePort(session.clientIP, session);
            if (port < 0) {
                FsUtils.fsExecutor.schedule(() -> tryPort(res, session), 100, TimeUnit.MILLISECONDS);
            } else {
                int port1 = port / 256;
                int port2 = port & 255;
                res.onNext(ENTER_PASSIVE_MODE.reply("(" + session.host.replace(".", ",") + "," + port1 + "," + port2 + ")"));
            }
        } else {
            res.onNext("\r\n");
        }
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.PASV)
    public Mono<String> pasv(FTPRequest ftpRequest, Session session) {
        MonoProcessor<String> res = MonoProcessor.create();
        tryPort(res, session);
        return res.doOnNext(s -> {
            session.dataRequestCache = MonoProcessor.create();
        });
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.CWD)
    public Mono<String> cwd(FTPRequest ftpRequest, Session session) {
        session.isRcloneClient = false;
        String directory = ftpRequest.args.get(0);
        String newWorkDir;
        // 设置当前文件路径
        if (!directory.startsWith(File.separator)) {
            newWorkDir = session.getWorkDir() + directory;
        } else {
            newWorkDir = directory;
        }

        if ("/".equalsIgnoreCase(newWorkDir)) {
            session.setWorkDir(newWorkDir);
            return Mono.just(COMMAND_OKAY.reply());
        }

        if (!newWorkDir.endsWith("/")) {
            newWorkDir += '/';
        }

        Tuple2<String, String> tuple2 = Session.getBucketAndObjectByFullPath(newWorkDir);

        String finalNewWorkDir = newWorkDir;

        return bucketIsExist(session, tuple2.var1)
                .flatMap(bucketIsExist -> bucketIsExist ? ftpOpenCheck(tuple2.var1, session.getUser()) : Mono.just(false))
                .flatMap(b -> {
                    if (b) {
                        if (StringUtils.isBlank(tuple2.var2)) {
                            session.setWorkDir(finalNewWorkDir);
                            return Mono.just(COMMAND_OKAY.reply());
                        } else {
                            return NFSBucketInfo.getFTPBucketInfoReactive(tuple2.var1)
                                    .flatMap(bktInfo -> {
                                        ReqInfo reqHeader = new ReqInfo();
                                        reqHeader.bucket = tuple2.var1;
                                        reqHeader.bucketInfo = bktInfo;
                                        return FsUtils.lookup(tuple2.var1, tuple2.var2, reqHeader, false, -1, null);
                                    })
                                    .map(inode -> {
                                        if (InodeUtils.isError(inode)) {
                                            return FAIL_CWD.reply();
                                        } else {
                                            // 针对游览器文件下载的情况，会发送请求"cwd file path"
                                            if (inode.getCifsMode() == FILE_CIFS_MODE) {
                                                return FAIL_CWD.reply();
                                            }
                                            session.setWorkDir(finalNewWorkDir);
                                            return COMMAND_OKAY.reply();
                                        }
                                    });
                        }
                    } else {
                        return Mono.just(FAIL_CWD.reply());
                    }
                });
    }

    // 当前用户下是否存在桶
    private Mono<Boolean> bucketIsExist(Session session, String bucket) {
        String userBucketSet = session.getUserId() + USER_BUCKET_SET_SUFFIX;
        if (!"anonymous".equalsIgnoreCase(session.getUser())) {
            return pool.getReactive(REDIS_USERINFO_INDEX).smembers(userBucketSet)
                    .filter(bucket0 -> bucket0.equals(bucket))
                    .flatMap(bucket0 -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket0))
                    .filter(bucketInfo -> StringUtils.isNotBlank(bucketInfo.get("ftp")) && bucketInfo.get("ftp").equals("1"))
                    .collectList()
                    .map(list -> list.size() > 0);
        } else {
            return pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucket)
                    .map(bucketInfo -> StringUtils.isNotBlank(bucketInfo.get("ftp")) && bucketInfo.get("ftp").equals("1") && "1".equals(bucketInfo.get("ftp_anonymous")));
        }
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.QUIT)
    public Mono<String> quit(FTPRequest ftpRequest, Session session) {
        return Mono.just(GOODBYE.reply());
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.TYPE)
    public Mono<String> type(FTPRequest ftpRequest, Session session) {
        //type 一般只支持I binary 和 A ascii
        //ascii和binary只对客户端有区别，服务端不需要处理
        if (ftpRequest.args.isEmpty() || !("I".equalsIgnoreCase(ftpRequest.args.get(0))
                || "A".equalsIgnoreCase(ftpRequest.args.get(0)))) {
            return Mono.just(COMMAND_NOT_IMPLEMENTED.reply());
        }

        String resStr = "200" + " " + "Type set to " + ftpRequest.args.get(0) + "." + "\r\n";
        return Mono.just(resStr);
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.STRU)
    public Mono<String> stru(FTPRequest ftpRequest, Session session) {
        //stru 一般只支持F 文件
        if (ftpRequest.args.isEmpty() || !"F".equalsIgnoreCase(ftpRequest.args.get(0))) {
            return Mono.just(STR_COMMAND_NOT_IMPLEMENTED.reply());
        }
        return Mono.just(COMMAND_OKAY.reply());
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.MODE)
    public Mono<String> mode(FTPRequest ftpRequest, Session session) {
        //mode 一般使用S(stream)模式，B(Block)和C(压缩)模式暂不支持。
        if (ftpRequest.args.isEmpty() || !"S".equalsIgnoreCase(ftpRequest.args.get(0))) {
            return Mono.just(STR_COMMAND_NOT_IMPLEMENTED.reply());
        }
        return Mono.just(COMMAND_OKAY.reply());
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.OPTS)
    public Mono<String> opts(FTPRequest ftpRequest, Session session) {
        return Mono.just(COMMAND_OKAY.reply());
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.LIST)
    public Mono<String> list(FTPRequest ftpRequest, Session session) {
        if (session.controlSocket != null) {
            session.controlSocket.write(OPENING_DATA_CONNECTION.reply());
            return Mono.just(notReply());
        } else {
            return Mono.just(OPENING_DATA_CONNECTION.reply());
        }
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.NLST)
    public Mono<String> nlst(FTPRequest ftpRequest, Session session) {
        if (session.controlSocket != null) {
            session.controlSocket.write(OPENING_DATA_CONNECTION.reply());
            return Mono.just(notReply());
        } else {
            return Mono.just(OPENING_DATA_CONNECTION.reply());
        }
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.FEAT)
    public Mono<String> feat(FTPRequest ftpRequest, Session session) {
        return Mono.just("211-Extensions supported\r\n" +
                " SIZE\r\n" +
                " MDTM\r\n" +
                " REST STREAM\r\n" +
                " LANG en;zh-tw;ja;is\r\n" +
                " MLST Size;Modify;Type;Perm\r\n" +
                " AUTH SSL\r\n" +
                " AUTH TLS\r\n" +
                " MODE Z\r\n" +
                " UTF8\r\n" + // 避免中文乱码
                " TVFS\r\n" +
                " MD5\r\n" +
                " MMD5\r\n" +
                " MFMT\r\n" +
                "211 End\r\n");
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.MKD)
    public Mono<String> mkd(FTPRequest ftpRequest, Session session) {
        Tuple2<String, String> tuple2 = session.getBucketAndObject(ftpRequest.args.get(0));
        String bucket = tuple2.var1;
        // 兼容curl传入带"/"后缀的目录名
        String objName = tuple2.var2.endsWith(File.separator) ? tuple2.var2.substring(0, tuple2.var2.length() - File.separator.length()) : tuple2.var2;

        if (StringUtils.isBlank(objName)) {
            //TODO create bucket
            return Mono.just(CANNOT_CREATE_DIRECTORY.reply());
        } else {
            return ftpOpenCheck(bucket, session.getUser())
                    .flatMap(open -> {
                        if (open) {
                            return InodeUtils.findDirInode(objName, bucket)
                                    .flatMap(dirInode -> {
                                        if (InodeUtils.isError(dirInode)) {
                                            return Mono.just(CANNOT_CREATE_DIRECTORY.reply());
                                        }

                                        //TODO 判断是否存在
                                        return CheckUtils.ftpWritePermissionCheck(bucket, session.getUser())
                                                .flatMap(pass -> {
                                                    session.writePermission = pass;
                                                    if (!pass) {
                                                        return Mono.just(NO_PERMISSION.reply());
                                                    }


                                                    int mode = DEFAULT_DIR_MODE | S_IFDIR;
                                                    return NFSBucketInfo.getFTPBucketInfoReactive(bucket)
                                                            .flatMap(bktInfo -> {
                                                                ReqInfo reqHeader = new ReqInfo();
                                                                reqHeader.bucketInfo = bktInfo;
                                                                reqHeader.bucket = bucket;
                                                                return InodeUtils.ftpCreate(reqHeader, dirInode, mode, FILE_ATTRIBUTE_DIRECTORY, objName);
                                                            })
                                                            .map(inode -> {
                                                                if (InodeUtils.isError(inode)) {
                                                                    return CANNOT_CREATE_DIRECTORY.reply();
                                                                } else {
                                                                    return DIRECTORY_CREATED.reply();
                                                                }
                                                            });
                                                });
                                    });
                        } else {
                            return Mono.just(CANNOT_CREATE_DIRECTORY.reply());
                        }
                    });
        }
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.RMD)
    public Mono<String> rmd(FTPRequest ftpRequest, Session session) {
        Tuple2<String, String> tuple2 = session.getBucketAndObject(ftpRequest.args.get(0));
        String bucket = tuple2.var1;
        String objName = tuple2.var2;

        if (StringUtils.isBlank(bucket)) {
            return Mono.just(NOT_A_VALID_DIRECTORY.reply());
        }

        if (StringUtils.isBlank(objName)) {
            //TODO delete bucket
            return Mono.just(NOT_A_VALID_DIRECTORY.reply());
        }

        ReqInfo reqHeader = new ReqInfo();
        return NFSBucketInfo.getFTPBucketInfoReactive(bucket)
                .flatMap(bktInfo -> {
                    reqHeader.bucketInfo = bktInfo;
                    reqHeader.bucket = bucket;
                    return ftpOpenCheck(bucket, session.getUser());
                })
                .flatMap(open -> {
                    if (open) {
                        return FsUtils.lookup(bucket, objName, reqHeader, false, -1, null)
                                .flatMap(inode -> {
                                    if (InodeUtils.isError(inode) || !inode.getObjName().endsWith("/")) {
                                        return Mono.just(NOT_A_VALID_DIRECTORY.reply());
                                    }

                                    return CheckUtils.ftpWritePermissionCheck(bucket, session.getUser())
                                            .flatMap(pass -> {
                                                session.writePermission = pass;
                                                if (!pass) {
                                                    return Mono.just(NO_PERMISSION.reply());
                                                }
                                                return FsUtils.listObject(bucket, inode.getObjName(), "", 1)
                                                        .flatMap(inodeList -> {
                                                            if (inodeList.isEmpty()) {
                                                                return nodeInstance.deleteInode(inode.getNodeId(), inode.getBucket(), inode.getObjName())
                                                                        .flatMap(r -> {
                                                                            if (InodeUtils.isError(r)) {
                                                                                return Mono.just(EXECUTION_FAILED.reply());
                                                                            }
                                                                            return Mono.just(ES_ON.equals(reqHeader.bucketInfo.get(ES_SWITCH)))
                                                                                    .flatMap(b -> b ? EsMetaTask.delEsMeta(r, inode, bucket, inode.getObjName(), inode.getNodeId(), true) : Mono.just(true))
                                                                                    .flatMap(c -> InodeUtils.findDirInode(inode.getObjName(), bucket)
                                                                                            .flatMap(dirInode -> {
                                                                                                long stamp = System.currentTimeMillis() / 1000;
                                                                                                int timeNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
                                                                                                return nodeInstance.updateInodeTime(dirInode.getNodeId(), bucket, stamp, timeNano, false, true, true);
                                                                                            }).map(b -> DIRECTORY_REMOVED.reply()));

                                                                        });
                                                            } else {
                                                                return Mono.just(CANNOT_REMOVE_DIRECTORY.reply());
                                                            }
                                                        });
                                            });
                                });
                    }
                    return Mono.just(NOT_A_VALID_DIRECTORY.reply());
                });
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.MLSD)
    public Mono<String> mlsd(FTPRequest ftpRequest, Session session) {
        if (session.controlSocket != null) {
            session.controlSocket.write(OPENING_DATA_CONNECTION.reply());
            return Mono.just(notReply());
        } else {
            return Mono.just(OPENING_DATA_CONNECTION.reply());
        }
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.MLST)
    public Mono<String> mlst(FTPRequest ftpRequest, Session session) {
        if (ftpRequest.args.size() > 0) {
            String path = ftpRequest.getArgs().get(0);
            if (!path.startsWith("/")) {
                path = session.getWorkDir() + path;
            }
            Tuple2<String, String> tuple2 = session.getBucketAndObjectByFullPath(path);
            String bucket = tuple2.var1;
            String object = tuple2.var2;
            if (object.equals("")) {
                Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucket);
                if (!bucketInfo.isEmpty()) {
                    StringBuilder listing = new StringBuilder();
                    listing.append("Size=").append(0).append(";")
                            .append("Modify=").append(MsDateUtils.formatFTPTime(Long.valueOf(bucketInfo.get("ctime")))).append(";")
                            .append("Type=").append("dir").append("; ")
                            .append(bucket).append("\r\n");
                    return Mono.just("250-\r\n" +
                            listing.toString() +
                            "\r\n" +
                            "250 Requested file action okay, completed.\r\n");
                }
            } else {
                ReqInfo reqHeader = new ReqInfo();
                return NFSBucketInfo.getFTPBucketInfoReactive(bucket)
                        .flatMap(bktInfo -> {
                            reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(bucket);
                            reqHeader.bucket = bucket;
                            return FsUtils.lookup(bucket, object, reqHeader, false, -1, null);
                        })
                        .flatMap(inode -> {
                            if (NOT_FOUND_INODE.equals(inode)) {
                                return Mono.just(NOT_A_VALID_PATHNAME.reply());
                            }
                            StringBuilder listing = new StringBuilder();
                            String type = inode.getObjName().endsWith("/") ? "dir" : "file";
                            listing.append("Size=").append(inode.getSize()).append(";")
                                    .append("Modify=").append(MsDateUtils.formatFTPTime(inode.getMtime() * 1000)).append(";")
                                    .append("Type=").append(type).append("; ")
                                    .append(inode.getObjName()).append("\r\n");
                            return Mono.just("250-\r\n" +
                                    listing.toString() +
                                    "\r\n" +
                                    "250 Requested file action okay, completed.\r\n");
                        });
            }
        }
        return Mono.just(NOT_A_VALID_PATHNAME.reply());
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.STOR)
    public Mono<String> stor(FTPRequest ftpRequest, Session session) {
        String fileName = ftpRequest.args.get(0);
        String bucket;
        String objName;
        try {
            Tuple2<String, String> tuple2 = session.getBucketAndObject(fileName);
            bucket = tuple2.var1;
            objName = tuple2.var2;
        } catch (Exception e) {
            log.error("fileName:{} parse error, ", fileName, e);
            return Mono.just(FILE_NAME_NOT_ALLOWED.reply());
        }

        if (StringUtils.isBlank(bucket) || StringUtils.isBlank(objName)) {
            return Mono.just(FILE_NAME_NOT_ALLOWED.reply());
        }

        ReqInfo reqHeader = new ReqInfo();
        return NFSBucketInfo.getFTPBucketInfoReactive(bucket)
                .flatMap(bktInfo -> {
                    reqHeader.bucketInfo = bktInfo;
                    reqHeader.bucket = bucket;
                    session.reqHeader = reqHeader;
                    return ftpOpenCheck(bucket, session.getUser());
                })
                .flatMap(open -> {
                    if (open) {
                        return CheckUtils.ftpWritePermissionCheck(bucket, session.getUser())
                                .flatMap(pass -> {
                                    session.writePermission = pass;
                                    if (!pass) {
                                        return Mono.just(PERMISSION_DENIED.reply());
                                    }
                                    return FsUtils.lookup(bucket, objName, reqHeader, false, 1, null)
                                            .flatMap(inode -> {
                                                if (InodeUtils.isError(inode)) {
                                                    if (NOT_FOUND_INODE.equals(inode)) {
                                                        return InodeUtils.findDirInode(objName, bucket)
                                                                .flatMap(dirInode -> {
                                                                    if (InodeUtils.isError(dirInode)) {
                                                                        return Mono.just(ERROR_ON_OUTPUT_FILE.reply());
                                                                    }
                                                                    return RedLockClient.lock(reqHeader, objName, LockType.WRITE, true, true)
                                                                            .flatMap(lock -> InodeUtils.ftpCreate(reqHeader, dirInode, DEFAULT_FILE_MODE | S_IFREG, FILE_CIFS_MODE, objName))
                                                                            .flatMap(createInode -> {
                                                                                if (InodeUtils.isError(createInode)) {
                                                                                    // inode未创建成功需要释放锁
                                                                                    session.releaseLock();
                                                                                    return Mono.just(ERROR_ON_OUTPUT_FILE.reply());
                                                                                } else {
                                                                                    session.cachedInode = createInode;
                                                                                    return Mono.just(OPENING_DATA_CONNECTION.reply());
                                                                                }
                                                                            });
                                                                });
                                                    } else {
                                                        return Mono.just(ERROR_ON_OUTPUT_FILE.reply());
                                                    }
                                                } else {
                                                    return RedLockClient.lock(reqHeader, objName, LockType.WRITE, true, true)
                                                            .flatMap(lock -> {
                                                                // check连接是否已经释放,避免上传期间多次覆盖导致锁争抢未按顺序执行，走下面的流程，参考 SERVER-2041 问题单
                                                                if (session.closed.get()) {
                                                                    session.releaseLock();
                                                                    return Mono.just(CANNOT_OPEN_DATA_CONNECTION.reply());
                                                                }
                                                                // 断点续传
                                                                if (session.restValue != -1) {
                                                                    session.cachedInode = inode;
                                                                    return Mono.just(OPENING_DATA_CONNECTION.reply());
                                                                }
                                                                // 上传同名文件
                                                                ObjAttr attr = new ObjAttr();
                                                                attr.hasSize = 1;
                                                                attr.size = 0L;
                                                                return nodeInstance.setAttr(inode.getNodeId(), bucket, attr, null)
                                                                        .flatMap(setInode -> {
                                                                            if (InodeUtils.isError(setInode)) {
                                                                                // inode属性未修改成功需要释放锁
                                                                                session.releaseLock();
                                                                                return Mono.just(ERROR_ON_OUTPUT_FILE.reply());
                                                                            } else {
                                                                                session.cachedInode = setInode;
                                                                                return Mono.just(OPENING_DATA_CONNECTION.reply());
                                                                            }
                                                                        });
                                                            });
                                                }
                                            });
                                });
                    }
                    return Mono.just(ERROR_ON_OUTPUT_FILE.reply());
                });
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.RETR)
    public Mono<String> retr(FTPRequest ftpRequest, Session session) {
        return searchFile(ftpRequest, session)
                .flatMap(inode -> {
                    if (InodeUtils.isError(inode) || inode.getCifsMode() != FILE_CIFS_MODE) {
                        return Mono.just(FILE_UNAVAILABLE.reply());
                    } else {
                        return Mono.just(OPENING_DATA_CONNECTION.reply());
                    }
                });
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.SYST)
    public Mono<String> syst(FTPRequest ftpRequest, Session session) {
        return Mono.just(SYSTEM_TYPE.reply());
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.DELE)
    public Mono<String> dele(FTPRequest ftpRequest, Session session) {
        Tuple2<String, String> tuple2 = session.getBucketAndObject(ftpRequest.args.get(0));
        String bucket = tuple2.var1;
        String objName = tuple2.var2;

        if (StringUtils.isBlank(objName) || objName.endsWith("/")) {
            return Mono.just(CANT_DELETE_FILE.reply());
        }

        ReqInfo reqHeader = new ReqInfo();
        return NFSBucketInfo.getFTPBucketInfoReactive(bucket)
                .flatMap(bktInfo -> {
                    reqHeader.bucketInfo = bktInfo;
                    reqHeader.bucket = bucket;
                    return ftpOpenCheck(bucket, session.getUser());
                })
                .flatMap(open -> {
                    if (open) {
                        return searchFile(ftpRequest, session)
                                .flatMap(inode -> {
                                    if (InodeUtils.isError(inode)) {
                                        return Mono.just(EXECUTION_FAILED.reply());
                                    }
                                    return CheckUtils.ftpWritePermissionCheck(bucket, session.getUser())
                                            .flatMap(pass -> {
                                                session.writePermission = pass;
                                                if (!pass) {
                                                    return Mono.just(NO_PERMISSION_TO_DELETE.reply());
                                                }
                                                return nodeInstance.deleteInode(inode.getNodeId(), inode.getBucket(), inode.getObjName())
                                                        .flatMap(r -> {
                                                            if (InodeUtils.isError(r)) {
                                                                return Mono.just(EXECUTION_FAILED.reply());
                                                            }
                                                            return Mono.just(ES_ON.equals(reqHeader.bucketInfo.get(ES_SWITCH)))
                                                                    .flatMap(b -> b ? EsMetaTask.delEsMeta(r, inode, bucket, inode.getObjName(), inode.getNodeId(), true) : Mono.just(true))
                                                                    .flatMap(c -> InodeUtils.findDirInode(inode.getObjName(), bucket))
                                                                    .flatMap(dirInode -> {
                                                                        long stamp = System.currentTimeMillis() / 1000;
                                                                        int timeNano = (int) (System.nanoTime() % ONE_SECOND_NANO);
                                                                        return nodeInstance.updateInodeTime(dirInode.getNodeId(), bucket, stamp, timeNano, false, true, true);
                                                                    }).map(b -> COMMAND_OK.reply());
                                                        });
                                            });
                                });
                    }
                    return Mono.just(CANT_DELETE_FILE.reply());
                });
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.NOOP)
    public Mono<String> noop(FTPRequest ftpRequest, Session session) {
        return Mono.just(COMMAND_OKAY.reply());
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.RNFR)
    public Mono<String> rnfr(FTPRequest ftpRequest, Session session) {
        Tuple2<String, String> tuple2 = session.getBucketAndObject(ftpRequest.args.get(0));
        String bucket = tuple2.var1;
        String objName = tuple2.var2;
        return searchFile(ftpRequest, session)
                .flatMap(inode -> {
                    if (InodeUtils.isError(inode)) {
                        return Mono.just(FILE_UNAVAILABLE.reply());
                    } else {
                        return CheckUtils.ftpWritePermissionCheck(bucket, session.getUser())
                                .flatMap(pass -> {
                                    session.writePermission = pass;
                                    if (!pass) {
                                        return Mono.just(NO_PERMISSION.reply());
                                    }
                                    session.setRenameFileInfo(new Session.RenameFileInfo(inode.getObjName(), inode.getNodeId()));
                                    session.sourcePath = Paths.get(File.separator, bucket).resolve(objName)
                                            .normalize().toString();
                                    return Mono.just(REQUESTED_FILE_ACTION_PENDING.reply());
                                });
                    }
                });
    }

    private Mono<Inode> searchFileRNTO(FTPRequest ftpRequest, Session session) {
        // 判断文件是否存在
        Tuple2<String, String> tuple2 = session.getBucketAndObject(ftpRequest.args.get(0));
        String bucket = tuple2.var1;
        String objName = tuple2.var2;

        // 判断文件是否存在
        return InodeUtils.findDirInode(objName, bucket)
                .flatMap(dirInode -> {
                    // 非rclone客户端，文件名不能包含 "/"
                    if (!session.isRcloneClient) {
                        Path targetPath = Paths.get(File.separator, bucket).resolve(objName).normalize();

                        Path dirPath = Paths.get(File.separator, bucket).resolve(Optional.ofNullable(dirInode.getObjName()).orElse(""))
                                .normalize();

                        // 提取“源文件的根目录” (/bucketName),
                        String rootPrefix = "";
                        int secondSlashIndex = session.sourcePath.indexOf(File.separator, 1); // 从索引1开始找下一个斜杠
                        if (secondSlashIndex > 0) {
                            rootPrefix = session.sourcePath.substring(0, secondSlashIndex);
                        } else {
                            // 只有一层目录的情况 (比如 /bucketName)，本身就是根
                            rootPrefix = session.sourcePath;
                        }

                        // 1.检查目标父目录是否存在：防止重命名到不存在的路径（基本的防创建目录）。
                        if (targetPath.getParent() != null && !targetPath.getParent().equals(dirPath) || StringUtils.isBlank(objName)) {
                            return Mono.just(INVALID_ARGUMENT_INODE);
                        }

                        // 2.检查：目标文件需要与源文件在一个桶 (根目录) 下
                        if(!targetPath.startsWith(rootPrefix)) {
                            return Mono.just(INVALID_ARGUMENT_INODE);
                        }

                        // 3.检查“源路径”是否等于“目标父目录”：禁止移动到自己的子目录 (dir -> dir/subdir)
                        // 如果 Source 和 TargetParent 路径相同，说明你在试图把一个目录变成它自己的子目录
                        if (targetPath.getParent() != null && session.sourcePath.equals(targetPath.getParent().toString())) {
                            return Mono.just(INVALID_ARGUMENT_INODE);
                        }

                        // 4.检查“源路径”是否是“目标路径”的上级：防止递归移动（例如把 A 移动到 A/B/C 中）。
                        // 如果是目录，且目标路径是以源路径开头，说明试图移动到自己里面
                        if (session.sourcePath != null && dirInode.getObjName().endsWith("/") && targetPath.toString().startsWith(session.sourcePath + "/")) {
                            return Mono.just(INVALID_ARGUMENT_INODE);
                        }
                    }

                    return NFSBucketInfo.getFTPBucketInfoReactive(bucket)
                            .flatMap(bktInfo -> {
                                ReqInfo reqHeader = new ReqInfo();
                                reqHeader.bucketInfo = bktInfo;
                                reqHeader.bucket = bucket;
                                return FsUtils.lookup(bucket, objName, reqHeader, true, dirInode.getNodeId(), dirInode.getACEs());
                            });
                });
    }

    private Mono<Inode> searchFile(FTPRequest ftpRequest, Session session) {
        // 判断文件是否存在
        Tuple2<String, String> tuple2 = session.getBucketAndObject(ftpRequest.args.get(0));
        String bucket = tuple2.var1;
        String objName = tuple2.var2;

        // 判断文件是否存在
        return NFSBucketInfo.getFTPBucketInfoReactive(bucket)
                .flatMap(bktInfo -> {
                    ReqInfo reqHeader = new ReqInfo();
                    reqHeader.bucketInfo = bktInfo;
                    reqHeader.bucket = bucket;
                    return InodeUtils.findDirInode(objName, bucket)
                            .flatMap(dirInode -> FsUtils.lookup(bucket, objName, reqHeader, true, dirInode.getNodeId(), dirInode.getACEs()));
                });
    }


    @FTPRequest.FTPOpt(FTPRequest.Command.RNTO)
    public Mono<String> rnto(FTPRequest ftpRequest, Session session) {
        String path = ftpRequest.args.get(0);

        if (session.getRenameFileInfo() == null) {
            return Mono.just(CANNOT_RENAME_FILE.reply());
        }

        Tuple2<String, String> tuple2 = session.getBucketAndObject(path);
        String bucket = tuple2.var1;

        String newObj = tuple2.var2;
        String oldObj = session.getRenameFileInfo().getObjName();

        if (newObj.getBytes(StandardCharsets.UTF_8).length > NFS_MAX_NAME_LENGTH || CheckUtils.isFileNameTooLong(newObj)) {
            return Mono.just(CANNOT_RENAME_FILE.reply());
        }
        // 判断重命名文件名是否已经存在
        return searchFileRNTO(ftpRequest, session)
                .flatMap(inode -> {
                    if (InodeUtils.isError(inode)) {
                        return CheckUtils.ftpWritePermissionCheck(bucket, session.getUser())
                                .flatMap(pass -> {
                                    session.writePermission = pass;
                                    if (!pass) {
                                        return Mono.just(NO_PERMISSION_RNTO.reply());
                                    }
                                    return FileOrDirRename.rename(bucket, session.getRenameFileInfo().getNodeId(), oldObj, newObj, false, session)
                                            .flatMap(b -> {
                                                if (b) {
                                                    return Mono.just(FILE_ACTION_OK_RENAMED.reply());
                                                } else {
                                                    return Mono.just(CANNOT_RENAME_FILE.reply());
                                                }
                                            });
                                });
                    } else if (INVALID_ARGUMENT_INODE.equals(inode)) {
                        return Mono.just(FILE_NAME_NOT_ALLOWED.reply());
                    } else {
                        return Mono.just(CANNOT_RENAME_FILE.reply());
                    }
                });
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.SITE)
    public Mono<String> site(FTPRequest ftpRequest, Session session) {
        if (ftpRequest.args.isEmpty()) {
            //TODO
        }

        String siteCMD = ftpRequest.args.get(0);

        switch (siteCMD.toUpperCase()) {
            case "CHMOD":
                //TODO
                return Mono.just(COMMAND_OKAY.reply());
        }

        return Mono.just(UNKNOWN_COMMAND.reply());
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.SIZE)
    public Mono<String> size(FTPRequest ftpRequest, Session session) {
        if (ftpRequest.args.get(0).endsWith("/")) {
            return Mono.just(NOT_A_PLAIN_FILE.reply());
        } else {
            return searchFile(ftpRequest, session)
                    .flatMap(inode -> {
                        if (InodeUtils.isError(inode)) {
                            return Mono.just(NOT_A_PLAIN_FILE.reply());
                        } else {
                            return Mono.just(SIZE.reply(String.valueOf(inode.getSize())));
                        }
                    });
        }
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.STOU)
    public Mono<String> stou(FTPRequest ftpRequest, Session session) {
        String objName = String.valueOf(VersionUtil.getVersionNum());
        ftpRequest.args.add(objName);
        Tuple2<String, String> tuple2 = session.getBucketAndObject(ftpRequest.args.get(0));
        String bucket = tuple2.var1;
        return CheckUtils.ftpWritePermissionCheck(bucket, session.getUser())
                .flatMap(pass -> {
                    session.writePermission = pass;
                    if (!pass) {
                        return Mono.just(PERMISSION_DENIED.reply());
                    }
                    return Mono.just(FILE_OPENING_DATA_CONNECTION.reply(objName));
                });
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.APPE)
    public Mono<String> appe(FTPRequest ftpRequest, Session session) {
        Tuple2<String, String> tuple2 = session.getBucketAndObject(ftpRequest.args.get(0));
        String bucket = tuple2.var1;
        String objName = tuple2.var2;
        return CheckUtils.ftpWritePermissionCheck(bucket, session.getUser())
                .flatMap(pass -> {
                    session.writePermission = pass;
                    if (!pass) {
                        return Mono.just(PERMISSION_DENIED.reply());
                    }
                    return Mono.just(OPENING_DATA_CONNECTION.reply());
                });
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.REST)
    public Mono<String> rest(FTPRequest ftpRequest, Session session) {
        if (ftpRequest.getArgs().size() == 0) {
            return Mono.just(SYNTAX_ERROR.reply());
        }
        long offset = 0L;
        try {
            offset = Long.valueOf(ftpRequest.args.get(0));
        } catch (Exception e) {
            return Mono.just(INVALID_NUMBER.reply());
        }
        if (offset < 0) {
            return Mono.just(NEGATIVE_MARKER.reply());
        }
        session.restValue = offset;
        return Mono.just(RESTART_POSITION.reply(String.valueOf(offset)));
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.ALLO)
    public Mono<String> allo(FTPRequest ftpRequest, Session session) {
        return Mono.just(COMMAND_OKAY.reply());
    }

    public Mono<Boolean> ftpOpenCheck(String bucket, String user) {
        if (StringUtils.isBlank(bucket)) {
            return Mono.just(true);
        }
        return CheckUtils.ftpOpenCheck(bucket, user);
    }

    @FTPRequest.FTPOpt(FTPRequest.Command.MFMT)
    public Mono<String> mfmt(FTPRequest ftpRequest, Session session) {
        String modifyTime = ftpRequest.getArgs().get(0);
        String filename = ftpRequest.getArgs().get(1);
        Tuple2<String, String> tuple2 = session.getBucketAndObject(filename);
        String bucket = tuple2.var1;
        String objName = tuple2.var2;

        // TODO 更新对象上传时间
        return NFSBucketInfo.getFTPBucketInfoReactive(bucket)
                .flatMap(bktInfo -> {
                    ReqInfo reqHeader = new ReqInfo();
                    reqHeader.bucketInfo = NFSBucketInfo.getBucketInfo(bucket);
                    reqHeader.bucket = bucket;
                    return InodeUtils.findDirInode(objName, bucket)
                            .flatMap(dirInode -> FsUtils.lookup(bucket, objName, reqHeader, true, dirInode.getNodeId(), dirInode.getACEs()));
                })
                .flatMap(inode -> {
                    if (InodeUtils.isError(inode)) {
                        return Mono.just(NO_SUCH_FILE_OR_DIRECTORY.reply());
                    }

                    return CheckUtils.ftpWritePermissionCheck(bucket, session.getUser())
                            .flatMap(pass -> {
                                session.writePermission = pass;
                                if (!pass) {
                                    return Mono.just(NO_PERMISSION_RNTO.reply());
                                }

                                ObjAttr mfmtAttr = new ObjAttr();
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
                                LocalDateTime localDateTime = LocalDateTime.parse(modifyTime, formatter);
                                // FTP 协议 (RFC 3659) 规定 MFMT 时间通常是 UTC
                                long timestampUtc = localDateTime.toEpochSecond(ZoneOffset.UTC);
                                // 设置需要修改的属性开关
                                mfmtAttr.hasMtime = 2;
                                // 获取 mtime
                                mfmtAttr.mtime = (int)timestampUtc;
                                mfmtAttr.mtimeNano = 0;

                                return nodeInstance.setAttr(inode.getNodeId(), inode.getBucket(), mfmtAttr, null)
                                        .flatMap(resInode -> {
                                            if (InodeUtils.isError(resInode)) {
                                                return Mono.just(REQUESTED_ACTION_NOT_TAKEN.reply());
                                            }

                                            return Mono.just("213 ModifyTime=" + modifyTime + "; " + objName + "\r\n");
                                        });
                            });
                });
    }
}
