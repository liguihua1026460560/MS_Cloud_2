package com.macrosan.filesystem.ftp.handler;

import com.macrosan.filesystem.ftp.FTP;
import com.macrosan.filesystem.ftp.FTPPort;
import com.macrosan.filesystem.ftp.FTPRequest;
import com.macrosan.filesystem.ftp.Session;
import com.macrosan.filesystem.nfs.NFSException;
import io.vertx.core.Context;
import io.vertx.reactivex.core.net.NetSocket;
import io.vertx.reactivex.core.parsetools.RecordParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_DQUOT;
import static com.macrosan.filesystem.ftp.FTP.ftpDebug;
import static com.macrosan.filesystem.ftp.FTPReply.*;
import static com.macrosan.filesystem.ftp.FTPRequest.Command.QUIT;
import static com.macrosan.message.jsonmsg.Inode.FILES_QUOTA_EXCCED_INODE;

@Slf4j
public class ControlHandler {

    StringBuilder sb = new StringBuilder();

    //ftp控制流不用于传数据，暂不限制running数量
    public void handle(NetSocket socket, Context context) {
        socket.pause();

        // 创建用户session
        Session session = new Session();
        // 初始化
        session.setControlSocket(socket);
        session.setContext(context);
        session.setHost(socket.localAddress().host());
        session.setClientIP(socket.remoteAddress().host());

        RecordParser parser = RecordParser.newDelimited("\r\n");

        parser.handler(buffer -> {
            try {
                String line = buffer.toString(StandardCharsets.UTF_8);
                if (StringUtils.isNotEmpty(line)) {
                    handleNewCommand(line, socket, session);
                }
            } catch (Exception e) {
                log.error("", e);
                socket.close();
            }
        });

        parser.exceptionHandler(throwable -> {
            // 解析层异常
            log.error("Parser exception, possibly malformed data", throwable);
            // 关闭socket
            socket.close();
        });

        // 处理连接
        socket.handler(parser);

        socket.exceptionHandler(e -> {
            log.error("ftp control socket error", e);
            // 释放文件锁
            session.close();
            session.releaseLock();
            // 释放数据连接端口号
            try {
                FTPPort.releasePort(session.clientIP, session.dataTransferport, session);
            } catch (Exception e0) {
                log.error("exception release error", e0);
            }

            if (ftpDebug) {
                log.info("data port rest: {}", session.calculateRestDataPortNum());
            }
        });
        socket.endHandler(v -> session.close());
        socket.closeHandler(v -> {
            session.close();
            session.releaseLock();
            // 释放数据连接端口号
            try {
                FTPPort.releasePort(session.clientIP, session.dataTransferport, session);
            } catch (Exception e) {
                log.error("close release error", e);
            }

            if (ftpDebug) {
                log.info("data port rest: {}", session.calculateRestDataPortNum());
            }
        });
        socket.resume();
        socket.write(WELCOME.reply());
    }

    private void handleNewCommand(String commandLine, NetSocket socket, Session session) {
        // 注册当前请求命令
        FTPRequest ftpRequest = new FTPRequest(commandLine);
        session.setCurRequest(ftpRequest);

        // 通过对应命令找到对应调用方法
        String cmdName = ftpRequest.command.name();
        FTP.OptInfo optInfo = FTP.ftpOpt.get(cmdName);
        if (optInfo != null) {
            if (ftpDebug) {
                log.info("ftp call {}:{}", ftpRequest.command, ftpRequest.args);
            }

            optInfo.run(ftpRequest, session)
                    .onErrorResume(e -> {
                        log.error("", e);
                        session.releaseLock();
                        if (e instanceof NFSException) {
                            NFSException nfsException = (NFSException) e;
                            if (nfsException.getErrCode() == NFS3ERR_DQUOT && nfsException.nfsError) {
                                session.cachedInode = FILES_QUOTA_EXCCED_INODE;
                                return Mono.just(EXCEEDED_QUOTA.reply());
                            } else {
                                return Mono.just(DATA_CONNECTION_ERROR.reply());
                            }
                        } else {
                            return Mono.just(DATA_CONNECTION_ERROR.reply());
                        }
                    })
                    .subscribe(msg -> {
                        if (ftpDebug) {
                            log.info("ftp reply {}", msg);
                        }

                        if (FTP.dataTransferOpt.get(cmdName) != null) {
                            if (session.dataRequestCache != null) {
                                session.dataRequestCache.onNext(ftpRequest);
                            }
                        }
                        if (!notReply().equals(msg)) {
                            socket.write(msg);
                        }
                        if (cmdName.equals(QUIT)) {
                            socket.end();
                        }
                    });
        } else {
            socket.write(UNKNOWN_COMMAND.reply());
        }
    }


}
