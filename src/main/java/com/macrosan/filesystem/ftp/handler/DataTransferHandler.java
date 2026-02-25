package com.macrosan.filesystem.ftp.handler;

import com.macrosan.filesystem.ftp.FTP;
import com.macrosan.filesystem.ftp.FTPPort;
import com.macrosan.filesystem.ftp.Session;
import com.macrosan.filesystem.nfs.NFSException;
import io.vertx.reactivex.core.net.NetSocket;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_DQUOT;
import static com.macrosan.filesystem.ftp.FTP.ftpDebug;
import static com.macrosan.filesystem.ftp.FTPReply.*;

@Log4j2
public class DataTransferHandler {
    public void handle(int port, NetSocket socket) {
        socket.pause();
        String ip = socket.remoteAddress().host();
        Session session = FTPPort.getSession(ip, port);

        if (session == null) {
            socket.close();
            return;
        }

        FTPPort.releasePort(session.clientIP,session.dataTransferport, session);
        session.dataSocket = null;
        session.dataTransferport = 0;

        socket.exceptionHandler(e -> {
            log.error("", e);
            session.getControlSocket().write(DATA_CONNECTION_ERROR.reply());
            session.releaseLock();
        });

        socket.closeHandler(v -> {
            session.releaseLock();
        });

        if (session.dataRequestCache != null) {
            session.dataRequestCache.subscribe(ftpRequest -> {
                session.dataRequestCache = null;
                try {
                    String cmdName = ftpRequest.command.name();
                    FTP.DataTransferOptInfo optInfo = FTP.dataTransferOpt.get(cmdName);

                    //ftp使用和s3类似的限流机制 不适用nfs和cifs的running
                    if (optInfo != null) {
                        optInfo.run(ftpRequest, session, socket)
                                .onErrorResume(e -> {
                                    log.error("", e);
                                    session.releaseLock();
                                    if (e instanceof NFSException) {
                                        NFSException nfsException = (NFSException) e;
                                        if (nfsException.getErrCode() == NFS3ERR_DQUOT && nfsException.nfsError) {
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

                                    if (!notReply().equals(msg)) {
                                        session.getControlSocket().write(msg);
                                    } else {
                                        socket.close();
                                    }
                                });
                    } else {
                        session.controlSocket.write(UNKNOWN_COMMAND.reply());
                        socket.close();
                    }
                } catch (Exception e) {
                    log.error("", e);
                    session.getControlSocket().write(DATA_CONNECTION_ERROR.reply());
                    socket.close();
                }
            });
        }
    }
}
