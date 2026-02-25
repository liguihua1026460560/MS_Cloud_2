package com.macrosan.filesystem.ftp;

import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FTP data port 锁
 * 调用 PASV命令时 对客户端ip 和 待连接的 port 加锁
 * Session失效或建立FTP数据连接后释放
 */
@Log4j2
public class FTPPort {
    public static final int FTP_DATA_MIN_PORT = 1200;
    public static final int FTP_DATA_MAX_PORT = 1210;
    public static final Map<String, Session>[] IP_MAP = new Map[FTP_DATA_MAX_PORT - FTP_DATA_MIN_PORT + 1];

    static {
        for (int i = 0; i < IP_MAP.length; i++) {
            IP_MAP[i] = new ConcurrentHashMap<>();
        }
    }

    public static int acquirePort(String ip, Session session) {
        if (session.dataTransferport != 0) {
            return session.dataTransferport;
        }
        for (int i = 0; i < IP_MAP.length; i++) {
            if (session == IP_MAP[i].computeIfAbsent(ip, k -> session)) {
                session.dataTransferport = i + FTP_DATA_MIN_PORT;
                return i + FTP_DATA_MIN_PORT;
            }
        }
        if (FTP.ftpDebug) {
            log.info("ftp data transfer port acquire fail");
        }
        return -1;
    }

    public static void releasePort(String ip, int port, Session session) {
        if (port == 0) {
            return;
        }
        int i = port - FTP_DATA_MIN_PORT;

        IP_MAP[i].computeIfPresent(ip, (k, v) -> {
            if (v == session) {
                return null;
            } else {
                return v;
            }
        });
    }
    public static Session getSession(String ip, int port) {
        int i = port - FTP_DATA_MIN_PORT;
        return IP_MAP[i].get(ip);
    }

}
