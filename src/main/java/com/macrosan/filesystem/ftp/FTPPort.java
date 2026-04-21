package com.macrosan.filesystem.ftp;

import com.macrosan.filesystem.FsUtils;
import com.macrosan.utils.functional.Tuple2;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FTP data port 锁
 * 调用 PASV命令时 对客户端ip 和 待连接的 port 加锁
 * Session失效或建立FTP数据连接后释放
 */
@Log4j2
public class FTPPort {
    public static int FTP_DATA_MIN_PORT = 1200;
    public static int FTP_DATA_MAX_PORT = 1210;

    static {
        try {
            Tuple2<Integer, Integer> minAndMax = FsUtils.getFsPortRange();
            if (minAndMax.var1 > 0 && minAndMax.var2 > 0) {
                FTP_DATA_MIN_PORT = minAndMax.var1;
                FTP_DATA_MAX_PORT = minAndMax.var2;
            }
        } catch (Exception e) {
            log.error("Failed to obtain FTP PASV port range, default settings applied.", e);
        }
    }

    public static Map<String, Session>[] IP_MAP = new Map[FTP_DATA_MAX_PORT - FTP_DATA_MIN_PORT + 1];
    private static Integer[] dataPortArr = new Integer[FTP_DATA_MAX_PORT - FTP_DATA_MIN_PORT + 1];

    static {
        for (int i = 0; i < IP_MAP.length; i++) {
            IP_MAP[i] = new ConcurrentHashMap<>();
            dataPortArr[i] = i;
        }
    }

    public static void reloadPort() {
        for (int i = 0; i < IP_MAP.length; i++) {
            IP_MAP[i].clear();
        }

        IP_MAP = new Map[FTP_DATA_MAX_PORT - FTP_DATA_MIN_PORT + 1];
        dataPortArr = new Integer[FTP_DATA_MAX_PORT - FTP_DATA_MIN_PORT + 1];
        for (int i = 0; i < IP_MAP.length; i++) {
            IP_MAP[i] = new ConcurrentHashMap<>();
            dataPortArr[i] = i;
        }
        log.info("reload ftp pasv port from: {} to {}", FTP_DATA_MIN_PORT, FTP_DATA_MAX_PORT);
    }

    public static int acquirePort(String ip, Session session) {
        if (session.dataTransferport != 0) {
            return session.dataTransferport;
        }

        Integer[] newArr = dataPortArr.clone();
        Collections.shuffle(Arrays.asList(newArr));

        for (int i = 0; i < newArr.length; i++) {
            int ip_map_index = newArr[i];

            if (session == IP_MAP[ip_map_index].computeIfAbsent(ip, k -> session)) {
                session.dataTransferport = ip_map_index + FTP_DATA_MIN_PORT;
                if (FTP.ftpDebug) {
                    log.info("【acquirePort】 index: {}, ip: {}, port: {}, arr: {}", ip_map_index, ip, session.dataTransferport, Arrays.toString(newArr));
                }
                return ip_map_index + FTP_DATA_MIN_PORT;
            }
        }

        if (FTP.ftpDebug) {
            log.info("ftp data transfer port acquire fail");
        }
        return -1;
    }

    public static void releasePort(String ip, int port, Session session) {
        if (port == 0 || StringUtils.isBlank(ip)) {
            return;
        }
        int i = port - FTP_DATA_MIN_PORT;

        if (FTP.ftpDebug) {
            log.info("release port: ip: {}, port: {}, i: {}", ip, port, i);
        }

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
        if (FTP.ftpDebug) {
            log.info("getSession: ip: {}, port: {}, i: {}", ip, port, i);
        }
        return IP_MAP[i].get(ip);
    }

}
