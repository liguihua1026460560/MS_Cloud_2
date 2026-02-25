package com.macrosan.filesystem.ftp;


import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.filesystem.lock.redlock.RedLockClient;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.reactivex.core.net.NetSocket;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.MonoProcessor;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.ServerConstants.SLASH;
import static com.macrosan.filesystem.ftp.FTPRequest.Command.STOR;

@Data
@Log4j2
public class Session {
    public NetSocket controlSocket;

    public NetSocket dataSocket;

    public FTPRequest curRequest;

    public Context context;

    public String user;

    public String userId;

    //TODO 记录workDir的nodeId，避免重复查询 目录nodeID
    public String workDir = File.separator;

    public String host;

    public String clientIP;

    public MonoProcessor<FTPRequest> dataRequestCache = null;
    // 断点续下载或上传偏移量
    public long restValue = -1;

    public Inode cachedInode = null;
    // 标记使用的客户端是否是rclone，rclone不使用cwd命令
    public boolean isRcloneClient = true;
    public ReqInfo reqHeader;
    public int dataTransferport;
    public boolean writePermission = true;

    public String sourcePath = "";

    @Data
    @AllArgsConstructor
    public static class RenameFileInfo {
        String objName;
        long nodeId;
    }

    public RenameFileInfo renameFileInfo;

    public Map<String, String> lock = new ConcurrentHashMap<>();


    public final AtomicBoolean closed = new AtomicBoolean(false);
    private Queue<Handler<Void>> closeHandler;

    public void addResponseCloseHandler(Handler<Void> handler) {
        synchronized (this) {
            if (closed.get()) {
                handler.handle(null);
                return;
            }

            if (closeHandler == null) {
                closeHandler = new LinkedList<>();
            }

            closeHandler.add(handler);
        }
    }

    public void close() {
        synchronized (this) {
            if (closed.compareAndSet(false, true)) {
                if (closeHandler != null) {
                    for (Handler<Void> handler : closeHandler) {
                        try {
                            handler.handle(null);
                        } catch (Exception e) {
                            log.error("", e);
                        }
                    }
                }
                if (dataSocket != null) {
                    dataSocket.close();
                }
                controlSocket.close();
            }
        }
    }

    public String getFullPath(String fileName) {
        // 统一斜杠 + 去掉 Windows 客户端乱发的 \r（极少数情况会出现）
        String rawPath = fileName.replace("\\", SLASH).trim();
        if (rawPath.endsWith("\r")) {
            rawPath = rawPath.substring(0, rawPath.length() - 1);
        }

        boolean isAbsolute = rawPath.startsWith(SLASH);

        String fullPath = "";
        if (isAbsolute) {
            fullPath = rawPath;
        } else {
            fullPath = workDir + rawPath;
        }

        fullPath = encodePath(fullPath);
        URI uri = URI.create(fullPath).normalize();
        return uri.getHost() == null ? uri.getPath() : '/' + uri.getHost() + uri.getPath();
    }



    public Tuple2<String, String> getBucketAndObject(String fileName) {
        String fullPath = getFullPath(fileName);
        String[] array = fullPath.split(SLASH, 3);
        String bucket = array[1];
        String objName = "";
        if (array.length > 2) {
            objName = array[2];
        }

        return new Tuple2<>(bucket, objName);
    }

    public static Tuple2<String, String> getBucketAndObjectByFullPath(String fullPath) {
        String[] array = fullPath.split(SLASH, 3);
        String bucket = array[1];
        String objName = "";
        if (array.length > 2) {
            objName = array[2];
        }

        return new Tuple2<>(bucket, objName);
    }

    public static String encodePath(String path) {
        try {
            StringBuilder encodedPath = new StringBuilder();
            String[] segments = path.split("/"); // 按 "/" 分割路径

            for (int i = 0; i < segments.length; i++) {
                if (!segments[i].isEmpty()) {
                    // 对每个路径片段进行 URL 编码
                    String encodedSegment = URLEncoder.encode(segments[i], "UTF-8");
                    // 处理空格
                    encodedSegment = encodedSegment.replace("+", "%20");
                    encodedPath.append(encodedSegment);
                }
                if (i < segments.length - 1 || path.endsWith("/")) {
                    encodedPath.append("/");
                }
            }
            return encodedPath.toString();
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    // 释放文件锁
    // TODO 未处理节点异常宕机释放锁失败的情况
    public void releaseLock() {
        if (reqHeader != null && !reqHeader.lock.isEmpty()) {
            for (String key : reqHeader.lock.keySet()) {
                String value = reqHeader.lock.get(key);
                RedLockClient.unlock(reqHeader.bucket, key, value, true).subscribe();
            }
        }
    }

    public int calculateRestDataPortNum() {
        int restPortNum = 0;
        for (int i = 0; i < FTPPort.IP_MAP.length; i++) {
            if (FTPPort.IP_MAP[i].get(clientIP) == null) {
                restPortNum++;
            }
        }
        return restPortNum;
    }
}
