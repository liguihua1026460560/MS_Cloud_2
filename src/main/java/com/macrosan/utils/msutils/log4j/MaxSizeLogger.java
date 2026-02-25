package com.macrosan.utils.msutils.log4j;

import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 根据压缩后的实际大小确定保留日志
 * <p>
 * 日志达到MAX_FILE_SIZE后，归档为 delObj.log.XX.gz
 * 第一次归档的日志，有多个小于MIN_GZ_FILE_SIZE的，重新合并成一个压缩包 delObj.log.merge.XX.gz(避免日志文件太多)
 */
@Log4j2
public class MaxSizeLogger {
    public static final String LOG_FILE = "delObj.log";
    public static final String GZ_LOG_FILE_PREFIX = "delObj.log.";
    public static final String LOG_DIR = "/var/log/moss/";
    //最新的日志写入路径
    private final static String path = LOG_DIR + File.separator + LOG_FILE;

    private static final long MAX_USED_SIZE = 380L << 20;
    private static final long MAX_FILE_SIZE = 20L << 20;
    private static final long MIN_GZ_FILE_SIZE = 20L << 20;

    private static FileOutputStream writer = null;
    private static long size = 0;

    private static final AtomicBoolean mergeRunning = new AtomicBoolean(false);

    private synchronized static void tryCheck() {
        if (mergeRunning.compareAndSet(false, true)) {
            ErasureServer.DISK_SCHEDULER.schedule(MaxSizeLogger::check);
        }
    }

    private static void check() {
        boolean merged = false;

        try {
            File dir = new File(LOG_DIR);

            long total;
            File[] files;

            //删除过多的旧日志
            do {
                files = dir.listFiles(f -> f.getName().startsWith(GZ_LOG_FILE_PREFIX));
                if (files != null) {
                    total = Arrays.stream(files).mapToLong(File::length).sum();
                    if (total > MAX_USED_SIZE) {
                        File file = Arrays.stream(files).sorted(Comparator.comparing(File::getName)).findFirst().get();
                        file.delete();
                    }

                } else {
                    total = 0;
                }
            } while (total >= MAX_USED_SIZE);

            if (files != null) {
                List<File> srcList = new LinkedList<>();

                Optional<File> mergedFile = Arrays.stream(files).filter(f -> f.getName().contains("merge"))
                        .sorted((f1, f2) -> f2.getName().compareTo(f1.getName())).findFirst();

                if (mergedFile.isPresent() && mergedFile.get().length() < MIN_GZ_FILE_SIZE) {
                    srcList.add(mergedFile.get());
                }

                files = Arrays.stream(files).filter(f -> !f.getName().contains("merge"))
                        .sorted(Comparator.comparing(File::getName))
                        .toArray(File[]::new);

                if (files.length > 1) {
                    for (int i = 0; i < files.length - 1; i++) {
                        srcList.add(files[i]);
                    }

                    try {
                        List<GZIPInputStream> in = new LinkedList<>();

                        try {
                            for (File f : srcList) {
                                in.add(new GZIPInputStream(new FileInputStream(f)));
                            }

                            String dst = LOG_DIR + File.separator + LOG_FILE + ".merge." + System.currentTimeMillis() + ".gz";
                            try (GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(dst))) {
                                final byte[] inbuf = new byte[128 << 10];
                                int n;

                                for (GZIPInputStream inputStream : in) {
                                    while ((n = inputStream.read(inbuf)) != -1) {
                                        out.write(inbuf, 0, n);
                                    }
                                }
                            }

                            for (File f : srcList) {
                                f.delete();
                            }

                            merged = true;
                        } finally {
                            for (GZIPInputStream stream : in) {
                                stream.close();
                            }
                        }
                    } catch (Exception e) {
                        log.error("merge log fail.", e);
                    }
                }
            }
        } finally {
            if (merged) {
                check();
            } else {
                mergeRunning.set(false);
            }
        }

    }

    /**
     * 第一次归档
     */
    private static void completeLog() {
        String src = path;

        try {
            writer.getChannel().force(true);
            writer.close();
            size = 0L;
            writer = null;

            String dst = LOG_DIR + File.separator + LOG_FILE + "." + System.currentTimeMillis() + ".gz";


            try (FileInputStream in = new FileInputStream(src);
                 GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(dst))) {

                final byte[] inbuf = new byte[128 << 10];
                int n;

                while ((n = in.read(inbuf)) != -1) {
                    out.write(inbuf, 0, n);
                }
            }
            new File(src).delete();

            tryCheck();
        } catch (Exception e) {
            log.error("complete log {} fail.", src, e);
        }
    }

    private static void newWriter() {
        if (writer != null) {
            completeLog();
        }

        try {
            File file = new File(path);
            if (file.exists()) {
                writer = new FileOutputStream(file, true);
                size = writer.getChannel().size();
            } else {
                writer = new FileOutputStream(file);
                size = 0L;
            }
        } catch (Exception e) {
            log.error("", e);
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "fail delObj log writer");
        }

    }

    public synchronized static void write(byte[] bytes, int offset, int len) {
        try {
            if (writer == null) {
                newWriter();
            }

            writer.write(bytes, offset, len);
            size += len;
            writer.getChannel().force(true);

            if (size > MAX_FILE_SIZE) {
                newWriter();
            }
        } catch (Exception e) {
            log.error("write {} to delObj log fail.", new String(bytes, offset, len), e);
        }
    }
}
