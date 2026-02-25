package com.macrosan.utils.msutils;

import com.macrosan.utils.functional.Tuple2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

/**
 * 调用ssh命令的客户端
 *
 * @author admin
 */
public class SshClientUtils {

    private static final Logger logger = LogManager.getLogger(SshClientUtils.class.getName());

    public static String exec(String cmd) {
        return exec(cmd, false).var1;
    }

    public static Tuple2<String, String> exec(String cmd, boolean checkError) {
        return exec(cmd, checkError, true);
    }

    public static Tuple2<String, String> exec(String cmd, boolean checkError, boolean inLine) {
        Process pro = null;
        InputStream in = null;
        BufferedReader read = null;
        StringBuilder stringBuilder = new StringBuilder();

        InputStream err = null;
        BufferedReader readErr = null;
        StringBuilder stringBuilderErr = new StringBuilder();
        try {
            pro = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", cmd});
            pro.waitFor();

            in = pro.getInputStream();
            read = new BufferedReader(new InputStreamReader(in));
            String result;
            boolean first = true;
            while ((result = read.readLine()) != null) {
                if (!inLine && !first) {
                    stringBuilder.append("\n");
                }
                if (first) {
                    first = false;
                }
                stringBuilder.append(result);
            }

            if (checkError) {
                err = pro.getErrorStream();
                readErr = new BufferedReader(new InputStreamReader(err));
                String resultErr;
                boolean firstErr = true;
                while ((resultErr = readErr.readLine()) != null) {
                    if (!inLine && !firstErr) {
                        stringBuilder.append("\n");
                    }
                    if (first) {
                        first = false;
                    }
                    stringBuilderErr.append(resultErr);
                }
            }
        } catch (Exception e) {
            logger.error("execute the script error", e);
        } finally {
            closeStream(read, in, readErr, err);
            if (null != pro) {
                pro.destroy();
            }
        }

        return new Tuple2<>(stringBuilder.toString(), stringBuilderErr.toString());
    }

    private static void closeStream(Closeable... closeables) {
        for (Closeable closeable : closeables) {
            if (null != closeable) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    logger.error("close io error:" + e);
                }
            }
        }
    }

}
