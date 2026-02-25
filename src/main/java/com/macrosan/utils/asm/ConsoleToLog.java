package com.macrosan.utils.asm;

import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * System.out和System.err的打印
 * 转换成 log.info 和 log.error进行输出
 */
@Log4j2
public class ConsoleToLog {
    private static final AtomicBoolean init = new AtomicBoolean(false);

    public static void init() {
        if (init.compareAndSet(false, true)) {
            System.setOut(new PrintStream(new LogOutputStream((b, l) -> {
                log.info("stdout:{}", new String(b, 0, l));
            }), true));

            System.setErr(new PrintStream(new LogOutputStream((b, l) -> {
                log.error("stderr:{}", new String(b, 0, l));
            }), true));
        }
    }

    private interface Log {
        void log(byte[] buf, int len);
    }

    private static class LogOutputStream extends OutputStream {
        byte[] buf = new byte[64 << 10];
        int index = 0;
        Log log;

        LogOutputStream(Log log) {
            this.log = log;
        }

        public void write(int b) throws IOException {
            buf[index++] = (byte) b;
            if (index >= buf.length) {
                flush();
            }
        }


        public void write(byte b[]) throws IOException {
            write(b, 0, b.length);
        }

        public void write(byte b[], int off, int len) throws IOException {
            if (b == null) {
                throw new NullPointerException();
            } else if ((off < 0) || (off > b.length) || (len < 0) ||
                    ((off + len) > b.length) || ((off + len) < 0)) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return;
            }

            int last = len;
            int off0 = off;
            while (last > 0) {
                int copy = Math.min(buf.length - index, last);
                System.arraycopy(b, off0, buf, index, copy);
                index += copy;
                off0 += copy;
                last -= copy;
                if (index >= buf.length) {
                    flush();
                }
            }
        }

        public void flush() throws IOException {
            if (index > 0) {
                log.log(buf, index);
                index = 0;
            }
        }


        public void close() throws IOException {
        }
    }
}
