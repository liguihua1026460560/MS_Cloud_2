package com.macrosan.utils.asm;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import lombok.extern.log4j.Log4j2;
import net.openhft.affinity.Affinity;
import org.springframework.cglib.core.ReflectUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
public class BindEpoll {
    static AtomicInteger num = new AtomicInteger();
    static List<BitSet> numaNodes = new ArrayList<>();

    static {
        File dir = new File("/sys/devices/system/node/");
        if (dir.exists() && dir.isDirectory()) {
            File[] nodes = dir.listFiles(f -> f.getName().startsWith("node") && f.isDirectory());
            if (nodes != null) {
                for (File nodeDir : nodes) {
                    String cpuListPath = nodeDir.getAbsolutePath() + "/cpulist";
                    try (BufferedReader reader = new BufferedReader(new FileReader(cpuListPath))) {
                        String node = reader.readLine();
                        BitSet set = new BitSet();

                        for (String cpus : node.split(",")) {
                            cpus = cpus.trim();
                            int cpuStart = Integer.parseInt(cpus.split("-")[0]);
                            int cpuEnd = Integer.parseInt(cpus.split("-")[1]);
                            for (int i = cpuStart; i <= cpuEnd; i++) {
                                set.set(i);
                            }
                        }
                        numaNodes.add(set);

                    } catch (Exception e) {

                    }
                }
            }
        }

        log.info("numa nodes:{}", numaNodes);
    }

    public static class Runnable implements java.lang.Runnable {
        public void run() {
            if (numaNodes.size() > 0) {
                int n = Math.abs(num.getAndIncrement() % numaNodes.size());
                Affinity.setAffinity(numaNodes.get(n));
            }
        }
    }

    public static String getCpu() {
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/cpuinfo"))) {
            String line = reader.readLine();

            while (line != null) {
                if (line.contains("model name")) {
                    int split = line.indexOf(":");
                    String model = line.substring(split + 1).trim();
                    return model;
                }

                line = reader.readLine();
            }

            return "";
        } catch (Exception e) {
            log.error("", e);
            return "";
        }
    }

    public static boolean ENABLE = false;

    static {
        String cpu = getCpu();
        if (cpu.contains("FT-2000") || cpu.contains("Phytium S5000C")) {
            ENABLE = true;
            log.info("enable bind epoll");
        }
    }

    public static void init() {
        try {
            if (ENABLE) {
                // 创建类池并获取目标类
                ClassPool pool = ClassPool.getDefault();
                CtClass cc = pool.get("io.netty.channel.epoll.EpollEventLoop");

                CtConstructor method = cc.getConstructors()[0];

                method.insertAfter("{" +
                        "$0.execute(new com.macrosan.utils.asm.BindEpoll$Runnable());" +
                        "}");

                // 将修改后的类写回文件系统
                ClassLoader loader = BindEpoll.class.getClassLoader();
                ReflectUtils.defineClass("io.netty.channel.epoll.EpollEventLoop", cc.toBytecode(), loader);
                log.info("reload EpollEventLoop.class");
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
