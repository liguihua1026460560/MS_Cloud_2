package com.macrosan.httpserver;

import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import com.macrosan.utils.msutils.SshClientUtils;
import io.reactivex.disposables.Disposable;
import io.vertx.core.Vertx;
import io.vertx.core.impl.BlockedThreadChecker;
import io.vertx.core.impl.VertxImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.*;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.ServerConstants.PROC_NUM;

/**
 * HealthChecker
 * 检查进程的健康状态
 *
 * @author liyixin
 * @date 2019/5/14
 */
public class HealthChecker {

    private static final Logger logger = LogManager.getLogger(HealthChecker.class);

    private final ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

    private static HealthChecker checker;

    private Timer timer;

    private final List<RestfulVerticle> verticles = new ArrayList<>(PROC_NUM);

//    private boolean checkDeadLock = true;

    private boolean checkBlocked = true;

    private boolean checkServerStatus = true;

    public static void init(Vertx vertx) {
        checker = new HealthChecker(vertx);
    }

    public static HealthChecker getInstance() {
        return checker;
    }

    private HealthChecker(Vertx vertx) {
        try {
            Field fieldTimer = BlockedThreadChecker.class.getDeclaredField("timer");
            fieldTimer.setAccessible(true);
            Field fieldChecker = VertxImpl.class.getDeclaredField("checker");
            fieldChecker.setAccessible(true);

            BlockedThreadChecker instance = (BlockedThreadChecker) fieldChecker.get(vertx);
            timer = (Timer) fieldTimer.get(instance);
            timer.cancel();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            logger.error("get timer fail, a new timer will be created", e);
        }

        timer = new Timer("health-checker", true);
    }

    MsExecutor loggerExec = new MsExecutor(1, 1, new MsThreadFactory("health-checker-log"));

    public void start() {
        MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("health-checker"));
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (checkServerStatus) {
                    checkServerStatus();
                }
            }
        }, 60 * 1000L, 60 * 1000L);

        executor.scheduleAtFixedRate(() -> {
            if (checkBlocked) {
                checkBlocked();
            }
        }, 60 * 1000L, 60 * 1000L, TimeUnit.MILLISECONDS);

    }


    private void checkDeadLock() {
        long[] deadlockedThreads = mxBean.findDeadlockedThreads();
        boolean dead = false;
        ThreadInfo[] dump = null;

        if (deadlockedThreads != null && deadlockedThreads.length > 0) {
            loggerExec.submit(() -> {
                logger.info("auto get deadlocked. progress will restart");
            });
            dead = true;
        } else {
            dump = mxBean.dumpAllThreads(true, true);

            Set<String> waitLock = new HashSet<>(dump.length);
            Set<String> locked = new HashSet<>(dump.length);

            for (ThreadInfo info : dump) {
                if (info.getThreadState() == Thread.State.BLOCKED) {
                    waitLock.add(info.getLockInfo().toString());
                }

                for (LockInfo lockInfo : info.getLockedSynchronizers()) {
                    locked.add(lockInfo.toString());
                }

                for (MonitorInfo monitorInfo : info.getLockedMonitors()) {
                    if (monitorInfo.getLockedStackDepth() == -1 && monitorInfo.getLockedStackFrame() == null) {
                        loggerExec.submit(() -> {
                            logger.info("get no frame monitor {} in {}", monitorInfo, info);
                        });
                    } else {
                        locked.add(monitorInfo.toString());
                    }
                }
            }

            waitLock.removeIf(locked::contains);

            if (waitLock.size() > 0) {
                loggerExec.submit(() -> {
                    logger.info("wait lock list no empty {}. progress will restart", waitLock);
                });
                dead = true;
            }
        }

        if (dead) {
            try (BufferedWriter deadLockLog = createLogFile()) {
                if (dump == null) {
                    dump = mxBean.dumpAllThreads(true, true);
                }
                for (ThreadInfo info : dump) {
                    deadLockLog.write(info.toString());
                    deadLockLog.newLine();
                }
                deadLockLog.flush();
            } catch (Exception e) {
                loggerExec.submit(() -> {
                    logger.error("write dead lock log fail");
                });
            } finally {
                //等待一段时间的异步日志打印
                try {
                    synchronized (Thread.currentThread()) {
                        Thread.currentThread().wait(1000);
                    }
                } catch (Exception e) {

                }
                SshClientUtils.exec("python /moss/ms_moss/restart_cloud.py");
            }
        }
    }

    private static final long BLOCKED_THREAD_TIMEOUT = 180_000;
    //var1:blockedCount var2:time
    final Map<Long, Tuple2<Long, Long>> cachedThreadInfo = new HashMap<>();
    private static final Tuple2<Long, Long> NO_BLOCK_THREAD_INFO = new Tuple2<>(-1L, -1L);

    private void checkBlocked() {
        long[] threadId = mxBean.getAllThreadIds();
        boolean blockedTimeOut = false;

        for (long id : threadId) {
            ThreadInfo threadInfo = mxBean.getThreadInfo(id);
            if (threadInfo.getThreadState() == Thread.State.BLOCKED) {
                Tuple2<Long, Long> last = cachedThreadInfo.getOrDefault(id, NO_BLOCK_THREAD_INFO);
                if (last.var1 == threadInfo.getBlockedCount()) {
                    long blockedTime = System.nanoTime() - last.var2;
                    if (blockedTime > BLOCKED_THREAD_TIMEOUT) {
                        blockedTimeOut = true;
                    }
                } else {
                    cachedThreadInfo.put(id, new Tuple2<>(threadInfo.getBlockedCount(), System.nanoTime()));
                }
            } else {
                cachedThreadInfo.put(id, NO_BLOCK_THREAD_INFO);
            }
        }

        if (cachedThreadInfo.size() > threadId.length) {
            //清除已经结束的thread
            Set<Long> set = new HashSet<>(threadId.length);
            for (long id : threadId) {
                set.add(id);
            }

            List<Long> needRemove = new LinkedList<>();

            for (long id : cachedThreadInfo.keySet()) {
                if (!set.contains(id)) {
                    needRemove.add(id);
                }
            }

            for (long id : needRemove) {
                cachedThreadInfo.remove(id);
            }
        }

        if (blockedTimeOut) {
            loggerExec.submit(() -> {
                logger.info("thread blocked timeout. try check deadlock");
            });
            checkDeadLock();
        }
    }

    private void checkServerStatus() {
        verticles.forEach(verticle -> {
            List<Disposable> serverStatus = verticle.getStatusList();
            serverStatus.forEach(status -> {
                if (status.isDisposed()) {
                    logger.error("the server was disposed, progress will restart");
                    SshClientUtils.exec("python /moss/ms_moss/restart_cloud.py");
                }
            });
        });
    }

    synchronized void register(RestfulVerticle verticle) {
        verticles.add(verticle);
    }

    private BufferedWriter createLogFile() throws IOException {
        File deadLock = new File("/var/log/moss/deadlock.log");
        return new BufferedWriter(new FileWriter(deadLock));
    }
}
