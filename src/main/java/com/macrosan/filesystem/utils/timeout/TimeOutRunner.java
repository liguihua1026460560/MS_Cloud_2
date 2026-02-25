package com.macrosan.filesystem.utils.timeout;

import com.macrosan.constants.ServerConstants;
import com.macrosan.filesystem.utils.timeout.MsLinkedList.Node;
import com.macrosan.utils.functional.Tuple2;
import lombok.AllArgsConstructor;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class TimeOutRunner {
    static final IntObjectHashMap<TimeOutRunner> allTimeOut = new IntObjectHashMap<>();

    static {
        Schedulers.parallel().schedule(TimeOutRunner::checkTimeout, 1, TimeUnit.SECONDS);
    }

    private static void checkTimeout() {
        allTimeOut.forEachKey(timeout -> {
            TimeOutRunner runner = allTimeOut.get(timeout);
            long timeoutNanos = timeout * 1000_000_000L;

            for (int i = 0; i < runner.list.length; i++) {
                long now = System.nanoTime();
                final MsLinkedList<TimeOutTask> list = runner.list[i];
                boolean needNext;

                do {
                    synchronized (list) {
                        if (list.peekFirst() != null && now - list.peekFirst().time > timeoutNanos) {
                            Schedulers.parallel().schedule(list.pollFirst().runnable);
                            needNext = true;
                        } else {
                            needNext = false;
                        }
                    }
                } while (needNext);
            }
        });

        Schedulers.parallel().schedule(TimeOutRunner::checkTimeout, 1, TimeUnit.SECONDS);
    }

    public static TimeOutRunner getInstance(Duration duration) {
        TimeOutRunner timeOutRunner = allTimeOut.get((int) duration.getSeconds());
        if (timeOutRunner == null) {
            synchronized (allTimeOut) {
                timeOutRunner = allTimeOut.get((int) duration.getSeconds());
                if (timeOutRunner == null) {
                    timeOutRunner = new TimeOutRunner(duration);
                    allTimeOut.put((int) duration.getSeconds(), timeOutRunner);
                }
            }
        }
        return timeOutRunner;
    }

    Duration timeout;
    MsLinkedList<TimeOutTask>[] list = new MsLinkedList[ServerConstants.PROC_NUM];
    int index;

    TimeOutRunner(Duration timeout) {
        this.timeout = timeout;
        for (int i = 0; i < list.length; i++) {
            list[i] = new MsLinkedList<>();
        }
    }

    public Tuple2<Integer, Node<TimeOutTask>> addTimeOut(Runnable runnable) {
        int index0 = Math.abs(index++ % list.length);
        Node<TimeOutTask> node;
        synchronized (list[index0]) {
            node = list[index0].linkLast(new TimeOutTask(runnable, System.nanoTime()));
        }

        return new Tuple2<>(index0, node);
    }

    public void cancelTimeOut(int index0, Node<TimeOutTask> node) {
        synchronized (list[index0]) {
            list[index0].unlink(node);
        }
    }

    @AllArgsConstructor
    static class TimeOutTask {
        Runnable runnable;
        long time;
    }

}
