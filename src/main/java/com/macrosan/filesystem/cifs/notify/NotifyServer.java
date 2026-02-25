package com.macrosan.filesystem.cifs.notify;

import com.macrosan.filesystem.cifs.reply.smb2.NotifyReply.NotifyAction;
import com.macrosan.filesystem.lock.Lock;
import com.macrosan.filesystem.lock.LockClient;
import com.macrosan.filesystem.lock.LockServer;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;


@Log4j2
public class NotifyServer extends LockServer<NotifyLock> {
    private final MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("cifs-notify-task"));
    private final ConcurrentLinkedQueue<NotifyTask> taskQueue = new ConcurrentLinkedQueue<>();

    private void runTask() {
        NotifyTask task;
        try {
            while ((task = taskQueue.poll()) != null) {
                String pathKey = CifsUtils.getPathName(task.notifyKey[0]);
                String k = task.bucket + '/' + pathKey;
                Set<NotifyLock> notifyLocks;
                Map<String, List<NotifyLock>> notified = new HashMap<>();
                if ((notifyLocks = map.get(k)) != null) {
                    for (NotifyLock lock : notifyLocks) {
                        if ((lock.filter & task.filter) != 0) {
                            notified.computeIfAbsent(pathKey, k0 -> new LinkedList<>()).add(lock);
                        }
                    }
                }

                do {
                    if ((notifyLocks = treeMap.get(k)) != null) {
                        for (NotifyLock lock : notifyLocks) {
                            notified.computeIfAbsent(pathKey, k0 -> new LinkedList<>()).add(lock);
                        }
                    }

                    pathKey = CifsUtils.getPathName(pathKey);
                    k = task.bucket + '/' + pathKey;
                } while (!StringUtils.isBlank(pathKey));

                for (String lockKey : notified.keySet()) {
                    for (NotifyLock lock : notified.get(lockKey)) {
                        lock.setTask(task);
                        LockClient.unlock(task.bucket, lockKey, lock)
                                .subscribe();
                    }
                }

            }
        } finally {
            executor.schedule(this::runTask, 1, TimeUnit.MILLISECONDS);
        }
    }


    public static void register() {
        NotifyServer server = instance;
        Lock.register(server.type, server, server.tClass);
    }

    private static final NotifyServer instance = new NotifyServer(Lock.NOTIFY_TYPE, NotifyLock.class);

    public static NotifyServer getInstance() {
        return instance;
    }

    private NotifyServer(int type, Class<NotifyLock> tClass) {
        super(type, tClass);
        executor.submit(this::runTask);
    }

    Map<String, Set<NotifyLock>> map = new ConcurrentHashMap<>();
    Map<String, Set<NotifyLock>> treeMap = new ConcurrentHashMap<>();

    @Override
    public Mono<Boolean> tryLock(String bucket, String key, NotifyLock value) {
        String k = bucket + '/' + key;

        if (value.tree) {
            treeMap.compute(k, (t, v) -> {
                if (v == null) {
                    v = new HashSet<>();
                }
                v.add(value);
                return v;
            });
        } else {
            map.compute(k, (t, v) -> {
                if (v == null) {
                    v = new HashSet<>();
                }
                v.add(value);
                return v;
            });
        }

        return Mono.just(true);
    }

    @Override
    public Mono<Boolean> tryUnLock(String bucket, String key, NotifyLock value) {
        String k = bucket + '/' + key;

        if (value.tree) {
            treeMap.compute(k, (t, v) -> {
                if (v == null) {
                    return null;
                } else {
                    v.remove(value);
                    return v.isEmpty() ? null : v;
                }
            });
        } else {
            map.compute(k, (t, v) -> {
                if (v == null) {
                    return null;
                } else {
                    v.remove(value);
                    return v.isEmpty() ? null : v;
                }
            });
        }

        value.cancel();

        return Mono.just(true);
    }

    @Override
    public Mono<Boolean> keep(String bucket, String key, NotifyLock value) {
        return tryLock(bucket, key, value);
    }

    public void maybeNotify(String bucket, String notifyKey, int filter, NotifyAction action) {
        NotifyTask task = new NotifyTask(bucket, new String[]{notifyKey}, filter, new NotifyAction[]{action});
        taskQueue.add(task);
    }

    public void maybeNotify(String bucket, String[] notifyKey, int filter, NotifyAction[] action) {
        NotifyTask task = new NotifyTask(bucket, notifyKey, filter, action);
        taskQueue.add(task);
    }

}
