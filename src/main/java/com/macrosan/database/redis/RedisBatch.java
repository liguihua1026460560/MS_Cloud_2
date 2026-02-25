package com.macrosan.database.redis;

import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lombok.extern.log4j.Log4j2;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.*;

/**
 * 缓存所有redis查询操作，重复的查询操作在一次执行后一起返回结果
 *
 * @author gaozhiyuan
 */
@Log4j2
public class RedisBatch {
    RedisReactiveCommands<String, String> commands;
    Map<Key, List<Request>>[] caches;
    int[] counter;

    public RedisBatch(RedisReactiveCommands<String, String> commands) {
        this.commands = commands;

        processors = new UnicastProcessor[1];
        caches = new HashMap[processors.length];
        counter = new int[processors.length];

        for (int i = 0; i < processors.length; i++) {
            int index = i;
            processors[i] = UnicastProcessor.create(Queues.<Request>unboundedMultiproducer().get());
            processors[i].subscribe(request -> handler(index, request), e -> log.error("", e));
            caches[i] = new HashMap<>();
        }

        Request flushRequest = new Request();
        flushRequest.flush = true;

        Flux.interval(Duration.ofMillis(100)).subscribe(l -> {
            for (int i = 0; i < processors.length; i++) {
                processors[i].onNext(flushRequest);
            }
        });

    }

    public void handler(int index, Request request) {
        if (request.flush) {
            if (counter[index] > 0) {
                flush(index);
            }
            return;
        }

        counter[index]++;
        Key key = new Key(request);

        if (caches[index].containsKey(key)) {
            caches[index].get(key).add(request);
        } else {
            List<Request> list = new ArrayList<>();
            list.add(request);
            caches[index].put(key, list);
        }

        if (counter[index] > 100) {
            flush(index);
        }
    }

    public void flush(int index) {
        for (Map.Entry<Key, List<Request>> entry : caches[index].entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }

            try {
                List<Request> tmpList = new ArrayList<>(entry.getValue());
                if (entry.getKey().method.getReturnType() == Mono.class) {
                    Mono res = (Mono) entry.getKey().method.invoke(commands, entry.getKey().args);
                    res.subscribe(s -> {
                        for (Request request : tmpList) {
                            request.res.onNext(s);
                        }
                    });
                }

                if (entry.getKey().method.getReturnType() == Flux.class) {
                    Flux res = (Flux) entry.getKey().method.invoke(commands, entry.getKey().args);
                    res.subscribe(s -> {
                        for (Request request : tmpList) {
                            request.res.onNext(s);
                        }
                    }, e -> {
                    }, () -> {
                        for (Request request : tmpList) {
                            request.res.onComplete();
                        }
                    });
                }
            } catch (Exception e) {
                log.error("", e);
            }

            entry.getValue().clear();
        }

        counter[index] = 0;
    }

    static class Request {
        Method method;
        Object[] args;
        CoreSubscriber res;
        boolean flush = false;
    }

    UnicastProcessor<Request>[] processors;
    ThreadLocal<Integer> threadLocal = ThreadLocal.withInitial(() -> 0);

    public Mono merge(Method method, Object[] args) {
        MonoProcessor res = MonoProcessor.create();
        int i = threadLocal.get() + 1;
        threadLocal.set(i);
        Request request = new Request();
        request.method = method;
        request.args = args;
        request.res = res;
        processors[i % processors.length].onNext(request);
        return res;
    }

    public Flux mergeFlux(Method method, Object[] args) {
        UnicastProcessor res = UnicastProcessor.create();
        int i = threadLocal.get() + 1;
        threadLocal.set(i);
        Request request = new Request();
        request.method = method;
        request.args = args;
        request.res = res;
        processors[i % processors.length].onNext(request);
        return res;
    }

    static class Key {
        Method method;
        Object[] args;

        Key(Request event) {
            this.method = event.method;
            this.args = event.args;
        }

        public Key(Method method, Object[] args) {
            this.method = method;
            this.args = args;
        }

        @Override
        public boolean equals(Object o) {
            if (null != o && o instanceof Key) {
                Key key = (Key) o;
                if (this.method.equals(key.method)
                        && this.args.length == key.args.length) {
                    for (int i = 0; i < this.args.length; i++) {
                        if (this.args[i] instanceof Object[] && key.args[i] instanceof Object[]) {
                            Object[] array1 = (Object[]) this.args[i];
                            Object[] array2 = (Object[]) key.args[i];
                            if (!Arrays.equals(array1, array2)) {
                                return false;
                            }

                        } else if (!this.args[i].equals(key.args[i])) {
                            return false;
                        }
                    }

                    return true;
                }
            }

            return false;
        }

        @Override
        public int hashCode() {
            if (args != null && args.length > 0) {
                return method.hashCode() ^ args[0].hashCode();
            } else {
                return method.hashCode();
            }
        }
    }
}
