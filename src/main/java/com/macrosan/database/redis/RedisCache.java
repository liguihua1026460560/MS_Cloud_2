package com.macrosan.database.redis;

import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;

@Log4j2
public class RedisCache {
    static class CachedValue {
        Object value;
        boolean mark = false;

        CachedValue(Object value) {
            this.value = value;
        }
    }

    static class Key {
        Method method;
        Object[] args;

        public Key(Method method, Object[] args) {
            this.method = method;
            this.args = args;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Key) {
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
                if (args[0] instanceof Object[]) {
                    Object[] o = (Object[]) args[0];
                    return method.hashCode() ^ o[0].hashCode();
                } else {
                    return method.hashCode() ^ args[0].hashCode();
                }
            } else {
                return method.hashCode();
            }
        }
    }

    /**
     * 指定database下用于响应式查询的连接
     */
    RedisReactiveCommands<String, String> commands;

    ConcurrentHashMap<Key, CachedValue> cache = new ConcurrentHashMap<>();

    long markTime = 0L;

    @SuppressWarnings("unchecked")
    private void updateCache() {
        long[] nums = new long[]{0L, System.currentTimeMillis()};
        UnicastProcessor<Long> processor = UnicastProcessor.create(Queues.<Long>unboundedMultiproducer().get());
        //更新redis是否结束
        boolean[] end = new boolean[]{false};

        int mark = 0;
        if (markTime == 0L) {
            mark = 1;
        } else if (System.nanoTime() - markTime >= 300_000_000_000L) {
            mark = 2;
        }

        //更新缓存的redis
        for (Map.Entry<Key, CachedValue> entry : cache.entrySet()) {
            Key key = entry.getKey();
            if (mark == 1) {
                entry.getValue().mark = true;
            } else if (mark == 2 && entry.getValue().mark) {
                //remove
                cache.remove(entry.getKey());
                continue;
            }

            try {
                if (key.method.getReturnType() == Mono.class) {
                    processor.onNext(-1L);
                    Mono mono = (Mono) key.method.invoke(commands, key.args);
                    boolean[] empty = new boolean[]{true};
                    mono.doFinally(s -> processor.onNext(1L))
                            .subscribe(o -> {
                                empty[0] = false;
                                entry.getValue().value = o;
                            }, e -> log.error("", e), () -> {
                                if (empty[0]) {
                                    entry.getValue().value = null;
                                }
                            });
                }

                if (key.method.getReturnType() == Flux.class) {
                    processor.onNext(-1L);
                    Flux flux = (Flux) key.method.invoke(commands, key.args);
                    List<Object> list = new LinkedList<>();
                    //processor传递1，表示遍历完了一个key.
                    flux.doFinally(s -> processor.onNext(1L))
                            .subscribe(list::add, e -> log.error("", e), () -> {
                                entry.getValue().value = list.toArray();
                            });

                }
            } catch (Exception e) {
                log.error("", e);
            }
        }

        if (mark == 1) {
            markTime = System.nanoTime();
        } else if (mark == 2) {
            markTime = 0;
        }

        //更新完毕，发布0
        processor.onNext(0L);

        processor.subscribe(l -> {
            //processor传递0，表示cache循环更新完毕
            if (l == 0) {
                end[0] = true;
            } else {
                //一个key更新完毕，结果为-1 + 1 = 0。
                nums[0] += l;
            }

            //processor传递0，且cache中全部key都更新完毕
            if (end[0] && nums[0] == 0L) {
                long delayMil = 5000 - (System.currentTimeMillis() - nums[1]);
                delayMil = delayMil > 0 ? delayMil : 0;

                Mono.delay(Duration.ofMillis(delayMil)).publishOn(DISK_SCHEDULER).subscribe(t -> updateCache());
            }
        });
    }

    int database;

    public RedisCache(int database, StatefulRedisPubSubConnection<String, String> subConnection, RedisReactiveCommands<String, String> commands) {
        this.commands = commands;
        this.database = database;

        subConnection.addListener(new RedisPubSubAdapter<String, String>() {
            @Override
            public void message(String channel, String message) {
                for (Map.Entry<Key, CachedValue> entry : cache.entrySet()) {
                    Key key = entry.getKey();
                    if (key.args.length > 0 && key.args[0].equals(message)) {
                        try {
                            if (key.method.getReturnType() == Mono.class) {
                                Mono mono = (Mono) key.method.invoke(commands, key.args);
                                mono.subscribe(o -> {
                                    entry.getValue().mark = false;
                                    entry.getValue().value = o;
                                }, e -> log.error("", e));
                            }

                            if (key.method.getReturnType() == Flux.class) {
                                Flux flux = (Flux) key.method.invoke(commands, key.args);
                                List<Object> list = new LinkedList<>();
                                flux.subscribe(o -> {
                                    list.add(o);
                                }, e -> log.error("", e), () -> {
                                    entry.getValue().mark = false;
                                    entry.getValue().value = list.toArray();
                                });

                            }
                        } catch (Exception e) {
                            log.error("", e);
                        }
                    }
                }
            }
        });
        subConnection.sync().subscribe("redis-cache-" + database);

        //延迟开启cache的缓存刷新
        Mono.delay(Duration.ofSeconds(10)).subscribe(t -> updateCache());

    }

    /**
     * 查询key的值，如果缓存中已有则返回缓存中的值，没有则去redis查询并加入缓存。
     *
     * @param method 动态代理
     * @param args   动态代理
     * @return 查询结果
     */
    public Mono invokeMono(Method method, Object[] args) {
        Key key = new Key(method, args);
        CachedValue res = cache.get(key);
        if (null != res) {
            if (res.value == null) {
                return Mono.empty();
            } else {
                return Mono.just(res.value);
            }
        }

        MonoProcessor processor = MonoProcessor.create();

        try {
            AtomicBoolean empty = new AtomicBoolean(true);
            Mono mono = (Mono) method.invoke(commands, args);
            mono.subscribe(o -> {
                processor.onNext(o);
                empty.set(false);
                if (cache.size() < 10_0000) {
                    cache.put(key, new CachedValue(o));
                }
            }, e -> {
                log.info("{} - {}", method.getName(), args);
                log.error("", e);
            }, () -> {
                if (empty.get() && cache.size() < 10_0000) {
                    cache.put(key, new CachedValue(null));
                }
                processor.onComplete();
            });
        } catch (Exception e) {
            log.error("", e);
        }

        return processor;
    }

    public Flux invokeFlux(Method method, Object[] args) {
        Key key = new Key(method, args);
        CachedValue res = cache.get(key);
        if (null != res) {
            res.mark = false;
            return Flux.create(sink -> {
                Object[] array = (Object[]) res.value;
                for (Object o : array) {
                    sink.next(o);
                }

                sink.complete();
            });
        }

        UnicastProcessor processor = UnicastProcessor.create();

        try {
            Flux flux = (Flux) method.invoke(commands, args);
            List<Object> list = new LinkedList<>();
            flux.subscribe(o -> {
                processor.onNext(o);
                list.add(o);
            }, e -> log.error("", e), () -> {
                processor.onComplete();
                if (cache.size() < 10_0000) {
                    cache.put(key, new CachedValue(list.toArray()));
                }
            });
        } catch (Exception e) {
            log.error("", e);
        }

        return processor;
    }
}
