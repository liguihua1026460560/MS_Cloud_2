package com.macrosan.ec;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache;
import com.macrosan.doubleActive.arbitration.DAVersionUtils;
import com.macrosan.filesystem.cache.Node;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.NodeCache;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.EscapeException;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_TIMER;
import static com.macrosan.doubleActive.arbitration.DAVersionUtils.refreshMasterInfo;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.GET_VERSION_NUM;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.SUCCESS;
import static com.macrosan.httpserver.RestfulVerticle.requestVersionNumMap;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class VersionUtil {
    /**
     * MOSS系统整体的版本号
     * 用redis的incr命令获得递增的长整形数字并补齐19位
     */
//    private static volatile String VERSION_TIME;
//    private static volatile String OLD_VERSION_TIME;
//    private static AtomicLong a = new AtomicLong(0);

    private static volatile Tuple3<String, String, AtomicLong> VERSION;

    private final static char[] DigitTens = {
            '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
            '1', '1', '1', '1', '1', '1', '1', '1', '1', '1',
            '2', '2', '2', '2', '2', '2', '2', '2', '2', '2',
            '3', '3', '3', '3', '3', '3', '3', '3', '3', '3',
            '4', '4', '4', '4', '4', '4', '4', '4', '4', '4',
            '5', '5', '5', '5', '5', '5', '5', '5', '5', '5',
            '6', '6', '6', '6', '6', '6', '6', '6', '6', '6',
            '7', '7', '7', '7', '7', '7', '7', '7', '7', '7',
            '8', '8', '8', '8', '8', '8', '8', '8', '8', '8',
            '9', '9', '9', '9', '9', '9', '9', '9', '9', '9',
    };

    private final static char[] DigitOnes = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    };

    private final static char[] digits = {
            '0', '1', '2', '3', '4', '5',
            '6', '7', '8', '9', 'a', 'b',
            'c', 'd', 'e', 'f', 'g', 'h',
            'i', 'j', 'k', 'l', 'm', 'n',
            'o', 'p', 'q', 'r', 's', 't',
            'u', 'v', 'w', 'x', 'y', 'z'
    };

    /**
     * 同Long.getChars
     */
    public static void getChars(long i, int index, char[] buf) {
        long q;
        int r;
        int charPos = index;
        char sign = 0;

        if (i < 0) {
            sign = '-';
            i = -i;
        }

        // Get 2 digits/iteration using longs until quotient fits into an int
        while (i > Integer.MAX_VALUE) {
            q = i / 100;
            // really: r = i - (q * 100);
            r = (int) (i - ((q << 6) + (q << 5) + (q << 2)));
            i = q;
            buf[--charPos] = DigitOnes[r];
            buf[--charPos] = DigitTens[r];
        }

        // Get 2 digits/iteration using ints
        int q2;
        int i2 = (int) i;
        while (i2 >= 65536) {
            q2 = i2 / 100;
            // really: r = i2 - (q * 100);
            r = i2 - ((q2 << 6) + (q2 << 5) + (q2 << 2));
            i2 = q2;
            buf[--charPos] = DigitOnes[r];
            buf[--charPos] = DigitTens[r];
        }

        // Fall thru to fast mode for smaller numbers
        // assert(i2 <= 65536, i2);
        for (; ; ) {
            q2 = (i2 * 52429) >>> (16 + 3);
            r = i2 - ((q2 << 3) + (q2 << 1));  // r = i2-(q2*10) ...
            buf[--charPos] = digits[r];
            i2 = q2;
            if (i2 == 0) {
                break;
            }
        }
        if (sign != 0) {
            buf[--charPos] = sign;
        }
    }

    public static boolean isMultiCluster = false;

    public static void init0() {
        RedisConnPool pool = RedisConnPool.getInstance();
        if (pool.getCommand(REDIS_SYSINFO_INDEX).exists(MASTER_CLUSTER) != 0) {
            BucketSyncSwitchCache.getInstance().init();
            isMultiCluster = true;
            DAVersionUtils.initMasterInfo();
        }
    }

    private static final ScheduledThreadPoolExecutor executor =
            new ScheduledThreadPoolExecutor(1, new MsThreadFactory("update-version"));

    private static AtomicInteger noTimer = new AtomicInteger();

    private static void updateVersion(boolean timer) {
        try {
            RedisConnPool pool = RedisConnPool.getInstance();
            int nodeNum = NodeCache.getCache().size();
            int offset = nodeNum * 30;

            char[] chs = new char[19];
            Arrays.fill(chs, '0');
            long version = pool.getShortMasterCommand(REDIS_ROCK_INDEX).incrby("EC_VERSION", 1);
            getChars(version, 19, chs);
            String newVersion = new String(chs);
            if (version < offset) {
                synchronized (executor) {
                    if (VERSION == null || VERSION.var1 == null || newVersion.compareTo(VERSION.var1) > 0) {
                        VERSION = new Tuple3<>(newVersion, null, new AtomicLong(100_0000L));
                    }
                }
            } else {
                if (timer) {
                    int n = noTimer.get();
                    offset += n;
                    noTimer.addAndGet(-n);
                } else {
                    offset += noTimer.incrementAndGet();
                }

                char[] last = new char[19];
                Arrays.fill(last, '0');
                getChars(version - offset, 19, last);
                synchronized (executor) {
                    if (VERSION == null || VERSION.var1 == null || newVersion.compareTo(VERSION.var1) > 0) {
                        VERSION = new Tuple3<>(new String(chs), new String(last), new AtomicLong(100_0000L));
                    }
                }
            }
        } catch (Exception e) {
            log.error("update ec version fail", e);
        }
    }

    private static Map<Long, AtomicBoolean> map = new ConcurrentHashMap<>();

    public static void updateState(long vnode) {
        //vnode第一次获得versionNum，主动更新一次version
        AtomicBoolean state = map.computeIfAbsent(vnode, k -> {
            log.debug("vnode update version {}", vnode);
            updateVersion(false);
            // 切节点拿versionNum时刷新一次任期
            refreshMasterInfo();
            return new AtomicBoolean(true);
        });

        state.set(true);
    }

    public static long V3_0_6_FS_NODE_ID;

    public static void initVersionNum() {
        RedisConnPool pool = RedisConnPool.getInstance();
        if (pool.getCommand(REDIS_SYSINFO_INDEX).exists(MASTER_CLUSTER) != 0) {
            BucketSyncSwitchCache.getInstance().init();
            isMultiCluster = true;
            DAVersionUtils.initMasterInfo();
        }

        //如果inode的nodeId小于 V3_0_6_INODE，那么inode读就走旧流程，否则走新流程
        String V3_0_6_INODE = pool.getCommand(REDIS_SYSINFO_INDEX).get("V3_0_6_INODE");
        if (V3_0_6_INODE == null) {
            long ecVersion = pool.getShortMasterCommand(REDIS_ROCK_INDEX).incrby("EC_VERSION", 1L);

            char[] chs = new char[19];
            Arrays.fill(chs, '0');
            getChars(ecVersion, 19, chs);
            String newVersion = new String(chs);
            long nodeId = Long.parseLong(newVersion) * 10000_0000L + 1L;
            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).set("V3_0_6_INODE", String.valueOf(nodeId));
            V3_0_6_INODE = pool.getCommand(REDIS_SYSINFO_INDEX).get("V3_0_6_INODE");
        }

        V3_0_6_FS_NODE_ID = Long.parseLong(V3_0_6_INODE);

        executor.scheduleAtFixedRate(() -> VersionUtil.updateVersion(true), 0, 1, TimeUnit.SECONDS);

        //一段时间没有vnode的流量进入，清除对应vnode的标记
        executor.scheduleAtFixedRate(() -> {
            map.keySet().removeIf(l -> !map.get(l).get());
            map.values().forEach(b -> b.set(false));
        }, 0, 30, TimeUnit.SECONDS);

        // 定时检查map移除request已结束的versionNum
        checkRequestVersionNumMap();
    }

    /**
     * 此方法生成本地节点生成的versionNum。
     * 如果是多站点，默认是未开复制开关的桶。
     *
     * @return 本地生成的versionNum
     */
    public static String getVersionNum() {
        if (isMultiCluster) {
            return DAVersionUtils.getDaVersionNum(true);
        }
        return getVersionNumTrue();
    }

    /**
     * 如果桶开启了复制开关，需要取得主站点的versionNum
     */
    public static String getVersionNum(boolean isSyncSwitchOff) {
        if (isMultiCluster) {
            return DAVersionUtils.getDaVersionNum(isSyncSwitchOff);
        }
        return getVersionNumTrue();
    }

    public static String getVersionNumTrue() {
        Tuple3<String, String, AtomicLong> tmp = VERSION;

        if (null == tmp || null == tmp.var1) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "version time is not init");
        }

        return (tmp.var1 + System.currentTimeMillis()) + "-" + tmp.var3.incrementAndGet();
    }


    public static String getLastVersionNum(String oldVersion) {
        if (isMultiCluster) {
            return DAVersionUtils.getLastDaVersionNum(oldVersion, true);
        }
        return getLastVersionNumTrue(oldVersion);
    }

    public static String getLastVersionNum(String oldVersion, boolean isSyncSwitchOff) {
        if (isMultiCluster) {
            return DAVersionUtils.getLastDaVersionNum(oldVersion, isSyncSwitchOff);
        }
        return getLastVersionNumTrue(oldVersion);
    }

    public static String getLastVersionNumTrue(String oldVersion) {
        Tuple3<String, String, AtomicLong> tmp = VERSION;
        if (null == tmp || null == tmp.var2) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "version time is not init");
        }

        String res = (tmp.var2 + System.currentTimeMillis()) + "-" + tmp.var3.incrementAndGet();
        return StringUtils.isNotEmpty(oldVersion) && res.compareTo(oldVersion) < 0 ? oldVersion + "0" : res;
    }

    public static long newInode() {
        Tuple3<String, String, AtomicLong> tmp = VERSION;

        if (null == tmp || null == tmp.var1) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "version time is not init");
        }

        long nodeId = Long.parseLong(tmp.var1) * 10000_0000L + tmp.var3.incrementAndGet();
        long num = 0;
        while (!Node.getInstance().getInodeV(nodeId).isMaster() && num++ < 8192) {
            nodeId = Long.parseLong(tmp.var1) * 10000_0000L + tmp.var3.incrementAndGet();
        }

        return nodeId;
    }

    /**
     * 根据桶名和对象名hash计算获取对象相应的versionNum
     *
     * @param bucket 桶名
     * @param object 对象名
     * @return versionNum
     */

    public static Mono<String> getVersionNum(String bucket, String object) {
        return getVersionNum0(bucket, object, false, "");
    }

    public static Mono<String> getLastVersionNum(String oldVersion, String bucket, String object) {
        return getVersionNum0(bucket, object, true, oldVersion);
    }

    private static final int LOCAL_INDEX = Integer.parseInt(ServerConfig.getInstance().getHostUuid()) - 1;

    private static Mono<String> getVersionNum0(String bucket, String object, boolean isLast, String oldVersion) {
        //最多1024个节点
        int vnode = Math.abs((bucket + object).hashCode() % 1024);
        int index = Math.abs(vnode % NodeCache.getCache().size());

        if (LOCAL_INDEX == index) {
            return BucketSyncSwitchCache.isSyncSwitchOffMono(bucket)
                    .map(isSyncSwitchOff -> {
                        String versionNum = isLast ? VersionUtil.getLastVersionNum(oldVersion, isSyncSwitchOff) : VersionUtil.getVersionNumMaybeUpdate(isSyncSwitchOff, vnode);
                        return versionNum;
                    });
        }

        MonoProcessor<String> processor = MonoProcessor.create();
        // 根据桶名对象名hash计算，前往相应的节点拿取versionNum
        List<String> ipList = NodeCache.getCache().keySet().stream().sorted().map(NodeCache::getIP).collect(Collectors.toList());

        AtomicInteger nodeAmount = new AtomicInteger(ipList.size());
        UnicastProcessor<Long> retryProcessor = UnicastProcessor.create();

        SocketReqMsg msg = new SocketReqMsg("", 0)
                .put("bucket", bucket)
                .put("isLast", String.valueOf(isLast))
                .put("vnode", String.valueOf(vnode))
                .put("oldVersion", oldVersion);

        Boolean[] isSyncSwitchOff = new Boolean[1];
        retryProcessor
                .flatMap(s -> BucketSyncSwitchCache.isSyncSwitchOffMono(bucket)
                        .doOnNext(st -> isSyncSwitchOff[0] = st)
                        .map(b -> s))
                .subscribe(l -> {
                    // 默认使用hash计算出的下标索引对应的ip
                    String ip;
                    if (l == 0) {
                        int newIndex = Math.abs(vnode % ipList.size());
                        ip = ipList.get(newIndex);
                    } else {
                        ip = ipList.get(index);
                    }
                    Disposable[] disposables = new Disposable[]{null};
                    String finalIp = ip;
                    disposables[0] = RSocketClient.getRSocket(ip, BACK_END_PORT)
                            .flatMap(rsocket -> rsocket.requestResponse(DefaultPayload.create(Json.encode(msg), GET_VERSION_NUM.name())))
                            .timeout(Duration.ofSeconds(30))
                            .doOnError(e -> {
                                if (!(e instanceof EscapeException)) {
                                    if (e instanceof TimeoutException) {
                                        log.error("get bucket {} object {} version num from ip {} encounter timout exception.{}", bucket, object, null, e);
                                    } else {
                                        log.error("get bucket {} object {} version num from ip {} encounter exception.{}", bucket, object, null, e);
                                    }
                                }
                                if (nodeAmount.decrementAndGet() > 0) {
                                    ipList.remove(finalIp);
                                    retryProcessor.onNext(0L);
                                } else {
                                    try {
                                        String versionNum = isLast ? getLastVersionNum(oldVersion, isSyncSwitchOff[0]) : getVersionNum(isSyncSwitchOff[0]);
                                        processor.onNext(versionNum);
                                    } catch (Exception ex) {
                                        processor.onError(ex);
                                    }
                                    retryProcessor.onComplete();
                                }
                            })
                            .flatMap(payload -> {
                                String metadataUtf8 = payload.getMetadataUtf8();
                                if (SUCCESS.name().equals(metadataUtf8)) {
                                    return Mono.just(payload.getDataUtf8());
                                }
                                return Mono.just("");
                            })
                            .doFinally(f -> {
                                if (disposables[0] != null) {
                                    disposables[0].dispose();
                                }
                            })
                            .subscribe(versionNum -> {
                                if (StringUtils.isNotBlank(versionNum)) {
                                    processor.onNext(versionNum);
                                } else {
                                    try {
                                        versionNum = isLast ? getLastVersionNum(oldVersion, isSyncSwitchOff[0]) : getVersionNum(isSyncSwitchOff[0]);
                                        processor.onNext(versionNum);
                                    } catch (Exception e) {
                                        processor.onError(e);
                                    }
                                    //processor.onError(new MsException(ErrorNo.UNKNOWN_ERROR, "version time is not init"));
                                }
                                retryProcessor.onComplete();
                            });
                });
        retryProcessor.onNext(1L);
        return processor;
    }

    /**
     * 解决时间回拨问题
     */
    private static long LAST_TIME = -1;

    private static long getCurrentTimeMillis(long vnode) {
        long currentTimeMillis;
        synchronized (VersionUtil.class) {
            currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis < LAST_TIME) {
                log.debug("update index version {}", currentTimeMillis);
                updateVersion(false);
            }
            LAST_TIME = currentTimeMillis;
        }
        return currentTimeMillis;
    }

    private static String getVersionNumTrueMaybeUpdate(long vnode) {
        long currentMillis = getCurrentTimeMillis(vnode);
        Tuple3<String, String, AtomicLong> tmp = VERSION;

        if (null == tmp || null == tmp.var1) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "version time is not init");
        }

        return (tmp.var1 + currentMillis + "-" + tmp.var3.incrementAndGet());
    }

    public static String getVersionNumMaybeUpdate(boolean isSyncSwitchOff, long vnode) {
        if (isMultiCluster) {
            return DAVersionUtils.getDaVersionNum(isSyncSwitchOff);
        }
        return getVersionNumTrueMaybeUpdate(vnode);
    }

    final static AtomicBoolean isScanning = new AtomicBoolean(false);

    public static void checkRequestVersionNumMap() {
        SCAN_TIMER.scheduleAtFixedRate(() -> {
            try {
                if (isScanning.compareAndSet(false, true)) {
                    for (Map.Entry<String, ConcurrentSkipListMap<String, MsHttpRequest>> stringConcurrentSkipListMapEntry : requestVersionNumMap.entrySet()) {
                        ConcurrentSkipListMap<String, MsHttpRequest> versionNumRequestMap = stringConcurrentSkipListMapEntry.getValue();
                        if (versionNumRequestMap.size() == 0) {
                            continue;
                        }

                        Iterator<Map.Entry<String, MsHttpRequest>> entryIterator = versionNumRequestMap.entrySet().iterator();
                        while (entryIterator.hasNext()) {
                            Map.Entry<String, MsHttpRequest> entry = entryIterator.next();
                            if (entry == null) {
                                continue;
                            }
                            MsHttpRequest msHttpRequest = entry.getValue();
                            if (msHttpRequest != null && (msHttpRequest.getDelegate().response().closed() || msHttpRequest.getDelegate().response().ended())) {
                                entryIterator.remove();
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.error("checkRequestVersionNumMap error, ", e);
            } finally {
                isScanning.set(false);
            }
        }, 10, 30, TimeUnit.SECONDS);
    }

    /**
     * 自 Revision 86045 起引入的 JSON 魔数前缀（显示为 "!耀³"）。
     * 用于新版本数据标识，在字节偏移量 5 开始处写入。
     */
    private static final byte[] JSON_MAGIC_PREFIX = {
            0x21, (byte) 0xE8, (byte) 0x80, (byte) 0x80, (byte) 0xC2, (byte) 0x89
    };

    public static boolean hasVersionMagicPrefix(byte[] data, int magicOffset) {
        if (data == null || data.length < magicOffset + JSON_MAGIC_PREFIX.length) {
            return false;
        }

        for (int i = 0; i < JSON_MAGIC_PREFIX.length; i++) {
            if (data[magicOffset + i] != JSON_MAGIC_PREFIX[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * 自 Revision 86045 起引入。
     * <p>
     * 通过检查 Metadata 元数据在字节偏移量 5 开始的 6 个字节是否匹配魔数 0x21 E8 80 80 C2 89 来判断版本：
     * - 新版本：在偏移量 5 处写入该魔数，后接标准 JSON。
     * 该魔数在 UTF-8 字符串中显示为 "!耀³"，在 : 与 { 符号之间。
     * - 旧版本：标准 JSON 内容
     */
    public static boolean hasVersionMagicPrefix(byte[] data) {
        return hasVersionMagicPrefix(data, 5);
    }
}
