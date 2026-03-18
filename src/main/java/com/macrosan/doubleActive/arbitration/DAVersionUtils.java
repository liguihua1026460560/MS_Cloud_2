package com.macrosan.doubleActive.arbitration;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.doubleActive.HeartBeatChecker;
import com.macrosan.ec.Utils;
import com.macrosan.ec.VersionUtil;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.http.HttpMethod;
import io.vertx.reactivex.core.http.HttpClientRequest;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_SCHEDULER;
import static com.macrosan.doubleActive.HeartBeatChecker.*;
import static com.macrosan.doubleActive.arbitration.Arbitrator.*;
import static com.macrosan.doubleActive.arbitration.ArbitratorUtils.*;
import static com.macrosan.httpserver.MossHttpClient.*;

/**
 * 双活站点开启了强一致需要去拿主站点的syncStamp，而非本地。
 * 3DC默认开启强一致，双活环境不开启，仍从本地生成新格式的syncStamp。
 */
@Log4j2
public class DAVersionUtils {
    private static DAVersionUtils instance;

    public static DAVersionUtils getInstance() {
        if (instance == null) {
            instance = new DAVersionUtils();
        }
        return instance;
    }

    private static RedisConnPool pool = RedisConnPool.getInstance();

    /**
     * 格式为字符串，为versionNum前十九位
     */
    public static volatile String daVersionPrefix = "-1";

    public static AtomicBoolean canGet = new AtomicBoolean(false);

    private static ScheduledFuture<?> masterSyncStampTimer;

    private static ScheduledFuture<?> getInfoTimer;

    private static ScheduledFuture<?> getTermTimer;

    public static AtomicBoolean isStrictConsis = new AtomicBoolean(false);

    public static AtomicBoolean isMasterCluster = new AtomicBoolean();

    private Disposable disposable;

    private static final int PERIOD_MILLIS = 1000;

    private static volatile Tuple3<String, String, AtomicLong> DA_VERSION;

    public static final ScheduledThreadPoolExecutor ARBITRATOR_TIMER = new ScheduledThreadPoolExecutor(PROC_NUM, runnable -> new Thread(runnable, "arbitrator-timer"));

    /**
     * 非处理仲裁请求的节点同步到TERM变化时，termChanged为true，表示主站点已切换，需要刷新一次本地的daVersionPrefix，下一次强制更新DaSyncStamp。
     */
    public static AtomicBoolean termChanged = new AtomicBoolean();

    private static int offset;

    public static String formatedHostUid;

    // 允许从站点的其他节点拿redis中的daVersionPrefix和上一秒的相同，但只有一次。
    private static final AtomicBoolean alreadyEquals = new AtomicBoolean();

    /**
     * 每秒拿一次主站点的syncStamp。
     */
    public void init() {
        int strLenth = 5;
        char[] chs = new char[strLenth];
        Arrays.fill(chs, '0');
        VersionUtil.getChars(Long.parseLong(ServerConfig.getInstance().getHostUuid()), strLenth, chs);
        formatedHostUid = new String(chs);

        // 复制站点不需要DASyncStamp
        if (ASYNC_INDEX_IPS_ENTIRE_MAP.containsKey(LOCAL_CLUSTER_INDEX)) {
            // 复制站点的非扫描节点，重启时在此重置evaluate状态
            isEvaluatingMaster.set(false);
            if ("master".equals(Utils.getRoleState())) {
                Mono.just(true).publishOn(SCAN_SCHEDULER)
                        .subscribe(s -> RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, IS_EVALUATING_MASTER, "0"));
            }

            initMasterInfo();

            // 复制站点的每个节点也需要同步主站点的term，用来生成VersionNum
            Optional.ofNullable(getTermTimer).ifPresent(s -> s.cancel(false));
            getTermTimer = ARBITRATOR_TIMER.scheduleWithFixedDelay(() -> {
                try {
                    synchronized (TERM) {
                        long curTerm = Long.parseLong(pool.getCommand(REDIS_ROCK_INDEX).get(ArbitratorUtils.DA_TERM));
                        if (TERM.get() < curTerm) {
                            TERM.set(curTerm);
                            // 每次从redis更新主站点和term
                            String masterIndex = pool.getCommand(REDIS_ROCK_INDEX).get(ArbitratorUtils.MASTER_CLUSTER_INDEX);
                            if (MASTER_INDEX != Integer.parseInt(masterIndex)) {
                                MASTER_INDEX = Integer.parseInt(masterIndex);
                            }
                            INDEX_TERM_MAP.put(TERM.get(), MASTER_INDEX);
                        }
                    }
                    log.debug("daversion situation: masterIndex:{}, term:{}, isEval:{}", MASTER_INDEX, TERM.get(), isEvaluatingMaster.get());
                } catch (Exception e) {
                    log.info("getTermTimer error, ", e);
                }
            }, INIT_DELAY_SECONDS, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);

            return;
        }

        // 集群中站点数量为偶数时跳过，此时为优化前的双活。需要等到仲裁者部署请求进来后才能初始化仲裁模块。
        if (IS_THREE_SYNC || ABT_INDEX_IPS_ENTIRE_MAP.size() < 3 || ABT_INDEX_IPS_ENTIRE_MAP.size() % 2 == 0) {
            // 没有开仲裁，默认是第一个term。（生成DASyncStamp用）
            String term = pool.getCommand(REDIS_ROCK_INDEX).get(ArbitratorUtils.DA_TERM);
            if (StringUtils.isNotBlank(term)) {
                TERM.set(Long.parseLong(term));
            } else {
                TERM.set(1L);
            }
            return;
        }

        isStrictConsis.set(true);
        BucketSyncSwitchCache.getInstance().init();
        VersionUtil.isMultiCluster = true;
        canGet.set(false);

        // 初始化DA_VERSION_PREFIX
        char[] temp = new char[19];
        Arrays.fill(temp, '0');
//        String daVer = pool.getCommand(REDIS_ROCK_INDEX).get(DA_VERSION_PREFIX);
//        if (StringUtils.isNotBlank(daVer)) {
//            VersionUtil.getChars(Long.parseLong(daVer), 19, temp);
//        } else {
//            if ("master".equals(Utils.getRoleState())) {
//                Mono.just(true).publishOn(SCAN_SCHEDULER)
//                        .subscribe(s -> RedisConnPool.getInstance().getShortMasterCommand(REDIS_ROCK_INDEX).set(DA_VERSION_PREFIX, "0"));
//            }
//        }
        daVersionPrefix = new String(temp);
        log.info("Start updateMasterSyncStampTimer,Term: {}, DASyncStamp: {}", TERM.get(), daVersionPrefix);

        int nodeNum = pool.getCommand(REDIS_NODEINFO_INDEX).keys("*").size();
        offset = nodeNum * 30;

        Optional.ofNullable(getInfoTimer).ifPresent(s -> s.cancel(false));
        getInfoTimer = ARBITRATOR_TIMER.scheduleWithFixedDelay(() -> {
            try {
                String abtAdmitStr = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, ARBITRATOR_ADMIT);
                boolean admit = "1".equals(abtAdmitStr);
                abtAdmit.compareAndSet(!admit, admit);

                //更新isEvaluatingMaster状态
                boolean isEvaluating = "1".equals(pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, IS_EVALUATING_MASTER));
                Arbitrator.isEvaluatingMaster.compareAndSet(!isEvaluating, isEvaluating);
                //当主站点还未确定时不执行后续，防止daVersionPrefix变动影响新旧的比较
                if (isEvaluatingMaster.get()) {
                    return;
                }

                ABT_IP = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, ARBITRATOR_IP);
                String abtPort = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, ARBITRATOR_PORT);
                if (StringUtils.isNotBlank(abtPort) && Integer.parseInt(abtPort) != ABT_PORT) {
                    ABT_PORT = Integer.parseInt(abtPort);
                }

                String abtHttpsPort = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, ARBITRATOR_HTTPS_PORT);
                if (StringUtils.isNotBlank(abtHttpsPort) && Integer.parseInt(abtHttpsPort) != ABT_HTTPS_PORT) {
                    ABT_HTTPS_PORT = Integer.parseInt(abtHttpsPort);
                }

                refreshMasterInfo();
                log.debug("daversion situation: masterIndex:{}, term:{}, isEval:{}", MASTER_INDEX, TERM.get(), isEvaluatingMaster.get());
            } catch (Exception e) {
                log.info("getInfoTimer error, ", e);
            }
        }, INIT_DELAY_SECONDS, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);

        Optional.ofNullable(masterSyncStampTimer).ifPresent(s -> s.cancel(false));
        AtomicInteger ipIndex = new AtomicInteger();
        masterSyncStampTimer = ARBITRATOR_TIMER.scheduleAtFixedRate(() -> {
            try {
                //当主站点还未确定时不执行后续，防止daVersionPrefix变动影响新旧的比较
                if (isEvaluatingMaster.get()) {
                    return;
                }

                // 主站点就不用再去远程拿syncStamp，只更新本地
                // 此处加锁原因：firstCheckMaster由扫描节点执行，与isMainNode并不一定是同一节点。
                // mainNode可能早上线，此时认为自己还是主站点，就更新了daVersionPrefix为本地的versionNum（比对站点的versinoNum大）。
                // 之后扫描节点确认当前主站点后，mainNode由定时器更新，refreshMasterInfo()方法更新daVersionPrefix为0但会被这里覆盖。
                // 最终导致TERM、MASTER_INDEX更新，但是daVersionPrefix依然为本地的versionNum，使得一直出现syncStamp gotten remote is smaller than expect
                synchronized (TERM) {
                    if (LOCAL_CLUSTER_INDEX.equals(MASTER_INDEX)) {
                        daVersionPrefix = syncStamp2DaPrefix(VersionUtil.getVersionNumTrue());
                        if ("master".equals(Utils.getRoleState())) {
                            Mono.just(true).publishOn(SCAN_SCHEDULER)
                                    .subscribe(s -> pool.getShortMasterCommand(REDIS_ROCK_INDEX).set(DA_VERSION_PREFIX, trimFirstZero(daVersionPrefix)));
                        }
                        isMasterCluster.compareAndSet(false, true);
                        canGet.compareAndSet(false, true);
                        return;
                    }
                }
                isMasterCluster.compareAndSet(true, false);
                //从站点的主节点每秒去向主站点拿一个versionNum作为DASyncStamp，更新到redis。
                // 非主节点去redis拿该syncStamp。这样非主节点保证eth4正常就可以处理请求
                if (!isMainNode()) {
                    getMainNodeDASyncStamp();
                    return;
                }

                Optional.ofNullable(disposable).ifPresent(Disposable::dispose);
                String[] ips = INDEX_IPS_MAP.get(MASTER_INDEX);

                UnicastProcessor<Integer> retryProcessor = UnicastProcessor.create();
                HttpClientRequest[] request = new HttpClientRequest[1];

                // 本次超时情况下的最大轮询次数
                int unitCount = 1;
                disposable = retryProcessor.doOnNext(tryTime -> {
                    Tuple2<String, Long> tuple2 = DoubleActiveUtil.getRequestTimeout(ips, PERIOD_MILLIS, unitCount, ipIndex);
                    String ip = tuple2.var1;
                    Long timeout = tuple2.var2;

                    request[0] = HeartBeatChecker.getHttpClient().request(HttpMethod.GET, DA_HTTP_PORT, ip, "?getMasterSyncStamp")
                            .setTimeout(timeout)
                            .setHost(ip + ":" + DA_HTTP_PORT)
                            .putHeader(SYNC_AUTH, PASSWORD)
                            .putHeader(CLUSTER_ALIVE_HEADER, ip)
                            .exceptionHandler(e -> {
                                boolean finish;
                                if (e instanceof TimeoutException) {
                                    finish = tryTime >= unitCount;
                                } else {
                                    finish = tryTime >= ips.length;
                                }
                                if (finish) {
                                    if (e instanceof TimeoutException) {
                                        TIMEOUT_RECHECK_IP_SET.add(ip);
                                    }
                                    if (alreadyEquals.get()) {
                                        log.error("redirect {} request to {} error1! {}, {}", request[0].method().name(), request[0].getHost() + request[0].uri(), e.getClass().getName(),
                                                e.getMessage());
                                        canGet.compareAndSet(true, false);
                                        retryProcessor.onComplete();
                                        return;
                                    }
                                    log.info("timeout syncStamp once.");
                                    alreadyEquals.compareAndSet(false, true);
                                    retryProcessor.onComplete();
                                } else {
                                    retryProcessor.onNext(tryTime + 1);
                                }
                                request[0].reset();
                            })
                            .handler(response -> {
                                if (response.statusCode() != SUCCESS) {
                                    log.info("dav request didn't return success. {}, {}", response.statusCode(), response.statusMessage());
                                    canGet.compareAndSet(true, false);
                                    return;
                                }

                                String newVersionPrefix = response.getHeader(DA_VERSION_PREFIX);
                                String newTerm = response.getHeader(ArbitratorUtils.DA_TERM);
                                String remoteDaVersionCore = getDaVersionCore(newVersionPrefix, Long.parseLong(newTerm));
                                String localDaversionCore = getDaVersionCore(daVersionPrefix, TERM.get());
                                log.debug("get syncstamp from master cluster, new {}, old {} ", remoteDaVersionCore, localDaversionCore);
                                if (remoteDaVersionCore.compareTo(localDaversionCore) > 0) {
                                    alreadyEquals.compareAndSet(true, false);
                                    daVersionPrefix = newVersionPrefix;

                                    // 生成lastDaVersion
                                    String daVersionPrefixNum = trimFirstZero(daVersionPrefix);
                                    long l = Long.parseLong(daVersionPrefixNum);
                                    char[] last = new char[19];
                                    Arrays.fill(last, '0');
                                    VersionUtil.getChars(l - offset, 19, last);

                                    DA_VERSION = new Tuple3<>(daVersionPrefix, new String(last), new AtomicLong(100_0000L));
                                    if (updateLastTimeFunc != null) {
                                        synchronized (DAVersionUtils.class) {
                                            if (updateLastTimeFunc != null) {
                                                updateLastTimeFunc.apply(null);
                                                updateLastTimeFunc = null;
                                            }
                                        }
                                    }
                                    canGet.compareAndSet(false, true);
                                    Mono.just(true).publishOn(SCAN_SCHEDULER)
                                            .subscribe(s -> pool.getShortMasterCommand(REDIS_ROCK_INDEX).set(DA_VERSION_PREFIX, daVersionPrefixNum));
                                } else if (remoteDaVersionCore.equals(localDaversionCore)) {
                                    // 允许相同一次。即允许一秒延迟之内的主站点syncStamp同步。
                                    if (alreadyEquals.get()) {
                                        log.error("syncStamp gotten remote is smaller than expect! new {}, old {} ", remoteDaVersionCore, localDaversionCore);
                                        canGet.compareAndSet(true, false);
                                        return;
                                    }
                                    log.info("similar syncStamp once.");
                                    alreadyEquals.compareAndSet(false, true);
                                } else {
                                    if (alreadyEquals.get()) {
                                        log.error("syncStamp gotten remote is smaller than expect! new {}, old {} ", remoteDaVersionCore, localDaversionCore);
                                        canGet.compareAndSet(true, false);
                                        long l1 = Long.parseLong(newVersionPrefix);
                                        long l2 = Long.parseLong(daVersionPrefix);
                                        if (Long.parseLong(newTerm) == TERM.get() && l2 - l1 > 1000) {
                                            log.info("recover daVersionPrefix.");
                                            alreadyEquals.compareAndSet(true, false);
                                            daVersionPrefix = newVersionPrefix;

                                            // 生成lastDaVersion
                                            String daVersionPrefixNum = trimFirstZero(daVersionPrefix);
                                            long l = Long.parseLong(daVersionPrefixNum);
                                            char[] last = new char[19];
                                            Arrays.fill(last, '0');
                                            VersionUtil.getChars(l - offset, 19, last);

                                            DA_VERSION = new Tuple3<>(daVersionPrefix, new String(last), new AtomicLong(100_0000L));
                                            if (updateLastTimeFunc != null) {
                                                synchronized (DAVersionUtils.class) {
                                                    if (updateLastTimeFunc != null) {
                                                        updateLastTimeFunc.apply(null);
                                                        updateLastTimeFunc = null;
                                                    }
                                                }
                                            }
                                            canGet.compareAndSet(false, true);
                                            Mono.just(true).publishOn(SCAN_SCHEDULER)
                                                    .subscribe(s -> pool.getShortMasterCommand(REDIS_ROCK_INDEX).set(DA_VERSION_PREFIX, daVersionPrefixNum));
                                        }
                                        return;
                                    }
                                    log.info("smaller syncStamp once.");
                                    alreadyEquals.compareAndSet(false, true);
                                }
                                retryProcessor.onComplete();
                            });
                    request[0].end();
                }).doOnError(e -> {
                    log.error("redirect error, ", e);
                    retryProcessor.cancel();
                }).subscribe();
                retryProcessor.onNext(1);
            } catch (Exception e) {
                log.error("getMasterSyncStampTimer failed", e);
            }
        }, INIT_DELAY_SECONDS * 1000, PERIOD_MILLIS, TimeUnit.MILLISECONDS);

    }

    public static String verNum2DaPrefix(String versionNum) {
        return versionNum.substring(0, 19);
    }

    public static String syncStamp2DaPrefix(String syncStamp) {
        String[] split = syncStamp.split("-");
        // 兼容旧版本
        if (split.length == 2) {
            return split[0].substring(0, 19);
        }
        return split[1].substring(0, 19);
    }

    public static String verNum2timeStamp(String versionNum) {
        String[] split = versionNum.split("-");
        if (split.length == 2) {
            return split[0].substring(19, 32);
        } else {
            return split[1].substring(19, 32);
        }
    }

    public static String getDaVersionCore(String daVersionPrefix, long term) {
        return getDaTermStr(term) + "-" + daVersionPrefix;
    }


    public static boolean isStrictConsis() {
        if (isStrictConsis == null) {
            return false;
        }
        return isStrictConsis.get();
    }


    /**
     * 返回DA的versionNum，格式为term-versionNum[0]+uuid-versionNum[1]。term为九位字符串。
     * TERM是递增的，放在最前面是为了防止切主后versionNum可能会比以前的小。
     * TODO F 两个节点之间currentTimeMillis一样时先后关系不准
     */
    public static String getDaVersionNum(boolean isSyncSwitchOff) {
        if (formatedHostUid == null) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "da version num is not init");
        }
        // 进入if判断后Term的值可能会变化，会导致生成的syncStamp的term和versionNum分属两个站点。需要将TERM和MASTER_INDEX绑定
        long curTerm = TERM.get();
        Integer curMasterIndex = INDEX_TERM_MAP.get(curTerm);
        if (isSyncSwitchOff || !isStrictConsis() || LOCAL_CLUSTER_INDEX.equals(curMasterIndex)) {
            String[] split = StringUtils.split(VersionUtil.getVersionNumTrue(), "-");
            return getDaTermStr(curTerm) + "-" + split[0] + formatedHostUid + "-" + split[1];
        }

        if (!canGet.get()) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "Cannot get correct syncStamp from master cluster. ");
        }
        long currentTimeMillis = getCurrentTimeMillis();
        Tuple3<String, String, AtomicLong> tmp = DA_VERSION;

        if (null == tmp || null == tmp.var1) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "da version num is not init");
        }

        return getDaTermStr(curTerm) + "-" + tmp.var1 + currentTimeMillis + formatedHostUid + "-" + tmp.var3.incrementAndGet();
    }


    private static long LAST_TIME = -1L;
    private static Function<Void, Boolean> updateLastTimeFunc = null;
    private static long getCurrentTimeMillis() {
        long currentTimeMillis;
        synchronized (DAVersionUtils.class) {
            currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis < LAST_TIME) {
                if (updateLastTimeFunc == null) {
                    updateLastTimeFunc = v -> {
                        LAST_TIME = System.currentTimeMillis();
                        return true;
                    };
                }
                return LAST_TIME;
            }
            LAST_TIME = currentTimeMillis;
        }
        return currentTimeMillis;
    }

    public static String getLastDaVersionNum(String oldVersion, boolean isSyncSwitchOff) {
        if (formatedHostUid == null) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "da version num is not init");
        }
        // 进入if判断后Term的值可能会变化，会导致生成的syncStamp的term和versionNum分属两个站点。需要将TERM和MASTER_INDEX绑定
        long curTerm = TERM.get();
        Integer curMasterIndex = INDEX_TERM_MAP.get(curTerm);
        if (isSyncSwitchOff || !isStrictConsis() || LOCAL_CLUSTER_INDEX.equals(curMasterIndex)) {
            String s = "";
            if (countHyphen(oldVersion) == 2) {
                String[] splitTemp = oldVersion.split("-");
                s = splitTemp[1].substring(0, splitTemp[1].length() - 5) + "-" + splitTemp[2];
            } else {
                s = oldVersion;
            }
            String[] split = StringUtils.split(VersionUtil.getLastVersionNumTrue(s), "-");
            return getDaTermStr(curTerm) + "-" + split[0] + formatedHostUid + "-" + split[1];
        }

        if (!canGet.get()) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "Cannot get correct syncStamp(last) from master cluster. ");
        }

        Tuple3<String, String, AtomicLong> tmp = DA_VERSION;

        if (null == tmp || null == tmp.var2) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "da version num is not init");
        }

        String res = getDaTermStr(curTerm) + "-" + tmp.var2 + System.currentTimeMillis() + formatedHostUid + "-" + tmp.var3.incrementAndGet();
        if (StringUtils.isNotEmpty(oldVersion) && res.compareTo(oldVersion) < 0) {
            res = oldVersion;
        }
        return res;
    }

    public static int countHyphen(String syncStamp) {
        if (StringUtils.isBlank(syncStamp)) {
            return 0;
        }
        String[] split = syncStamp.split("-");
        return split.length - 1;
    }

    /**
     * 返回包含term的9位长度的字符串
     */
    public static String getDaTermStr(long term) {
        int strLenth = 9;
        char[] chs = new char[strLenth];
        Arrays.fill(chs, '0');
        VersionUtil.getChars(term, strLenth, chs);
        return new String(chs);
    }

    private String trimFirstZero(String s) {
        return s.replaceFirst("^0*", "");
    }

    public static void getMainNodeDASyncStamp() {
        try {
            char[] chs = new char[19];
            Arrays.fill(chs, '0');
            long version = Long.parseLong(pool.getCommand(REDIS_ROCK_INDEX).get(DA_VERSION_PREFIX));
            VersionUtil.getChars(version, 19, chs);
            String redisVersionPrefix = new String(chs);
//            String redisDaVersionCore = getDaVersionCore(redisVersionPrefix, TERM.get());
//            String localDaversionCore = getDaVersionCore(daVersionPrefix, PRE_TERM.get());
            log.debug("get syncStamp from main node, new {}, old {} ", redisVersionPrefix, daVersionPrefix);
            // 进入到下一个任期也允许更新daVersionPrefix
            if (redisVersionPrefix.compareTo(daVersionPrefix) > 0 || termChanged.compareAndSet(true, false)) {
                alreadyEquals.compareAndSet(true, false);
                daVersionPrefix = redisVersionPrefix;
                // 生成lastDaVersion
                char[] last = new char[19];
                Arrays.fill(last, '0');
                VersionUtil.getChars(version - offset, 19, last);

                DA_VERSION = new Tuple3<>(daVersionPrefix, new String(last), new AtomicLong(100_0000L));
                if (updateLastTimeFunc != null) {
                    synchronized (DAVersionUtils.class) {
                        if (updateLastTimeFunc != null) {
                            updateLastTimeFunc.apply(null);
                            updateLastTimeFunc = null;
                        }
                    }
                }
                canGet.compareAndSet(false, true);
            } else if (redisVersionPrefix.equals(daVersionPrefix)) {
                // 允许相同一次。即允许一秒延迟之内的主站点syncStamp同步。
                if (alreadyEquals.get()) {
                    log.error("syncStamp gotten is smaller than expect! new {}, old {} ", redisVersionPrefix, daVersionPrefix);
                    canGet.compareAndSet(true, false);
                    return;
                }
                log.info("similar syncStamp once.");
                alreadyEquals.compareAndSet(false, true);
            } else {
                log.error("syncStamp gotten is smaller than expect! new {}, old {} ", redisVersionPrefix, daVersionPrefix);
                canGet.compareAndSet(true, false);
                // 如果内存里的版本号和redis中主站点更新的差距很大，说明切主后内存里的未更新。一般是非主节点有这个问题。
                long redis = Long.parseLong(redisVersionPrefix);
                long mem = Long.parseLong(daVersionPrefix);
                if (mem - redis > 1000) {
                    log.info("refresh daVersionPrefix.");
                    daVersionPrefix = redisVersionPrefix;
                }
            }
        } catch (Exception e) {
            canGet.compareAndSet(true, false);
            if (e instanceof NumberFormatException) {
                return;
            }
            log.error("getMainNodeDASyncStamp error, ", e);
        }
    }


    public static boolean isMainNode() {
        String firstIp = "";
        // 找出eth4可用，eth12也可用的第一个节点
        for (Map.Entry<String, String> entry : SYNC_ETH4_IP_MAP.entrySet()) {
            String syncIp = entry.getKey();
            String eth4Ip = entry.getValue();
            if (Arrays.asList(INDEX_IPS_MAP.get(LOCAL_CLUSTER_INDEX)).contains(syncIp)
                    && AVAIL_BACKEND_IP_ENTIRE_SET.contains(eth4Ip)) {
                firstIp = syncIp;
                break;
            }
        }

        if (StringUtils.isEmpty(firstIp)) {
            return false;
        }
        return firstIp.equals(LOCAL_NODE_IP);
    }

    public static void initMasterInfo() {
        synchronized (TERM) {
            // 初始化MASTER_INDEX
            String masterIndex = pool.getCommand(REDIS_ROCK_INDEX).get(ArbitratorUtils.MASTER_CLUSTER_INDEX);
            if (StringUtils.isBlank(masterIndex)) {
                masterIndex = "0";
                Mono.just(true).publishOn(SCAN_SCHEDULER)
                        .subscribe(s -> pool.getShortMasterCommand(REDIS_ROCK_INDEX).set(ArbitratorUtils.MASTER_CLUSTER_INDEX, "0"));
            }
            MASTER_INDEX = Integer.parseInt(masterIndex);

            // 初始化term
            String term = pool.getCommand(REDIS_ROCK_INDEX).get(ArbitratorUtils.DA_TERM);
            if (StringUtils.isNotBlank(term)) {
                TERM.set(Long.parseLong(term));
            } else {
                TERM.set(1L);
                Mono.just(true).publishOn(SCAN_SCHEDULER).subscribe(s -> pool.getShortMasterCommand(REDIS_ROCK_INDEX).set(ArbitratorUtils.DA_TERM, "1"));
            }
            INDEX_TERM_MAP.put(TERM.get(), MASTER_INDEX);
        }
        log.info("initMasterInfo complete, Master:{}, TERM:{}", MASTER_INDEX, TERM.get());
    }

    public static void refreshMasterInfo() {
        if (!isStrictConsis()) {
            return;
        }
        try {
            synchronized (TERM) {
                // 每次从redis更新主站点和term
                long curTerm = Long.parseLong(pool.getCommand(REDIS_ROCK_INDEX).get(ArbitratorUtils.DA_TERM));
                if (TERM.get() < curTerm) {
                    termChanged.compareAndSet(false, true);
                    TERM.set(curTerm);
                    String masterIndex = pool.getCommand(REDIS_ROCK_INDEX).get(ArbitratorUtils.MASTER_CLUSTER_INDEX);
                    if (MASTER_INDEX != Integer.parseInt(masterIndex)) {
                        MASTER_INDEX = Integer.parseInt(masterIndex);
                        // 主站点变化时重置daVersionPrefix。这里从站点会用到，而主站点的重置在evaluateMasterIndex()完成。
                        char[] tempChar = new char[19];
                        Arrays.fill(tempChar, '0');
                        daVersionPrefix = new String(tempChar);
                    }
                    INDEX_TERM_MAP.put(TERM.get(), MASTER_INDEX);
                }
            }
        } catch (Exception e) {
            log.error("refreshMasterInfo error, ", e);
        }
    }

}
