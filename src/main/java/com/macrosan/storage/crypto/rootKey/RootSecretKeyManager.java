package com.macrosan.storage.crypto.rootKey;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.crypto.CryptoUtils;
import com.macrosan.utils.msutils.EscapeException;
import io.lettuce.core.SetArgs;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.ADD_ROOT_SECRET_KEY;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.GET_ROOT_KEY_LIST;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;
import static com.macrosan.storage.crypto.common.CryptoConstants.AES;
import static com.macrosan.storage.crypto.rootKey.RootSecretKeyConstants.*;
import static com.macrosan.storage.crypto.rootKey.RootSecretKeyUtils.getEndTimeFromRedis;

/**
 * @Description: 管理数据加密的根密钥
 * @Author wanhao
 * @Date 2023/2/24 0024 上午 11:28
 */
@Log4j2
public class RootSecretKeyManager {

    static final RedisConnPool POOL = RedisConnPool.getInstance();

    /**
     * 当前最新根密钥的版本号
     */
    static final AtomicInteger KEY_VERSION_NUM = new AtomicInteger();

    /**
     * 每次上传下载对象使用keystore.getEntry的方式读取根密钥对性能影响较大，第一次加载密钥文件时通过keystore.getEntry将密钥文件中所有密钥缓存在map中
     */
    static final Map<String, String> ROOT_KEY_CACHE = new ConcurrentHashMap<>();

    /**
     * 修复未写入的密钥和更新密钥的流程可能同时进行，会打开同一文件，使用processor保证修复密钥和更新密钥的操作同时只有一个在进行
     */
    private static final UnicastProcessor<MonoProcessor<Boolean>> PROCESSOR = UnicastProcessor.create(Queues.<MonoProcessor<Boolean>>unboundedMultiproducer().get());

    private static final String NODE_UUID = ServerConfig.getInstance().getHostUuid();
    private static final Map<String, String> IP_UUID_MAP = new HashMap<>();

    private static char[] filePasswd;
    private static KeyStore keyStore;

    /**
     * 代表根密钥管理模块是否初始化完成
     */
    public static final AtomicBoolean INIT_FLAG = new AtomicBoolean();

    static {
        PROCESSOR.publishOn(CRYPTO_SCHEDULER)
                .subscribe(res -> {
                    try {
                        Files.copy(Paths.get(SECRET_KEY_FILE_PATH), Paths.get(BACKUP_SECRET_KEY_FILE_PATH), StandardCopyOption.REPLACE_EXISTING);
                        try (FileOutputStream fo = new FileOutputStream(SECRET_KEY_FILE_PATH)) {
                            keyStore.store(fo, filePasswd);
                        } catch (Exception e) {
                            log.error("", e);
                        }
                    } catch (Exception ignored) {
                    }
                    res.onNext(true);
                });
    }

    public static void init() {
        CRYPTO_SCHEDULER.schedule(() -> init0(0, true), 0, TimeUnit.SECONDS);
    }

    private static void init0(int tryNum, boolean isFirst) {
        try {
            if (tryNum == 5) {
                log.info("retry init root secret key error");
                return;
            }
            boolean isInit = false;
            Map<String, String> skMap = POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hgetall(REDIS_SECRET_KEY);
            if (skMap.isEmpty()) {
                SetArgs setArgs = SetArgs.Builder.nx().ex(10);
                if ("OK".equals(POOL.getShortMasterCommand(REDIS_ROCK_INDEX).set("root_key_init", "", setArgs))) {
                    isInit = true;
                    log.info("first init root secret key");
                    long startTime = System.currentTimeMillis();
                    skMap.put("startTime", String.valueOf(startTime));
                    skMap.put("versionNum", String.valueOf(KEY_VERSION_NUM.get()));
                    skMap.put("passwd", CryptoUtils.generateSecretKey(ROOT_KEY_ALGORTHM));
                    skMap.put("rotationUnit", "DAYS");
                    skMap.put("rotationInterval", "365");
                    skMap.put("automaticRotation", "true");
                    skMap.put("isFirst", "true");
                    POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hmset(REDIS_SECRET_KEY, skMap);
                } else {
                    for (int i = 0; i < 10; i++) {
                        Map<String, String> map1 = POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hgetall(REDIS_SECRET_KEY);
                        if (!map1.isEmpty()) {
                            skMap = map1;
                            break;
                        }
                        TimeUnit.SECONDS.sleep(1);
                    }
                }
            }
            if (skMap.isEmpty()) {
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (Exception ignored) {
                }
                init0(tryNum + 1, true);
                return;
            }
            KEY_VERSION_NUM.set(Integer.parseInt(skMap.get("versionNum")));
            List<String> nodes = POOL.getCommand(REDIS_NODEINFO_INDEX).keys("*");
            for (String node : nodes) {
                String heartIp = POOL.getCommand(REDIS_NODEINFO_INDEX).hget(node, SysConstants.HEART_IP);
                IP_UUID_MAP.put(heartIp, node);
            }

            boolean needLoad = true;
            if (Files.notExists(Paths.get(SECRET_KEY_FILE_PARENT_PATH))) {
                Files.createDirectory(Paths.get(SECRET_KEY_FILE_PARENT_PATH));
            }
            Path filePath = Paths.get(SECRET_KEY_FILE_PATH);
            keyStore = KeyStore.getInstance(KEY_STORE_TYPE);
            filePasswd = skMap.get("passwd").toCharArray();
            if (Files.notExists(filePath)) {
                needLoad = false;
                Files.createFile(filePath);
                keyStore.load(null, null);
                //第一次初始化密钥信息的节点不需要修复密钥文件
                if (!isInit) {
                    repairSecretKeyFile();
                }
            } else if (Files.size(filePath) <= 0) {
                keyStore.load(null, null);
                needLoad = false;
            }

            if (needLoad) {
                try (FileInputStream fi = new FileInputStream(SECRET_KEY_FILE_PATH)) {
                    keyStore.load(fi, filePasswd);
                } catch (EOFException e) {
                    if (!isFirst) {
                        log.info("load root secret key file error");
                        return;
                    }
                    Files.copy(Paths.get(BACKUP_SECRET_KEY_FILE_PATH), Paths.get(SECRET_KEY_FILE_PATH), StandardCopyOption.REPLACE_EXISTING);
                    init0(0, false);
                    return;
                }
                Enumeration<String> aliases = keyStore.aliases();
                while (aliases.hasMoreElements()) {
                    String alias = aliases.nextElement();
                    if (keyStore.entryInstanceOf(alias, KeyStore.SecretKeyEntry.class)) {
                        KeyStore.SecretKeyEntry entry = (KeyStore.SecretKeyEntry) keyStore.getEntry(alias, new KeyStore.PasswordProtection(alias.toCharArray()));
                        String rootKey = CryptoUtils.bytesToString(entry.getSecretKey().getEncoded());
                        ROOT_KEY_CACHE.put(alias, rootKey);
                    }
                }
            }

            //其他节点更新根密钥时本节点不在线，将不在线期间生成的根密钥写入当前节点的密钥文件中
            List<String> keys = POOL.getShortMasterCommand(REDIS_ROCK_INDEX).keys(NODE_UUID + "_ROOT_KEY_*");
            if (!keys.isEmpty()) {
                try (StatefulRedisConnection<String, String> tmpConnection =
                             RedisConnPool.getInstance().getSharedConnection(REDIS_ROCK_INDEX).newMaster()) {
                    RedisCommands<String, String> command = tmpConnection.sync();
                    command.multi();
                    for (String key : keys) {
                        command.hgetall(key);
                    }
                    TransactionResult result = command.exec();
                    List<String> recoverVersionList = new ArrayList<>();
                    for (int i = 0; i < result.size(); i++) {
                        Map<String, String> map = result.get(i);
                        String versionNum = map.get("versionNum");
                        if (keyStore.containsAlias(versionNum)) {
                            continue;
                        }
                        recoverVersionList.add(NODE_UUID + "_ROOT_KEY_" + versionNum);
                        String rootKey = map.get("rootKey");
                        log.info("root secret key add version:{} success", versionNum);
                        ROOT_KEY_CACHE.putIfAbsent(versionNum, rootKey);
                        SecretKey secretKey = new SecretKeySpec(CryptoUtils.stringToBytes(rootKey), AES);
                        KeyStore.SecretKeyEntry entry = new KeyStore.SecretKeyEntry(secretKey);
                        keyStore.setEntry(versionNum, entry, new KeyStore.PasswordProtection(versionNum.toCharArray()));
                    }
                    //有内容写入时打开输出流，无内容写入时以非追加方式打开FileOutputStream时会清空文件
                    if (!recoverVersionList.isEmpty()) {
                        MonoProcessor<Boolean> res = MonoProcessor.create();
                        PROCESSOR.onNext(res);
                        res.block();
                        POOL.getShortMasterCommand(REDIS_ROCK_INDEX).del(recoverVersionList.toArray(new String[0]));
                    }
                }
            }

            boolean automaticRotation = Boolean.parseBoolean(skMap.get("automaticRotation"));
            if (automaticRotation) {
                update();
            }
            INIT_FLAG.set(true);
            log.info("init root secret key!");
        } catch (Exception e) {
            log.error("", e);
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (Exception ignored) {
            }
            init0(tryNum + 1, true);
        }
    }


    private static void update() {
        try {
            boolean isFirst = Boolean.parseBoolean(POOL.getCommand(REDIS_SYSINFO_INDEX).hget(REDIS_SECRET_KEY, "isFirst"));
            long currTime = System.currentTimeMillis();
            long beforeEndTime = getEndTimeFromRedis();

            //其他节点已更新根密钥 或 还未到下次更新根密钥时间
            if (!isFirst && currTime < beforeEndTime) {
                CRYPTO_SCHEDULER.schedule(RootSecretKeyManager::update, beforeEndTime - currTime, TimeUnit.MILLISECONDS);
                return;
            }

            SetArgs setArgs = SetArgs.Builder.nx().ex(30);
            if ("OK".equals(POOL.getShortMasterCommand(REDIS_ROCK_INDEX).set(ROOT_LOCK, NODE_UUID, setArgs))) {
                updateRootKey();
                if (isFirst) {
                    POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel(REDIS_SECRET_KEY, "isFirst");
                }
            } else {
                String beforeUuid = POOL.getShortMasterCommand(REDIS_ROCK_INDEX).get(ROOT_LOCK);
                for (int i = 0; i < 4; i++) {
                    if ("OK".equals(POOL.getShortMasterCommand(REDIS_ROCK_INDEX).set(ROOT_LOCK, NODE_UUID, setArgs))) {
                        long afterEndTime = getEndTimeFromRedis();
                        //endTime未修改，表示其他节点获取锁后更新根密钥失败，需要重新更新根密钥
                        if (beforeEndTime == afterEndTime) {
                            updateRootKey();
                        }
                        if (isFirst) {
                            POOL.getShortMasterCommand(REDIS_ROCK_INDEX).hdel(REDIS_SECRET_KEY, "isFirst");
                        }
                        if (beforeEndTime != afterEndTime) {
                            CRYPTO_SCHEDULER.schedule(RootSecretKeyManager::update, afterEndTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                        }
                        return;
                    } else {
                        TimeUnit.SECONDS.sleep(10);
                        String afterUuid = POOL.getShortMasterCommand(REDIS_ROCK_INDEX).get(ROOT_LOCK);

                        //获取锁过程中锁的持有者发生改变，则重置重试次数
                        if (afterUuid != null && !afterUuid.equals(beforeUuid)) {
                            i = 0;
                            beforeUuid = afterUuid;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("", e);
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (Exception ignored) {
            }
            update();
        }
    }

    private static void updateRootKey() {
        try {
            log.info("start update root secretKey");
            int oldVersionNum = Integer.parseInt(POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hget(REDIS_SECRET_KEY, "versionNum"));
            String newVersionNum = String.valueOf(new AtomicInteger(KEY_VERSION_NUM.get()).incrementAndGet());
            String rootKey = CryptoUtils.generateSecretKey(ROOT_KEY_ALGORTHM);

            //待更新的versionNum必须大于当前最新的versionNum，否则由其他节点更新根密钥，并同步最新的versionNum到本节点。
            if (oldVersionNum >= Integer.parseInt(newVersionNum)) {
                CRYPTO_SCHEDULER.schedule(RootSecretKeyManager::update, getEndTimeFromRedis() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                return;
            }
            long startTime = System.currentTimeMillis();
            long endTime = getEndTimeFromRedis(startTime);

            Map<String, String> map = new HashMap<>(2);
            map.put("startTime", String.valueOf(startTime));
            map.put("versionNum", newVersionNum);
            String res = POOL.getShortMasterCommand(REDIS_SYSINFO_INDEX).hmset(REDIS_SECRET_KEY, map);
            if ("OK".equals(res)) {
                SocketReqMsg msg = new SocketReqMsg("", 0).put("versionNum", newVersionNum).put("rootKey", rootKey).put("endTime", String.valueOf(endTime));
                Map<String, String> map1 = new HashMap<>(2);
                map1.put("versionNum", newVersionNum);
                map1.put("rootKey", rootKey);
                for (Map.Entry<String, String> entry : IP_UUID_MAP.entrySet()) {
                    String ip = entry.getKey();
                    String uuid = entry.getValue();
                    Mono.just(true).publishOn(CRYPTO_SCHEDULER)
                            .timeout(Duration.ofSeconds(25))
                            .doOnNext(x -> POOL.getShortMasterCommand(REDIS_ROCK_INDEX).hmset(uuid + "_ROOT_KEY_" + newVersionNum, map1))
                            .flatMap(b -> RSocketClient.getRSocket(ip, BACK_END_PORT))
                            .flatMap(r -> r.requestResponse(DefaultPayload.create(Json.encode(msg), ADD_ROOT_SECRET_KEY.name())))
                            .map(r -> ErasureServer.PayloadMetaType.SUCCESS.name().equalsIgnoreCase(r.getMetadataUtf8()))
                            .doOnError(e -> {
                                //发送消息时其他节点的rsocket可能还未初始化完成或不在线，处理EscapeException("Request-Response not implemented.")
                                if (!(e instanceof EscapeException)) {
                                    log.error("", e);
                                }
                                retryUpdateKeyRsocket(ip, uuid, msg);
                            }).subscribe(b -> {
                                if (b) {
                                    POOL.getShortMasterCommand(REDIS_ROCK_INDEX).del(uuid + "_ROOT_KEY_" + newVersionNum);
                                }
                            });
                }
            }
        } catch (Exception e) {
            log.error("", e);
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (Exception ignored) {
            }
            updateRootKey();
        }
    }

    /**
     * 处理接收到其他节点添加根密钥的消息
     */
    public static boolean addRootSecretKey(String versionNumStr, String rootKey, String endTime) {
        try {
            if (keyStore.containsAlias(versionNumStr)) {
                return true;
            }
            int versionNum = Integer.parseInt(versionNumStr);
            SecretKey secretKey = new SecretKeySpec(CryptoUtils.stringToBytes(rootKey), AES);
            KeyStore.SecretKeyEntry entry = new KeyStore.SecretKeyEntry(secretKey);
            keyStore.setEntry(versionNumStr, entry, new KeyStore.PasswordProtection(versionNumStr.toCharArray()));
            MonoProcessor<Boolean> res = MonoProcessor.create();
            PROCESSOR.onNext(res);
            res.block();
            ROOT_KEY_CACHE.putIfAbsent(versionNumStr, rootKey);
            //保证密钥已写入密钥文件，再更新版本号
            KEY_VERSION_NUM.getAndUpdate(old -> Math.max(versionNum, old));
            log.info("root secret key add version: {} success", versionNum);
            if (KEY_VERSION_NUM.get() == versionNum) {
                CRYPTO_SCHEDULER.schedule(RootSecretKeyManager::update, Long.parseLong(endTime) - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            log.error("", e);
            return false;
        }
        return true;
    }

    /**
     * 发送更新密钥的消息给其它节点失败，定期重试
     */
    private static void retryUpdateKeyRsocket(String ip, String uuid, SocketReqMsg msg) {
        Disposable[] disposables = new Disposable[1];
        disposables[0] = CRYPTO_SCHEDULER.schedulePeriodically(() -> Mono.just(true)
                .timeout(Duration.ofSeconds(25))
                .flatMap(b -> RSocketClient.getRSocket(ip, BACK_END_PORT))
                .flatMap(r -> r.requestResponse(DefaultPayload.create(Json.encode(msg), ADD_ROOT_SECRET_KEY.name())))
                .map(r -> ErasureServer.PayloadMetaType.SUCCESS.name().equalsIgnoreCase(r.getMetadataUtf8()))
                .doOnError(e -> {
                })
                .subscribe(b -> {
                    if (b) {
                        POOL.getShortMasterCommand(REDIS_ROCK_INDEX).del(uuid + "_ROOT_KEY_" + msg.get("versionNum"));
                    }
                    disposables[0].dispose();
                }), 30, 30, TimeUnit.SECONDS);

    }

    public static Map<String, String> getRootKeyList() {
        return ROOT_KEY_CACHE;
    }

    /**
     * 新加的节点，或误删了密钥文件时，本节点是不存在密钥文件的，需执行本节点密钥文件的修复,至少需保证至少有一个其他节点还存在密钥文件。
     */
    private static void repairSecretKeyFile() {
        //过滤掉本节点
        Set<String> ipList = IP_UUID_MAP.entrySet().stream().filter(entry -> !entry.getValue().equals(NODE_UUID)).map(Map.Entry::getKey).collect(Collectors.toSet());
        List<Payload> rootKeyList = Flux.fromIterable(ipList)
                .flatMap(ip -> RSocketClient.getRSocket(ip, BACK_END_PORT)
                        .timeout(Duration.ofSeconds(3))
                        .flatMap(r -> r.requestResponse(DefaultPayload.create("", GET_ROOT_KEY_LIST.name())))
                        .onErrorReturn(ErasureServer.ERROR_PAYLOAD))
                .filter(payload -> ErasureServer.PayloadMetaType.SUCCESS.name().equalsIgnoreCase(payload.getMetadataUtf8()))
                .collectList()
                .block();
        MonoProcessor<List<Payload>> res = MonoProcessor.create();

        //所有节点rsocket连接失败
        if (rootKeyList == null || rootKeyList.isEmpty()) {
            retryRepairRsocket(ipList, res);
            rootKeyList = res.block();
        }
        Map<String, String> rootKeyMap = new HashMap<>();
        for (Payload payload : rootKeyList) {
            String dataUtf8 = payload.getDataUtf8();
            Map<String, String> map = Json.decodeValue(dataUtf8, new TypeReference<Map<String, String>>() {
            });
            rootKeyMap.putAll(map);
        }
        Set<Map.Entry<String, String>> set = rootKeyMap.entrySet();

        for (Map.Entry<String, String> entry : set) {
            ROOT_KEY_CACHE.putIfAbsent(entry.getKey(), entry.getValue());
        }

        log.info("repair rootKey versionList:{}", rootKeyMap.keySet());

        //将待修复的密钥暂写到redis中，待后续的修复流程修复
        for (Map.Entry<String, String> entry : set) {
            String versionNum = entry.getKey();
            String rootKey = entry.getValue();
            Map<String, String> map1 = new HashMap<>(2);
            map1.put("versionNum", versionNum);
            map1.put("rootKey", rootKey);
            POOL.getShortMasterCommand(REDIS_ROCK_INDEX).hmset(NODE_UUID + "_ROOT_KEY_" + versionNum, map1);
        }
    }

    /**
     * 修复密钥时，连接其他节点都失败，定期重试。
     */
    private static void retryRepairRsocket(Set<String> ipList, MonoProcessor<List<Payload>> res) {
        Disposable[] disposables = new Disposable[1];
        disposables[0] = CRYPTO_SCHEDULER.schedulePeriodically(() -> {
            List<Payload> rootKeyList = Flux.fromIterable(ipList)
                    .flatMap(ip -> RSocketClient.getRSocket(ip, BACK_END_PORT)
                            .timeout(Duration.ofSeconds(3))
                            .flatMap(r -> r.requestResponse(DefaultPayload.create("", GET_ROOT_KEY_LIST.name())))
                            .onErrorReturn(ErasureServer.ERROR_PAYLOAD))
                    .filter(payload -> ErasureServer.PayloadMetaType.SUCCESS.name().equalsIgnoreCase(payload.getMetadataUtf8()))
                    .collectList()
                    .block();
            //从其他节点获取到了待修复的密钥列表，不再进行重试
            if (rootKeyList != null && !rootKeyList.isEmpty()) {
                disposables[0].dispose();
                res.onNext(rootKeyList);
            }
        }, 3, 3, TimeUnit.SECONDS);
    }

    public static void addNode(String uuid) {
        try {
            if (!IP_UUID_MAP.containsValue(uuid)) {
                String heartIp = POOL.getCommand(REDIS_NODEINFO_INDEX).hget(uuid, SysConstants.HEART_IP);
                IP_UUID_MAP.put(heartIp, uuid);
            }
        } catch (Exception ignored) {
        }
    }

    public static void removeNode(String uuid) {
        try {
            if (IP_UUID_MAP.containsValue(uuid)) {
                String heartIp = POOL.getCommand(REDIS_NODEINFO_INDEX).hget(uuid, SysConstants.HEART_IP);
                IP_UUID_MAP.remove(heartIp);
            }
        } catch (Exception ignored) {
        }
    }

    public static Map<String, String> getIpUuidMap() {
        return IP_UUID_MAP;
    }


}
