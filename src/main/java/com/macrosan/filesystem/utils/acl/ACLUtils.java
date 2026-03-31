package com.macrosan.filesystem.utils.acl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.database.redis.Redis6380ConnPool;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.HeartBeatChecker;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.ReqInfo;
import com.macrosan.filesystem.cifs.types.smb2.SID;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.nfs.RpcCallHeader;
import com.macrosan.filesystem.nfs.auth.AuthUnix;
import com.macrosan.filesystem.nfs.types.ObjAttr;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.message.jsonmsg.FSIdentity;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.rsocket.client.RSocketClient;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.macrosan.constants.AccountConstants.DEFAULT_USER_ID;
import static com.macrosan.constants.ErrorNo.BUCKET_OWNER_MISSING_FS_ACCOUNT;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.server.ErasureServer.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.FsConstants.ACLConstants.*;
import static com.macrosan.filesystem.FsConstants.NFSACLType.*;
import static com.macrosan.filesystem.FsConstants.ProtoFlag.*;
import static com.macrosan.filesystem.FsConstants.SMB2ACEFlag.*;
import static com.macrosan.filesystem.FsConstants.SMB2ACEType.*;
import static com.macrosan.filesystem.async.AsyncUtils.MOUNT_CLUSTER;
import static com.macrosan.filesystem.nfs.reply.AccessReply.*;
import static com.macrosan.rsocket.server.Rsocket.BACK_END_PORT;

/**
 * @Author: WANG CHENXING
 * @Date: 2025/2/10
 * @Description:
 */
@Log4j2
public class ACLUtils {
    public static final RedisConnPool pool = RedisConnPool.getInstance();
    public static IamRedisConnPool iamPool = IamRedisConnPool.getInstance();
    private static Redis6380ConnPool pool_6380 = Redis6380ConnPool.getInstance();
    public static Map<String, FSIdentity> userInfo = new HashMap<>();
    public static Map<Integer, String> uidToS3ID = new HashMap<>();
    public static Map<String, String> sidToS3ID = new HashMap<>();
    public static Map<Integer, String> gidToS3ID = new HashMap<>();
    public static Map<String, Set<Integer>> s3IDToGids = new HashMap<>();
    public static int DEFAULT_ANONY_UID = 65534;
    public static int DEFAULT_ANONY_GID = 65534;
    public static final MsExecutor executor = new MsExecutor(1, 1, new MsThreadFactory("fs-acl-set"));
    private static final ScheduledThreadPoolExecutor UPDATE_FS_ENTITY_EXECUTOR = new ScheduledThreadPoolExecutor(1, runnable -> new Thread(runnable, "aclEntityUpdater"));

    public static final Scheduler FS_ACL_SCHEDULER = Schedulers.fromExecutor(executor);
    public static boolean aclDebug = false;
    public static final String S3ID_PATTERN = "^[0-9]+$";
    public static boolean NFS_ACL_START = false;
    public static boolean CIFS_ACL_START = false;
    public static int MAX_ID = 65533;
    public static int MIN_ID = 1000;
    public static boolean notAllowInvalidSID = true;

    public static enum ProtoType {
        S3(0),
        NFSV3(1),
        NFSV4(2),
        SMB2(3),
        FTP(4);

        public int type;

        ProtoType(int type) {
            this.type = type;
        }
    }

    public static void init() {
        init(true);
    }

    public static void init(boolean mainStart) {
        try {
            //keys出全是数字的key，但不能全为0
            ScanArgs scanArgs = new ScanArgs().match("[0-9]*").limit(10);
            KeyScanCursor<String> keyScanCursor = new KeyScanCursor<>();
            keyScanCursor.setCursor("0");
            KeyScanCursor<String> res;
            int count = 0;
            if (mainStart) {
                uidToS3ID.clear();
                gidToS3ID.clear();
                s3IDToGids.clear();
                userInfo.clear();
            }
            do {
                res = pool.getCommand(REDIS_USERINFO_INDEX).scan(keyScanCursor, scanArgs);
                List<String> userKeys = res.getKeys();
                Map<String, String> userMap = new HashMap<>();
                for (String key : userKeys) {
                    if (!key.matches(S3ID_PATTERN)) {
                        continue;
                    }

                    //避免数字ak、sk与s3Id混淆
                    if (key.length() != 12) {
                        continue;
                    }

                    try {
                        if (!"hash".equals(pool.getCommand(REDIS_USERINFO_INDEX).type(key))) {
                            continue;
                        }

                        count++;
                        userMap.clear();
                        userMap = pool.getCommand(REDIS_USERINFO_INDEX).hgetall(key);
                        if (null == userMap || userMap.isEmpty()) {
                            log.error("account {} info is empty", key);
                            userMap = new HashMap<>();
                            continue;
                        }

                        String uidStr = userMap.get("uid");
                        String gidStr = userMap.get("master_gid");
                        String gids = userMap.get("gids");
                        String name = userMap.get("name");
                        int uid = 0, gid = 0;
                        if (DEFAULT_USER_ID.equals(key)) {
                            //匿名账户默认对应65534
                            if (StringUtils.isNotBlank(uidStr)) {
                                uid = Integer.parseInt(uidStr);
                            } else {
                                uid = DEFAULT_ANONY_UID;
                            }
                            uidToS3ID.put(uid, key);

                            if (StringUtils.isNotBlank(gidStr)) {
                                gid = Integer.parseInt(gidStr);
                            } else {
                                gid = DEFAULT_ANONY_GID;
                            }
                            gidToS3ID.put(gid, key);

                            if (StringUtils.isNotBlank(gids)) {
                                Set<Integer> set = Json.decodeValue(gids, new TypeReference<Set<Integer>>() {
                                });
                                s3IDToGids.put(key, set);
                            } else {
                                Set<Integer> set = new HashSet<>();
                                set.add(DEFAULT_ANONY_GID);
                                s3IDToGids.put(key, set);
                            }
                        } else {
                            if (StringUtils.isNotBlank(uidStr)) {
                                uid = Integer.parseInt(uidStr);
                                uidToS3ID.put(uid, key);
                            }
                            if (StringUtils.isNotBlank(gidStr)) {
                                gid = Integer.parseInt(gidStr);
                                gidToS3ID.put(gid, key);
                            }
                            if (StringUtils.isNotBlank(gids)) {
                                Set<Integer> set = Json.decodeValue(gids, new TypeReference<Set<Integer>>() {
                                });
                                s3IDToGids.put(key, set);
                            }
                        }

                        FSIdentity i = new FSIdentity(key, uid, gid, FSIdentity.getUserSIDByUid(uid), FSIdentity.getGroupSIDByGid(gid), name);
                        userInfo.put(key, i);
                    } catch (Exception e) {
                        log.error("load fileSystem Account ID:{} error", key, e);
                        userMap = new HashMap<>();
                    }
                }
                keyScanCursor.setCursor(res.getCursor());
            } while (!res.isFinished());

            //todo root用户对应的s3Id暂用"0"代替
            uidToS3ID.put(0, "0");
            userInfo.put(ADMIN_S3ID, new FSIdentity(ADMIN_S3ID, 0, 0, FSIdentity.getUserSIDByUid(0), FSIdentity.getGroupSIDByGid(0), ADMIN_NAME));
            log.info("init fileSystem userInfo success.user count:{}.", count);
        } catch (Exception e) {
            log.error("load fileSystem identity error ", e);
        }

        try {
            // nfs_acl_start:0、未设置->关闭文件端权限判断；1->开启文件端权限判断
            String nfsAclStart = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("nfs_acl_start");
            if (null != nfsAclStart) {
                NFS_ACL_START = !"0".equals(nfsAclStart);
            }

            String cifsAclStart = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("cifs_acl_start");
            if (null != cifsAclStart) {
                CIFS_ACL_START = !"0".equals(cifsAclStart);
            }

            if (RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).exists("fs_max_id") > 0) {
                String maxIdStr = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("fs_max_id");
                MAX_ID = Integer.parseInt(maxIdStr);
            }

            String notAllowInvalid = pool.getCommand(REDIS_SYSINFO_INDEX).get("not_allow_invalid_sid");
            notAllowInvalidSID = !"0".equals(notAllowInvalid);
        } catch (Exception e) {
            log.error("nfs acl start flag check error ", e);
        }
    }

    public static String updateAllFsIdentityCommand(String action) {
        //异步进行更新
        UPDATE_FS_ENTITY_EXECUTOR.submit(() -> {
            Mono.just(1L)
                    .publishOn(FS_ACL_SCHEDULER)
                    .flatMap(i -> updateAllFsIdentity(action))
                    .timeout(Duration.ofSeconds(35))
                    .doOnError(e -> {
                        log.error("update all fs identity error ", e);
                    })
                    .subscribe();
        });
        return "success";
    }

    public static Mono<Integer> updateAllFsIdentity(String action) {
        SocketReqMsg msg = new SocketReqMsg("", 0);
        msg.put("action", action);
        MonoProcessor<Integer> res = MonoProcessor.create();
        AtomicInteger num = new AtomicInteger(0);
        List<String> ipList = RabbitMqUtils.HEART_IP_LIST;
        Flux<Tuple2<String, Boolean>> responses = Flux.empty();

        for (int i = 0; i < ipList.size(); i++) {
            String ip = ipList.get(i);
            Mono<Tuple2<String, Boolean>> response = RSocketClient.getRSocket(ip, BACK_END_PORT)
                    .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), UPDATE_ALL_FS_IDENTITY.name())))
                    .timeout(Duration.ofSeconds(30))
                    .map(payload -> {
                        try {
                            String metaDataPayload = payload.getMetadataUtf8();
                            if (metaDataPayload.equalsIgnoreCase(SUCCESS.name())) {
                                num.incrementAndGet();
                                return new Tuple2<>(ip, true);
                            }
                            return new Tuple2<>(ip, false);
                        } finally {
                            payload.release();
                        }
                    })
                    .doOnError(e -> {
                        log.error("notify {} update fsIdentity error ", ip, e);
                    })
                    .onErrorReturn(new Tuple2<>(ip, false));

            responses = responses.mergeWith(response);
        }

        responses.collectList().subscribe(t -> {
            if (num.get() > ipList.size() / 2) {
                res.onNext(1);
            } else {
                res.onNext(-1);
            }
        });

        return res;

    }

    public static Mono<Payload> updateAllFsIdentity0(Payload payload) {
        try {
            SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
            String action = msg.get("action");
            if ("update".equals(action)) {
                //keys出全是数字的key，但不能全为0
                ScanArgs scanArgs = new ScanArgs().match("[0-9]*").limit(10);
                KeyScanCursor<String> keyScanCursor = new KeyScanCursor<>();
                keyScanCursor.setCursor("0");
                KeyScanCursor<String> res;

                do {
                    res = pool.getCommand(REDIS_USERINFO_INDEX).scan(keyScanCursor, scanArgs);
                    List<String> userKeys = res.getKeys();
                    for (String key : userKeys) {
                        if (!key.matches(S3ID_PATTERN)) {
                            continue;
                        }
                        //避免数字ak、sk与s3Id混淆
                        if (key.length() != 12) {
                            continue;
                        }

                        //匿名账户不重复加载
                        if (DEFAULT_USER_ID.equals(key)) {
                            continue;
                        }

                        try {
                            if (!"hash".equals(pool.getCommand(REDIS_USERINFO_INDEX).type(key))) {
                                continue;
                            }

                            String uidStr = pool.getCommand(REDIS_USERINFO_INDEX).hget(key, "uid");
                            String gidStr = pool.getCommand(REDIS_USERINFO_INDEX).hget(key, "master_gid");
                            String gids = pool.getCommand(REDIS_USERINFO_INDEX).hget(key, "gids");
                            ACLUtils.userInfo.compute(key, (k, v) -> {
                                //不存在s3Id对应的信息则创建；各参数的校验已在后端包完成
                                int newUid = 0;
                                int newGid = 0;
                                boolean updateFlag = false;
                                if (StringUtils.isNotEmpty(uidStr)) {
                                    synchronized (ACLUtils.uidToS3ID) {
                                        newUid = Integer.parseInt(uidStr);
                                        ACLUtils.uidToS3ID.put(newUid, key);
                                        updateFlag = true;
                                    }
                                }
                                if (StringUtils.isNotEmpty(gidStr)) {
                                    synchronized (ACLUtils.gidToS3ID) {
                                        newGid = Integer.parseInt(gidStr);
                                        ACLUtils.gidToS3ID.put(newGid, key);
                                        updateFlag = true;
                                    }
                                }
                                Set<Integer> set = null;
                                if (null != gids && !gids.isEmpty()) {
                                    set = Json.decodeValue(gids, new TypeReference<Set<Integer>>() {
                                    });

                                } else {
                                    if (updateFlag) {
                                        set = new HashSet<>();
                                        set.add(DEFAULT_ANONY_GID);
                                        s3IDToGids.put(key, set);
                                    }
                                }
                                final Set<Integer> set1 = set;
                                if (null != set1) {
                                    ACLUtils.s3IDToGids.compute(key, (k0, v0) -> {
                                        if (!set1.isEmpty()) {
                                            return set1;
                                        } else {
                                            return v0;
                                        }
                                    });
                                }

                                FSIdentity fsIdentity = new FSIdentity(key, newUid, newGid, FSIdentity.getUserSIDByUid(newUid), FSIdentity.getGroupSIDByGid(newGid));
                                return fsIdentity;

                            });
                        } catch (Exception e) {
                            if (!e.getMessage().contains("WRONGTYPE")) {
                                log.error("updateAllFsIdentity0 error key:{}", key, e);
                            }
                        }

                    }
                    keyScanCursor.setCursor(res.getCursor());
                } while (!res.isFinished());
            } else if ("delete".equals(action)) {
                List<String> keySet = ACLUtils.userInfo.keySet().stream().collect(Collectors.toList());
                //每次查询redis 1000个key，将keySet 截取成每段1000个元素进行查询
                for (int i = 0; i < keySet.size(); i += 1000) {
                    List<String> subList = new ArrayList<>(keySet.subList(i, Math.min(i + 1000, keySet.size())));
                    try (StatefulRedisConnection<String, String> tmpConnection = pool.getSharedConnection(REDIS_USERINFO_INDEX).newMaster()) {
                        RedisCommands<String, String> sync = tmpConnection.sync();
                        sync.multi();
                        subList.forEach(key -> {
                            sync.exists(key);
                        });
                        TransactionResult exec = sync.exec();
                        for (int j = 0; j < exec.size(); j++) {
                            long exists = exec.get(j);
                            String key = subList.get(j);
                            if (exists <= 0 && !"0".equals(key)) {
                                FSIdentity fsIdentity = ACLUtils.userInfo.get(key);
                                if (null != fsIdentity) {
                                    ACLUtils.uidToS3ID.remove(fsIdentity.getUid());
                                    ACLUtils.sidToS3ID.remove(fsIdentity.getUserSid());
                                    ACLUtils.gidToS3ID.remove(fsIdentity.getGid());
                                    ACLUtils.s3IDToGids.remove(key);
                                    ACLUtils.userInfo.remove(key);
                                }
                            }
                        }
                    }
                }

            }

        } catch (Exception e) {
            log.error("update fs identity error ", e);
            return Mono.just(ERROR_PAYLOAD);
        }
        return Mono.just(SUCCESS_PAYLOAD);
    }

    /**
     * todo 当前对于root用户暂返回“0”
     * 1、超级用户，uid=0的用户，无视权限可以执行任何操作
     * 2、普通用户，uid被记录在存储设备中，根据权限执行操作
     * 3、匿名用户，uid没有被记录在存储设备中，执行时根据other权限操作
     **/
    public static String getS3IdByUid(int uid) {
        return uidToS3ID.getOrDefault(uid, DEFAULT_USER_ID);
    }

    public static String getS3IdBySID(String sid) {
        if (null != sid && sid.startsWith("S-1")) {
            return sidToS3ID.getOrDefault(sid, DEFAULT_USER_ID);
        } else {
            return DEFAULT_USER_ID;
        }
    }

    /**
     * 文件只需要将继承权限转换为 NFS ACL
     * 只有目录在创建时不仅要保存继承权限，还要将继承权限转换为 NFS ACL
     **/
    public static List<Inode.ACE> pickNFSDefACL(List<Inode.ACE> ACEs, String obj, Inode inode) {
        if (null == ACEs || ACEs.isEmpty()) {
            return null;
        }

        boolean isDir = false;
        if (StringUtils.isNotBlank(obj)) {
            isDir = obj.endsWith("/");
        }

        try {
            List<Inode.ACE> curAcl = ACEs;
            List<Inode.ACE> defAcl = new LinkedList<>();

            for (Inode.ACE ace : curAcl) {
                int type = ace.getNType();
                if (type > 4096) {
                    if (isDir) {
                        Inode.ACE defAce = ace.clone();
                        //对于DEFAULT_NFSACL_USER_OBJ、DEFULT_NFSACL_GROUP_OBJ，继承下来时需修正uid为当前文件拥有者的uid
                        if (type == DEFAULT_NFSACL_USER_OBJ) {
                            defAce.setId(inode.getUid());
                            defAcl.add(defAce);
                            defAcl.add(new Inode.ACE(type - 4096, ace.getRight(), inode.getUid()));
                        } else if (type == DEFAULT_NFSACL_GROUP_OBJ) {
                            defAce.setId(inode.getGid());
                            defAcl.add(defAce);
                            defAcl.add(new Inode.ACE(type - 4096, ace.getRight(), inode.getGid()));
                        } else {
                            defAcl.add(ace);
                            defAcl.add(new Inode.ACE(type - 4096, ace.getRight(), ace.getId()));
                        }
                    } else {
                        defAcl.add(new Inode.ACE(type - 4096, ace.getRight(), ace.getId()));
                    }
                }
            }

            if (defAcl.isEmpty()) {
                return null;
            }

            return defAcl;
        } catch (Exception e) {
            log.error("pick default error: {}, ", ACEs, e);
            return new LinkedList<>();
        }
    }

    public static List<Inode.ACE> pickAllDefACL(List<Inode.ACE> ACEs, String obj) {
        if (null == ACEs || ACEs.isEmpty()) {
            return null;
        }

        boolean isDir = false;
        if (StringUtils.isNotBlank(obj)) {
            isDir = obj.endsWith("/");
        }

        try {
            List<Inode.ACE> curAcl = ACEs;
            List<Inode.ACE> defAcl = new LinkedList<>();

            if (CIFSACL.isExistNfsACE(curAcl)) {
                for (Inode.ACE ace : curAcl) {
                    int type = ace.getNType();
                    if (type > 4096) {
                        if (isDir) {
                            //createS3Inode时不能确定实际的拥有者，因此该函数中不做修改，在最终createS3Inode时再做修正
                            defAcl.add(ace.clone());
                            defAcl.add(new Inode.ACE(type - 4096, ace.getRight(), ace.getId()));
                        } else {
                            defAcl.add(new Inode.ACE(type - 4096, ace.getRight(), ace.getId()));
                        }
                    }
                }
            } else {
                //如果acl列表中都是cifs，则继承时以cifs规则继承
                for (Inode.ACE dirACE : curAcl) {
                    short dirFlag = dirACE.getFlag();
                    boolean inheritDir = (dirFlag & CONTAINER_INHERIT_ACE) != 0;
                    boolean inheritFile = (dirFlag & OBJECT_INHERIT_ACE) != 0;

                    //非继承权限直接跳过
                    if (!inheritDir && !inheritFile) {
                        continue;
                    }

                    short setInheritFlag = 0;
                    if (isDir) {
                        //创建的是目录，则所有继承权限全部继承，其中若有目录继承权限，则同时转为普通控制权限
                        //目录有继承权限，保留原本的继承权限，同时将继承权限转为普通权限存储
                        setInheritFlag |= (inheritDir ? CONTAINER_INHERIT_ACE : 0);
                        setInheritFlag |= (inheritFile ? OBJECT_INHERIT_ACE : 0);
                        setInheritFlag |= INHERIT_ONLY_ACE;

                        if (inheritDir) {
                            //转为普通权限ACE
                            Inode.ACE selfACE = new Inode.ACE(dirACE.getCType(), (short) 0, dirACE.getSid(), dirACE.getMask());
                            defAcl.add(selfACE);
                        }

                        //保留继承权限
                        Inode.ACE inheritACE = new Inode.ACE(dirACE.getCType(), setInheritFlag, dirACE.getSid(), dirACE.getMask());
                        defAcl.add(inheritACE);
                    } else {
                        //创建的是文件
                        if (inheritFile) {
                            //创建的是文件，将继承权限转为普通权限
                            Inode.ACE selfACE = new Inode.ACE(dirACE.getCType(), (short) 0, dirACE.getSid(), dirACE.getMask());
                            defAcl.add(selfACE);
                        }
                    }
                }
            }

            if (defAcl.isEmpty()) {
                return null;
            }

            return defAcl;
        } catch (Exception e) {
            log.error("pick default error: {}, ", ACEs, e);
            return new LinkedList<>();
        }
    }

    /**
     * 存在以下几种情况：
     * 第一个是超级用户，uid=0，什么事情都可以干，无视权限；
     * 第二个是正常的记录MOSS中有记录的用户，根据权限执行；
     * 第三个是匿名的用户，其没有被记录在存储设备中，无任何权限；
     **/
    public static int[] getUidAndGid(RpcCallHeader callHeader) {
        int[] uidAndGid = {0, 0};
        if (callHeader.getAuth() instanceof AuthUnix) {
            AuthUnix auth = (AuthUnix) callHeader.getAuth();
            uidAndGid[0] = auth.getUid();
            uidAndGid[1] = auth.getGid();
        }

        return uidAndGid;
    }

    /**
     * 多在创建时使用，若创建者未配置文件账户，则创建文件默认为root所有；但创建者本身未配置的uid或s3Id对应为匿名账户
     * 匿名账户在访问未配置账户时创建的文件会经过权限提升的处理
     **/
    public static int[] getUidAndGid(String reqS3ID) {
        int[] uidAndGid = {0, 0};
        try {
            if (userInfo.containsKey(reqS3ID) && userInfo.get(reqS3ID) != null) {
                FSIdentity user = userInfo.get(reqS3ID);
                uidAndGid[0] = user.getUid();
                uidAndGid[1] = user.getGid();
            }
        } catch (Exception e) {
            log.error("get uid and gid error, map:{}, userInfo: {}", userInfo.get(reqS3ID), userInfo, e);
        }

        return uidAndGid;
    }

    public static FSIdentity getFsIdentity(String reqS3ID) {
        FSIdentity identity = userInfo.get(ADMIN_S3ID);
        try {
            if (userInfo.containsKey(reqS3ID) && userInfo.get(reqS3ID) != null) {
                identity = userInfo.get(reqS3ID);
            }
        } catch (Exception e) {
            log.error("get fs identity error, map:{}, reqS3Id: {}", userInfo.get(reqS3ID), reqS3ID, e);
        }

        return identity;
    }

    /**
     * 获取用户合用户组id
     *
     * @param metaData 对象元数据
     * @return [uid, gid]
     */
    public static int[] getUidAndGid(MetaData metaData) {
        int[] uidAndGid = {0, 0};
        if (StringUtils.isNotBlank(metaData.sysMetaData)) {
            Map<String, String> sysMetaMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {
            });
            uidAndGid = ACLUtils.getUidAndGid(sysMetaMap.getOrDefault("owner", "0"));
        }
        return uidAndGid;
    }

    public static Set<Integer> getGids(String s3Id) {
        return s3IDToGids.get(s3Id);
    }

    /**
     * @param opt          请求的操作
     * @param identity     请求的身份(若访问旧版本数据可能经过权限提升)
     * @param isIdtPromote 请求的身份是否经过了权限提升
     * @param srcHeader    请求头
     **/
    public static RpcCallHeader createNfsHeader(int opt, FSIdentity identity, boolean isIdtPromote, RpcCallHeader srcHeader) {
        RpcCallHeader reqHeader = new RpcCallHeader(null);
        AuthUnix auth = new AuthUnix();
        auth.flavor = 1;

        auth.setUid(identity.getUid());
        auth.setGid(identity.getGid());

        if (isIdtPromote) {
            //访问旧数据，账户权限提升的情况下不使用请求头中的gids
            //s3进来的请求不会携带gid，应当把gid设置为reqID对应的gid；如果不存在组，按nfs的方式，把uid当成gid
            Set<Integer> gids = s3IDToGids.get(identity.getS3Id());
            if (gids == null) {
                auth.setGidN(1);
                auth.setGids(new int[]{identity.getGid()});
            } else {
                auth.setGidN(gids.size());
                auth.setGids(gids.stream().mapToInt(Integer::intValue).toArray());
            }
        } else {
            if (srcHeader.auth.flavor == 1) {
                int[] gids = ((AuthUnix) (srcHeader.auth)).getGids();
                auth.setGids(gids);
            }
        }

        reqHeader.auth = auth;
        reqHeader.opt = opt;
        return reqHeader;
    }

    public static RpcCallHeader createCallHeader(int opt, FSIdentity identity) {
        RpcCallHeader reqHeader = new RpcCallHeader(null);
        AuthUnix auth = new AuthUnix();
        auth.flavor = 1;

        auth.setUid(identity.getUid());
        auth.setGid(identity.getGid());
        //todo s3进来的请求不会携带gid，应当把gid设置为reqID对应的gid；如果不存在组，按nfs的方式，把uid当成gid
        Set<Integer> gids = s3IDToGids.get(identity.getS3Id());
        if (gids == null) {
            auth.setGidN(1);
            auth.setGids(new int[]{identity.getGid()});
        } else {
            auth.setGidN(gids.size());
            auth.setGids(gids.stream().mapToInt(Integer::intValue).toArray());
        }
        reqHeader.auth = auth;
        reqHeader.opt = opt;
        return reqHeader;
    }

    /**
     * 获取inode.objAcl中记录的owner，若没有对应的owner，会返回匿名帐户
     **/
    public static String getInodeOwner(Inode inode) {
        String inoS3Id = null;
        try {
            if (StringUtils.isBlank(inode.getObjAcl())) {
                inoS3Id = NFSBucketInfo.isExistBucketOwner(inode.getBucket()) ?
                        NFSBucketInfo.getBucketOwner(inode.getBucket()) : DEFAULT_USER_ID;
            } else {
                Map<String, String> metaAclMap = Json.decodeValue(inode.getObjAcl(), new TypeReference<Map<String, String>>() {
                });
                inoS3Id = metaAclMap.getOrDefault("owner", DEFAULT_USER_ID);
            }
        } catch (Exception e) {
            inoS3Id = DEFAULT_USER_ID;
        }

        return inoS3Id;
    }

    /**
     * 获取inode.objAcl中记录的owner，若没有对应的owner，会返回空值
     **/
    public static String getRealInodeOwner(Inode inode) {
        String inoS3Id = null;
        try {
            if (StringUtils.isNotBlank(inode.getObjAcl())) {
                Map<String, String> metaAclMap = Json.decodeValue(inode.getObjAcl(), new TypeReference<Map<String, String>>() {
                });
                inoS3Id = metaAclMap.getOrDefault("owner", DEFAULT_USER_ID);
            }
        } catch (Exception e) {
        }

        return inoS3Id;
    }

    /**
     * 【cifs、s3端使用】检查当前请求的账户身份，与被检查的旧版本 inode是否为拥有者关系
     * 设请求者身份为  reqUid、reqS3Id
     * inode 身份为  inoUid、inoS3Id
     * 存在以下四种情况：
     * 1) reqUid == inoUid、reqS3Id == inoS3Id  -- 视请求者【为】inode的拥有者
     * 2) reqUid != inoUid、reqS3Id != inoS3Id  -- 视请求者【不为】inode的拥有者
     * 3) reqUid != inoUid、reqS3Id == inoS3Id
     * a. 【reqUid == 0 管理员访问，无视所有权限】   -- 视请求者【为】inode的拥有者
     * b. 65534 > reqUid > 0 (reqUid != 65534)，inoUid = 0
     * - inode 原为 nfs端root创建、或cifs端创建、或ftp端创建、或s3端上传被转为文件格式 -- 视请求者【为】inode的拥有者
     * - inode 原为旧版本 s3匿名账户上传的对象，格式转为文件后，uid默认等于0，但s3Owner仍然是匿名账户，此时应当视匿名账户【为】inode的拥有者
     * c. 65534 > reqUid > 0 (reqUid != 65534)，inoUid > 0
     * - inode 原为旧版本 nfs端uid=1001创建(由于旧版本统一s3Owner为桶拥有者)，新版本 uid=1002配置了桶拥有者，此时uid=1002 访问
     * 当前文件，实为两个uid所创建，不应予以拥有者权限 -- 视请求者【不为】inode的拥有者
     * d. reqUid = 65534， inoUid = 65534，无需特殊处理，不属于checkNewInode检查后的旧 inode
     * 4) reqUid == inoUid, reqS3Id != inoS3Id
     * a. 【reqUid == 0】 -- 管理员访问，无视权限，仅nfs端存在访问uid=0的情况，其余协议若未配置账户，则默认均为 65534
     * b. 65534 > reqUid > 0
     * - inode 原为旧版本 nfs端uid=1001创建(由于旧版本统一s3Owner为桶拥有者)，新版本 uid=1001配置了非桶拥有者的账户，此时uid=1001
     * 访问当前文件，实为同一个uid所创建，应予以拥有者权限 -- 视请求者【为】inode的拥有者
     * c. reqUid = 65534， inoUid = 65534，无需特殊处理，不属于checkNewInode检查后的旧 inode
     *
     * @param inode   待判断的inode
     * @param reqS3Id 请求账户的uid
     **/
    public static boolean isOldInodeOwner(Inode inode, String reqS3Id) {
        boolean res = false;
        //cifs或s3端用匿名账户访问的inode也为匿名账户创建时无需提升权限
        boolean isAnonymousReq = DEFAULT_USER_ID.equals(reqS3Id) && inode.getUid() == DEFAULT_ANONY_UID;
        //检查请求的moss账户是否已经配置uid的账户，若没有，则不做权限提升处理，即不认定为旧文件的所有者
        boolean isReqS3IdMatch = !isAnonymousReq && ACLUtils.userInfo.containsKey(reqS3Id)
                && ACLUtils.userInfo.get(reqS3Id).getUid() != 0;
        if (isReqS3IdMatch) {
            int inoUid = inode.getUid();
            String inoS3Id = getInodeOwner(inode);
            FSIdentity identity = ACLUtils.userInfo.get(reqS3Id);
            int reqUid = identity.getUid();
            if (reqUid == inoUid && reqS3Id.equals(inoS3Id)) {
                res = true;
            } else if (reqUid != inoUid && !reqS3Id.equals(inoS3Id)) {
                res = false;
            } else if (reqUid != inoUid && reqS3Id.equals(inoS3Id)) {
                if (inoUid == 0 && reqUid > 0) {
                    res = true;
                } else if (inoUid > 0 && reqUid > 0) {
                    res = false;
                }
            } else {
                res = true;
            }
        }

        return res;
    }

    /**
     * 【nfs端使用】检查当前请求的账户身份，与被检查的旧版本 inode是否为拥有者关系
     * 设请求者身份为  reqUid、reqS3Id
     * inode 身份为  inoUid、inoS3Id
     * 存在以下四种情况：
     * 1) reqUid == inoUid、reqS3Id == inoS3Id  -- 视请求者【为】inode的拥有者
     * 2) reqUid != inoUid、reqS3Id != inoS3Id  -- 视请求者【不为】inode的拥有者
     * 3) reqUid != inoUid、reqS3Id == inoS3Id
     * a. 【reqUid == 0 管理员访问，无视所有权限】   -- 视请求者【为】inode的拥有者
     * b. 65534 > reqUid > 0 (reqUid != 65534)，inoUid = 0
     * - inode 原为 nfs端root创建、或cifs端创建、或ftp端创建、或s3端上传被转为文件格式 -- 视请求者【为】inode的拥有者
     * - inode 原为旧版本 s3匿名账户上传的对象，格式转为文件后，uid默认等于0，但s3Owner仍然是匿名账户，此时应当视匿名账户【为】inode的拥有者
     * c. 65534 > reqUid > 0 (reqUid != 65534)，inoUid > 0
     * - inode 原为旧版本 nfs端uid=1001创建(由于旧版本统一s3Owner为桶拥有者)，新版本 uid=1002配置了桶拥有者，此时uid=1002 访问
     * 当前文件，实为两个uid所创建，不应予以拥有者权限 -- 视请求者【不为】inode的拥有者
     * d. reqUid = 65534， inoUid = 65534，无需特殊处理，不属于checkNewInode检查后的旧 inode
     * 4) reqUid == inoUid, reqS3Id != inoS3Id
     * a. 【reqUid == 0】 -- 管理员访问，无视权限，仅nfs端存在访问uid=0的情况，其余协议若未配置账户，则默认均为 65534
     * b. 65534 > reqUid > 0
     * - inode 原为旧版本 nfs端uid=1001创建(由于旧版本统一s3Owner为桶拥有者)，新版本 uid=1001配置了非桶拥有者的账户，此时uid=1001
     * 访问当前文件，实为同一个uid所创建，应予以拥有者权限 -- 视请求者【为】inode的拥有者
     * c. reqUid = 65534， inoUid = 65534，无需特殊处理，不属于checkNewInode检查后的旧 inode
     *
     * @param inode  待判断的inode
     * @param reqUid 请求账户的uid
     **/
    public static boolean isOldInodeOwner(Inode inode, int reqUid) {
        boolean res = false;
        boolean isRootReq = reqUid == 0;
        //nfs端匿名账户请求的inode也为匿名时无需提升权限
        boolean isAnonymousReq = reqUid == DEFAULT_ANONY_UID && inode.getUid() == 65534;
        //检查请求的uid是否已经配置moss账户，若没有，则不做权限提升处理，即不认定为旧文件的所有者
        boolean isReqUidMatch = !isRootReq && !isAnonymousReq && uidToS3ID.containsKey(reqUid);
        if (isReqUidMatch) {
            String inoS3Id = getInodeOwner(inode);
            int inoUid = inode.getUid();
            String reqS3Id = ACLUtils.uidToS3ID.get(reqUid);
            if (reqUid == inoUid && reqS3Id.equals(inoS3Id)) {
                res = true;
            } else if (reqUid != inoUid && !reqS3Id.equals(inoS3Id)) {
                res = false;
            } else if (reqUid != inoUid && reqS3Id.equals(inoS3Id)) {
                if (inoUid == 0 && reqUid > 0) {
                    res = true;
                } else if (inoUid > 0 && reqUid > 0) {
                    res = false;
                }
            } else {
                res = true;
            }
        }

        return res;
    }

    /**
     * 在cifs端使用该函数获取身份
     **/
    public static FSIdentity getIdentityByS3Id(String s3Id) {
        FSIdentity identity = userInfo.get(s3Id);
        if (null == identity || identity.getUid() == 0) {
            //正常情况下，userInfo中查不到s3Id，说明这个s3Id还没有配置文件账户
            return new FSIdentity(s3Id, DEFAULT_ANONY_UID, DEFAULT_ANONY_GID, FSIdentity.getUserSIDByUid(DEFAULT_ANONY_UID), FSIdentity.getGroupSIDByGid(DEFAULT_ANONY_GID));
        } else {
            return identity;
        }
    }

    /**
     * 根据请求的s3Id获取用于判断权限的请求者身份，并根据inode是否为旧版本文对请求者身份进行权限的提升
     *
     * @param inode   待判断是否要提升请求者权限的 inode
     * @param reqS3Id 请求者的 s3Id
     * @return 返回一个经过权限提升的请求者身份
     **/
    public static FSIdentity getIdentityByS3IDAndInode(Inode inode, String reqS3Id) {
        FSIdentity identity = getIdentityByS3Id(reqS3Id);
        if (checkNewInode(inode)) {
            //不需要进行权限的提升
            return identity;
        } else {
            //需要进行权限的提升；uid=65534 不需要提升，inode.uid不可能为65534
            //检查当前账户与待请求的inode，是否有uid或者s3Id上的一致，若有一个一致，则表示该文件为此前同一个账户所创建，可以被读取
            if (isOldInodeOwner(inode, reqS3Id)) {
                FSIdentity proIdt = identity.cloneIdentity();
                proIdt.setS3Id(reqS3Id)
                        .setUid(inode.getUid())
                        .setUserSid(FSIdentity.getUserSIDByUid(inode.getUid()));
                return proIdt;
            } else {
                return identity;
            }
        }
    }

    /**
     * 根据请求的s3Id获取用于判断权限的请求者身份，并根据inode是否为旧版本文对请求者身份进行权限的提升
     *
     * @param inode 待判断是否要提升请求者权限的 inode
     * @return 返回一个经过权限提升的请求者身份
     **/
    public static FSIdentity getIdentityByUidAndInode(Inode inode, RpcCallHeader callHeader) {
        int[] uidAndGid = ACLUtils.getUidAndGid(callHeader);
        int reqUid = uidAndGid[0];
        int reqGid = uidAndGid[1];
        FSIdentity identity = null;
        if (!uidToS3ID.containsKey(reqUid)) {
            identity = new FSIdentity(DEFAULT_USER_ID, reqUid, reqGid, FSIdentity.getUserSIDByUid(reqUid), FSIdentity.getGroupSIDByGid(reqGid));
        } else {
            //root账户对应的s3Id是"0"
            String reqS3Id = ACLUtils.getS3IdByUid(reqUid);
            identity = ACLUtils.userInfo.get(reqS3Id);
        }

        if (checkNewInode(inode)) {
            //不需要进行权限的提升
            return identity;
        } else {
            //【宗旨】nfs权限开以后，nfs端如果uid没有配置moss账户，s3Owner的身份就应该是匿名，此时不允许访问其它uid或者已有
            // moss账户创建的数据，但后续uid配置moss账户后，则可以访问之前没有配置uid时创建的数据
            //【只要配置账户前后，uid或者s3Id有一个相等，则允许其继续访问】
            //这样处理可能会导致，nfs端旧版本root创建的文件，能够在新版本配置uid之后，被桶拥有者对应的uid控制
            if (isOldInodeOwner(inode, reqUid)) {
                FSIdentity proIdt = identity.cloneIdentity();
                proIdt.setUid(inode.getUid())
                        .setUserSid(FSIdentity.getUserSIDByUid(inode.getUid()));
                return proIdt;
            } else {
                return identity;
            }
        }
    }

    /**
     * 根据请求的s3Id获取用于判断权限的请求者身份，并根据inode是否为旧版本文对请求者身份进行权限的提升
     *
     * @param inode 待判断是否要提升请求者权限的 inode
     * @return 返回一个经过权限提升的请求者身份
     **/
    public static Tuple2<FSIdentity, Boolean> getIdtAndJudgeByUidAndInode(Inode inode, RpcCallHeader callHeader) {
        int[] uidAndGid = ACLUtils.getUidAndGid(callHeader);
        int reqUid = uidAndGid[0];
        int reqGid = uidAndGid[1];
        FSIdentity identity = null;
        if (!uidToS3ID.containsKey(reqUid)) {
            identity = new FSIdentity(DEFAULT_USER_ID, reqUid, reqGid, FSIdentity.getUserSIDByUid(reqUid), FSIdentity.getGroupSIDByGid(reqGid));
        } else {
            //root账户对应的s3Id是"0"
            String reqS3Id = ACLUtils.getS3IdByUid(reqUid);
            identity = ACLUtils.userInfo.get(reqS3Id);
        }

        if (checkNewInode(inode)) {
            //不需要进行权限的提升
            return new Tuple2<>(identity, false);
        } else {
            //【宗旨】nfs权限开以后，nfs端如果uid没有配置moss账户，s3Owner的身份就应该是匿名，此时不允许访问其它uid或者已有
            // moss账户创建的数据，但后续uid配置moss账户后，则可以访问之前没有配置uid时创建的数据
            //【只要配置账户前后，uid或者s3Id有一个相等，则允许其继续访问】
            //这样处理可能会导致，nfs端旧版本root创建的文件，能够在新版本配置uid之后，被桶拥有者对应的uid控制
            if (isOldInodeOwner(inode, reqUid)) {
                FSIdentity proIdt = identity.cloneIdentity();
                proIdt.setUid(inode.getUid())
                        .setUserSid(FSIdentity.getUserSIDByUid(inode.getUid()));
                return new Tuple2<>(proIdt, true);
            } else {
                return new Tuple2<>(identity, false);
            }
        }
    }

    public static Mono<Map<String, String>> getUserInfo(MetaData metaData) {
        Map<String, String> objAclMap = (Map<String, String>) Json.decodeValue(metaData.getObjectAcl(), Map.class);
        String ownerID = objAclMap.get("owner");
        return pool.getSharedConnection(REDIS_USERINFO_INDEX).reactive().hgetall(ownerID);
    }

    public static Mono<Map<String, String>> getUserInfo(String accountID) {
        return pool.getSharedConnection(REDIS_USERINFO_INDEX).reactive().hgetall(accountID);
    }

    /**
     * 为处理升级，旧版本的元数据记录的uid和owner并不正确；若新版本配置uid，开启权限后，将无法读取旧版本创建的目录和文件；
     * 因此为了能够使权限开启后，新的配置了moss账户的uid或者配置了uid的moss账户能够继续访问此前创建的数据，在判断时将请求者身份
     * 临时提升为文件或目录的拥有者
     * <p>
     * 考虑在创建文件的uid 配置了moss账户 或者 创建文件的s3Id配置了uid之后，常见的inode增加一个新的标志位字段，用于和未配置时的情况区分
     * 根目录权限判断时不做身份的处理，根目录inode中也不额外增加标志位，因为默认就是777
     *
     * @return 是否是旧版本创建的inode
     * false 是旧版本创建的inode，意味着访问时候需要提升请求者的权限
     * true  不是旧版本创建的inode，访问时候不需要提升请求者的权限
     **/
    public static boolean checkNewInode(Inode inode) {
        long nodeId = inode.getNodeId();
        //根目录权限全开，拥有者无法改变，不需要提升权限
        if (nodeId == 1) {
            return true;
        }

        int uid = inode.getUid();
        int aclTag = inode.getAclTag();

        boolean res = false;
        if (aclTag == FsConstants.ACLConstants.START) {
            //含有标志位不需要提升权限
            res = true;
        } else if (aclTag == 0 && uid == 65534) {
            //文件是匿名用户创建的，即s3或者cifs匿名用户或nfs squash映射下创建的，无需提升权限
            res = true;
        }

        return res;
    }

    /**
     * 提升请求者的权限至文件的拥有者
     **/
    public static FSIdentity promoteAccessByUid(Inode inode, int uid, FSIdentity identity) {
        boolean needPromote = checkNewInode(inode);
        if (needPromote) {
            FSIdentity idt = identity.cloneIdentity();

        }

        return identity;
    }

    public static Mono<Boolean> updateRootACL(String bucket, Inode root, boolean changeMode) {
        MonoProcessor<Boolean> res = MonoProcessor.create();

        try {
            if (null != root.getACEs()) {
                RedisConnPool.getInstance().getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket, "ACE", Json.encode(root.getACEs()));
            }

            if (changeMode) {
                RedisConnPool.getInstance().getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket, "mode", String.valueOf(root.getMode()));
            }

            StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
            String bucketVnode = pool.getBucketVnodeId(bucket);
            List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();

            List<SocketReqMsg> msgs = nodeList.stream()
                    .map(tuple -> new SocketReqMsg("", 0)
                            .put("bucket", bucket))
                    .collect(Collectors.toList());

            ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, UPDATE_ROOT_ACL, String.class, nodeList);

            Disposable subscribe = responseInfo.responses.subscribe(s -> {
            }, e -> {
                log.error("update bucket:{} root ACE:{} error", bucket, root.getACEs(), e);
                res.onNext(false);
            }, () -> {
                res.onNext(true);
            });
        } catch (Exception e) {
            log.error("update root acl error ", e);
            res.onNext(false);
        }

        return res;
    }

    /**
     * 更新主节点桶信息中的根目录信息，再发请求至相关节点更新根节点的缓存
     **/
    public static Mono<Boolean> updateRootMode(String bucket, Inode root) {
        MonoProcessor<Boolean> res = MonoProcessor.create();

        try {
            Map<String, String> map = new HashMap<>();
            map.put("mode", String.valueOf(root.getMode()));

            if (null != root.getACEs() && !root.getACEs().isEmpty()) {
                map.put("ACE", Json.encode(root.getACEs()));
            }

            RedisConnPool.getInstance().getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hmset(bucket, map);

            StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucket);
            String bucketVnode = pool.getBucketVnodeId(bucket);
            List<Tuple3<String, String, String>> nodeList = pool.mapToNodeInfo(bucketVnode).block();

            List<SocketReqMsg> msgs = nodeList.stream()
                    .map(tuple -> new SocketReqMsg("", 0)
                            .put("bucket", bucket))
                    .collect(Collectors.toList());

            ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgs, UPDATE_ROOT_ACL, String.class, nodeList);

            Disposable subscribe = responseInfo.responses.subscribe(s -> {
            }, e -> {
                log.error("update bucket:{} root mode:{} error", bucket, root.getMode(), e);
                res.onNext(false);
            }, () -> {
                res.onNext(true);
            });
        } catch (Exception e) {
            log.error("update root mode error ", e);
            res.onNext(false);
        }

        return res;
    }

    public static Mono<Payload> reloadRootCache(Payload payload) {
        SocketReqMsg msg = Json.decodeValue(payload.getDataUtf8(), SocketReqMsg.class);
        String bucket = msg.get("bucket");

        return InodeUtils.getAndPutRootInodeReactive(bucket, true)
                .map(i -> SUCCESS_PAYLOAD)
                .onErrorReturn(ERROR_PAYLOAD);
    }

    /**
     * 检查moss账户是否配置了
     **/
    public static boolean isUserMatchUid() {
        return false;
    }

    /**
     * 仅配置moss账户的uid或者配置uid的moss账户创建时才会设置aclTag=1
     *
     * @param inode 待判断的inode
     * @param uid   请求创建inode的账户
     **/
    public static void setNfsInodeAclTag(Inode inode, int uid, ReqInfo reqHeader) {
        //请求的账户已经配置了uid或者moss账户，则创建inode时设置标志位
        boolean isMatch = false;

        if (uid == 0) {
            //检查对应的桶拥有者是否配置了其它uid，如果有配置，说明是nfs端root账户创建；如果没有配置，说明是配置moss账户没有配置uid时候创建
            String bucketOwner = reqHeader.bucketInfo.get(BUCKET_USER_ID);
            if (userInfo.containsKey(bucketOwner) && userInfo.get(bucketOwner) != null) {
                FSIdentity user = userInfo.get(bucketOwner);
                int recordUid = user.getUid();
                if (recordUid != 0) {
                    isMatch = true;
                }
            }
        } else if (uid == DEFAULT_ANONY_UID) {
            //开启权限后默认squash创建的文件
            isMatch = false;
        } else if (uid > 0 && uid < DEFAULT_ANONY_UID) {
            //缓存中有uid的记录，且uid对应的s3Id不是匿名s3账户
            boolean exist = uidToS3ID.containsKey(uid) && !uidToS3ID.get(uid).equals(DEFAULT_USER_ID);
            if (exist) {
                isMatch = true;
            }
        }

        if (isMatch) {
            inode.setAclTag(FsConstants.ACLConstants.START);
        }
    }

    /**
     * 仅配置moss账户的uid或者配置uid的moss账户创建时才会设置aclTag=1
     *
     * @param inode 待判断的inode
     * @param s3Id  请求创建inode的账户
     **/
    public static void setCifsInodeAclTag(Inode inode, String s3Id) {
        boolean isMatch = false;
        if (userInfo.containsKey(s3Id) && userInfo.get(s3Id) != null) {
            FSIdentity user = userInfo.get(s3Id);
            int recordUid = user.getUid();
            if (recordUid != 0) {
                isMatch = true;
            }
        }

        if (isMatch) {
            inode.setAclTag(FsConstants.ACLConstants.START);
        }
    }

    /**
     * 设置inode的属主与属组
     **/
    public static String setInodeIdAndS3Id(Inode inode, RpcCallHeader[] callHeaders, Map<String, String> parameter, ReqInfo reqHeader) {
        String[] s3Account = {null};

        //nfs端及s3不存在元数据的目录转为文件格式时通过callHeader设置拥有者
        if (callHeaders.length > 0) {
            if (callHeaders[0] != null && null != callHeaders[0].auth && callHeaders[0].auth.flavor == 1) {
                int uid = ((AuthUnix) callHeaders[0].auth).getUid();
                int gid = ((AuthUnix) callHeaders[0].auth).getGid();
                if (uid > 0) {
                    s3Account[0] = ACLUtils.getS3IdByUid(uid);
                }

                setNfsInodeAclTag(inode, uid, reqHeader);
                inode.setUid(uid);
                inode.setGid(gid);
            }
        }

        //cifs端创建时设置文件拥有者
        if (null != parameter && StringUtils.isNotBlank(parameter.get(CIFS_CREATE))) {
            String s3Id = parameter.get(CIFS_CREATE);
            //修正uid、gid
            int[] uidAndGid = ACLUtils.getUidAndGid(s3Id);
            inode.setUid(uidAndGid[0]);
            inode.setGid(uidAndGid[1]);
            s3Account[0] = s3Id;
            setCifsInodeAclTag(inode, s3Id);
            if (CIFSACL.cifsACL) {
                log.info("create: name: {}, uid: {}, gid: {}", inode.getObjName(), uidAndGid[0], uidAndGid[1]);
            }
        }

        // todo 根据uid获取账户id 设置objACL, 文件端创建的文件默认对象ACL为公共读写
        if (StringUtils.isBlank(s3Account[0])) {
            //root账户创建的文件owner即为桶所有者
            s3Account[0] = reqHeader.bucketInfo.get(BUCKET_USER_ID);
        }

        return s3Account[0];
    }

    /**
     * 对象格式转为文件格式时设置拥有者和权限，对象上传的数据，objectAcl中的owner与sysMeta中的owner一定是一致的所以无需处理
     **/
    public static void setIdWhenCreateS3Inode(Inode inode, MetaData metaData, String dirDefACEs) {
        Map<String, String> objAclMap = (Map<String, String>) Json.decodeValue(metaData.getObjectAcl(), Map.class);
        String ownerID = objAclMap.get("owner");
        int[] uidAndGid = ACLUtils.getUidAndGid(ownerID);
        inode.setUid(uidAndGid[0]);
        inode.setGid(uidAndGid[1]);

        //如果owner是default user，那么无需设置；如果owner不是default user
        //如果owner有配uid，那么uid不可能是0；
        if (!DEFAULT_USER_ID.equals(ownerID)) {
            if (uidAndGid[0] != 0) {
                inode.setAclTag(FsConstants.ACLConstants.START);
            }
        }

        //对象数据转为文件格式默认ugo设置为777
        inode.setMode(metaData.getKey().endsWith("/") ? (S_IFDIR | OBJECT_TRANS_FILE_MODE) : (S_IFREG | OBJECT_TRANS_FILE_MODE));

        inode.setObjAcl(metaData.getObjectAcl());
        if (StringUtils.isNotBlank(dirDefACEs)) {
            List<Inode.ACE> curAcl = Json.decodeValue(dirDefACEs, new TypeReference<List<Inode.ACE>>() {
            });

            if (CIFSACL.isExistCifsACE(curAcl)) {
                CIFSACL.updateUGOACE(curAcl, inode);
            } else {
                //修正目录default ace中的id为当前文件拥有者的id
                curAcl = ACLUtils.fixNfsDefAceId(curAcl, inode.getUid(), inode.getGid());

                //存在继承权限，且是s3目录转换而来，则应当将ugo以mode来替代
                curAcl = NFSACL.removeUGOACE(curAcl, inode);
            }

            inode.setACEs(curAcl);
        }

        //去除metaData中的objAcl
        metaData.setObjectAcl(null);
    }

    /**
     * uid改变时，同步修改对象ACL，清除原本对象acl中所有的owner相关权限，同时将owner改为attr.uid对应的s3Id
     *
     * @param inode     元数据
     * @param oriUid    原本的uid
     * @param changeUid 待更改为的uid
     **/
    public static void updateObjAcl(Inode inode, int oriUid, int changeUid, String bucketOwner) {
        try {
            if (oriUid != changeUid) {
                String objAcl = inode.getObjAcl();
                if (StringUtils.isNotBlank(objAcl)) {
                    Map<String, String> objAclMap = Json.decodeValue(objAcl, new TypeReference<Map<String, String>>() {
                    });

                    if (null != objAclMap && !objAclMap.isEmpty()) {
                        int ownerACL = Integer.parseInt(objAclMap.get("acl"));
                        //根据uid获取s3Id
                        String s3Id = ACLUtils.getS3IdByUid(changeUid);
                        if (ADMIN_S3ID.equals(s3Id)) {
                            s3Id = bucketOwner;
                        }

                        //用于记录对象acl中每个权限类型的存在个数，仅当删减为0时，才会减去acl数值
                        int[] numArr = {0, 0, 0, 0};
                        //分别对应四种权限是否要移除acl，仅当发生过移除，且该种权限总个数为1才会减去acl数值
                        boolean[] removeArr = {false, false, false, false};
                        int[] permissionArr = {OBJECT_PERMISSION_READ_NUM, OBJECT_PERMISSION_READ_CAP_NUM, OBJECT_PERMISSION_WRITE_CAP_NUM, OBJECT_PERMISSION_FULL_CON_NUM};
                        Iterator<Map.Entry<String, String>> iterator = objAclMap.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<String, String> entry = iterator.next();
                            if (!"owner".equals(entry.getKey()) && !"acl".equals(entry.getKey()) && !"bucketName".equals(entry.getKey()) && !"keyName".equals(entry.getKey())) {
                                String aceOwner = entry.getKey().split("-")[1];
                                String permission = entry.getValue();
                                int index = -1;
                                switch (permission) {
                                    case OBJECT_PERMISSION_READ:
                                        index = 0;
                                        break;
                                    case OBJECT_PERMISSION_READ_CAP:
                                        index = 1;
                                        break;
                                    case OBJECT_PERMISSION_WRITE_CAP:
                                        index = 2;
                                        break;
                                    case OBJECT_PERMISSION_FULL_CON:
                                        index = 3;
                                        break;
                                    default:
                                        break;
                                }

                                if (index >= 0) {
                                    numArr[index]++;
                                    if (s3Id.equals(aceOwner)) {
                                        iterator.remove();
                                        removeArr[index] = true;
                                    }
                                }
                            }
                        }

                        for (int i = 0; i < numArr.length; i++) {
                            int num = numArr[i];
                            if (num == 1 && removeArr[i]) {
                                ownerACL -= permissionArr[i];
                            }
                        }

                        objAclMap.put("owner", s3Id);
                        objAclMap.put("acl", String.valueOf(ownerACL));
                        inode.setObjAcl(Json.encode(objAclMap));
                        updateAclTag(inode, changeUid, s3Id);
                    }
                } else {
                    //objAcl为空，说明是未合入权限功能时的旧数据，应为旧数据增加新的objAcl
                    //旧数据一律视为对象acl 私有读写
                    JsonObject aclJson = new JsonObject();
                    aclJson.put("acl", String.valueOf(OBJECT_PERMISSION_PRIVATE_NUM));

                    //根据uid获取s3Id
                    String s3Id = ACLUtils.getS3IdByUid(changeUid);

                    if (ADMIN_S3ID.equals(s3Id)) {
                        s3Id = bucketOwner;
                    }

                    aclJson.put("owner", s3Id);
                    inode.setObjAcl(aclJson.encode());
                    updateAclTag(inode, changeUid, s3Id);
                }
            }
        } catch (Exception e) {
            log.error("update objAcl error: ino: {}, obj: {}, linkN: {}, {}->{}", inode.getNodeId(), inode.getObjName(), inode.getLinkN(), oriUid, changeUid, e);
        }
    }

    /**
     * 如果旧版本拥有者不匹配的文件重新被修改拥有者，则如果被修改的拥有者已经有匹配的账户，则aclTag设置为1
     *
     * @param inode
     * @param changeUid  待更新为的uid
     * @param changeS3Id 待更新为的s3Id
     **/
    public static void updateAclTag(Inode inode, int changeUid, String changeS3Id) {
        if (inode.getAclTag() == 0) {
            if (changeUid == 0) {
                //要改成root所有，则看bucketOwner是否有匹配的uid
                if (userInfo.containsKey(changeS3Id) && userInfo.get(changeS3Id) != null
                        && userInfo.get(changeS3Id).getUid() != 0) {
                    inode.setAclTag(ACLConstants.START);
                }
            } else if (changeUid > 0 && changeUid < DEFAULT_ANONY_UID) {
                if (!DEFAULT_USER_ID.equals(changeS3Id)) {
                    inode.setAclTag(ACLConstants.START);
                }
            }
        }
    }

    /**
     * 在创建inode时根据继承权限设置 acl
     **/
    public static Inode updateDefACL(Inode inode, Map<String, String> parameter) {
        try {
            if (null != parameter) {
                String name = inode.getObjName();

                //创建时父目录中ACE非空则会传递该参数
                String ACEs = parameter.get(NFS_ACE);
                if (StringUtils.isNotBlank(ACEs)) {
                    //存在继承权限
                    List<Inode.ACE> curAcl = Json.decodeValue(ACEs, new TypeReference<List<Inode.ACE>>() {
                    });

                    if (!curAcl.isEmpty()) {
                        List<Inode.ACE> saveACEs = new LinkedList<>();
                        if (CIFSACL.isExistNfsACE(curAcl)) {
                            //如果acl列表中都是nfs，则继承是以nfs规则继承
                            saveACEs = ACLUtils.pickNFSDefACL(curAcl, name, inode);
                            if (null != saveACEs && !saveACEs.isEmpty()) {
                                //存在继承权限，且是s3目录转换而来，则应当将ugo以mode来替代
                                saveACEs = NFSACL.removeUGOACE(saveACEs, inode);
                                inode.setACEs(saveACEs);
                            }
                        } else {
                            //如果acl列表中都是cifs，则继承时以cifs规则继承
                            boolean isDir = false;
                            if (StringUtils.isNotBlank(name)) {
                                isDir = name.endsWith("/");
                            }

                            //分别存储要修正的user、group和other权限
                            long[] allowAccess = {-1, -1, -1};
                            long[] deniedAccess = {-1, -1, -1};

                            //获取inode对应的属主和属组
                            String inoOwnerSID = FSIdentity.getUserSIDByUid(inode.getUid());
                            String inoGroupSID = FSIdentity.getGroupSIDByGid(inode.getGid());

                            for (Inode.ACE dirACE : curAcl) {
                                short dirFlag = dirACE.getFlag();
                                boolean inheritDir = (dirFlag & CONTAINER_INHERIT_ACE) != 0;
                                boolean inheritFile = (dirFlag & OBJECT_INHERIT_ACE) != 0;

                                //非继承权限直接跳过
                                if (!inheritDir && !inheritFile) {
                                    continue;
                                }

                                short setInheritFlag = 0;

                                if (isDir) {
                                    //具备目录继承权限
                                    //创建的是目录，保留原本的继承权限，同时将继承权限转为普通权限存储
                                    setInheritFlag |= (inheritDir ? CONTAINER_INHERIT_ACE : 0);
                                    setInheritFlag |= (inheritFile ? OBJECT_INHERIT_ACE : 0);
                                    setInheritFlag |= INHERIT_ONLY_ACE;

                                    if (inheritDir) {
                                        //转为普通权限ACE
                                        Inode.ACE selfACE = new Inode.ACE(dirACE.getCType(), (short) 0, dirACE.getSid(), dirACE.getMask());
                                        saveACEs.add(selfACE);
                                        statisticAccess(allowAccess, deniedAccess, selfACE, inoOwnerSID, inoGroupSID);
                                    }

                                    //保留继承权限
                                    Inode.ACE inheritACE = new Inode.ACE(dirACE.getCType(), setInheritFlag, dirACE.getSid(), dirACE.getMask());
                                    saveACEs.add(inheritACE);
                                } else {
                                    if (inheritFile) {
                                        //创建的是文件，将继承权限转为普通权限
                                        Inode.ACE selfACE = new Inode.ACE(dirACE.getCType(), (short) 0, dirACE.getSid(), dirACE.getMask());
                                        saveACEs.add(selfACE);
                                        statisticAccess(allowAccess, deniedAccess, selfACE, inoOwnerSID, inoGroupSID);
                                    }
                                }
                            }

                            //根据ace修正mode权限
                            if (allowAccess[0] > -1 || allowAccess[1] > -1 || allowAccess[2] > -1
                                    || deniedAccess[0] > -1 || deniedAccess[1] > -1 || deniedAccess[2] > -1) {
                                for (int i = 0; i < UGO_ACE_ARR.length; i++) {
                                    long curAllow = allowAccess[i];
                                    long curDenied = deniedAccess[i];
                                    int type = UGO_ACE_ARR[i];
                                    if (curAllow > -1 || curDenied > -1) {
                                        int mode = inode.getMode();
                                        int curRight = NFSACL.parseModeToInt(mode, type);
                                        long curAccess = CIFSACL.ugoToCIFSAccess(curRight);
                                        if (curAllow == -1 && curDenied > -1) {
                                            curAccess = 0;
                                        } else if (curAllow > -1 && curDenied == -1) {
                                            curAccess = curAllow;
                                        } else {
                                            curAccess = curAllow - curDenied;
                                            curAccess = curAccess <= 0 ? 0 : curAccess;
                                        }

                                        mode = CIFSACL.refreshModeByCifsAccess(curAccess, mode, type, isDir);
                                        inode.setMode(mode);
                                    }
                                }
                            }

                            if (!saveACEs.isEmpty()) {
                                inode.setACEs(saveACEs);
                            }
                        }

                        if (aclDebug) {
                            log.info("【create】 name: {}, curAcl: {}", name, curAcl);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("update default acl error, obj: {}, parameter: {}", inode.getObjName(), parameter, e);
        }

        return inode;
    }

    /**
     * 统计所有ace中的对应ugo权限的允许权限和拒绝权限
     *
     * @param allowAccess  允许的权限 [-1, -1, -1]
     * @param deniedAccess 拒绝的权限 [-1, -1, -1]
     **/
    public static void statisticAccess(long[] allowAccess, long[] deniedAccess, Inode.ACE cifsAce, String ownerSid, String groupSid) {
        byte cType = cifsAce.getCType();
        String sid = cifsAce.getSid();
        long access = cifsAce.getMask();

        switch (cType) {
            case ACCESS_ALLOWED_ACE_TYPE:
                //无继承权限
                if (ownerSid.equals(sid)) {
                    //对应user权限
                    if (allowAccess[0] == -1) {
                        allowAccess[0] = access;
                    } else {
                        allowAccess[0] |= access;
                    }
                } else if (groupSid.equals(sid)) {
                    //对应group权限
                    if (allowAccess[1] == -1) {
                        allowAccess[1] = access;
                    } else {
                        allowAccess[1] |= access;
                    }
                } else if (SID.EVERYONE.getDisplayName().equals(sid)) {
                    //对应other权限
                    if (allowAccess[2] == -1) {
                        allowAccess[2] = access;
                    } else {
                        allowAccess[2] |= access;
                    }
                }
                break;
            case ACCESS_DENIED_ACE_TYPE:
                if (ownerSid.equals(sid)) {
                    //对应user权限
                    if (deniedAccess[0] == -1) {
                        deniedAccess[0] = access;
                    } else {
                        deniedAccess[0] |= access;
                    }
                } else if (groupSid.equals(sid)) {
                    //对应group权限
                    if (deniedAccess[1] == -1) {
                        deniedAccess[1] = access;
                    } else {
                        deniedAccess[1] |= access;
                    }
                } else if (SID.EVERYONE.getDisplayName().equals(sid)) {
                    //对应other权限
                    if (deniedAccess[2] == -1) {
                        deniedAccess[2] = access;
                    } else {
                        deniedAccess[2] |= access;
                    }
                }
                break;
            case ACCESS_ALLOWED_OBJECT_ACE_TYPE:
            case ACCESS_DENIED_OBJECT_ACE_TYPE:
            case SYSTEM_AUDIT_ACE_TYPE:
            case SYSTEM_ALARM_ACE_TYPE:
            case ACCESS_ALLOWED_COMPOUND_ACE_TYPE:
            case SYSTEM_AUDIT_OBJECT_ACE_TYPE:
            case SYSTEM_ALARM_OBJECT_ACE_TYPE:
            case ACCESS_ALLOWED_CALLBACK_ACE_TYPE:
            case ACCESS_DENIED_CALLBACK_ACE_TYPE:
            case ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE:
            case ACCESS_DENIED_CALLBACK_OBJECT_ACE_TYPE:
            case SYSTEM_AUDIT_CALLBACK_ACE_TYPE:
            case SYSTEM_ALARM_CALLBACK_ACE_TYPE:
            case SYSTEM_AUDIT_CALLBACK_OBJECT_ACE_TYPE:
            case SYSTEM_ALARM_CALLBACK_OBJECT_ACE_TYPE:
            case SYSTEM_MANDATORY_LABEL_ACE_TYPE:
            case SYSTEM_RESOURCE_ATTRIBUTE_ACE_TYPE:
            case SYSTEM_SCOPED_POLICY_ID_ACE_TYPE:
            default:
        }
    }

    /**
     * 修正ace列表中的继承权限的拥有者id，如1001创建的dir目录，default user权限是 rwx，1002创建子目录dir1,其继承权限 uid应该对应1002
     **/
    public static List<Inode.ACE> fixNfsDefAceId(List<Inode.ACE> ACEs, int uid, int gid) {
        if (null == ACEs || ACEs.isEmpty() || !CIFSACL.isExistNfsACE(ACEs)) {
            return null;
        }

        try {
            List<Inode.ACE> curAcl = ACEs;

            Iterator<Inode.ACE> iterator = curAcl.listIterator();
            while (iterator.hasNext()) {
                Inode.ACE ace = iterator.next();
                int type = ace.getNType();
                if (type == DEFAULT_NFSACL_USER_OBJ) {
                    if (ace.getId() != uid) {
                        ace.setId(uid);
                    }
                } else if (type == DEFAULT_NFSACL_GROUP_OBJ) {
                    if (ace.getId() != gid) {
                        ace.setId(gid);
                    }
                }
            }

            return curAcl;
        } catch (Exception e) {
            log.error("fix nfs default ace error: {}, ", ACEs, e);
            return null;
        }
    }

    public static Mono<Integer> notifyOtherNodes0(SocketReqMsg msg, PayloadMetaType metaType) {
        MonoProcessor<Integer> res = MonoProcessor.create();
        AtomicInteger num = new AtomicInteger(0);
        List<String> ipList = RabbitMqUtils.HEART_IP_LIST;
        Flux<Tuple2<String, Boolean>> responses = Flux.empty();

        for (int i = 0; i < ipList.size(); i++) {
            String ip = ipList.get(i);
            Mono<Tuple2<String, Boolean>> response = RSocketClient.getRSocket(ip, BACK_END_PORT)
                    .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(Json.encode(msg), metaType.name())))
                    .timeout(Duration.ofSeconds(30))
                    .map(payload -> {
                        try {
                            String metaDataPayload = payload.getMetadataUtf8();
                            if (metaDataPayload.equalsIgnoreCase(SUCCESS.name())) {
                                num.incrementAndGet();
                                return new Tuple2<>(ip, true);
                            }

                            return new Tuple2<>(ip, false);
                        } finally {
                            payload.release();
                        }
                    })
                    .doOnError(e -> {
                        log.error("notify {} {} error ", ip, metaType.name(), e);
                    })
                    .onErrorReturn(new Tuple2<>(ip, false));

            responses = responses.mergeWith(response);
        }

        responses.collectList().subscribe(t -> {
            if (num.get() > ipList.size() / 2) {
                res.onNext(1);
            } else {
                res.onNext(-1);
            }
        });

        return res;
    }

    public static int notifyOtherNodes(SocketReqMsg msg, PayloadMetaType metaType) {
        int res = -1;
        try {
            res = Mono.just(1L)
                    .publishOn(FS_ACL_SCHEDULER)
                    .flatMap(i -> notifyOtherNodes0(msg, metaType))
                    .block(Duration.ofSeconds(35));
        } catch (Exception e) {
            res = -1;
            log.error("notify error, metaType: {}, msg: {} ", metaType.name(), msg, e);
        }

        return res;
    }

    public static int updateFsIdentiy(String reqMsg) {
        if (StringUtils.isBlank(reqMsg)) {
            return -1;
        }

        int res = -1;
        try {
            SocketReqMsg msg = new SocketReqMsg("", 0);
            msg.put("param", reqMsg.replace("@", "\""));
            res = notifyOtherNodes(msg, UPDATE_FS_IDENTITY);
        } catch (Exception e) {
            res = -1;
            log.error("update fs id error", e);
        }

        return res;
    }

    public static int switchNFSACL(boolean start, String protoType) {
        SocketReqMsg msg = new SocketReqMsg("", 0);
        msg.put("start", String.valueOf(start));
        msg.put(PROTO, protoType);
        int res = notifyOtherNodes(msg, START_NFS_ACL);
        return res;
    }

    public static int adjustMaxId(int maxId) {
        SocketReqMsg msg = new SocketReqMsg("", 0);
        msg.put(FS_ID_MAX, String.valueOf(maxId));
        int res = notifyOtherNodes(msg, ADJUST_FS_ID_RANGE);
        return res;
    }

    public static boolean checkSpecMode(ObjAttr attr) {
        boolean res = false;

        if (attr.hasMode != 0) {
            res = (attr.mode & SPEC_USER) != 0
                    || (attr.mode & SPEC_GROUP) != 0
                    || (attr.mode & SPEC_VTX) != 0;
        }

        return res;
    }

    public static boolean existNFSMask(Inode inode) {
        boolean res = false;
        if (inode.getNodeId() >= 1) {
            if (null != inode.getACEs() && !inode.getACEs().isEmpty()) {
                List<Inode.ACE> curAcl = inode.getACEs();
                ListIterator<Inode.ACE> listIterator = curAcl.listIterator();
                while (listIterator.hasNext()) {
                    Inode.ACE ace = listIterator.next();
                    int type = ace.getNType();
                    if (NFSACL_CLASS == type) {
                        res = true;
                        break;
                    }
                }
            }
        }

        return res;
    }

    public static int getNFSMask(List<Inode.ACE> list) {
        int mask = -1;
        ListIterator<Inode.ACE> listIterator = list.listIterator();
        while (listIterator.hasNext()) {
            Inode.ACE ace = listIterator.next();
            int type = ace.getNType();
            if (NFSACL_CLASS == type) {
                mask = ace.getRight();
                break;
            }
        }

        return mask;
    }

    public static boolean checkSIDFormat(String sid) {
        try {
            if (StringUtils.isBlank(sid)) {
                return false;
            }

            if (sid.startsWith(FSIdentity.USER_SID_PREFIX)) {
                String uidStr = sid.substring(FSIdentity.USER_SID_PREFIX.length());
                int uid = Integer.parseInt(uidStr);
                if (uid >= 0) {
                    return true;
                }
            } else if (sid.startsWith(FSIdentity.GROUP_SID_PREFIX)) {
                String gidStr = sid.substring(FSIdentity.GROUP_SID_PREFIX.length());
                int gid = Integer.parseInt(gidStr);
                if (gid >= 0) {
                    return true;
                }
            } else {
                //特定账户格式：everyOne
                if ("S-1-1-0".equals(sid)){
                    return true;
                }
                return false;
            }
        } catch (Exception e) {
            log.error("sid: {} is invalid", sid);
        }

        return false;
    }

    /**
     * 检查sid是否符合标准，在S-1-5-0或者S-1-5-1001 - S-1-5-65534之间，若不在则返 false
     * 且sid是否有对应存在的文件账户，若没有则返回 false
     *
     * @param isUser 当前sid是否是用户sid
     **/
    public static boolean checkSID(String sid, boolean isUser) {
        if (StringUtils.isBlank(sid)) {
            return false;
        }

        if (isUser) {
            if (sid.startsWith(FSIdentity.USER_SID_PREFIX)) {
                try {
                    String uidStr = sid.substring(FSIdentity.USER_SID_PREFIX.length());
                    int uid = Integer.parseInt(uidStr);
                    if (uid >= 0) {
                        return true;
                    }
                } catch (Exception e) {
                    log.error("sid(isUser: {}): {} is invalid", isUser, sid);
                }
            }

            return false;
        }


        if (sid.startsWith(FSIdentity.GROUP_SID_PREFIX)) {
            try {
                String gidStr = sid.substring(FSIdentity.GROUP_SID_PREFIX.length());
                int gid = Integer.parseInt(gidStr);
                if (gid >= 0) {
                    return true;
                }
            } catch (Exception e) {
                log.error("sid(isUser: {}): {} is invalid", isUser, sid);
            }
        }

        return false;
    }

    /**
     * 检查待更改的id是否存在
     * <p>
     * id=0和65534允许更改
     * 其余id需在范围内，且需要是已经完成配置的uid
     **/
    public static boolean checkId(int id) {
        if (id == 0 || id == DEFAULT_ANONY_UID) {
            return true;
        }

        if (id > MIN_ID && id <= MAX_ID) {
            if (uidToS3ID.containsKey(id)) {
                return true;
            }
        }

        return false;
    }

    public static boolean needChangeGroupMode(int oldMode, int newMode) {
        int oldGroupMode = oldMode & (GROUP_READ | GROUP_WRITE | GROUP_EXEC);
        int newGroupMode = newMode & (GROUP_READ | GROUP_WRITE | GROUP_EXEC);
        return oldGroupMode != newGroupMode;
    }

    public static int setFsProtoType(Map<String, String> bucketInfo) {
        if (null == bucketInfo || bucketInfo.isEmpty() || null == bucketInfo.get("fsid")) {
            return 0;
        }

        if (HeartBeatChecker.isMultiAliveStarted && !"1".equals(bucketInfo.get(MOUNT_CLUSTER))) {
            // 复制环境的文件如果不是挂在本地站点，客户端发送的原始请求、差异记录的同步请求将跳过acl校验。
            return 0;
        }

        int res = 0;

        if ("1".equals(bucketInfo.get("nfs"))) {
            res |= NFSV3_START;
        }

        if ("1".equals(bucketInfo.get("cifs"))) {
            res |= CIFS_START;
        }

        if ("1".equals(bucketInfo.get("ftp"))) {
            res |= FTP_START;
        }

        return res;
    }

    public static boolean checkNFSv3Start(int flag) {
        return (flag & NFSV3_START) != 0;
    }

    public static boolean checkCifsStart(int flag) {
        return (flag & CIFS_START) != 0;
    }

    public static boolean checkFTPStart(int flag) {
        return (flag & FTP_START) != 0;
    }

    public static int setDirOrFile(boolean isDir) {
        return isDir ? FsConstants.CIFSJudge.IS_DIR : FsConstants.CIFSJudge.IS_FILE;
    }

    /**
     * s3端创桶时检查账户是否配置uid
     * <p>
     * 若nfs或者cifs权限任意开关已经开启，此时创建桶又要求开启任意一种文件协议，则需要检查创桶的账户是否已经配置uid，若没有配置则拒绝创桶
     **/
    public static void checkAccountFsAcl(String s3Id, String bucketName, boolean nfsOpen, boolean cifsOpen, boolean ftpOpen) {
        boolean isFileAclStart = NFS_ACL_START || CIFS_ACL_START;
        boolean isOpenAnyFileProtocol = nfsOpen || cifsOpen || ftpOpen;
        if (isFileAclStart && isOpenAnyFileProtocol) {
            //检查请求创建桶的账户是否已经配置uid，若未配置则不予创建
            FSIdentity identity = userInfo.get(s3Id);
            if (null == identity || identity.getUid() == 0) {
                throw new MsException(BUCKET_OWNER_MISSING_FS_ACCOUNT,
                        "createBucket failed, s3 account does not match any file system ID, bucket_name: " + bucketName + ".");
            }
        }
    }
}
