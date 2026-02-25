package com.macrosan.utils.iam;/**
 * @author niechengxing
 * @create 2024-09-20 11:25
 */

import com.alibaba.fastjson.JSONObject;
import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.ErasureClient;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.Credential;
import com.macrosan.message.jsonmsg.InlinePolicy;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.macrosan.constants.SysConstants.REDIS_IAM_INDEX;
import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;
import static com.macrosan.utils.sts.RoleUtils.AK_PREFIX;

/**
 *@program: MS_Cloud
 *@description:
 *@author: niechengxing
 *@create: 2024-09-20 11:25
 */
@Log4j2
public class STSTokenMove {
    private static final IamRedisConnPool iamConnPool = IamRedisConnPool.getInstance();
    private static final RedisConnPool pool = RedisConnPool.getInstance();
    AtomicBoolean locked = new AtomicBoolean();
    private static final Scheduler move = Schedulers.fromExecutor(new MsExecutor(1, 1, new MsThreadFactory("sts-move")));
    public static void start(){//如果标志位不存在，开始设置锁，设置锁成功，后面
        if (1 == pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).exists("complete_upgrade")) {//启动后判断标志位是否存在，不存在则定时10s再判断；存在则开始抢占锁，抢占成功的节点执行迁移，其他节点不处理
            moveToken();
        } else {
            move.schedule(STSTokenMove::start, 10, TimeUnit.SECONDS);
            //如果某个标志位存在，那么说明在执行，但是如果执行的很快没，不存在标志位需要继续跑
        }
        //如果设置标志位，此时前端包都启动需要抢占锁，获取锁的节点处理迁移，其他节点若未获取，则不再处理，处理完成后删除标志位和锁

    }
    public static boolean tryLock() {
        try {
            SetArgs setArgs = SetArgs.Builder.nx();
            String setKey = pool.getShortMasterCommand(0).set("STS_move", ServerConfig.getInstance().getHostUuid(), setArgs);
            boolean res = "OK".equalsIgnoreCase(setKey);

            return res;
        } catch (Exception e) {
            return false;
        }

    }
    public static void moveToken() {
        try {
            if (tryLock()) {
                StoragePool metaStoragePool = StoragePoolFactory.getMetaStoragePool("");
                RedisCommands<String, String> command = iamConnPool.getCommand(REDIS_IAM_INDEX);
                ScanIterator<String> redisIterator = ScanIterator.scan(command, new ScanArgs().match(AK_PREFIX + "*"));
                while (redisIterator.hasNext()) {
                    String akKey = redisIterator.next();

                    long akTtl = iamConnPool.getCommand(REDIS_IAM_INDEX).ttl(akKey);
                    if (akTtl > 0) {
                        String akInfoStr = iamConnPool.getCommand(REDIS_IAM_INDEX).get(akKey);
                        JSONObject object = JSONObject.parseObject(akInfoStr);
                        String userId = object.getString("userId");
                        Credential credential = new Credential()
                                .setAccountId(object.getString("accountId"))
                                .setAccessKey(akKey)
                                .setSecretKey(object.getString("secretKey"))
                                .setAssumeId(userId)
                                .setUseName("userName");

                        String assumeInfoStr = iamConnPool.getCommand(REDIS_IAM_INDEX).get(userId);
                        Credential credential0 = Json.decodeValue(assumeInfoStr, Credential.class);
                        List<String> policyIds = credential0.getPolicyIds();
                        for (String policyId : policyIds) {
                            long t = iamConnPool.getCommand(REDIS_IAM_INDEX).ttl(policyId);
                            if (t > 0) {
                                String inlinePolicy = iamConnPool.getCommand(REDIS_IAM_INDEX).get(policyId);
                                credential.setInlinePolicy(new InlinePolicy().setPolicyId(policyId).setPolicy(inlinePolicy));
                                break;
                            }
                        }
                        credential.setGroupIds(credential0.getGroupIds()).setPolicyIds(policyIds);

                        long cur = System.currentTimeMillis() / 1000;
                        credential.setDeadline(cur + akTtl);


                        String vnode = metaStoragePool.getBucketVnodeId(credential.accessKey);
                        Disposable disposable = metaStoragePool.mapToNodeInfo(vnode)
                                .flatMap(nodeList -> {
                                    String credentialKey = credential.getCredentialKey(vnode);
                                    return ErasureClient.putCredential(credential, credentialKey, nodeList);
                                })
                                .doOnNext(b -> {
                                    if (b) {

                                    } else {
                                        log.error("move STS token to rocksdb error!");
    //                            throw new MsException(UNKNOWN_ERROR, "put STS token to rocksdb error!");
                                    }
                                }).subscribe(b -> {
                                }, e -> log.error("move STS token to rocksdb error!"));

                    }

    //            String assumeInfoStr = iamConnPool.getCommand(REDIS_IAM_INDEX).get(key);
    //            Credential credential = Json.decodeValue(assumeInfoStr, Credential.class);
    //            credential.setAssumeId(key);
    //            List<String> policyIds = credential.getPolicyIds();
    //            for (String policyId : policyIds) {
    //                long t = iamConnPool.getCommand(REDIS_IAM_INDEX).ttl(policyId);
    //                if (t > 0) {
    //                    String inlinePolicy = iamConnPool.getCommand(REDIS_IAM_INDEX).get(policyId);
    //                    credential.setInlinePolicy(new InlinePolicy().setPolicyId(policyId).setPolicy(inlinePolicy));
    //                    break;
    //                }
    //            }
    //            //获取临时ak/sk
    //            ScanIterator<String> iterator = ScanIterator.scan(command, new ScanArgs().match(AK_PREFIX + "*"));
    //            while (iterator.hasNext()) {
    //                String akKey = redisIterator.next();
    //                long akTtl = iamConnPool.getCommand(REDIS_IAM_INDEX).ttl(akKey);
    //                if (akTtl > 0) {
    //                    String akInfoStr = iamConnPool.getCommand(REDIS_IAM_INDEX).get(akKey);
    //                    JSONObject object = JSONObject.parseObject(akInfoStr);
    //                    String userId = object.getString("userId");
    //                    if (key.equals(userId)) {
    //                        credential.setAccessKey(akKey)
    //                                .setSecretKey(object.getString("secretKey"))
    //                                .setUseName(object.getString("userName"));
    //                        break;
    //                    }
    //                }
    //            }
    //            long t = iamConnPool.getCommand(REDIS_IAM_INDEX).ttl(key);
    //            long cur = System.currentTimeMillis() / 1000;
    //            credential.setDeadline(cur + t);
    //
    //
    //            String vnode = metaStoragePool.getBucketVnodeId(credential.accessKey);
    //            Disposable disposable = metaStoragePool.mapToNodeInfo(vnode)
    //                    .flatMap(nodeList -> {
    //                        String credentialKey = credential.getCredentialKey(vnode);
    //                        return ErasureClient.putCredential(credential, credentialKey, nodeList);
    //                    })
    //                    .doOnNext(b -> {
    //                        if (b) {
    //
    //                        } else {
    //                            log.error("move STS token to rocksdb error!");
    ////                            throw new MsException(UNKNOWN_ERROR, "put STS token to rocksdb error!");
    //                        }
    //                    }).subscribe(b -> {}, e -> log.error("move STS token to rocksdb error!"));
                }

//                //所有key迁移完成后，删除6381中的数据
//                List<String> delKey = new ArrayList<>();
//                ScanIterator<String> akIterator = ScanIterator.scan(command, new ScanArgs().match(AK_PREFIX + "*"));
//                try (StatefulRedisConnection<String, String> tmpConnection = iamConnPool.getSharedConnection(REDIS_IAM_INDEX).newMaster()) {
//                    tmpConnection.setAutoFlushCommands(true);
//                    RedisCommands<String, String> redisCommands = tmpConnection.sync();
//                    while(akIterator.hasNext()) {
//                        String akKey = akIterator.next();
//                        long akTtl = command.ttl(akKey);
//                        if (akTtl > 0) {
//                            redisCommands.del(akKey);
//                        }
//                    }
//                }
                Thread.sleep(30_000);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del("complete_upgrade");
                pool.getShortMasterCommand(0).del("STS_move");
            }
        } catch (Exception e) {
            log.error("", e);
        }

    }
}

