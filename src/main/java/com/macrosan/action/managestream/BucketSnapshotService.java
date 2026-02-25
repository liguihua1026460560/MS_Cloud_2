package com.macrosan.action.managestream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisLock;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.BucketSnapshot;
import com.macrosan.message.xmlmsg.BucketSnapshotResult;
import com.macrosan.snapshot.enums.BucketSnapshotType;
import com.macrosan.snapshot.enums.MergeTaskType;
import com.macrosan.snapshot.pojo.SnapshotMergeTask;
import com.macrosan.snapshot.enums.BucketSnapshotState;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.PolicyCheckUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import com.macrosan.snapshot.SnapshotMarkGenerator;
import io.lettuce.core.KeyValue;
import io.lettuce.core.ScanIterator;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;

/**
 * @author zhaoyang
 * @date 2024/06/25
 **/
@Log4j2
public class BucketSnapshotService extends BaseService {

    private static BucketSnapshotService instance = null;

    private BucketSnapshotService() {
        super();
    }

    private final int MAX_BUCKET_SNAPSHOT_NAME_LENGTH = 16;
    private final String DEFAULT_MAX_ROLL_BACK_SIZE = "10";
    private final String DEFAULT_SNAPSHOT_SIZE = "1";


    public final String BUCKET_SNAPSHOT_NAME_PARAM_KEY = "snapshotName";
    public final String ROLLBACK_TARGET_SNAPSHOT_MARK_PARAM_KEY = "rollBackTargetMark";
    public final String ROLLBACK_TARGET_SNAPSHOT_DISCARD_KEY = "discard";
    public final String MAX_SNAPSHOT_SIZE_KEY = "max_snapshot_size";
    public final String MAX_ROLL_BACK_NUM_KEY = "max_roll_back_num";
    public static final String SNAPSHOT_LOCK_KEY = "snapshot_lock";
    public final String BUCKET_SNAPSHOT_MARK_PARAM_KEY = "snapshotMark";
    public final String BUCKET_SNAPSHOT_RECOVER_PARAM_KEY = "recover";


    /**
     * 每一个Service都必须提供一个getInstance方法
     */
    public static BucketSnapshotService getInstance() {
        if (instance == null) {
            instance = new BucketSnapshotService();
        }
        return instance;
    }

    public ResponseMsg postBucketSnapshot(UnifiedMap<String, String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(userId);
        //桶所有者或拥有PutBucketSnapshot权限的用户可以操作
        String method = "PostBucketSnapshot";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketName);
        }

        Runnable runnable = () -> {

            // 获取bucket信息
            Map<String, String> bucket = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
            if (bucket == null) {
                throw new MsException(ErrorNo.NO_SUCH_BUCKET, "createBucketSnapshot fail , no such bucket");
            }
            // 判断桶是否允许快照---合入桶快照功能后，创建桶时都会将改标记置为true
            if (!"on".equals(bucket.get(SNAPSHOT_SWITCH))) {
                throw new MsException(ErrorNo.BUCKET_SNAPSHOT_NOT_ALLOWED, "createBucketSnapshot fail , bucket snapshot not allowed");
            }
            String snapshotName = paramMap.get(BUCKET_SNAPSHOT_NAME_PARAM_KEY);
            if (StringUtils.isBlank(snapshotName) || snapshotName.length() > MAX_BUCKET_SNAPSHOT_NAME_LENGTH) {
                throw new MsException(ErrorNo.BUCKET_SNAPSHOT_NAME_INVALID, "bucket snapshot cannot be empty");
            }

            // 判断快照数量是否超出限制
            Long snapshotSize = pool.getCommand(REDIS_SNAPSHOT_INDEX).hlen(BUCKET_SNAPSHOT_PREFIX + bucketName);
            String maxSnapshotSize = Optional.ofNullable(pool.getCommand(REDIS_SNAPSHOT_INDEX).get(MAX_SNAPSHOT_SIZE_KEY)).orElse(DEFAULT_SNAPSHOT_SIZE);
            if (snapshotSize >= Long.parseLong(maxSnapshotSize)) {
                throw new MsException(ErrorNo.BUCKET_SNAPSHOT_SIZE_EXCEEDED, "bucket snapshot size exceed");
            }

            // 获取待创建快照mark
            String currentMark = bucket.get(CURRENT_SNAPSHOT_MARK);
            // 获取快照链接关系
            String snapshotLinkRelationship = bucket.get(SNAPSHOT_LINK);
            Deque<String> snapshotLink;
            if (StringUtils.isBlank(snapshotLinkRelationship)) {
                snapshotLink = new LinkedList<>();
            } else {
                snapshotLink = Json.decodeValue(snapshotLinkRelationship, new TypeReference<Deque<String>>() {
                });
            }
            String parentMark = null;
            Map<String, String> snapshotMap = new HashMap<>(2);
            if (!snapshotLink.isEmpty()) {
                // 父快照不为空，则将当前创建快照添加到父快照的子快照中
                parentMark = snapshotLink.peek();
                String parent = pool.getCommand(REDIS_SNAPSHOT_INDEX).hget(BUCKET_SNAPSHOT_PREFIX + bucketName, parentMark);
                BucketSnapshot parentSnapshot = Json.decodeValue(parent, BucketSnapshot.class);
                if (parentSnapshot.getChildren() == null) {
                    parentSnapshot.setChildren(new ArrayList<>());
                }
                parentSnapshot.getChildren().add(currentMark);
                snapshotMap.put(parentMark, Json.encode(parentSnapshot));
            }
            BucketSnapshot bucketSnapshot = new BucketSnapshot();
            bucketSnapshot.setName(snapshotName);
            bucketSnapshot.setCreateTime(String.valueOf(System.currentTimeMillis()));
            bucketSnapshot.setMark(currentMark);
            bucketSnapshot.setParent(parentMark);
            bucketSnapshot.setType(BucketSnapshotType.ManualSnapshot);
            bucketSnapshot.setState(BucketSnapshotState.CREATED.getState());
            snapshotMap.put(currentMark, Json.encode(bucketSnapshot));
            // 更新快照链接关系和待创建快照mark
            Map<String, String> bucketMap = new HashMap<>(2);
            bucketMap.put(CURRENT_SNAPSHOT_MARK, SnapshotMarkGenerator.getNextSnapshotMark(currentMark));
            snapshotLink.addFirst(currentMark);
            bucketMap.put(SNAPSHOT_LINK, Json.encode(snapshotLink));
            // 保存当前快照信息到redis
            pool.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).hmset(BUCKET_SNAPSHOT_PREFIX + bucketName, snapshotMap);
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hmset(bucketName, bucketMap);
        };
        boolean tryLock = RedisLock.tryLock(REDIS_SNAPSHOT_INDEX, SNAPSHOT_LOCK_KEY, 10, TimeUnit.SECONDS, runnable);
        if (!tryLock) {
            throw new MsException(ErrorNo.BUCKET_SNAPSHOT_LOCK_TIMEOUT, "get bucket snapshot lock time out");
        }
        log.info("create bucket snapshot success, snapshot :{}", paramMap.get(BUCKET_SNAPSHOT_NAME_PARAM_KEY));
        return new ResponseMsg();
    }


    public ResponseMsg putBucketSnapshot(UnifiedMap<String, String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(userId);
        //桶所有者或拥有RollBackBucketSnapshot权限的用户可以操作
        String method = "PutBucketSnapshot";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketName);
        }
        // 是否丢弃当前最新快照标记下的对象数据
        boolean discard = Boolean.parseBoolean(paramMap.get(ROLLBACK_TARGET_SNAPSHOT_DISCARD_KEY));
        boolean recover = Boolean.parseBoolean(paramMap.get(BUCKET_SNAPSHOT_RECOVER_PARAM_KEY));
        Runnable runnable = () -> {
            // 获取bucket信息
            Map<String, String> bucket = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
            if (bucket == null) {
                throw new MsException(ErrorNo.NO_SUCH_BUCKET, "rollBackBucketSnapshot fail , no such bucket");
            }
            // 判断桶是否允许回滚
            if (!"on".equals(bucket.get(SNAPSHOT_SWITCH))) {
                throw new MsException(ErrorNo.BUCKET_SNAPSHOT_NOT_ALLOWED, "rollBackBucketSnapshot fail , bucket snapshot not allowed");
            }
            String currentSnapshotMark = bucket.get(CURRENT_SNAPSHOT_MARK);
            if (recover) {
                // 恢复数据
                recoverLatestData(bucket, bucketName, currentSnapshotMark);
                return;
            }
            String rollbackTargetMark = paramMap.get(ROLLBACK_TARGET_SNAPSHOT_MARK_PARAM_KEY);
            if (StringUtils.isBlank(rollbackTargetMark)){
                throw new MsException(ErrorNo.ROLLBACK_SNAPSHOT_MARK_INVALID,"The roll back snapshot mark is invalid");
            }
            // 获取回滚目标
            String rollbackTarget = pool.getCommand(REDIS_SNAPSHOT_INDEX).hget(BUCKET_SNAPSHOT_PREFIX + bucketName, rollbackTargetMark);
            Boolean exists = pool.getCommand(REDIS_SNAPSHOT_INDEX).hexists(SNAPSHOT_MERGE_TASK_PREFIX + bucketName, rollbackTargetMark);
            if (StringUtils.isBlank(rollbackTarget) || exists) {
                throw new MsException(ErrorNo.NO_SUCH_ROLLBACK_TARGET_SNAPSHOT, "rollbackBucketSnapshot fail , no such rollback target bucket snapshot");
            }

            BucketSnapshot rollbackTargetSnapshot = Json.decodeValue(rollbackTarget, BucketSnapshot.class);
            if (rollbackTargetSnapshot.getType().equals(BucketSnapshotType.RollbackSnapshot)) {
                throw new MsException(ErrorNo.NO_SUCH_ROLLBACK_TARGET_SNAPSHOT, "rollbackBucketSnapshot fail , no such rollback target bucket snapshot");
            }
            // 判断是否处理回滚中
            Map<String, String> allMergeTask = pool.getCommand(REDIS_SNAPSHOT_INDEX).hgetall(SNAPSHOT_MERGE_TASK_PREFIX + bucketName);
            allMergeTask.values().forEach(task -> {
                SnapshotMergeTask snapshotMergeTask = Json.decodeValue(task, SnapshotMergeTask.class);
                if (snapshotMergeTask.getTargetSnapshotMark().equals(currentSnapshotMark)) {
                    // 正在恢复数据，不能进行回滚
                    throw new MsException(ErrorNo.CURRENTLY_RECOVERING_LATEST_DATA, "rollback fail , Currently rolling back");
                }
            });

            // 判断回滚次数是否超过上限
            String maxViewSize = Optional.ofNullable(pool.getCommand(REDIS_SNAPSHOT_INDEX).get(MAX_ROLL_BACK_NUM_KEY)).orElse(DEFAULT_MAX_ROLL_BACK_SIZE);
            Long snapshotSize = pool.getCommand(REDIS_SNAPSHOT_INDEX).hlen(BUCKET_SNAPSHOT_PREFIX + bucketName);
            long rollBackSize = snapshotSize - 1;
            Long discardSize = pool.getCommand(REDIS_SNAPSHOT_INDEX).scard(SNAPSHOT_DISCARD_LIST_PREFIX + bucketName);
            if (rollBackSize + discardSize == Integer.parseInt(maxViewSize)) {
                throw new MsException(ErrorNo.ROLLBACK_QUANTITY_EXCEEDS_LIMIT, "rollback fail ,the rollback quantity exceeds limit");
            }
            // 快照链接关系
            List<String> snapshotLinkRelationship = new ArrayList<>();
            // 判断回滚时是否需要删除目标回滚快照
            String maxSnapshotSize = Optional.ofNullable(pool.getCommand(REDIS_SNAPSHOT_INDEX).get(MAX_SNAPSHOT_SIZE_KEY)).orElse(DEFAULT_SNAPSHOT_SIZE);
            boolean isMultiSnapshot = false;
            if (!DEFAULT_SNAPSHOT_SIZE.equals(maxSnapshotSize)) {// 多快照模式
                isMultiSnapshot = true;
                if (snapshotSize >= Long.parseLong(maxSnapshotSize)) {
                    throw new MsException(ErrorNo.BUCKET_SNAPSHOT_SIZE_EXCEEDED, "bucket snapshot size exceed");
                }
                // 创建快照链接关系
                recursiveLinkTree(bucketName, rollbackTargetSnapshot, snapshotLinkRelationship);
            }
            if (discard) {
                // 丢弃最新快照标记下的对象数据
                rollBackDiscardMode(currentSnapshotMark, bucketName, isMultiSnapshot, snapshotLinkRelationship, rollbackTargetMark);
                return;
            }
            // 保留当前最新快照标记下的对象
            rollBackRetentionMode(currentSnapshotMark, rollbackTargetSnapshot, bucketName, bucket.get(SNAPSHOT_LINK));
        };
        boolean tryLock = RedisLock.tryLock(REDIS_SNAPSHOT_INDEX, SNAPSHOT_LOCK_KEY, 10, TimeUnit.SECONDS, runnable);
        if (!tryLock) {
            throw new MsException(ErrorNo.BUCKET_SNAPSHOT_LOCK_TIMEOUT, "get bucket snapshot lock time out");
        }
        return new ResponseMsg();
    }

    private static void recoverLatestData(Map<String, String> bucket, String bucketName, String currentSnapshotMark) {
        String snapshotLink = bucket.get(SNAPSHOT_LINK);
        if (StringUtils.isBlank(snapshotLink)) {
            // 未创建快照，则肯定也没有进行回滚过
            throw new MsException(ErrorNo.CURRENTLY_IS_LATEST_DATA, "recover fail , currently is latest data");
        }
        TreeSet<String> createdSnapshots = Json.decodeValue(snapshotLink, new TypeReference<TreeSet<String>>() {
        });
        String createdSnapshotMark = createdSnapshots.first();
        String createdSnapshotJson = pool.getCommand(REDIS_SNAPSHOT_INDEX).hget(BUCKET_SNAPSHOT_PREFIX + bucketName, createdSnapshotMark);
        BucketSnapshot bucketSnapshot = Json.decodeValue(createdSnapshotJson, BucketSnapshot.class);
        if (bucketSnapshot.getChildren() == null || bucketSnapshot.getChildren().isEmpty() || bucketSnapshot.getState().equals(BucketSnapshotState.DELETING.getState())) {
            // 没有进行回滚过，则不需要恢复数据，当前已经是最新数据了
            throw new MsException(ErrorNo.CURRENTLY_IS_LATEST_DATA, "recover fail , currently is latest data");
        }
        Map<String, String> allMergeTask = pool.getCommand(REDIS_SNAPSHOT_INDEX).hgetall(SNAPSHOT_MERGE_TASK_PREFIX + bucketName);
        allMergeTask.values().forEach(task -> {
            SnapshotMergeTask snapshotMergeTask = Json.decodeValue(task, SnapshotMergeTask.class);
            if (snapshotMergeTask.getTargetSnapshotMark().equals(currentSnapshotMark)) {
                // 正在恢复数据，则不能重复恢复
                throw new MsException(ErrorNo.CURRENTLY_RECOVERING_LATEST_DATA, "rollbackBucketSnapshot fail , Currently rolling back");
            }
        });
        List<String> mergeSrc = bucketSnapshot.getChildren().stream().sorted(Comparator.reverseOrder()).limit(1).collect(Collectors.toList());
        SnapshotMergeTask snapshotMergeTask = new SnapshotMergeTask(mergeSrc.get(0), currentSnapshotMark, bucketName, MergeTaskType.RECOVER_MERGE, snapshotLink, System.currentTimeMillis());
        // 保存恢复的合并任务
        pool.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).hset(SNAPSHOT_MERGE_TASK_PREFIX + bucketName, snapshotMergeTask.getSrcSnapshotMark(), Json.encode(snapshotMergeTask));
        log.info("create recover merge task success,bucket:{},targetMark:{},currentMark:{}", bucketName, snapshotMergeTask.getSrcSnapshotMark(), currentSnapshotMark);
    }


    /**
     * 删除快照
     *
     * @param paramMap 请求参数
     * @return 结果
     */
    public ResponseMsg deleteBucketSnapshot(UnifiedMap<String, String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        String snapshotMark = paramMap.get(BUCKET_SNAPSHOT_MARK_PARAM_KEY);
        MsAclUtils.checkIfAnonymous(userId);
        //桶所有者或拥有DeleteBucketSnapshot权限的用户可以操作
        String method = "DeleteBucketSnapshot";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketName);
        }

        // 添加异步合并快照之间数据的任务
        Runnable runnable = () -> {
            // 获取bucket信息
            Map<String, String> bucket = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
            if (bucket == null) {
                throw new MsException(ErrorNo.NO_SUCH_BUCKET, "deleteBucketSnapshot fail , no such bucket");
            }
            // 是否允许快照操作
            if (!"on".equals(bucket.get(SNAPSHOT_SWITCH))) {
                throw new MsException(ErrorNo.BUCKET_SNAPSHOT_NOT_ALLOWED, "deleteBucketSnapshot fail , bucket snapshot not allowed");
            }
            if (StringUtils.isBlank(snapshotMark)) {
                throw new MsException(ErrorNo.BUCKET_SNAPSHOT_NAME_INVALID, "deleteBucketSnapshot fail , snapshot name is empty");
            }
            // 判断快照是否存在
            String bucketSnapshot = pool.getCommand(REDIS_SNAPSHOT_INDEX).hget(BUCKET_SNAPSHOT_PREFIX + bucketName, snapshotMark);
            if (StringUtils.isBlank(bucketSnapshot)) {
                throw new MsException(ErrorNo.NO_SUCH_BUCKET_SNAPSHOT, "deleteBucketSnapshot fail , no such bucket snapshot");
            }
            // 判断快照是否正在删除
            BucketSnapshot bucketSnapshotObj = Json.decodeValue(bucketSnapshot, BucketSnapshot.class);
            if (BucketSnapshotState.DELETING.getState().equals(bucketSnapshotObj.getState()) || !bucketSnapshotObj.getType().equals(BucketSnapshotType.ManualSnapshot)) {
                throw new MsException(ErrorNo.NO_SUCH_BUCKET_SNAPSHOT, "deleteBucketSnapshot fail , no such bucket snapshot");
            }
            bucketSnapshotObj.setState(BucketSnapshotState.DELETING.getState());

            Map<String, String> allMergeTask = pool.getCommand(REDIS_SNAPSHOT_INDEX).hgetall(SNAPSHOT_MERGE_TASK_PREFIX + bucketName);
            allMergeTask.values().forEach(task -> {
                SnapshotMergeTask snapshotMergeTask = Json.decodeValue(task, SnapshotMergeTask.class);
                if (snapshotMergeTask.getTargetSnapshotMark().equals(bucket.get(CURRENT_SNAPSHOT_MARK))) {
                    // 正在恢复数据，则不能重复恢复
                    throw new MsException(ErrorNo.CURRENTLY_RECOVERING_LATEST_DATA, "rollbackBucketSnapshot fail , Currently rolling back");
                }
            });

            // 判断是否处存在回滚快照，必须恢复到最新数据才能删除快照
            Long snapshotSize = pool.getCommand(REDIS_SNAPSHOT_INDEX).hlen(BUCKET_SNAPSHOT_PREFIX + bucketName);
            if (snapshotSize > 1) {
                throw new MsException(ErrorNo.CURRENTLY_IS_NOT_LATEST_DATA, "deleteBucketSnapshot fail ,currently is not latest data");
            }

            // 创建快照合并任务
            String targetMerge = bucket.get(CURRENT_SNAPSHOT_MARK);
            SnapshotMergeTask snapshotMergeTask = new SnapshotMergeTask(bucketSnapshotObj.getMark(), targetMerge, bucketName, MergeTaskType.SNAPSHOT_MERGE, bucket.get(SNAPSHOT_LINK), System.currentTimeMillis());
            pool.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).hset(BUCKET_SNAPSHOT_PREFIX + bucketName, bucketSnapshotObj.getMark(), Json.encode(bucketSnapshotObj));
            pool.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).hset(SNAPSHOT_MERGE_TASK_PREFIX + bucketName, bucketSnapshotObj.getMark(), Json.encode(snapshotMergeTask));
        };
        boolean tryLock = RedisLock.tryLock(REDIS_SNAPSHOT_INDEX, SNAPSHOT_LOCK_KEY, 10, TimeUnit.SECONDS, runnable);
        if (!tryLock) {
            throw new MsException(ErrorNo.BUCKET_SNAPSHOT_LOCK_TIMEOUT, "get bucket snapshot lock time out");
        }
        log.info("delete bucket snapshot success,bucket:{} snapshot :{}", bucketName, snapshotMark);

        return new ResponseMsg();
    }

    public ResponseMsg getBucketSnapshot(UnifiedMap<String, String> paramMap) {
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(userId);
        //桶所有者或拥有GetBucketSnapshot权限的用户可以操作
        String method = "GetBucketSnapshot";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (policyResult == 0) {
            userCheck(userId, bucketName);
        }
        Map<String, String> bucket = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucket == null) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET, "getBucketSnapshot fail , no such bucket");
        }
        // 是否允许快照操作
        if (!"on".equals(bucket.get(SNAPSHOT_SWITCH))) {
            throw new MsException(ErrorNo.BUCKET_SNAPSHOT_NOT_ALLOWED, "getBucketSnapshot fail , bucket snapshot not allowed");
        }
        BucketSnapshotResult bucketSnapshotResult = new BucketSnapshotResult();
        Set<BucketSnapshot> bucketSnapshots = new TreeSet<>();
        ScanIterator<KeyValue<String, String>> scanIterator = ScanIterator.hscan(pool.getCommand(REDIS_SNAPSHOT_INDEX), BUCKET_SNAPSHOT_PREFIX + bucketName);
        while (scanIterator.hasNext()) {
            KeyValue<String, String> next = scanIterator.next();
            BucketSnapshot snapshot = Json.decodeValue(next.getValue(), BucketSnapshot.class);
            bucketSnapshots.add(snapshot);
        }
        bucketSnapshotResult.setBucketSnapshots(bucketSnapshots);
        // 返回结果
        byte[] bytes = JaxbUtils.toByteArray(bucketSnapshotResult);
        ResponseMsg responseMsg = new ResponseMsg();
        return responseMsg.setData(bytes)
                .addHeader(CONTENT_TYPE, "application/xml")
                .addHeader(CONTENT_LENGTH, String.valueOf(bytes.length));
    }


    /**
     * 递归快照链接树，获取快照链接关系
     *
     * @param bucket 桶名
     * @param node   当前快照
     * @param set    结果集
     */
    private void recursiveLinkTree(String bucket, BucketSnapshot node, List<String> set) {
        set.add(node.getMark());
        if (StringUtils.isBlank(node.getParent())) {
            return;
        }
        String parent = pool.getCommand(REDIS_SNAPSHOT_INDEX).hget(BUCKET_SNAPSHOT_PREFIX + bucket, node.getParent());
        if (StringUtils.isBlank(parent)) {
            return;
        }
        BucketSnapshot parentNode = Json.decodeValue(parent, BucketSnapshot.class);
        recursiveLinkTree(bucket, parentNode, set);
    }


    private void rollBackDiscardMode(String currentMark, String bucketName, boolean isMultiSnapshot, List<String> snapshotLinkRelationship, String rollBackTarget) {
        // 更新桶中快照信息
        Map<String, String> map = new HashMap<>(2);
        map.put(CURRENT_SNAPSHOT_MARK, SnapshotMarkGenerator.getNextSnapshotMark(currentMark));
        // 更新快照链接关系
        if (isMultiSnapshot) {
            map.put(SNAPSHOT_LINK, Json.encode(snapshotLinkRelationship));
        }
        // 保存到redis中
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hmset(bucketName, map);
        // 添加跳过的快照标记
        pool.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).sadd(SNAPSHOT_DISCARD_LIST_PREFIX + bucketName, currentMark);
        log.info("roll back success, mode:【{}】,bucket:{},rollBackTarget:{},curSnapshotMark:{}", "Discard", bucketName, rollBackTarget, map.get(CURRENT_SNAPSHOT_MARK));
    }

    private void rollBackRetentionMode(String currentMark, BucketSnapshot rollBackTarget, String bucketName, String snapshotLink) {
        // 创建一个回滚快照
        BucketSnapshot rollBackSnapshot = new BucketSnapshot();
        rollBackSnapshot.setMark(currentMark);
        rollBackSnapshot.setType(BucketSnapshotType.RollbackSnapshot);
        rollBackSnapshot.setParent(rollBackTarget.getMark());
        rollBackSnapshot.setCreateTime(String.valueOf(System.currentTimeMillis()));

        if (rollBackTarget.getChildren() == null) {
            rollBackTarget.setChildren(new ArrayList<>());
        }
        rollBackTarget.getChildren().add(rollBackSnapshot.getMark());
        // 创建合并任务
        SnapshotMergeTask snapshotMergeTask = null;
        if (rollBackTarget.getChildren().size() > 1) {
            // 创建合并回滚快照任务
            List<String> mergeMark = rollBackTarget.getChildren().stream().sorted(Comparator.reverseOrder()).limit(2).collect(Collectors.toList());
            snapshotMergeTask = new SnapshotMergeTask(mergeMark.get(1), mergeMark.get(0), bucketName, MergeTaskType.RECOVER_MERGE, snapshotLink, System.currentTimeMillis());
        }

        // 保存到redis中
        Map<String, String> snapshotMap = new HashMap<>(2);
        snapshotMap.put(rollBackSnapshot.getMark(), Json.encode(rollBackSnapshot));
        snapshotMap.put(rollBackTarget.getMark(), Json.encode(rollBackTarget));
        // 更新最新快照标记
        String newCurrentMark = SnapshotMarkGenerator.getNextSnapshotMark(currentMark);
        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucketName, CURRENT_SNAPSHOT_MARK, newCurrentMark);
        pool.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).hmset(BUCKET_SNAPSHOT_PREFIX + bucketName, snapshotMap);
        if (snapshotMergeTask != null) {
            pool.getShortMasterCommand(REDIS_SNAPSHOT_INDEX).hset(SNAPSHOT_MERGE_TASK_PREFIX + bucketName, snapshotMergeTask.getSrcSnapshotMark(), Json.encode(snapshotMergeTask));
            log.info("create merge task, bucket:{},srcMark:{},targetMark:{}", bucketName, snapshotMergeTask.getSrcSnapshotMark(), snapshotMergeTask.getTargetSnapshotMark());
        }
        log.info("roll back success, mode:【{}】,bucket:{},rollBackTarget:{},curSnapshotMark:{}", "Retention", bucketName, rollBackTarget.getMark(), newCurrentMark);
    }

}
