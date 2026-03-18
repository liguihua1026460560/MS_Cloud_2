package com.macrosan.action.managestream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.ComponentTask;
import com.macrosan.message.xmlmsg.ListComponentTaskResponse;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.serialize.JaxbUtils;
import com.macrosan.snapshot.utils.SnapshotUtil;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.*;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;

/**
 * @author zhaoyang
 * @date 2023/07/25
 **/

@Log4j2
public class ComponentTaskService extends BaseService {
    private static ComponentTaskService instance = null;

    private static final IamRedisConnPool IAM_POOL = IamRedisConnPool.getInstance();

    public static ComponentTaskService getInstance() {
        if (instance == null) {
            instance = new ComponentTaskService();
        }
        return instance;
    }

    /**
     * 新增任务
     *
     * @param paramMap 参数
     * @return
     */
    public ResponseMsg putComponentTask(UnifiedMap<String, String> paramMap) {
        //鉴权
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(userId);
        String taskName = paramMap.get(COMPONENT_TASK_NAME);
        String bucket = paramMap.get(COMPONENT_TASK_BUCKET);
        String prefix = "null".equals(paramMap.get(COMPONENT_TASK_PREFIX)) ? "" : paramMap.get(COMPONENT_TASK_PREFIX);
        String startTime = paramMap.get(COMPONENT_TASK_TIME);
        String strategyName = paramMap.get(COMPONENT_TASK_STRATEGY_NAME);
        //参数校验 除了prefix其他都不能为空
        if (StringUtils.isEmpty(taskName) || StringUtils.isEmpty(bucket)
                || StringUtils.isEmpty(startTime) || StringUtils.isEmpty(strategyName)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
        }
        if (CheckUtils.bucketFsCheck(bucket)){
            throw new MsException(ErrorNo.NFS_NOT_STOP, "The bucket already start nfs or cifs, can not add componentTask");
        }
        //判断startTime格式
        checkStartTime(startTime);
        //判断任务名称是否已存在
        ScanIterator<String> iterator = ScanIterator.scan(pool.getCommand(REDIS_POOL_INDEX), new ScanArgs().match(COMPONENT_TASK_REDIS_PREFIX + "*"));
        while (iterator.hasNext()) {
            String key = iterator.next();
            String redisTaskName = pool.getCommand(REDIS_POOL_INDEX).hget(key, "taskName");
            if (Objects.equals(taskName, redisTaskName)) {
                throw new MsException(ErrorNo.TASK_NAME_ALREADY_EXISTS, "task name already exists,name:" + taskName);
            }
        }
        //判断源桶是否存在
        Long exists = pool.getCommand(REDIS_BUCKETINFO_INDEX).exists(bucket);
        if (exists != 1L) {
            throw new MsException(ErrorNo.SOURCE_BUCKET_NOT_EXISTS, "The source bucket does not exists,sourceBucket:" + bucket);
        }
        // 判断目标桶是否为本区域的桶
        String regionName = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, REGION_NAME);
        regionCheck(regionName);
        SnapshotUtil.checkOperationCompatibility(SnapshotUtil.checkBucketSnapshotEnable(bucket) ? "on" : null);
        //判断策略是否存在，并且获取到策略类型
        String strategyType = pool.getCommand(REDIS_POOL_INDEX).hget(COMPONENT_STRATEGY_REDIS_PREFIX + strategyName, "type");
        if (StringUtils.isEmpty(strategyType)){
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
        }
        //封装map，并添加到redis中
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("taskName", taskName);
        valueMap.put("bucket", bucket);
        valueMap.put("prefix", prefix);
        valueMap.put("strategyName", strategyName);
        valueMap.put("startTime", startTime);
        valueMap.put("strategyType", strategyType);
        valueMap.put("marker",RandomStringUtils.randomAlphanumeric(10).toLowerCase());
        pool.getShortMasterCommand(REDIS_POOL_INDEX).hmset(COMPONENT_TASK_REDIS_PREFIX + taskName, valueMap);
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }


    /**
     * 获取任务列表
     *
     * @param paramMap
     * @return
     */
    public ResponseMsg listComponentTasks(UnifiedMap<String, String> paramMap) {
        //鉴权
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(userId);
        ListComponentTaskResponse listComponentTaskResponse = getListComponentTaskResponse();
        byte[] data = JaxbUtils.toByteArray(listComponentTaskResponse);
        return new ResponseMsg().setData(data).addHeader(CONTENT_TYPE, XML_RESPONSE);
    }

    /**
     * 获取指定name的任务
     *
     * @param paramMap
     * @return
     */
    public ResponseMsg getComponentTask(UnifiedMap<String, String> paramMap) {
        //鉴权
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(userId);
        String taskName = paramMap.get(COMPONENT_TASK_NAME);
        if (StringUtils.isEmpty(taskName)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
        }
        Map<String, String> taskMap = pool.getCommand(REDIS_POOL_INDEX).hgetall(COMPONENT_TASK_REDIS_PREFIX + taskName);
        if (taskMap == null || taskMap.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_COMPONENT_TASK, "no such component task;  taskName:" + taskName);
        }
        //将redis查出来的map转成实体类
        ComponentTask componentTask = JSON.parseObject(JSON.toJSONString(taskMap), ComponentTask.class);
        ListComponentTaskResponse listComponentTaskResponse = new ListComponentTaskResponse().setComponentTaskList(Collections.singletonList(componentTask));
        //将实体类序列化成xml格式
        byte[] data = JaxbUtils.toByteArray(listComponentTaskResponse);
        return new ResponseMsg().setData(data).addHeader(CONTENT_TYPE, XML_RESPONSE);
    }

    /**
     * 删除指定name的任务
     *
     * @param paramMap
     * @return
     */
    public ResponseMsg deleteComponentTask(UnifiedMap<String, String> paramMap) {
        //鉴权
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(userId);
        String taskName = paramMap.get(COMPONENT_TASK_NAME);

        if (StringUtils.isEmpty(taskName)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
        }
        //将task添加到delete_task_set中，定期删除还没处理的record
        Map<String, String> taskMap = pool.getCommand(REDIS_POOL_INDEX).hgetall(COMPONENT_TASK_REDIS_PREFIX + taskName);
        if (taskMap != null && !StringUtils.isEmpty(taskMap.get("taskName"))) {
            JSONObject values = new JSONObject();
            values.put("bucket", taskMap.get("bucket"));
            values.put("marker", taskMap.get("marker"));
            pool.getShortMasterCommand(REDIS_POOL_INDEX).sadd(COMPONENT_TASK_DELETE_SET, values.toJSONString());
        }
        // 删除该任务下的所有失败列表
        IAM_POOL.getShortMasterCommand(REDIS_COMPONENT_ERROR_INDEX).del(TASK_ERROR_RECORD_PREFIX + taskName);
        Long delCode = pool.getShortMasterCommand(REDIS_POOL_INDEX).del(COMPONENT_TASK_REDIS_PREFIX + taskName);
        if (-1 == delCode) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "delete component task fail; name is " + taskName);
        }
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    protected ListComponentTaskResponse getListComponentTaskResponse() {
        //根据前缀扫描获取到所有componentTask的key
        ScanIterator<String> iterator = ScanIterator.scan(pool.getCommand(REDIS_POOL_INDEX), new ScanArgs().match(COMPONENT_TASK_REDIS_PREFIX + "*"));
        List<ComponentTask> componentTaskList = new ArrayList<>();
        while (iterator.hasNext()) {
            String key = iterator.next();
            Map<String, String> taskMap = pool.getCommand(REDIS_POOL_INDEX).hgetall(key);
            //某些情况可能导致task已被删除了，但是又设置了其他属性，因此检查task是否已被删除
            if (StringUtils.isEmpty(taskMap.get("taskName"))) {
                pool.getShortMasterCommand(REDIS_POOL_INDEX).del(key);
            }
            componentTaskList.add(JSON.parseObject(JSON.toJSONString(taskMap), ComponentTask.class));
        }
        return new ListComponentTaskResponse().setComponentTaskList(componentTaskList);
    }

    private void checkStartTime(String startTime) {
        String[] timeArray = startTime.split("-");
        if (timeArray.length != 2) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
        }
        //类型转换可能会出错，出错就说明time参数格式有问题直接抛出
        try {
            int hour = Integer.parseInt(timeArray[0]);
            int minutes = Integer.parseInt(timeArray[1]);
            //判断hour是否在0-23之间，minutes是否在0-59之间
            if (hour < 0 || hour > 23 || minutes < 0 || minutes > 59) {
                throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
            }
        } catch (NumberFormatException e) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
        }
    }

}
