package com.macrosan.action.managestream;

import com.alibaba.fastjson.JSON;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.*;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.PolicyCheckUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import io.lettuce.core.KeyValue;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.Value;
import lombok.extern.log4j.Log4j2;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.macrosan.action.managestream.BucketInventoryService.checkBucketName;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;

/**
 * @author zhaoyang
 * @date 2023/11/23
 **/
@Log4j2
public class ComponentErrorService extends BaseService {

    private ComponentErrorService() {
        super();
    }

    private static ComponentErrorService instance = null;

    public static ComponentErrorService getInstance() {
        if (instance == null) {
            instance = new ComponentErrorService();
        }
        return instance;
    }


    private static final IamRedisConnPool IAM_POOL = IamRedisConnPool.getInstance();


    /**
     * 查询任务级别失败记录
     *
     * @param paramMap 参数
     * @return 失败列表
     */
    public ResponseMsg listTaskComponentErrors(UnifiedMap<String, String> paramMap) {
        //鉴权
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(userId);

        String taskName = paramMap.get("taskName");

        ScanIterator<KeyValue<String, String>> scanIterator = ScanIterator.hscan(IAM_POOL.getCommand(REDIS_COMPONENT_ERROR_INDEX), TASK_ERROR_RECORD_PREFIX + taskName);
        List<ComponentTaskErrorResponse> componentTaskErrorResponses = scanIterator.stream()
                .map(Value::getValue)
                .map(value -> JSON.parseObject(value, ComponentTaskErrorResponse.class))
                .collect(Collectors.toList())
                .stream()
                .sorted(Comparator.comparing(ComponentTaskErrorResponse::getErrorTime).reversed())
                .collect(Collectors.toList());
        // 封装结果
        ListComponentTaskErrorResponse listComponentErrorResponse = new ListComponentTaskErrorResponse().setComponentErrorResponseList(componentTaskErrorResponses);

        // 返回结果
        byte[] bytes = JaxbUtils.toByteArray(listComponentErrorResponse);
        ResponseMsg responseMsg = new ResponseMsg();
        return responseMsg.setData(bytes)
                .addHeader(CONTENT_TYPE, "application/xml")
                .addHeader(CONTENT_LENGTH, String.valueOf(bytes.length));
    }


    /**
     * 删除任务级别失败记录  errorMarks参数为空时，则删除该任务的所有错误记录
     *
     * @param paramMap 参数
     * @return 删除结果
     */
    public ResponseMsg deleteTaskComponentErrors(UnifiedMap<String, String> paramMap) {
        //鉴权
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(userId);
        Long ret;
        String taskName = paramMap.get("taskName");
        String body = paramMap.get(BODY);
        assertRequestBodyNotEmpty(body);
        DeleteTaskComponentErrorRequest delete = (DeleteTaskComponentErrorRequest) JaxbUtils.toObject(body);
        assertParseXmlError(delete);
        if ("true".equals(delete.getClear())) {
            // 删除该任务的所有错误记录
            ret = IAM_POOL.getShortMasterCommand(REDIS_COMPONENT_ERROR_INDEX).del(TASK_ERROR_RECORD_PREFIX + taskName);
        } else {
            // 删除该任务下的某些失败记录
            String[] marks = delete.getErrorMarks().stream().map(ErrorMark::getMark).toArray(String[]::new);
            if (marks.length > 0) {
                ret = IAM_POOL.getShortMasterCommand(REDIS_COMPONENT_ERROR_INDEX).hdel(TASK_ERROR_RECORD_PREFIX + taskName, marks);
            }
        }
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }


    /**
     * 查询对象级别失败记录
     *
     * @param paramMap 参数
     * @return 失败列表
     */
    public ResponseMsg listBucketComponentErrors(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        checkBucketName(bucketName);
        MsAclUtils.checkIfAnonymous(userId);

        Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketInfo.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }

        // 检查权限
        String method = "listBucketComponentErrors";
        int policy = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        if (policy == 0 && !userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "No such list component error records permission");
        }

        // 获取所有bucket开头的key
        ScanIterator<KeyValue<String, String>> scanIterator = ScanIterator.hscan(IAM_POOL.getCommand(REDIS_COMPONENT_ERROR_INDEX), OBJECT_ERROR_RECORD_PREFIX + bucketName);
        List<ComponentObjectErrorResponse> componentObjectErrorResponses = scanIterator.stream()
                .map(Value::getValue)
                .map(value -> JSON.parseObject(value, ComponentObjectErrorResponse.class))
                .collect(Collectors.toList())
                .stream()
                .sorted(Comparator.comparing(ComponentObjectErrorResponse::getErrorTime).reversed())
                .collect(Collectors.toList());

        // 封装结果
        ListComponentObjectErrorResponse listComponentErrorResponse = new ListComponentObjectErrorResponse().setComponentObjectErrorResponses(componentObjectErrorResponses);

        // 返回结果
        byte[] bytes = JaxbUtils.toByteArray(listComponentErrorResponse);
        ResponseMsg responseMsg = new ResponseMsg();
        return responseMsg.setData(bytes)
                .addHeader(CONTENT_TYPE, "application/xml")
                .addHeader(CONTENT_LENGTH, String.valueOf(bytes.length));
    }

    /**
     * 删除对象级别失败记录  errorMarks参数为空时，则删除该桶的所有错误记录
     *
     * @param paramMap 参数
     * @return 删除结果
     */
    public ResponseMsg deleteBucketComponentErrors(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        checkBucketName(bucketName);
        MsAclUtils.checkIfAnonymous(userId);

        Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketInfo.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }

        //检查权限
        String method = "deleteBucketComponentErrors";
        int policy = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        if (policy == 0 && !userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "No such delete component error records permission");
        }
        Long ret;
        String body = paramMap.get(BODY);
        assertRequestBodyNotEmpty(body);
        DeleteBucketComponentErrorRequest delete = (DeleteBucketComponentErrorRequest) JaxbUtils.toObject(body);
        assertParseXmlError(delete);
        if ("true".equals(delete.getClear())) {
            // 删除该桶的所有错误记录
            ret = deleteBucketComponentErrors(bucketName);
        } else if (delete.getErrorMarks() != null) {
            String[] marks = delete.getErrorMarks().stream().map(ErrorMark::getMark).toArray(String[]::new);
            if (marks.length > 0) {
                ret = IAM_POOL.getShortMasterCommand(REDIS_COMPONENT_ERROR_INDEX).hdel(OBJECT_ERROR_RECORD_PREFIX + bucketName, marks);
            }
        }

        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    /**
     * 删除桶的所有错误记录
     *
     * @param bucketName 桶名
     * @return 删除结果
     */
    public long deleteBucketComponentErrors(String bucketName) {
        return IAM_POOL.getShortMasterCommand(REDIS_COMPONENT_ERROR_INDEX).del(OBJECT_ERROR_RECORD_PREFIX + bucketName);
    }
}
