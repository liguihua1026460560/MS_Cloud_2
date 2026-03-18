package com.macrosan.action.managestream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.message.jsonmsg.BucketPolicy;
import com.macrosan.message.jsonmsg.Statement;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.BucketPolicyUtils;
import com.macrosan.utils.policy.PolicyCheckUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.macrosan.constants.ErrorNo.SUCCESS_STATUS;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DoubleActiveUtil.notifySlaveSite;
import static com.macrosan.utils.policy.BucketPolicyUtils.isMatch;
import static com.macrosan.utils.regex.PatternConst.ACCOUNTID_PATTERN;
import static com.rabbitmq.client.ConnectionFactoryConfigurator.USERNAME;


public class BucketPolicyService extends BaseService {
    private static final Logger logger = LogManager.getLogger(BucketPolicyService.class.getName());
    private static BucketPolicyService instance = null;


    public static BucketPolicyService getInstance() {
        if (instance == null) {
            instance = new BucketPolicyService();
        }
        return instance;
    }

    private BucketPolicyService() {
        super();
    }


    /**
     * 设置桶策略
     *
     * @param paramMap
     * @return
     */
    public ResponseMsg putBucketPolicy(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String policyTxt = paramMap.get("body");

        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        userCheck(userId, bucketInfo.get(BUCKET_USER_ID));
        logger.info(bucketName + " policy:" + policyTxt);

        String method = "PutBucketPolicy";
        String userName = paramMap.getOrDefault(USERNAME, "");
        if (CheckUtils.bucketFsCheck(bucketName)){
            throw new MsException(ErrorNo.NFS_NOT_STOP, "The bucket already start nfs or cifs, can not enable bucketPolicy");
        }
        //防止所有接口配置了无法使用的策略后,可以进行修改桶策略
        if (!"".equals(userName)) {
            PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        }
        if (StringUtils.isEmpty(policyTxt)){
            throw new MsException(ErrorNo.INVALID_POLICY, "The length of policy cannot be empty");
        }
        String convertPolicyToJsonStr = convertPolicyToJsonStr(policyTxt);
        if (convertPolicyToJsonStr.length() > POLICY_MAX_SIZE) {
            throw new MsException(ErrorNo.INVALID_POLICY, "The length of policy is more than the 20KB");
        }

        JSONObject policy;
        BucketPolicy bucketPolicys;
        try {
            policy = JSONObject.parseObject(convertPolicyToJsonStr);
            // 检查policy是否有不合法元素
            BucketPolicyUtils.policyCheck(policy);
            if (!"1.0".equals(policy.getString(POLICY_VERSION))) {
                throw new MsException(ErrorNo.INVALID_POLICY_VERSION, "The Version in the policy is invalid");
            }
            bucketPolicys = policy.toJavaObject(BucketPolicy.class);
            // statement相同元素检测
            BucketPolicyUtils.statementCheck(convertPolicyToJsonStr, bucketPolicys);
        } catch (MsException e) {
            throw e;
        } catch (Exception e) {
            throw new MsException(ErrorNo.INVALID_POLICY, "The policy is invalid, policy: " + convertPolicyToJsonStr);
        }

        //简化json中元素公共前缀
        JSONArray jsonArray = policy.getJSONArray(POLICY_Statement);
        if (jsonArray == null || jsonArray.size() == 0) {
            throw new MsException(ErrorNo.INVALID_POLICY_STATEMENT, "The policy is invalid in statement");
        }
        BucketPolicyUtils.sameLabelCheck(convertPolicyToJsonStr, jsonArray);

        //判断策略json转化成的PolicyJsonObj是否非法
        if (BucketPolicyUtils.isPolicyJsonObjError(bucketPolicys, bucketName)) {
            logger.error("policy json is invalid ");
            throw new MsException(ErrorNo.INVALID_POLICY, "The policy is invalid ");
        }

        paramMap.put("body", convertPolicyToJsonStr);

        //简化json中元素公共前缀
        BucketPolicyUtils.updateJson(jsonArray, POLICY_PRINCIPAL, PRINCIPAL_PREFIX);
        BucketPolicyUtils.updateJson(jsonArray, POLICY_RESOURCE, RESOURCE_PREFIX);

        /**从站点请求主站点执行**/
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);

        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_PUT_BUCKET_POLICY, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(SUCCESS);
        }

        List<Statement> statements = jsonArray.toJavaList(Statement.class);

        // 处理sid为null或者重复的情况
        updateStatementSid(statements);

        //存放策略信息
        String policys = bucketName + POLICY_SUFFIX;

        String spiltPolicy = JSONObject.toJSONString(statements);
        pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hset(policys, STATEMENTS_POLICES[0], spiltPolicy);

        int res = notifySlaveSite(paramMap, ACTION_PUT_BUCKET_POLICY);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave put bucket policy error!");
        }

        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    private void updateStatementSid(List<Statement> statements) {
        HashSet<String> idSet = new HashSet<>();
        HashSet<String> noIdSet = new HashSet<>();
        for (Statement statement : statements) {
            String sid = statement.getSid();
            if (StringUtils.isBlank(sid)) {
                sid = String.valueOf(System.currentTimeMillis());
                while (!noIdSet.add(sid)) {
                    sid = String.valueOf(System.currentTimeMillis());
                }
                statement.setSid(sid);
                continue;
            }
            if (!idSet.add(sid)) {
                throw new MsException(ErrorNo.INVALID_POLICY_SID, "The policy sid is invalid ");
            }
        }
    }

    /**
     * 将statement拆分
     */
    public static List<List<Statement>> spiltList(List<Statement> list, int subNum) {
        List<List<Statement>> tNewList = new ArrayList<List<Statement>>();
        int priIndex = 0;
        int lastPriIndex = 0;
        int insertTimes = list.size() / subNum;
        List<Statement> subList;
        for (int i = 0; i <= insertTimes; i++) {
            priIndex = subNum * i;
            lastPriIndex = priIndex + subNum;
            if (i == insertTimes) {
                subList = list.subList(priIndex, list.size());
            } else {
                subList = list.subList(priIndex, lastPriIndex);
            }
            if (subList.size() > 0) {
                tNewList.add(subList);
            }
        }
        return tNewList;
    }

    /**
     * fastjson 二维数组转List<List<Integer>>
     *
     * @param retireStringArray
     * @return
     */
    public static List<List<Integer>> parseStringToList(String retireStringArray) {
        List<List<Integer>> lists;
        lists = JSON.parseObject(retireStringArray,
                new TypeReference<List<List<Integer>>>() {
                });
        return lists;


    }

    /**
     * 获取桶策略
     *
     * @param paramMap
     * @return
     */
    public ResponseMsg getBucketPolicy(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);

        MsAclUtils.checkIfAnonymous(userId);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, false);
        userCheck(userId, bucketInfo.get(BUCKET_USER_ID));
        String method = "GetBucketPolicy";
        PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        // 判断桶是否含有桶策略


        //存放策略信息
        String policys = bucketName + POLICY_SUFFIX;
        if (pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).exists(policys) == 0) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET_POLICY, "The bucket policy is not exist in this bucket ");
        }

        String policy = pool.getCommand(REDIS_TASKINFO_INDEX).hget(policys, STATEMENTS_POLICES[0]);


        List<Statement> pols = new ArrayList<>();
        if (policy != null) {
            pols = JSON.parseObject(policy,
                    new TypeReference<List<Statement>>() {
                    });
        }

        //使用result存储还原后的列表
        Object result = JSONObject.toJSON(pols);
        BucketPolicyUtils.reductionJson(result, POLICY_PRINCIPAL, PRINCIPAL_PREFIX);
        BucketPolicyUtils.reductionJson(result, POLICY_RESOURCE, RESOURCE_PREFIX);

        BucketPolicy res = new BucketPolicy().setStatement((List<Statement>) result).setVersion("1.0");

        return new ResponseMsg(ErrorNo.SUCCESS_STATUS).setData(JSONObject.toJSONString(res));
    }

    /**
     * 删除桶策略
     *
     * @param paramMap
     * @return
     */
    public ResponseMsg deleteBucketPolicy(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        MsAclUtils.checkIfAnonymous(userId);

        String method = "DeleteBucketPolicy";
        PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        userCheck(userId, bucketInfo.get(BUCKET_USER_ID));

        /**从站点请求主站点执行**/
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        boolean isMasterCluster = StringUtils.isEmpty(masterCluster) || localCluster.equals(masterCluster);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_DEL_BUCKET_POLICY, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(DEL_SUCCESS);
        }


        //存放策略信息
        String policys = bucketName + POLICY_SUFFIX;

        //桶策略存放位置
        pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).del(policys);

        /**主站点执行多区域转发**/
        int res = notifySlaveSite(paramMap, ACTION_DEL_BUCKET_POLICY);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave delete bucket policy error!");
        }
        logger.info("delete the bucket {} policy successfully.", bucketName);

        return new ResponseMsg().setHttpCode(DEL_SUCCESS);
    }

    public static void deleteBucketPolicy(String bucketName) {
        pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).del(bucketName + POLICY_SUFFIX);
    }


    /**
     * 检查用户Id是否匹配
     *
     * @param userId       用户id
     * @param bucketUserId 桶用户的id
     */
    @Override
    protected void userCheck(String userId, String bucketUserId) {
        MsAclUtils.checkIfAnonymous(userId);

        if (!userId.equals(bucketUserId)) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,
                    "no permission.user " + userId + " can not delete bucket.");
        }
    }


    /**
     * 将JSON传来的带有空格、换行符、tab缩进、首字母大写的策略转换成标准的json字符串形式
     *
     * @return
     */
    public static String convertPolicyToJsonStr(String policyContent) {
        if (policyContent == null) {
            return null;
        }
        StringBuffer jsonForMatStr = new StringBuffer();
        // 当前字符是否在引号内
        boolean foundQuot = false;
        for (int i = 0; i < policyContent.length(); i++) {
            char c = policyContent.charAt(i);
            // 保证""中的内容不被去掉空格与换行
            if ('"' == c) {
                if (foundQuot) {
                    foundQuot = false;
                } else {
                    foundQuot = true;
                }
            }
            if (foundQuot) {
                jsonForMatStr.append(c);
                continue;
            }
            switch (c) {
                case ' ':
                    break;
                case '\t':
                    break;
                case '\n':
                    break;
                case '\r':
                    break;
                default:
                    jsonForMatStr.append(c);
                    break;
            }
        }
        return jsonForMatStr.toString();
    }


    /**
     * 校验principal 中value是否合法
     * true 表示不合法
     * mrn::iam::100570189158:user/exuser
     */
    public boolean principalCheck(String resources) {
        if (resources.startsWith(PRINCIPAL_PREFIX)) {
            String[] sourceSpilt = resources.split(resources);
            if (sourceSpilt.length < 2) {
                return true;
            }


        } else {
            String[] split = resources.split(":", 6);
            String mrn = split[0];
            String partition = split[1];
            String service = split[2];
            String region = split[3];
            String account = split[4];
            String resource = split[5];

            //service z只能是iam
            if (!"iam".equals(service)) {
                return true;
            }

            //accountId 不能为空
            if (!ACCOUNTID_PATTERN.matcher(account).matches()) {
                return true;
            }

            if ("root".equals(resource) || isMatch("user/", resource)) {
                return false;
            } else {
                return true;
            }
        }
        return false;
    }
}
