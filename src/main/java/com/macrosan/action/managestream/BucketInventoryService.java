package com.macrosan.action.managestream;

import com.auth0.jwt.exceptions.TokenExpiredException;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.inventory.InventoryField;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.inventory.*;
import com.macrosan.utils.authorize.JwtUtils;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.PolicyCheckUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import io.jsonwebtoken.Claims;
import io.lettuce.core.Range;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.macrosan.constants.ErrorNo.SUCCESS_STATUS;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DoubleActiveUtil.notifySlaveSite;
import static com.macrosan.doubleActive.arbitration.BucketSyncSwitchCache.isSwitchOn;
import static com.macrosan.filesystem.utils.CheckUtils.bucketFsCheck;
import static com.macrosan.utils.regex.PatternConst.*;

/**
 * <p>
 *    配置桶的Inventory相关信息
 * </p>
 */

public class BucketInventoryService extends BaseService {
    public static final Logger logger = LoggerFactory.getLogger(BucketInventoryService.class);
    public static final String SUFFIX = "_inventory";
    public static final String SUFFIX_ID = "_inventory_id";
    private static final int MAX_KEYS = 1;
    private static final int MAX_PAGE_SIZE = 10;
    private static final int SCORE = 1;

    private BucketInventoryService() {}
    private static BucketInventoryService instance;
    public static BucketInventoryService getInstance() {
        if (instance == null) {
            instance = new BucketInventoryService();
        }
        return instance;
    }

    static void checkBucketName(String bucketName) {
        if (!BUCKET_NAME_PATTERN.matcher(bucketName).matches()) {
            if (StringUtils.isEmpty(bucketName) || ILLEGAL_CHARACTER.matcher(bucketName).find() || CHINESE_CHARACTER.matcher(bucketName).find()) {
                throw new MsException(ErrorNo.BUCKET_NAME_INPUT_ERR, "Invalid bucketName " + bucketName);
            } else {
                throw new MsException(ErrorNo.NO_SUCH_BUCKET, "Invalid bucketName " + bucketName);
            }
        }
    }

    static void checkDestinationBucketName(String bucketName) {
        if (!BUCKET_NAME_PATTERN.matcher(bucketName).matches()) {
            throw new MsException(ErrorNo.InvalidS3DestinationBucket, "Invalid destination bucketName " + bucketName);
        }
    }

    /**
     * 设置桶的Inventory配置信息
     * @param paramMap 请求参数
     * @return 响应结果
     */
    public ResponseMsg putBucketInventory(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String userName = paramMap.get(USERNAME);
        String bucketName = paramMap.get(BUCKET_NAME);
        String inventoryId = paramMap.get(ID);
        checkBucketName(bucketName);
        // 权限校验
        MsAclUtils.checkIfAnonymous(userId);
        //只有桶的所有者可以操作
        String method = "PutBucketInventory";
        PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        if (bucketFsCheck(bucketName)){
            throw new MsException(ErrorNo.NFS_NOT_STOP, "The bucket already start nfs ro cifs, can not create bucketInventory");
        }
        String body = paramMap.get(BODY);
        logger.info(bucketName + " inventoryConfiguration:" + body);

        if (StringUtils.isEmpty(inventoryId) || StringUtils.isEmpty(body)) {
            InventoryValidation.throwInvalidArgumentException("inventory id or body can't be empty!");
        }

        /** --------------------------     输入参数检查    -------------------------------------------------**/
        InventoryConfiguration inventoryConfiguration = (InventoryConfiguration) JaxbUtils.toObject(body);
        InventoryValidation.validationInventoryConfiguration(inventoryConfiguration);
        String id = inventoryConfiguration.getId();
        if (!inventoryId.equals(id)) {
            throw new MsException(ErrorNo.INVALID_INVENTORY_ID, "The inventory id in the request parameters must be consistent with the parameter identification in the InventoryConfiguration ID.");
        }

        if(inventoryConfiguration.getInventoryType() != null){
            String inventoryType = inventoryConfiguration.getInventoryType();
            if(!"Total".equals(inventoryType) && !"Incremental".equals(inventoryType)){
                throw new MsException(ErrorNo.InvalidInventoryType, "InventoryType in inventory is invalid");
            }
        }

//        if(inventoryConfiguration.getInventoryType() != null && "Incremental".equals(inventoryConfiguration.getInventoryType())){
//            String bucketHashSwitch = pool.getCommand(REDIS_SYSINFO_INDEX).get("bucket_hash_switch");
//            if("1".equals(bucketHashSwitch)){
//                throw new MsException(ErrorNo.InvalidInventory, "InventoryType in inventory is invalid when bucket hash switch is on");
//            }
//        }

        /** --------------------------   处理双活请求:从站点请求主站点执行   ----------------------------------------------**/
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        paramMap.put("body",body);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_PUT_INVENTORY, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(SUCCESS);
        }

        /** -----------------------------      权限校验    --------------------------------------------------- **/
        userCheck(userId,bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        String destBucket = inventoryConfiguration.getDestination().getS3BucketDestination().getBucket();
        String object = inventoryConfiguration.getDestination().getS3BucketDestination().getPrefix();
        Map<String, String> destinationBucketInfo = pool.getCommand(SysConstants.REDIS_BUCKETINFO_INDEX).hgetall(destBucket);
        Map<String, String> sourceBucketInfo = pool.getCommand(SysConstants.REDIS_BUCKETINFO_INDEX).hgetall(bucketName);

        // 若源桶与目标桶都未启用数据同步，则只源桶与目标桶的站点属性必须一致
        String destinationSite = destinationBucketInfo.get(CLUSTER_NAME);
        String sourceSite = sourceBucketInfo.get(CLUSTER_NAME);
        if (!isSwitchOn(destinationBucketInfo) && !isSwitchOn(sourceBucketInfo) && !destinationSite.equals(sourceSite)) {
            throw new MsException(ErrorNo.INVALID_SITE_CONSTRAINT, "The site specified by the source bucket and destination bucket is inconsistent.");
        }
        // 源源桶启用数据同步，目标桶也必须启用数据同步功能
        if (isSwitchOn(sourceBucketInfo) && !isSwitchOn(destinationBucketInfo)) {
            throw new MsException(ErrorNo.InvalidS3DestinationBucket, "InvalidS3DestinationBucket");
        }

        // 目标桶与源桶的所属区域需要一致
        String regionName = destinationBucketInfo.get("region_name");
        String sourceRegionName = sourceBucketInfo.get("region_name");
        // 目标桶与源桶所属区域必须一致
        if (regionName != null && sourceRegionName != null && !regionName.equals(sourceRegionName)) {
            throw new MsException(ErrorNo.INVALID_LOCATION_CONSTRAINT, "the bucket does not belong to the current region.");
        }

        object = StringUtils.isEmpty(object) ? "BucketInventory" : object;
        paramMap.put(BUCKET_NAME, destBucket);
        final int checkResult = PolicyCheckUtils.getPolicyResult(paramMap, destBucket, object, userId, "PutObject");
        paramMap.put(BUCKET_NAME, bucketName);

        // 查询是否有目标桶的写入权限
        if(checkResult == 0){
            MsAclUtils.checkWriteAcl(destinationBucketInfo, userId, destBucket);
        }

        // 清单数不能超过10条
        Long lrange = pool.getCommand(SysConstants.REDIS_TASKINFO_INDEX).zcount(bucketName + SUFFIX_ID, Range.create(0, SCORE));
        Boolean hexists = pool.getCommand(SysConstants.REDIS_TASKINFO_INDEX).hexists(bucketName + SUFFIX, inventoryId);
        if (lrange >= MAX_KEYS && !hexists) {
            throw new MsException(ErrorNo.TOO_MANY_CONFIGURATIONS, "You are attempting to create a new configuration but have already reached the 10-configuration limit.");
        }

        if(inventoryConfiguration.getFilter() != null && StringUtils.isNotEmpty(inventoryConfiguration.getFilter().getPrefix()) ){
            throw new MsException(ErrorNo.InvalidFilterPrefix, "can not set prefix in inventory");
        }

        // 清单任务之间是否存在前端包含关系
        List<String> prefix = filterPrefix(bucketName, inventoryId);
        if (hexists) {
            String oldConfig = pool.getCommand(SysConstants.REDIS_TASKINFO_INDEX).hget(bucketName + SUFFIX, inventoryId);
            InventoryConfiguration oldInventoryConfig = (InventoryConfiguration) JaxbUtils.toObject(oldConfig);
            boolean noPrefixLastest = oldInventoryConfig != null &&
                    (oldInventoryConfig.getFilter() == null || StringUtils.isEmpty(oldInventoryConfig.getFilter().getPrefix()));
            boolean noPrefix = (inventoryConfiguration.getFilter() == null )
                    || (StringUtils.isEmpty(inventoryConfiguration.getFilter().getPrefix()));

            if (oldInventoryConfig == null || (noPrefixLastest && noPrefix)
               || (oldInventoryConfig.getFilter() !=null
                    && oldInventoryConfig.getFilter().getPrefix() != null
                    && inventoryConfiguration.getFilter() != null
                    && oldInventoryConfig.getFilter().getPrefix().equals(inventoryConfiguration.getFilter().getPrefix()))) {
            } else {
                checkPrefix(prefix, inventoryConfiguration);
            }
        } else {
            checkPrefix(prefix, inventoryConfiguration);
        }

        // 记录至redis中
        String configuration = new String(JaxbUtils.toByteArray(inventoryConfiguration));
        pool.getShortMasterCommand(SysConstants.REDIS_TASKINFO_INDEX).hset(bucketName + SUFFIX, inventoryId, configuration);
        pool.getShortMasterCommand(SysConstants.REDIS_TASKINFO_INDEX).zadd(bucketName + SUFFIX_ID, SCORE, inventoryId);

         /**---------------------------------处理双活请求:同步至其他站点------------------------------------------**/
        int res = notifySlaveSite(paramMap, ACTION_PUT_BUCKET_INVENTORY);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave put bucket version error!");
        }

        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    private static void checkPrefix(List<String> prefix, InventoryConfiguration inventoryConfiguration) {
        if (prefix.contains("All")) {
            throw new MsException(ErrorNo.PrefixExistInclusionRelationship, "PrefixExistInclusionRelationship");
        }

        if ((inventoryConfiguration.getFilter() == null || StringUtils.isEmpty(inventoryConfiguration.getFilter().getPrefix())) && !prefix.isEmpty()) {
            throw new MsException(ErrorNo.PrefixExistInclusionRelationship, "PrefixExistInclusionRelationship");
        }

        if (inventoryConfiguration.getFilter() != null && StringUtils.isNotEmpty(inventoryConfiguration.getFilter().getPrefix())) {
            if (prefix.contains(inventoryConfiguration.getFilter().getPrefix())) {
                throw new MsException(ErrorNo.PrefixExistInclusionRelationship, "PrefixExistInclusionRelationship");
            }

            for (String pre : prefix) {
                if (inventoryConfiguration.getFilter().getPrefix().startsWith(pre) || pre.startsWith(inventoryConfiguration.getFilter().getPrefix())) {
                    throw new MsException(ErrorNo.PrefixExistInclusionRelationship, "PrefixExistInclusionRelationship");
                }
            }
        }
    }

    /**
     * 获取桶的Inventory配置信息
     * @param paramMap 请求参数
     * @return 响应结果
     */
    public ResponseMsg getBucketInventory(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String inventoryId = paramMap.get(ID);
        checkBucketName(bucketName);
        MsAclUtils.checkIfAnonymous(userId);
        userCheck(userId,bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, false);
        //只有桶的所有者可以操作
        String method = "GetBucketInventory";
        PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        String inventoryConfiguration = pool.getCommand(SysConstants.REDIS_TASKINFO_INDEX).hget(bucketName + SUFFIX, inventoryId);

        if(inventoryConfiguration == null) {
            throw new MsException(ErrorNo.NO_INVENTORY_CONFIGURATION, "the inventory configuration does not exist.");
        }

        return new ResponseMsg().setData(inventoryConfiguration).addHeader(CONTENT_TYPE, "application/xml")
                .addHeader(CONTENT_LENGTH, String.valueOf(inventoryConfiguration.getBytes().length));
    }

    /**
     * 删除桶的Inventory配置信息
     * @param paramMap 请求参数
     * @return 响应结果
     */
    public ResponseMsg deleteBucketInventory(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        String inventoryId = paramMap.get(ID);
        checkBucketName(bucketName);
        MsAclUtils.checkIfAnonymous(userId);
        //只有桶的所有者可以操作
        String method = "DeleteBucketInventory";
        PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        if (StringUtils.isEmpty(bucketName) || StringUtils.isEmpty(inventoryId)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "invalid bucket or inventory Id");
        }

        /**从站点请求主站点执行**/
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(paramMap, MSG_TYPE_SITE_DEL_INVENTORY, localCluster, masterCluster)) {
            return new ResponseMsg().setHttpCode(DEL_SUCCESS);
        }

        userCheck(userId,bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));

        Boolean hexists = pool.getCommand(SysConstants.REDIS_TASKINFO_INDEX).hexists(bucketName + SUFFIX, inventoryId);
        if (!hexists) {
            throw new MsException(ErrorNo.NO_INVENTORY_CONFIGURATION, "the inventory configuration does not exist.");
        }

        pool.getShortMasterCommand(SysConstants.REDIS_TASKINFO_INDEX).hdel(bucketName + SUFFIX, inventoryId);
        pool.getShortMasterCommand(SysConstants.REDIS_TASKINFO_INDEX).zrem(bucketName + SUFFIX_ID, inventoryId);
        pool.getShortMasterCommand(SysConstants.REDIS_TASKINFO_INDEX).hdel(bucketName + "_archive", inventoryId);
        pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hdel(bucketName + "_incrementalStamp", inventoryId);

        logger.info("delete the bucket {} inventory id {} successfully.", bucketName, inventoryId);

        int res = notifySlaveSite(paramMap, ACTION_DEL_BUCKET_INVENTORY);
        if (res != SUCCESS_STATUS) {
            throw new MsException(res, "slave put bucket version error!");
        }

        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    /**
     * 列举桶的Inventory配置信息
     * @param paramMap 请求参数
     * @return 响应结果
     */
    public ResponseMsg listBucketInventory(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);
        checkBucketName(bucketName);
        String continueToken = paramMap.get("continuation-token");
        MsAclUtils.checkIfAnonymous(userId);
        userCheck(userId, bucketName);
        DoubleActiveUtil.siteConstraintCheck(bucketName, false);
        String sk = userId;
        //只有桶的所有者可以操作
        String method = "ListBucketInventory";
        PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);

        int pageNum = StringUtils.isEmpty(continueToken) ? 1 : getPageNumByContinueToken(continueToken, sk);
        int start = (pageNum - 1) * MAX_PAGE_SIZE;
        int end = start + MAX_PAGE_SIZE - 1;
        List<String> inventoryIdList = pool.getCommand(SysConstants.REDIS_TASKINFO_INDEX).zrange(bucketName + SUFFIX_ID, start, end);
        List<InventoryConfiguration> inventoryConfigurations = inventoryIdList.stream()
                .map(id -> pool.getCommand(SysConstants.REDIS_TASKINFO_INDEX).hget(bucketName + SUFFIX, id))
                .filter(Objects::nonNull)
                .map(configString -> (InventoryConfiguration) JaxbUtils.toObject(configString))
                .map(inventoryConfiguration -> StringUtils.isNotEmpty(inventoryConfiguration.getInventoryType()) ? inventoryConfiguration : inventoryConfiguration.setInventoryType("Total"))
                .collect(Collectors.toList());

        boolean isTruncated;
        if (inventoryIdList.isEmpty() || inventoryConfigurations.isEmpty()) {
            isTruncated = false;
        } else {
            String last = inventoryIdList.get(inventoryIdList.size() - 1);
            Long zlexcount = pool.getCommand(SysConstants.REDIS_TASKINFO_INDEX).zlexcount(bucketName + SUFFIX_ID, Range.create("(" + last, "+"));
            isTruncated = zlexcount != 0;
        }

        String nextContinueToken = isTruncated ? produceContinueTokenByPageNum(pageNum + 1, sk, userId) : "";

        ListInventoryConfigurationsResult listInventoryConfigurationsResult = new ListInventoryConfigurationsResult();
        listInventoryConfigurationsResult.setInventoryConfigurations(inventoryConfigurations);
        listInventoryConfigurationsResult.setTruncated(isTruncated);
        listInventoryConfigurationsResult.setContinuationToken(continueToken);
        listInventoryConfigurationsResult.setNextContinuation(nextContinueToken);
        byte[] bytes = JaxbUtils.toByteArray(listInventoryConfigurationsResult);

        // 获取每个清单配置最近的
        ResponseMsg responseMsg = new ResponseMsg();
        Map<String, String> map = pool.getCommand(SysConstants.REDIS_TASKINFO_INDEX).hgetall(bucketName + "_lastTime");
        map.forEach((key, value) -> responseMsg.addHeader("latest-time-" + key, value));
        return responseMsg.setData(bytes)
                .addHeader(CONTENT_TYPE, "application/xml")
                .addHeader(CONTENT_LENGTH, String.valueOf(bytes.length));
    }

    private static List<String> filterPrefix(String bucketName, String inventoryId) {
        List<String> hvals = pool.getCommand(SysConstants.REDIS_TASKINFO_INDEX).hvals(bucketName + "_inventory");
        return hvals.stream().filter(StringUtils::isNotEmpty)
                .map(config -> (InventoryConfiguration)JaxbUtils.toObject(config))
                .filter(Objects::nonNull)
                .filter(inventoryConfiguration -> !inventoryConfiguration.getId().equals(inventoryId))
                .map(inventoryConfiguration -> {
                    if (inventoryConfiguration.getFilter() == null || StringUtils.isEmpty(inventoryConfiguration.getFilter().getPrefix())) {
                        return "All";
                    }
                    return inventoryConfiguration.getFilter().getPrefix();
                })
                .collect(Collectors.toList());
    }

    /**
     * 从token令牌中获取信息
     * @param continueToken token
     * @param sk 账户密钥
     * @return 页码
     */
    private static int getPageNumByContinueToken(String continueToken, String sk) {
        try {
            Claims claims = JwtUtils.parseJwtToken(continueToken, sk);
            return claims.get("pageNum", Integer.class);
        } catch (TokenExpiredException e) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "token has expired.");
        } catch (Exception e) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "invalid request token!");
        }
    }

    /**
     * 将账户请求信息生成token
     * @param pageNum 页码
     * @param sk 用户密钥
     * @param userId 用户id
     * @return token字符串
     */
    private static String produceContinueTokenByPageNum(int pageNum, String sk, String userId) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("pageNum", pageNum);
        return JwtUtils.generateJwtToken(payload, 10 * 60 * 1000L, sk, userId);
    }

    /**
     * 校验Inventory配置的合法性
     */
    public static class InventoryValidation {

        static void validationInventoryConfiguration(InventoryConfiguration inventoryConfiguration) {
            try {
                Objects.requireNonNull(inventoryConfiguration);

                isValidInventoryId(inventoryConfiguration.getId());

                validationDestination(inventoryConfiguration.getDestination());

                String objectVersions = inventoryConfiguration.getIncludedObjectVersions();
                if (StringUtils.isEmpty(objectVersions) || !"All".equals(objectVersions) && !"Current".equals(objectVersions)) {
                    throw new MsException(ErrorNo.INVALID_OBJECT_VERSIONS, "IncludeObjectVersions cannot be empty and can only be All or Current.");
                }

                if (inventoryConfiguration.getOptionalFields() != null && inventoryConfiguration.getOptionalFields().getFields() != null) {
                    validateOptionField(inventoryConfiguration.getOptionalFields().getFields());
                } else {
                    // 若没有设置OptionFields则默认添加桶名，对象名
                    inventoryConfiguration.setOptionalFields(new OptionalFields().setFields(Arrays.asList("Bucket", "Key")));
                }

                validationSchedule(inventoryConfiguration.getSchedule());

                String prefix = inventoryConfiguration.getFilter() != null ? inventoryConfiguration.getFilter().getPrefix() : null;
                if (StringUtils.isNotEmpty(prefix) && prefix.length() > 1024) {
                    throw new MsException(ErrorNo.InvalidFilterPrefix, "InvalidFilterPrefix:" + prefix);
                }

                String isEnabled = inventoryConfiguration.getIsEnabled();
                if (StringUtils.isEmpty(isEnabled) || (!"true".equalsIgnoreCase(isEnabled) && !"false".equalsIgnoreCase(isEnabled))) {
                    throw new MsException(ErrorNo.INVALID_ARGUMENT, "Invalid parameter isEnabled.");
                } else {
                    inventoryConfiguration.setIsEnabled(isEnabled.toLowerCase());
                }


            } catch (NullPointerException e) {
                throwInvalidArgumentException("InventoryConfiguration");
            }
        }

        static void validationDestination(Destination destination) {
            try {
                Objects.requireNonNull(destination);
                Objects.requireNonNull(destination.getS3BucketDestination());
                if (ServerConfig.isBucketUpper()) {
                    destination.getS3BucketDestination().setBucket(destination.getS3BucketDestination().getBucket().toLowerCase());
                }
                String bucket = destination.getS3BucketDestination().getBucket();
                checkDestinationBucketName(bucket);
                if (StringUtils.isEmpty(bucket)) {
                    throw new MsException(ErrorNo.NO_SPECIFIED_DEST_BUCKET, "Target bucket for which inventory is not specified.");
                }

                Long exists = pool.getCommand(SysConstants.REDIS_BUCKETINFO_INDEX).exists(bucket);
                if (exists == 0) {
                    throw new MsException(ErrorNo.NO_SUCH_DEST_BUCKET, "destination bucket don't exists.");
                }

                // 目标存储桶的账户id
                String accountId = destination.getS3BucketDestination().getAccountId();
                if (StringUtils.isNotEmpty(accountId)) {
                    String userId = pool.getCommand(SysConstants.REDIS_BUCKETINFO_INDEX).hget(bucket, "user_id");
                    if (!accountId.equals(userId)) {
                        throw new MsException(ErrorNo.INVALID_INVENTORY_ACCOUNT_ID, "The specified account ID is not the owner of the target bucket.");
                    }
                }

                String prefix = destination.getS3BucketDestination().getPrefix();
                if (StringUtils.isNotEmpty(prefix) && prefix.length() > 512) {
                    throw new MsException(ErrorNo.InvalidS3DestinationBucketPrefix, "InvalidS3DestinationBucketPrefix");
                }

                String format = destination.getS3BucketDestination().getFormat();
                if (StringUtils.isEmpty(format) || !"CSV".equals(format)) {
                    throw new MsException(ErrorNo.INVALID_INVENTORY_FORMAT, "The inventory file format is invalid");
                }
            } catch (NullPointerException e) {
                throwInvalidArgumentException("Destination");
            }
        }

        static void validateOptionField(List<String> list) {
            if (!InventoryField.fields().containsAll(list)) {
                throw new MsException(ErrorNo.INVALID_OPTIONAL_FIELDS, "OptionField contains an invalid field.");
            }

            if (!list.contains("Key")) {
                list.add(0, "Key");
            }

            if (!list.contains("Bucket")) {
                list.add(0, "Bucket");
            }
        }

        static void validationSchedule(Schedule schedule) {
            try {
                Objects.requireNonNull(schedule);
                String frequency = schedule.getFrequency();
                if (StringUtils.isEmpty(frequency) || !"Daily".equals(frequency) && !"Weekly".equals(frequency)) {
                    throw new MsException(ErrorNo.INVALID_SCHEDULE, "Schedule Frequency cannot be empty and can only be Daily or Weekly.");
                }
            } catch (NullPointerException e) {
                throw new MsException(ErrorNo.INVALID_SCHEDULE, "Schedule Frequency cannot be empty and can only be Daily or Weekly.");
            }
        }

        static void throwInvalidArgumentException(String msg) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, msg);
        }

        static void isValidInventoryId(final String inventoryId) {
            if (StringUtils.isEmpty(inventoryId)) {
                throw new MsException(ErrorNo.INVALID_INVENTORY_ID, "The inventory ID cannot be empty.");
            }

            if (inventoryId.length() < 1 || inventoryId.length() > 64) {
                throw new MsException(ErrorNo.INVALID_INVENTORY_ID, "Invalid argument \"" + inventoryId + "\".inventoryId's length must between 1 and 64 characters");
            }

            for (int i = 0; i < inventoryId.length(); ++i) {
                char next = inventoryId.charAt(i);

                if (next == ' ' || next == '\t' || next == '\r' || next == '\n') {
                    throw new MsException(ErrorNo.INVALID_INVENTORY_ID, "Invalid argument \"" + inventoryId + "\". InventoryId should not contain whitespace");
                }

                if (next == '.') {
                    continue;
                }

                if (next == '_') {
                    continue;
                }

                if (next == '-') {
                    continue;
                }

                if (next >= 'a' && next <= 'z') {
                    continue;
                }

                if (next >= 'A' && next <= 'Z') {
                    continue;
                }

                if (next >= '0' && next <= '9') {
                    continue;
                }

                throw new MsException(ErrorNo.INVALID_INVENTORY_ID, "The inventory ID is invalid.");
            }
        }
    }
}