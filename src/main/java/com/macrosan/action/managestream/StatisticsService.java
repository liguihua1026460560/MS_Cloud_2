package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.action.core.MergeMap;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.mongodb.MsMongoClient;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.ObjectStatisticsResult;
import com.macrosan.message.xmlmsg.TrafficStatisticsResult;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.regex.PatternConst;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;


/**
 * StatisticsService
 * 管理容量相关接口服务
 *
 * @author sunfang
 * @date 2019/7/10
 */
public class StatisticsService extends BaseService {
    /**
     * logger日志引用
     **/
    private static Logger logger = LogManager.getLogger(StatisticsService.class.getName());

    private static StatisticsService instance = null;

    private StatisticsService() {
        super();
    }

    /**
     * 每一个Service都必须提供一个getInstance方法
     */
    public static StatisticsService getInstance() {
        if (instance == null) {
            instance = new StatisticsService();
        }
        return instance;
    }

    /**
     * 获取某个时间段内统计信息
     * 包括：读请求次数，写请求次数，总请求次数，写流量大小，读流量大小，访问总流量大小
     *
     * @param paramMap 请求参数
     * @return xml结果
     */
    public ResponseMsg getTrafficStatistics(Map<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        Map<String, String> dataMap = findAccountInfoByIam(accountId);
        String startTime = paramMap.get(MGT_STAT_START_TIME);
        String endTime = paramMap.get(MGT_STAT_END_TIME);
        String requestsType = paramMap.get(MGT_STAT_REQ_TYPE);

        long startStamp = MsDateUtils.standardDateToStamp(startTime);
        long endStamp = MsDateUtils.standardDateToStamp(endTime);
        checkTrafficStatisticsParam(startTime, endTime, requestsType, startStamp, endStamp);

        dataMap.put(MGT_STAT_START_TIME, String.valueOf(startStamp));
        dataMap.put(MGT_STAT_END_TIME, String.valueOf(endStamp));
        dataMap.put(MGT_STAT_REQ_TYPE, requestsType);
        dataMap.put(REQUESTID, paramMap.get(REQUESTID));
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_GET_TRAFFIC_STATISTICS, 0)
                .setDataMap(dataMap);

        MapResMsg resMsg = sender.sendAndGetResponse(msg, MapResMsg.class, true);
        int retCode = resMsg.getCode();
        Map<String, String> resData = resMsg.getData();
        if (ErrorNo.SUCCESS_STATUS != retCode || null == resData) {
            throw new MsException(ErrorNo.SOCKET_MSG_ERROR, "param error when get traffic statistics!");
        }

        logger.info("getTrafficStatistics success: {}", resData);
        TrafficStatisticsResult trafficStatisticsResult = new TrafficStatisticsResult()
                .setStartTime(resData.get("startTime"))
                .setEndTime(resData.get("endTime"));
        if (MGT_STAT_REQ_TYPE_READ.equalsIgnoreCase(requestsType)) {
            trafficStatisticsResult.setReadTraffic(resData.get("readSize"));
            trafficStatisticsResult.setReadCount(resData.get("readCount"));
        } else if (MGT_STAT_REQ_TYPE_WRITE.equalsIgnoreCase(requestsType)) {
            trafficStatisticsResult.setWriteTraffic(resData.get("writeSize"));
            trafficStatisticsResult.setWriteCount(resData.get("writeCount"));
        } else {
            trafficStatisticsResult.setTotalTraffic(resData.get("totalSize"));
            trafficStatisticsResult.setTotalCount(resData.get("totalCount"));
        }

        return new ResponseMsg()
                .setData(trafficStatisticsResult)
                .addHeader(CONTENT_TYPE, "application/xml");

    }

    /**
     * 获取当前账户下对象统计信息
     * 包括：对象总个数、存储总容量
     *
     * @param paramMap 请求参数
     * @return xml结果
     */
    public ResponseMsg getObjectStatistics(Map<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        Map<String, String> accountInfo = findAccountInfoByIam(accountId);

        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_GET_OBJECT_STATISTICS, 0)
                .setDataMap(accountInfo);

        MapResMsg resMsg = sender.sendAndGetResponse(msg, MapResMsg.class, true);
        Map<String, String> resData = resMsg.getData();
        if (null == resData) {
            throw new MsException(ErrorNo.SOCKET_MSG_ERROR, "get account used capacity error!");
        }
        String storageCapacityUsed = resData.get("storageCapacityUsed");
        long objectCount = getAccountTotalObjectCount(accountId);

        ObjectStatisticsResult objectStatisticsResult = new ObjectStatisticsResult()
                .setObjectCount(String.valueOf(objectCount))
                .setStorageCapacityUsed(storageCapacityUsed);

        return new ResponseMsg()
                .setData(objectStatisticsResult)
                .addHeader(CONTENT_TYPE, "application/xml");
    }


    /**
     * 根据accountId找到对应的账户信息
     *
     * @param accountId accountId
     * @return account info
     */
    private Map<String, String> findAccountInfoByIam(String accountId) {
        Map<String, String> map = new HashMap<>(4);
        String accountName = pool.getCommand(REDIS_USERINFO_INDEX).hget(accountId, USER_DATABASE_ID_NAME);
        map.put(USER_DATABASE_NAME_ID, accountId);
        map.put(USER_DATABASE_NAME_TYPE, accountName);
        return map;
    }

    /**
     * 判断流量统计参数有效性
     *
     * @param startTime   YYYY-MM-DD HH:MM:SS
     * @param endTime     YYYY-MM-DD HH:MM:SS
     * @param requestType read|write|total
     */
    private static void checkTrafficStatisticsParam(String startTime, String endTime, String requestType,
                                                    long startStamp, long endStamp) {

        if (!(requestType.equalsIgnoreCase(MGT_STAT_REQ_TYPE_READ) || requestType.equalsIgnoreCase(MGT_STAT_REQ_TYPE_WRITE)
                || requestType.equalsIgnoreCase(MGT_STAT_REQ_TYPE_TOTAL))) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "Set requestType invalid when get traffic statistics.");
        }

        checkTimePattern(startTime);
        checkTimePattern(endTime);
        if (startStamp > endStamp) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "Time param error, startTime must <= endTime when get traffic statistics.");
        }
    }

    /**
     * 检查时间格式有效性
     *
     * @param time YYYY-MM-DD HH:MM:SS
     */
    private static void checkTimePattern(String time) {
        if (!PatternConst.TRAFFIC_STATIS_TIME_PATTERN.matcher(time).matches()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "Time param must set YYYY-MM-DD HH:MM:SS when get traffic statistics.");
        }
    }

    /**
     * 获取账户下对象总个数
     *
     * @param userId accountId
     * @return long
     */
    private long getAccountTotalObjectCount(String userId) {
        long count = 0L;
        String userBucketSet = userId + USER_BUCKET_SET_SUFFIX;
        Set<String> bucketSet = pool.getCommand(REDIS_USERINFO_INDEX).smembers(userBucketSet);

        for (String bucket : bucketSet) {
            String vnodeId = StoragePoolFactory.getMetaStoragePool(bucket).getBucketVnodeId(bucket);
            MergeMap<String, String> mergeMap = getTargetInfo(vnodeId);
            // MongoDB连接
            MsMongoClient mongoClient = MsMongoClient.getInstance(mergeMap.get(HEART_ETH1), mergeMap.get(HEART_ETH2),
                    mergeMap.get("take_over"));

            String database = MONGODB_COLLECTION_PREFIX + mergeMap.get(VNODE_LUN_NAME).split("-")[2];
            MongoCollection<Document> collection = mongoClient.getCollection(database, database);
            count += collection.countDocuments(Filters.eq("bucket", bucket));
        }
        logger.info("getAccountTotalObjectCount userId: {}, count: {}", userId, count);
        return count;
    }


}
