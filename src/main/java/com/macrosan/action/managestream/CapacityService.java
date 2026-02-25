package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.SysCapInfoResult;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.msutils.MsAclUtils;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.macrosan.constants.ServerConstants.CONTENT_TYPE;
import static com.macrosan.constants.ServerConstants.USER_ID;
import static com.macrosan.constants.SysConstants.REDIS_POOL_INDEX;


/**
 * CapacityService
 * 管理容量相关接口服务
 *
 * @author sunfang
 * @date 2019/6/28
 */
@Log4j2
public class CapacityService extends BaseService {

    /**
     * logger日志引用
     **/
    private static Logger logger = LogManager.getLogger(CapacityService.class.getName());

    private static CapacityService instance = null;

    private CapacityService() {
        super();
    }

    /**
     * 每一个Service都必须提供一个getInstance方法
     */
    public static CapacityService getInstance() {
        if (instance == null) {
            instance = new CapacityService();
        }
        return instance;
    }

    /**
     * 获取MOSS容量信息的操作：包括总容量、剩余容量、已使用容量
     *
     * @param paramMap 请求参数
     * @return xml结果
     */
//    public ResponseMsg getSysCapacityInfo(Map<String, String> paramMap) {
//        String userId = paramMap.get(USER_ID);
//
//        MsAclUtils.checkIfManageAccount(userId);
//        long metaPoolTotalCapacity = 0, metaPoolUsedCapacity = 0;
//        long dataPoolTotalCapacity = 0, dataPoolUsedCapacity = 0;
//        long esPoolTotalCapacity = 0, esPoolUsedCapacity = 0;
//        List<String> keys = pool.getCommand(REDIS_POOL_INDEX).keys("Pool_*");
//        for (String key : keys) {
//            Map<String, String> poolInfo = pool.getCommand(REDIS_POOL_INDEX).hgetall(key);
//            long total_size = Long.parseLong(Optional.ofNullable(poolInfo.get("total_size")).orElse("0"));
//            long used_size = Long.parseLong(Optional.ofNullable(poolInfo.get("used_size")).orElse("0"));
//            if (StringUtils.isEmpty(poolInfo.get("role"))) {
//                continue;
//            }
//            switch (poolInfo.get("role")) {
//                case "meta":
//                    metaPoolTotalCapacity += total_size;
//                    metaPoolUsedCapacity += used_size;
//                    break;
//                case "data":
//                    dataPoolTotalCapacity += total_size;
//                    dataPoolUsedCapacity += used_size;
//                    break;
//                case "es":
//                    esPoolTotalCapacity += total_size;
//                    esPoolUsedCapacity += used_size;
//                    break;
//                default:
//                    break;
//            }
//        }
//
//        long totalNakedCapacity = metaPoolTotalCapacity + dataPoolTotalCapacity + esPoolTotalCapacity;
//        long usedNakedCapacity = metaPoolUsedCapacity + dataPoolUsedCapacity + esPoolUsedCapacity;
//        long remainingNakedCapacity = totalNakedCapacity - usedNakedCapacity;
//        StoragePool metaPool = StoragePoolFactory.getStoragePoolForDefaultStrategy(StorageOperate.META);
//        StoragePool dataPool = StoragePoolFactory.getStoragePoolForDefaultStrategy(StorageOperate.DATA);
//        long totalAvailableCapacity = metaPoolTotalCapacity / (metaPool.getK() + metaPool.getM()) + dataPoolTotalCapacity / (dataPool.getK() + dataPool.getM()) * dataPool.getK() + esPoolTotalCapacity;
//        long usedAvailableCapacity = metaPoolUsedCapacity / (metaPool.getK() + metaPool.getM()) + dataPoolUsedCapacity / (dataPool.getK() + dataPool.getM()) * dataPool.getK() + esPoolUsedCapacity;
//        long remainingAvailableCapacity = totalAvailableCapacity - usedAvailableCapacity;
//
//        SysCapInfoResult sysCapInfoResult = new SysCapInfoResult()
//                .setTotalCapacity(String.valueOf(totalNakedCapacity))
//                .setUsedCapacity(String.valueOf(usedNakedCapacity))
//                .setAvailableCapacity(String.valueOf(remainingNakedCapacity))
//                .setTotalAvailableCapacity(String.valueOf(totalAvailableCapacity))
//                .setUsedAvailableCapacity(String.valueOf(usedAvailableCapacity))
//                .setRemainingAvailableCapacity(String.valueOf(remainingAvailableCapacity));
//
//        return new ResponseMsg()
//                .setData(sysCapInfoResult)
//                .addHeader(CONTENT_TYPE, "application/xml");
//
//    }
    public ResponseMsg getSysCapacityInfo(Map<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);

        MsAclUtils.checkIfManageAccount(userId);
        long totalNakedCapacity = 0;
        long usedNakedCapacity = 0;
        long totalAvailableCapacity = 0;
        long usedAvailableCapacity = 0;

        List<String> keys = pool.getCommand(REDIS_POOL_INDEX).keys("Pool_*");
        for (String key : keys) {
            Map<String, String> poolInfo = pool.getCommand(REDIS_POOL_INDEX).hgetall(key);
            long totalSize = Long.parseLong(Optional.ofNullable(poolInfo.get("total_size")).orElse("0"));
            long usedSize = Long.parseLong(Optional.ofNullable(poolInfo.get("used_size")).orElse("0"));
            if (StringUtils.isEmpty(poolInfo.get("role"))) {
                continue;
            }

            StoragePool storagePool = StoragePoolFactory.getStoragePool(poolInfo.get("prefix"), null);
            switch (poolInfo.get("role")) {
                case "meta":
                    totalNakedCapacity += totalSize;
                    usedNakedCapacity += usedSize;
                    totalAvailableCapacity += (totalSize / (storagePool.getK() + storagePool.getM()));
                    usedAvailableCapacity += (usedSize / (storagePool.getK() + storagePool.getM()));
                    break;
                case "data":
                    totalNakedCapacity += totalSize;
                    usedNakedCapacity += usedSize;
                    totalAvailableCapacity += (totalSize / (storagePool.getK() + storagePool.getM()) * storagePool.getK());
                    usedAvailableCapacity += (usedSize / (storagePool.getK() + storagePool.getM()) * storagePool.getK());
                    break;
                case "es":
                    totalNakedCapacity += totalSize;
                    usedNakedCapacity += usedSize;
                    totalAvailableCapacity += totalSize;
                    usedAvailableCapacity += usedSize;
                    break;
                default:
                    break;
            }
        }

        long remainingNakedCapacity = totalNakedCapacity - usedNakedCapacity;
        long remainingAvailableCapacity = totalAvailableCapacity - usedAvailableCapacity;

        SysCapInfoResult sysCapInfoResult = new SysCapInfoResult()
                .setTotalCapacity(String.valueOf(totalNakedCapacity))
                .setUsedCapacity(String.valueOf(usedNakedCapacity))
                .setAvailableCapacity(String.valueOf(remainingNakedCapacity))
                .setTotalAvailableCapacity(String.valueOf(totalAvailableCapacity))
                .setUsedAvailableCapacity(String.valueOf(usedAvailableCapacity))
                .setRemainingAvailableCapacity(String.valueOf(remainingAvailableCapacity));

        return new ResponseMsg()
                .setData(sysCapInfoResult)
                .addHeader(CONTENT_TYPE, "application/xml");

    }
}

