package com.macrosan.utils.trash;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.notification.BucketNotification;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.SysConstants.REDIS_BUCKETINFO_INDEX;

/**
 * @author zhangzhixin
 */

@Log4j2
public class TrashUtils {
    public static Map<String,String> bucketTrash = new HashMap<>();
    private static final RedisConnPool POOL = RedisConnPool.getInstance();

    private static TrashUtils instance;

    public static TrashUtils getInstance() {
        if (instance == null) {
            instance = new TrashUtils();
        }
        return instance;
    }

    public static void getBucketTrash(){
        Map<String,String> tempMap = new HashMap<>();
        List<String> buckets = POOL.getCommand(REDIS_BUCKETINFO_INDEX).keys("*");
        buckets.forEach(bucket ->{
            String type = POOL.getCommand(REDIS_BUCKETINFO_INDEX).type(bucket);
            if (type.equals("hash")){
                String trashDir = POOL.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket,"trashDir");
                if (StringUtils.isNotEmpty(trashDir)){
                    tempMap.put(bucket,trashDir);
                }
            }
        });
        bucketTrash = tempMap;
        ErasureServer.DISK_SCHEDULER.schedule(TrashUtils::getBucketTrash,10, TimeUnit.SECONDS);
    }

    public static void checkEnvTrash(String bucket, RedisConnPool pool){
        String result = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket,"trashDir");
        if (!StringUtils.isEmpty(result)){
            throw new MsException(ErrorNo.BUCKET_OPEN_VERSION,"the open error");
        }
    }

    public static void checkEnvVersion(String bucket, RedisConnPool pool){
        String result = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket,"versionstatus");
        if (!StringUtils.isEmpty(result)){
            throw new MsException(ErrorNo.BUCKET_OPEN_VERSION,"the bucket is version");
        }
    }
}
