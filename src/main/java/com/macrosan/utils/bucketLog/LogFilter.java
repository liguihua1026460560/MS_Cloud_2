package com.macrosan.utils.bucketLog;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.MsHttpRequest;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;

/**
 * @author zhangzhixin
 */
@Log4j2
public class LogFilter {

    private static final RedisConnPool redisConnPool = RedisConnPool.getInstance();
    private static String INTERNAL_INVOCATION = "/?ObjectStatistics";

    public static Mono<Boolean> judgeRequest(MsHttpRequest request) {
        if (INTERNAL_INVOCATION.equals(request.uri())) {
            return Mono.just(false);
        }

        if (request.getBucketName() != null) {
            String bucket = request.getBucketName();
            String memAddress = request.getMember("address");
            List<Map.Entry<String, String>> headerList = request.headers().entries();
            for (Map.Entry<String, String> entry : headerList) {
                // 多站点间请求不记录桶日志
                if (CLUSTER_ALIVE_HEADER.equals(entry.getKey()) && !LOCAL_IP_ADDRESS.equals(entry.getValue()) && !"ip".equals(entry.getValue())) {
                    return Mono.just(false);
                }
                if (IS_SYNCING.equals(entry.getKey())) {
                    return Mono.just(false);
                }
                if (SITE_FLAG.equals(entry.getKey())) {
                    return Mono.just(false);
                }
            }
            if (memAddress != null) {
                if (StringUtils.isBlank(memAddress)) {
                    return Mono.just(false);
                } else {
                    return Mono.just(true);
                }
            }

            return redisConnPool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucket, "address")
                    .map(s -> true)
                    .defaultIfEmpty(false);
        }

        return Mono.just(false);
    }


}
