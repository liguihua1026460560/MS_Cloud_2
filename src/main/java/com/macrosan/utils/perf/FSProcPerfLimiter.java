package com.macrosan.utils.perf;

import reactor.core.publisher.Mono;

import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;

/**
 * 接口级别的限制。
 */
public class FSProcPerfLimiter extends BasePerfLimiter {
    private FSProcPerfLimiter() {
    }

    private static FSProcPerfLimiter instance = new FSProcPerfLimiter();

    public static FSProcPerfLimiter getInstance() {
        return instance;
    }

    public static final String FS_PROC_PERF = "fs_proc_perf";


    /**
     * 表2，fs_proc_perf，保存接口名和qos
     * 1) "NFS3PROC_CREATE_throughput_quota"
     * 2) "0"
     *
     * @param key   接口名称
     * @param value 性能配额的类型，带宽或吞吐量
     * @return
     */
    @Override
    protected Mono<Long> getQuotaHandler(String key, String value) {
        return pool.getReactive(REDIS_SYSINFO_INDEX).hget(FS_PROC_PERF, key + "_" + value).map(Long::parseLong);
    }
}
