package com.macrosan.utils.ratelimiter;

public enum LimitStrategy {
    NO_LIMIT((normal, disk) -> LimitThreshold.getNo_limit()),
    ADAPT((normal, disk) -> normal > 0 ? normal / LimitThreshold.getAdapt() : 0),
    LIMIT((normal, disk) -> LimitThreshold.getLimit(disk)),   // 主用于大对象限流
    LIMIT_META((normal, disk) -> LimitThreshold.getLimit_meta());  // 主用于小对象限流

    private interface LimitStrategy0 {
        long getLimit(long normal, String disk);
    }

    LimitStrategy0 limitStrategy;

    LimitStrategy(LimitStrategy0 s) {
        limitStrategy = s;
    }

    public long getLimit(long normal, String disk) {
        return limitStrategy.getLimit(normal, disk);
    }
}
