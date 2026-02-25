package com.macrosan.constants;

/**
 * RedisConstants
 * 配置redis的常量
 * <p>
 * TODO 后续改成读配置文件
 *
 * @author liyixin
 * @date 2018/11/22
 */
public class RedisConstants {

    private RedisConstants() {
    }

    /**
     * Redis密码
     */
    public static final String REDIS_PASSWD = "Gw@uUp8tBedfrWDy";

    public static final String LOCALHOST = "127.0.0.1";

    public static final String THREAD_NAME = "parallel-";

    public static final String UDS_ADDRESS = "/tmp/redis.sock";

    public static final String IAM_UDS_ADDRESS = "/tmp/redis_iam.sock";

    public static final String UDS_6381_ADDRESS = "/tmp/redis_6381/redis_6381_iam.sock";
}
