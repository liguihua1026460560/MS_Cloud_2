package com.macrosan.utils.ratelimiter;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.rabbitmq.RabbitMqChannels;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 *@program: MS_Cloud
 *@description:
 *@author: niechengxing
 *@create: 2022-12-13 14:38
 */
public class UpdateLimitStrategy implements Job {
    protected static RedisConnPool pool = RedisConnPool.getInstance();

    private String limitStrategy;

    public void setLimitStrategy(String limitStrategy) {
        this.limitStrategy = limitStrategy;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        //定时任务更新生效策略变量
        RecoverLimiter.updateStrategy(LimitStrategy.valueOf(limitStrategy));
    }
}



