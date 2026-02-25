package com.macrosan.utils.ratelimiter;/**
 * @author niechengxing
 * @create 2022-12-13 15:42
 */

import lombok.extern.log4j.Log4j2;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.quartz.CronScheduleBuilder.cronSchedule;

/**
 *@program: MS_Cloud
 *@description:
 *@author: niechengxing
 *@create: 2022-12-13 15:42
 */
@Log4j2
public class JobScheduler {
    public static final String limitStrategyJobGroup = "LimitStrategyJobGroup";
    public static final String limitStrategyTriggerGroup = "LimitStrategyTriggerGroup";
    public static Scheduler scheduler;

    static {

        try {
            scheduler = StdSchedulerFactory.getDefaultScheduler();
            startScheduler();
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }

    public static void createJobScheduler(String strategy,String time,String cron) throws SchedulerException {
        if (scheduler == null) {
            scheduler = StdSchedulerFactory.getDefaultScheduler();
            startScheduler();
        }
        JobDetail jobDetail = JobBuilder.newJob(UpdateLimitStrategy.class)
                .usingJobData("limitStrategy",strategy)
                .withIdentity(JobKey.jobKey(strategy + "_" + time + "_" + "updateJob",limitStrategyJobGroup))
                .build();
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(TriggerKey.triggerKey(strategy + "_" + time + "_" + "updateTrigger",limitStrategyTriggerGroup))
                .withSchedule(cronSchedule(cron).withMisfireHandlingInstructionDoNothing())
                .build();
        scheduler.scheduleJob(jobDetail,trigger);
    }

    public static void startScheduler() throws SchedulerException {
        scheduler.start();
    }

    /**
     * 移除系统中所有恢复qos相关的定时任务
     * @throws SchedulerException
     */
    public static void removeJobs() throws SchedulerException {
        GroupMatcher<JobKey> jobMatcher = GroupMatcher.groupEquals(limitStrategyJobGroup);
        Set<JobKey> jobKeySet = scheduler.getJobKeys(jobMatcher);
        if (jobKeySet.size() == 0){
            return;
        }
        List<JobKey> jobKeyList = new ArrayList<>(jobKeySet);
        GroupMatcher<TriggerKey> triggerMatcher = GroupMatcher.groupEquals(limitStrategyTriggerGroup);
        Set<TriggerKey> triggerKeySet = scheduler.getTriggerKeys(triggerMatcher);
        List<TriggerKey> triggerKeyList = new ArrayList<>(triggerKeySet);

        scheduler.pauseTriggers(triggerMatcher);//停止触发器
        scheduler.unscheduleJobs(triggerKeyList);//移除触发器

        scheduler.deleteJobs(jobKeyList);//删除任务
    }

    public static void resume() throws SchedulerException {
        GroupMatcher<TriggerKey> triggerMatcher = GroupMatcher.groupEquals(limitStrategyTriggerGroup);
        Set<String> pausedTriggerGroups = scheduler.getPausedTriggerGroups();
        if (!pausedTriggerGroups.contains(limitStrategyTriggerGroup)){
            return;
        }
        scheduler.resumeTriggers(triggerMatcher);
    }
}



