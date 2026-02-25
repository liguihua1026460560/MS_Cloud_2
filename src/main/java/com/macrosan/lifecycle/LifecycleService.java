package com.macrosan.lifecycle;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.lifecycle.mq.LifecycleChannels;
import com.macrosan.utils.msutils.SshClientUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import reactor.core.scheduler.Schedulers;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.*;

import static com.macrosan.constants.ServerConstants.PROC_NUM;
import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;
import static com.macrosan.lifecycle.LifecycleCommandProducer.*;
import static com.macrosan.utils.lifecycle.LifecycleUtils.getLifecycleEndStamp;
import static com.macrosan.utils.lifecycle.LifecycleUtils.makeDailyTrigger;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.TriggerBuilder.newTrigger;


public class LifecycleService {
    private static final Logger logger = LogManager.getLogger(LifecycleService.class.getName());
    protected static RedisConnPool pool = RedisConnPool.getInstance();
    private static final String uuid = ServerConfig.getInstance().getHostUuid();
    private static final ExecutorService COMMAND_PRODUCER_EXECUTOR = new ThreadPoolExecutor(PROC_NUM * 2, Integer.MAX_VALUE,
            15L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(),
            new DefaultThreadFactory("lifecycle_produce_thread"));
    private static final ThreadPoolExecutor COMMAND_CONSUMER_EXECUTOR = new ThreadPoolExecutor(10, 200,
            15L, TimeUnit.SECONDS, new LinkedBlockingQueue(),  // 无界队列
            new DefaultThreadFactory("lifecycle_consume_thread"), new ThreadPoolExecutor.CallerRunsPolicy());
    private static long lifecycleEndStamp;
    private static long lifecycleStartStamp;
    private static String[] startTimeList = new String[]{"00:00"};
    private static String[] endTimeList = new String[]{"7:00"};


    private static final String LIFECYCLE_SERVICE_JOB_GROUP = "LifecycleServiceJobGroup";
    private static final String LIFECYCLE_SERVICE_TRIGGER_GROUP = "LifecycleServiceTriggerGroup";
    private static final String LIFECYCLE_COMMAND_PRODUCER_JOB = "LifecycleCommandProducerJob";
    private static final String LIFECYCLE_COMMAND_PRODUCER_TRIGGER = "LifecycleCommandProducerTrigger";

    private static final String TEN_SECOND = "*/10 * * * * ? ";
    private static final String TEN_MINITE = "0 0/10 * * * ? ";
    private static final String ONE_HOUR = "0 0 0/1 1/1 * ? ";
    private static final String ONE_DAY = "0 0 0 1/1 * ? ";

    private static String startTime;

    public static ThreadPoolExecutor getCommandConsumerExecutor() {
        return COMMAND_CONSUMER_EXECUTOR;
    }

    private static reactor.core.scheduler.Scheduler scheduler = Schedulers.fromExecutor(COMMAND_PRODUCER_EXECUTOR);

    public static reactor.core.scheduler.Scheduler getScheduler() {
        return scheduler;
    }

    public static void setStartTime(String time) {
        startTime = time;
    }

    public static void setEndStamp(long stamp) {
        lifecycleEndStamp = stamp;
    }

    public static void setStartStamp(long stamp) {
        lifecycleStartStamp = stamp;
    }

    public static long getEndStamp() {
        return lifecycleEndStamp;
    }

    public static long getStartStamp() {
        return lifecycleStartStamp;
    }

    public void start() {
        String lifecycleStartDate = pool.getCommand(REDIS_SYSINFO_INDEX).get("lifecycle_start_date");
        String lifecycleStartTime = StringUtils.isEmpty(lifecycleStartDate) ? "00:00" : lifecycleStartDate;
        lifecycleStartDate = StringUtils.isEmpty(lifecycleStartDate) ? ONE_DAY : makeDailyTrigger(lifecycleStartDate);
        setStartTime(lifecycleStartDate);
        String lifecycleEndDate = pool.getCommand(REDIS_SYSINFO_INDEX).get("lifecycle_end_date");
        lifecycleEndDate = StringUtils.isEmpty(lifecycleEndDate) ? "7:00" : lifecycleEndDate;
        setEndStamp(getLifecycleEndStamp(lifecycleStartTime, lifecycleEndDate));
        JaxbUtils.initJaxb();
        LifecycleChannels.init();
        LifecycleChannels.initChannel();
        LifecycleChannels.startCommandConsumer();
        scheduleCommandProducer();
        getLifecycleStartTime();
        logger.info("Lifecycle timing task start! the end date: {} ,the end date: {}", lifecycleStartDate, lifecycleEndDate);
    }

    private static void scheduleCommandProducer() {
        try {
            //创建一个JobDetail实例，并将其与生产者绑定
            JobDetail jobDetail = JobBuilder
                    .newJob(LifecycleCommandProducer.class)
                    .withIdentity(LIFECYCLE_COMMAND_PRODUCER_JOB, LIFECYCLE_SERVICE_JOB_GROUP)
                    .build();

            //创建一个Trigger触发器
            Trigger trigger = newTrigger()
                    .withIdentity(LIFECYCLE_COMMAND_PRODUCER_TRIGGER, LIFECYCLE_SERVICE_TRIGGER_GROUP)
//                    .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(36000).repeatForever())
                    .withSchedule(cronSchedule(startTime).withMisfireHandlingInstructionDoNothing())
                    .forJob(LIFECYCLE_COMMAND_PRODUCER_JOB, LIFECYCLE_SERVICE_JOB_GROUP)
                    .startNow()
                    .build();
            //创建schedule实例
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.start();
            scheduler.scheduleJob(jobDetail, trigger);

        } catch (SchedulerException e) {
            logger.error("scheduleCommandProducer.", e);
        }
    }

    /**
     * 周期性获取生命周期执行时间，发生变化更新触发器
     */
    private static void getLifecycleStartTime() {
        try {
            String startTime = pool.getCommand(REDIS_SYSINFO_INDEX).get("lifecycle_start_date");
            Set<String> nodes = pool.getCommand(REDIS_SYSINFO_INDEX).smembers("lifecycle_restart_schedule");

            if (!StringUtils.isEmpty(startTime) && !startTime.equals(startTimeList[0])) {
                startTimeList[0] = startTime;
                modifyJobTime(makeDailyTrigger(startTime), false);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem("lifecycle_restart_schedule", uuid);
            } else {
                if (nodes.size() > 0 && nodes.contains(uuid)) {
                    modifyJobTime(makeDailyTrigger(startTimeList[0]), true);
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem("lifecycle_restart_schedule", uuid);
                }
            }
            MQ_DISK_USED_RATE = SshClientUtils.exec("df -h | awk '/\\/cg_sp0$/ {gsub(/%/, \"\", $5); print $5}'");
        } catch (Exception e) {
            logger.error("", e);
        } finally {
            ErasureServer.DISK_SCHEDULER.schedule(LifecycleService::getLifecycleStartTime, 10, TimeUnit.SECONDS);
        }
    }

    private static void modifyJobTime(String time, boolean b) {
        TriggerKey triggerKey = TriggerKey.triggerKey(
                LIFECYCLE_COMMAND_PRODUCER_TRIGGER, LIFECYCLE_SERVICE_TRIGGER_GROUP);
        try {
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            CronTrigger oldTrigger = (CronTrigger) scheduler.getTrigger(triggerKey);
            if (oldTrigger == null) {
                logger.error("The old trigger is empty.");
                return;
            }
            String oldTime = oldTrigger.getCronExpression();

            if (b || !oldTime.equalsIgnoreCase(time)) {
                // 按新的cronExpression表达式重新构建trigger
                Trigger newTrigger = newTrigger()
                        .withIdentity(LIFECYCLE_COMMAND_PRODUCER_TRIGGER, LIFECYCLE_SERVICE_TRIGGER_GROUP)
                        .withSchedule(cronSchedule(time).withMisfireHandlingInstructionDoNothing())
                        .forJob(LIFECYCLE_COMMAND_PRODUCER_JOB, LIFECYCLE_SERVICE_JOB_GROUP)
                        .startNow()
                        .build();

                // 按新的trigger重新设置job执行
                Date date = scheduler.rescheduleJob(triggerKey, newTrigger);
                if (date != null) {
                    logger.info("Reset lifecycleSchedule success, new Time: {}", startTimeList[0]);
                    return;
                }
                logger.error("The lifecycle scheduler rescheduleJob failed.");
            }
        } catch (SchedulerException e) {
            logger.error("scheduleCommandProducer.", e);
        }
    }

}