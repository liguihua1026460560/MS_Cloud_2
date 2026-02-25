package com.macrosan.clearmodel;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.ServerConfig;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.macrosan.clearmodel.ClearModelExecutor.*;
import static com.macrosan.constants.SysConstants.*;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

@Log4j2
public class ClearModelStarter {

    private static final Logger logger = LogManager.getLogger(ClearModelStarter.class.getName());
    private static RedisConnPool pool = RedisConnPool.getInstance();

    private static final String CLEAR_MODEL_PRODUCER_JOB = "ClearModelProducerJob";
    private static final String CLEAR_MODEL_JOB_GROUP = "ClearModelJobGroup";
    private static final String CLEAR_MODEL_TRIGGER_GROUP = "ClearModelTriggerGroup";
    private static final String CLEAR_MODEL_PRODUCER_TRIGGER = "ClearModelProducerTrigger";

    private static final String startTime = "0 0/1 * * * ?";

    public static void start() {
        //重启删除标记
        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(LOCK_KEY);
        //遍历桶，删除标记
        RedisCommands<String, String> command = pool.getCommand(REDIS_BUCKETINFO_INDEX);
        ScanArgs scanArgs = new ScanArgs();
        scanArgs.limit(10);
        ScanIterator<String> scan = ScanIterator.scan(command, scanArgs);
        while (scan.hasNext()) {
            String bucketName = scan.next();
            String bucketKeyType = pool.getCommand(REDIS_BUCKETINFO_INDEX).type(bucketName);
            if (!"hash".equals(bucketKeyType)) {
                continue;
            }
            String localRegion = ServerConfig.getInstance().getRegion();
            Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
            String bucketRegion = bucketInfo.get(REGION_NAME);
            if (StringUtils.isNotEmpty(bucketRegion) && !localRegion.equals(bucketRegion)) {
                continue;
            }
            String deteleMark = bucketInfo.get(DELETE_MARK);
            if (StringUtils.isNotEmpty(deteleMark) && deteleMark.equals(NODE_NAME)){
                pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucketName, DELETE_MARK);
            }
        }
        scheduleClearModelProducer();
    }

    private static void scheduleClearModelProducer() {
        try {
            //创建一个JobDetail实例，并将其与生产者绑定
            JobDetail jobDetail = JobBuilder
                    .newJob(ClearModelExecutor.class)
                    .withIdentity(CLEAR_MODEL_PRODUCER_JOB, CLEAR_MODEL_JOB_GROUP)
                    .build();

            //创建一个Trigger触发器
            Trigger trigger = newTrigger()
                    .withIdentity(CLEAR_MODEL_PRODUCER_TRIGGER, CLEAR_MODEL_TRIGGER_GROUP)
                    .withSchedule(cronSchedule(startTime).withMisfireHandlingInstructionDoNothing())
                    .forJob(CLEAR_MODEL_PRODUCER_JOB, CLEAR_MODEL_JOB_GROUP)
                    .startNow()
                    .build();
            //创建schedule实例
            org.quartz.Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.start();
            scheduler.scheduleJob(jobDetail, trigger);

        } catch (SchedulerException e) {
            logger.error("scheduleCommandProducer.", e);
        }
    }
}
