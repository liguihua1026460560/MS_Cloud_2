package com.macrosan.filesystem.lifecycle;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.constants.SysConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.xmlmsg.lifecycle.*;
import com.macrosan.utils.serialize.JaxbUtils;
import io.vertx.core.json.Json;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.filesystem.lifecycle.FileLifecycleMove.scanTasks;
import static com.macrosan.utils.lifecycle.LifecycleUtils.*;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

@Log4j2
public class FileLifecycle implements Job {
    public static boolean fileLifecycleSwitch = false;
    public static long fileTimestamps;
    public static long startStamp;
    public static long endStamp;
    protected static RedisConnPool pool = RedisConnPool.getInstance();
    private static final String LIFECYCLE_JOB_GROUP = "FileLifecycleJobGroup";
    private static final String LIFECYCLE_MOVE_JOB = "FileLifecycleMoveJob";
    public static final String TEN_SECOND = "*/10 * * * * ? ";
    private static final String ONE_DAY = "0 0 0 1/1 * ? ";
    private static final long ONE_DAY_TIMESTAMPS = (60 * 60 * 24 * 1000);
    public static String cron;
    static Scheduler scheduler;

    // 定期执行迁移任务，桶s3配置
    public static void register() {
        if (!fileLifecycleSwitch) {
            return;
        }
        cron = pool.getCommand(REDIS_SYSINFO_INDEX).get("lifecycle_start_date");
        cron = StringUtils.isEmpty(cron) ? ONE_DAY : makeDailyTrigger(cron);

        try {
            scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.start();
            //创建一个JobDetail实例
            JobDetail jobDetail = JobBuilder
                    .newJob(FileLifecycle.class)
                    .withIdentity(LIFECYCLE_MOVE_JOB, LIFECYCLE_JOB_GROUP)
                    .build();

            //创建一个Trigger触发器
            Trigger trigger = newTrigger()
                    .withIdentity("FILE_LIFECYCLE_MOVE_TRIGGER", "FILE_LIFECYCLE_MOVE_GROUP")
                    .withSchedule(cronSchedule(cron).withMisfireHandlingInstructionDoNothing())
                    .forJob(LIFECYCLE_MOVE_JOB, LIFECYCLE_JOB_GROUP)
                    .startNow()
                    .build();

            scheduler.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        startMove();
    }

    public static void startMove() {
        // 每天预加载一次生命周期规则和生命周期启动时间
        String endTime = pool.getCommand(REDIS_SYSINFO_INDEX).get("lifecycle_end_date");
        endTime = StringUtils.isEmpty(endTime) ? "7:00" : endTime;
        String startTime = pool.getCommand(REDIS_SYSINFO_INDEX).get("lifecycle_start_date");
        startTime = StringUtils.isEmpty(startTime) ? "00:00" : startTime;

        startStamp = getLifecycleStartStamp(startTime);
        endStamp = getLifecycleEndStamp(startTime, endTime);

        // 加载生命周期规则
        loadLifecycle();


        ErasureServer.DISK_SCHEDULER.schedule(FileLifecycleMove.lifecycleMove, 10, TimeUnit.SECONDS);
    }

    public static void loadLifecycle() {
        scanTasks.clear();

        List<String> bucketKeys = pool.getCommand(REDIS_BUCKETINFO_INDEX).keys("*");
        for (String bucketName : bucketKeys) {
            List<RuleInfo> daysRule = new ArrayList<>();
            List<RuleInfo> dateRule = new ArrayList<>();

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

            // 处理迁移策略
            LifecycleConfiguration lifecycleConfiguration = getLifecycleConfiguration(bucketName);
//            log.info("lifecycleConfiguration : {}", lifecycleConfiguration);
            if (lifecycleConfiguration == null) {
                continue;
            }

            for (Rule rule : lifecycleConfiguration.getRules()) {
                //判断该条rule是否可用
                if (!Rule.ENABLED.equals(rule.getStatus())) {
                    continue;
                }
                String prefix;
                Map<String, String> tagMap = new HashMap<>();

                // 获取对象名前缀
                if (rule.getFilter() != null) {
                    prefix = rule.getFilter().getPrefix();
                } else {
                    prefix = rule.getPrefix();
                }
                // 获取tag集合
                if (StringUtils.isEmpty(prefix)) {//当Filter中的prefix不存在 互斥
                    if (null != rule.getFilter()) {
                        List<Tag> tags = rule.getFilter().getTags();//阿里云中的规则tag可以设置多个
                        if (tags != null) {
                            tagMap = getTagMap(tags);
                        }
                    }
                }

                boolean applicationScope = false;
                if (StringUtils.isEmpty(prefix) && tagMap.isEmpty()) {
                    applicationScope = true;
                }

                for (Condition condition : rule.getConditionList()) {
                    if (condition instanceof Transition && applicationScope) {
                        dealTransition((Transition) condition, dateRule, daysRule);
                    } else {
                        log.error("Unsupported lifecycle operation." + condition.getClass().getName());
                    }
                    // TODO 处理有prefix和tag的情况
                }
            }

            Set<String> allStrategy = getBucketAllStrategy(bucketInfo);
            // 根据生命周期规则，生成扫描任务
            if (!daysRule.isEmpty()) {
                log.info("{}", daysRule);
                List<ScanTask> daysTask = ScanTask.getDaysTask(bucketName, allStrategy, daysRule);
                scanTasks.addAll(daysTask);
            } else {
                log.info("{}", dateRule);
                List<ScanTask> dateTask = ScanTask.getDateTask(bucketName, allStrategy, dateRule);
                scanTasks.addAll(dateTask);
            }

        }
    }

    @Data
    @AllArgsConstructor
    public static class RuleInfo {
        String targetStrategy;
        long timestamps;
    }


    private static Set<String> getBucketAllStrategy(Map<String, String> bucketInfo) {
        Set<String> sourceStrategy = new HashSet<>();
        sourceStrategy.add(bucketInfo.get("storage_strategy"));
        HashSet<String> transitionStrategy = Json.decodeValue(bucketInfo.get("transition_strategy"), new TypeReference<HashSet<String>>() {
        });
        sourceStrategy.addAll(transitionStrategy);
        return sourceStrategy;
    }

    public static void dealTransition(Transition condition, List<RuleInfo> dateRule, List<RuleInfo> daysRule) {

        String date = condition.getDate();
        Integer days = condition.getDays();

        if (date != null && dateActivated(date)) {
            long timestamp = parseDateToTimestamps(date);
            dateRule.add(new RuleInfo(condition.getStorageClass(), timestamp));
        }

        if (days != null) {
            long timestamp = daysToDeadlineTimestamps(days);
            daysRule.add(new RuleInfo(condition.getStorageClass(), timestamp));
        }
    }

    public static LifecycleConfiguration getLifecycleConfiguration(String bucket) {
        String lifecycleXml = pool.getCommand(SysConstants.REDIS_SYSINFO_INDEX)
                .hget("bucket_lifecycle_rules", bucket);

        if (lifecycleXml == null) {
            return null;
        }
        return (LifecycleConfiguration) JaxbUtils.toObject(lifecycleXml);
    }

    public static Map<String, String> getTagMap(List<Tag> tags) {
        Map<String, String> tagMap = new HashMap<>();
        for (Tag tag : tags) {
            tagMap.put(tag.getKey(), tag.getValue());
        }
        return tagMap;
    }

    public static long daysToDeadlineTimestamps(Integer days) {
        return startStamp - ONE_DAY_TIMESTAMPS * days;
    }
}
