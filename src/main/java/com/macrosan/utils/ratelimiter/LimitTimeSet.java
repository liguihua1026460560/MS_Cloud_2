package com.macrosan.utils.ratelimiter;

import com.macrosan.database.redis.RedisConnPool;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.log4j.Log4j2;
import org.quartz.SchedulerException;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.utils.ratelimiter.LimitStrategy.NO_LIMIT;

/**
 *@program: MS_Cloud
 *@description:
 *@author: niechengxing
 *@create: 2022-12-13 11:40
 */
@Log4j2
public class LimitTimeSet {
    protected static RedisConnPool pool = RedisConnPool.getInstance();
    private static final String TIME_ZERO = "00";
    private static final String TIME_24 = "24";
    private static final String FORMAT_PATTERN = "HH";
    private static final String CRON_PATTERN = "00 00 HH * * ?";

    public static final String START_TIME = "startTime";
    public static final String END_TIME = "endTime";
    public static final String LIMIT_STRATEGY = "limitStrategy";

    public static final LimitStrategy DEFAULT_STRATEGY = NO_LIMIT;

    public static void init(){
        //重启或更改规则之后重设分时段恢复限流的任务
        ScanArgs scanArgs = new ScanArgs().match("recover_*");//使用默认count
        RedisCommands<String,String> sync = pool.getCommand(REDIS_SYSINFO_INDEX);
        KeyScanCursor<String> scanCursor = null;

        List<String> strategyList = new ArrayList<>();
        do {
            if (scanCursor == null){
                scanCursor = sync.scan(scanArgs);
            } else {
                scanCursor = sync.scan(scanCursor,scanArgs);
            }
            strategyList.addAll(scanCursor.getKeys());
        } while (!scanCursor.isFinished());

        String[] strategies = strategyList.toArray(new String[strategyList.size()]);
        if (strategies.length == 0){//未设置时间段生效规则,则默认业务优先
            RecoverLimiter.updateStrategy(DEFAULT_STRATEGY);
        } else {
            //获取当前系统时间
            Calendar now = Calendar.getInstance();
            int hour = now.get(Calendar.HOUR_OF_DAY);

            boolean changed = false;
            for (String strategy : strategies){
                Map<String, String> strategyInfo = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(strategy);
                int startTime = Integer.parseInt(strategyInfo.get(START_TIME));
                int endTime = Integer.parseInt(strategyInfo.get(END_TIME));
                if (startTime > endTime){//判断跨零点时间段
                    if (hour >= startTime || hour < endTime){//在时间段内
                        RecoverLimiter.updateStrategy(LimitStrategy.valueOf(strategyInfo.get(LIMIT_STRATEGY)));
                        changed = true;
                        break;
                    }
                } else {
                    if (hour >= startTime && hour < endTime){
                        RecoverLimiter.updateStrategy(LimitStrategy.valueOf(strategyInfo.get(LIMIT_STRATEGY)));
                        changed = true;
                        break;
                    }
                }
            }
            if (!changed){//不在用户设置的时间段中
                RecoverLimiter.updateStrategy(DEFAULT_STRATEGY);
            }
        }

        // 重启节点若当前存在索引盘重构，且为业务优先策略，开启元数据限流器对数据库的扫描
        try {
            List<String> runningKeys = pool.getCommand(REDIS_MIGING_V_INDEX).keys("running_*");
            for (String runningKey : runningKeys) {
                if ("hash".equals(pool.getCommand(REDIS_MIGING_V_INDEX).type(runningKey))) {
                    if (pool.getCommand(REDIS_MIGING_V_INDEX).hexists(runningKey, "operate") && pool.getCommand(REDIS_MIGING_V_INDEX).hexists(runningKey, "diskName")){
                        String operate = pool.getCommand(REDIS_MIGING_V_INDEX).hget(runningKey, "operate");
                        String disk = pool.getCommand(REDIS_MIGING_V_INDEX).hget(runningKey, "diskName");
                        if (operate.equalsIgnoreCase("remove_disk") && disk.endsWith("index")) {  // 索引池重构
                            RecoverLimiter.getInstance().getMetaLimiter().recoverFlag = true;
                            log.info("Exists meta pool rebalanced");
                            break;
                        }
                    }

                }
            }

        } catch (Exception e) {
            log.error("Read redis error", e);
        }

        try {
            JobScheduler.removeJobs();
            operate(strategies);
        } catch (SchedulerException e) {
            log.error("", e);
        }

    }

    /**
     * 根据设置的规则初始化本地节点的定时任务
     * @param strategies
     */
    public static void operate(String[] strategies) throws SchedulerException {

        if (strategies.length == 0){
            return;
        }
        Map<String,Map<String,String>> timeStrategyInfo = new HashMap<>();

        for (String strategy : strategies){
            Map<String,String> map = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(strategy);
            timeStrategyInfo.put(strategy,map);
        }

        List<Map.Entry<String,Map<String,String>>> strategyInfoList = new ArrayList<>(timeStrategyInfo.entrySet());
        strategyInfoList.sort(Comparator.comparing(o -> o.getValue().get(START_TIME)));



        for (int i = 0;i < strategyInfoList.size();i++) {
            //获取数据库中用户的时间段配置
            Map<String,String> map = strategyInfoList.get(i).getValue();
            //根据时间段中的值来设置定时任务
            String startTime = map.get(START_TIME);
            String endTime = map.get(END_TIME);
            String startCron = everyDay(startTime);
            String endCron = everyDay(endTime);
            if (i > 0){
                Map<String,String> prevMap = strategyInfoList.get(i-1).getValue();//获取前一个策略的信息
                String prevEndTime = prevMap.get(END_TIME);
                if (!startTime.equals(prevEndTime)){
                    JobScheduler.createJobScheduler(map.get(LIMIT_STRATEGY),startTime,startCron);
                }
            } else {//第一个时间段
                JobScheduler.createJobScheduler(map.get(LIMIT_STRATEGY),startTime,startCron);
            }
            if ((i + 1) < strategyInfoList.size()){
                Map<String,String> laterMap = strategyInfoList.get(i+1).getValue();
                String laterStartTime = laterMap.get(START_TIME);
                if (endTime.equals(laterStartTime)){
                    JobScheduler.createJobScheduler(laterMap.get(LIMIT_STRATEGY),endTime,endCron);
                } else {
                    JobScheduler.createJobScheduler(DEFAULT_STRATEGY.name(),endTime,endCron);
                }
            } else {//最后一个时间段
                if (endTime.equals(TIME_24)){
                    endTime = TIME_ZERO;
                }
                Map<String,String> first = strategyInfoList.get(0).getValue();
                if (!endTime.equals(first.get(START_TIME))) {
                    JobScheduler.createJobScheduler(DEFAULT_STRATEGY.name(),endTime,endCron);
                }
            }
        }
        JobScheduler.resume();
        log.info("set recover limit schedule job successful");

    }

    public static String everyDay(String timeStr){
        LocalTime time = LocalTime.parse(timeStr, DateTimeFormatter.ofPattern(FORMAT_PATTERN));
        String cronStr = time.format(DateTimeFormatter.ofPattern(CRON_PATTERN));
        return cronStr;
    }


}



