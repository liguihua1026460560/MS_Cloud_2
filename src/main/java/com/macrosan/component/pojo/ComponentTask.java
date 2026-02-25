package com.macrosan.component.pojo;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.macrosan.database.redis.RedisConnPool;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.util.Calendar;
import java.util.Map;

import static com.macrosan.constants.SysConstants.REDIS_POOL_INDEX;

/**
 * 组件的定时任务。保存在表15，admin创建，上限为10。
 * 定时任务只会处理startTime之前的对象。
 */
@Data
@CompiledJson
@Accessors(chain = true)
@Log4j2
public class ComponentTask {
    @JsonIgnore
    public static final String REDIS_KEY = "component_task";

    @JsonAttribute
    public String taskName;

    /**
     * 每个Task对应一个strategy
     */
    @JsonAttribute
    public String strategyName;
    @JsonAttribute
    public ComponentRecord.Type strategyType;

    /**
     * 每个Task对应一个桶和对象前缀。相同的桶和前缀也只能有一个Task
     */
    @JsonAttribute
    public String bucket;
    @JsonAttribute
    public String prefix;
    /**
     * 每天执行的时间，格式为HH-mm。0表示立即执行。
     */
    @JsonAttribute
    public String startTime;

    /**
     * 任务的唯一标识
     */
    @JsonAttribute
    public String marker;

    @JsonIgnore
    private ComponentStrategy strategy = new ComponentStrategy();

    public ComponentTask() {

    }

    public ComponentTask(Map<String, String> redisMap) {
        this.taskName = redisMap.get("taskName");
        this.strategyName = redisMap.get("strategyName");
        this.strategyType = ComponentRecord.Type.parseType(redisMap.get("strategyType"));
        this.bucket = redisMap.get("bucket");
        this.prefix = redisMap.get("prefix");
        this.startTime = redisMap.get("startTime");
        this.marker =redisMap.get("marker");
    }

    /**
     * 将HH-mm转化成当天的时间
     */
    private Calendar calendarFromStr(String timeStr) {
        String[] split = timeStr.split("-");
        int hour = Integer.parseInt(split[0]);
        int minute = Integer.parseInt(split[1]);
        Calendar instance = Calendar.getInstance();
        instance.set(Calendar.HOUR_OF_DAY, hour);
        instance.set(Calendar.MINUTE, minute);
        //设置second为0，让task到达执行时间后就执行，不用等到下一分钟在执行
        instance.set(Calendar.SECOND,0);
        return instance;
    }

    public Calendar calendarFromStartTime() {
        return calendarFromStr(this.startTime);
    }

    public String taskRedisKey() {
        return REDIS_KEY + "_" + taskName;
    }

    /**
     * 扫描到该task时，查询对应的strategy内容。需要setStrategyRedisKey先执行
     */
    public Mono<ComponentStrategy> strategyMono() {
        return RedisConnPool.getInstance().getReactive(REDIS_POOL_INDEX)
                .hgetall(ComponentStrategy.strategyRedisKey(this.strategyName))
                .map(map -> {
                    this.strategy = new ComponentStrategy(map);
                    return this.strategy;
                });
    }

    public boolean allowStart() {
        if ("0".equals(startTime)) {
            return true;
        }
        boolean res = false;
        try {
            Calendar currentCalendar = Calendar.getInstance();
            Calendar startCalendar = calendarFromStr(startTime);
            res = currentCalendar.after(startCalendar);
        } catch (Exception e) {
            log.error("allowStart error, ", e);
        }
        return res;
    }

}
