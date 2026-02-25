package com.macrosan.filesystem.lifecycle;

import lombok.Data;
import lombok.extern.log4j.Log4j2;

import java.util.*;

@Log4j2
@Data
public class ScanTask {
    public static String DAYS_TYPE = "days";
    public static String DATE_TYPE = "date";
    String type;
    String sourceStrategy;
    String targetStrategy;
    long beginTimestamps;
    long timestamps;

    String bucket;

    int scanEnd;

    public ScanTask(String bucket, String type, String sourceStrategy, String targetStrategy, long beginTimestamps, long timestamps) {
        this.bucket = bucket;
        this.type = type;
        this.sourceStrategy = sourceStrategy;
        this.targetStrategy = targetStrategy;
        this.beginTimestamps = beginTimestamps;
        this.timestamps = timestamps;
    }

    public static List<ScanTask> getDateTask(String bucket, Set<String> allStrategy, List<FileLifecycle.RuleInfo> dateRule) {
        List<ScanTask> list = new ArrayList<>();
        if (!dateRule.isEmpty()) {
            // 生效的规则中获取最大时间戳的rule
            Collections.sort(dateRule, (o1, o2) -> Long.compare(o2.timestamps, o1.timestamps));
            FileLifecycle.RuleInfo rule = dateRule.get(0);

            HashSet<String> set = new HashSet<>(allStrategy);
            set.remove(rule.targetStrategy);

            set.forEach(strategy -> {
                list.add(new ScanTask(bucket, DATE_TYPE, strategy, rule.targetStrategy, -1, rule.timestamps));
            });
        }

        return list;
    }

    public static List<ScanTask> getDaysTask(String bucket, Set<String> allStrategy, List<FileLifecycle.RuleInfo> daysRule) {
        List<ScanTask> list = new ArrayList<>();
        if (!daysRule.isEmpty()) {
            // 排序，策略，时间戳从大到小
            Collections.sort(daysRule, (o1, o2) -> {
                int compare = o1.targetStrategy.compareTo(o2.targetStrategy);
                return compare == 0 ? Long.compare(o2.timestamps, o1.timestamps) : compare;
            });

            // 过滤相同迁移策略的规则， 例如：30天，60天， 只需执行 30天的迁移
            String pre = "";
            Iterator<FileLifecycle.RuleInfo> iterator = daysRule.iterator();
            while (iterator.hasNext()) {
                String stratege = iterator.next().targetStrategy;
                if (stratege.equals(pre)) {
                    iterator.remove();
                }
                pre = stratege;
            }

            // 根据时间戳从小到大排序
            Collections.sort(daysRule, (o1, o2) -> Long.compare(o1.timestamps, o2.timestamps));

            // 不过滤相同时间戳，在客户端做了限制

            for (int i = 0; i < daysRule.size(); i++) {
                FileLifecycle.RuleInfo rule = daysRule.get(i);
                HashSet<String> set = new HashSet<>(allStrategy);
                set.remove(rule.targetStrategy);
                if (i == 0) {
                    int finalI = i;
                    set.forEach(strategy -> {
                        ScanTask daysTask = new ScanTask(bucket, DAYS_TYPE, strategy, daysRule.get(finalI).targetStrategy, -1L, rule.timestamps);
                        list.add(daysTask);
                    });

                } else {
                    FileLifecycle.RuleInfo lastRule = daysRule.get(i - 1);
                    int finalI1 = i;
                    set.forEach(strategy -> {
                        ScanTask daysTask = new ScanTask(bucket, DAYS_TYPE, strategy, daysRule.get(finalI1).targetStrategy, lastRule.timestamps, rule.timestamps);
                        list.add(daysTask);
                    });
                }
            }

        }
        return list;
    }
}
