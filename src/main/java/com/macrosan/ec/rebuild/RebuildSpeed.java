package com.macrosan.ec.rebuild;

import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Log4j2
public class RebuildSpeed {
    static class Record {
        long size;
        long num;
    }

    private static long firstRecordSize = 0L;
    private static long firstRecordTime = 0L;
    private static long recordTime = 0L;
    private static long lastTotalSize = 0L;
    private static long lastTotalNum = 0L;
    static Map<String, Record> recordMap = new HashMap<>();

    static {
        Flux.interval(Duration.ofSeconds(10)).subscribe(l -> {
            long curTotalSize = 0L;
            long curTotalNum = 0L;
            long curRecordTime = System.currentTimeMillis();

            for (Record record : recordMap.values()) {
                curTotalSize += record.size;
                curTotalNum += record.num;
            }

            long addSize = curTotalSize - lastTotalSize;
            if (addSize > 0L) {
                RebuildRabbitMq.getMaster()
                        .hincrby("rebuild_speed", "rebuild_size", addSize);
            }

            long addNum = curTotalNum - lastTotalNum;
            if (addNum > 0L) {
                RebuildRabbitMq.getMaster()
                        .hincrby("rebuild_speed", "rebuild_num", addNum);
            }

            if (addSize > 0L || addNum > 0L) {
                RebuildRabbitMq.getMaster()
                        .hset("rebuild_speed", "record_time", curRecordTime + "");
            }

            recordTime = curRecordTime;
            lastTotalSize = curTotalSize;
            lastTotalNum = curTotalNum;
        });
    }

    public static void add(long size) {
        String key = Thread.currentThread().getName();
        Record record = recordMap.get(key);
        if (record == null) {
            synchronized (recordMap) {
                record = recordMap.get(key);
                if (null == record) {
                    record = new Record();
                    recordMap.put(key, record);
                }
            }
        }

        record.size += size;
    }

    public static void addNum(long n) {
        String key = Thread.currentThread().getName();
        Record record = recordMap.get(key);
        if (record == null) {
            synchronized (recordMap) {
                record = recordMap.get(key);
                if (null == record) {
                    record = new Record();
                    recordMap.put(key, record);
                }
            }
        }

        record.num += n;
    }
}
