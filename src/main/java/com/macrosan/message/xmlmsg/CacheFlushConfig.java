package com.macrosan.message.xmlmsg;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

import java.util.Collections;
import java.util.Map;

import static com.macrosan.storage.move.CacheFlushConfigRefresher.*;
import static com.macrosan.storage.move.CacheFlushConfigRefresher.DEFAULT_FULL_WATER_MARK;

/**
 * @author zhaoyang
 * @date 2025/06/26
 **/
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@Accessors(chain = true)
@XmlRootElement(name = "CacheFlushConfig")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CacheFlushConfig", propOrder = {
        "enableDelayedFlush",
        "enableOrderedFlush",
        "delayedFlushWaterMark",
        "low",
        "high",
        "full"
})
public class CacheFlushConfig {
    @XmlElement(name = "EnableDelayedFlush", required = true)
    private  boolean enableDelayedFlush;
    @XmlElement(name = "EnableOrderedFlush", required = true)
    private  boolean enableOrderedFlush;
    @XmlElement(name = "DelayedFlushWaterMark", required = true)
    private  int delayedFlushWaterMark;
    @XmlElement(name = "Low", required = true)
    private  int low;
    @XmlElement(name = "High", required = true)
    private  int high;
    @XmlElement(name = "Full", required = true)
    private  int full;

    public static final CacheFlushConfig DEFAULT_CONFIG = new CacheFlushConfig(Collections.emptyMap());

    public CacheFlushConfig(Map<String, String> map) {
        this.enableDelayedFlush = Boolean.parseBoolean(map.getOrDefault(ENABLE_DELAYED_FLUSH_KEY, DEFAULT_ENABLE_DELAYED_FLUSH));
        this.enableOrderedFlush = Boolean.parseBoolean(map.getOrDefault(ENABLE_ORDERED_FLUSH_KEY, DEFAULT_ENABLE_ORDERED_FLUSH));
        this.delayedFlushWaterMark = Integer.parseInt(map.getOrDefault(DELAYED_FLUSH_WATER_MARK_KEY, DEFAULT_DELAYED_FLUSH_WATER_MARK));
        this.low = Integer.parseInt(map.getOrDefault(LOW_WATER_MARK_KEY, DEFAULT_LOW_WATER_MARK));
        this.high = Integer.parseInt(map.getOrDefault(HIGH_WATER_MARK_KEY, DEFAULT_HIGH_WATER_MARK));
        this.full = Integer.parseInt(map.getOrDefault(FULL_WATER_MARK_KEY, DEFAULT_FULL_WATER_MARK));
    }

    @Override
    public String toString() {
        return "enableDelayedFlush:" + enableDelayedFlush
                + " enableOrderedFlush:" + enableOrderedFlush
                + " delayedFlushWaterMark:" + delayedFlushWaterMark
                + " low:" + low
                + " high:" + high
                + " full:" + full;
    }

}
