package com.macrosan.utils;

import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhaoyang
 * @date 2026/02/05
 * @description 统一管理模块的debug标志
 */
@Log4j2
public class ModuleDebug {

    private static final Map<String, AtomicBoolean> DEBUG_FLAGS = new ConcurrentHashMap<>();

    /*
      注册模块的debug标志
     */
    static {
        DEBUG_FLAGS.put("media_component", new AtomicBoolean(false));
    }

    public static boolean isEnabled(String module) {
        AtomicBoolean flag = DEBUG_FLAGS.get(module);
        return flag != null && flag.get();
    }

    public static void setEnabled(String module, boolean enabled) {
        AtomicBoolean flag = DEBUG_FLAGS.get(module);
        if (flag != null) {
            flag.set(enabled);
        } else {
            log.warn("Unknown module: {}", module);
        }
    }

    public static Map<String, Boolean> getAll() {
        Map<String, Boolean> result = new ConcurrentHashMap<>();
        DEBUG_FLAGS.forEach((k, v) -> result.put(k, v.get()));
        return result;
    }

    public static boolean mediaComponentDebug() {
        return isEnabled("media_component");
    }
}
