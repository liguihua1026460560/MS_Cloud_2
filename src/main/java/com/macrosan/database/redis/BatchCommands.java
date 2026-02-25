package com.macrosan.database.redis;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.dynamic.Commands;
import io.lettuce.core.dynamic.batch.BatchExecutor;
import io.lettuce.core.dynamic.batch.BatchSize;

import java.util.Map;

@BatchSize(1000)
public interface BatchCommands extends Commands, BatchExecutor {
    RedisFuture<Map<String, String>> hgetall(String key);
}
