package com.macrosan.database.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lombok.Getter;

/**
 * @author gaozhiyuan
 */
public class SampleConnection {
    @Getter
    private StatefulRedisConnection<String, String> connection;

    public SampleConnection(RedisURI uri) {
        RedisClient client = RedisClient.create(VertxEventLoopProvider.resources);
        connection = client.connect(uri);
    }

    public SampleConnection(RedisClient client, RedisURI uri) {
        connection = client.connect(uri);
    }

    public RedisReactiveCommands<String, String> reactive() {
        return connection.reactive();
    }
}
