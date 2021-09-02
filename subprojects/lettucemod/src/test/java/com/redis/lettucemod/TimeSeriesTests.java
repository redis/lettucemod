package com.redis.lettucemod;

import com.redis.lettucemod.api.sync.RedisTimeSeriesCommands;
import com.redis.lettucemod.api.timeseries.CreateOptions;
import com.redis.testcontainers.RedisServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TimeSeriesTests extends AbstractModuleTestBase {

    @ParameterizedTest
    @MethodSource("redisServers")
    void create(RedisServer redis) {
        // temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
        String status = sync(redis).create("temperature:3:11", CreateOptions.<String, String>builder().retentionTime(6000).build());
        Assertions.assertEquals("OK", status);
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void add(RedisServer redis) {
        RedisTimeSeriesCommands<String, String> ts = sync(redis);
        // TS.CREATE temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
        ts.create("temperature:3:11", CreateOptions.<String, String>builder().retentionTime(6000).label("sensor_id", "2").label("area_id", "32").build());
        // TS.ADD temperature:3:11 1548149181 30
        Long add1 = ts.add("temperature:3:11", 1548149181, 30);
        Assertions.assertEquals(1548149181, add1);
        // TS.ADD temperature:3:11 1548149191 42
        Long add2 = sync(redis).add("temperature:3:11", 1548149191, 42);
        Assertions.assertEquals(1548149191, add2);
    }

}
