package com.redislabs.mesclun;

import com.redislabs.mesclun.timeseries.CreateOptions;
import com.redislabs.mesclun.timeseries.Label;
import com.redislabs.testcontainers.RedisServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("unchecked")
public class TestTimeSeries extends BaseRedisModulesTest {

    @ParameterizedTest
    @MethodSource("redisServers")
    void create(RedisServer redis) {
        // temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
        String status = sync(redis).create("temperature:3:11", CreateOptions.builder().retentionTime(6000).build(), Label.of("sensor_id", "2"), Label.of("area_id", "32"));
        Assertions.assertEquals("OK", status);
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void add(RedisServer redis) {
        // TS.CREATE temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
        sync(redis).create("temperature:3:11", CreateOptions.builder().retentionTime(6000).build(), Label.of("sensor_id", "2"), Label.of("area_id", "32"));
        // TS.ADD temperature:3:11 1548149181 30
        Long add1 = sync(redis).add("temperature:3:11", 1548149181, 30);
        Assertions.assertEquals(1548149181, add1);
        // TS.ADD temperature:3:11 1548149191 42
        Long add2 = sync(redis).add("temperature:3:11", 1548149191, 42);
        Assertions.assertEquals(1548149191, add2);
    }

}
