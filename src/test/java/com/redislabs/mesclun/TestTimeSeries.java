package com.redislabs.mesclun;

import com.redislabs.mesclun.timeseries.*;
import com.redislabs.testcontainers.BaseRedisModulesTest;
import com.redislabs.testcontainers.RedisModulesContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.lettuce.core.RedisURI;

public class TestTimeSeries extends BaseRedisModulesTest {

    @ParameterizedTest
    @MethodSource("containers")
    void create(RedisModulesContainer redisContainer) {
        RedisTimeSeriesCommands<String, String> ts = redisContainer.sync();
        // temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
        String status = ts.create("temperature:3:11", CreateOptions.builder().retentionTime(6000).build(), Label.of("sensor_id", "2"), Label.of("area_id", "32"));
        Assertions.assertEquals("OK", status);
    }

    @ParameterizedTest
    @MethodSource("containers")
    void add(RedisModulesContainer redisContainer) {
        RedisTimeSeriesCommands<String, String> ts = redisContainer.sync();
        // TS.CREATE temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
        ts.create("temperature:3:11", CreateOptions.builder().retentionTime(6000).build(), Label.of("sensor_id", "2"), Label.of("area_id", "32"));
        // TS.ADD temperature:3:11 1548149181 30
        Long add1 = ts.add("temperature:3:11", 1548149181, 30);
        Assertions.assertEquals(1548149181, add1);
        // TS.ADD temperature:3:11 1548149191 42
        Long add2 = ts.add("temperature:3:11", 1548149191, 42);
        Assertions.assertEquals(1548149191, add2);
    }

}
