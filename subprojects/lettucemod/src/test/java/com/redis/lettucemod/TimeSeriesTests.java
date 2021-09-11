package com.redis.lettucemod;

import com.redis.lettucemod.api.sync.RedisTimeSeriesCommands;
import com.redis.lettucemod.api.timeseries.Aggregation;
import com.redis.lettucemod.api.timeseries.CreateOptions;
import com.redis.lettucemod.api.timeseries.RangeOptions;
import com.redis.lettucemod.api.timeseries.Sample;
import com.redis.testcontainers.RedisServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

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
        Long add2 = ts.add("temperature:3:11", 1548149191, 42);
        Assertions.assertEquals(1548149191, add2);
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void range(RedisServer redis) {
        RedisTimeSeriesCommands<String, String> ts = sync(redis);
        String key = "temperature:3:11";
        // TS.CREATE temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
        ts.create(key, CreateOptions.<String, String>builder().retentionTime(6000).label("sensor_id", "2").label("area_id", "32").build());
        // TS.ADD temperature:3:11 1548149181 30
        ts.add(key, 1548149181, 30);
        // TS.ADD temperature:3:11 1548149191 42
        ts.add(key, 1548149191, 42);
        // TS.RANGE temperature:3:11 1548149180 1548149210 AGGREGATION avg 5
        List<Sample> range = ts.range(key, RangeOptions.builder().from(1548149180).to(1548149210).aggregation(Aggregation.builder().type(Aggregation.Type.AVG).timeBucket(5).build()).build());
        Assertions.assertEquals(2, range.size());
        Assertions.assertEquals(1548149180, range.get(0).getTimestamp());
        Assertions.assertEquals(30, range.get(0).getValue());
        Assertions.assertEquals(1548149190, range.get(1).getTimestamp());
        Assertions.assertEquals(42, range.get(1).getValue());
    }

}
