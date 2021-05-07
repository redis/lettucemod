package com.redislabs.mesclun;

import com.redislabs.mesclun.timeseries.CreateOptions;
import com.redislabs.mesclun.timeseries.Label;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
public class TestTimeSeries extends BaseRedisModulesTest {

    @Test
    void create() {
        // temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
        String status = sync.create("temperature:3:11", CreateOptions.builder().retentionTime(6000).build(), Label.of("sensor_id", "2"), Label.of("area_id", "32"));
        Assertions.assertEquals("OK", status);
    }

    @Test
    void add() {
        // TS.CREATE temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
        sync.create("temperature:3:11", CreateOptions.builder().retentionTime(6000).build(), Label.of("sensor_id", "2"), Label.of("area_id", "32"));
        // TS.ADD temperature:3:11 1548149181 30
        Long add1 = sync.add("temperature:3:11", 1548149181, 30);
        Assertions.assertEquals(1548149181, add1);
        // TS.ADD temperature:3:11 1548149191 42
        Long add2 = sync.add("temperature:3:11", 1548149191, 42);
        Assertions.assertEquals(1548149191, add2);
    }

}
