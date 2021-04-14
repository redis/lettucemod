package com.redislabs.mesclun;

import com.redislabs.mesclun.gears.RedisGearsCommands;
import com.redislabs.mesclun.timeseries.RedisTimeSeriesCommands;

public class Usage {

    public void redisTimeSeries() {
        RedisModulesClient client = RedisModulesClient.create("redis://localhost:6379"); // <1>
        StatefulRedisModulesConnection<String, String> connection = client.connect(); // <2>

        // RedisGears
        RedisGearsCommands<String, String> gears = connection.sync(); // <3>
        gears.pyExecute("GearsBuilder().run('person:*')"); // <4>

        // RedisTimeSeries
        RedisTimeSeriesCommands<String, String> ts = connection.sync(); // <5>
        ts.add("temp:3:11", 1548149181, 30); // <6>

    }
}
