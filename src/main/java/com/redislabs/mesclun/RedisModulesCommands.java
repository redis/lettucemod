package com.redislabs.mesclun;

import com.redislabs.mesclun.gears.RedisGearsCommands;
import com.redislabs.mesclun.timeseries.RedisTimeSeriesCommands;
import io.lettuce.core.api.sync.RedisCommands;

public interface RedisModulesCommands<K, V> extends RedisCommands<K, V>, RedisTimeSeriesCommands<K, V>, RedisGearsCommands<K, V> {

    StatefulRedisModulesConnection<K, V> getStatefulConnection();

}
