package com.redislabs.mesclun;

import com.redislabs.mesclun.gears.RedisGearsReactiveCommands;
import com.redislabs.mesclun.timeseries.RedisTimeSeriesReactiveCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;

public interface RedisModulesReactiveCommands<K, V> extends RedisReactiveCommands<K, V>, RedisTimeSeriesReactiveCommands<K, V>, RedisGearsReactiveCommands<K, V> {

    StatefulRedisModulesConnection<K, V> getStatefulConnection();

}
