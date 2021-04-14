package com.redislabs.mesclun;

import com.redislabs.mesclun.gears.RedisGearsReactiveCommands;
import com.redislabs.mesclun.search.RediSearchReactiveCommands;
import com.redislabs.mesclun.timeseries.RedisTimeSeriesReactiveCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;

public interface RedisModulesReactiveCommands<K, V> extends RedisReactiveCommands<K, V>, RedisGearsReactiveCommands<K, V>, RediSearchReactiveCommands<K, V>, RedisTimeSeriesReactiveCommands<K, V> {

    @Override
    StatefulRedisModulesConnection<K, V> getStatefulConnection();

}
