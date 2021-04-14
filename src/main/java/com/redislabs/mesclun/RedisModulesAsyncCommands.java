package com.redislabs.mesclun;

import com.redislabs.mesclun.gears.RedisGearsAsyncCommands;
import com.redislabs.mesclun.search.RediSearchAsyncCommands;
import com.redislabs.mesclun.timeseries.RedisTimeSeriesAsyncCommands;
import io.lettuce.core.api.async.RedisAsyncCommands;

public interface RedisModulesAsyncCommands<K, V> extends RedisAsyncCommands<K, V>, RedisGearsAsyncCommands<K, V>, RediSearchAsyncCommands<K, V>, RedisTimeSeriesAsyncCommands<K, V> {

    @Override
    StatefulRedisModulesConnection<K, V> getStatefulConnection();

}
