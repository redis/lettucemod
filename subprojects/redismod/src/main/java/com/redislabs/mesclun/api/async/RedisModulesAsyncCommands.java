package com.redislabs.mesclun.api.async;

import com.redislabs.mesclun.api.StatefulRedisModulesConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

public interface RedisModulesAsyncCommands<K, V> extends RedisAsyncCommands<K, V>, RedisGearsAsyncCommands<K, V>, RediSearchAsyncCommands<K, V>, RedisTimeSeriesAsyncCommands<K, V> {

    @Override
    StatefulRedisModulesConnection<K, V> getStatefulConnection();

}
