package com.redislabs.lettucemod.api.reactive;

import com.redislabs.lettucemod.api.StatefulRedisModulesConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;

public interface RedisModulesReactiveCommands<K, V> extends RedisReactiveCommands<K, V>, RedisGearsReactiveCommands<K, V>, RediSearchReactiveCommands<K, V>, RedisTimeSeriesReactiveCommands<K, V> {

    @Override
    StatefulRedisModulesConnection<K, V> getStatefulConnection();

}
