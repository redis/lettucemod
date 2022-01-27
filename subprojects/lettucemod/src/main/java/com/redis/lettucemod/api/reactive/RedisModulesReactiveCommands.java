package com.redis.lettucemod.api.reactive;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Mono;

public interface RedisModulesReactiveCommands<K, V> extends RedisReactiveCommands<K, V>, RedisGearsReactiveCommands<K, V>, RedisJSONReactiveCommands<K, V>, RediSearchReactiveCommands<K, V>, RedisTimeSeriesReactiveCommands<K, V> {

    @Override
    StatefulRedisModulesConnection<K, V> getStatefulConnection();
    
    Mono<Long> pfaddNoValue(K key);

}
