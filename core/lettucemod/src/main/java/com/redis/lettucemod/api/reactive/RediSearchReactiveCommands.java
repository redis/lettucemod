package com.redis.lettucemod.api.reactive;

import reactor.core.publisher.Flux;

public interface RediSearchReactiveCommands<K, V> extends io.lettuce.core.api.reactive.RediSearchReactiveCommands<K, V> {

    Flux<Object> ftInfo(K index);

}
