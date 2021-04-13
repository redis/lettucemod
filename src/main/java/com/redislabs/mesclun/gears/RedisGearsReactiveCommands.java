package com.redislabs.mesclun.gears;

import reactor.core.publisher.Mono;

import java.util.List;

public interface RedisGearsReactiveCommands<K, V> {

    Mono<String> pyExecute(String function, PyExecuteOptions options);

    Mono<List<Registration>> dumpRegistrations();

}
