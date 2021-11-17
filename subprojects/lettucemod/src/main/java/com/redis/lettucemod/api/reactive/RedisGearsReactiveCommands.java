package com.redis.lettucemod.api.reactive;

import com.redis.lettucemod.gears.Execution;
import com.redis.lettucemod.gears.ExecutionDetails;
import com.redis.lettucemod.gears.ExecutionMode;
import com.redis.lettucemod.gears.Registration;
import com.redis.lettucemod.output.ExecutionResults;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

@SuppressWarnings("unchecked")
public interface RedisGearsReactiveCommands<K, V> {

    Mono<String> abortexecution(String id);

    Flux<V> configget(K... keys);

    Flux<V> configset(Map<K, V> map);

    Mono<String> dropexecution(String id);

    Flux<Execution> dumpexecutions();

    Flux<Registration> dumpregistrations();

    Mono<ExecutionDetails> getexecution(String id);

    Mono<ExecutionDetails> getexecution(String id, ExecutionMode mode);

    Mono<ExecutionResults> getresults(String id);

    Mono<ExecutionResults> getresultsBlocking(String id);

    Mono<ExecutionResults> pyexecute(String function, V... requirements);

    Mono<String> pyexecuteUnblocking(String function, V... requirements);

    Flux<Object> trigger(String trigger, V... args);

    Mono<String> unregister(String id);

}
