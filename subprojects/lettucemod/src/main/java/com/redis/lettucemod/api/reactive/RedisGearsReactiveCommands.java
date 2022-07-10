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

    Mono<String> rgAbortexecution(String id);

    Flux<V> rgConfigget(K... keys);

    Flux<V> rgConfigset(Map<K, V> map);

    Mono<String> rgDropexecution(String id);

    Flux<Execution> rgDumpexecutions();

    Flux<Registration> rgDumpregistrations();

    Mono<ExecutionDetails> rgGetexecution(String id);

    Mono<ExecutionDetails> rgGetexecution(String id, ExecutionMode mode);

    Mono<ExecutionResults> rgGetresults(String id);

    Mono<ExecutionResults> rgGetresultsblocking(String id);

    Mono<ExecutionResults> rgPyexecute(String function, V... requirements);

    Mono<String> rgPyexecuteUnblocking(String function, V... requirements);

    Flux<Object> rgTrigger(String trigger, V... args);

    Mono<String> rgUnregister(String id);

}
