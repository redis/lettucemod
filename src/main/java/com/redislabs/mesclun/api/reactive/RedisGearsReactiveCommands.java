package com.redislabs.mesclun.api.reactive;

import com.redislabs.mesclun.gears.Execution;
import com.redislabs.mesclun.gears.ExecutionDetails;
import com.redislabs.mesclun.gears.ExecutionMode;
import com.redislabs.mesclun.gears.Registration;
import com.redislabs.mesclun.gears.output.ExecutionResults;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

@SuppressWarnings("unchecked")
public interface RedisGearsReactiveCommands<K, V> {

    Mono<String> abortExecution(String id);

    Flux<V> configGet(K... keys);

    Flux<V> configSet(Map<K, V> map);

    Mono<String> dropExecution(String id);

    Flux<Execution> dumpExecutions();

    Flux<Registration> dumpRegistrations();

    Mono<ExecutionDetails> getExecution(String id);

    Mono<ExecutionDetails> getExecution(String id, ExecutionMode mode);

    Mono<ExecutionResults> getResults(String id);

    Mono<ExecutionResults> getResultsBlocking(String id);

    Mono<ExecutionResults> pyExecute(String function, V... requirements);

    Mono<String> pyExecuteUnblocking(String function, V... requirements);

    Flux<Object> trigger(String trigger, V... args);

    Mono<String> unregister(String id);

}
