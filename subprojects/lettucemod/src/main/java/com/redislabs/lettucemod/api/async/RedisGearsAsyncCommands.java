package com.redislabs.lettucemod.api.async;

import com.redislabs.lettucemod.gears.Execution;
import com.redislabs.lettucemod.gears.ExecutionDetails;
import com.redislabs.lettucemod.gears.ExecutionMode;
import com.redislabs.lettucemod.gears.Registration;
import com.redislabs.lettucemod.gears.output.ExecutionResults;
import io.lettuce.core.RedisFuture;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public interface RedisGearsAsyncCommands<K, V> {

    RedisFuture<String> abortExecution(String id);

    RedisFuture<List<V>> configGet(K... keys);

    RedisFuture<List<V>> configSet(Map<K, V> map);

    RedisFuture<String> dropExecution(String id);

    RedisFuture<List<Execution>> dumpExecutions();

    RedisFuture<List<Registration>> dumpRegistrations();

    RedisFuture<ExecutionDetails> getExecution(String id);

    RedisFuture<ExecutionDetails> getExecution(String id, ExecutionMode mode);

    RedisFuture<ExecutionResults> getResults(String id);

    RedisFuture<ExecutionResults> getResultsBlocking(String id);

    RedisFuture<ExecutionResults> pyExecute(String function, V... requirements);

    RedisFuture<String> pyExecuteUnblocking(String function, V... requirements);

    RedisFuture<List<Object>> trigger(String trigger, V... args);

    RedisFuture<String> unregister(String id);

}
