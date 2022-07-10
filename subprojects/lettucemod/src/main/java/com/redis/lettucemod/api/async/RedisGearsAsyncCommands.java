package com.redis.lettucemod.api.async;

import com.redis.lettucemod.gears.Execution;
import com.redis.lettucemod.gears.ExecutionDetails;
import com.redis.lettucemod.gears.ExecutionMode;
import com.redis.lettucemod.gears.Registration;
import com.redis.lettucemod.output.ExecutionResults;
import io.lettuce.core.RedisFuture;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public interface RedisGearsAsyncCommands<K, V> {

    RedisFuture<String> rgAbortexecution(String id);

    RedisFuture<List<V>> rgConfigget(K... keys);

    RedisFuture<List<V>> rgConfigset(Map<K, V> map);

    RedisFuture<String> rgDropexecution(String id);

    RedisFuture<List<Execution>> rgDumpexecutions();

    RedisFuture<List<Registration>> rgDumpregistrations();

    RedisFuture<ExecutionDetails> rgGetexecution(String id);

    RedisFuture<ExecutionDetails> rgGetexecution(String id, ExecutionMode mode);

    RedisFuture<ExecutionResults> rgGetresults(String id);

    RedisFuture<ExecutionResults> rgGetresultsblocking(String id);

    RedisFuture<ExecutionResults> rgPyexecute(String function, V... requirements);

    RedisFuture<String> rgPyexecuteUnblocking(String function, V... requirements);

    RedisFuture<List<Object>> rgTrigger(String trigger, V... args);

    RedisFuture<String> rgUnregister(String id);

}
