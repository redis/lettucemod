package com.redis.lettucemod.api.async;

import com.redis.lettucemod.api.gears.Execution;
import com.redis.lettucemod.api.gears.ExecutionDetails;
import com.redis.lettucemod.api.gears.ExecutionMode;
import com.redis.lettucemod.api.gears.Registration;
import com.redis.lettucemod.output.ExecutionResults;
import io.lettuce.core.RedisFuture;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public interface RedisGearsAsyncCommands<K, V> {

    RedisFuture<String> abortexecution(String id);

    RedisFuture<List<V>> configget(K... keys);

    RedisFuture<List<V>> configset(Map<K, V> map);

    RedisFuture<String> dropexecution(String id);

    RedisFuture<List<Execution>> dumpexecutions();

    RedisFuture<List<Registration>> dumpregistrations();

    RedisFuture<ExecutionDetails> getexecution(String id);

    RedisFuture<ExecutionDetails> getexecution(String id, ExecutionMode mode);

    RedisFuture<ExecutionResults> getresults(String id);

    RedisFuture<ExecutionResults> getresultsBlocking(String id);

    RedisFuture<ExecutionResults> pyexecute(String function, V... requirements);

    RedisFuture<String> pyexecuteUnblocking(String function, V... requirements);

    RedisFuture<List<Object>> trigger(String trigger, V... args);

    RedisFuture<String> unregister(String id);

}
