package com.redis.lettucemod.api.sync;

import com.redis.lettucemod.gears.Execution;
import com.redis.lettucemod.gears.ExecutionDetails;
import com.redis.lettucemod.gears.ExecutionMode;
import com.redis.lettucemod.gears.Registration;
import com.redis.lettucemod.output.ExecutionResults;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public interface RedisGearsCommands<K, V> {

    String abortexecution(String id);

    List<V> configget(K... keys);

    List<V> configset(Map<K, V> map);

    String dropexecution(String id);

    List<Execution> dumpexecutions();

    List<Registration> dumpregistrations();

    ExecutionDetails getexecution(String id);

    ExecutionDetails getexecution(String id, ExecutionMode mode);

    ExecutionResults getresults(String id);

    ExecutionResults getresultsBlocking(String id);

    ExecutionResults pyexecute(String function, V... requirements);

    String pyexecuteUnblocking(String function, V... requirements);

    List<Object> trigger(String trigger, V... args);

    String unregister(String id);

}
