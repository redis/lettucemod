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

    String rgAbortexecution(String id);

    List<V> rgConfigget(K... keys);

    List<V> rgConfigset(Map<K, V> map);

    String rgDropexecution(String id);

    List<Execution> rgDumpexecutions();

    List<Registration> rgDumpregistrations();

    ExecutionDetails rgGetexecution(String id);

    ExecutionDetails rgGetexecution(String id, ExecutionMode mode);

    ExecutionResults rgGetresults(String id);

    ExecutionResults rgGetresultsblocking(String id);

    ExecutionResults rgPyexecute(String function, V... requirements);

    String pyExecuteUnblocking(String function, V... requirements);

    List<Object> rgTrigger(String trigger, V... args);

    String rgUnregister(String id);

}
