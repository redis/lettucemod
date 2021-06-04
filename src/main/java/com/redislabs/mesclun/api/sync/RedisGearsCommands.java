package com.redislabs.mesclun.api.sync;

import com.redislabs.mesclun.gears.Execution;
import com.redislabs.mesclun.gears.ExecutionDetails;
import com.redislabs.mesclun.gears.ExecutionMode;
import com.redislabs.mesclun.gears.Registration;
import com.redislabs.mesclun.gears.output.ExecutionResults;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public interface RedisGearsCommands<K, V> {

    String abortExecution(String id);

    List<V> configGet(K... keys);

    List<V> configSet(Map<K, V> map);

    String dropExecution(String id);

    List<Execution> dumpExecutions();

    List<Registration> dumpRegistrations();

    ExecutionDetails getExecution(String id);

    ExecutionDetails getExecution(String id, ExecutionMode mode);

    ExecutionResults getResults(String id);

    ExecutionResults getResultsBlocking(String id);

    ExecutionResults pyExecute(String function, V... requirements);

    String pyExecuteUnblocking(String function, V... requirements);

    List<Object> trigger(String trigger, V... args);

    String unregister(String id);

}
