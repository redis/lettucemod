package com.redislabs.lettucemod;

import com.redislabs.lettucemod.api.sync.RedisGearsCommands;
import com.redislabs.lettucemod.api.sync.RedisModulesCommands;
import com.redislabs.lettucemod.gears.Execution;
import com.redislabs.lettucemod.gears.ExecutionDetails;
import com.redislabs.lettucemod.gears.RedisGearsUtils;
import com.redislabs.lettucemod.gears.Registration;
import com.redislabs.lettucemod.gears.output.ExecutionResults;
import com.redislabs.testcontainers.RedisServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

public class TestGears extends BaseRedisModulesTest {

    @ParameterizedTest
    @MethodSource("redisServers")
    void pyExecute(RedisServer redis) {
        RedisModulesCommands<String, String> sync = sync(redis);
        sync.set("foo", "bar");
        ExecutionResults results = pyExecute(sync, "sleep.py");
        Assertions.assertEquals("1", results.getResults().get(0));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void pyExecuteUnblocking(RedisServer redis) {
        RedisModulesCommands<String, String> sync = sync(redis);
        sync.set("foo", "bar");
        String executionId = pyExecuteUnblocking(sync, "sleep.py");
        String[] array = executionId.split("-");
        Assertions.assertEquals(2, array.length);
        Assertions.assertEquals(40, array[0].length());
        Assertions.assertTrue(Integer.parseInt(array[1]) >= 0);
    }

//    @ParameterizedTest
//    @MethodSource("redisServers")
//    void pyExecuteNoResults(RedisServer redis) {
//        RedisModulesCommands<String, String> sync = sync(redis);
//        ExecutionResults results = pyExecute(sync, "sleep.py");
//        Assertions.assertTrue(results.getResults().isEmpty());
//        Assertions.assertTrue(results.getErrors().isEmpty());
//    }

    private ExecutionResults pyExecute(RedisGearsCommands<String, String> sync, String resourceName) {
        return sync.pyExecute(load(resourceName));
    }

    @SuppressWarnings("SameParameterValue")
    private String pyExecuteUnblocking(RedisGearsCommands<String, String> sync, String resourceName) {
        return sync.pyExecuteUnblocking(load(resourceName));
    }

    private String load(String resourceName) {
        return RedisGearsUtils.toString(getClass().getClassLoader().getResourceAsStream(resourceName));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void dumpRegistrations(RedisServer redis) {
        RedisModulesCommands<String, String> sync = sync(redis);
        // Single registration
        List<Registration> registrations = sync.dumpRegistrations();
        Assertions.assertEquals(0, registrations.size());
        ExecutionResults results = pyExecute(sync, "streamreader.py");
        Assertions.assertFalse(results.isError());
        registrations = sync.dumpRegistrations();
        Assertions.assertEquals(1, registrations.size());
        Registration registration = registrations.get(0);
        Assertions.assertEquals("StreamReader", registration.getReader());
        Assertions.assertEquals("MyStreamReader", registration.getDescription());
        Assertions.assertEquals("async", registration.getData().getMode());
        Map<String, Object> args = registration.getData().getArgs();
        Assertions.assertEquals(3, args.size());
        Assertions.assertEquals(1L, args.get("batchSize"));
        Assertions.assertEquals("mystream", args.get("stream"));
        Assertions.assertEquals("OK", registration.getData().getStatus());
        Assertions.assertTrue(registration.getPrivateData().contains("'sessionId'"));

        // Multiple registrations
        sync.dumpRegistrations().forEach(r -> sync.unregister(r.getId()));
        String function = "GB('KeysReader').register('*', keyTypes=['hash'])";
        Assertions.assertTrue(sync.pyExecute(function).isOk());
        Assertions.assertTrue(sync.pyExecute(function).isOk());
        registrations = sync.dumpRegistrations();
        Assertions.assertEquals(2, registrations.size());
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void testGetResults(RedisServer redis) {
        RedisModulesCommands<String, String> sync = sync(redis);
        sync.set("foo", "bar");
        ExecutionResults results = sync.pyExecute("GB().foreach(lambda x: log('test')).register()");
        Assertions.assertTrue(results.isOk());
        Assertions.assertFalse(results.isError());
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void testDumpExecutions(RedisServer redis) throws InterruptedException {
        RedisModulesCommands<String, String> sync = sync(redis);
        List<Execution> executions = sync.dumpExecutions();
        executions.forEach(e -> sync.dropExecution(e.getId()));
        sync.set("foo", "bar");
        pyExecuteUnblocking(sync, "sleep.py");
        pyExecuteUnblocking(sync, "sleep.py");
        Thread.sleep(100);
        executions = sync.dumpExecutions();
        Assertions.assertEquals(2, executions.size());
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void testDropExecution(RedisServer redis) throws InterruptedException {
        RedisModulesCommands<String, String> sync = sync(redis);
        sync.set("foo", "bar");
        pyExecuteUnblocking(sync, "sleep.py");
        pyExecuteUnblocking(sync, "sleep.py");
        Thread.sleep(100);
        List<Execution> executions = sync.dumpExecutions();
        executions.forEach(e -> sync.abortExecution(e.getId()));
        executions.forEach(e -> sync.dropExecution(e.getId()));
        Assertions.assertEquals(0, sync.dumpExecutions().size());
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void abortExecution(RedisServer redis) throws InterruptedException {
        RedisModulesCommands<String, String> sync = sync(redis);
        sync.set("foo", "bar");
        pyExecuteUnblocking(sync, "sleep.py");
        pyExecuteUnblocking(sync, "sleep.py");
        Thread.sleep(100);
        List<Execution> executions = sync.dumpExecutions();
        executions.forEach(e -> sync.abortExecution(e.getId()));
        for (Execution execution : executions) {
            ExecutionDetails details = sync.getExecution(execution.getId());
            Assertions.assertTrue(details.getPlan().getStatus().matches("done|aborted"));
        }
    }

}
