package com.redis.lettucemod;

import com.redis.lettucemod.api.gears.Execution;
import com.redis.lettucemod.api.gears.ExecutionDetails;
import com.redis.lettucemod.api.gears.Registration;
import com.redis.lettucemod.api.sync.RedisGearsCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.output.ExecutionResults;
import com.redis.testcontainers.RedisServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;

public class GearsTests extends AbstractModuleTestBase {

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
        return sync.pyexecute(load(resourceName));
    }

    @SuppressWarnings("SameParameterValue")
    private String pyExecuteUnblocking(RedisGearsCommands<String, String> sync, String resourceName) {
        return sync.pyexecuteUnblocking(load(resourceName));
    }

    private String load(String resourceName) {
        return Utils.toString(getClass().getClassLoader().getResourceAsStream(resourceName));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void dumpRegistrations(RedisServer redis) {
        RedisModulesCommands<String, String> sync = sync(redis);
        // Single registration
        List<Registration> registrations = sync.dumpregistrations();
        Assertions.assertEquals(0, registrations.size());
        ExecutionResults results = pyExecute(sync, "streamreader.py");
        Assertions.assertFalse(results.isError());
        registrations = sync.dumpregistrations();
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
        sync.dumpregistrations().forEach(r -> sync.unregister(r.getId()));
        String function = "GB('KeysReader').register('*', keyTypes=['hash'])";
        Assertions.assertTrue(sync.pyexecute(function).isOk());
        Assertions.assertTrue(sync.pyexecute(function).isOk());
        registrations = sync.dumpregistrations();
        Assertions.assertEquals(2, registrations.size());
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void testGetResults(RedisServer redis) {
        RedisModulesCommands<String, String> sync = sync(redis);
        sync.set("foo", "bar");
        ExecutionResults results = sync.pyexecute("GB().foreach(lambda x: log('test')).register()");
        Assertions.assertTrue(results.isOk());
        Assertions.assertFalse(results.isError());
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void testDumpExecutions(RedisServer redis) throws InterruptedException {
        RedisModulesCommands<String, String> sync = sync(redis);
        List<Execution> executions = sync.dumpexecutions();
        executions.forEach(e -> sync.dropexecution(e.getId()));
        sync.set("foo", "bar");
        pyExecuteUnblocking(sync, "sleep.py");
        pyExecuteUnblocking(sync, "sleep.py");
        Thread.sleep(100);
        executions = sync.dumpexecutions();
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
        List<Execution> executions = sync.dumpexecutions();
        executions.forEach(e -> sync.abortexecution(e.getId()));
        executions.forEach(e -> sync.dropexecution(e.getId()));
        Assertions.assertEquals(0, sync.dumpexecutions().size());
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void abortExecution(RedisServer redis) throws InterruptedException {
        RedisModulesCommands<String, String> sync = sync(redis);
        sync.set("foo", "bar");
        pyExecuteUnblocking(sync, "sleep.py");
        pyExecuteUnblocking(sync, "sleep.py");
        Thread.sleep(100);
        List<Execution> executions = sync.dumpexecutions();
        executions.forEach(e -> sync.abortexecution(e.getId()));
        for (Execution execution : executions) {
            ExecutionDetails details = sync.getexecution(execution.getId());
            Assertions.assertTrue(details.getPlan().getStatus().matches("done|aborted"));
        }
    }

}
