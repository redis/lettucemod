package com.redislabs.mesclun;

import com.redislabs.mesclun.gears.*;
import com.redislabs.mesclun.gears.output.ExecutionResults;
import com.redislabs.testcontainers.BaseRedisModulesTest;
import com.redislabs.testcontainers.RedisModulesContainer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;

@SuppressWarnings("SameParameterValue")
@Testcontainers
public class TestGears extends BaseRedisModulesTest {

    @ParameterizedTest
    @MethodSource("containers")
    void pyExecute(RedisModulesContainer redisContainer) {
        RedisModulesCommands<String, String> sync = redisContainer.sync();
        sync.set("foo", "bar");
        ExecutionResults results = pyExecute(sync, "sleep.py");
        Assertions.assertEquals("1", results.getResults().get(0));
    }

    @ParameterizedTest
    @MethodSource("containers")
    void pyExecuteUnblocking(RedisModulesContainer redisContainer) {
        RedisModulesCommands<String, String> sync = redisContainer.sync();
        sync.set("foo", "bar");
        String executionId = pyExecuteUnblocking(sync, "sleep.py");
        String[] array = executionId.split("-");
        Assertions.assertEquals(2, array.length);
        Assertions.assertEquals("0000000000000000000000000000000000000000", array[0]);
        Assertions.assertTrue(Integer.parseInt(array[1]) >= 0);
    }

    @ParameterizedTest
    @MethodSource("containers")
    void pyExecuteNoResults(RedisModulesContainer redisContainer) {
        RedisModulesCommands<String, String> sync = redisContainer.sync();
        ExecutionResults results = pyExecute(sync, "sleep.py");
        Assertions.assertTrue(results.getResults().isEmpty());
        Assertions.assertTrue(results.getErrors().isEmpty());
    }

    private ExecutionResults pyExecute(RedisGearsCommands<String, String> sync, String resourceName) {
        return sync.pyExecute(load(resourceName));
    }

    private String pyExecuteUnblocking(RedisGearsCommands<String, String> sync, String resourceName) {
        return sync.pyExecuteUnblocking(load(resourceName));
    }

    private String load(String resourceName) {
        return RedisGearsUtils.toString(getClass().getClassLoader().getResourceAsStream(resourceName));
    }

    @ParameterizedTest
    @MethodSource("containers")
    void dumpRegistrations(RedisModulesContainer redisContainer) {
        RedisModulesCommands<String, String> sync = redisContainer.sync();

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
    @MethodSource("containers")
    void testGetResults(RedisModulesContainer redisContainer) {
        RedisModulesCommands<String, String> sync = redisContainer.sync();
        sync.set("foo", "bar");
        ExecutionResults results = sync.pyExecute("GB().foreach(lambda x: log('test')).register()");
        Assertions.assertTrue(results.isOk());
        Assertions.assertFalse(results.isError());
    }

    @ParameterizedTest
    @MethodSource("containers")
    void testDumpExecutions(RedisModulesContainer redisContainer) throws InterruptedException {
        RedisModulesCommands<String, String> sync = redisContainer.sync();
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
    @MethodSource("containers")
    void testDropExecution(RedisModulesContainer redisContainer) throws InterruptedException {
        RedisModulesCommands<String, String> sync = redisContainer.sync();
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
    @MethodSource("containers")
    void abortExecution(RedisModulesContainer redisContainer) throws InterruptedException {
        RedisModulesCommands<String,String> sync = redisContainer.sync();
        sync.set("foo", "bar");
        pyExecuteUnblocking(sync, "sleep.py");
        pyExecuteUnblocking(sync, "sleep.py");
        Thread.sleep(100);
        List<Execution> executions = sync.dumpExecutions();
        executions.forEach(e -> sync.abortExecution(e.getId()));
        for (Execution execution : executions) {
            ExecutionDetails details = sync.getExecution(execution.getId());
            Assertions.assertEquals("done", details.getPlan().getStatus());
        }
    }

}
