package com.redislabs.mesclun;

import com.redislabs.mesclun.gears.Execution;
import com.redislabs.mesclun.gears.ExecutionDetails;
import com.redislabs.mesclun.gears.RedisGearsUtils;
import com.redislabs.mesclun.gears.Registration;
import com.redislabs.mesclun.gears.output.ExecutionResults;
import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

@Testcontainers
public class TestGears {

    private RedisModulesClient client;
    protected StatefulRedisModulesConnection<String, String> connection;
    protected RedisModulesCommands<String, String> sync;
    protected RedisModulesAsyncCommands<String, String> async;
    protected RedisModulesReactiveCommands<String, String> reactive;

    protected String host;
    protected int port;

    @Container
    @SuppressWarnings("rawtypes")
    public static final GenericContainer REDISGEARS = new GenericContainer(DockerImageName.parse("redislabs/redisgears")).withExposedPorts(6379);

    @BeforeEach
    public void setup() {
        host = REDISGEARS.getHost();
        port = REDISGEARS.getFirstMappedPort();
        client = RedisModulesClient.create(RedisURI.create(host, port));
        connection = client.connect();
        sync = connection.sync();
        async = connection.async();
        reactive = connection.reactive();
        sync.flushall();
    }

    @AfterEach
    public void teardown() {
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }

    @Test
    public void testPyExecute() {
        sync.set("foo", "bar");
        ExecutionResults results = pyExecute("sleep.py");
        Assertions.assertEquals("1", results.getResults().get(0));
    }

    @Test
    public void testPyExecuteUnblocking() {
        sync.set("foo", "bar");
        String executionId = pyExecuteUnblocking("sleep.py");
        String[] array = executionId.split("-");
        Assertions.assertEquals(2, array.length);
        Assertions.assertEquals("0000000000000000000000000000000000000000", array[0]);
        Assertions.assertTrue(Integer.parseInt(array[1]) >= 0);
    }

    @Test
    public void testPyExecuteNoResults() {
        ExecutionResults results = pyExecute("sleep.py");
        Assertions.assertTrue(results.getResults().isEmpty());
        Assertions.assertTrue(results.getErrors().isEmpty());
    }

    private ExecutionResults pyExecute(String resourceName) {
        return sync.pyExecute(load(resourceName));
    }

    private String pyExecuteUnblocking(String resourceName) {
        return sync.pyExecuteUnblocking(load(resourceName));
    }

    private String load(String resourceName) {
        return RedisGearsUtils.toString(getClass().getClassLoader().getResourceAsStream(resourceName));
    }

    @Test
    public void testDumpRegistrations() {
        List<Registration> registrations = sync.dumpRegistrations();
        Assertions.assertEquals(0, registrations.size());
        ExecutionResults results = pyExecute("streamreader.py");
        Assertions.assertFalse(results.isError());
        registrations = sync.dumpRegistrations();
        Assertions.assertEquals(1, registrations.size());
        Registration registration = registrations.get(0);
        Assertions.assertEquals("StreamReader", registration.getReader());
        Assertions.assertNull(registration.getDescription());
        Assertions.assertEquals("async", registration.getData().getMode());
        Map<String, Object> args = registration.getData().getArgs();
        Assertions.assertEquals(3, args.size());
        Assertions.assertEquals(1L, args.get("batchSize"));
        Assertions.assertEquals("mystream", args.get("stream"));
        Assertions.assertEquals("OK", registration.getData().getStatus());
        Assertions.assertEquals("{'sessionId':'0000000000000000000000000000000000000000-0', 'depsList':[]}", registration.getPrivateData());
    }

    @Test
    public void testDumpExecutions() throws InterruptedException {
        List<Execution> executions = sync.dumpExecutions();
        executions.forEach(e -> sync.dropExecution(e.getId()));
        sync.set("foo", "bar");
        pyExecuteUnblocking("sleep.py");
        pyExecuteUnblocking("sleep.py");
        Thread.sleep(100);
        executions = sync.dumpExecutions();
        Assertions.assertEquals(2, executions.size());
    }

    @Test
    public void testDropExecution() throws InterruptedException {
        sync.set("foo", "bar");
        pyExecuteUnblocking("sleep.py");
        pyExecuteUnblocking("sleep.py");
        Thread.sleep(100);
        List<Execution> executions = sync.dumpExecutions();
        executions.forEach(e -> sync.abortExecution(e.getId()));
        executions.forEach(e -> sync.dropExecution(e.getId()));
        Assertions.assertEquals(0, sync.dumpExecutions().size());
    }

    @Test
    public void testAbortExecution() throws InterruptedException {
        sync.set("foo", "bar");
        pyExecuteUnblocking("sleep.py");
        pyExecuteUnblocking("sleep.py");
        Thread.sleep(100);
        List<Execution> executions = sync.dumpExecutions();
        executions.forEach(e -> sync.abortExecution(e.getId()));
        for (Execution execution : executions) {
            ExecutionDetails details = sync.getExecution(execution.getId());
            Assertions.assertEquals("done", details.getPlan().getStatus());
        }
    }

}
