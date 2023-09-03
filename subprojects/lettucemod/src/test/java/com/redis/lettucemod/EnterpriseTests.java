package com.redis.lettucemod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.unit.DataSize;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.lettucemod.api.sync.RedisGearsCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.lettucemod.gears.Execution;
import com.redis.lettucemod.gears.ExecutionDetails;
import com.redis.lettucemod.gears.Registration;
import com.redis.lettucemod.output.ExecutionResults;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;

@EnabledOnOs(OS.LINUX)
class EnterpriseTests extends ModulesTests {

    private static final Logger log = LoggerFactory.getLogger(EnterpriseTests.class);

    private final RedisEnterpriseContainer container = new RedisEnterpriseContainer(
            RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag("latest"))
                    .withDatabase(Database.name("ModulesTests").memory(DataSize.ofMegabytes(110)).ossCluster(true)
                            .modules(RedisModule.SEARCH, RedisModule.JSON, RedisModule.GEARS, RedisModule.TIMESERIES).build());

    @Override
    protected RedisServer getRedisServer() {
        return container;
    }

    @Test
    void client() {
        try (RedisModulesClusterClient client = RedisModulesClusterClient.create(container.getRedisURI());
                StatefulRedisModulesClusterConnection<String, String> connection = client.connect();) {
            assertPing(connection);
        }
        DefaultClientResources resources = DefaultClientResources.create();
        try (RedisModulesClusterClient client = RedisModulesClusterClient.create(resources,
                RedisURI.create(container.getRedisURI()));
                StatefulRedisModulesClusterConnection<String, String> connection = client.connect();) {
            assertPing(connection);
        }
        resources.shutdown();
        try (RedisModulesClusterClient client = RedisModulesClusterClient.create(container.getRedisURI());
                StatefulRedisModulesClusterConnection<String, String> connection = client.connect(StringCodec.UTF8);) {
            assertPing(connection);
        }
    }

    @Test
    void rgPyExecute() {
        RedisModulesCommands<String, String> sync = connection.sync();
        sync.set("foo", "bar");
        ExecutionResults results = pyExecute(sync, "sleep.py");
        assertEquals("1", results.getResults().get(0));
    }

    @Test
    void rgPyExecuteUnblocking() {
        RedisModulesCommands<String, String> sync = connection.sync();
        sync.set("foo", "bar");
        String executionId = pyExecuteUnblocking(sync, "sleep.py");
        String[] array = executionId.split("-");
        assertEquals(2, array.length);
        assertEquals(40, array[0].length());
        Assertions.assertTrue(Integer.parseInt(array[1]) >= 0);
    }

    private ExecutionResults pyExecute(RedisGearsCommands<String, String> sync, String resourceName) {
        return sync.rgPyexecute(load(resourceName));
    }

    private String pyExecuteUnblocking(RedisGearsCommands<String, String> sync, String resourceName) {
        return sync.rgPyexecuteUnblocking(load(resourceName));
    }

    private String load(String resourceName) {
        return RedisModulesUtils.toString(getClass().getClassLoader().getResourceAsStream(resourceName));
    }

    private void clearGears() throws InterruptedException {
        RedisModulesCommands<String, String> sync = connection.sync();
        // Unregister all registrations
        for (Registration registration : sync.rgDumpregistrations()) {
            log.info("Unregistering {}", registration.getId());
            sync.rgUnregister(registration.getId());
        }
        // Drop all executions
        for (Execution execution : sync.rgDumpexecutions()) {
            if (execution.getStatus().matches("running|created")) {
                log.info("Aborting execution {} with status {}", execution.getId(), execution.getStatus());
                sync.rgAbortexecution(execution.getId());
            }
            try {
                sync.rgDropexecution(execution.getId());
            } catch (RedisCommandExecutionException e) {
                log.info("Execution status: {}", execution.getStatus());
                throw e;
            }
        }
    }

    @Test
    @Disabled("This test is not passing at the moment")
    void rgDumpRegistrations() throws InterruptedException {
        clearGears();
        RedisModulesCommands<String, String> sync = connection.sync();
        // Single registration
        assertEquals(0, sync.rgDumpregistrations().size());
        ExecutionResults results = pyExecute(sync, "streamreader.py");
        Assertions.assertFalse(results.isError());
        Awaitility.await().until(() -> sync.rgDumpregistrations().size() == 1);
        Registration registration = sync.rgDumpregistrations().get(0);
        assertEquals("StreamReader", registration.getReader());
        assertEquals("MyStreamReader", registration.getDescription());
        assertEquals("async", registration.getData().getMode());
        Map<String, Object> args = registration.getData().getArgs();
        assertTrue(args.size() >= 3);
        assertEquals(1L, args.get("batchSize"));
        assertEquals("mystream", args.get("stream"));
        assertEquals("OK", registration.getData().getStatus());

        // Multiple registrations
        sync.rgDumpregistrations().forEach(r -> sync.rgUnregister(r.getId()));
        String function = "GB('KeysReader').register('*', keyTypes=['hash'])";
        Assertions.assertTrue(sync.rgPyexecute(function).isOk());
        Assertions.assertEquals(1, sync.rgDumpregistrations().size());
    }

    @Test
    void rgPyExecuteResults() {
        RedisModulesCommands<String, String> sync = connection.sync();
        sync.set("foo", "bar");
        ExecutionResults results = sync.rgPyexecute("GB().foreach(lambda x: log('test')).register()");
        Assertions.assertTrue(results.isOk());
        Assertions.assertFalse(results.isError());
    }

    private void executions() {
        RedisModulesCommands<String, String> sync = connection.sync();
        sync.set("foo", "bar");
        pyExecuteUnblocking(sync, "sleep.py");
    }

    @Test
    void rgDumpExecutions() throws InterruptedException {
        clearGears();
        executions();
        assertFalse(connection.sync().rgDumpexecutions().isEmpty());
    }

    @Test
    @Disabled("Flaky test")
    void rgDropExecution() throws InterruptedException {
        clearGears();
        executions();
        RedisModulesCommands<String, String> sync = connection.sync();
        List<Execution> executions = sync.rgDumpexecutions();
        executions.forEach(e -> sync.rgAbortexecution(e.getId()));
        executions.forEach(e -> sync.rgDropexecution(e.getId()));
        assertEquals(0, sync.rgDumpexecutions().size());
    }

    @Test
    void rgAbortExecution() throws InterruptedException {
        clearGears();
        executions();
        RedisModulesCommands<String, String> sync = connection.sync();
        for (Execution execution : sync.rgDumpexecutions()) {
            sync.rgAbortexecution(execution.getId());
            ExecutionDetails details = sync.rgGetexecution(execution.getId());
            Assertions.assertTrue(details.getPlan().getStatus().matches("done|aborted"));
        }
    }

}
