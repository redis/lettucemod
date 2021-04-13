package com.redislabs.mesclun;

import com.redislabs.mesclun.gears.GearsUtils;
import com.redislabs.mesclun.gears.PyExecuteOptions;
import com.redislabs.mesclun.gears.Registration;
import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    @SuppressWarnings("unchecked")
    @Test
    public void testPyExecute() {
        String status = sync.pyExecute(GearsUtils.toString(getClass().getClassLoader().getResourceAsStream("streamreader.py")), PyExecuteOptions.builder().build());
        Assertions.assertEquals("OK", status);
        Map<String, String> message1 = new HashMap<>();
        message1.put("field1", "value1");
        String messageId = sync.xadd("mystream", message1);
        Set<String> myset = sync.smembers("myset");
        Assertions.assertEquals(1, myset.size());
        Assertions.assertEquals(messageId, myset.iterator().next());
    }

    @Test
    public void testDumpRegistrations() {
        List<Registration> registrations = sync.dumpRegistrations();
        Assertions.assertEquals(0, registrations.size());
        String status = sync.pyExecute(GearsUtils.toString(getClass().getClassLoader().getResourceAsStream("streamreader.py")), PyExecuteOptions.builder().build());
        Assertions.assertEquals("OK", status);
        registrations = sync.dumpRegistrations();
        Assertions.assertEquals(1, registrations.size());
        Registration registration = registrations.get(0);
        Assertions.assertEquals("StreamReader", registration.getReader());
        Assertions.assertEquals(null, registration.getDescription());
        Assertions.assertEquals("async", registration.getData().getMode());
        Map<String, Object> args = registration.getData().getArgs();
        Assertions.assertEquals(3, args.size());
        Assertions.assertEquals(1L, args.get("batchSize"));
        Assertions.assertEquals("mystream", args.get("stream"));
        Assertions.assertEquals("OK", registration.getData().getStatus());
        Assertions.assertEquals("{'sessionId':'0000000000000000000000000000000000000000-0', 'depsList':[]}", registration.getPrivateData());
    }

}
