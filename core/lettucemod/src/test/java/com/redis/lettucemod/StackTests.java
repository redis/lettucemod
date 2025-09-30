package com.redis.lettucemod;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.MRangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;
import io.lettuce.core.AclSetuserArgs;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StackTests extends ModulesTests {

    private DockerImageName imageName = RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG);

    private final RedisStackContainer container = new RedisStackContainer(imageName);

    @Override
    protected RedisServer getRedisServer() {
        return container;
    }

    @Test
    void credentials() {
        String username = "alice";
        String password = "ecila";
        commands.aclSetuser(username, AclSetuserArgs.Builder.on().addPassword(password).allCommands().allKeys());
        RedisURI.Builder wrongPasswordURI = RedisURI.builder(RedisURI.create(getRedisServer().getRedisURI()));
        wrongPasswordURI.withAuthentication(username, "wrongpassword");
        try (RedisModulesClient client = RedisModulesClient.create(wrongPasswordURI.build())) {
            client.connect();
            Assertions.fail("Expected connection failure");
        } catch (Exception e) {
            // expected
        }
        String key = "foo";
        String value = "bar";
        RedisURI.Builder uri = RedisURI.builder(RedisURI.create(getRedisServer().getRedisURI()));
        uri.withAuthentication(username, password);
        try (RedisModulesClient client = RedisModulesClient.create(uri.build());
                StatefulRedisModulesConnection<String, String> connection = client.connect()) {
            commands.set(key, value);
            Assertions.assertEquals(value, commands.get(key));
        }
    }

    @Test
    void tsMget() {
        populate();
        List<GetResult<String, String>> results = commands.tsMget(FILTER);
        Assertions.assertEquals(2, results.size());
        Assertions.assertEquals(TIMESTAMP_2, results.get(0).getSample().getTimestamp());
        Assertions.assertEquals(VALUE_2, results.get(0).getSample().getValue());
        Assertions.assertEquals(TIMESTAMP_2, results.get(1).getSample().getTimestamp());
        Assertions.assertEquals(VALUE_2, results.get(1).getSample().getValue());
    }

    @Test
    void tsCreate() {
        assertEquals("OK", commands.tsCreate(TS_KEY,
                com.redis.lettucemod.timeseries.CreateOptions.<String, String> builder().retentionPeriod(6000).build()));
        String key = "virag";
        List<KeyValue<String, String>> labels = Arrays.asList(KeyValue.just("name", "value"));
        assertEquals("OK", commands.tsCreate(key, com.redis.lettucemod.timeseries.CreateOptions.<String, String> builder()
                .retentionPeriod(100000L).labels(labels).policy(DuplicatePolicy.LAST).build()));
        List<GetResult<String, String>> results = commands.tsMgetWithLabels("name=value");
        KeyValue<String, String> expectedLabel = labels.get(0);
        assertEquals(expectedLabel, results.get(0).getLabels().get(0));
    }

    @Test
    void tsQueryIndex() {
        String key1 = "tsQueryIndex:key1";
        String key2 = "tsQueryIndex:key2";
        commands.del(key1, key2);
        String id = "tsQueryIndex";
        List<KeyValue<String, String>> labels = Collections.singletonList(KeyValue.just("id", id));
        assertEquals("OK", commands.tsCreate(key1, CreateOptions.<String, String> builder().labels(labels).build()));
        assertEquals("OK", commands.tsCreate(key2, CreateOptions.<String, String> builder().labels(labels).build()));
        List<String> res = commands.tsQueryIndex(String.format("id=%s", id));
        assertTrue(res.contains(key1));
        assertTrue(res.contains(key2));
    }

    @Test
    void tsDel() {
        String key = "tsDel:key";
        commands.del(key);
        assertEquals("OK", commands.tsCreate(key, CreateOptions.<String, String> builder().build()));
        for (int i = 0; i < 5; i++) {
            commands.tsAdd(key, Sample.of(42));

            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                // ignored
            }
        }

        assertEquals(5, commands.tsDel(key, TimeRange.from(0).to(Long.MAX_VALUE).build()));
    }

    @Test
    void tsMrange() {
        populate();
        List<String> keys = Arrays.asList(TS_KEY, TS_KEY_2);
        assertMrange(keys, commands.tsMrange(TimeRange.unbounded(), MRangeOptions.<String, String> filters(FILTER).build()));
        assertMrange(keys, commands.tsMrange(TimeRange.from(TIMESTAMP_1 - 10).build(),
                MRangeOptions.<String, String> filters(FILTER).build()));
        assertMrange(keys, commands.tsMrange(TimeRange.to(TIMESTAMP_2 + 10).build(),
                MRangeOptions.<String, String> filters(FILTER).build()));
        List<RangeResult<String, String>> results = commands.tsMrange(TimeRange.unbounded(),
                MRangeOptions.<String, String> filters(FILTER).withLabels().build());
        assertEquals(2, results.size());
        RangeResult<String, String> key1Result;
        RangeResult<String, String> key2Result;
        if (results.get(0).getKey().equals(TS_KEY)) {
            key1Result = results.get(0);
            key2Result = results.get(1);
        } else {
            key1Result = results.get(1);
            key2Result = results.get(0);
        }
        assertEquals(TS_KEY, key1Result.getKey());
        assertEquals(2, key1Result.getSamples().size());
        assertEquals(TIMESTAMP_1, key1Result.getSamples().get(0).getTimestamp());
        assertEquals(VALUE_1, key1Result.getSamples().get(0).getValue());
        assertEquals(TIMESTAMP_2, key1Result.getSamples().get(1).getTimestamp());
        assertEquals(VALUE_2, key1Result.getSamples().get(1).getValue());
        assertEquals(2, key1Result.getLabels().size());
        assertEquals(SENSOR_ID, key1Result.getLabels().get(LABEL_SENSOR_ID));
        assertEquals(AREA_ID, key1Result.getLabels().get(LABEL_AREA_ID));
        assertEquals(TS_KEY_2, key2Result.getKey());
        assertEquals(2, key2Result.getSamples().size());
        assertEquals(TIMESTAMP_1, key2Result.getSamples().get(0).getTimestamp());
        assertEquals(VALUE_1, key2Result.getSamples().get(0).getValue());
        assertEquals(TIMESTAMP_2, key2Result.getSamples().get(1).getTimestamp());
        assertEquals(VALUE_2, key2Result.getSamples().get(1).getValue());
        assertEquals(2, key2Result.getLabels().size());
        assertEquals(SENSOR_ID, key2Result.getLabels().get(LABEL_SENSOR_ID));
        assertEquals(AREA_ID_2, key2Result.getLabels().get(LABEL_AREA_ID));
    }

    private void assertMrange(List<String> keys, List<RangeResult<String, String>> results) {
        assertEquals(2, results.size());
        assertEquals(new HashSet<>(keys), results.stream().map(RangeResult::getKey).collect(Collectors.toSet()));
        assertEquals(2, results.get(0).getSamples().size());
        assertEquals(TIMESTAMP_1, results.get(0).getSamples().get(0).getTimestamp());
        assertEquals(VALUE_1, results.get(0).getSamples().get(0).getValue());
        assertEquals(TIMESTAMP_2, results.get(0).getSamples().get(1).getTimestamp());
        assertEquals(VALUE_2, results.get(0).getSamples().get(1).getValue());
    }

    @Test
    void client() {
        DefaultClientResources resources = DefaultClientResources.create();
        try (RedisModulesClient client = RedisModulesClient.create();
                StatefulRedisModulesConnection<String, String> connection = client
                        .connect(RedisURI.create(container.getRedisURI()))) {
            assertPing(connection);
        }
        try (RedisModulesClient client = RedisModulesClient.create(resources);
                StatefulRedisModulesConnection<String, String> connection = client
                        .connect(RedisURI.create(container.getRedisURI()))) {
            assertPing(connection);
        }
        try (RedisModulesClient client = RedisModulesClient.create(resources, container.getRedisURI());
                StatefulRedisModulesConnection<String, String> connection = client.connect()) {
            assertPing(connection);
        }
        try (RedisModulesClient client = RedisModulesClient.create(resources, RedisURI.create(container.getRedisURI()));
                StatefulRedisModulesConnection<String, String> connection = client.connect()) {
            assertPing(connection);
        }
        try (RedisModulesClient client = RedisModulesClient.create();
                StatefulRedisModulesConnection<String, String> connection = client.connect(StringCodec.UTF8,
                        RedisURI.create(container.getRedisURI()))) {
            assertPing(connection);
        }
        resources.shutdown();
    }



}
