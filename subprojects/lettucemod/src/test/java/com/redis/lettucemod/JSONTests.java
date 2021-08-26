package com.redis.lettucemod;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.api.sync.RedisJSONCommands;
import com.redis.lettucemod.json.GetOptions;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.KeyValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;

public class JSONTests extends AbstractModuleTestBase {

    private static final String JSON = "{\"name\":\"Leonard Cohen\",\"lastSeen\":1478476800,\"loggedOut\": true}";

    @ParameterizedTest
    @MethodSource("redisServers")
    void set(RedisServer redis) {
        RedisJSONCommands<String, String> sync = sync(redis);
        String result = sync.set("obj", ".", JSON);
        Assertions.assertEquals("OK", result);
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void setNX(RedisServer redis) {
        RedisJSONCommands<String, String> sync = sync(redis);
        sync.set("obj", ".", JSON);
        String result = sync.setNX("obj", ".", JSON);
        Assertions.assertNull(result);
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void setXX(RedisServer redis) {
        RedisJSONCommands<String, String> sync = sync(redis);
        String result = sync.setXX("obj", ".", "true");
        Assertions.assertNull(result);
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void get(RedisServer redis) throws JsonProcessingException {
        RedisJSONCommands<String, String> sync = sync(redis);
        sync.set("obj", ".", JSON);
        String result = sync.get("obj");
        ObjectMapper mapper = new ObjectMapper();
        Assertions.assertEquals(mapper.readTree(JSON), mapper.readTree(result));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void getPaths(RedisServer redis) throws JsonProcessingException {
        RedisJSONCommands<String, String> sync = sync(redis);
        sync.set("obj", ".", JSON);
        String result = sync.get("obj", ".name", ".loggedOut");
        ObjectMapper mapper = new ObjectMapper();
        Assertions.assertEquals(mapper.readTree("{\".name\":\"Leonard Cohen\",\".loggedOut\": true}"), mapper.readTree(result));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void getOptions(RedisServer redis) {
        RedisJSONCommands<String, String> sync = sync(redis);
        sync.set("obj", ".", JSON);
        String result = sync.get("obj", GetOptions.<String, String>builder().indent("___").newline("#").noEscape(true).space("_").build());
        Assertions.assertEquals("{#___\"name\":_\"Leonard Cohen\",#___\"lastSeen\":_1478476800,#___\"loggedOut\":_true#}", result);
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void getOptionsPaths(RedisServer redis) {
        RedisJSONCommands<String, String> sync = sync(redis);
        sync.set("obj", ".", JSON);
        String result = sync.get("obj", GetOptions.<String, String>builder().indent("___").newline("#").noEscape(true).space("_").build(), ".name", ".loggedOut");
        Assertions.assertEquals("{#___\".name\":_\"Leonard Cohen\",#___\".loggedOut\":_true#}", result);
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void mget(RedisServer redis) throws JsonProcessingException {
        RedisJSONCommands<String, String> sync = sync(redis);
        sync.set("obj1", ".", JSON);
        String json2 = "{\"name\":\"Herbie Hancock\",\"lastSeen\":1478476810,\"loggedOut\": false}";
        sync.set("obj2", ".", json2);
        String json3 = "{\"name\":\"Lalo Schifrin\",\"lastSeen\":1478476820,\"loggedOut\": false}";
        sync.set("obj3", ".", json3);

        List<KeyValue<String, String>> results = sync.mget(".", "obj1", "obj2", "obj3");
        Assertions.assertEquals(3, results.size());
        ObjectMapper mapper = new ObjectMapper();
        Assertions.assertEquals("obj1", results.get(0).getKey());
        Assertions.assertEquals("obj2", results.get(1).getKey());
        Assertions.assertEquals("obj3", results.get(2).getKey());
        Assertions.assertEquals(mapper.readTree(JSON), mapper.readTree(results.get(0).getValue()));
        Assertions.assertEquals(mapper.readTree(json2), mapper.readTree(results.get(1).getValue()));
        Assertions.assertEquals(mapper.readTree(json3), mapper.readTree(results.get(2).getValue()));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void del(RedisServer redis) {
        RedisJSONCommands<String, String> sync = sync(redis);
        sync.set("obj", ".", JSON);
        sync.del("obj");
        String result = sync.get("obj");
        Assertions.assertNull(result);
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void type(RedisServer redis) {
        RedisJSONCommands<String, String> sync = sync(redis);
        sync.set("obj", ".", JSON);
        Assertions.assertEquals("object", sync.type("obj"));
        Assertions.assertEquals("string", sync.type("obj", ".name"));
        Assertions.assertEquals("boolean", sync.type("obj", ".loggedOut"));
        Assertions.assertEquals("integer", sync.type("obj", ".lastSeen"));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void numIncrBy(RedisServer redis) {
        RedisJSONCommands<String, String> sync = sync(redis);
        sync.set("obj", ".", JSON);
        long lastSeen = 1478476800;
        double increment = 123.456;
        String result = sync.numIncrBy("obj", ".lastSeen", increment);
        Assertions.assertEquals(lastSeen + increment, Double.parseDouble(result));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void numMultBy(RedisServer redis) {
        RedisJSONCommands<String, String> sync = sync(redis);
        sync.set("obj", ".", JSON);
        long lastSeen = 1478476800;
        double factor = 123.456;
        String result = sync.numMultBy("obj", ".lastSeen", factor);
        Assertions.assertEquals(lastSeen * factor, Double.parseDouble(result));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void strings(RedisServer redis) {
        RedisJSONCommands<String, String> sync = sync(redis);
        sync.set("foo", ".", "\"bar\"");
        Assertions.assertEquals(3, sync.strLen("foo", "."));
        Assertions.assertEquals("barbaz".length(), sync.strAppend("foo", ".", "\"baz\""));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void arrays(RedisServer redis) {
        RedisJSONCommands<String, String> sync = sync(redis);
        sync.set("arr", ".", "[]");
        Assertions.assertEquals(1, sync.arrAppend("arr", ".", "0"));
        Assertions.assertEquals("[0]", sync.get("arr"));
        Assertions.assertEquals(3, sync.arrInsert("arr", ".", 0, "-2", "-1"));
        Assertions.assertEquals("[-2,-1,0]", sync.get("arr"));
        Assertions.assertEquals(1, sync.arrTrim("arr", ".", 1, 1));
        Assertions.assertEquals("[-1]", sync.get("arr"));
        Assertions.assertEquals("-1", sync.arrPop("arr"));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void obj(RedisServer redis) {
        RedisJSONCommands<String, String> sync = sync(redis);
        sync.set("obj", ".", JSON);
        Assertions.assertEquals(3, sync.objLen("obj", "."));
        Assertions.assertEquals(Arrays.asList("name", "lastSeen", "loggedOut"), sync.objKeys("obj", "."));
    }
}
