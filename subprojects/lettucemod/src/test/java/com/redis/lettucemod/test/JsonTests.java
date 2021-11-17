package com.redis.lettucemod.test;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.api.sync.RedisJSONCommands;
import com.redis.lettucemod.json.GetOptions;
import com.redis.lettucemod.json.SetMode;
import com.redis.testcontainers.junit.jupiter.RedisTestContext;
import com.redis.testcontainers.junit.jupiter.RedisTestContextsSource;

import io.lettuce.core.KeyValue;

class JsonTests extends AbstractLettuceModTestBase {

	private static final String JSON = "{\"name\":\"Leonard Cohen\",\"lastSeen\":1478476800,\"loggedOut\": true}";

	@ParameterizedTest
	@RedisTestContextsSource
	void set(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		String result = sync.jsonSet("obj", ".", JSON);
		Assertions.assertEquals("OK", result);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void setNX(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		String result = sync.jsonSet("obj", ".", JSON, SetMode.NX);
		Assertions.assertNull(result);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void setXX(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		String result = sync.jsonSet("obj", ".", "true", SetMode.XX);
		Assertions.assertNull(result);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void get(RedisTestContext context) throws JsonProcessingException {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		String result = sync.jsonGet("obj");
		ObjectMapper mapper = new ObjectMapper();
		Assertions.assertEquals(mapper.readTree(JSON), mapper.readTree(result));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void getPaths(RedisTestContext context) throws JsonProcessingException {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		String result = sync.jsonGet("obj", ".name", ".loggedOut");
		ObjectMapper mapper = new ObjectMapper();
		Assertions.assertEquals(mapper.readTree("{\".name\":\"Leonard Cohen\",\".loggedOut\": true}"),
				mapper.readTree(result));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void getOptions(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		String result = sync.jsonGet("obj",
				GetOptions.builder().indent("___").newline("#").noEscape(true).space("_").build());
		Assertions.assertEquals(
				"{#___\"name\":_\"Leonard Cohen\",#___\"lastSeen\":_1478476800,#___\"loggedOut\":_true#}", result);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void getOptionsPaths(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		String result = sync.jsonGet("obj",
				GetOptions.builder().indent("___").newline("#").noEscape(true).space("_").build(), ".name",
				".loggedOut");
		Assertions.assertEquals("{#___\".name\":_\"Leonard Cohen\",#___\".loggedOut\":_true#}", result);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void mget(RedisTestContext context) throws JsonProcessingException {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj1", ".", JSON);
		String json2 = "{\"name\":\"Herbie Hancock\",\"lastSeen\":1478476810,\"loggedOut\": false}";
		sync.jsonSet("obj2", ".", json2);
		String json3 = "{\"name\":\"Lalo Schifrin\",\"lastSeen\":1478476820,\"loggedOut\": false}";
		sync.jsonSet("obj3", ".", json3);

		List<KeyValue<String, String>> results = sync.jsonMget(".", "obj1", "obj2", "obj3");
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
	@RedisTestContextsSource
	void del(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		sync.jsonDel("obj");
		String result = sync.jsonGet("obj");
		Assertions.assertNull(result);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void type(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		Assertions.assertEquals("object", sync.jsonType("obj"));
		Assertions.assertEquals("string", sync.jsonType("obj", ".name"));
		Assertions.assertEquals("boolean", sync.jsonType("obj", ".loggedOut"));
		Assertions.assertEquals("integer", sync.jsonType("obj", ".lastSeen"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void numIncrBy(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		long lastSeen = 1478476800;
		double increment = 123.456;
		String result = sync.numincrby("obj", ".lastSeen", increment);
		Assertions.assertEquals(lastSeen + increment, Double.parseDouble(result));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void numMultBy(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		long lastSeen = 1478476800;
		double factor = 123.456;
		String result = sync.nummultby("obj", ".lastSeen", factor);
		Assertions.assertEquals(lastSeen * factor, Double.parseDouble(result));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void strings(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("foo", ".", "\"bar\"");
		Assertions.assertEquals(3, sync.strlen("foo", "."));
		Assertions.assertEquals("barbaz".length(), sync.strappend("foo", ".", "\"baz\""));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void arrays(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("arr", ".", "[]");
		Assertions.assertEquals(1, sync.arrappend("arr", ".", "0"));
		Assertions.assertEquals("[0]", sync.jsonGet("arr"));
		Assertions.assertEquals(3, sync.arrinsert("arr", ".", 0, "-2", "-1"));
		Assertions.assertEquals("[-2,-1,0]", sync.jsonGet("arr"));
		Assertions.assertEquals(1, sync.arrtrim("arr", ".", 1, 1));
		Assertions.assertEquals("[-1]", sync.jsonGet("arr"));
		Assertions.assertEquals("-1", sync.arrpop("arr"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void obj(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		Assertions.assertEquals(3, sync.objlen("obj", "."));
		Assertions.assertEquals(Arrays.asList("name", "lastSeen", "loggedOut"), sync.objkeys("obj", "."));
	}
}
