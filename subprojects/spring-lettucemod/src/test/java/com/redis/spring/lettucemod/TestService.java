package com.redis.spring.lettucemod;

import org.springframework.stereotype.Service;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;

@Service
public class TestService {

	private final RedisModulesClient client;

	public TestService(RedisModulesClient client) {
		this.client = client;
	}

	public String ping() {
		try (StatefulRedisModulesConnection<String, String> connection = client.connect()) {
			return connection.sync().ping();
		}
	}
}
