package com.redis.spring.lettucemod;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

@Service
@ConditionalOnBean(RedisModulesClusterClient.class)
public class TestClusterService {

	private final RedisModulesClusterClient client;

	public TestClusterService(RedisModulesClusterClient client) {
		this.client = client;
	}

	public String ping() {
		try (StatefulRedisModulesConnection<String, String> connection = client.connect()) {
			return connection.sync().ping();
		}
	}
}
