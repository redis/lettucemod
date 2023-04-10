package com.redis.spring.lettucemod;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.junit.jupiter.Container;

import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.testcontainers.RedisClusterContainer;

@SpringBootTest(classes = TestApplication.class)
@RunWith(SpringRunner.class)
class RedisClusterIntegrationTests {

	@Autowired
	private RedisModulesClusterClient client;

	@Container
	static final RedisClusterContainer container = new RedisClusterContainer(
			RedisClusterContainer.DEFAULT_IMAGE_NAME.withTag(RedisClusterContainer.DEFAULT_TAG));

	@DynamicPropertySource
	static void redisProperties(DynamicPropertyRegistry registry) {
		container.start();
		registry.add("spring.redis.url", container::getRedisURI);
		registry.add("spring.redis.cluster.enabled", () -> true);
	}

	@Test
	void connectionTest() {
		Assertions.assertEquals("PONG", client.connect().sync().ping());
	}
}