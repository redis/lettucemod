package com.redis.lettucemod.spring;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.junit.jupiter.Container;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.testcontainers.RedisStackContainer;

@SpringBootTest(classes = TestApplication.class)
@RunWith(SpringRunner.class)
class RedisStackIntegrationTests {

	@Autowired
	private RedisModulesClient client;

	@Container
	static final RedisStackContainer container = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	@DynamicPropertySource
	static void redisProperties(DynamicPropertyRegistry registry) {
		container.start();
		registry.add("spring.data.redis.url", container::getRedisURI);
	}

	@Test
	void connectionTest() {
		Assertions.assertEquals("PONG", client.connect().sync().ping());
	}
}