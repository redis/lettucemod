package com.redis.spring.lettucemod;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.unit.DataSize;
import org.testcontainers.junit.jupiter.Container;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.testcontainers.RedisEnterpriseContainer;

@SpringBootTest(classes = TestApplication.class)
@RunWith(SpringRunner.class)
@EnabledOnOs(value = OS.LINUX)
class RedisEnterpriseIntegrationTests {

	@Autowired
	private TestClusterService service;

	@Container
	private static final RedisEnterpriseContainer container = new RedisEnterpriseContainer(
			RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag("latest"))
			.withDatabase(Database.name("ModulesTests").memory(DataSize.ofMegabytes(110)).ossCluster(true)
					.modules(RedisModule.SEARCH, RedisModule.JSON, RedisModule.TIMESERIES).build());

	@DynamicPropertySource
	static void redisProperties(DynamicPropertyRegistry registry) {
		container.start();
		registry.add("spring.redis.url", container::getRedisURI);
		registry.add("spring.redis.cluster", () -> true);
	}

	@Test
	void connectionTest() {
		Assertions.assertEquals("PONG", service.ping());
	}
}