package com.redis.spring.lettucemod.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import io.lettuce.core.AbstractRedisClient;

@Component
public class RedisService implements InitializingBean {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final AbstractRedisClient client;

	public RedisService(AbstractRedisClient client) {
		this.client = client;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		log.info("Pinging Redis");
		StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client);
		String reply = connection.sync().ping();
		if ("PONG".equalsIgnoreCase(reply)) {
			log.info("Successfully pinged Redis (response: {})", reply);
		} else {
			log.error("Failed to ping Redis (response: {})", reply);
		}

	}

}
