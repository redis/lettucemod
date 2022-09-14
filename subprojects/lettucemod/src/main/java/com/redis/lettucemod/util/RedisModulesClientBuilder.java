package com.redis.lettucemod.util;

import com.redis.lettucemod.RedisModulesClient;

import io.lettuce.core.RedisURI;

public class RedisModulesClientBuilder extends AbstractClientBuilder<RedisModulesClientBuilder> {

	private RedisModulesClientBuilder(RedisURI redisURI) {
		super(redisURI);
	}

	@Override
	public RedisModulesClient build() {
		return client();
	}

	public static RedisModulesClientBuilder create(RedisURI redisURI) {
		return new RedisModulesClientBuilder(redisURI);
	}
}
