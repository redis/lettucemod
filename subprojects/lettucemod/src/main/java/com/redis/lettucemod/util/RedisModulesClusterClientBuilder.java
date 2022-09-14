package com.redis.lettucemod.util;

import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.RedisURI;

public class RedisModulesClusterClientBuilder extends AbstractClusterClientBuilder<RedisModulesClusterClientBuilder> {

	private RedisModulesClusterClientBuilder(RedisURI redisURI) {
		super(redisURI);
	}

	@Override
	public RedisModulesClusterClient build() {
		return clusterClient();
	}

	public static RedisModulesClusterClientBuilder create(RedisURI redisURI) {
		return new RedisModulesClusterClientBuilder(redisURI);
	}

}
