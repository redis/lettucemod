package com.redis.lettucemod.util;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public class ClientBuilder extends AbstractClusterClientBuilder<ClientBuilder> {

	private boolean cluster;

	private ClientBuilder(RedisURI redisURI) {
		super(redisURI);
	}

	public ClientBuilder cluster(boolean cluster) {
		this.cluster = cluster;
		return this;
	}

	@Override
	public AbstractRedisClient build() {
		if (cluster) {
			return clusterClient();
		}
		return client();
	}

	public static ClientBuilder create(RedisURI redisURI) {
		return new ClientBuilder(redisURI);
	}

}
