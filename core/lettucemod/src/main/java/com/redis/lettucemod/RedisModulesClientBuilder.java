package com.redis.lettucemod;

import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.resource.ClientResources;

public class RedisModulesClientBuilder {

	private RedisURI uri = RedisURI.create(RedisURIBuilder.DEFAULT_HOST, RedisURI.DEFAULT_REDIS_PORT);
	private boolean cluster;
	private ClientOptions options;
	private ClientResources resources;

	public AbstractRedisClient build() {
		if (cluster) {
			RedisModulesClusterClient client = resources == null ? RedisModulesClusterClient.create(uri)
					: RedisModulesClusterClient.create(resources, uri);
			if (options != null) {
				client.setOptions((ClusterClientOptions) options);
			}
			return client;
		}
		RedisModulesClient client = resources == null ? RedisModulesClient.create(uri)
				: RedisModulesClient.create(resources, uri);
		if (options != null) {
			client.setOptions(options);
		}
		return client;
	}

	public RedisModulesClientBuilder uri(RedisURI uri) {
		this.uri = uri;
		return this;
	}

	public RedisModulesClientBuilder cluster(boolean cluster) {
		this.cluster = cluster;
		return this;
	}

	public RedisModulesClientBuilder resources(ClientResources resources) {
		this.resources = resources;
		return this;
	}

	public RedisModulesClientBuilder options(ClientOptions options) {
		this.options = options;
		return this;
	}

}
