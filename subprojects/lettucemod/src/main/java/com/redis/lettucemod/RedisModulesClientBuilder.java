package com.redis.lettucemod;

import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.internal.LettuceAssert;

public class RedisModulesClientBuilder {

	private RedisURI uri = RedisURI.create(RedisURIBuilder.DEFAULT_HOST, RedisURI.DEFAULT_REDIS_PORT);
	private boolean cluster;
	private ClientOptions clientOptions;

	public AbstractRedisClient build() {
		if (cluster) {
			RedisModulesClusterClient client = RedisModulesClusterClient.create(uri);
			if (clientOptions != null) {
				client.setOptions(clusterClientOptions());
			}
			return client;
		}
		RedisModulesClient client = RedisModulesClient.create(uri);
		if (clientOptions != null) {
			client.setOptions(clientOptions);
		}
		return client;
	}

	private ClusterClientOptions clusterClientOptions() {
		LettuceAssert.isTrue(clientOptions instanceof ClusterClientOptions,
				"Client options must be an instance of ClusterClientOptions");
		return (ClusterClientOptions) clientOptions;
	}

	public RedisModulesClientBuilder uri(RedisURI uri) {
		this.uri = uri;
		return this;
	}

	public RedisModulesClientBuilder cluster(boolean cluster) {
		this.cluster = cluster;
		return this;
	}

	public RedisModulesClientBuilder clientOptions(ClientOptions options) {
		this.clientOptions = options;
		return this;
	}

}
