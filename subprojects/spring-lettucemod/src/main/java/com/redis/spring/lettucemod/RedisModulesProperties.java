package com.redis.spring.lettucemod;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for Redis Cluster.
 *
 */
@ConfigurationProperties(prefix = "spring.redis")
public class RedisModulesProperties {

	private boolean cluster;

	public boolean getCluster() {
		return this.cluster;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

}
