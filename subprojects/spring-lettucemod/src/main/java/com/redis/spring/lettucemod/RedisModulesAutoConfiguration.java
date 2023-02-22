package com.redis.spring.lettucemod;

import java.time.Duration;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties.Lettuce.Cluster.Refresh;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties.Pool;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.util.RedisURIBuilder;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions.Builder;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RedisProperties.class)
public class RedisModulesAutoConfiguration {

	@Bean
	RedisURI redisURI(RedisProperties properties) {
		RedisURIBuilder builder = RedisURIBuilder.create();
		builder.clientName(properties.getClientName());
		builder.database(properties.getDatabase());
		builder.host(properties.getHost());
		builder.password(properties.getPassword());
		builder.port(properties.getPort());
		builder.ssl(properties.isSsl());
		builder.timeout(properties.getTimeout());
		builder.uri(properties.getUrl());
		builder.username(properties.getUsername());
		properties.getConnectTimeout();
		return builder.build();
	}

	private <B extends ClientOptions.Builder> B clientOptions(B builder, RedisProperties properties) {
		Duration connectTimeout = properties.getConnectTimeout();
		if (connectTimeout != null) {
			builder.socketOptions(SocketOptions.builder().connectTimeout(connectTimeout).build());
		}
		builder.timeoutOptions(TimeoutOptions.enabled());
		return builder;
	}

	private ClusterClientOptions clusterClientOptions(RedisProperties properties) {
		ClusterClientOptions.Builder builder = ClusterClientOptions.builder();
		Refresh refreshProperties = properties.getLettuce().getCluster().getRefresh();
		Builder refreshBuilder = ClusterTopologyRefreshOptions.builder()
				.dynamicRefreshSources(refreshProperties.isDynamicRefreshSources());
		if (refreshProperties.getPeriod() != null) {
			refreshBuilder.enablePeriodicRefresh(refreshProperties.getPeriod());
		}
		if (refreshProperties.isAdaptive()) {
			refreshBuilder.enableAllAdaptiveRefreshTriggers();
		}
		builder.topologyRefreshOptions(refreshBuilder.build());
		return clientOptions(builder, properties).build();
	}

	@Bean(destroyMethod = "shutdown")
	ClientResources clientResources() {
		return DefaultClientResources.create();
	}

	@Bean(destroyMethod = "shutdown")
	AbstractRedisClient client(RedisURI redisURI, RedisProperties properties, ClientResources clientResources) {
		if (properties.getCluster() != null) {
			RedisModulesClusterClient client = RedisModulesClusterClient.create(clientResources, redisURI);
			client.setOptions(clusterClientOptions(properties));
			return client;
		}
		RedisModulesClient client = RedisModulesClient.create(clientResources, redisURI);
		client.setOptions(clientOptions(ClientOptions.builder(), properties).build());
		return client;
	}

	@Bean(name = "redisModulesConnection", destroyMethod = "close")
	StatefulRedisModulesConnection<String, String> connection(RedisModulesClient redisModulesClient) {
		return redisModulesClient.connect();
	}

	@Bean(name = "redisModulesConnectionPoolConfig")
	GenericObjectPoolConfig<StatefulRedisModulesConnection<String, String>> poolConfig(
			RedisProperties redisProperties) {
		return configure(redisProperties, new GenericObjectPoolConfig<>());
	}

	public <K, V> GenericObjectPoolConfig<StatefulRedisModulesConnection<K, V>> configure(
			RedisProperties redisProperties, GenericObjectPoolConfig<StatefulRedisModulesConnection<K, V>> config) {
		config.setJmxEnabled(false);
		Pool poolProps = redisProperties.getLettuce().getPool();
		if (poolProps != null) {
			config.setMaxTotal(poolProps.getMaxActive());
			config.setMaxIdle(poolProps.getMaxIdle());
			config.setMinIdle(poolProps.getMinIdle());
			if (poolProps.getMaxWait() != null) {
				config.setMaxWait(poolProps.getMaxWait());
			}
		}
		return config;
	}

	@Bean(name = "redisModulesConnectionPool", destroyMethod = "close")
	GenericObjectPool<StatefulRedisModulesConnection<String, String>> pool(
			GenericObjectPoolConfig<StatefulRedisModulesConnection<String, String>> config, RedisModulesClient client) {
		return ConnectionPoolSupport.createGenericObjectPool(client::connect, config);
	}

}
