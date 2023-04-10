package com.redis.spring.lettucemod;

import java.time.Duration;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties.Lettuce.Cluster.Refresh;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties.Pool;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.lettucemod.util.RedisURIBuilder;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({ RedisProperties.class, RedisClusterProperties.class })
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

	@Bean(destroyMethod = "shutdown")
	ClientResources clientResources() {
		return DefaultClientResources.create();
	}

	@Bean(destroyMethod = "shutdown")
	@ConditionalOnProperty(name = "spring.redis.cluster.enabled", havingValue = "false", matchIfMissing = true)
	RedisModulesClient client(RedisURI redisURI, RedisProperties properties, ClientResources clientResources) {
		RedisModulesClient client = RedisModulesClient.create(clientResources, redisURI);
		client.setOptions(clientOptions(ClientOptions.builder(), properties).build());
		return client;
	}

	@Bean(destroyMethod = "shutdown")
	@ConditionalOnProperty(name = "spring.redis.cluster.enabled", havingValue = "true", matchIfMissing = false)
	RedisModulesClusterClient clusterClient(RedisURI redisURI, RedisProperties properties,
			ClientResources clientResources) {
		RedisModulesClusterClient client = RedisModulesClusterClient.create(clientResources, redisURI);
		ClusterClientOptions.Builder builder = ClusterClientOptions.builder();
		Refresh refreshProperties = properties.getLettuce().getCluster().getRefresh();
		ClusterTopologyRefreshOptions.Builder refreshBuilder = ClusterTopologyRefreshOptions.builder()
				.dynamicRefreshSources(refreshProperties.isDynamicRefreshSources());
		if (refreshProperties.getPeriod() != null) {
			refreshBuilder.enablePeriodicRefresh(refreshProperties.getPeriod());
		}
		if (refreshProperties.isAdaptive()) {
			refreshBuilder.enableAllAdaptiveRefreshTriggers();
		}
		builder.topologyRefreshOptions(refreshBuilder.build());
		client.setOptions(clientOptions(builder, properties).build());
		return client;

	}

	@Bean(name = "redisConnection", destroyMethod = "close")
	@ConditionalOnBean(RedisModulesClient.class)
	StatefulRedisModulesConnection<String, String> redisConnection(RedisModulesClient client) {
		return client.connect();
	}

	@Bean(name = "redisClusterConnection", destroyMethod = "close")
	@ConditionalOnBean(RedisModulesClusterClient.class)
	StatefulRedisModulesClusterConnection<String, String> redisClusterConnection(RedisModulesClusterClient client) {
		return client.connect();
	}

	private <K, V, C extends StatefulRedisModulesConnection<K, V>> GenericObjectPoolConfig<C> poolConfig(
			RedisProperties redisProperties) {
		GenericObjectPoolConfig<C> config = new GenericObjectPoolConfig<>();
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

	@Bean(name = "redisConnectionPool", destroyMethod = "close")
	@ConditionalOnBean(RedisModulesClient.class)
	GenericObjectPool<StatefulRedisModulesConnection<String, String>> redisConnectionPool(RedisProperties properties,
			RedisModulesClient client) {
		return ConnectionPoolSupport.createGenericObjectPool(client::connect, poolConfig(properties));
	}

	@Bean(name = "redisClusterConnectionPool", destroyMethod = "close")
	@ConditionalOnBean(RedisModulesClusterClient.class)
	GenericObjectPool<StatefulRedisModulesClusterConnection<String, String>> redisClusterConnectionPool(
			RedisProperties properties, RedisModulesClusterClient client) {
		return ConnectionPoolSupport.createGenericObjectPool(client::connect, poolConfig(properties));
	}

}
