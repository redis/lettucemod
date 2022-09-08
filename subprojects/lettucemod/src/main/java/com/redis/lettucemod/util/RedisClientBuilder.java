package com.redis.lettucemod.util;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisCredentials;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.StaticCredentialsProvider;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

public class RedisClientBuilder {

	private final RedisClientOptions options;

	public RedisClientBuilder() {
		this.options = RedisClientOptions.builder().build();
	}

	public RedisClientBuilder(RedisClientOptions options) {
		this.options = options;
	}

	public RedisClientOptions getOptions() {
		return options;
	}

	public AbstractRedisClient client() {
		ClientResources clientResources = clientResources();
		RedisURI redisURI = uri();
		ClientOptions clientOptions = clientOptions();
		if (options.isCluster()) {
			ClusterClientOptions.Builder clusterClientOptions = ClusterClientOptions.builder(clientOptions);
			clusterClientOptions.maxRedirects(options.getMaxRedirects());
			clusterClientOptions.validateClusterNodeMembership(options.isValidateClusterNodeMembership());
			clusterClientOptions.nodeFilter(options.getNodeFilter());
			options.getTopologyRefreshOptions().ifPresent(clusterClientOptions::topologyRefreshOptions);
			RedisModulesClusterClient client = RedisModulesClusterClient.create(clientResources, redisURI);
			client.setOptions(clusterClientOptions.build());
			return client;
		}
		RedisModulesClient client = RedisModulesClient.create(clientResources, redisURI);
		client.setOptions(clientOptions);
		return client;
	}

	public ClientResources clientResources() {
		DefaultClientResources.Builder builder = DefaultClientResources.builder();
		options.getCommandLatencyRecorder().ifPresent(builder::commandLatencyRecorder);
		options.getCommandLatencyPublisherOptions().ifPresent(builder::commandLatencyPublisherOptions);
		return builder.build();
	}

	public ClientOptions clientOptions() {
		ClientOptions.Builder builder = ClientOptions.builder().autoReconnect(options.isAutoReconnect())
				.decodeBufferPolicy(options.getDecodeBufferPolicy())
				.disconnectedBehavior(options.getDisconnectedBehavior())
				.publishOnScheduler(options.isPublishOnScheduler()).sslOptions(sslOptions())
				.suspendReconnectOnProtocolFailure(options.isSuspendReconnectOnProtocolFailure())
				.requestQueueSize(options.getRequestQueueSize()).timeoutOptions(options.getTimeoutOptions());
		options.getProtocolVersion().ifPresent(builder::protocolVersion);
		options.getSocketOptions().ifPresent(builder::socketOptions);
		return builder.build();
	}

	public RedisURI uri() {
		RedisURI redisURI = options.getUri().orElse(RedisURI.create(options.getHost(), options.getPort()));
		redisURI.setVerifyPeer(options.getSslVerifyMode());
		if (options.isSsl()) {
			redisURI.setSsl(true);
		}
		if (options.isStartTls()) {
			redisURI.setStartTls(true);
		}
		options.getSocket().ifPresent(redisURI::setSocket);
		redisURI.setCredentialsProvider(
				new StaticCredentialsProvider(RedisCredentials.just(options.getUsername(), options.getPassword())));
		options.getCredentialsProvider().ifPresent(redisURI::setCredentialsProvider);
		redisURI.setDatabase(options.getDatabase());
		options.getTimeout().ifPresent(redisURI::setTimeout);
		options.getClientName().ifPresent(redisURI::setClientName);
		return redisURI;
	}

	public SslOptions sslOptions() {
		SslOptions.Builder sslOptions = SslOptions.builder();
		options.getKeystore().ifPresent(s -> options.getKeystorePassword()
				.ifPresentOrElse(p -> sslOptions.keystore(s, p.toCharArray()), () -> sslOptions.keystore(s)));
		options.getTruststore().ifPresent(s -> options.getTruststorePassword()
				.ifPresentOrElse(p -> sslOptions.truststore(s, p), () -> sslOptions.truststore(s)));
		options.getCert().ifPresent(sslOptions::trustManager);
		return sslOptions.build();
	}

	public static RedisClientBuilder create() {
		return new RedisClientBuilder();
	}

	public static RedisClientBuilder create(RedisClientOptions options) {
		return new RedisClientBuilder(options);
	}

}
