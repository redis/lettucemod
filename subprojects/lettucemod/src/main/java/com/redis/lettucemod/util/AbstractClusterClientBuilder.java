package com.redis.lettucemod.util;

import java.util.Optional;
import java.util.function.Predicate;

import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.resource.ClientResources;

public abstract class AbstractClusterClientBuilder<B extends AbstractClusterClientBuilder<B>>
		extends AbstractClientBuilder<B> {

	private int maxRedirects = DEFAULT_MAX_REDIRECTS;
	private boolean validateClusterNodeMembership = DEFAULT_VALIDATE_CLUSTER_MEMBERSHIP;
	private Predicate<RedisClusterNode> nodeFilter = DEFAULT_NODE_FILTER;
	private Optional<ClusterTopologyRefreshOptions> topologyRefreshOptions = Optional.empty();

	protected AbstractClusterClientBuilder(RedisURI redisURI) {
		super(redisURI);
	}

	@SuppressWarnings("unchecked")
	public B maxRedirects(int maxRedirects) {
		this.maxRedirects = maxRedirects;
		return (B) this;
	}

	@SuppressWarnings("unchecked")
	public B validateClusterNodeMembership(boolean validateClusterNodeMembership) {
		this.validateClusterNodeMembership = validateClusterNodeMembership;
		return (B) this;
	}

	@SuppressWarnings("unchecked")
	public B nodeFilter(Predicate<RedisClusterNode> nodeFilter) {
		this.nodeFilter = nodeFilter;
		return (B) this;
	}

	public B topologyRefreshOptions(ClusterTopologyRefreshOptions topologyRefreshOptions) {
		return topologyRefreshOptions(Optional.of(topologyRefreshOptions));
	}

	@SuppressWarnings("unchecked")
	public B topologyRefreshOptions(Optional<ClusterTopologyRefreshOptions> topologyRefreshOptions) {
		this.topologyRefreshOptions = topologyRefreshOptions;
		return (B) this;
	}

	protected RedisModulesClusterClient clusterClient() {
		ClientResources clientResources = clientResources();
		ClusterClientOptions.Builder clientOptions = configure(ClusterClientOptions.builder());
		clientOptions.maxRedirects(maxRedirects);
		clientOptions.validateClusterNodeMembership(validateClusterNodeMembership);
		clientOptions.nodeFilter(nodeFilter);
		topologyRefreshOptions.ifPresent(clientOptions::topologyRefreshOptions);
		RedisModulesClusterClient client = RedisModulesClusterClient.create(clientResources, redisURI);
		client.setOptions(clientOptions.build());
		return client;
	}

}
