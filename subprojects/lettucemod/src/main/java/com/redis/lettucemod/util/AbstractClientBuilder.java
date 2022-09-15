package com.redis.lettucemod.util;

import java.io.File;
import java.util.Optional;
import java.util.function.Predicate;

import com.redis.lettucemod.RedisModulesClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.ClientOptions.DisconnectedBehavior;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.SslOptions;
import io.lettuce.core.SslOptions.Resource;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.event.EventPublisherOptions;
import io.lettuce.core.metrics.CommandLatencyRecorder;
import io.lettuce.core.protocol.DecodeBufferPolicies;
import io.lettuce.core.protocol.DecodeBufferPolicy;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

@SuppressWarnings("unchecked")
public abstract class AbstractClientBuilder<B extends AbstractClientBuilder<B>> {

	public static final boolean DEFAULT_AUTO_RECONNECT = ClientOptions.DEFAULT_AUTO_RECONNECT;
	public static final boolean DEFAULT_PUBLISH_ON_SCHEDULER = ClientOptions.DEFAULT_PUBLISH_ON_SCHEDULER;
	public static final DisconnectedBehavior DEFAULT_DISCONNECTED_BEHAVIOR = ClientOptions.DEFAULT_DISCONNECTED_BEHAVIOR;
	public static final boolean DEFAULT_SUSPEND_RECONNECT_ON_PROTOCOL_FAILURE = ClientOptions.DEFAULT_SUSPEND_RECONNECT_PROTO_FAIL;
	public static final DecodeBufferPolicy DEFAULT_DECODE_BUFFER_POLICY = DecodeBufferPolicies
			.ratio(ClientOptions.DEFAULT_BUFFER_USAGE_RATIO);
	public static final int DEFAULT_REQUEST_QUEUE_SIZE = ClientOptions.DEFAULT_REQUEST_QUEUE_SIZE;
	public static final TimeoutOptions DEFAULT_TIMEOUT_OPTIONS = TimeoutOptions.create();
	public static final boolean DEFAULT_SHOW_METRICS = false;
	public static final int DEFAULT_MAX_REDIRECTS = ClusterClientOptions.DEFAULT_MAX_REDIRECTS;
	public static final boolean DEFAULT_VALIDATE_CLUSTER_MEMBERSHIP = ClusterClientOptions.DEFAULT_VALIDATE_CLUSTER_MEMBERSHIP;
	public static final Predicate<RedisClusterNode> DEFAULT_NODE_FILTER = ClusterClientOptions.DEFAULT_NODE_FILTER;

	protected final RedisURI redisURI;
	private DisconnectedBehavior disconnectedBehavior = DEFAULT_DISCONNECTED_BEHAVIOR;
	private boolean publishOnScheduler = DEFAULT_PUBLISH_ON_SCHEDULER;
	private boolean autoReconnect = DEFAULT_AUTO_RECONNECT;
	private boolean suspendReconnectOnProtocolFailure = DEFAULT_SUSPEND_RECONNECT_ON_PROTOCOL_FAILURE;
	private Optional<SocketOptions> socketOptions = Optional.empty();
	private DecodeBufferPolicy decodeBufferPolicy = DEFAULT_DECODE_BUFFER_POLICY;
	private Optional<ProtocolVersion> protocolVersion = Optional.empty();
	private int requestQueueSize = DEFAULT_REQUEST_QUEUE_SIZE;
	private TimeoutOptions timeoutOptions = DEFAULT_TIMEOUT_OPTIONS;
	private Optional<CommandLatencyRecorder> commandLatencyRecorder = Optional.empty();
	private Optional<EventPublisherOptions> commandLatencyPublisherOptions = Optional.empty();
	private Optional<File> keystore = Optional.empty();
	private char[] keystorePassword;
	private Optional<File> truststore = Optional.empty();
	private char[] truststorePassword;
	private Optional<File> trustManager = Optional.empty();
	private Optional<File> key = Optional.empty();
	private File keyCert;
	private char[] keyPassword;

	protected AbstractClientBuilder(RedisURI redisURI) {
		this.redisURI = redisURI;
	}

	public B disconnectedBehavior(DisconnectedBehavior disconnectedBehavior) {
		this.disconnectedBehavior = disconnectedBehavior;
		return (B) this;
	}

	public B publishOnScheduler(boolean publishOnScheduler) {
		this.publishOnScheduler = publishOnScheduler;
		return (B) this;
	}

	public B autoReconnect(boolean autoReconnect) {
		this.autoReconnect = autoReconnect;
		return (B) this;
	}

	public B suspendReconnectOnProtocolFailure(boolean suspendReconnectOnProtocolFailure) {
		this.suspendReconnectOnProtocolFailure = suspendReconnectOnProtocolFailure;
		return (B) this;
	}

	public B socketOptions(SocketOptions socketOptions) {
		return socketOptions(Optional.of(socketOptions));
	}

	public B socketOptions(Optional<SocketOptions> socketOptions) {
		this.socketOptions = socketOptions;
		return (B) this;
	}

	public B decodeBufferPolicy(DecodeBufferPolicy decodeBufferPolicy) {
		this.decodeBufferPolicy = decodeBufferPolicy;
		return (B) this;
	}

	public B protocolVersion(ProtocolVersion protocolVersion) {
		return protocolVersion(Optional.of(protocolVersion));
	}

	public B protocolVersion(Optional<ProtocolVersion> protocolVersion) {
		this.protocolVersion = protocolVersion;
		return (B) this;
	}

	public B requestQueueSize(int requestQueueSize) {
		this.requestQueueSize = requestQueueSize;
		return (B) this;
	}

	public B timeoutOptions(TimeoutOptions timeoutOptions) {
		this.timeoutOptions = timeoutOptions;
		return (B) this;
	}

	public B commandLatencyRecorder(CommandLatencyRecorder commandLatencyRecorder) {
		return commandLatencyRecorder(Optional.of(commandLatencyRecorder));
	}

	public B commandLatencyRecorder(Optional<CommandLatencyRecorder> commandLatencyRecorder) {
		this.commandLatencyRecorder = commandLatencyRecorder;
		return (B) this;
	}

	public B commandLatencyPublisherOptions(EventPublisherOptions commandLatencyPublisherOptions) {
		return commandLatencyPublisherOptions(Optional.of(commandLatencyPublisherOptions));
	}

	public B commandLatencyPublisherOptions(Optional<EventPublisherOptions> commandLatencyPublisherOptions) {
		this.commandLatencyPublisherOptions = commandLatencyPublisherOptions;
		return (B) this;
	}

	public B key(File key) {
		return key(Optional.of(key));
	}

	public B key(Optional<File> key) {
		this.key = key;
		return (B) this;
	}

	public B keyCert(File cert) {
		this.keyCert = cert;
		return (B) this;
	}

	public B keyPassword(char[] password) {
		this.keyPassword = password;
		return (B) this;
	}

	public B keystore(File keystore) {
		return keystore(Optional.of(keystore));
	}

	public B keystore(Optional<File> keystore) {
		this.keystore = keystore;
		return (B) this;
	}

	public B keystorePassword(char[] password) {
		this.keystorePassword = password;
		return (B) this;
	}

	public B truststore(File truststore) {
		return truststore(Optional.of(truststore));
	}

	public B truststore(Optional<File> truststore) {
		this.truststore = truststore;
		return (B) this;
	}

	public B truststorePassword(char[] password) {
		this.truststorePassword = password;
		return (B) this;
	}

	public B trustManager(File trustManager) {
		return trustManager(Optional.of(trustManager));
	}

	public B trustManager(Optional<File> trustManager) {
		this.trustManager = trustManager;
		return (B) this;
	}

	public ClientResources clientResources() {
		DefaultClientResources.Builder builder = DefaultClientResources.builder();
		commandLatencyRecorder.ifPresent(builder::commandLatencyRecorder);
		commandLatencyPublisherOptions.ifPresent(builder::commandLatencyPublisherOptions);
		return builder.build();
	}

	protected <C extends ClientOptions.Builder> C configure(C builder) {
		builder.autoReconnect(autoReconnect).decodeBufferPolicy(decodeBufferPolicy)
				.disconnectedBehavior(disconnectedBehavior).publishOnScheduler(publishOnScheduler)
				.sslOptions(sslOptions()).suspendReconnectOnProtocolFailure(suspendReconnectOnProtocolFailure)
				.requestQueueSize(requestQueueSize).timeoutOptions(timeoutOptions);
		protocolVersion.ifPresent(builder::protocolVersion);
		socketOptions.ifPresent(builder::socketOptions);
		return builder;
	}

	public SslOptions sslOptions() {
		SslOptions.Builder ssl = SslOptions.builder();
		key.ifPresent(k -> ssl.keyManager(keyCert, k, keyPassword));
		keystore.ifPresent(s -> ssl.keystore(s, keystorePassword));
		truststore.ifPresent(s -> ssl.truststore(Resource.from(s), truststorePassword));
		trustManager.ifPresent(ssl::trustManager);
		return ssl.build();
	}

	protected RedisModulesClient client() {
		RedisModulesClient client = RedisModulesClient.create(clientResources(), redisURI);
		client.setOptions(configure(ClientOptions.builder()).build());
		return client;
	}

	public abstract AbstractRedisClient build();

}