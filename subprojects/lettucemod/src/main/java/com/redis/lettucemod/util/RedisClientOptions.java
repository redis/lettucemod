package com.redis.lettucemod.util;

import java.io.File;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Predicate;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.ClientOptions.DisconnectedBehavior;
import io.lettuce.core.RedisCredentialsProvider;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.event.EventPublisherOptions;
import io.lettuce.core.metrics.CommandLatencyRecorder;
import io.lettuce.core.protocol.DecodeBufferPolicies;
import io.lettuce.core.protocol.DecodeBufferPolicy;
import io.lettuce.core.protocol.ProtocolVersion;

public class RedisClientOptions {

	public static final boolean DEFAULT_CLUSTER = false;
	public static final String DEFAULT_HOST = "localhost";
	public static final int DEFAULT_PORT = RedisURI.DEFAULT_REDIS_PORT;
	public static final SslVerifyMode DEFAULT_SSL_VERIFY_MODE = SslVerifyMode.FULL;
	public static final boolean DEFAULT_SSL = false;
	public static final boolean DEFAULT_START_TLS = false;
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

	private String host = DEFAULT_HOST;
	private int port = DEFAULT_PORT;
	private Optional<RedisURI> uri = Optional.empty();
	private boolean cluster = DEFAULT_CLUSTER;
	private boolean ssl = DEFAULT_SSL;
	private boolean startTls = DEFAULT_START_TLS;
	private SslVerifyMode sslVerifyMode = DEFAULT_SSL_VERIFY_MODE;
	private Optional<String> socket = Optional.empty();
	private Optional<String> username = Optional.empty();
	private Optional<String> password = Optional.empty();
	private Optional<RedisCredentialsProvider> credentialsProvider = Optional.empty();
	private int database;
	private Optional<Duration> timeout = Optional.empty();
	private Optional<String> clientName = Optional.empty();
	private DisconnectedBehavior disconnectedBehavior = DEFAULT_DISCONNECTED_BEHAVIOR;
	private boolean publishOnScheduler = DEFAULT_PUBLISH_ON_SCHEDULER;
	private boolean autoReconnect = DEFAULT_AUTO_RECONNECT;
	private boolean suspendReconnectOnProtocolFailure = DEFAULT_SUSPEND_RECONNECT_ON_PROTOCOL_FAILURE;
	private Optional<SocketOptions> socketOptions = Optional.empty();
	private DecodeBufferPolicy decodeBufferPolicy = DEFAULT_DECODE_BUFFER_POLICY;
	private Optional<ProtocolVersion> protocolVersion = Optional.empty();
	private int requestQueueSize = DEFAULT_REQUEST_QUEUE_SIZE;
	private TimeoutOptions timeoutOptions = DEFAULT_TIMEOUT_OPTIONS;
	private int maxRedirects = DEFAULT_MAX_REDIRECTS;
	private boolean validateClusterNodeMembership = DEFAULT_VALIDATE_CLUSTER_MEMBERSHIP;
	private Predicate<RedisClusterNode> nodeFilter = DEFAULT_NODE_FILTER;
	private Optional<ClusterTopologyRefreshOptions> topologyRefreshOptions = Optional.empty();
	private Optional<CommandLatencyRecorder> commandLatencyRecorder = Optional.empty();
	private Optional<EventPublisherOptions> commandLatencyPublisherOptions = Optional.empty();
	private Optional<File> keystore = Optional.empty();
	private Optional<String> keystorePassword = Optional.empty();
	private Optional<File> truststore = Optional.empty();
	private Optional<String> truststorePassword = Optional.empty();
	private Optional<File> cert = Optional.empty();

	public RedisClientOptions() {
	}

	private RedisClientOptions(Builder builder) {
		this.host = builder.host;
		this.port = builder.port;
		this.uri = builder.uri;
		this.cluster = builder.cluster;
		this.ssl = builder.ssl;
		this.startTls = builder.startTls;
		this.sslVerifyMode = builder.sslVerifyMode;
		this.socket = builder.socket;
		this.username = builder.username;
		this.password = builder.password;
		this.credentialsProvider = builder.credentialsProvider;
		this.database = builder.database;
		this.timeout = builder.timeout;
		this.clientName = builder.clientName;
		this.disconnectedBehavior = builder.disconnectedBehavior;
		this.publishOnScheduler = builder.publishOnScheduler;
		this.autoReconnect = builder.autoReconnect;
		this.suspendReconnectOnProtocolFailure = builder.suspendReconnectOnProtocolFailure;
		this.socketOptions = builder.socketOptions;
		this.decodeBufferPolicy = builder.decodeBufferPolicy;
		this.protocolVersion = builder.protocolVersion;
		this.requestQueueSize = builder.requestQueueSize;
		this.timeoutOptions = builder.timeoutOptions;
		this.maxRedirects = builder.maxRedirects;
		this.validateClusterNodeMembership = builder.validateClusterNodeMembership;
		this.nodeFilter = builder.nodeFilter;
		this.topologyRefreshOptions = builder.topologyRefreshOptions;
		this.commandLatencyRecorder = builder.commandLatencyRecorder;
		this.commandLatencyPublisherOptions = builder.commandLatencyPublisherOptions;
		this.keystore = builder.keystore;
		this.keystorePassword = builder.keystorePassword;
		this.truststore = builder.truststore;
		this.truststorePassword = builder.truststorePassword;
		this.cert = builder.cert;
	}

	public Optional<File> getKeystore() {
		return keystore;
	}

	public void setKeystore(File keystore) {
		setKeystore(Optional.of(keystore));
	}

	public void setKeystore(Optional<File> keystore) {
		this.keystore = keystore;
	}

	public Optional<String> getKeystorePassword() {
		return keystorePassword;
	}

	public void setKeystorePassword(String keystorePassword) {
		setKeystorePassword(Optional.of(keystorePassword));
	}

	public void setKeystorePassword(Optional<String> keystorePassword) {
		this.keystorePassword = keystorePassword;
	}

	public Optional<File> getTruststore() {
		return truststore;
	}

	public void setTruststore(File truststore) {
		setTruststore(Optional.of(truststore));
	}

	public void setTruststore(Optional<File> truststore) {
		this.truststore = truststore;
	}

	public Optional<String> getTruststorePassword() {
		return truststorePassword;
	}

	public void setTruststorePassword(String truststorePassword) {
		setTruststorePassword(Optional.of(truststorePassword));
	}

	public void setTruststorePassword(Optional<String> truststorePassword) {
		this.truststorePassword = truststorePassword;
	}

	public Optional<File> getCert() {
		return cert;
	}

	public void setCert(File cert) {
		setCert(Optional.of(cert));
	}

	public void setCert(Optional<File> cert) {
		this.cert = cert;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public Optional<RedisURI> getUri() {
		return uri;
	}

	public boolean isCluster() {
		return cluster;
	}

	public boolean isSsl() {
		return ssl;
	}

	public boolean isStartTls() {
		return startTls;
	}

	public SslVerifyMode getSslVerifyMode() {
		return sslVerifyMode;
	}

	public Optional<String> getSocket() {
		return socket;
	}

	public Optional<String> getUsername() {
		return username;
	}

	public Optional<String> getPassword() {
		return password;
	}

	public Optional<RedisCredentialsProvider> getCredentialsProvider() {
		return credentialsProvider;
	}

	public int getDatabase() {
		return database;
	}

	public Optional<Duration> getTimeout() {
		return timeout;
	}

	public Optional<String> getClientName() {
		return clientName;
	}

	public DisconnectedBehavior getDisconnectedBehavior() {
		return disconnectedBehavior;
	}

	public boolean isPublishOnScheduler() {
		return publishOnScheduler;
	}

	public boolean isAutoReconnect() {
		return autoReconnect;
	}

	public boolean isSuspendReconnectOnProtocolFailure() {
		return suspendReconnectOnProtocolFailure;
	}

	public Optional<SocketOptions> getSocketOptions() {
		return socketOptions;
	}

	public DecodeBufferPolicy getDecodeBufferPolicy() {
		return decodeBufferPolicy;
	}

	public Optional<ProtocolVersion> getProtocolVersion() {
		return protocolVersion;
	}

	public int getRequestQueueSize() {
		return requestQueueSize;
	}

	public TimeoutOptions getTimeoutOptions() {
		return timeoutOptions;
	}

	public int getMaxRedirects() {
		return maxRedirects;
	}

	public boolean isValidateClusterNodeMembership() {
		return validateClusterNodeMembership;
	}

	public Predicate<RedisClusterNode> getNodeFilter() {
		return nodeFilter;
	}

	public Optional<ClusterTopologyRefreshOptions> getTopologyRefreshOptions() {
		return topologyRefreshOptions;
	}

	public Optional<CommandLatencyRecorder> getCommandLatencyRecorder() {
		return commandLatencyRecorder;
	}

	public Optional<EventPublisherOptions> getCommandLatencyPublisherOptions() {
		return commandLatencyPublisherOptions;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setUri(RedisURI uri) {
		setUri(Optional.of(uri));
	}

	public void setUri(Optional<RedisURI> uri) {
		this.uri = uri;
	}

	public void setUriString(String uri) {
		setUri(RedisURI.create(uri));
	}

	public void setUriString(Optional<String> uri) {
		setUri(uri.map(RedisURI::create));
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	public void setSsl(boolean ssl) {
		this.ssl = ssl;
	}

	public void setStartTls(boolean startTls) {
		this.startTls = startTls;
	}

	public void setSslVerifyMode(SslVerifyMode sslVerifyMode) {
		this.sslVerifyMode = sslVerifyMode;
	}

	public void setSocket(String socket) {
		setSocket(Optional.of(socket));
	}

	public void setSocket(Optional<String> socket) {
		this.socket = socket;
	}

	public void setUsername(String username) {
		setUsername(Optional.of(username));
	}

	public void setUsername(Optional<String> username) {
		this.username = username;
	}

	public void setPassword(String password) {
		setPassword(Optional.of(password));
	}

	public void setPassword(Optional<String> password) {
		this.password = password;
	}

	public void setCredentialsProvider(RedisCredentialsProvider credentialsProvider) {
		setCredentialsProvider(Optional.of(credentialsProvider));
	}

	public void setCredentialsProvider(Optional<RedisCredentialsProvider> credentialsProvider) {
		this.credentialsProvider = credentialsProvider;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public void setTimeout(Duration timeout) {
		setTimeout(Optional.of(timeout));
	}

	public void setTimeout(Optional<Duration> timeout) {
		this.timeout = timeout;
	}

	public void setClientName(String clientName) {
		setClientName(Optional.of(clientName));
	}

	public void setClientName(Optional<String> clientName) {
		this.clientName = clientName;
	}

	public void setDisconnectedBehavior(DisconnectedBehavior disconnectedBehavior) {
		this.disconnectedBehavior = disconnectedBehavior;
	}

	public void setPublishOnScheduler(boolean publishOnScheduler) {
		this.publishOnScheduler = publishOnScheduler;
	}

	public void setAutoReconnect(boolean autoReconnect) {
		this.autoReconnect = autoReconnect;
	}

	public void setSuspendReconnectOnProtocolFailure(boolean suspendReconnectOnProtocolFailure) {
		this.suspendReconnectOnProtocolFailure = suspendReconnectOnProtocolFailure;
	}

	public void setSocketOptions(SocketOptions socketOptions) {
		setSocketOptions(Optional.of(socketOptions));
	}

	public void setSocketOptions(Optional<SocketOptions> socketOptions) {
		this.socketOptions = socketOptions;
	}

	public void setDecodeBufferPolicy(DecodeBufferPolicy decodeBufferPolicy) {
		this.decodeBufferPolicy = decodeBufferPolicy;
	}

	public void setProtocolVersion(ProtocolVersion protocolVersion) {
		setProtocolVersion(Optional.of(protocolVersion));
	}

	public void setProtocolVersion(Optional<ProtocolVersion> protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

	public void setRequestQueueSize(int requestQueueSize) {
		this.requestQueueSize = requestQueueSize;
	}

	public void setTimeoutOptions(TimeoutOptions timeoutOptions) {
		this.timeoutOptions = timeoutOptions;
	}

	public void setMaxRedirects(int maxRedirects) {
		this.maxRedirects = maxRedirects;
	}

	public void setValidateClusterNodeMembership(boolean validateClusterNodeMembership) {
		this.validateClusterNodeMembership = validateClusterNodeMembership;
	}

	public void setNodeFilter(Predicate<RedisClusterNode> nodeFilter) {
		this.nodeFilter = nodeFilter;
	}

	public void setTopologyRefreshOptions(ClusterTopologyRefreshOptions topologyRefreshOptions) {
		setTopologyRefreshOptions(Optional.of(topologyRefreshOptions));
	}

	public void setTopologyRefreshOptions(Optional<ClusterTopologyRefreshOptions> topologyRefreshOptions) {
		this.topologyRefreshOptions = topologyRefreshOptions;
	}

	public void setCommandLatencyRecorder(CommandLatencyRecorder commandLatencyRecorder) {
		setCommandLatencyRecorder(Optional.of(commandLatencyRecorder));
	}

	public void setCommandLatencyRecorder(Optional<CommandLatencyRecorder> commandLatencyRecorder) {
		this.commandLatencyRecorder = commandLatencyRecorder;
	}

	public void setCommandLatencyPublisherOptions(EventPublisherOptions commandLatencyPublisherOptions) {
		setCommandLatencyPublisherOptions(Optional.of(commandLatencyPublisherOptions));
	}

	public void setCommandLatencyPublisherOptions(Optional<EventPublisherOptions> commandLatencyPublisherOptions) {
		this.commandLatencyPublisherOptions = commandLatencyPublisherOptions;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {
		private String host = DEFAULT_HOST;
		private int port = RedisURI.DEFAULT_REDIS_PORT;
		private Optional<RedisURI> uri = Optional.empty();
		private boolean cluster = DEFAULT_CLUSTER;
		private boolean ssl = DEFAULT_SSL;
		private boolean startTls = DEFAULT_START_TLS;
		private SslVerifyMode sslVerifyMode = DEFAULT_SSL_VERIFY_MODE;
		private Optional<String> socket = Optional.empty();
		private Optional<String> username = Optional.empty();
		private Optional<String> password = Optional.empty();
		private Optional<RedisCredentialsProvider> credentialsProvider = Optional.empty();
		private int database;
		private Optional<Duration> timeout = Optional.empty();
		private Optional<String> clientName = Optional.empty();
		private DisconnectedBehavior disconnectedBehavior = DEFAULT_DISCONNECTED_BEHAVIOR;
		private boolean publishOnScheduler = DEFAULT_PUBLISH_ON_SCHEDULER;
		private boolean autoReconnect = DEFAULT_AUTO_RECONNECT;
		private boolean suspendReconnectOnProtocolFailure = DEFAULT_SUSPEND_RECONNECT_ON_PROTOCOL_FAILURE;
		private Optional<SocketOptions> socketOptions = Optional.empty();
		private DecodeBufferPolicy decodeBufferPolicy = DEFAULT_DECODE_BUFFER_POLICY;
		private Optional<ProtocolVersion> protocolVersion = Optional.empty();
		private int requestQueueSize = DEFAULT_REQUEST_QUEUE_SIZE;
		private TimeoutOptions timeoutOptions = DEFAULT_TIMEOUT_OPTIONS;
		private int maxRedirects = DEFAULT_MAX_REDIRECTS;
		private boolean validateClusterNodeMembership = DEFAULT_VALIDATE_CLUSTER_MEMBERSHIP;
		private Predicate<RedisClusterNode> nodeFilter = DEFAULT_NODE_FILTER;
		private Optional<ClusterTopologyRefreshOptions> topologyRefreshOptions = Optional.empty();
		private Optional<CommandLatencyRecorder> commandLatencyRecorder = Optional.empty();
		private Optional<EventPublisherOptions> commandLatencyPublisherOptions = Optional.empty();
		private Optional<File> keystore = Optional.empty();
		private Optional<String> keystorePassword = Optional.empty();
		private Optional<File> truststore = Optional.empty();
		private Optional<String> truststorePassword = Optional.empty();
		private Optional<File> cert = Optional.empty();

		private Builder() {
		}

		public Builder host(String host) {
			this.host = host;
			return this;
		}

		public Builder port(int port) {
			this.port = port;
			return this;
		}

		public Builder uri(RedisURI uri) {
			return uri(Optional.of(uri));
		}

		public Builder uri(Optional<RedisURI> uri) {
			this.uri = uri;
			return this;
		}

		public Builder uriString(String uri) {
			return uri(RedisURI.create(uri));
		}

		public Builder uriString(Optional<String> uri) {
			return uri(uri.map(RedisURI::create));
		}

		public Builder cluster(boolean cluster) {
			this.cluster = cluster;
			return this;
		}

		public Builder ssl(boolean ssl) {
			this.ssl = ssl;
			return this;
		}

		public Builder startTls(boolean startTls) {
			this.startTls = startTls;
			return this;
		}

		public Builder sslVerifyMode(SslVerifyMode sslVerifyMode) {
			this.sslVerifyMode = sslVerifyMode;
			return this;
		}

		public Builder socket(String socket) {
			return socket(Optional.of(socket));
		}

		public Builder socket(Optional<String> socket) {
			this.socket = socket;
			return this;
		}

		public Builder username(Optional<String> username) {
			this.username = username;
			return this;
		}

		public Builder username(String username) {
			return username(Optional.of(username));
		}

		public Builder password(String password) {
			return password(Optional.of(password));
		}

		public Builder password(Optional<String> password) {
			this.password = password;
			return this;
		}

		public Builder credentialsProvider(RedisCredentialsProvider credentialsProvider) {
			return credentialsProvider(Optional.of(credentialsProvider));
		}

		public Builder credentialsProvider(Optional<RedisCredentialsProvider> credentialsProvider) {
			this.credentialsProvider = credentialsProvider;
			return this;
		}

		public Builder database(int database) {
			this.database = database;
			return this;
		}

		public Builder timeoutInSeconds(long timeout) {
			return timeout(Duration.ofSeconds(timeout));
		}

		public Builder timeout(Duration timeout) {
			return timeout(Optional.of(timeout));
		}

		public Builder timeout(Optional<Duration> timeout) {
			this.timeout = timeout;
			return this;
		}

		public Builder clientName(String clientName) {
			return clientName(Optional.of(clientName));
		}

		public Builder clientName(Optional<String> clientName) {
			this.clientName = clientName;
			return this;
		}

		public Builder disconnectedBehavior(DisconnectedBehavior disconnectedBehavior) {
			this.disconnectedBehavior = disconnectedBehavior;
			return this;
		}

		public Builder publishOnScheduler(boolean publishOnScheduler) {
			this.publishOnScheduler = publishOnScheduler;
			return this;
		}

		public Builder autoReconnect(boolean autoReconnect) {
			this.autoReconnect = autoReconnect;
			return this;
		}

		public Builder suspendReconnectOnProtocolFailure(boolean suspendReconnectOnProtocolFailure) {
			this.suspendReconnectOnProtocolFailure = suspendReconnectOnProtocolFailure;
			return this;
		}

		public Builder socketOptions(SocketOptions socketOptions) {
			return socketOptions(Optional.of(socketOptions));
		}

		public Builder socketOptions(Optional<SocketOptions> socketOptions) {
			this.socketOptions = socketOptions;
			return this;
		}

		public Builder decodeBufferPolicy(DecodeBufferPolicy decodeBufferPolicy) {
			this.decodeBufferPolicy = decodeBufferPolicy;
			return this;
		}

		public Builder protocolVersion(ProtocolVersion protocolVersion) {
			return protocolVersion(Optional.of(protocolVersion));
		}

		public Builder protocolVersion(Optional<ProtocolVersion> protocolVersion) {
			this.protocolVersion = protocolVersion;
			return this;
		}

		public Builder requestQueueSize(int requestQueueSize) {
			this.requestQueueSize = requestQueueSize;
			return this;
		}

		public Builder timeoutOptions(TimeoutOptions timeoutOptions) {
			this.timeoutOptions = timeoutOptions;
			return this;
		}

		public Builder maxRedirects(int maxRedirects) {
			this.maxRedirects = maxRedirects;
			return this;
		}

		public Builder validateClusterNodeMembership(boolean validateClusterNodeMembership) {
			this.validateClusterNodeMembership = validateClusterNodeMembership;
			return this;
		}

		public Builder nodeFilter(Predicate<RedisClusterNode> nodeFilter) {
			this.nodeFilter = nodeFilter;
			return this;
		}

		public Builder topologyRefreshOptions(ClusterTopologyRefreshOptions topologyRefreshOptions) {
			return topologyRefreshOptions(Optional.of(topologyRefreshOptions));
		}

		public Builder topologyRefreshOptions(Optional<ClusterTopologyRefreshOptions> topologyRefreshOptions) {
			this.topologyRefreshOptions = topologyRefreshOptions;
			return this;
		}

		public Builder commandLatencyRecorder(CommandLatencyRecorder commandLatencyRecorder) {
			return commandLatencyRecorder(Optional.of(commandLatencyRecorder));
		}

		public Builder commandLatencyRecorder(Optional<CommandLatencyRecorder> commandLatencyRecorder) {
			this.commandLatencyRecorder = commandLatencyRecorder;
			return this;
		}

		public Builder commandLatencyPublisherOptions(EventPublisherOptions commandLatencyPublisherOptions) {
			return commandLatencyPublisherOptions(Optional.of(commandLatencyPublisherOptions));
		}

		public Builder commandLatencyPublisherOptions(Optional<EventPublisherOptions> commandLatencyPublisherOptions) {
			this.commandLatencyPublisherOptions = commandLatencyPublisherOptions;
			return this;
		}

		public RedisClientOptions build() {
			return new RedisClientOptions(this);
		}

		public Builder keystore(File keystore) {
			return keystore(Optional.of(keystore));
		}

		public Builder keystore(Optional<File> keystore) {
			this.keystore = keystore;
			return this;
		}

		public Builder keystorePassword(String password) {
			return keystorePassword(Optional.of(password));
		}

		public Builder keystorePassword(Optional<String> password) {
			this.keystorePassword = password;
			return this;
		}

		public Builder truststore(File truststore) {
			return truststore(Optional.of(truststore));
		}

		public Builder truststore(Optional<File> truststore) {
			this.truststore = truststore;
			return this;
		}

		public Builder truststorePassword(String password) {
			return truststorePassword(Optional.of(password));
		}

		public Builder truststorePassword(Optional<String> password) {
			this.truststorePassword = password;
			return this;
		}

		public Builder cert(File cert) {
			return cert(Optional.of(cert));
		}

		public Builder cert(Optional<File> cert) {
			this.cert = cert;
			return this;
		}
	}

}