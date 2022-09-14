package com.redis.lettucemod.util;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Predicate;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.ClientOptions.DisconnectedBehavior;
import io.lettuce.core.RedisCredentialsProvider;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslVerifyMode;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.protocol.DecodeBufferPolicies;
import io.lettuce.core.protocol.DecodeBufferPolicy;

public class RedisURIBuilder {

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
	private int port = RedisURI.DEFAULT_REDIS_PORT;
	private Optional<RedisURI> uri = Optional.empty();
	private boolean ssl = DEFAULT_SSL;
	private boolean startTls = DEFAULT_START_TLS;
	private SslVerifyMode sslVerifyMode = DEFAULT_SSL_VERIFY_MODE;
	private Optional<String> socket = Optional.empty();
	private String username;
	private char[] password;
	private Optional<RedisCredentialsProvider> credentialsProvider = Optional.empty();
	private int database;
	private Optional<Duration> timeout = Optional.empty();
	private Optional<String> clientName = Optional.empty();

	public RedisURIBuilder host(String host) {
		this.host = host;
		return this;
	}

	public RedisURIBuilder port(int port) {
		this.port = port;
		return this;
	}

	public RedisURIBuilder uri(RedisURI uri) {
		return uri(Optional.of(uri));
	}

	public RedisURIBuilder uri(Optional<RedisURI> uri) {
		this.uri = uri;
		return this;
	}

	public RedisURIBuilder uriString(String uri) {
		return uri(RedisURI.create(uri));
	}

	public RedisURIBuilder uriString(Optional<String> uri) {
		return uri(uri.map(RedisURI::create));
	}

	public RedisURIBuilder ssl(boolean ssl) {
		this.ssl = ssl;
		return this;
	}

	public RedisURIBuilder startTls(boolean startTls) {
		this.startTls = startTls;
		return this;
	}

	public RedisURIBuilder sslVerifyMode(SslVerifyMode sslVerifyMode) {
		this.sslVerifyMode = sslVerifyMode;
		return this;
	}

	public RedisURIBuilder socket(String socket) {
		return socket(Optional.of(socket));
	}

	public RedisURIBuilder socket(Optional<String> socket) {
		this.socket = socket;
		return this;
	}

	public RedisURIBuilder username(String username) {
		this.username = username;
		return this;
	}

	public RedisURIBuilder password(char[] password) {
		this.password = password;
		return this;
	}

	public RedisURIBuilder credentialsProvider(RedisCredentialsProvider credentialsProvider) {
		return credentialsProvider(Optional.of(credentialsProvider));
	}

	public RedisURIBuilder credentialsProvider(Optional<RedisCredentialsProvider> credentialsProvider) {
		this.credentialsProvider = credentialsProvider;
		return this;
	}

	public RedisURIBuilder database(int database) {
		this.database = database;
		return this;
	}

	public RedisURIBuilder timeoutInSeconds(long timeout) {
		return timeout(Duration.ofSeconds(timeout));
	}

	public RedisURIBuilder timeout(Duration timeout) {
		return timeout(Optional.of(timeout));
	}

	public RedisURIBuilder timeout(Optional<Duration> timeout) {
		this.timeout = timeout;
		return this;
	}

	public RedisURIBuilder clientName(String clientName) {
		return clientName(Optional.of(clientName));
	}

	public RedisURIBuilder clientName(Optional<String> clientName) {
		this.clientName = clientName;
		return this;
	}

	@SuppressWarnings("deprecation")
	public RedisURI build() {
		RedisURI redisURI = uri.orElse(RedisURI.create(host, port));
		redisURI.setVerifyPeer(sslVerifyMode);
		if (ssl) {
			redisURI.setSsl(true);
		}
		if (startTls) {
			redisURI.setStartTls(true);
		}
		socket.ifPresent(redisURI::setSocket);
		if (credentialsProvider.isPresent()) {
			redisURI.setCredentialsProvider(credentialsProvider.get());
		} else {
			if (username != null) {
				redisURI.setUsername(username);
			}
			if (password != null) {
				redisURI.setPassword(password);
			}
		}
		redisURI.setDatabase(database);
		timeout.ifPresent(redisURI::setTimeout);
		clientName.ifPresent(redisURI::setClientName);
		return redisURI;
	}

	public static RedisURIBuilder create() {
		return new RedisURIBuilder();
	}

}